"""Discovery Worker - Consumes discovery.m365 queue and runs periodic discovery

Two modes:
1. Queue-driven: consumes discovery.m365 messages from RabbitMQ (primary mode for onboarding)
2. Periodic: runs discovery for all tenants every 24 hours (fallback for re-discovery)
"""
import asyncio
import hashlib
import json
import logging
import uuid
from datetime import datetime, timezone
from uuid import UUID
from typing import Dict, Any, List, Optional, Set, Tuple

import aio_pika
import httpx
from aio_pika import IncomingMessage
from sqlalchemy import bindparam, select, text
from sqlalchemy.dialects.postgresql import JSONB

from shared.config import settings
from shared.database import async_session_factory, init_db
from shared.power_bi_client import PowerBIClient
from shared.models import (
    Tenant, TenantStatus, TenantType,
    Resource, ResourceStatus, ResourceType,
    DiscoveryRun,
)
from shared.graph_client import GraphClient
from shared.message_bus import message_bus
from azure_discovery import discover_azure_tenant as discover_azure_resources

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("discovery-worker")


async def _emit_discovery_audit(
    action: str,
    tenant_id: UUID,
    tenant_name: str,
    discovery_type: str,
    outcome: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
) -> None:
    """Post a discovery event to the audit service so it shows up in the
    activity feed. Non-fatal: network/audit-service errors are swallowed so
    they never break the discovery flow itself."""
    payload: Dict[str, Any] = {
        "action": action,
        "tenant_id": str(tenant_id),
        "actor_type": "WORKER",
        "resource_type": discovery_type,
        "resource_name": tenant_name,
        "details": {"type": discovery_type, **(details or {})},
    }
    if outcome:
        payload["outcome"] = outcome
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            await client.post(
                f"{settings.AUDIT_SERVICE_URL}/api/v1/audit/log", json=payload,
            )
    except Exception as exc:
        logger.warning("[audit] failed to emit %s for tenant %s: %s", action, tenant_id, exc)


TYPE_MAP: Dict[str, ResourceType] = {
    "MAILBOX": ResourceType.MAILBOX,
    "SHARED_MAILBOX": ResourceType.SHARED_MAILBOX,
    "ROOM_MAILBOX": ResourceType.ROOM_MAILBOX,
    "ONEDRIVE": ResourceType.ONEDRIVE,
    "SHAREPOINT_SITE": ResourceType.SHAREPOINT_SITE,
    "TEAMS_CHANNEL": ResourceType.TEAMS_CHANNEL,
    "TEAMS_CHAT": ResourceType.TEAMS_CHAT,
    "TEAMS_CHAT_EXPORT": ResourceType.TEAMS_CHAT_EXPORT,
    "ENTRA_USER": ResourceType.ENTRA_USER,
    "ENTRA_GROUP": ResourceType.ENTRA_GROUP,
    "M365_GROUP": ResourceType.M365_GROUP,
    "ENTRA_CONDITIONAL_ACCESS": ResourceType.ENTRA_CONDITIONAL_ACCESS,
    "ENTRA_BITLOCKER_KEY": ResourceType.ENTRA_BITLOCKER_KEY,
    "ENTRA_APP": ResourceType.ENTRA_APP,
    "ENTRA_DEVICE": ResourceType.ENTRA_DEVICE,
    "AZURE_VM": ResourceType.AZURE_VM,
    "AZURE_SQL_DB": ResourceType.AZURE_SQL_DB,
    "AZURE_POSTGRESQL": ResourceType.AZURE_POSTGRESQL,
    "POWER_BI": ResourceType.POWER_BI,
    "POWER_APPS": ResourceType.POWER_APPS,
    "POWER_AUTOMATE": ResourceType.POWER_AUTOMATE,
    "POWER_DLP": ResourceType.POWER_DLP,
    "PLANNER": ResourceType.PLANNER,
    "TODO": ResourceType.TODO,
    "ONENOTE": ResourceType.ONENOTE,
}

DISCOVERY_SCOPE_DEFINITIONS: Dict[str, Dict[str, Any]] = {
    "users": {"method": "discover_users", "resource_types": {ResourceType.ENTRA_USER}},
    # discover_groups emits a mix of ENTRA_GROUP (DLs / security groups) and
    # M365_GROUP (Unified groups) — list both so stale-marking covers them.
    "groups": {"method": "discover_groups", "resource_types": {ResourceType.ENTRA_GROUP, ResourceType.M365_GROUP}},
    "mailboxes": {
        "method": "discover_mailboxes",
        # Tier 1 only emits SHARED + ROOM mailboxes — user MAILBOX rows are
        # Tier 2 children of an ENTRA_USER (USER_MAIL). Pass the explicit
        # `kinds` filter so the scope-based discovery path matches
        # `discover_all()` (graph_client.py:1768) and never creates a
        # duplicate user MAILBOX row alongside its Tier 2 USER_MAIL sibling.
        # MAILBOX stays in the stale-mark set so any pre-migration legacy
        # rows get cleaned up by the next pass.
        "method_kwargs": {"kinds": {"SHARED_MAILBOX", "ROOM_MAILBOX"}},
        "resource_types": {ResourceType.MAILBOX, ResourceType.SHARED_MAILBOX, ResourceType.ROOM_MAILBOX},
    },
    # `discover_onedrive` is intentionally NOT a tenant-wide scope: per-user
    # drives are Tier 2 USER_ONEDRIVE rows created via `discover_user_content`
    # under each ENTRA_USER. Keeping the method on GraphClient (for shared /
    # group drives, group-OneDrive recovery, etc.) but never auto-firing at
    # the tenant scope avoids the Tier-1/Tier-2 double walk that bloated
    # backup time + Graph quota by ~2× per user.
    "sharepoint": {"method": "discover_sharepoint", "resource_types": {ResourceType.SHAREPOINT_SITE}},
    # Tier 1: channels only. Per-user TEAMS_CHAT + TEAMS_CHAT_EXPORT shards
    # are now produced by the Tier 2 per-user discovery, not the tenant scan.
    "teams": {
        "method": "discover_teams",
        "method_kwargs": {"include_chats": False},
        "resource_types": {ResourceType.TEAMS_CHANNEL},
    },
    "planner": {"method": "discover_planner", "resource_types": {ResourceType.PLANNER}},
    "todo": {"method": "discover_todo", "resource_types": {ResourceType.TODO}},
    "power_platform": {
        "method": "discover_power_platform",
        "resource_types": {ResourceType.POWER_BI, ResourceType.POWER_APPS, ResourceType.POWER_AUTOMATE},
    },
    # Phase 2 P2 — security-critical Entra extras. CA policies and BitLocker
    # keys are tenant-singleton resources; afi captures both so a takeover or
    # misconfiguration can be reverted from the last clean snapshot.
    "conditional_access": {
        "method": "discover_conditional_access",
        "resource_types": {ResourceType.ENTRA_CONDITIONAL_ACCESS},
    },
    "bitlocker": {
        "method": "discover_bitlocker_keys",
        "resource_types": {ResourceType.ENTRA_BITLOCKER_KEY},
    },
}

DISCOVERY_SCOPE_ALIASES: Dict[str, str] = {
    "shared_mailboxes": "mailboxes",
}

FULL_DISCOVERY_RESOURCE_TYPES: Set[ResourceType] = {
    # Tier 1 only — what `graph.discover_all()` actually scans. If you add a
    # category to discover_all, mirror it here so stale-marking covers it; if
    # you don't, leave it out so a tenant-wide scan doesn't accidentally
    # mark, say, every PLANNER row stale just because we no longer scan
    # planner by default.
    ResourceType.ENTRA_USER,
    ResourceType.ENTRA_GROUP,
    ResourceType.M365_GROUP,
    ResourceType.SHARED_MAILBOX,
    ResourceType.ROOM_MAILBOX,
    ResourceType.SHAREPOINT_SITE,
    ResourceType.TEAMS_CHANNEL,
    ResourceType.POWER_BI,
    ResourceType.POWER_APPS,
    ResourceType.POWER_AUTOMATE,
}

STAGE_INSERT_STMT = text(
    """
    INSERT INTO resource_discovery_staging (
        run_id,
        tenant_id,
        resource_type,
        external_id,
        display_name,
        email,
        metadata,
        resource_status,
        resource_hash,
        azure_subscription_id,
        azure_resource_group,
        azure_region,
        discovered_at
    ) VALUES (
        :run_id,
        :tenant_id,
        :resource_type,
        :external_id,
        :display_name,
        :email,
        :metadata,
        :resource_status,
        :resource_hash,
        :azure_subscription_id,
        :azure_resource_group,
        :azure_region,
        :discovered_at
    )
    """
).bindparams(bindparam("metadata", type_=JSONB))

RESOURCE_STATUS_EXPR = """
CASE
    WHEN r.status = 'ACTIVE'::resourcestatus AND s.resource_status = 'DISCOVERED' THEN 'ACTIVE'::resourcestatus
    WHEN r.status IN ('ARCHIVED'::resourcestatus, 'SUSPENDED'::resourcestatus, 'PENDING_DELETION'::resourcestatus) THEN r.status
    ELSE s.resource_status::resourcestatus
END
"""

MERGE_UPDATED_COUNT_STMT = text(
    f"""
    WITH updated AS (
        UPDATE resources AS r
        SET
            display_name = s.display_name,
            email = s.email,
            metadata = s.metadata::json,
            resource_hash = s.resource_hash,
            azure_subscription_id = s.azure_subscription_id,
            azure_resource_group = s.azure_resource_group,
            azure_region = s.azure_region,
            discovered_at = s.discovered_at,
            status = {RESOURCE_STATUS_EXPR},
            updated_at = NOW()
        FROM resource_discovery_staging AS s
        WHERE s.run_id = :run_id
          AND r.tenant_id = s.tenant_id
          AND r.type::text = s.resource_type
          AND r.external_id = s.external_id
          AND (
              r.display_name IS DISTINCT FROM s.display_name
              OR r.email IS DISTINCT FROM s.email
              OR r.metadata::jsonb IS DISTINCT FROM s.metadata
              OR r.resource_hash IS DISTINCT FROM s.resource_hash
              OR r.azure_subscription_id IS DISTINCT FROM s.azure_subscription_id
              OR r.azure_resource_group IS DISTINCT FROM s.azure_resource_group
              OR r.azure_region IS DISTINCT FROM s.azure_region
              OR (
                  CASE
                      WHEN r.status = 'ACTIVE'::resourcestatus AND s.resource_status = 'DISCOVERED' THEN 'ACTIVE'::resourcestatus
                      WHEN r.status IN ('ARCHIVED'::resourcestatus, 'SUSPENDED'::resourcestatus, 'PENDING_DELETION'::resourcestatus) THEN r.status
                      ELSE s.resource_status::resourcestatus
                  END
              ) IS DISTINCT FROM r.status
          )
        RETURNING 1
    )
    SELECT COUNT(*) FROM updated
    """
)

MERGE_INSERTED_COUNT_STMT = text(
    """
    WITH inserted AS (
        INSERT INTO resources (
            id,
            tenant_id,
            type,
            external_id,
            display_name,
            email,
            metadata,
            resource_hash,
            status,
            discovered_at,
            azure_subscription_id,
            azure_resource_group,
            azure_region,
            created_at,
            updated_at
        )
        SELECT
            gen_random_uuid(),
            s.tenant_id,
            s.resource_type::resourcetype,
            s.external_id,
            s.display_name,
            s.email,
            s.metadata::json,
            s.resource_hash,
            s.resource_status::resourcestatus,
            s.discovered_at,
            s.azure_subscription_id,
            s.azure_resource_group,
            s.azure_region,
            NOW(),
            NOW()
        FROM resource_discovery_staging AS s
        LEFT JOIN resources AS r
          ON r.tenant_id = s.tenant_id
         AND r.type::text = s.resource_type
         AND r.external_id = s.external_id
        WHERE s.run_id = :run_id
          AND r.id IS NULL
        RETURNING 1
    )
    SELECT COUNT(*) FROM inserted
    """
)

STALE_MARK_COUNT_STMT = text(
    """
    WITH stale AS (
        UPDATE resources AS r
        SET
            status = 'INACCESSIBLE'::resourcestatus,
            updated_at = NOW()
        WHERE r.tenant_id = :tenant_id
          AND r.status IN ('DISCOVERED'::resourcestatus, 'ACTIVE'::resourcestatus)
          AND r.type::text IN :resource_types
          AND NOT EXISTS (
              SELECT 1
              FROM resource_discovery_staging AS s
              WHERE s.run_id = :run_id
                AND s.tenant_id = r.tenant_id
                AND s.resource_type = r.type::text
                AND s.external_id = r.external_id
          )
        RETURNING 1
    )
    SELECT COUNT(*) FROM stale
    """
).bindparams(bindparam("resource_types", expanding=True))


async def decrypt_tenant_secret(tenant: Tenant) -> str:
    """Decrypt the tenant's Graph client secret from the DB."""
    from shared.security import decrypt_secret
    if not tenant.graph_client_secret_encrypted:
        # Fall back to env var (legacy mode)
        return settings.MICROSOFT_CLIENT_SECRET or ""
    return decrypt_secret(tenant.graph_client_secret_encrypted)


def _dedupe_resources(resources: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    unique: List[Dict[str, Any]] = []
    for resource in resources:
        resource_type = resource.get("type") or resource.get("resource_type")
        key = f"{resource.get('external_id')}:{resource_type}"
        if key and key not in seen:
            seen.add(key)
            unique.append(resource)
    return unique


def _tenant_lock_id(tenant_id: UUID) -> int:
    tenant_int = tenant_id.int
    return ((tenant_int >> 64) ^ (tenant_int & 0x7FFFFFFFFFFFFFFF)) & 0x7FFFFFFFFFFFFFFF


def _chunked(items: List[Dict[str, Any]], chunk_size: int) -> List[List[Dict[str, Any]]]:
    if chunk_size <= 0:
        return [items]
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _build_resource_hash(resource_type: str, external_id: str, display_name: str, email: Optional[str], metadata: Dict[str, Any], resource_status: str) -> str:
    payload = {
        "type": resource_type,
        "external_id": external_id,
        "display_name": display_name,
        "email": email,
        "metadata": metadata,
        "status": resource_status,
    }
    return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


def _prepare_staging_rows(tenant_id: UUID, resources: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    prepared: List[Dict[str, Any]] = []
    discovered_at = datetime.now(timezone.utc).replace(tzinfo=None)
    for resource in resources:
        rtype_str = resource.get("type", "ENTRA_USER")
        if rtype_str not in TYPE_MAP:
            logger.warning("Skipping discovery row with unsupported type '%s'", rtype_str)
            continue

        external_id = resource.get("external_id")
        if not external_id:
            logger.warning("Skipping discovery row missing external_id for type '%s'", rtype_str)
            continue

        display_name = resource.get("display_name") or "Unknown"
        email = resource.get("email")
        metadata = resource.get("metadata") or {}
        resource_status = (
            ResourceStatus.DISCOVERED.value
            if resource.get("_account_enabled", True)
            else ResourceStatus.INACCESSIBLE.value
        )
        prepared.append(
            {
                "tenant_id": tenant_id,
                "resource_type": rtype_str,
                "external_id": external_id,
                "display_name": display_name,
                "email": email,
                "metadata": metadata,
                "resource_status": resource_status,
                "resource_hash": _build_resource_hash(
                    rtype_str,
                    external_id,
                    display_name,
                    email,
                    metadata,
                    resource_status,
                ),
                "azure_subscription_id": None,
                "azure_resource_group": None,
                "azure_region": None,
                "discovered_at": discovered_at,
            }
        )
    return prepared


AZURE_DISCOVERY_RESOURCE_TYPES: Set[ResourceType] = {
    ResourceType.AZURE_VM,
    ResourceType.AZURE_SQL_DB,
    ResourceType.AZURE_POSTGRESQL,
    ResourceType.AZURE_POSTGRESQL_SINGLE,
}


def _prepare_azure_staging_rows(tenant_id: UUID, discovered: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Set[ResourceType]]:
    prepared: List[Dict[str, Any]] = []
    discovered_at = datetime.now(timezone.utc).replace(tzinfo=None)

    for vm in discovered.get("vms", []):
        metadata = {
            "vm_id": vm.get("id", ""),
            "location": vm.get("location", ""),
            "subscription_id": vm.get("subscription_id", ""),
        }
        display_name = vm.get("name", "Unknown")
        external_id = vm.get("name", vm.get("id", ""))
        prepared.append(
            {
                "tenant_id": tenant_id,
                "resource_type": ResourceType.AZURE_VM.value,
                "external_id": external_id,
                "display_name": display_name,
                "email": None,
                "metadata": metadata,
                "resource_status": ResourceStatus.DISCOVERED.value,
                "resource_hash": _build_resource_hash(
                    ResourceType.AZURE_VM.value,
                    external_id,
                    display_name,
                    None,
                    metadata,
                    ResourceStatus.DISCOVERED.value,
                ),
                "azure_subscription_id": vm.get("subscription_id"),
                "azure_resource_group": vm.get("resource_group"),
                "azure_region": vm.get("location"),
                "discovered_at": discovered_at,
            }
        )

    for sql_db in discovered.get("sql_dbs", []):
        metadata = {
            "server_name": sql_db.get("server_name", ""),
            "server_fqdn": sql_db.get("server_fqdn", ""),
            "server_id": sql_db.get("server_id", ""),
            "database_id": sql_db.get("database_id", ""),
            "subscription_id": sql_db.get("subscription_id", ""),
        }
        external_id = sql_db.get("database_name", "")
        display_name = f"{sql_db.get('server_name', '')}/{external_id}"
        prepared.append(
            {
                "tenant_id": tenant_id,
                "resource_type": ResourceType.AZURE_SQL_DB.value,
                "external_id": external_id,
                "display_name": display_name,
                "email": None,
                "metadata": metadata,
                "resource_status": ResourceStatus.DISCOVERED.value,
                "resource_hash": _build_resource_hash(
                    ResourceType.AZURE_SQL_DB.value,
                    external_id,
                    display_name,
                    None,
                    metadata,
                    ResourceStatus.DISCOVERED.value,
                ),
                "azure_subscription_id": sql_db.get("subscription_id"),
                "azure_resource_group": sql_db.get("resource_group"),
                "azure_region": sql_db.get("location"),
                "discovered_at": discovered_at,
            }
        )

    for pg in discovered.get("postgres_servers", []):
        resource_type = (
            ResourceType.AZURE_POSTGRESQL_SINGLE
            if pg.get("type", "") == "Microsoft.DBforPostgreSQL/servers"
            else ResourceType.AZURE_POSTGRESQL
        )
        db_name = pg.get("database_name", "postgres")
        metadata = {
            "server_name": pg.get("name", ""),
            "server_id": pg.get("id", ""),
            "database_name": db_name,
            "location": pg.get("location", ""),
            "subscription_id": pg.get("subscription_id", ""),
            "type": pg.get("type", ""),
        }
        external_id = pg.get("name", pg.get("id", ""))
        display_name = f"{pg.get('name', 'Unknown')}/{db_name}"
        prepared.append(
            {
                "tenant_id": tenant_id,
                "resource_type": resource_type.value,
                "external_id": external_id,
                "display_name": display_name,
                "email": None,
                "metadata": metadata,
                "resource_status": ResourceStatus.DISCOVERED.value,
                "resource_hash": _build_resource_hash(
                    resource_type.value,
                    external_id,
                    display_name,
                    None,
                    metadata,
                    ResourceStatus.DISCOVERED.value,
                ),
                "azure_subscription_id": pg.get("subscription_id"),
                "azure_resource_group": pg.get("resource_group"),
                "azure_region": pg.get("location"),
                "discovered_at": discovered_at,
            }
        )

    return _dedupe_resources(prepared), set(AZURE_DISCOVERY_RESOURCE_TYPES)


async def _mark_discovery_failed(
    tenant_id: UUID,
    run_id: Optional[UUID],
    previous_status: Optional[TenantStatus],
    error_message: str,
) -> None:
    async with async_session_factory() as db:
        tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
        if tenant and previous_status is not None:
            tenant.status = previous_status

        if run_id is not None:
            run = (await db.execute(select(DiscoveryRun).where(DiscoveryRun.id == run_id))).scalar_one_or_none()
            if run is not None:
                run.status = "FAILED"
                run.error_message = error_message[:2000]
                run.finished_at = datetime.now(timezone.utc).replace(tzinfo=None)

        await db.commit()


async def _persist_discovery_rows(
    db,
    tenant: Tenant,
    run: DiscoveryRun,
    tenant_id: UUID,
    staged_rows: List[Dict[str, Any]],
    successful_scope_types: Optional[Set[ResourceType]],
    *,
    fetched_count: int,
) -> Tuple[int, int, int, int]:
    staged_count = len(staged_rows)
    if staged_rows:
        processed = 0
        progress_every = max(settings.DISCOVERY_PROGRESS_LOG_EVERY, 1)
        for chunk in _chunked(staged_rows, settings.DISCOVERY_STAGE_CHUNK_SIZE):
            payload = [{"run_id": run.id, **row} for row in chunk]
            await db.execute(STAGE_INSERT_STMT, payload)
            processed += len(chunk)
            if processed % progress_every == 0 or processed == staged_count:
                logger.info(
                    "Discovery staging progress for tenant %s run %s: %d/%d rows staged.",
                    tenant_id,
                    run.id,
                    processed,
                    staged_count,
                )

    updated_count = int((await db.execute(MERGE_UPDATED_COUNT_STMT, {"run_id": run.id})).scalar() or 0)
    inserted_count = int((await db.execute(MERGE_INSERTED_COUNT_STMT, {"run_id": run.id})).scalar() or 0)
    unchanged_count = max(staged_count - updated_count - inserted_count, 0)

    stale_marked_count = 0
    if successful_scope_types:
        stale_marked_count = int(
            (
                await db.execute(
                    STALE_MARK_COUNT_STMT,
                    {
                        "run_id": run.id,
                        "tenant_id": tenant_id,
                        "resource_types": sorted({resource_type.value for resource_type in successful_scope_types}),
                    },
                )
            ).scalar()
            or 0
        )

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    run.status = "COMPLETED"
    run.fetched_count = fetched_count
    run.staged_count = staged_count
    run.inserted_count = inserted_count
    run.updated_count = updated_count
    run.unchanged_count = unchanged_count
    run.stale_marked_count = stale_marked_count
    run.finished_at = now
    run.error_message = None

    tenant.status = TenantStatus.ACTIVE
    tenant.last_discovery_at = now

    await db.execute(
        text("DELETE FROM resource_discovery_staging WHERE run_id = :run_id"),
        {"run_id": run.id},
    )

    # Phase 2 — auto-protect: apply resource-group policies to newly-inserted / unassigned resources.
    # Evaluated AFTER the MERGE so newly-inserted rows exist. Only touches resources
    # with NULL sla_policy_id, so we never override an explicit admin assignment.
    try:
        await _apply_auto_protect_groups(db, tenant_id)
    except Exception as exc:
        # Non-fatal: group matching should never block a discovery run from finishing.
        logger.warning("Auto-protect group evaluation failed for tenant %s: %s", tenant_id, exc)

    return staged_count, inserted_count, updated_count, stale_marked_count


async def _apply_auto_protect_groups(db, tenant_id: UUID) -> int:
    """Evaluate auto-protect resource groups for this tenant and assign policies.

    Flow:
      1. Load enabled groups with auto_protect_new=true and at least one policy attached
      2. Load resources in this tenant without an sla_policy_id (or optionally
         include those missing — current design: only assigns to nulls)
      3. For each resource, find matching groups (sorted by priority ASC — lower
         number = higher precedence, matches afi convention)
      4. Take the first matching group's first attached policy — assign it to the resource
      5. Commit

    Returns the count of resources auto-assigned."""
    from shared.models import ResourceGroup, GroupPolicyAssignment, Resource as _Res, SlaPolicy
    from shared.resource_group_matcher import find_matching_groups
    from sqlalchemy import or_

    # 1. candidate groups — eligible when EITHER:
    #    a) the group itself has auto_protect_new=true (group-level opt-in), OR
    #    b) at least one policy attached to the group has
    #       auto_apply_to_matching=true (policy-level opt-in from the SLA wizard).
    # The wizard's checkbox sets (b); the resource-group admin sets (a).
    # Both routes funnel through the same matching + assign logic below.
    auto_apply_policy_ids_stmt = select(SlaPolicy.id).where(
        SlaPolicy.tenant_id == tenant_id,
        SlaPolicy.enabled.is_(True),
        SlaPolicy.auto_apply_to_matching.is_(True),
    )
    auto_apply_policy_ids = [row[0] for row in (await db.execute(auto_apply_policy_ids_stmt)).all()]

    if auto_apply_policy_ids:
        groups_attached_to_auto_policies = select(GroupPolicyAssignment.group_id).where(
            GroupPolicyAssignment.policy_id.in_(auto_apply_policy_ids)
        )
        groups_stmt = select(ResourceGroup).where(
            ResourceGroup.tenant_id == tenant_id,
            ResourceGroup.enabled.is_(True),
            or_(
                ResourceGroup.auto_protect_new.is_(True),
                ResourceGroup.id.in_(groups_attached_to_auto_policies),
            ),
        ).order_by(ResourceGroup.priority.asc())
    else:
        groups_stmt = select(ResourceGroup).where(
            ResourceGroup.tenant_id == tenant_id,
            ResourceGroup.enabled.is_(True),
            ResourceGroup.auto_protect_new.is_(True),
        ).order_by(ResourceGroup.priority.asc())

    groups = (await db.execute(groups_stmt)).scalars().all()
    if not groups:
        return 0

    # Preload policy assignments per group (single query)
    group_ids = [g.id for g in groups]
    assign_stmt = select(GroupPolicyAssignment).where(GroupPolicyAssignment.group_id.in_(group_ids))
    assignments = (await db.execute(assign_stmt)).scalars().all()
    group_to_policies: Dict[UUID, List[UUID]] = {}
    for a in assignments:
        group_to_policies.setdefault(a.group_id, []).append(a.policy_id)
    # Drop groups with no attached policy — nothing to assign
    eligible_groups = [g for g in groups if group_to_policies.get(g.id)]
    if not eligible_groups:
        return 0

    # 2. Resources in this tenant without a policy. Stream + chunked
    #    bulk UPDATE so a 25k-resource tenant doesn't materialize the
    #    full unpolicied set into memory and doesn't hold a single huge
    #    transaction across the whole sweep.
    from sqlalchemy import update as sa_update

    CHUNK = 1000
    pending: Dict[UUID, List[UUID]] = {}  # policy_id -> list of resource ids
    assigned = 0

    res_stream = await db.stream(
        select(_Res)
        .where(_Res.tenant_id == tenant_id)
        .where(_Res.sla_policy_id.is_(None))
        .execution_options(yield_per=CHUNK)
    )

    async def _flush() -> int:
        nonlocal pending
        n = 0
        for pol_id, rids in pending.items():
            if not rids:
                continue
            await db.execute(
                sa_update(_Res)
                .where(_Res.id.in_(rids))
                .where(_Res.sla_policy_id.is_(None))  # don't clobber races
                .values(sla_policy_id=pol_id)
            )
            n += len(rids)
        pending = {}
        if n:
            await db.commit()
        return n

    async for resource in res_stream.scalars():
        matched = find_matching_groups(resource, eligible_groups)
        if not matched:
            continue
        top_group = matched[0]
        policies_for_group = group_to_policies.get(top_group.id) or []
        if not policies_for_group:
            continue
        pol_id = policies_for_group[0]
        pending.setdefault(pol_id, []).append(resource.id)
        # Flush when any single bucket reaches CHUNK — keeps the bulk
        # UPDATE statement's parameter count predictable.
        if len(pending[pol_id]) >= CHUNK:
            assigned += await _flush()

    assigned += await _flush()

    if assigned:
        logger.info("[auto-protect] Assigned policies to %d newly-discovered resource(s) for tenant %s",
                    assigned, tenant_id)
    return assigned


async def _discover_resources_for_scope(
    graph: GraphClient,
    discovery_scope: Optional[List[str]],
) -> Tuple[List[Dict[str, Any]], Optional[Set[ResourceType]]]:
    if not discovery_scope:
        return _dedupe_resources(await graph.discover_all()), set(FULL_DISCOVERY_RESOURCE_TYPES)

    canonical_scopes: List[str] = []
    seen_scopes = set()
    for raw_scope in discovery_scope:
        if not raw_scope:
            continue
        scope = DISCOVERY_SCOPE_ALIASES.get(raw_scope, raw_scope)
        if scope not in DISCOVERY_SCOPE_DEFINITIONS:
            logger.warning("Ignoring unknown discovery scope '%s'", raw_scope)
            continue
        if scope in seen_scopes:
            continue
        seen_scopes.add(scope)
        canonical_scopes.append(scope)

    if not canonical_scopes:
        logger.warning("No valid discovery scopes supplied; skipping scoped discovery run.")
        return [], set()

    async def _safe_discover(scope_name: str) -> Tuple[str, List[Dict[str, Any]], bool]:
        spec = DISCOVERY_SCOPE_DEFINITIONS[scope_name]
        method = getattr(graph, spec["method"])
        kwargs = spec.get("method_kwargs") or {}
        try:
            result = await method(**kwargs)
            logger.info("Scoped discovery '%s' returned %d resource(s)", scope_name, len(result))
            return scope_name, result, False
        except Exception as exc:
            logger.warning("Scoped discovery '%s' failed: %s", scope_name, exc)
            return scope_name, [], True

    results = await asyncio.gather(*[_safe_discover(scope) for scope in canonical_scopes])

    resources: List[Dict[str, Any]] = []
    successful_types: Set[ResourceType] = set()
    for scope_name, result, failed in results:
        if failed:
            continue
        resources.extend(result)
        successful_types.update(DISCOVERY_SCOPE_DEFINITIONS[scope_name]["resource_types"])

    return _dedupe_resources(resources), successful_types


async def discover_tenant(tenant_id: UUID, discovery_scope: Optional[List[str]] = None) -> int:
    """Run discovery for a single tenant. Returns count of staged resources."""
    run_id: Optional[UUID] = None
    previous_status: Optional[TenantStatus] = None
    tenant_display_name = str(tenant_id)
    power_bi_refresh_token: Optional[str] = None

    try:
        async with async_session_factory() as db:
            tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
            if not tenant:
                logger.warning("Tenant %s not found", tenant_id)
                return 0

            previous_status = tenant.status
            tenant_display_name = tenant.display_name

            client_id = tenant.graph_client_id or settings.MICROSOFT_CLIENT_ID or settings.AZURE_AD_CLIENT_ID
            client_secret = await decrypt_tenant_secret(tenant)
            ext_tenant_id = tenant.external_tenant_id or settings.MICROSOFT_TENANT_ID or settings.AZURE_AD_TENANT_ID or "common"

            if not client_id or not client_secret:
                logger.warning("No Microsoft credentials. Skipping tenant %s.", tenant_id)
                return 0

            power_bi_refresh_token = PowerBIClient.get_refresh_token_from_tenant(tenant)
            run_id = uuid.uuid4()
            tenant.status = TenantStatus.DISCOVERING
            db.add(
                DiscoveryRun(
                    id=run_id,
                    tenant_id=tenant.id,
                    scope=discovery_scope or [],
                    status="RUNNING",
                )
            )
            await db.commit()

        logger.info(
            "Starting discovery for tenant %s (%s) using %s auth for Power BI with scope=%s...",
            tenant_display_name,
            tenant_id,
            "delegated service-user" if power_bi_refresh_token else "app-only",
            discovery_scope or "FULL",
        )
        await _emit_discovery_audit(
            "DISCOVERY_STARTED", tenant_id, tenant_display_name, "M365",
            details={"scope": discovery_scope or "FULL"},
        )

        graph = GraphClient(
            client_id,
            client_secret,
            ext_tenant_id,
            power_bi_refresh_token=power_bi_refresh_token,
        )
        resources, successful_scope_types = await _discover_resources_for_scope(graph, discovery_scope)
        staged_rows = _prepare_staging_rows(tenant_id, resources)
        logger.info(
            "Discovered %d raw resources for tenant %s; %d prepared for persistence.",
            len(resources),
            tenant_display_name,
            len(staged_rows),
        )

        async with async_session_factory() as db:
            await db.execute(
                text("SELECT pg_advisory_xact_lock(:lock_id)"),
                {"lock_id": _tenant_lock_id(tenant_id)},
            )

            tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
            run = (await db.execute(select(DiscoveryRun).where(DiscoveryRun.id == run_id))).scalar_one_or_none()
            if tenant is None or run is None:
                raise RuntimeError(f"Discovery bookkeeping missing for tenant {tenant_id}")

            if (
                power_bi_refresh_token
                and graph.power_bi_refresh_token
                and graph.power_bi_refresh_token != power_bi_refresh_token
            ):
                await PowerBIClient.persist_refresh_token(db, tenant, graph.power_bi_refresh_token)

            staged_count, inserted_count, updated_count, stale_marked_count = await _persist_discovery_rows(
                db,
                tenant,
                run,
                tenant_id,
                staged_rows,
                successful_scope_types,
                fetched_count=len(resources),
            )
            unchanged_count = run.unchanged_count
            await db.commit()

        logger.info(
            "Discovery complete for tenant %s: %d staged, %d inserted, %d updated, %d unchanged, %d stale-marked.",
            tenant_display_name,
            staged_count,
            inserted_count,
            updated_count,
            unchanged_count,
            stale_marked_count,
        )
        await _emit_discovery_audit(
            "DISCOVERY_RUN", tenant_id, tenant_display_name, "M365",
            outcome="SUCCESS",
            details={
                "resourcesFound": staged_count,
                "inserted": inserted_count,
                "updated": updated_count,
                "unchanged": unchanged_count,
                "staleMarked": stale_marked_count,
            },
        )
        return staged_count

    except Exception as e:
        logger.exception("Discovery failed for tenant %s: %s", tenant_id, str(e))
        try:
            await _mark_discovery_failed(tenant_id, run_id, previous_status, str(e))
        except Exception as recovery_error:
            logger.warning("Failed to persist discovery failure state for tenant %s: %s", tenant_id, recovery_error)
        await _emit_discovery_audit(
            "DISCOVERY_RUN", tenant_id, tenant_display_name, "M365",
            outcome="FAILURE", details={"error": str(e)},
        )
        return 0


async def handle_discovery_message(message: Dict[str, Any]):
    """Handle a single discovery.m365 message from RabbitMQ."""
    tenant_id = UUID(message["tenantId"])
    discovery_scope = message.get("discoveryScope")
    logger.info(
        "[discovery-worker] Consumed discovery.m365 for tenant %s (jobId=%s, scope=%s)",
        tenant_id, message.get("jobId"), discovery_scope or "FULL",
    )
    result = await discover_tenant(tenant_id, discovery_scope=discovery_scope)
    logger.info(
        "[discovery-worker] Discovery result for tenant %s: %d resources",
        tenant_id, result,
    )


async def consume_discovery_queue():
    """Consume messages from discovery.m365 queue."""
    # Retry connecting to RabbitMQ
    max_retries = 30
    for attempt in range(max_retries):
        try:
            await message_bus.connect()
            if message_bus.channel:
                break
            raise RuntimeError("Channel is None after connect")
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning("[discovery-worker] RabbitMQ not ready (attempt %d/%d): %s", attempt + 1, max_retries, e)
                await asyncio.sleep(5)
            else:
                logger.error("[discovery-worker] Failed to connect to RabbitMQ after %d attempts", max_retries)
                raise

    queue = await message_bus.channel.get_queue("discovery.m365")
    logger.info("[discovery-worker] Listening on discovery.m365...")

    # Discovery is low-volume, high-leverage (feeds every downstream
    # backup job). Runs at HIGH priority on the Graph rate limiter so
    # a backlog of backup.normal traffic can't delay new-tenant onboarding.
    from shared.graph_client import graph_priority
    from shared.graph_priority import priority_for_queue
    queue_priority = priority_for_queue("discovery.m365")

    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            try:
                body = json.loads(message.body.decode())
                tenant_id = UUID(body["tenantId"])
                discovery_scope = body.get("discoveryScope")
                logger.info(
                    "[discovery-worker] Consumed discovery.m365 for tenant %s (jobId=%s, scope=%s)",
                    tenant_id, body.get("jobId"), discovery_scope or "FULL",
                )
                # Process discovery and commit within this message scope
                with graph_priority(queue_priority):
                    result_count = await discover_tenant(tenant_id, discovery_scope=discovery_scope)
                # ONLY ack after successful DB commit
                await message.ack()
                logger.info(
                    "[discovery-worker] Discovery complete for tenant %s: %d resources upserted, message acked",
                    tenant_id, result_count,
                )
            except Exception as e:
                logger.exception("[discovery-worker] Error processing discovery message: %s", e)
                try:
                    headers = message.headers or {}
                    retry_count = int(headers.get("x-retry-count", 0))
                    if retry_count >= 5:
                        logger.error(
                            "[discovery-worker] Message exceeded max retries (5), routing to DLQ"
                        )
                        await message.reject(requeue=False)  # goes to DLQ
                    else:
                        await message.nack(requeue=True)
                except Exception:
                    pass


async def discover_azure_tenant(tenant_id: UUID) -> int:
    """
    Run Azure resource discovery for a specific tenant.
    Uses Afi-style delegated ARM access with auto-assignment of SP as SQL admin.
    """
    run_id: Optional[UUID] = None
    previous_status: Optional[TenantStatus] = None
    tenant_display_name = str(tenant_id)

    try:
        async with async_session_factory() as db:
            tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
            if not tenant:
                logger.warning("Azure tenant %s not found", tenant_id)
                return 0

            previous_status = tenant.status
            tenant_display_name = tenant.display_name
            run_id = uuid.uuid4()
            tenant.status = TenantStatus.DISCOVERING
            db.add(
                DiscoveryRun(
                    id=run_id,
                    tenant_id=tenant.id,
                    scope=["azure"],
                    status="RUNNING",
                )
            )
            await db.commit()

        logger.info("Starting Azure resource discovery for tenant %s (%s)...", tenant_display_name, tenant_id)
        await _emit_discovery_audit(
            "DISCOVERY_STARTED", tenant_id, tenant_display_name, "AZURE",
        )

        async with async_session_factory() as db:
            tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
            if tenant is None:
                raise RuntimeError(f"Azure tenant {tenant_id} disappeared during discovery")
            discovered = await discover_azure_resources(tenant)

        logger.info(
            "Discovered %d subscriptions, %d VMs, %d SQL DBs, %d Postgres servers for tenant %s.",
            len(discovered["subscriptions"]), len(discovered["vms"]),
            len(discovered["sql_dbs"]), len(discovered["postgres_servers"]),
            tenant_display_name,
        )
        staged_rows, successful_scope_types = _prepare_azure_staging_rows(tenant_id, discovered)

        async with async_session_factory() as db:
            await db.execute(
                text("SELECT pg_advisory_xact_lock(:lock_id)"),
                {"lock_id": _tenant_lock_id(tenant_id)},
            )

            tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
            run = (await db.execute(select(DiscoveryRun).where(DiscoveryRun.id == run_id))).scalar_one_or_none()
            if tenant is None or run is None:
                raise RuntimeError(f"Azure discovery bookkeeping missing for tenant {tenant_id}")

            staged_count, inserted_count, updated_count, stale_marked_count = await _persist_discovery_rows(
                db,
                tenant,
                run,
                tenant_id,
                staged_rows,
                successful_scope_types,
                fetched_count=len(staged_rows),
            )
            unchanged_count = run.unchanged_count
            await db.commit()

        logger.info(
            "Azure discovery complete for tenant %s: %d staged, %d inserted, %d updated, %d unchanged, %d stale-marked.",
            tenant_display_name,
            staged_count,
            inserted_count,
            updated_count,
            unchanged_count,
            stale_marked_count,
        )
        await _emit_discovery_audit(
            "DISCOVERY_RUN", tenant_id, tenant_display_name, "AZURE",
            outcome="SUCCESS",
            details={
                "resourcesFound": staged_count,
                "inserted": inserted_count,
                "updated": updated_count,
                "unchanged": unchanged_count,
                "staleMarked": stale_marked_count,
            },
        )
        return staged_count

    except Exception as e:
        logger.exception("Azure discovery failed for tenant %s: %s", tenant_id, str(e))
        try:
            await _mark_discovery_failed(tenant_id, run_id, previous_status, str(e))
        except Exception as recovery_error:
            logger.warning("Failed to persist Azure discovery failure state for tenant %s: %s", tenant_id, recovery_error)
        await _emit_discovery_audit(
            "DISCOVERY_RUN", tenant_id, tenant_display_name, "AZURE",
            outcome="FAILURE", details={"error": str(e)},
        )
        return 0


async def consume_azure_discovery_queue():
    """Consume messages from discovery.azure queue for Azure-only tenants."""
    max_retries = 30
    for attempt in range(max_retries):
        try:
            await message_bus.connect()
            if message_bus.channel:
                break
            raise RuntimeError("Channel is None after connect")
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning("[azure-discovery] RabbitMQ not ready (attempt %d/%d): %s", attempt + 1, max_retries, e)
                await asyncio.sleep(5)
            else:
                logger.error("[azure-discovery] Failed to connect to RabbitMQ after %d attempts", max_retries)
                raise

    try:
        queue = await message_bus.channel.get_queue("discovery.azure")
    except Exception:
        logger.warning("[azure-discovery] Queue discovery.azure not found, skipping Azure discovery consumer")
        return

    logger.info("[azure-discovery] Listening on discovery.azure...")

    # Same elevated priority as m365 discovery — low volume, high leverage.
    from shared.graph_client import graph_priority
    from shared.graph_priority import priority_for_queue
    queue_priority = priority_for_queue("discovery.azure")

    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            try:
                body = json.loads(message.body.decode())
                tenant_id = UUID(body["tenantId"])
                logger.info("[azure-discovery] Consumed discovery.azure for tenant %s (jobId=%s)", tenant_id, body.get("jobId"))
                with graph_priority(queue_priority):
                    result_count = await discover_azure_tenant(tenant_id)
                await message.ack()
                logger.info("[azure-discovery] Azure discovery complete for tenant %s: %d resources upserted, message acked", tenant_id, result_count)
            except Exception as e:
                logger.exception("[azure-discovery] Error processing Azure discovery message: %s", e)
                try:
                    headers = message.headers or {}
                    retry_count = int(headers.get("x-retry-count", 0))
                    if retry_count >= 5:
                        logger.error("[azure-discovery] Message exceeded max retries (5), routing to DLQ")
                        await message.reject(requeue=False)
                    else:
                        await message.nack(requeue=True)
                except Exception:
                    pass


async def _consume_tier2_discovery():
    """Consume discovery.tier2 messages.

    Message shape (both single-user and batch forms supported):
      {
        "tenantId": "<uuid>",
        "userResourceIds": ["<uuid>", ...],   # preferred — batch many users
        "userResourceId": "<uuid>",           # legacy single-user
        "source": "SLA_ASSIGNED|BULK_TRIGGER|SCHEDULER_BACKSTOP",
        "thenBackup": true,                   # default false
        "fullBackup": false                   # forwarded to trigger-bulk
      }

    For each user, runs the idempotent helper, then — only if `thenBackup` —
    POSTs the resulting child IDs to job-service `/backups/trigger-bulk`.
    Sequencing matters: rows must exist before the backup is enqueued, or the
    backup worker would chase ghost UUIDs. The HTTP call is what stitches the
    two stages safely without a separate event channel."""
    from shared.tier2_discovery import ensure_tier2_children

    max_retries = 30
    for attempt in range(max_retries):
        try:
            await message_bus.connect()
            if message_bus.channel:
                break
            raise RuntimeError("Channel is None after connect")
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning("[tier2-discovery] RabbitMQ not ready (attempt %d/%d): %s", attempt + 1, max_retries, e)
                await asyncio.sleep(5)
            else:
                logger.error("[tier2-discovery] Failed to connect to RabbitMQ after %d attempts", max_retries)
                raise

    try:
        queue = await message_bus.channel.get_queue("discovery.tier2")
    except Exception:
        logger.warning("[tier2-discovery] Queue discovery.tier2 not declared yet, skipping consumer")
        return

    logger.info("[tier2-discovery] Listening on discovery.tier2...")

    from shared.graph_client import graph_priority
    from shared.graph_priority import priority_for_queue
    # Reuse the m365 discovery priority — Tier-2 is a downstream fan-out of
    # the same discovery class and shouldn't queue behind heavy backups on
    # the Graph rate limiter.
    queue_priority = priority_for_queue("discovery.m365")

    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            try:
                body = json.loads(message.body.decode())
                tenant_id = UUID(body["tenantId"])
                user_ids_raw = body.get("userResourceIds") or (
                    [body["userResourceId"]] if body.get("userResourceId") else []
                )
                if not user_ids_raw:
                    logger.warning("[tier2-discovery] message has no userResourceId(s); dropping")
                    await message.ack()
                    continue
                user_ids = [UUID(u) for u in user_ids_raw]
                then_backup = bool(body.get("thenBackup", False))
                full_backup = bool(body.get("fullBackup", False))
                source = body.get("source") or "UNKNOWN"
                # batch_id propagates from the original bulk-trigger so
                # child Jobs created below share the same Activity row
                # with their parent. Loud WARN on missing — when the
                # BATCH_ROW_REDESIGN_ENABLED flag is on, job-service will
                # also 400 the forwarded trigger-bulk, surfacing any
                # regression on the producer side.
                forward_batch_id = body.get("batchId")
                if not forward_batch_id:
                    logger.warning(
                        "[tier2-discovery] discovery message missing batchId — "
                        "Activity row grouping for tenant=%s users=%d will break",
                        tenant_id, len(user_ids_raw),
                    )

                logger.info(
                    "[tier2-discovery] %s tenant=%s users=%d thenBackup=%s",
                    source, tenant_id, len(user_ids), then_backup,
                )

                # Build the GraphClient ONCE per tenant for the whole batch —
                # avoids rebuilding the MSAL app + token cache per user.
                async with async_session_factory() as db:
                    tenant = await db.get(Tenant, tenant_id)
                    if tenant is None:
                        logger.warning("[tier2-discovery] tenant %s not found", tenant_id)
                        await message.ack()
                        continue
                    client_id = tenant.graph_client_id or settings.MICROSOFT_CLIENT_ID or settings.AZURE_AD_CLIENT_ID
                    client_secret = await decrypt_tenant_secret(tenant)
                    ext_tenant_id = tenant.external_tenant_id or settings.MICROSOFT_TENANT_ID or settings.AZURE_AD_TENANT_ID or "common"
                    if not client_id or not client_secret:
                        logger.warning("[tier2-discovery] missing M365 creds for tenant %s, dropping", tenant_id)
                        await message.ack()
                        continue
                    graph = GraphClient(client_id, client_secret, ext_tenant_id)

                    all_child_ids: list[str] = []
                    # Per-user outcome (matches shared.batch_pending.BatchPendingState
                    # strings). Used below to write batch_pending_users
                    # terminal states for chained-from-batch discovery —
                    # closes the 2026-05-15 race where the batch
                    # could pin IN_PROGRESS forever on users whose
                    # discovery never completed.
                    per_user_outcome: dict = {}
                    for uid in user_ids:
                        user = await db.get(Resource, uid)
                        if user is None or user.type != ResourceType.ENTRA_USER:
                            logger.info("[tier2-discovery] skipping %s (not an ENTRA_USER)", uid)
                            # No outcome row: only ENTRA_USERs are batch
                            # scope candidates.
                            continue
                        with graph_priority(queue_priority):
                            try:
                                children = await ensure_tier2_children(db, user, graph, commit=False)
                            except Exception as gex:
                                logger.exception("[tier2-discovery] discovery failed for user %s: %s", uid, gex)
                                per_user_outcome[str(uid)] = "DISCOVERY_FAILED"
                                continue
                        live_children = [
                            c for c in children
                            if c.status != ResourceStatus.INACCESSIBLE
                        ]
                        all_child_ids.extend(str(c.id) for c in live_children)
                        # NO_CONTENT iff discovery returned zero live
                        # Tier-2 children — finalizer treats this as a
                        # terminal gate-1 pass for the user. Otherwise
                        # BACKUP_ENQUEUED (the thenBackup chain below
                        # actually posts to /backups/trigger-bulk).
                        per_user_outcome[str(uid)] = (
                            "BACKUP_ENQUEUED" if live_children else "NO_CONTENT"
                        )

                    # Flip pending rows from WAITING_DISCOVERY → terminal
                    # state for batch-chained discovery only. The
                    # WHERE state='WAITING_DISCOVERY' clause makes the
                    # UPDATE race-commute with the watchdog sweeper —
                    # whichever runs first wins, the other is a no-op.
                    if forward_batch_id and per_user_outcome:
                        for uid_str, outcome in per_user_outcome.items():
                            await db.execute(text("""
                                UPDATE batch_pending_users
                                   SET state = :st, updated_at = NOW()
                                 WHERE batch_id = cast(:bid AS uuid)
                                   AND user_id  = cast(:uid AS uuid)
                                   AND state    = 'WAITING_DISCOVERY'
                            """), {
                                "st": outcome,
                                "bid": forward_batch_id,
                                "uid": uid_str,
                            })
                    # Commit all users in one transaction — keeps the batch
                    # atomic and amortises the WAL flush across N users.
                    await db.commit()

                if then_backup and all_child_ids:
                    try:
                        import httpx as _httpx
                        async with _httpx.AsyncClient(timeout=15.0) as _c:
                            payload = {
                                "resourceIds": all_child_ids,
                                "fullBackup": full_backup,
                                "priority": 1,
                                # Mark this fan-out as Tier-2 so audit-service
                                # doesn't double-count the discovered children
                                # in the parent click's Activity-row "X
                                # resources" total. The Jobs still run, still
                                # contribute progress / status — only the
                                # display count omits them.
                                "tier2": True,
                            }
                            # Always forward — may be None when caller forgot;
                            # job-service rejects in that case under the flag.
                            payload["batchId"] = forward_batch_id
                            r = await _c.post(
                                f"{settings.JOB_SERVICE_URL}/api/v1/backups/trigger-bulk",
                                json=payload,
                            )
                            if r.status_code >= 300:
                                logger.warning(
                                    "[tier2-discovery] trigger-bulk rejected (%d): %s",
                                    r.status_code, r.text[:300],
                                )
                            else:
                                logger.info(
                                    "[tier2-discovery] queued bulk backup for %d Tier-2 child(ren)",
                                    len(all_child_ids),
                                )
                    except Exception as e:
                        logger.exception("[tier2-discovery] trigger-bulk call failed: %s", e)

                await message.ack()

            except Exception as e:
                logger.exception("[tier2-discovery] error processing message: %s", e)
                try:
                    headers = message.headers or {}
                    retry_count = int(headers.get("x-retry-count", 0))
                    if retry_count >= 5:
                        logger.error("[tier2-discovery] exceeded max retries (5), routing to DLQ")
                        await message.reject(requeue=False)
                    else:
                        await message.nack(requeue=True)
                except Exception:
                    pass


async def run_all_discoveries():
    """Run discovery for all eligible tenants (periodic fallback)."""
    async with async_session_factory() as db:
        try:
            stmt = select(Tenant).where(
                text("type IN ('M365', 'AZURE')"),
                text("status IN ('ACTIVE', 'PENDING', 'DISCOVERING')"),
            )
            result = await db.execute(stmt)
            tenants = result.scalars().all()

            if not tenants:
                logger.info("No tenants found for discovery.")
                return

            logger.info("Running discovery for %d tenant(s)...", len(tenants))

            for tenant in tenants:
                try:
                    count = await discover_tenant(tenant.id)
                except Exception as e:
                    logger.error("Failed to process tenant %s: %s", tenant.display_name, e)
        except Exception as e:
            logger.error("Failed to fetch tenants: %s", e)


async def main():
    from shared.storage.startup import startup_router
    from shared import core_metrics
    from shared.graph_rate_limiter import graph_rate_limiter
    core_metrics.init()
    await graph_rate_limiter.maybe_init_redis()
    await startup_router()
    logger.info("=== Discovery Worker Starting ===")
    logger.info("DB: %s@%s:%s/%s", settings.DB_USERNAME, settings.DB_HOST, settings.DB_PORT, settings.DB_NAME)
    logger.info("Microsoft Client ID: %s", settings.MICROSOFT_CLIENT_ID or settings.AZURE_AD_CLIENT_ID or "NOT SET")
    logger.info("ARM Client ID: %s", settings.EFFECTIVE_ARM_CLIENT_ID or "NOT SET")
    logger.info("Schema: %s", settings.DB_SCHEMA)
    logger.info("RabbitMQ: %s", settings.RABBITMQ_ENABLED)

    # Ensure DB tables exist
    await init_db()

    # Start M365 queue consumer (primary mode)
    m365_consumer_task = asyncio.create_task(consume_discovery_queue())

    # Start Azure discovery queue consumer (separate from M365)
    azure_consumer_task = asyncio.create_task(consume_azure_discovery_queue())

    # Per-user Tier-2 content discovery consumer
    tier2_consumer_task = asyncio.create_task(_consume_tier2_discovery())

    # Also run periodic M365 discovery as fallback (every 24 hours)
    async def periodic_discovery():
        while True:
            await asyncio.sleep(24 * 60 * 60)
            logger.info("=== Running scheduled periodic M365 discovery ===")
            await run_all_discoveries()

    periodic_task = asyncio.create_task(periodic_discovery())

    # Wait forever
    await asyncio.gather(
        m365_consumer_task,
        azure_consumer_task,
        tier2_consumer_task,
        periodic_task,
    )


if __name__ == "__main__":
    asyncio.run(main())
