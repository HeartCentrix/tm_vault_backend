"""Resource Service - Manages resources and SLA policies"""
from contextlib import asynccontextmanager
from typing import Optional, Iterable, List, Dict, Any, Tuple
from uuid import UUID, uuid4
from datetime import datetime, timezone
from datetime import timedelta
import httpx

import time
from fastapi import FastAPI, Depends, HTTPException, Query, Header
from fastapi.responses import JSONResponse
from sqlalchemy import select, func, or_, text

from shared.config import settings
from shared.database import get_db, init_db, close_db, AsyncSession
from shared.models import (
    Resource, SlaPolicy, ResourceType, ResourceStatus, Tenant, TenantType,
    SlaExclusion, ResourceGroup, GroupPolicyAssignment, UI_HIDDEN_TYPES,
)
from shared.schemas import (
    ResourceResponse, ResourceListResponse, UserResourceResponse,
    AssignPolicyRequest, BulkOperationRequest,
    BulkAssignRequest, BulkUnassignRequest,
    SlaPolicyResponse, SlaPolicyCreateRequest,
    SlaExclusionRequest, SlaExclusionResponse,
    ResourceGroupRequest, ResourceGroupResponse,
    GroupPolicyAssignmentRequest,
)
from shared.message_bus import message_bus
from shared.sla_validation import (
    gate_immutability_lock as _gate_immutability_lock,
    validate_policy_payload as _validate_policy_payload,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    from shared import core_metrics
    core_metrics.init()
    await init_db()
    yield
    await close_db()


# UI_HIDDEN_TYPES now lives in shared.models so dashboard-service can apply
# the same exclusion to its Protection Status GROUP BY. See that module for
# the per-type rationale.


def format_bytes(bytes_val: int) -> str:
    if bytes_val < 1024**3:
        return f"{bytes_val / 1024**2:.1f} MB"
    return f"{bytes_val / 1024**3:.1f} GB"


async def notify_scheduler_reschedule():
    """Notify the backup Scheduler to reschedule all SLA policy jobs"""
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            await client.post("http://backup-scheduler:8008/scheduler/reschedule-all")
    except Exception as e:
        print(f"[resource-service] Failed to notify scheduler: {e}")


async def _enqueue_tier2_for_entra_users(resources: Iterable[Resource]) -> None:
    """Fire-and-forget Tier-2 discovery prep when SLA gets attached to one or
    more ENTRA_USERs.

    The bulk-backup, scheduled backup, and per-user backup paths all expect
    USER_MAIL/USER_ONEDRIVE/USER_CONTACTS/USER_CALENDAR/USER_CHATS rows to
    exist for any user with SLA. Without this hook, those rows would only be
    created the first time the operator clicked into a user's backup flow,
    leaving scheduled and bulk runs to silently skip the user's content.

    thenBackup=false: SLA assignment is a "be ready" signal, not an
    immediate backup request. The next scheduled or manual backup picks up
    the now-existing children via the normal `M365_RESOURCE_TYPES + SLA`
    query."""
    if not settings.RABBITMQ_ENABLED:
        return
    # Group by tenant — discovery-worker batches Graph calls per tenant for
    # token-cache efficiency.
    by_tenant: Dict[str, List[str]] = {}
    for r in resources:
        if r.type != ResourceType.ENTRA_USER:
            continue
        by_tenant.setdefault(str(r.tenant_id), []).append(str(r.id))
    for tid, user_ids in by_tenant.items():
        try:
            await message_bus.publish(
                "discovery.tier2",
                {
                    "tenantId": tid,
                    "userResourceIds": user_ids,
                    "source": "SLA_ASSIGNED",
                    "thenBackup": False,
                },
                priority=5,
            )
            print(
                f"[resource-service] Tier-2 prep enqueued for {len(user_ids)} user(s) "
                f"under tenant {tid}",
            )
        except Exception as e:
            # Best-effort. Backstop sweep + bulk-trigger fallback both cover
            # the gap if this publish fails.
            print(f"[resource-service] Tier-2 prep publish failed (non-fatal): {e}")


async def notify_lifecycle_reconcile():
    """Trigger an immediate lifecycle / immutability / legal-hold reconciliation
    on the scheduler, so policy changes apply right away instead of waiting
    for the daily 24h cron tick. Best-effort — failure logged, not raised.
    """
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            await client.post("http://backup-scheduler:8008/scheduler/reconcile-lifecycle")
    except Exception as e:
        print(f"[resource-service] Lifecycle reconcile notification failed: {e}")


# In-process idempotency cache for POST /api/v1/policies. Maps the
# Idempotency-Key header → (created_at_unix, response_body_dict). 24-hour
# TTL. In-memory is acceptable here because the cache is purely a
# duplicate-suppression hint — losing it on restart just means a retried
# POST may create a fresh policy, which the client/UI can detect and
# reconcile (e.g. by listing). For multi-pod coverage move to Redis.
_IDEMPOTENCY_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_IDEMPOTENCY_TTL_S = 24 * 3600


def _idempotency_get(key: Optional[str]) -> Optional[Dict[str, Any]]:
    if not key:
        return None
    entry = _IDEMPOTENCY_CACHE.get(key)
    if not entry:
        return None
    ts, body = entry
    if (time.time() - ts) > _IDEMPOTENCY_TTL_S:
        _IDEMPOTENCY_CACHE.pop(key, None)
        return None
    return body


def _idempotency_put(key: Optional[str], body: Dict[str, Any]) -> None:
    if not key:
        return
    # Cap the cache so a malicious client can't OOM us.
    if len(_IDEMPOTENCY_CACHE) > 5000:
        # Drop the oldest 1000.
        oldest = sorted(_IDEMPOTENCY_CACHE.items(), key=lambda kv: kv[1][0])[:1000]
        for k, _ in oldest:
            _IDEMPOTENCY_CACHE.pop(k, None)
    _IDEMPOTENCY_CACHE[key] = (time.time(), body)


async def _enforce_default_singleton(db: AsyncSession, tenant_id: UUID, keep_id: UUID) -> None:
    """If a policy is being saved with is_default=True, flip every other
    policy in the same tenant to is_default=False. Paired with the partial
    unique index `ix_sla_policies_one_default_per_tenant` on (tenant_id)
    WHERE is_default=TRUE. Caller is responsible for the surrounding
    transaction commit."""
    from sqlalchemy import update
    await db.execute(
        update(SlaPolicy)
        .where(SlaPolicy.tenant_id == tenant_id)
        .where(SlaPolicy.id != keep_id)
        .where(SlaPolicy.is_default == True)  # noqa: E712 — SQLA needs literal
        .values(is_default=False)
    )


async def _guard_zero_default(
    db: AsyncSession, tenant_id: UUID, this_policy_id: UUID,
    request_is_default: Optional[bool],
    prior_is_default: bool,
) -> None:
    """Refuse a save that would leave the tenant with zero default policies.

    The retention-cleanup fallback selects the tenant's default policy
    when a resource has no explicit assignment (`retention_cleanup.py:159`).
    A tenant with zero defaults silently picks the "first enabled policy"
    which is non-deterministic across deploys and migrations.

    Allowed:
      - Setting is_default=true on a policy (singleton sweep handles others).
      - Leaving is_default unchanged.
      - Clearing is_default if some OTHER enabled policy is also default.
      - Clearing is_default if there's exactly one policy in the tenant
        (operator deleting the last default explicitly is fine — they
        have nothing to back up to default-against anyway).
    Refused:
      - Clearing is_default if it would leave 0 defaults AND there are
        other enabled policies present (operator must designate one).
    """
    if request_is_default is None or request_is_default is True:
        return
    if not prior_is_default:
        return  # already non-default; no-op clear

    # Count remaining defaults excluding this policy + the count of
    # enabled policies in the tenant.
    remaining_defaults = (await db.execute(
        select(func.count(SlaPolicy.id)).where(
            SlaPolicy.tenant_id == tenant_id,
            SlaPolicy.id != this_policy_id,
            SlaPolicy.is_default == True,  # noqa: E712
        )
    )).scalar() or 0
    if remaining_defaults > 0:
        return

    # Are there other enabled policies that COULD be made default?
    other_enabled = (await db.execute(
        select(func.count(SlaPolicy.id)).where(
            SlaPolicy.tenant_id == tenant_id,
            SlaPolicy.id != this_policy_id,
            SlaPolicy.enabled == True,  # noqa: E712
        )
    )).scalar() or 0
    if other_enabled == 0:
        # Last policy in the tenant; clearing default is fine — there's
        # nothing for retention_cleanup to fall back to anyway.
        return

    raise HTTPException(
        status_code=409,
        detail=(
            "Cannot clear isDefault on the only default policy while other "
            "enabled policies exist. Designate another policy as default "
            "first, then revisit this one."
        ),
    )


async def _apply_policy_to_matching(db: AsyncSession, policy: "SlaPolicy") -> int:
    """Retroactive auto-apply for a single policy.

    Scale strategy (25k+ resources, single tenant):
      1. Stream resources via server-side cursor (yield_per) so peak
         memory stays at chunk size, never the whole resource set.
      2. Match in-memory chunk-by-chunk against the policy's attached
         groups. Static groups are skipped (explicit lists handled
         elsewhere).
      3. Buffer matched ids in chunks of 1000, fire bulk UPDATE per
         chunk — keeps a single statement's parameter count under
         the SQL driver's safe limit and lets the DB reuse the index
         lookup plan instead of one big WHERE id IN (25k uuids).

    No-op when auto_apply_to_matching=False or no enabled groups attached.
    """
    if not policy.auto_apply_to_matching:
        return 0
    from shared.resource_group_matcher import resource_matches_group
    from sqlalchemy import update as sa_update

    rows = (await db.execute(
        select(ResourceGroup)
        .join(GroupPolicyAssignment, GroupPolicyAssignment.group_id == ResourceGroup.id)
        .where(GroupPolicyAssignment.policy_id == policy.id)
        .where(ResourceGroup.tenant_id == policy.tenant_id)
        .where(ResourceGroup.enabled == True)  # noqa: E712
    )).scalars().all()
    # Skip static groups up-front — they have no rules to evaluate.
    dynamic_groups = [g for g in rows if (g.group_type or "DYNAMIC").upper() != "STATIC"]
    if not dynamic_groups:
        return 0

    CHUNK = 1000
    matched_buffer: List[UUID] = []
    total_bound = 0

    # SQLAlchemy 2.x async-streaming pattern: execution_options(yield_per=N)
    # keeps the driver in chunked-fetch mode so we don't load all rows.
    stream = await db.stream(
        select(Resource)
        .where(Resource.tenant_id == policy.tenant_id)
        .execution_options(yield_per=CHUNK)
    )
    async for res in stream.scalars():
        for g in dynamic_groups:
            if resource_matches_group(res, g.rules or [], g.combinator or "AND"):
                matched_buffer.append(res.id)
                break
        if len(matched_buffer) >= CHUNK:
            await db.execute(
                sa_update(Resource)
                .where(Resource.id.in_(matched_buffer))
                .values(sla_policy_id=policy.id)
            )
            total_bound += len(matched_buffer)
            matched_buffer.clear()

    if matched_buffer:
        await db.execute(
            sa_update(Resource)
            .where(Resource.id.in_(matched_buffer))
            .values(sla_policy_id=policy.id)
        )
        total_bound += len(matched_buffer)

    return total_bound


async def _auto_bind_new_resource(db: AsyncSession, resource: "Resource") -> Optional[UUID]:
    """Future auto-bind: called by discovery (or any caller that creates a
    Resource) before commit. Looks up every auto_apply_to_matching policy
    in the tenant whose attached resource groups match this resource;
    binds to the highest-priority match. Returns the bound policy id or
    None if no policy matched. Caller commits."""
    from shared.resource_group_matcher import find_matching_groups

    # All auto-apply policies for this tenant + their attached groups in
    # one query, indexed by policy id.
    rows = (await db.execute(
        select(SlaPolicy, ResourceGroup)
        .join(GroupPolicyAssignment, GroupPolicyAssignment.policy_id == SlaPolicy.id)
        .join(ResourceGroup, ResourceGroup.id == GroupPolicyAssignment.group_id)
        .where(SlaPolicy.tenant_id == resource.tenant_id)
        .where(SlaPolicy.auto_apply_to_matching == True)  # noqa: E712
        .where(SlaPolicy.enabled == True)  # noqa: E712
        .where(ResourceGroup.enabled == True)  # noqa: E712
    )).all()
    if not rows:
        return None
    policies_to_groups: Dict[UUID, List[ResourceGroup]] = {}
    for pol, grp in rows:
        policies_to_groups.setdefault(pol.id, []).append(grp)

    # Pick highest-priority matching group and use its policy.
    best_policy: Optional[UUID] = None
    best_priority: int = 10**9
    for pid, groups in policies_to_groups.items():
        matched = find_matching_groups(resource, groups)
        if matched:
            prio = getattr(matched[0], "priority", 100)
            if prio < best_priority:
                best_priority = prio
                best_policy = pid
    if best_policy is None:
        return None
    resource.sla_policy_id = best_policy
    return best_policy


app = FastAPI(title="Resource Service", version="1.0.0", lifespan=lifespan)


USER_LINKED_TYPES = {
    ResourceType.ENTRA_USER,
    ResourceType.MAILBOX,
    ResourceType.SHARED_MAILBOX,
    ResourceType.ROOM_MAILBOX,
    ResourceType.ONEDRIVE,
    ResourceType.TODO,
    ResourceType.ONENOTE,
}

GROUP_LINKED_TYPES = {
    ResourceType.ENTRA_GROUP,
    ResourceType.DYNAMIC_GROUP,
    ResourceType.PLANNER,
    ResourceType.TEAMS_CHANNEL,
}

VALID_POLICY_SERVICE_TYPES = {"m365", "azure"}
AZURE_POLICY_RESOURCE_TYPES = {
    ResourceType.AZURE_VM,
    ResourceType.AZURE_SQL_DB,
    ResourceType.AZURE_POSTGRESQL,
    ResourceType.AZURE_POSTGRESQL_SINGLE,
    ResourceType.RESOURCE_GROUP,
}


def normalize_policy_service_type(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    normalized = value.strip().lower()
    if normalized not in VALID_POLICY_SERVICE_TYPES:
        raise HTTPException(status_code=400, detail=f"Unsupported serviceType '{value}'")
    return normalized


def tenant_policy_service_type(tenant: Tenant) -> str:
    tenant_type = tenant.type.value if hasattr(tenant.type, "value") else str(tenant.type or "")
    return "azure" if tenant_type.upper() == TenantType.AZURE.value else "m365"


def resource_policy_service_type(resource: Resource) -> str:
    return "azure" if resource.type in AZURE_POLICY_RESOURCE_TYPES else "m365"


async def validate_policy_scope(
    db: AsyncSession,
    *,
    policy_id: UUID,
    tenant_id: UUID,
    resources: Iterable[Resource],
) -> SlaPolicy:
    policy = await db.get(SlaPolicy, policy_id)
    if not policy:
        raise HTTPException(status_code=404, detail="Policy not found")
    if policy.tenant_id != tenant_id:
        raise HTTPException(status_code=400, detail="Policy belongs to a different tenant")

    policy_service_type = normalize_policy_service_type(getattr(policy, "service_type", None)) or "m365"
    tenant = await db.get(Tenant, tenant_id)
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    resource_service_types = set()
    for resource in resources:
        if resource.tenant_id != tenant_id:
            raise HTTPException(status_code=400, detail="All resources must belong to the same tenant as the policy")
        resource_service_types.add(resource_policy_service_type(resource))

    if len(resource_service_types) > 1:
        raise HTTPException(status_code=400, detail="Resources must belong to a single service type")

    target_service_type = next(iter(resource_service_types), tenant_policy_service_type(tenant))
    if policy_service_type != target_service_type:
        raise HTTPException(
            status_code=400,
            detail=f"{policy_service_type.upper()} SLA policies can't be assigned to {target_service_type.upper()} resources",
        )

    return policy


def _resource_user_key(resource: Resource) -> Optional[str]:
    if resource.type in {
        ResourceType.ENTRA_USER,
        ResourceType.MAILBOX,
        ResourceType.SHARED_MAILBOX,
        ResourceType.ROOM_MAILBOX,
        ResourceType.TODO,
        ResourceType.ONENOTE,
    }:
        return resource.external_id
    if resource.type == ResourceType.ONEDRIVE:
        return (resource.extra_data or {}).get("user_id")
    return None


def _resource_group_key(resource: Resource) -> Optional[str]:
    if resource.type in {
        ResourceType.ENTRA_GROUP,
        ResourceType.DYNAMIC_GROUP,
        ResourceType.PLANNER,
        ResourceType.TEAMS_CHANNEL,
    }:
        return resource.external_id
    return None


async def expand_linked_policy_scope(db: AsyncSession, seed_resources: Iterable[Resource]) -> list[Resource]:
    seed_resources = list(seed_resources)
    if not seed_resources:
        return []

    tenant_ids = {resource.tenant_id for resource in seed_resources}
    candidate_types = list(USER_LINKED_TYPES | GROUP_LINKED_TYPES)
    result = await db.execute(
        select(Resource).where(
            Resource.tenant_id.in_(tenant_ids),
            Resource.type.in_(candidate_types),
        )
    )
    candidates = result.scalars().all()

    user_keys = {
        (resource.tenant_id, key)
        for resource in seed_resources
        for key in [_resource_user_key(resource)]
        if key
    }
    group_keys = {
        (resource.tenant_id, key)
        for resource in seed_resources
        for key in [_resource_group_key(resource)]
        if key
    }

    expanded: dict[UUID, Resource] = {resource.id: resource for resource in seed_resources}
    for candidate in candidates:
        candidate_user_key = _resource_user_key(candidate)
        candidate_group_key = _resource_group_key(candidate)
        if candidate_user_key and (candidate.tenant_id, candidate_user_key) in user_keys:
            expanded[candidate.id] = candidate
            continue
        if candidate_group_key and (candidate.tenant_id, candidate_group_key) in group_keys:
            expanded[candidate.id] = candidate

    return list(expanded.values())


@app.get("/health")
async def health():
    return {"status": "ok", "service": "resource"}


# ============ Resources ============

@app.get("/api/v1/resources")
async def list_resources(
    tenantId: str = Query(...),
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1),
    status: Optional[str] = Query(None),
    types: Optional[str] = Query(None),  # comma-separated resource types
    includeHidden: bool = Query(False),  # opt-in to include UI_HIDDEN_TYPES like POWER_BI
    db: AsyncSession = Depends(get_db),
):
    status_clause = "AND r.status = :rstatus" if status else ""
    type_clause = ""
    hidden_clause = ""
    if types:
        # Explicit type filter. Respect it as-is but still drop hidden unless opted in —
        # e.g. if someone passes types=MAILBOX,POWER_BI the POWER_BI rows get stripped.
        type_list = [t.strip() for t in types.split(",")]
        if not includeHidden:
            type_list = [t for t in type_list if t not in UI_HIDDEN_TYPES]
        if not type_list:
            # All requested types were hidden — empty result without hitting the DB.
            return {"items": [], "item_number": 0, "page_number": page, "next_page_token": None}
        placeholders = ", ".join([f":rt{i}" for i in range(len(type_list))])
        type_clause = f"AND r.type IN ({placeholders})"
    elif not includeHidden and UI_HIDDEN_TYPES:
        # No explicit filter — exclude hidden by default.
        hidden_placeholders = ", ".join([f":hidden{i}" for i in range(len(UI_HIDDEN_TYPES))])
        hidden_clause = f"AND r.type NOT IN ({hidden_placeholders})"

    # `last_backup_status` is derived from the latest Job rather than the
    # denormalized resources.last_backup_status column so it stays in lockstep
    # with the Activity page (which reads Job.status). The lateral join picks
    # up the most recent BACKUP job touching this resource — either as the
    # single resource_id or as a member of a batch.
    query = text(f"""
        SELECT r.id, r.tenant_id, r.type, r.external_id, r.display_name, r.email, r.metadata, r.sla_policy_id,
               r.status, r.storage_bytes, r.last_backup_at,
               COALESCE(latest_job.status::text, r.last_backup_status) AS last_backup_status,
               r.azure_region, r.azure_subscription_id, r.azure_resource_group, r.created_at
        FROM resources r
        LEFT JOIN LATERAL (
            SELECT j.status FROM jobs j
            WHERE j.type = 'BACKUP'
              AND (j.resource_id = r.id OR r.id = ANY(j.batch_resource_ids))
            ORDER BY j.created_at DESC
            LIMIT 1
        ) latest_job ON TRUE
        WHERE r.tenant_id = :rtenant {status_clause} {type_clause} {hidden_clause}
        ORDER BY r.created_at DESC
        LIMIT :rlimit OFFSET :roffset
    """)
    params = {"rtenant": tenantId, "rlimit": size, "roffset": (page - 1) * size}
    if status:
        params["rstatus"] = status
    if types:
        for i, t in enumerate(type_list):
            params[f"rt{i}"] = t
    if hidden_clause:
        for i, t in enumerate(sorted(UI_HIDDEN_TYPES)):
            params[f"hidden{i}"] = t

    result = await db.execute(query, params)
    rows = result.fetchall()

    # Count
    count_query = text(f"""
        SELECT count(*) FROM resources r WHERE r.tenant_id = :rtenant {status_clause} {type_clause} {hidden_clause}
    """)
    count_result = await db.execute(count_query, params)
    total = count_result.scalar() or 0

    # Get SLA policy names
    sla_ids = [r[7] for r in rows if r[7]]
    policies = {}
    if sla_ids:
        policy_stmt = select(SlaPolicy).where(SlaPolicy.id.in_(sla_ids))
        policy_result = await db.execute(policy_stmt)
        policies = {str(p.id): p.name for p in policy_result.scalars().all()}

    # Get backup counts for all resources in one query
    resource_ids = [r[0] for r in rows]
    backup_counts = {}
    if resource_ids:
        counts_query = text("""
            SELECT resource_id, COUNT(*) as backup_count
            FROM snapshots
            WHERE resource_id = ANY(:resource_ids)
            AND status = 'COMPLETED'
            GROUP BY resource_id
        """)
        counts_result = await db.execute(counts_query, {"resource_ids": resource_ids})
        backup_counts = {str(row[0]): row[1] for row in counts_result.fetchall()}

    # Per-resource backup size + windowed deltas, computed from
    # snapshots.bytes_added across the resource subtree (self + Tier-2
    # children linked via parent_resource_id). This mirrors the
    # /storage-summary endpoint so the Protection table and the
    # Recovery panel can never disagree.
    #
    # Why bytes_added (not storage_bytes): resources.storage_bytes is a
    # cached counter the backup worker bumps per run; the bug history
    # is full of cases where it drifted (double-counting after retries,
    # not decremented after retention purge, not zeroed after schema
    # reset). Snapshots are the source of truth — bytes_added is the
    # bytes the worker actually wrote on that run, so summing them
    # across all retained snapshots gives the true on-disk footprint.
    #
    # Also computes the real 1w / 1m / 1y deltas (previously hardcoded
    # to zero in the response).
    subtree_size_map: dict = {}
    if resource_ids:
        # Subtree includes:
        #   self (the root row)
        #   direct children — EXCEPT Tier-2 USER_ONEDRIVE/USER_MAIL when the
        #     user already has a Tier-1 ONEDRIVE/MAILBOX peer (those duplicate
        #     each other's bytes_added; Tier-1 is canonical).
        #   Tier-1 ONEDRIVE/MAILBOX peers — matched by (tenant_id, email).
        #     These are NOT structurally children of ENTRA_USER, but they hold
        #     the same user's drive/mail content, so they belong in the
        #     per-user rollup.
        subtree_size_query = text("""
            WITH targets AS (
                SELECT id AS root_id, id AS leaf_id
                FROM resources
                WHERE id = ANY(:resource_ids)
                UNION ALL
                SELECT child.parent_resource_id AS root_id, child.id AS leaf_id
                FROM resources child
                WHERE child.parent_resource_id = ANY(:resource_ids)
                  AND NOT (
                    child.type::text IN ('USER_ONEDRIVE', 'USER_MAIL')
                    AND EXISTS (
                      SELECT 1 FROM resources peer
                      WHERE peer.tenant_id = child.tenant_id
                        AND peer.email = child.email
                        AND peer.archived_at IS NULL
                        AND peer.type::text = CASE child.type::text
                                                WHEN 'USER_ONEDRIVE' THEN 'ONEDRIVE'
                                                WHEN 'USER_MAIL'     THEN 'MAILBOX'
                                              END
                    )
                  )
                UNION ALL
                SELECT parent.id AS root_id, peer.id AS leaf_id
                FROM resources parent
                JOIN resources peer
                  ON peer.tenant_id = parent.tenant_id
                 AND peer.email = parent.email
                 AND peer.archived_at IS NULL
                 AND peer.type::text IN ('ONEDRIVE', 'MAILBOX')
                WHERE parent.id = ANY(:resource_ids)
                  AND parent.type::text = 'ENTRA_USER'
            )
            SELECT
                t.root_id,
                COALESCE(SUM(s.bytes_added), 0) AS total,
                COALESCE(SUM(s.bytes_added) FILTER (
                    WHERE s.started_at >= now() - interval '7 days'
                ), 0) AS d7,
                COALESCE(SUM(s.bytes_added) FILTER (
                    WHERE s.started_at >= now() - interval '30 days'
                ), 0) AS d30,
                COALESCE(SUM(s.bytes_added) FILTER (
                    WHERE s.started_at >= now() - interval '365 days'
                ), 0) AS d365
            FROM targets t
            LEFT JOIN snapshots s
                ON s.resource_id = t.leaf_id
                AND s.status IN ('COMPLETED', 'PARTIAL', 'PENDING_DELETION')
            GROUP BY t.root_id
        """)
        subtree_size_result = await db.execute(
            subtree_size_query, {"resource_ids": resource_ids}
        )
        for row in subtree_size_result.fetchall():
            subtree_size_map[str(row[0])] = {
                "size": int(row[1] or 0),
                "size_delta_week": int(row[2] or 0),
                "size_delta_month": int(row[3] or 0),
                "size_delta_year": int(row[4] or 0),
            }

    # Likewise, treat a completed snapshot on ANY child as "the parent was
    # backed up" so the UI chip flips to "protected" for parents whose only
    # completed snapshots live on their children.
    if resource_ids:
        child_backup_query = text("""
            SELECT r.parent_resource_id, COUNT(*) AS child_backups
            FROM snapshots s
            JOIN resources r ON r.id = s.resource_id
            WHERE r.parent_resource_id = ANY(:resource_ids)
              AND s.status = 'COMPLETED'
            GROUP BY r.parent_resource_id
        """)
        child_backup_result = await db.execute(child_backup_query, {"resource_ids": resource_ids})
        for row in child_backup_result.fetchall():
            pid = str(row[0])
            backup_counts[pid] = backup_counts.get(pid, 0) + int(row[1] or 0)

    def map_kind(t):
        m = {"MAILBOX": "office_user", "SHARED_MAILBOX": "shared_mailbox", "ROOM_MAILBOX": "room_mailbox",
             "ONEDRIVE": "onedrive", "SHAREPOINT_SITE": "sharepoint_site", "TEAMS_CHANNEL": "teams_channel",
             "TEAMS_CHAT": "teams_chat", "TEAMS_CHAT_EXPORT": "teams_chat_export",
             "ENTRA_USER": "entra_user", "ENTRA_GROUP": "entra_group", "ENTRA_DIRECTORY": "entra_directory",
             "ENTRA_APP": "entra_app", "ENTRA_DEVICE": "entra_device",
             "ENTRA_SERVICE_PRINCIPAL": "entra_service_principal", "ENTRA_ROLE": "entra_role",
             "ENTRA_ADMIN_UNIT": "entra_admin_unit", "ENTRA_CONDITIONAL_ACCESS": "entra_conditional_access",
             "ENTRA_BITLOCKER_KEY": "entra_bitlocker_key", "INTUNE_MANAGED_DEVICE": "intune_managed_device",
             "M365_GROUP": "m365_group",
             "AZURE_VM": "azure_vm",
             "AZURE_SQL_DB": "azure_sql", "AZURE_POSTGRESQL": "azure_postgresql", "AZURE_POSTGRESQL_SINGLE": "azure_postgresql",
             "RESOURCE_GROUP": "resource_group", "DYNAMIC_GROUP": "dynamic_group",
             "POWER_BI": "power_bi", "POWER_APPS": "power_apps", "POWER_AUTOMATE": "power_automate",
             "POWER_DLP": "power_dlp", "COPILOT": "copilot", "PLANNER": "planner",
             "TODO": "todo", "ONENOTE": "onenote"}
        return m.get(t, t.lower() if t else "unknown")

    def map_status(s):
        return {"ACTIVE": "protected", "ARCHIVED": "archived", "SUSPENDED": "suspended"}.get(s, "discovered")

    def format_backup_size(bytes_val: int) -> str:
        """Format bytes to human-readable size string"""
        if not bytes_val or bytes_val == 0:
            return "0 B"
        if bytes_val >= 1099511627776:
            return f"{bytes_val / 1099511627776:.2f} TB"
        if bytes_val >= 1073741824:
            return f"{bytes_val / 1073741824:.2f} GB"
        if bytes_val >= 1048576:
            return f"{bytes_val / 1048576:.2f} MB"
        if bytes_val >= 1024:
            return f"{bytes_val / 1024:.2f} KB"
        return f"{bytes_val} B"

    items = []
    for r in rows:
        rid = str(r[0])
        sub = subtree_size_map.get(rid, {})
        storage_bytes = sub.get("size", 0)
        size_delta_week = sub.get("size_delta_week", 0)
        size_delta_month = sub.get("size_delta_month", 0)
        size_delta_year = sub.get("size_delta_year", 0)
        has_backup = r[10] is not None  # last_backup_at is not None
        backup_count = backup_counts.get(rid, 0)
        items.append({
            "id": str(r[0]), "tenant_id": str(r[1]), "owner": None,
            "kind": map_kind(r[2]),
            "provider": "azure" if r[2] and "AZURE" in r[2] else "o365",
            "external_id": r[3], "name": r[4], "email": r[5],
            # Merge the Azure top-level columns into the metadata JSON
            # so the Recover modal can read subscription/RG/region
            # without a second fetch. Also backfills `location` for
            # SQL resources (discovery only stores it on PostgreSQL).
            "data": {
                **(r[6] or {}),
                **({"azure_region": r[12]} if len(r) > 12 and r[12] else {}),
                **({"azure_subscription_id": r[13]} if len(r) > 13 and r[13] else {}),
                **({"azure_resource_group": r[14]} if len(r) > 14 and r[14] else {}),
            },
            "archived": r[8] == "ARCHIVED", "deleted": r[8] == "PENDING_DELETION",
            "protections": [{"policy_id": str(r[7])}] if r[7] else None,
            "usage": {"resource_id": rid, "tenant_id": str(r[1]), "backups": backup_count,
                      "size": storage_bytes,
                      "size_delta_year": size_delta_year,
                      "size_delta_month": size_delta_month,
                      "size_delta_week": size_delta_week},
            "backupSize": format_backup_size(storage_bytes) if has_backup else None,
            "status": map_status(r[8]),
            "sla": policies.get(str(r[7])) if r[7] else None,
            "last_backup": r[10].isoformat() if r[10] else None,
            "last_backup_status": r[11] if r[11] else None,
            "group_ids": [],
        })

    has_next = (page * size) < total
    return {"item_number": total, "page_number": page, "next_page_token": str(page + 1) if has_next else None, "items": items}


@app.get("/api/v1/resources/search")
async def search_resources(
    query: str = Query(...),
    type: Optional[str] = Query(None),
    includeHidden: bool = Query(False),
    db: AsyncSession = Depends(get_db),
):
    filters = [or_(Resource.display_name.ilike(f"%{query}%"), Resource.email.ilike(f"%{query}%"))]
    if type:
        if not includeHidden and type in UI_HIDDEN_TYPES:
            return []
        filters.append(Resource.type == type)
    elif not includeHidden and UI_HIDDEN_TYPES:
        filters.append(Resource.type.notin_(list(UI_HIDDEN_TYPES)))
    stmt = select(Resource).where(*filters).limit(50)
    result = await db.execute(stmt)
    return [
        ResourceResponse(id=str(r.id), name=r.display_name, email=r.email,
                        type=r.type.value if hasattr(r.type, 'value') else str(r.type),
                        totalSize=format_bytes(r.storage_bytes or 0))
        for r in result.scalars().all()
    ]


@app.get("/api/v1/resources/by-type")
async def get_resources_by_type(
    type: str = Query(...),
    tenantId: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1),
    includeHidden: bool = Query(False),
    db: AsyncSession = Depends(get_db),
):
    # Hidden types return an empty page unless the caller explicitly opts in.
    if not includeHidden and type in UI_HIDDEN_TYPES:
        return {"items": [], "item_number": 0, "page_number": page, "next_page_token": None}

    # Validate tenant exists when a tenantId filter is supplied — a stale
    # UUID from a localStorage cache (e.g. after a DB reset) previously
    # returned a silent 200 with empty items, which the UI rendered as
    # "nothing discovered yet" even though 38 ENTRA_USER rows were in
    # the DB under a different tenant id. Returning 404 gives the
    # frontend a clear signal to clear its cache + re-fetch tenants.
    if tenantId:
        try:
            tenant_uuid = UUID(tenantId)
        except (ValueError, TypeError):
            raise HTTPException(
                status_code=400, detail=f"Invalid tenantId: {tenantId}",
            )
        exists = (await db.execute(
            text("SELECT 1 FROM tenants WHERE id = :tid LIMIT 1"),
            {"tid": tenant_uuid},
        )).first()
        if not exists:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"Tenant {tenantId} not found — "
                    "cached id may be stale; refresh tenants"
                ),
            )

    # SharePoint sub-section filter: exclude sites whose name+email
    # collides with a Microsoft 365 group / Entra group / Teams channel.
    # Those appear under the Groups & Teams tab instead, so showing them
    # here duplicates the row. Match is case-insensitive on display_name
    # AND identical on email (NULL treated equal).
    sp_exclude_clause = ""
    if type == "SHAREPOINT_SITE":
        # A SP site backed by an M365 group / Entra group / Teams channel
        # shares the group's display name (the admin API surfaces the
        # group's name as the site title). When both the SP site and the
        # group also have a mail address, require it to match; when the
        # SP site has no email (the common case), match on name alone.
        sp_exclude_clause = """AND NOT EXISTS (
            SELECT 1 FROM resources g
            WHERE g.tenant_id = r.tenant_id
              AND g.type IN ('M365_GROUP', 'ENTRA_GROUP', 'TEAMS_CHANNEL')
              AND LOWER(g.display_name) = LOWER(r.display_name)
              AND (
                    COALESCE(r.email, '') = ''
                 OR LOWER(COALESCE(g.email, '')) = LOWER(COALESCE(r.email, ''))
              )
        )"""

    # Same lateral join as the main list — derive last_backup_status from
    # the latest BACKUP job touching this resource so Protection mirrors
    # Activity.
    query = text(f"""
        SELECT r.id, r.tenant_id, r.type, r.external_id, r.display_name, r.email, r.metadata, r.sla_policy_id,
               r.status, r.storage_bytes, r.last_backup_at,
               COALESCE(latest_job.status::text, r.last_backup_status) AS last_backup_status,
               r.azure_region, r.azure_subscription_id, r.azure_resource_group, r.created_at
        FROM resources r
        LEFT JOIN LATERAL (
            SELECT j.status FROM jobs j
            WHERE j.type = 'BACKUP'
              AND (j.resource_id = r.id OR r.id = ANY(j.batch_resource_ids))
            ORDER BY j.created_at DESC
            LIMIT 1
        ) latest_job ON TRUE
        WHERE r.type = :rtype
        {'AND r.tenant_id = :rtenant' if tenantId else ''}
        {sp_exclude_clause}
        ORDER BY r.created_at DESC
        LIMIT :rlimit OFFSET :roffset
    """)
    params = {"rtype": type, "rlimit": size, "roffset": (page - 1) * size}
    if tenantId:
        params["rtenant"] = tenantId

    result = await db.execute(query, params)
    rows = result.fetchall()

    count_query = text(f"""
        SELECT count(*) FROM resources r WHERE r.type = :rtype
        {'AND r.tenant_id = :rtenant' if tenantId else ''}
        {sp_exclude_clause}
    """)
    count_result = await db.execute(count_query, params)
    total = count_result.scalar() or 0

    def map_kind(t):
        m = {"MAILBOX": "office_user", "SHARED_MAILBOX": "shared_mailbox", "ROOM_MAILBOX": "room_mailbox",
             "ONEDRIVE": "onedrive", "SHAREPOINT_SITE": "sharepoint_site", "TEAMS_CHANNEL": "teams_channel",
             "TEAMS_CHAT": "teams_chat", "TEAMS_CHAT_EXPORT": "teams_chat_export",
             "ENTRA_USER": "entra_user", "ENTRA_GROUP": "entra_group", "ENTRA_DIRECTORY": "entra_directory",
             "ENTRA_APP": "entra_app", "ENTRA_DEVICE": "entra_device",
             "ENTRA_SERVICE_PRINCIPAL": "entra_service_principal", "ENTRA_ROLE": "entra_role",
             "ENTRA_ADMIN_UNIT": "entra_admin_unit", "ENTRA_CONDITIONAL_ACCESS": "entra_conditional_access",
             "ENTRA_BITLOCKER_KEY": "entra_bitlocker_key", "INTUNE_MANAGED_DEVICE": "intune_managed_device",
             "M365_GROUP": "m365_group",
             "AZURE_VM": "azure_vm",
             "AZURE_SQL_DB": "azure_sql", "AZURE_POSTGRESQL": "azure_postgresql", "AZURE_POSTGRESQL_SINGLE": "azure_postgresql",
             "RESOURCE_GROUP": "resource_group", "DYNAMIC_GROUP": "dynamic_group",
             "POWER_BI": "power_bi", "POWER_APPS": "power_apps", "POWER_AUTOMATE": "power_automate",
             "POWER_DLP": "power_dlp", "COPILOT": "copilot", "PLANNER": "planner",
             "TODO": "todo", "ONENOTE": "onenote"}
        return m.get(t, t.lower() if t else "unknown")

    def map_status(s):
        return {"ACTIVE": "protected", "ARCHIVED": "archived", "SUSPENDED": "suspended"}.get(s, "discovered")

    def format_backup_size(bytes_val: int) -> str:
        """Format bytes to human-readable size string"""
        if not bytes_val or bytes_val == 0:
            return "0 B"
        if bytes_val >= 1099511627776:
            return f"{bytes_val / 1099511627776:.2f} TB"
        if bytes_val >= 1073741824:
            return f"{bytes_val / 1073741824:.2f} GB"
        if bytes_val >= 1048576:
            return f"{bytes_val / 1048576:.2f} MB"
        if bytes_val >= 1024:
            return f"{bytes_val / 1024:.2f} KB"
        return f"{bytes_val} B"

    # Get SLA policy names
    sla_ids = [row[7] for row in rows if row[7]]
    policies = {}
    if sla_ids:
        policy_stmt = select(SlaPolicy).where(SlaPolicy.id.in_(sla_ids))
        policy_result = await db.execute(policy_stmt)
        policies = {str(p.id): p.name for p in policy_result.scalars().all()}

    # Get backup counts for all resources in one query
    resource_ids = [row[0] for row in rows]
    backup_counts = {}
    if resource_ids:
        counts_query = text("""
            SELECT resource_id, COUNT(*) as backup_count
            FROM snapshots
            WHERE resource_id = ANY(:resource_ids)
            AND status = 'COMPLETED'
            GROUP BY resource_id
        """)
        counts_result = await db.execute(counts_query, {"resource_ids": resource_ids})
        backup_counts = {str(r[0]): r[1] for r in counts_result.fetchall()}

    # Subtree-aware size + window deltas (mirrors /resources path and the
    # /storage-summary endpoint — single source of truth so the Protection
    # table, the Recovery panel, and any future surfaces all agree).
    subtree_size_map: dict = {}
    if resource_ids:
        # See subtree CTE comment in get_resources for the dedup logic.
        subtree_size_query = text("""
            WITH targets AS (
                SELECT id AS root_id, id AS leaf_id
                FROM resources
                WHERE id = ANY(:resource_ids)
                UNION ALL
                SELECT child.parent_resource_id AS root_id, child.id AS leaf_id
                FROM resources child
                WHERE child.parent_resource_id = ANY(:resource_ids)
                  AND NOT (
                    child.type::text IN ('USER_ONEDRIVE', 'USER_MAIL')
                    AND EXISTS (
                      SELECT 1 FROM resources peer
                      WHERE peer.tenant_id = child.tenant_id
                        AND peer.email = child.email
                        AND peer.archived_at IS NULL
                        AND peer.type::text = CASE child.type::text
                                                WHEN 'USER_ONEDRIVE' THEN 'ONEDRIVE'
                                                WHEN 'USER_MAIL'     THEN 'MAILBOX'
                                              END
                    )
                  )
                UNION ALL
                SELECT parent.id AS root_id, peer.id AS leaf_id
                FROM resources parent
                JOIN resources peer
                  ON peer.tenant_id = parent.tenant_id
                 AND peer.email = parent.email
                 AND peer.archived_at IS NULL
                 AND peer.type::text IN ('ONEDRIVE', 'MAILBOX')
                WHERE parent.id = ANY(:resource_ids)
                  AND parent.type::text = 'ENTRA_USER'
            )
            SELECT
                t.root_id,
                COALESCE(SUM(s.bytes_added), 0) AS total,
                COALESCE(SUM(s.bytes_added) FILTER (
                    WHERE s.started_at >= now() - interval '7 days'
                ), 0) AS d7,
                COALESCE(SUM(s.bytes_added) FILTER (
                    WHERE s.started_at >= now() - interval '30 days'
                ), 0) AS d30,
                COALESCE(SUM(s.bytes_added) FILTER (
                    WHERE s.started_at >= now() - interval '365 days'
                ), 0) AS d365
            FROM targets t
            LEFT JOIN snapshots s
                ON s.resource_id = t.leaf_id
                AND s.status IN ('COMPLETED', 'PARTIAL', 'PENDING_DELETION')
            GROUP BY t.root_id
        """)
        subtree_size_result = await db.execute(
            subtree_size_query, {"resource_ids": resource_ids}
        )
        for row in subtree_size_result.fetchall():
            subtree_size_map[str(row[0])] = {
                "size": int(row[1] or 0),
                "size_delta_week": int(row[2] or 0),
                "size_delta_month": int(row[3] or 0),
                "size_delta_year": int(row[4] or 0),
            }

    if resource_ids:
        child_backup_query = text("""
            SELECT r.parent_resource_id, COUNT(*) AS child_backups
            FROM snapshots s
            JOIN resources r ON r.id = s.resource_id
            WHERE r.parent_resource_id = ANY(:resource_ids)
              AND s.status = 'COMPLETED'
            GROUP BY r.parent_resource_id
        """)
        child_backup_result = await db.execute(child_backup_query, {"resource_ids": resource_ids})
        for r in child_backup_result.fetchall():
            pid = str(r[0])
            backup_counts[pid] = backup_counts.get(pid, 0) + int(r[1] or 0)

    # Also bring in the latest child snapshot timestamp so a parent with
    # no direct snapshots still flips to "protected" / shows a last-backup
    # time when one of its children has been backed up.
    child_last_backup_map: dict = {}
    if resource_ids:
        child_last_query = text("""
            SELECT r.parent_resource_id, MAX(s.created_at) AS last_backup
            FROM snapshots s
            JOIN resources r ON r.id = s.resource_id
            WHERE r.parent_resource_id = ANY(:resource_ids)
              AND s.status = 'COMPLETED'
            GROUP BY r.parent_resource_id
        """)
        child_last_result = await db.execute(child_last_query, {"resource_ids": resource_ids})
        child_last_backup_map = {str(r[0]): r[1] for r in child_last_result.fetchall() if r[1] is not None}

    items = []
    for row in rows:
        rid = str(row[0])
        sub = subtree_size_map.get(rid, {})
        storage_bytes = sub.get("size", 0)
        size_delta_week = sub.get("size_delta_week", 0)
        size_delta_month = sub.get("size_delta_month", 0)
        size_delta_year = sub.get("size_delta_year", 0)
        # has_backup = parent has own last_backup_at OR any child does
        last_backup_dt = row[10] or child_last_backup_map.get(rid)
        has_backup = last_backup_dt is not None
        backup_count = backup_counts.get(rid, 0)
        items.append({
            "id": rid, "tenant_id": str(row[1]), "owner": None,
            "kind": map_kind(row[2]),
            "provider": "azure" if "AZURE" in (row[2] or "") else "o365",
            "external_id": row[3], "name": row[4], "email": row[5],
            "data": row[6] or {},
            "archived": row[8] == "ARCHIVED", "deleted": row[8] == "PENDING_DELETION",
            "protections": [{"policy_id": str(row[7])}] if row[7] else None,
            "usage": {"resource_id": rid, "tenant_id": str(row[1]), "backups": backup_count,
                      "size": storage_bytes,
                      "size_delta_year": size_delta_year,
                      "size_delta_month": size_delta_month,
                      "size_delta_week": size_delta_week},
            "backupSize": format_backup_size(storage_bytes) if has_backup else None,
            "status": map_status(row[8]),
            "sla": policies.get(str(row[7])) if row[7] else None,
            "last_backup": last_backup_dt.isoformat() if last_backup_dt else None,
            "last_backup_status": row[11] if row[11] else None,
            "group_ids": [],
        })

    has_next = (page * size) < total
    return {"item_number": total, "page_number": page, "next_page_token": str(page + 1) if has_next else None, "items": items}


@app.get("/api/v1/resources/users")
async def get_users_with_workloads(tenantId: str = Query(...), db: AsyncSession = Depends(get_db)):
    stmt = select(Resource).where(
        Resource.tenant_id == UUID(tenantId),
        Resource.type.in_([ResourceType.MAILBOX, ResourceType.SHARED_MAILBOX, ResourceType.ONEDRIVE, ResourceType.TEAMS_CHAT])
    )
    result = await db.execute(stmt)
    resources = result.scalars().all()

    users_map = {}
    for r in resources:
        email = r.email or f"unknown-{r.id}"
        if email not in users_map:
            users_map[email] = {"id": str(r.id), "tenantId": str(r.tenant_id), "email": email, "displayName": r.display_name, "resources": []}
        users_map[email]["resources"].append(r)

    return [
        UserResourceResponse(
            id=v["id"], tenantId=v["tenantId"], email=v["email"], displayName=v["displayName"],
            hasMailbox=any("MAILBOX" in (r.type.value if hasattr(r.type, 'value') else str(r.type)) for r in v["resources"]),
            hasOneDrive=any("ONEDRIVE" in (r.type.value if hasattr(r.type, 'value') else str(r.type)) for r in v["resources"]),
            hasTeamsChat=any("TEAMS" in (r.type.value if hasattr(r.type, 'value') else str(r.type)) for r in v["resources"]),
        )
        for v in users_map.values()
    ]


@app.get("/api/v1/resources/{resource_id}", response_model=ResourceResponse)
async def get_resource(resource_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(Resource).where(Resource.id == UUID(resource_id))
    result = await db.execute(stmt)
    resource = result.scalar_one_or_none()
    if not resource:
        raise HTTPException(status_code=404, detail="Resource not found")
    return ResourceResponse(
        id=str(resource.id), name=resource.display_name, email=resource.email,
        type=resource.type.value if hasattr(resource.type, 'value') else str(resource.type),
        totalSize=format_bytes(resource.storage_bytes or 0),
        status=resource.status.value if hasattr(resource.status, 'value') else str(resource.status),
        tenantId=str(resource.tenant_id),
    )


@app.get("/api/v1/resources/{resource_id}/storage-history")
async def get_storage_history(resource_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(Resource).where(Resource.id == UUID(resource_id))
    result = await db.execute(stmt)
    resource = result.scalar_one_or_none()
    if not resource:
        raise HTTPException(status_code=404, detail="Resource not found")
    current_size = resource.storage_bytes or 0
    return [{"date": (datetime.now(timezone.utc) - timedelta(days=29-i)).date().isoformat(), "size": int(current_size * (0.5 + 0.5 * (i / 30)))} for i in range(30)]


@app.post("/api/v1/resources/{resource_id}/assign-policy", status_code=204)
async def assign_policy(resource_id: str, request: AssignPolicyRequest, db: AsyncSession = Depends(get_db)):
    stmt = select(Resource).where(Resource.id == UUID(resource_id))
    result = await db.execute(stmt)
    resource = result.scalar_one_or_none()
    if not resource:
        raise HTTPException(status_code=404, detail="Resource not found")
    await validate_policy_scope(
        db,
        policy_id=UUID(request.policyId),
        tenant_id=resource.tenant_id,
        resources=[resource],
    )
    resources = await expand_linked_policy_scope(db, [resource])
    for target in resources:
        target.sla_policy_id = UUID(request.policyId)
        target.status = ResourceStatus.ACTIVE
    await db.commit()
    await _enqueue_tier2_for_entra_users(resources)


@app.post("/api/v1/resources/{resource_id}/unassign-policy", status_code=204)
async def unassign_policy(resource_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(Resource).where(Resource.id == UUID(resource_id))
    result = await db.execute(stmt)
    resource = result.scalar_one_or_none()
    if not resource:
        raise HTTPException(status_code=404, detail="Resource not found")
    resources = await expand_linked_policy_scope(db, [resource])
    for target in resources:
        target.sla_policy_id = None
    await db.commit()


@app.post("/api/v1/resources/{resource_id}/archive", status_code=204)
async def archive_resource(resource_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(Resource).where(Resource.id == UUID(resource_id))
    result = await db.execute(stmt)
    resource = result.scalar_one_or_none()
    if not resource:
        raise HTTPException(status_code=404, detail="Resource not found")
    resource.status = ResourceStatus.ARCHIVED
    await db.commit()


@app.post("/api/v1/resources/{resource_id}/unarchive", status_code=204)
async def unarchive_resource(resource_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(Resource).where(Resource.id == UUID(resource_id))
    result = await db.execute(stmt)
    resource = result.scalar_one_or_none()
    if not resource:
        raise HTTPException(status_code=404, detail="Resource not found")
    resource.status = ResourceStatus.ACTIVE
    await db.commit()


@app.delete("/api/v1/resources/{resource_id}", status_code=204)
async def delete_resource(resource_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(Resource).where(Resource.id == UUID(resource_id))
    result = await db.execute(stmt)
    resource = result.scalar_one_or_none()
    if not resource:
        raise HTTPException(status_code=404, detail="Resource not found")
    resource.status = ResourceStatus.PENDING_DELETION
    await db.commit()


@app.post("/api/v1/resources/bulk-assign-policy", status_code=200)
async def bulk_assign_policy(request: BulkAssignRequest, db: AsyncSession = Depends(get_db)):
    """
    Assign an SLA policy to multiple resources at once.

    Body:
    {
        "resourceIds": ["uuid1", "uuid2", ...],
        "policyId": "uuid-of-policy"
    }

    Returns:
    {
        "assigned": 10,
        "not_found": ["uuid-of-missing-resource", ...]
    }
    """
    print(f"[BULK_ASSIGN] Received policyId: '{request.policyId}', resourceIds: {request.resourceIds}")

    # Validate policyId
    if not request.policyId or request.policyId.strip() == "":
        # If policyId is empty, unassign policy from resources
        resource_ids = []
        for rid in request.resourceIds:
            try:
                resource_ids.append(UUID(rid))
            except ValueError:
                print(f"[BULK_ASSIGN] Skipping invalid resource ID: {rid}")
                continue

        stmt = select(Resource).where(Resource.id.in_(resource_ids))
        result = await db.execute(stmt)
        resources = result.scalars().all()

        expanded_resources = await expand_linked_policy_scope(db, resources)

        for resource in expanded_resources:
            resource.sla_policy_id = None
        await db.commit()

        return {
            "assigned": 0,
            "unassigned": len(expanded_resources),
            "not_found": [],
        }

    try:
        policy_id = UUID(request.policyId)
    except ValueError:
        print(f"[BULK_ASSIGN] ERROR: Invalid policy ID format: '{request.policyId}'")
        raise HTTPException(status_code=400, detail=f"Invalid policy ID format: '{request.policyId}'. Must be a valid UUID.")

    resource_ids = []
    for rid in request.resourceIds:
        try:
            resource_ids.append(UUID(rid))
        except ValueError:
            print(f"[BULK_ASSIGN] Skipping invalid resource ID: {rid}")
            continue
    
    # Fetch all matching resources in one query
    stmt = select(Resource).where(Resource.id.in_(resource_ids))
    result = await db.execute(stmt)
    resources = result.scalars().all()
    found_ids = {str(r.id) for r in resources}
    not_found = [rid for rid in request.resourceIds if rid not in found_ids]
    if resources:
        tenant_ids = {resource.tenant_id for resource in resources}
        if len(tenant_ids) != 1:
            raise HTTPException(status_code=400, detail="Resources must belong to a single tenant")
        await validate_policy_scope(
            db,
            policy_id=policy_id,
            tenant_id=next(iter(tenant_ids)),
            resources=resources,
        )
    
    # Bulk update
    expanded_resources = await expand_linked_policy_scope(db, resources)

    updated_count = 0
    for resource in expanded_resources:
        resource.sla_policy_id = policy_id
        resource.status = ResourceStatus.ACTIVE
        updated_count += 1

    await db.commit()
    await _enqueue_tier2_for_entra_users(expanded_resources)

    return {
        "assigned": updated_count,
        "not_found": not_found,
    }


@app.post("/api/v1/resources/bulk-unassign-policy", status_code=200)
async def bulk_unassign_policy(request: BulkUnassignRequest, db: AsyncSession = Depends(get_db)):
    """
    Remove SLA policy from multiple resources at once.
    
    Body:
    {
        "resourceIds": ["uuid1", "uuid2", ...]
    }
    
    Returns:
    {
        "unassigned": 10,
        "not_found": ["uuid-of-missing-resource", ...]
    }
    """
    resource_ids = [UUID(rid) for rid in request.resourceIds]
    
    stmt = select(Resource).where(Resource.id.in_(resource_ids))
    result = await db.execute(stmt)
    resources = result.scalars().all()
    found_ids = {str(r.id) for r in resources}
    not_found = [rid for rid in request.resourceIds if rid not in found_ids]
    
    expanded_resources = await expand_linked_policy_scope(db, resources)

    for resource in expanded_resources:
        resource.sla_policy_id = None
    
    await db.commit()
    
    return {
        "unassigned": len(expanded_resources),
        "not_found": not_found,
    }


# ============ SLA Policies ============

def build_schedule(policy):
    """Build schedule object from policy fields"""
    hours = []
    if policy.frequency == "THREE_DAILY":
        hours = [4, 12, 20]
        sched_type = "hourly"
    else:
        # Parse backup_window_start like "21:00" -> [21]
        if policy.backup_window_start:
            try:
                hours = [int(policy.backup_window_start.split(":")[0])]
            except:
                hours = [21]
        else:
            hours = [21]
        sched_type = "daily"
    
    return {
        "type": sched_type,
        "hours": hours,
        "timezone": "Asia/Calcutta",
        "week_days": [0, 1, 2, 3, 4, 5, 6],
        "jitter_sec": 21600,
    }


def policy_to_dict(p):
    """Convert policy to API response format"""
    result = SlaPolicyResponse.model_validate(p).model_dump()
    result["serviceType"] = (result.get("serviceType") or "m365").lower()
    print(f"[POLICY] Converted policy: id={result.get('id')}, name={result.get('name')}")
    return result


@app.get("/api/v1/policies")
async def list_policies(
    tenantId: Optional[str] = Query(None),
    serviceType: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    """List SLA policies. Paginated to keep list-page response sizes
    bounded — at 5k-user / single-tenant scale operators may have
    hundreds of policies; an unpaginated dump can be MB-sized JSON.

    Response shape:
      {"items": [...], "total": N, "limit": L, "offset": O}

    Back-compat: callers that pass no pagination params still get the
    first 100 rows. Wizard page should drive pagination via the items
    list and the `total` for "X of Y policies" footers.
    """
    base_filter = []
    if tenantId:
        base_filter.append(SlaPolicy.tenant_id == UUID(tenantId))
    normalized_service_type = normalize_policy_service_type(serviceType)
    if normalized_service_type:
        base_filter.append(SlaPolicy.service_type == normalized_service_type)

    # Total — single COUNT(*) so pagination doesn't pay the full row cost.
    count_stmt = select(func.count(SlaPolicy.id))
    for f in base_filter:
        count_stmt = count_stmt.where(f)
    total = (await db.execute(count_stmt)).scalar() or 0

    page_stmt = select(SlaPolicy).order_by(SlaPolicy.created_at.desc())
    for f in base_filter:
        page_stmt = page_stmt.where(f)
    page_stmt = page_stmt.offset(offset).limit(limit)

    rows = (await db.execute(page_stmt)).scalars().all()
    items = [policy_to_dict(p) for p in rows]
    return {"items": items, "total": total, "limit": limit, "offset": offset}


@app.get("/api/v1/policies/{policy_id}", response_model=SlaPolicyResponse)
async def get_policy(policy_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(SlaPolicy).where(SlaPolicy.id == UUID(policy_id))
    result = await db.execute(stmt)
    policy = result.scalar_one_or_none()
    if not policy:
        raise HTTPException(status_code=404, detail="Policy not found")
    return SlaPolicyResponse.model_validate(policy)


@app.post("/api/v1/policies")
async def create_policy(
    request: dict,
    db: AsyncSession = Depends(get_db),
    idempotency_key: Optional[str] = Header(default=None, alias="Idempotency-Key"),
):
    # Idempotent retries (operator double-clicks Save, network flake) MUST
    # NOT create duplicate policies. The client supplies an opaque
    # Idempotency-Key on the first POST; subsequent POSTs with the same
    # key inside the 24h window return the cached response without ever
    # touching the DB. RFC standard pattern (Stripe / GitHub / AWS).
    cached = _idempotency_get(idempotency_key)
    if cached is not None:
        return cached

    # Helper to get value by camelCase or snake_case
    def get_val(camel: str, snake: str, default=None):
        return request.get(camel, request.get(snake, default))

    tenant_id = UUID(get_val("tenantId", "tenant_id"))
    tenant = await db.get(Tenant, tenant_id)
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    service_type = normalize_policy_service_type(get_val("serviceType", "service_type")) or tenant_policy_service_type(tenant)

    # Validate-then-gate. Validation comes first because the WORM gate
    # references the typed name, which we want resolved against a clean
    # payload (e.g. trimmed whitespace).
    _validate_policy_payload(request)
    # On create, the policy name is the typed name (there's no prior name).
    _gate_immutability_lock(
        request,
        prior_mode=None,
        current_name=(get_val("name", "name") or "").strip(),
    )

    policy = SlaPolicy(
        id=uuid4(),
        tenant_id=tenant_id,
        service_type=service_type,
        name=get_val("name", "name", "New Policy"),
        frequency=get_val("frequency", "frequency", "DAILY"),
        backup_days=get_val("backupDays", "backup_days", ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]),
        backup_window_start=get_val("backupWindowStart", "backup_window_start", "21:00"),
        backup_exchange=get_val("backupExchange", "backup_exchange", service_type == "m365"),
        backup_exchange_archive=get_val("backupExchangeArchive", "backup_exchange_archive", False),
        backup_exchange_recoverable=get_val("backupExchangeRecoverable", "backup_exchange_recoverable", False),
        backup_onedrive=get_val("backupOneDrive", "backup_onedrive", service_type == "m365"),
        backup_sharepoint=get_val("backupSharepoint", "backup_sharepoint", service_type == "m365"),
        backup_teams=get_val("backupTeams", "backup_teams", service_type == "m365"),
        backup_teams_chats=get_val("backupTeamsChats", "backup_teams_chats", False),
        backup_entra_id=get_val("backupEntraId", "backup_entra_id", service_type == "m365"),
        backup_power_platform=get_val("backupPowerPlatform", "backup_power_platform", False),
        backup_copilot=get_val("backupCopilot", "backup_copilot", False),
        contacts=get_val("contacts", "contacts", service_type == "m365"),
        calendars=get_val("calendars", "calendars", service_type == "m365"),
        tasks=get_val("tasks", "tasks", False),
        group_mailbox=get_val("groupMailbox", "group_mailbox", service_type == "m365"),
        planner=get_val("planner", "planner", False),
        backup_azure_vm=get_val("backupAzureVm", "backup_azure_vm", service_type == "azure"),
        backup_azure_sql=get_val("backupAzureSql", "backup_azure_sql", service_type == "azure"),
        backup_azure_postgresql=get_val("backupAzurePostgresql", "backup_azure_postgresql", service_type == "azure"),
        retention_type=get_val("retentionType", "retention_type", "INDEFINITE"),
        retention_days=get_val("retentionDays", "retention_days"),
        # Phase 1 SLA expansion fields — all optional; sensible defaults applied
        retention_mode=get_val("retentionMode", "retention_mode", "FLAT"),
        retention_hot_days=get_val("retentionHotDays", "retention_hot_days", 7),
        retention_cool_days=get_val("retentionCoolDays", "retention_cool_days", 30),
        retention_archive_days=get_val("retentionArchiveDays", "retention_archive_days"),
        gfs_daily_count=get_val("gfsDailyCount", "gfs_daily_count"),
        gfs_weekly_count=get_val("gfsWeeklyCount", "gfs_weekly_count"),
        gfs_monthly_count=get_val("gfsMonthlyCount", "gfs_monthly_count"),
        gfs_yearly_count=get_val("gfsYearlyCount", "gfs_yearly_count"),
        item_retention_days=get_val("itemRetentionDays", "item_retention_days"),
        item_retention_basis=get_val("itemRetentionBasis", "item_retention_basis", "SNAPSHOT"),
        archived_retention_mode=get_val("archivedRetentionMode", "archived_retention_mode", "SAME"),
        archived_retention_days=get_val("archivedRetentionDays", "archived_retention_days"),
        legal_hold_enabled=get_val("legalHoldEnabled", "legal_hold_enabled", False),
        legal_hold_until=get_val("legalHoldUntil", "legal_hold_until"),
        immutability_mode=get_val("immutabilityMode", "immutability_mode", "None"),
        encryption_mode=get_val("encryptionMode", "encryption_mode", "VAULT_MANAGED"),
        key_vault_uri=get_val("keyVaultUri", "key_vault_uri"),
        key_name=get_val("keyName", "key_name"),
        key_version=get_val("keyVersion", "key_version"),
        auto_apply_to_matching=get_val("autoApplyToMatching", "auto_apply_to_matching", False),
        enabled=get_val("enabled", "enabled", True),
        is_default=get_val("isDefault", "is_default", False),
    )
    # Mark dirty in the SAME transaction as the insert. The 5-min sweeper
    # in backup-scheduler picks this up if the HTTP nudge below drops on
    # the floor — durable end-to-end with no message-bus dependency.
    policy.lifecycle_dirty = True

    # Flip existing default BEFORE the INSERT — the partial unique index
    # `ix_sla_policies_one_default_per_tenant` fires at INSERT time, not at
    # commit, so the prior holder must already be is_default=False or the
    # INSERT raises UniqueViolation. Both UPDATE and INSERT live in the
    # same transaction so a concurrent reader never observes "two defaults"
    # or "zero defaults" mid-flip.
    if policy.is_default:
        from sqlalchemy import update as sa_update
        await db.execute(
            sa_update(SlaPolicy)
            .where(SlaPolicy.tenant_id == tenant_id)
            .where(SlaPolicy.is_default == True)  # noqa: E712
            .values(is_default=False)
        )
        await db.flush()

    db.add(policy)
    await db.flush()  # need policy.id for auto-apply downstream

    bound_count = 0
    if policy.auto_apply_to_matching:
        bound_count = await _apply_policy_to_matching(db, policy)

    await db.commit()
    if bound_count:
        print(f"[POLICY] auto_apply: {bound_count} resources bound to policy {policy.id}")

    # Notify scheduler — reschedule cron jobs and reconcile lifecycle / WORM /
    # legal-hold so policy changes take effect immediately instead of after
    # the 24h cron tick. Failure is non-fatal: lifecycle_dirty=True (set
    # transactionally above) ensures the 5-min sweeper picks it up.
    await notify_scheduler_reschedule()
    await notify_lifecycle_reconcile()

    response = policy_to_dict(policy)
    _idempotency_put(idempotency_key, response)
    return response


@app.put("/api/v1/policies/{policy_id}", response_model=SlaPolicyResponse)
async def update_policy(
    policy_id: str,
    request: dict,
    db: AsyncSession = Depends(get_db),
    if_match: Optional[str] = Header(default=None, alias="If-Match"),
):
    # Lock the row for the duration of this transaction so two concurrent
    # PUTs serialize cleanly — the loser's If-Match check then fails
    # cleanly with 412 instead of silently overwriting.
    stmt = (
        select(SlaPolicy)
        .where(SlaPolicy.id == UUID(policy_id))
        .with_for_update()
    )
    result = await db.execute(stmt)
    policy = result.scalar_one_or_none()
    if not policy:
        raise HTTPException(status_code=404, detail="Policy not found")

    # Optimistic concurrency: client sends If-Match: <updated_at-iso> from
    # the GET response; server rejects with 412 if the row has moved on.
    # We accept a missing header for back-compat (Wizard sends it; older
    # callers don't), but log a warning so we can spot hot edit paths.
    if if_match:
        current_etag = (
            policy.updated_at.isoformat() if policy.updated_at else
            (policy.created_at.isoformat() if policy.created_at else "")
        )
        if if_match.strip().strip('"') != current_etag:
            raise HTTPException(
                status_code=412,
                detail=(
                    "Policy has been modified by another writer since you "
                    "loaded it. Reload and re-apply your changes."
                ),
            )

    # Validate the proposed payload, then gate WORM transitions. The gate
    # passes the *prospective* new name (from the request, falling back
    # to the existing name if the request doesn't change it) so the typed-
    # name confirmation is checked against what the policy will be called
    # AFTER this save — which is what the operator typed in the modal.
    _validate_policy_payload(request)
    new_name = (request.get("name") or request.get("name") or policy.name or "").strip()
    _gate_immutability_lock(
        request,
        prior_mode=policy.immutability_mode,
        current_name=new_name,
    )
    # Zero-default guard. Reads is_default from the request (camelCase or
    # snake_case); compares against the prior value. Refuses 409 if the
    # change would leave the tenant with zero default policies while
    # other enabled policies are around.
    requested_is_default = None
    if "isDefault" in request:
        requested_is_default = bool(request["isDefault"])
    elif "is_default" in request:
        requested_is_default = bool(request["is_default"])
    await _guard_zero_default(
        db,
        tenant_id=policy.tenant_id,
        this_policy_id=policy.id,
        request_is_default=requested_is_default,
        prior_is_default=bool(policy.is_default),
    )

    # Helper to get value by camelCase or snake_case
    def get_val(camel: str, snake: str, default=None):
        return request.get(camel, request.get(snake, default))

    tenant = await db.get(Tenant, policy.tenant_id)
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    requested_service_type = normalize_policy_service_type(get_val("serviceType", "service_type"))
    if requested_service_type is not None:
        policy.service_type = requested_service_type
    elif not getattr(policy, "service_type", None):
        policy.service_type = tenant_policy_service_type(tenant)
    
    # Map camelCase field names to snake_case DB columns
    field_map = {
        'name': 'name', 'frequency': 'frequency', 'backupDays': 'backup_days',
        'backupWindowStart': 'backup_window_start',
        'backupExchange': 'backup_exchange', 'backupExchangeArchive': 'backup_exchange_archive',
        'backupExchangeRecoverable': 'backup_exchange_recoverable',
        'backupOneDrive': 'backup_onedrive', 'backupSharepoint': 'backup_sharepoint',
        'backupTeams': 'backup_teams', 'backupTeamsChats': 'backup_teams_chats',
        'backupEntraId': 'backup_entra_id', 'backupPowerPlatform': 'backup_power_platform',
        'backupCopilot': 'backup_copilot',
        'contacts': 'contacts', 'calendars': 'calendars', 'tasks': 'tasks',
        'groupMailbox': 'group_mailbox', 'planner': 'planner',
        'backupAzureVm': 'backup_azure_vm', 'backupAzureSql': 'backup_azure_sql',
        'backupAzurePostgresql': 'backup_azure_postgresql',
        'retentionType': 'retention_type', 'retentionDays': 'retention_days',
        # Phase 1 fields — editable after create
        'retentionMode': 'retention_mode',
        'retentionHotDays': 'retention_hot_days',
        'retentionCoolDays': 'retention_cool_days',
        'retentionArchiveDays': 'retention_archive_days',
        'gfsDailyCount': 'gfs_daily_count',
        'gfsWeeklyCount': 'gfs_weekly_count',
        'gfsMonthlyCount': 'gfs_monthly_count',
        'gfsYearlyCount': 'gfs_yearly_count',
        'itemRetentionDays': 'item_retention_days',
        'itemRetentionBasis': 'item_retention_basis',
        'archivedRetentionMode': 'archived_retention_mode',
        'archivedRetentionDays': 'archived_retention_days',
        'legalHoldEnabled': 'legal_hold_enabled',
        'legalHoldUntil': 'legal_hold_until',
        'immutabilityMode': 'immutability_mode',
        'encryptionMode': 'encryption_mode',
        'keyVaultUri': 'key_vault_uri',
        'keyName': 'key_name',
        'keyVersion': 'key_version',
        'autoApplyToMatching': 'auto_apply_to_matching',
        'enabled': 'enabled', 'isDefault': 'is_default',
    }
    
    # Special handling for is_default: flip OTHER policies' default to false
    # BEFORE letting the ORM apply the field_map (and thus before autoflush
    # writes this row's is_default=True). The partial unique index fires at
    # statement time, not commit time — if we let setattr happen first, the
    # next implicit flush UPDATEs this row to True while the prior holder
    # is still True, raising UniqueViolation.
    incoming_is_default = get_val('isDefault', 'is_default')
    if incoming_is_default is True and not policy.is_default:
        from sqlalchemy import update as sa_update
        await db.execute(
            sa_update(SlaPolicy)
            .where(SlaPolicy.tenant_id == policy.tenant_id)
            .where(SlaPolicy.id != policy.id)
            .where(SlaPolicy.is_default == True)  # noqa: E712
            .values(is_default=False)
        )
        await db.flush()

    for camel_key, snake_key in field_map.items():
        val = get_val(camel_key, snake_key)
        if val is not None:
            setattr(policy, snake_key, val)

    policy.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
    # Mark dirty so the 5-min sweeper picks this change up if the HTTP
    # nudge to the scheduler fails. Reset attempts so a previously-capped
    # policy gets one more shot whenever the operator edits it.
    policy.lifecycle_dirty = True
    policy.reconcile_attempts = 0

    bound_count = 0
    if policy.auto_apply_to_matching:
        # Only re-bind when something that affects matching has changed.
        # Trivial saves (rename, frequency change, retention day tweak)
        # don't need a re-bind sweep — the existing assignments still
        # match. At 25k resources / single tenant this skip saves a
        # full table scan + 25k UPDATE statements per noisy save.
        rebind_relevant_keys = {
            "autoApplyToMatching", "auto_apply_to_matching",
        }
        rebind = bool(rebind_relevant_keys.intersection(request.keys()))
        if rebind:
            bound_count = await _apply_policy_to_matching(db, policy)

    await db.commit()
    if bound_count:
        print(f"[POLICY] auto_apply: {bound_count} resources rebound to policy {policy.id}")

    # Notify scheduler — reschedule cron jobs and reconcile lifecycle / WORM /
    # legal-hold so policy changes take effect immediately instead of after
    # the 24h cron tick. Failure is non-fatal: lifecycle_dirty=True (set
    # transactionally above) ensures the 5-min sweeper picks it up.
    await notify_scheduler_reschedule()
    await notify_lifecycle_reconcile()

    return SlaPolicyResponse.model_validate(policy)


@app.delete("/api/v1/policies/{policy_id}", status_code=204)
async def delete_policy(policy_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(SlaPolicy).where(SlaPolicy.id == UUID(policy_id))
    result = await db.execute(stmt)
    policy = result.scalar_one_or_none()
    if not policy:
        raise HTTPException(status_code=404, detail="Policy not found")
    await db.delete(policy)
    await db.commit()

    # Notify scheduler to reschedule jobs without this policy
    await notify_scheduler_reschedule()


@app.get("/api/v1/policies/{policy_id}/resources")
async def get_policy_resources(policy_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(Resource).where(Resource.sla_policy_id == UUID(policy_id))
    result = await db.execute(stmt)
    resources = result.scalars().all()
    return {"content": [{"id": str(r.id), "name": r.display_name, "type": r.type.value, "assignedAt": r.created_at.isoformat()} for r in resources], "totalPages": 1, "totalElements": len(resources)}


@app.post("/api/v1/policies/{policy_id}/auto-assign", status_code=204)
async def auto_assign_policy(policy_id: str, request: dict):
    pass


@app.post("/api/v1/policies/{policy_id}/force-reconcile")
async def force_reconcile_policy(policy_id: str, db: AsyncSession = Depends(get_db)):
    """Operator-driven retry hook. Marks the policy lifecycle_dirty and
    nudges the scheduler to run an immediate reconcile pass.

    Why this exists: when a policy is stuck in `KEY_VAULT_ACCESS_DENIED`
    or capped on `reconcile_attempts`, the operator's normal recovery
    path is to fix the upstream config (Key Vault role, vault URI typo)
    and wait up to 24h for the dirty sweeper to retry. That's too slow
    when on-call is debugging a CMK outage. This endpoint clears the
    attempt cap, sets dirty=true, and pings the scheduler so the next
    reconcile happens within seconds rather than within the next sweep
    window. Idempotent — safe to call repeatedly.

    Returns: the post-flip policy state (encryption_status, attempts).
    """
    pol = await db.get(SlaPolicy, UUID(policy_id))
    if not pol:
        raise HTTPException(status_code=404, detail="Policy not found")
    pol.lifecycle_dirty = True
    pol.reconcile_attempts = 0
    pol.last_cap_alert_at = None
    pol.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
    await db.commit()
    await db.refresh(pol)
    # Best-effort nudge — durable retry via the 5-min sweeper if the
    # scheduler call fails (e.g. scheduler restarting during the call).
    try:
        await notify_lifecycle_reconcile()
    except Exception as exc:
        print(f"[force-reconcile] scheduler nudge failed (sweeper will retry): {exc}")
    return {
        "id": str(pol.id),
        "lifecycle_dirty": pol.lifecycle_dirty,
        "reconcile_attempts": pol.reconcile_attempts,
        "encryption_status": pol.encryption_status or "",
        "last_cap_alert_at": pol.last_cap_alert_at.isoformat() if pol.last_cap_alert_at else None,
    }


# ==================== SLA Exclusions ====================
# Per-policy exclusion rules (folder paths, file extensions, subject regex, etc.)
# Backup-worker consults these before staging each item. apply_to_historical flags
# items for offline purge from existing snapshots.

@app.get("/api/v1/policies/{policy_id}/exclusions", response_model=List[SlaExclusionResponse])
async def list_policy_exclusions(policy_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(SlaExclusion).where(SlaExclusion.policy_id == UUID(policy_id)).order_by(SlaExclusion.created_at.desc())
    result = await db.execute(stmt)
    return [SlaExclusionResponse.model_validate(x) for x in result.scalars().all()]


@app.post("/api/v1/policies/{policy_id}/exclusions", response_model=SlaExclusionResponse, status_code=201)
async def create_policy_exclusion(policy_id: str, body: SlaExclusionRequest, db: AsyncSession = Depends(get_db)):
    policy = await db.get(SlaPolicy, UUID(policy_id))
    if not policy:
        raise HTTPException(status_code=404, detail="Policy not found")

    allowed_types = {"FOLDER_PATH", "FILE_EXTENSION", "SUBJECT_REGEX", "MIME_TYPE", "EMAIL_ADDRESS", "FILENAME_GLOB"}
    if body.exclusionType not in allowed_types:
        raise HTTPException(status_code=400, detail=f"exclusion_type must be one of {sorted(allowed_types)}")

    exclusion = SlaExclusion(
        id=uuid4(),
        policy_id=policy.id,
        exclusion_type=body.exclusionType,
        pattern=body.pattern,
        workload=body.workload,
        apply_to_historical=body.applyToHistorical or False,
        enabled=body.enabled if body.enabled is not None else True,
    )
    db.add(exclusion)
    await db.commit()
    await db.refresh(exclusion)
    return SlaExclusionResponse.model_validate(exclusion)


@app.delete("/api/v1/policies/{policy_id}/exclusions/{exclusion_id}", status_code=204)
async def delete_policy_exclusion(policy_id: str, exclusion_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(SlaExclusion).where(
        SlaExclusion.id == UUID(exclusion_id),
        SlaExclusion.policy_id == UUID(policy_id),
    )
    exclusion = (await db.execute(stmt)).scalar_one_or_none()
    if not exclusion:
        raise HTTPException(status_code=404, detail="Exclusion not found")
    await db.delete(exclusion)
    await db.commit()


# ==================== Resource Groups ====================
# Dynamic (rule-based) or static groups for mass-policy-assignment.
# Discovery-worker evaluates rules on newly-discovered resources when
# auto_protect_new=true on any group that has a policy attached.

async def _serialize_group(db: AsyncSession, g: ResourceGroup) -> Dict[str, Any]:
    """Fetch attached policy ids for a group and return the full API response dict."""
    assignments = (await db.execute(
        select(GroupPolicyAssignment.policy_id).where(GroupPolicyAssignment.group_id == g.id)
    )).scalars().all()
    payload = ResourceGroupResponse.model_validate(g).model_dump(by_alias=False)
    payload["attachedPolicyIds"] = [str(pid) for pid in assignments]
    return payload


@app.get("/api/v1/resource-groups")
async def list_resource_groups(
    tenantId: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    stmt = (select(ResourceGroup)
            .where(ResourceGroup.tenant_id == UUID(tenantId))
            .order_by(ResourceGroup.priority.asc(), ResourceGroup.created_at.desc()))
    groups = (await db.execute(stmt)).scalars().all()
    return [await _serialize_group(db, g) for g in groups]


@app.get("/api/v1/resource-groups/{group_id}")
async def get_resource_group(group_id: str, db: AsyncSession = Depends(get_db)):
    g = await db.get(ResourceGroup, UUID(group_id))
    if not g:
        raise HTTPException(status_code=404, detail="Resource group not found")
    return await _serialize_group(db, g)


@app.post("/api/v1/resource-groups", status_code=201)
async def create_resource_group(body: dict, db: AsyncSession = Depends(get_db)):
    tenant_id = body.get("tenantId") or body.get("tenant_id")
    if not tenant_id:
        raise HTTPException(status_code=400, detail="tenantId is required")
    if not body.get("name"):
        raise HTTPException(status_code=400, detail="name is required")
    group = ResourceGroup(
        id=uuid4(),
        tenant_id=UUID(tenant_id),
        name=body.get("name"),
        description=body.get("description"),
        group_type=body.get("groupType") or body.get("group_type") or "DYNAMIC",
        rules=body.get("rules") or [],
        combinator=body.get("combinator") or "AND",
        priority=body.get("priority", 100),
        auto_protect_new=body.get("autoProtectNew", body.get("auto_protect_new", False)),
        enabled=body.get("enabled", True),
    )
    db.add(group)
    await db.commit()
    await db.refresh(group)
    return await _serialize_group(db, group)


@app.put("/api/v1/resource-groups/{group_id}")
async def update_resource_group(group_id: str, body: dict, db: AsyncSession = Depends(get_db)):
    g = await db.get(ResourceGroup, UUID(group_id))
    if not g:
        raise HTTPException(status_code=404, detail="Resource group not found")
    field_map = {
        "name": "name", "description": "description",
        "groupType": "group_type", "group_type": "group_type",
        "rules": "rules", "combinator": "combinator", "priority": "priority",
        "autoProtectNew": "auto_protect_new", "auto_protect_new": "auto_protect_new",
        "enabled": "enabled",
    }
    # Capture the prior rules/combinator/enabled snapshot so we can detect
    # whether matching semantics actually changed. Only a real change
    # warrants a re-bind sweep — saving a description shouldn't kick off
    # a 25k-resource scan.
    prior_rules = g.rules
    prior_combinator = g.combinator
    prior_enabled = g.enabled
    prior_priority = g.priority

    for key, column in field_map.items():
        if key in body:
            setattr(g, column, body[key])
    g.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
    await db.commit()
    await db.refresh(g)

    # Re-bind hook: if the rule expression changed (or the group flipped
    # enabled/disabled, or the priority changed in a way that may shift
    # which policy wins on a contested resource), trigger a re-evaluation
    # against any auto-apply policies attached to this group. Resources
    # that previously matched but no longer do retain their assignment —
    # we don't unbind here; that's a separate "purge" workflow operators
    # have to opt into explicitly to avoid accidental data exposure.
    rules_changed = (
        prior_rules != g.rules
        or prior_combinator != g.combinator
        or prior_enabled != g.enabled
        or prior_priority != g.priority
    )
    if rules_changed and (g.enabled is True):
        try:
            attached_policies = (await db.execute(
                select(SlaPolicy)
                .join(GroupPolicyAssignment, GroupPolicyAssignment.policy_id == SlaPolicy.id)
                .where(GroupPolicyAssignment.group_id == g.id)
                .where(SlaPolicy.tenant_id == g.tenant_id)
                .where(SlaPolicy.enabled.is_(True))
                .where(SlaPolicy.auto_apply_to_matching.is_(True))
            )).scalars().all()
            for pol in attached_policies:
                await _apply_policy_to_matching(db, pol)
        except Exception as exc:
            # Re-bind is best-effort. The discovery-worker's auto-protect
            # sweep is the durable fallback so a transient DB hiccup here
            # doesn't strand resources on the wrong policy long-term.
            print(f"[resource-group rebind] {g.id}: {exc}")
    return await _serialize_group(db, g)


@app.delete("/api/v1/resource-groups/{group_id}", status_code=204)
async def delete_resource_group(group_id: str, db: AsyncSession = Depends(get_db)):
    g = await db.get(ResourceGroup, UUID(group_id))
    if not g:
        raise HTTPException(status_code=404, detail="Resource group not found")
    await db.delete(g)  # assignments cascade
    await db.commit()


# ---------- Group ↔ policy attach / detach ----------

@app.post("/api/v1/resource-groups/{group_id}/policies", status_code=201)
async def attach_policy_to_group(
    group_id: str,
    body: GroupPolicyAssignmentRequest,
    db: AsyncSession = Depends(get_db),
):
    g = await db.get(ResourceGroup, UUID(group_id))
    if not g:
        raise HTTPException(status_code=404, detail="Resource group not found")
    policy = await db.get(SlaPolicy, UUID(body.policyId))
    if not policy:
        raise HTTPException(status_code=404, detail="Policy not found")
    if policy.tenant_id != g.tenant_id:
        raise HTTPException(status_code=400, detail="Policy and group must belong to the same tenant")

    # Idempotent — if already attached, return existing
    existing_stmt = select(GroupPolicyAssignment).where(
        GroupPolicyAssignment.group_id == g.id,
        GroupPolicyAssignment.policy_id == policy.id,
    )
    existing = (await db.execute(existing_stmt)).scalar_one_or_none()
    if existing:
        return {"id": str(existing.id), "groupId": str(g.id), "policyId": str(policy.id), "alreadyAttached": True}

    link = GroupPolicyAssignment(id=uuid4(), group_id=g.id, policy_id=policy.id)
    db.add(link)
    await db.commit()
    return {"id": str(link.id), "groupId": str(g.id), "policyId": str(policy.id)}


@app.delete("/api/v1/resource-groups/{group_id}/policies/{policy_id}", status_code=204)
async def detach_policy_from_group(
    group_id: str, policy_id: str,
    db: AsyncSession = Depends(get_db),
):
    stmt = select(GroupPolicyAssignment).where(
        GroupPolicyAssignment.group_id == UUID(group_id),
        GroupPolicyAssignment.policy_id == UUID(policy_id),
    )
    link = (await db.execute(stmt)).scalar_one_or_none()
    if not link:
        raise HTTPException(status_code=404, detail="Assignment not found")
    await db.delete(link)
    await db.commit()
