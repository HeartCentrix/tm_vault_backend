"""Tenant Service - Manages tenants and organizations"""
from contextlib import asynccontextmanager
from typing import Optional
from uuid import UUID, uuid4
from datetime import datetime, timezone, timedelta
import csv
import io

from fastapi import FastAPI, Depends, HTTPException, Query, Response, BackgroundTasks
from sqlalchemy import select, func, text

from shared.config import settings
from shared.database import get_db, close_db, AsyncSession, engine, async_session_factory
from shared.models import Tenant, TenantType, TenantStatus, Organization, Resource, ResourceStatus, ResourceType, SlaPolicy, BackupBatch
from shared.schemas import (
    TenantResponse, TenantCreateRequest, DiscoveryStatus, TenantInfoResponse,
    StorageSummaryItem, OrganizationResponse
)
from shared.graph_client import GraphClient
from shared.tier2_discovery import ensure_tier2_children


TYPE_MAP = {
    "MAILBOX": ResourceType.MAILBOX,
    "SHARED_MAILBOX": ResourceType.SHARED_MAILBOX,
    "ROOM_MAILBOX": ResourceType.ROOM_MAILBOX,
    "ONEDRIVE": ResourceType.ONEDRIVE,
    "SHAREPOINT_SITE": ResourceType.SHAREPOINT_SITE,
    "TEAMS_CHANNEL": ResourceType.TEAMS_CHANNEL,
    "TEAMS_CHAT": ResourceType.TEAMS_CHAT,
    "ENTRA_USER": ResourceType.ENTRA_USER,
    "ENTRA_GROUP": ResourceType.ENTRA_GROUP,
    "M365_GROUP": ResourceType.M365_GROUP,
    "DYNAMIC_GROUP": ResourceType.DYNAMIC_GROUP,
    "ENTRA_APP": ResourceType.ENTRA_APP,
    "ENTRA_DEVICE": ResourceType.ENTRA_DEVICE,
    "POWER_BI": ResourceType.POWER_BI,
    "POWER_APPS": ResourceType.POWER_APPS,
    "POWER_AUTOMATE": ResourceType.POWER_AUTOMATE,
    "POWER_DLP": ResourceType.POWER_DLP,
    # Tier 2 child types — created by the per-user content discovery endpoint.
    "USER_MAIL": ResourceType.USER_MAIL,
    "USER_ONEDRIVE": ResourceType.USER_ONEDRIVE,
    "USER_CONTACTS": ResourceType.USER_CONTACTS,
    "USER_CALENDAR": ResourceType.USER_CALENDAR,
    "USER_CHATS": ResourceType.USER_CHATS,
}


async def run_tenant_discovery(db: AsyncSession, tenant: Tenant) -> int:
    """Run discovery for a single tenant. Returns count of new resources."""
    tenant.status = TenantStatus.DISCOVERING
    await db.flush()

    client_id = settings.MICROSOFT_CLIENT_ID or settings.AZURE_AD_CLIENT_ID
    client_secret = settings.MICROSOFT_CLIENT_SECRET or settings.AZURE_AD_CLIENT_SECRET
    ext_tenant_id = tenant.external_tenant_id or settings.MICROSOFT_TENANT_ID or settings.AZURE_AD_TENANT_ID or "common"

    graph = GraphClient(client_id, client_secret, ext_tenant_id)
    resources = await graph.discover_all()

    count = 0
    for r in resources:
        rtype = TYPE_MAP.get(r.get("type", "ENTRA_USER"), ResourceType.ENTRA_USER)
        existing_stmt = select(Resource).where(
            Resource.tenant_id == tenant.id,
            Resource.type == rtype,
            Resource.external_id == r["external_id"],
        )
        existing_result = await db.execute(existing_stmt)
        existing = existing_result.scalar_one_or_none()

        if existing is None:
            rtype = TYPE_MAP.get(r.get("type", "ENTRA_USER"), ResourceType.ENTRA_USER)
            resource = Resource(
                id=uuid4(),
                tenant_id=tenant.id,
                type=rtype,
                external_id=r["external_id"],
                display_name=r.get("display_name", "Unknown"),
                email=r.get("email"),
                extra_data=r.get("metadata", {}),
                status=ResourceStatus.DISCOVERED,
            )
            db.add(resource)
            count += 1
        else:
            existing.display_name = r.get("display_name", existing.display_name)
            if r.get("email"):
                existing.email = r["email"]
            existing.extra_data = {**(existing.extra_data or {}), **(r.get("metadata") or {})}

    # Singleton "Azure Active Directory" resource — matches AFI's
    # office_directory model. One row per M365 tenant; holds all
    # Entra-wide content (users / groups / roles / applications /
    # audit / security / intune / admin units) as snapshot_items.
    if tenant.type == TenantType.M365:
        dir_ext_id = tenant.external_tenant_id or str(tenant.id)
        dir_stmt = select(Resource).where(
            Resource.tenant_id == tenant.id,
            Resource.type == ResourceType.ENTRA_DIRECTORY,
        )
        dir_row = (await db.execute(dir_stmt)).scalar_one_or_none()
        if dir_row is None:
            db.add(Resource(
                id=uuid4(),
                tenant_id=tenant.id,
                type=ResourceType.ENTRA_DIRECTORY,
                external_id=dir_ext_id,
                display_name="Azure Active Directory",
                email=None,
                extra_data={"source": "tenant_singleton"},
                status=ResourceStatus.DISCOVERED,
            ))
            count += 1

    tenant.status = TenantStatus.ACTIVE
    tenant.last_discovery_at = datetime.now(timezone.utc).replace(tzinfo=None)
    await db.flush()
    return count


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Keep startup lightweight so worker boot does not block tenant reads.
    from shared import core_metrics
    core_metrics.init()
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))
    yield
    await close_db()


app = FastAPI(title="Tenant Service", version="1.0.0", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "tenant"}


@app.get("/api/v1/tenants", response_model=list[TenantResponse])
async def list_tenants(
    orgId: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(Tenant).order_by(Tenant.created_at)
    if orgId:
        stmt = stmt.where(Tenant.org_id == UUID(orgId))
    result = await db.execute(stmt)
    tenants = result.scalars().all()
    return [
        TenantResponse(
            id=str(t.id),
            displayName=t.display_name,
            orgId=str(t.org_id) if t.org_id else None,
            type=t.type.value if t.type else None,
            externalTenantId=t.external_tenant_id,
            customerId=t.customer_id,
            status=t.status.value if t.status else "PENDING",
            storageRegion=t.storage_region,
            lastDiscoveryAt=t.last_discovery_at.isoformat() if t.last_discovery_at else None,
            createdAt=t.created_at.isoformat() if t.created_at else None,
        )
        for t in tenants
    ]


@app.get("/api/v1/tenants/{tenant_id}", response_model=TenantResponse)
async def get_tenant(tenant_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(Tenant).where(Tenant.id == UUID(tenant_id))
    result = await db.execute(stmt)
    tenant = result.scalar_one_or_none()
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    return TenantResponse(
        id=str(tenant.id),
        displayName=tenant.display_name,
        orgId=str(tenant.org_id) if tenant.org_id else None,
        status=tenant.status.value if tenant.status else "PENDING",
        createdAt=tenant.created_at.isoformat() if tenant.created_at else None,
    )


@app.post("/api/v1/tenants", response_model=TenantResponse)
async def create_tenant(request: TenantCreateRequest, db: AsyncSession = Depends(get_db)):
    tenant = Tenant(
        id=uuid4(),
        org_id=UUID(request.organizationId),
        type=TenantType.M365,
        display_name=request.name,
        external_tenant_id=request.microsoftTenantId,
        customer_id=str(uuid4()),  # Auto-generate customer ID
        status=TenantStatus.PENDING,
    )
    db.add(tenant)
    await db.flush()

    # Auto-trigger discovery for the new tenant
    try:
        count = await run_tenant_discovery(db, tenant)
        await db.commit()
    except Exception as e:
        # Don't fail tenant creation if discovery fails
        tenant.status = TenantStatus.DISCONNECTED
        await db.commit()
        # Still return the tenant - user can retry discovery manually
        pass

    return TenantResponse(
        id=str(tenant.id),
        displayName=tenant.display_name,
        orgId=str(tenant.org_id) if tenant.org_id else None,
        type=tenant.type.value if tenant.type else None,
        externalTenantId=tenant.external_tenant_id,
        status=tenant.status.value if tenant.status else "PENDING",
        storageRegion=tenant.storage_region,
        lastDiscoveryAt=tenant.last_discovery_at.isoformat() if tenant.last_discovery_at else None,
        createdAt=tenant.created_at.isoformat() if tenant.created_at else None,
    )


@app.put("/api/v1/tenants/{tenant_id}", response_model=TenantResponse)
async def update_tenant(tenant_id: str, request: dict, db: AsyncSession = Depends(get_db)):
    stmt = select(Tenant).where(Tenant.id == UUID(tenant_id))
    result = await db.execute(stmt)
    tenant = result.scalar_one_or_none()
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    if request.get("name"):
        tenant.display_name = request["name"]
    if request.get("status"):
        tenant.status = TenantStatus(request["status"])
    tenant.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
    await db.flush()
    return TenantResponse(
        id=str(tenant.id),
        displayName=tenant.display_name,
        status=tenant.status.value,
        createdAt=tenant.created_at.isoformat(),
    )


@app.patch("/api/v1/tenants/{tenant_id}/dr-config")
async def set_dr_config(
    tenant_id: str, request: dict, db: AsyncSession = Depends(get_db),
):
    """Toggle and configure cross-region DR replication for a tenant.

    Request body fields (all optional):
      enabled (bool)              — flip `dr_region_enabled`
      region (str)                — Azure region for the secondary backup store
      dr_storage_account_name (str)
      dr_storage_account_key (str) — plaintext on the wire; encrypted at rest
                                     via ``shared.security.encrypt_secret``.

    DR worker (`dr_replication` Railway service) scans every 5 min for
    snapshots with status COMPLETED + dr_replication_status='pending'
    on tenants where this is enabled, and replicates them to the
    secondary store. The worker is already deployed; this endpoint
    flips the per-tenant switch + provisioning info.

    SeaweedFS caveat: the current DR worker is Azure-Blob-specific
    (uses StorageManagementClient + AzureStorageShard). For SeaweedFS-
    backed tenants, DR requires a separate cross-region SeaweedFS
    cluster + replication policy — outside this endpoint. The flag
    is still honored (worker skips with INFO log) so toggling it for
    a SeaweedFS tenant is non-destructive.
    """
    stmt = select(Tenant).where(Tenant.id == UUID(tenant_id))
    result = await db.execute(stmt)
    tenant = result.scalar_one_or_none()
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")

    if "enabled" in request:
        tenant.dr_region_enabled = bool(request["enabled"])
    if "region" in request:
        tenant.dr_region = request["region"] or None
    if "dr_storage_account_name" in request:
        tenant.dr_storage_account_name = request["dr_storage_account_name"] or None
    if "dr_storage_account_key" in request:
        plaintext_key = request["dr_storage_account_key"]
        if plaintext_key:
            from shared.security import encrypt_secret
            tenant.dr_storage_account_key_encrypted = encrypt_secret(plaintext_key)
        else:
            tenant.dr_storage_account_key_encrypted = None

    tenant.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
    await db.flush()
    return {
        "tenant_id": str(tenant.id),
        "dr_region_enabled": bool(tenant.dr_region_enabled),
        "dr_region": tenant.dr_region,
        "dr_storage_account_name": tenant.dr_storage_account_name,
        "dr_credentials_configured": tenant.dr_storage_account_key_encrypted is not None,
    }


@app.get("/api/v1/tenants/{tenant_id}/dr-config")
async def get_dr_config(tenant_id: str, db: AsyncSession = Depends(get_db)):
    """Return current DR configuration for a tenant (no secrets surfaced)."""
    stmt = select(Tenant).where(Tenant.id == UUID(tenant_id))
    result = await db.execute(stmt)
    tenant = result.scalar_one_or_none()
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    return {
        "tenant_id": str(tenant.id),
        "dr_region_enabled": bool(tenant.dr_region_enabled),
        "dr_region": tenant.dr_region,
        "dr_storage_account_name": tenant.dr_storage_account_name,
        "dr_credentials_configured": tenant.dr_storage_account_key_encrypted is not None,
        "dr_last_replicated_at": (
            tenant.dr_last_replicated_at.isoformat()
            if tenant.dr_last_replicated_at else None
        ),
    }


@app.delete("/api/v1/tenants/{tenant_id}", status_code=204)
async def delete_tenant(tenant_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(Tenant).where(Tenant.id == UUID(tenant_id))
    result = await db.execute(stmt)
    tenant = result.scalar_one_or_none()
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    tenant.status = TenantStatus.PENDING_DELETION
    await db.flush()


@app.post("/api/v1/tenants/{tenant_id}/discover")
@app.post("/api/v1/tenants/{tenant_id}/discover-m365")
async def trigger_discovery(tenant_id: str, background_tasks: BackgroundTasks, db: AsyncSession = Depends(get_db)):
    """Trigger M365 discovery in background and return immediately"""
    stmt = select(Tenant).where(Tenant.id == UUID(tenant_id))
    result = await db.execute(stmt)
    tenant = result.scalar_one_or_none()
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")

    tenant.status = TenantStatus.DISCOVERING
    await db.commit()

    tenant_display = tenant.display_name

    async def _post_audit(payload: dict) -> None:
        try:
            import httpx
            async with httpx.AsyncClient(timeout=5.0) as _c:
                await _c.post(f"{settings.AUDIT_SERVICE_URL}/api/v1/audit/log", json=payload)
        except Exception:
            pass

    await _post_audit({
        "action": "DISCOVERY_STARTED",
        "tenant_id": tenant_id,
        "actor_type": "USER",
        "resource_type": "M365",
        "resource_name": tenant_display,
        "details": {"type": "M365"},
    })

    async def _run():
        async with async_session_factory() as bg_db:
            t = await bg_db.get(Tenant, tenant.id)
            try:
                count = await run_tenant_discovery(bg_db, t)
                await bg_db.commit()
                print(f"[DISCOVERY] M365 complete for {tenant_id}: {count} resources")
                await _post_audit({
                    "action": "DISCOVERY_RUN",
                    "tenant_id": tenant_id,
                    "actor_type": "USER",
                    "resource_type": "M365",
                    "resource_name": tenant_display,
                    "outcome": "SUCCESS",
                    "details": {"resourcesFound": count, "type": "M365"},
                })
            except Exception as e:
                t.status = TenantStatus.DISCONNECTED
                await bg_db.commit()
                print(f"[DISCOVERY] M365 failed for {tenant_id}: {e}")
                await _post_audit({
                    "action": "DISCOVERY_RUN",
                    "tenant_id": tenant_id,
                    "actor_type": "USER",
                    "resource_type": "M365",
                    "resource_name": tenant_display,
                    "outcome": "FAILURE",
                    "details": {"error": str(e), "type": "M365"},
                })

    background_tasks.add_task(_run)
    return {"discoveryId": str(uuid4()), "status": "started", "message": "Discovery running in background"}


@app.post("/api/v1/tenants/{tenant_id}/discover-azure")
async def trigger_azure_discovery(tenant_id: str, db: AsyncSession = Depends(get_db)):
    """Trigger Azure resource discovery for a specific tenant.
    Publishes to discovery.azure queue for the discovery worker to consume."""
    import uuid as _uuid
    stmt = select(Tenant).where(Tenant.id == UUID(tenant_id))
    result = await db.execute(stmt)
    tenant = result.scalar_one_or_none()
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")

    tenant.status = TenantStatus.DISCOVERING
    await db.commit()

    # Publish to discovery.azure queue
    from shared.message_bus import message_bus as msg_bus
    if not msg_bus.connection:
        await msg_bus.connect()

    discovery_id = str(_uuid.uuid4())
    await msg_bus.publish("discovery.azure", {
        "jobId": discovery_id,
        "tenantId": str(tenant.id),
        "externalTenantId": tenant.external_tenant_id or "",
        "discoveryScope": ["azure_vms", "azure_sql", "azure_postgresql"],
        "triggeredBy": "API",
        "triggeredAt": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).replace(tzinfo=None).isoformat(),
    }, priority=5)

    return {"discoveryId": discovery_id, "tenantId": str(tenant.id), "resourcesFound": -1}


@app.get("/api/v1/resources/{resource_id}/subsites")
async def list_sharepoint_subsites(
    resource_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Return the live list of subsites for a SharePoint site resource.
    Used by the Recovery page's Subsites panel. The backup-worker tracks
    subsite delta tokens per-resource but doesn't persist display names,
    so we hit Graph directly here and let the frontend render whatever
    subsites currently exist on the site.
    """
    resource = (await db.execute(select(Resource).where(Resource.id == UUID(resource_id)))).scalar_one_or_none()
    if not resource:
        raise HTTPException(status_code=404, detail="Resource not found")
    if resource.type != ResourceType.SHAREPOINT_SITE:
        raise HTTPException(status_code=400, detail=f"Resource type {resource.type.value} is not a SharePoint site")

    tenant = (await db.execute(select(Tenant).where(Tenant.id == resource.tenant_id))).scalar_one_or_none()
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")

    client_id = settings.MICROSOFT_CLIENT_ID or settings.AZURE_AD_CLIENT_ID
    client_secret = settings.MICROSOFT_CLIENT_SECRET or settings.AZURE_AD_CLIENT_SECRET
    ext_tenant_id = tenant.external_tenant_id or settings.MICROSOFT_TENANT_ID or settings.AZURE_AD_TENANT_ID or "common"
    graph = GraphClient(client_id, client_secret, ext_tenant_id)

    try:
        data = await graph.get_sharepoint_subsites(resource.external_id)
    except Exception as exc:
        print(f"[tenant-service] subsites fetch failed for {resource.external_id}: {exc}")
        raise HTTPException(status_code=502, detail=f"Failed to fetch subsites: {exc}")

    subsites = []
    for s in (data.get("value") or []):
        subsites.append({
            "id": (s.get("id") or "").replace(",", "/"),
            "displayName": s.get("displayName") or s.get("name") or "Unknown subsite",
            "name": s.get("name"),
            "webUrl": s.get("webUrl"),
            "createdDateTime": s.get("createdDateTime"),
            "lastModifiedDateTime": s.get("lastModifiedDateTime"),
        })
    return {"resourceId": resource_id, "subsites": subsites, "count": len(subsites)}


@app.post("/api/v1/tenants/{tenant_id}/users/{user_resource_id}/discover-content")
async def discover_user_content(
    tenant_id: str,
    user_resource_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Tier 2 discovery: fetch the five fixed content categories
    (Mail / OneDrive / Contacts / Calendar / Chats) for one user and persist
    them as child rows under the user resource.

    Triggered when the user opens the backup flow for a specific user — gives
    the UI per-content counts and gives the backup worker the IDs it needs
    (drive id, chat ids, etc.) without re-walking Graph at backup time."""
    tenant = (await db.execute(select(Tenant).where(Tenant.id == UUID(tenant_id)))).scalar_one_or_none()
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")

    user = (await db.execute(
        select(Resource).where(Resource.id == UUID(user_resource_id), Resource.tenant_id == tenant.id)
    )).scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User resource not found")
    if user.type != ResourceType.ENTRA_USER:
        raise HTTPException(status_code=400, detail=f"Resource type {user.type.value} is not a user (must be ENTRA_USER)")

    client_id = settings.MICROSOFT_CLIENT_ID or settings.AZURE_AD_CLIENT_ID
    client_secret = settings.MICROSOFT_CLIENT_SECRET or settings.AZURE_AD_CLIENT_SECRET
    ext_tenant_id = tenant.external_tenant_id or settings.MICROSOFT_TENANT_ID or settings.AZURE_AD_TENANT_ID or "common"
    graph = GraphClient(client_id, client_secret, ext_tenant_id)

    children = await ensure_tier2_children(db, user, graph)
    child_ids = [str(c.id) for c in children]
    return {
        "tenantId": tenant_id,
        "userResourceId": user_resource_id,
        "contentDiscovered": len(children),
        "categories": [c.type.value if hasattr(c.type, "value") else str(c.type) for c in children],
        # Frontend uses these to fan a bulk backup out across all 5 children
        # immediately after discovery so the user's Mail/OneDrive/Contacts/
        # Calendar/Chats actually get persisted as snapshots.
        "childResourceIds": child_ids,
    }


@app.post("/api/v1/tenants/{tenant_id}/users/{user_resource_id}/backup", status_code=202)
async def backup_user_with_discovery(
    tenant_id: str,
    user_resource_id: str,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
    """Fire-and-forget user backup: returns 202 immediately, then in the
    background runs Tier 2 content discovery and queues a bulk backup for
    parent + every discovered child.

    Replaces the two-step `discover-content` → `trigger-bulk` flow that
    forced the UI to await both round-trips before the user could navigate
    away. Now a single POST hands the work off to the server."""
    tenant = (await db.execute(select(Tenant).where(Tenant.id == UUID(tenant_id)))).scalar_one_or_none()
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    user = (await db.execute(
        select(Resource).where(Resource.id == UUID(user_resource_id), Resource.tenant_id == tenant.id)
    )).scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User resource not found")
    if user.type != ResourceType.ENTRA_USER:
        raise HTTPException(status_code=400, detail=f"Resource type {user.type.value} is not a user (must be ENTRA_USER)")
    if not user.sla_policy_id:
        raise HTTPException(status_code=400, detail="User must have an SLA policy assigned before triggering a backup")

    # Snapshot the bits we need so the background task doesn't depend on
    # the request-scoped DB session.
    user_id = user.id
    user_external_id = user.external_id
    user_display_name = user.display_name
    user_upn = (user.extra_data or {}).get("user_principal_name") or user.email
    user_sla_policy_id = user.sla_policy_id
    tenant_external_id = tenant.external_tenant_id
    tenant_uuid = tenant.id

    # One operator click = one batch_id, so Tier-1 + Tier-2 child Jobs
    # produced by the routing-key split inside trigger-bulk collapse to
    # one Activity row. Generated here (not in trigger-bulk) so we have a
    # stable id BEFORE the orchestrator HTTP roundtrip — future-proof for
    # any other side-channels that need the same id.
    #
    # When BATCH_ROW_REDESIGN_ENABLED, also INSERT a backup_batches row
    # with operator intent — Activity feed reads this row directly and
    # the finalizer gates the terminal flip on every scoped leaf.
    batch_row_id = uuid4()
    batch_id = str(batch_row_id)
    bytes_expected = None
    try:
        from shared.storage_rollup import exclude_tier2_storage_dupes_clause
        bytes_expected = (await db.execute(
            select(func.coalesce(func.sum(Resource.storage_bytes), 0))
            .where(
                Resource.tenant_id == tenant_uuid,
                Resource.id == user_id,
                exclude_tier2_storage_dupes_clause(),
            )
        )).scalar() or None
    except Exception as _e:
        # bytes_expected is ETA-only — non-fatal if the rollup query fails.
        bytes_expected = None
    db.add(BackupBatch(
        id=batch_row_id,
        tenant_id=tenant_uuid,
        source="manual_user",
        actor_email=None,  # request-scope user identity not propagated here yet
        scope_user_ids=[user_id],
        bytes_expected=int(bytes_expected) if bytes_expected else None,
        status="IN_PROGRESS",
    ))
    await db.commit()

    async def _orchestrate():
        client_id = settings.MICROSOFT_CLIENT_ID or settings.AZURE_AD_CLIENT_ID
        client_secret = settings.MICROSOFT_CLIENT_SECRET or settings.AZURE_AD_CLIENT_SECRET
        ext_tenant_id = tenant_external_id or settings.MICROSOFT_TENANT_ID or settings.AZURE_AD_TENANT_ID or "common"
        graph = GraphClient(client_id, client_secret, ext_tenant_id)

        child_ids: List[str] = []
        async with async_session_factory() as bg_db:
            user = await bg_db.get(Resource, user_id)
            if user is None:
                print(f"[BACKUP-ORCHESTRATOR] user resource {user_id} vanished before discovery — skipping")
                return
            try:
                children = await ensure_tier2_children(bg_db, user, graph)
            except Exception as e:
                print(f"[BACKUP-ORCHESTRATOR] discovery failed for user {user_id}: {e}")
                children = []
            # Don't fan out backup to INACCESSIBLE children (license-missing
            # workloads). They stay visible in the UI with a "No license"
            # badge but the bulk trigger excludes them.
            child_ids = [
                str(c.id) for c in children
                if c.status != ResourceStatus.INACCESSIBLE
            ]

        # Hand the bulk backup to the job-service. Direct HTTP call so we
        # reuse all of trigger-bulk's validation, audit logging, and queue
        # routing without re-implementing it here.
        backup_targets = [str(user_id), *child_ids]
        try:
            import httpx as _httpx
            async with _httpx.AsyncClient(timeout=15.0) as _c:
                r = await _c.post(
                    f"{settings.JOB_SERVICE_URL}/api/v1/backups/trigger-bulk",
                    json={
                        "resourceIds": backup_targets,
                        "fullBackup": False,
                        "priority": 1,
                        "batchId": batch_id,
                    },
                )
                if r.status_code >= 300:
                    print(f"[BACKUP-ORCHESTRATOR] trigger-bulk rejected (status={r.status_code}): {r.text[:300]}")
                else:
                    print(f"[BACKUP-ORCHESTRATOR] queued bulk backup for {len(backup_targets)} resource(s) under user {user_id}")
        except Exception as e:
            print(f"[BACKUP-ORCHESTRATOR] trigger-bulk call failed for user {user_id}: {e}")

    background_tasks.add_task(_orchestrate)
    return {
        "accepted": True,
        "tenantId": tenant_id,
        "userResourceId": user_resource_id,
        "batchId": batch_id,
        "message": "Discovery + backup queued. Status will appear on the Activity page shortly.",
    }


@app.get("/api/v1/tenants/{tenant_id}/discovery-status", response_model=DiscoveryStatus)
async def get_discovery_status(tenant_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(Tenant).where(Tenant.id == UUID(tenant_id))
    result = await db.execute(stmt)
    tenant = result.scalar_one_or_none()
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    is_running = tenant.status == TenantStatus.DISCOVERING
    return DiscoveryStatus(
        tenantId=tenant_id,
        status="RUNNING" if is_running else "COMPLETED",
        progress=100 if not is_running else 50,
        resourcesDiscovered=0,
        startedAt=tenant.last_discovery_at.isoformat() if tenant.last_discovery_at else datetime.now(timezone.utc).isoformat(),
    )


@app.get("/api/v1/tenants/{tenant_id}/storage-summary")
async def get_storage_summary(tenant_id: str, db: AsyncSession = Depends(get_db)):
    # Simplified - return empty list
    return []


@app.post("/api/v1/tenants/{tenant_id}/test-connection")
async def test_connection(tenant_id: str):
    return {"connected": True, "message": "Connection successful"}


@app.get("/api/v1/organizations", response_model=list[OrganizationResponse])
async def list_orgs(db: AsyncSession = Depends(get_db)):
    stmt = select(Organization).order_by(Organization.created_at)
    result = await db.execute(stmt)
    orgs = result.scalars().all()
    return [
        OrganizationResponse(
            id=str(o.id),
            name=o.name,
            status="ACTIVE",
            tenantCount=0,
            createdAt=o.created_at.isoformat() if o.created_at else "",
        )
        for o in orgs
    ]


@app.get("/api/v1/organizations/{org_id}", response_model=OrganizationResponse)
async def get_org(org_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(Organization).where(Organization.id == UUID(org_id))
    result = await db.execute(stmt)
    org = result.scalar_one_or_none()
    if not org:
        raise HTTPException(status_code=404, detail="Organization not found")
    return OrganizationResponse(
        id=str(org.id),
        name=org.name,
        status="ACTIVE",
        tenantCount=0,
        createdAt=org.created_at.isoformat() if org.created_at else "",
    )


@app.get("/api/v1/tenants/{tenant_id}/info", response_model=TenantInfoResponse)
async def get_tenant_info(tenant_id: str, db: AsyncSession = Depends(get_db)):
    """Get tenant info for the settings info page (Customer ID, Tenant ID, Region)"""
    stmt = select(Tenant).where(Tenant.id == UUID(tenant_id))
    result = await db.execute(stmt)
    tenant = result.scalar_one_or_none()
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    
    # Generate customer_id if it doesn't exist
    if not tenant.customer_id:
        tenant.customer_id = str(uuid4())
        await db.flush()
    
    # Resolve the *actual* region from the Azure Storage Account holding this
    # tenant's backups. Try in order:
    #  1. Dynamic ARM lookup of the account's location (authoritative, but
    #     requires the service principal to have Reader on the account).
    #  2. AZURE_BACKUP_REGION env setting — the region the admin configured.
    #  3. Legacy tenant.storage_region coarse code (DB column).
    from shared.azure_region import format_azure_region, get_storage_account_region

    region_name: Optional[str] = None
    try:
        account_name = settings.AZURE_STORAGE_ACCOUNT_NAME
        if account_name:
            region_code = await get_storage_account_region(account_name)
            region_name = format_azure_region(region_code)
    except Exception:
        region_name = None

    if not region_name:
        region_name = format_azure_region(settings.AZURE_BACKUP_REGION)

    if not region_name:
        legacy_region_map = {
            "AU": "Australia", "US": "United States", "EU": "Europe",
            "UK": "United Kingdom", "CA": "Canada", "DE": "Germany",
            "FR": "France", "JP": "Japan", "IN": "India", "BR": "Brazil",
        }
        fallback_code = tenant.storage_region or "US"
        region_name = legacy_region_map.get(fallback_code, fallback_code)

    return TenantInfoResponse(
        customerId=tenant.customer_id,
        tenantId=tenant.external_tenant_id or tenant_id,
        region=region_name,
    )


@app.get("/api/v1/tenants/{tenant_id}/usage-report")
async def download_usage_report(tenant_id: str, db: AsyncSession = Depends(get_db)):
    """Download usage report as CSV for the tenant"""
    stmt = select(Tenant).where(Tenant.id == UUID(tenant_id))
    result = await db.execute(stmt)
    tenant = result.scalar_one_or_none()
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    
    # Get all resources for this tenant
    resource_stmt = select(Resource).where(Resource.tenant_id == UUID(tenant_id))
    resource_result = await db.execute(resource_stmt)
    resources = resource_result.scalars().all()
    
    # Get SLA policies for mapping
    policy_stmt = select(SlaPolicy).where(SlaPolicy.tenant_id == UUID(tenant_id))
    policy_result = await db.execute(policy_stmt)
    policies = {str(p.id): p.name for p in policy_result.scalars().all()}
    
    # Generate dates for the last 12 days (including today)
    today = datetime.now(timezone.utc).replace(tzinfo=None)
    dates = [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(11, -1, -1)]
    
    # Map resource types to human-readable kinds
    type_map = {
        ResourceType.MAILBOX: "User",
        ResourceType.SHARED_MAILBOX: "Shared Mailbox",
        ResourceType.ONEDRIVE: "OneDrive",
        ResourceType.SHAREPOINT_SITE: "SharePoint site",
        ResourceType.TEAMS_CHANNEL: "Team Channel",
        ResourceType.TEAMS_CHAT: "Teams Chat",
        ResourceType.ENTRA_USER: "User",
        ResourceType.ENTRA_GROUP: "Microsoft 365 group",
        ResourceType.ENTRA_APP: "App",
        ResourceType.ENTRA_DEVICE: "Device",
    }
    
    # Create CSV in memory
    output = io.StringIO()
    writer = csv.writer(output)
    
    # Header row with report date
    writer.writerow(["Report date:", today.strftime("%b %d %Y")])
    writer.writerow([])  # Empty row
    
    # Column headers
    headers = [
        "Resource ID",
        "Resource name",
        "Resource kind",
        "SLA",
        "Is active",
        "Backup Size, GB (current)",
    ] + dates
    writer.writerow(headers)
    
    # Data rows
    for resource in resources:
        # Determine resource kind
        resource_kind = type_map.get(resource.type, "Unknown")
        
        # Determine SLA
        sla_name = "Not protected"
        if resource.sla_policy_id:
            sla_name = policies.get(str(resource.sla_policy_id), "Manual")
        
        # Determine active status
        is_active = "active" if resource.status == ResourceStatus.ACTIVE else "archived"
        
        # Calculate backup size in GB
        backup_size_gb = resource.storage_bytes / (1024 ** 3) if resource.storage_bytes else 0.0
        
        # Row data
        row = [
            str(resource.id),
            resource.display_name,
            resource_kind,
            sla_name,
            is_active,
            f"{backup_size_gb:.1f}",
        ] + [f"{backup_size_gb:.1f}" for _ in dates]  # Same size for all dates (simplified)
        
        writer.writerow(row)
    
    # Get CSV content
    csv_content = output.getvalue()
    output.close()
    
    # Generate filename
    filename = f"{tenant.display_name.replace(' ', '_')}_report_{today.strftime('%b-%d-%Y')}.csv"
    
    return Response(
        content=csv_content,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


# ──────────────────────────────────────────────────────────────────────
# Tenant Secrets — reusable credentials + KMS-key references
# ──────────────────────────────────────────────────────────────────────
#
# UI surfaces that consume these:
#   • Azure DB Recover modal — picks a SQL_SERVER_LOGIN / POSTGRESQL_LOGIN
#     secret as the destination server credential.
#   • Settings → Secrets — manages AES_256_KEY / external-KMS references
#     for backup-data encryption.
#
# Encrypted payload (password / key material) is stored via
# shared.security.encrypt_secret and never returned to the frontend —
# the list endpoint only exposes non-sensitive metadata + hints.
import base64 as _b64
import json as _json
from shared.models import TenantSecret
from shared.security import encrypt_secret


def _secret_to_response(s: TenantSecret) -> dict:
    """Shape returned to the UI — hides the encrypted payload."""
    return {
        "id": str(s.id),
        "tenantId": str(s.tenant_id),
        "type": s.type,
        "name": s.name,
        "description": s.description or "",
        "metadata": s.metadata_hints or {},
        "isDefault": bool(s.is_default),
        "createdAt": s.created_at.isoformat() if s.created_at else None,
        "updatedAt": s.updated_at.isoformat() if s.updated_at else None,
    }


@app.get("/api/v1/tenants/{tenant_id}/secrets")
async def list_tenant_secrets(
    tenant_id: str,
    type: Optional[str] = Query(None, description="Filter by secret type"),
    db: AsyncSession = Depends(get_db),
):
    """Return every secret for a tenant (optionally filtered by type)."""
    tenant_uuid = UUID(tenant_id)
    stmt = select(TenantSecret).where(TenantSecret.tenant_id == tenant_uuid)
    if type:
        stmt = stmt.where(TenantSecret.type == type)
    stmt = stmt.order_by(TenantSecret.created_at.desc())
    rows = (await db.execute(stmt)).scalars().all()
    return {"items": [_secret_to_response(s) for s in rows]}


@app.post("/api/v1/tenants/{tenant_id}/secrets")
async def create_tenant_secret(
    tenant_id: str,
    body: dict,
    db: AsyncSession = Depends(get_db),
):
    """Create a new tenant secret. `payload` is encrypted before persist."""
    tenant_uuid = UUID(tenant_id)
    stype = (body.get("type") or "").strip()
    if not stype:
        raise HTTPException(status_code=400, detail="`type` is required")
    name = (body.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="`name` is required")

    payload = body.get("payload") or {}
    enc_blob = None
    if payload:
        raw = _json.dumps(payload, default=str)
        enc_blob = _b64.b64encode(encrypt_secret(raw)).decode("ascii")

    # Single-default-per-type invariant.
    if bool(body.get("isDefault")):
        await db.execute(
            TenantSecret.__table__.update()
            .where(TenantSecret.tenant_id == tenant_uuid)
            .where(TenantSecret.type == stype)
            .values(is_default=False)
        )

    row = TenantSecret(
        tenant_id=tenant_uuid,
        type=stype,
        name=name,
        description=(body.get("description") or None),
        metadata_hints=(body.get("metadata") or {}),
        encrypted_payload=enc_blob,
        is_default=bool(body.get("isDefault")),
    )
    db.add(row)
    await db.commit()
    await db.refresh(row)
    return _secret_to_response(row)


@app.get("/api/v1/tenants/{tenant_id}/secrets/{secret_id}")
async def get_tenant_secret(
    tenant_id: str, secret_id: str,
    db: AsyncSession = Depends(get_db),
):
    row = await db.get(TenantSecret, UUID(secret_id))
    if not row or str(row.tenant_id) != tenant_id:
        raise HTTPException(status_code=404, detail="Secret not found")
    return _secret_to_response(row)


@app.delete("/api/v1/tenants/{tenant_id}/secrets/{secret_id}", status_code=204)
async def delete_tenant_secret(
    tenant_id: str, secret_id: str,
    db: AsyncSession = Depends(get_db),
):
    row = await db.get(TenantSecret, UUID(secret_id))
    if not row or str(row.tenant_id) != tenant_id:
        raise HTTPException(status_code=404, detail="Secret not found")
    await db.delete(row)
    await db.commit()


# ──────────────────────────────────────────────────────────────────────
# Azure restore options — feed the destination dropdowns in the Azure
# DB Recover modal. Both the tenant domain and the cascading
# subscription/RG/location/server lists are discovered LIVE from
# Microsoft Graph + Azure ARM each time the modal opens. Nothing is
# persisted in the DB. Per-process caches (60s) keep modal opens
# snappy while still reflecting fresh state.
# ──────────────────────────────────────────────────────────────────────

import asyncio
import time
import httpx

# in-process cache — { external_tenant_id: (domain, expires_at_unix) }
_TENANT_DOMAIN_CACHE: dict[str, tuple[str, float]] = {}
_TENANT_DOMAIN_TTL = 3600

# Caches keyed on the full filter combination so each cascading step
# only re-fetches what's actually changed.
_AZURE_OPTIONS_CACHE: dict[tuple, tuple[dict, float]] = {}
_AZURE_OPTIONS_TTL = 60

# Static Azure region catalogue, extracted from
# https://learn.microsoft.com/en-us/azure/reliability/regions-list
# Used for the Location dropdown so the user can pick any region the
# subscription is eligible for, not just regions that currently host a
# server. Sorted alphabetically by displayName at endpoint time.
AZURE_REGIONS: list[dict] = [
    {"name": "eastus",             "displayName": "East US"},
    {"name": "eastus2",            "displayName": "East US 2"},
    {"name": "westus",             "displayName": "West US"},
    {"name": "westus2",            "displayName": "West US 2"},
    {"name": "westus3",            "displayName": "West US 3"},
    {"name": "centralus",          "displayName": "Central US"},
    {"name": "northcentralus",     "displayName": "North Central US"},
    {"name": "southcentralus",     "displayName": "South Central US"},
    {"name": "westcentralus",      "displayName": "West Central US"},
    {"name": "canadacentral",      "displayName": "Canada Central"},
    {"name": "canadaeast",         "displayName": "Canada East"},
    {"name": "brazilsouth",        "displayName": "Brazil South"},
    {"name": "brazilsoutheast",    "displayName": "Brazil Southeast"},
    {"name": "mexicocentral",      "displayName": "Mexico Central"},
    {"name": "chilecentral",       "displayName": "Chile Central"},
    {"name": "northeurope",        "displayName": "North Europe"},
    {"name": "westeurope",         "displayName": "West Europe"},
    {"name": "uksouth",            "displayName": "UK South"},
    {"name": "ukwest",             "displayName": "UK West"},
    {"name": "francecentral",      "displayName": "France Central"},
    {"name": "francesouth",        "displayName": "France South"},
    {"name": "germanywestcentral", "displayName": "Germany West Central"},
    {"name": "germanynorth",       "displayName": "Germany North"},
    {"name": "switzerlandnorth",   "displayName": "Switzerland North"},
    {"name": "switzerlandwest",    "displayName": "Switzerland West"},
    {"name": "norwayeast",         "displayName": "Norway East"},
    {"name": "norwaywest",         "displayName": "Norway West"},
    {"name": "swedencentral",      "displayName": "Sweden Central"},
    {"name": "polandcentral",      "displayName": "Poland Central"},
    {"name": "italynorth",         "displayName": "Italy North"},
    {"name": "spaincentral",       "displayName": "Spain Central"},
    {"name": "austriaeast",        "displayName": "Austria East"},
    {"name": "belgiumcentral",     "displayName": "Belgium Central"},
    {"name": "denmarkeast",        "displayName": "Denmark East"},
    {"name": "uaenorth",           "displayName": "UAE North"},
    {"name": "uaecentral",         "displayName": "UAE Central"},
    {"name": "qatarcentral",       "displayName": "Qatar Central"},
    {"name": "israelcentral",      "displayName": "Israel Central"},
    {"name": "southafricanorth",   "displayName": "South Africa North"},
    {"name": "southafricawest",    "displayName": "South Africa West"},
    {"name": "centralindia",       "displayName": "Central India"},
    {"name": "southindia",         "displayName": "South India"},
    {"name": "westindia",          "displayName": "West India"},
    {"name": "jioindiacentral",    "displayName": "Jio India Central"},
    {"name": "jioindiawest",       "displayName": "Jio India West"},
    {"name": "eastasia",           "displayName": "East Asia"},
    {"name": "southeastasia",      "displayName": "Southeast Asia"},
    {"name": "japaneast",          "displayName": "Japan East"},
    {"name": "japanwest",          "displayName": "Japan West"},
    {"name": "koreacentral",       "displayName": "Korea Central"},
    {"name": "koreasouth",         "displayName": "Korea South"},
    {"name": "australiaeast",      "displayName": "Australia East"},
    {"name": "australiasoutheast", "displayName": "Australia Southeast"},
    {"name": "australiacentral",   "displayName": "Australia Central"},
    {"name": "australiacentral2",  "displayName": "Australia Central 2"},
    {"name": "newzealandnorth",    "displayName": "New Zealand North"},
    {"name": "indonesiacentral",   "displayName": "Indonesia Central"},
    {"name": "malaysiawest",       "displayName": "Malaysia West"},
    {"name": "taiwannorth",        "displayName": "Taiwan North"},
]


async def _acquire_token(tenant_external_id: str, scope: str) -> Optional[str]:
    """OAuth2 client_credentials against the customer's Azure AD tenant
    using the platform's multi-tenant SP. Returns None on any failure
    so callers can degrade gracefully."""
    client_id = settings.EFFECTIVE_ARM_CLIENT_ID or settings.MICROSOFT_CLIENT_ID
    client_secret = settings.EFFECTIVE_ARM_CLIENT_SECRET or settings.MICROSOFT_CLIENT_SECRET
    if not (client_id and client_secret and tenant_external_id):
        return None
    try:
        async with httpx.AsyncClient(timeout=10) as cli:
            r = await cli.post(
                f"https://login.microsoftonline.com/{tenant_external_id}/oauth2/v2.0/token",
                data={
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "scope": scope,
                    "grant_type": "client_credentials",
                },
            )
            if r.status_code != 200:
                return None
            return r.json().get("access_token")
    except Exception:
        return None


async def _fetch_tenant_domain(tenant_external_id: str) -> str:
    """Default verified domain via Graph /organization. Empty string on
    failure so the UI can fall back to the tenant GUID."""
    if not tenant_external_id:
        return ""
    cached = _TENANT_DOMAIN_CACHE.get(tenant_external_id)
    if cached and cached[1] > time.time():
        return cached[0]

    token = await _acquire_token(tenant_external_id, "https://graph.microsoft.com/.default")
    if not token:
        return ""
    try:
        async with httpx.AsyncClient(timeout=10) as cli:
            r = await cli.get(
                "https://graph.microsoft.com/v1.0/organization",
                headers={"Authorization": f"Bearer {token}"},
            )
            if r.status_code != 200:
                return ""
            orgs = r.json().get("value", [])
            if not orgs:
                return ""
            domains = orgs[0].get("verifiedDomains", [])
            default = next((d.get("name") for d in domains if d.get("isDefault")), None)
            domain = default or (domains[0].get("name") if domains else "")
            _TENANT_DOMAIN_CACHE[tenant_external_id] = (domain or "", time.time() + _TENANT_DOMAIN_TTL)
            return domain or ""
    except Exception:
        return ""


@app.get("/api/v1/azure/tenants")
async def list_azure_tenants(db: AsyncSession = Depends(get_db)):
    """Every AZURE tenant the user can restore into, with the verified
    default domain discovered live from Graph (cached 1h)."""
    stmt = select(Tenant).where(Tenant.type == TenantType.AZURE)
    rows = (await db.execute(stmt)).scalars().all()

    domains = await asyncio.gather(
        *[_fetch_tenant_domain(t.external_tenant_id or "") for t in rows],
        return_exceptions=False,
    )

    return {
        "items": [
            {
                "id": str(t.id),
                "externalTenantId": t.external_tenant_id or "",
                "displayName": t.display_name,
                "domain": dom,
            }
            for t, dom in zip(rows, domains)
        ]
    }


async def _arm_get(token: str, url: str) -> Optional[dict]:
    try:
        async with httpx.AsyncClient(timeout=15) as cli:
            r = await cli.get(url, headers={"Authorization": f"Bearer {token}"})
            if r.status_code != 200:
                return None
            return r.json()
    except Exception:
        return None


def _rg_from_id(arm_id: str) -> str:
    # /subscriptions/<sub>/resourceGroups/<rg>/providers/...
    parts = (arm_id or "").split("/")
    try:
        i = parts.index("resourceGroups")
        return parts[i + 1]
    except (ValueError, IndexError):
        return ""


@app.get("/api/v1/azure/tenants/{tenant_id}/options")
async def azure_restore_options(
    tenant_id: str,
    dbType: Optional[str] = Query(None, description="sql | postgresql — filter the server list"),
    subscription: Optional[str] = Query(None, description="Filter RGs / servers to this subscription"),
    resourceGroup: Optional[str] = Query(None, description="Filter servers to this RG (requires subscription)"),
    location: Optional[str] = Query(None, description="Filter servers to this Azure region"),
    db: AsyncSession = Depends(get_db),
):
    """Live discovery against Azure ARM for the destination dropdowns.
    Lists every subscription the platform SP can see in the tenant,
    then enumerates SQL or PostgreSQL servers across them and projects
    the (subscription, resourceGroup, location, server) tuples."""
    tenant = await db.get(Tenant, UUID(tenant_id))
    if not tenant or tenant.type != TenantType.AZURE:
        raise HTTPException(status_code=404, detail="Tenant not found")
    if not tenant.external_tenant_id:
        raise HTTPException(status_code=400, detail="Tenant missing external_tenant_id")

    sub_filter = (subscription or "").strip()
    rg_filter = (resourceGroup or "").strip()
    loc_filter = (location or "").strip().lower()

    cache_key = (tenant.external_tenant_id, dbType or "all", sub_filter, rg_filter, loc_filter)
    cached = _AZURE_OPTIONS_CACHE.get(cache_key)
    if cached and cached[1] > time.time():
        return cached[0]

    token = await _acquire_token(tenant.external_tenant_id, "https://management.azure.com/.default")
    if not token:
        raise HTTPException(status_code=502, detail="Failed to acquire ARM token for tenant")

    # Subscriptions: always the full list visible to the SP — never
    # filtered, since the user uses this dropdown to pick which sub to
    # restore into.
    subs_payload = await _arm_get(token, "https://management.azure.com/subscriptions?api-version=2022-12-01")
    raw_subs = (subs_payload or {}).get("value", []) or []
    subs_out: list[dict] = []
    seen_sub_ids: set[str] = set()
    for s in raw_subs:
        sid = s.get("subscriptionId")
        if not sid or sid in seen_sub_ids:
            continue
        seen_sub_ids.add(sid)
        subs_out.append({"id": sid, "displayName": s.get("displayName") or sid})
    subs_out.sort(key=lambda x: (x["displayName"] or "").lower())
    all_sub_ids = [s["id"] for s in subs_out]
    sub_ids = [sub_filter] if sub_filter else all_sub_ids

    # Resource groups: when a subscription is provided we list the sub's
    # actual RGs straight from ARM (so empty RGs are still selectable).
    # Without a subscription we fall back to the union of RGs that
    # currently host a matching server — better than returning nothing.
    rgs: set[str] = set()
    if sub_filter:
        rg_payload = await _arm_get(
            token,
            f"https://management.azure.com/subscriptions/{sub_filter}/resourcegroups?api-version=2021-04-01",
        )
        for rg in (rg_payload or {}).get("value", []) or []:
            if rg.get("name"):
                rgs.add(rg["name"])

    # Server provider paths per dbType.
    if dbType == "sql":
        provider_paths = [("Microsoft.Sql/servers", "2023-08-01-preview")]
    elif dbType == "postgresql":
        provider_paths = [
            ("Microsoft.DBforPostgreSQL/flexibleServers", "2023-06-01-preview"),
            ("Microsoft.DBforPostgreSQL/servers", "2017-12-01"),
        ]
    else:
        provider_paths = [
            ("Microsoft.Sql/servers", "2023-08-01-preview"),
            ("Microsoft.DBforPostgreSQL/flexibleServers", "2023-06-01-preview"),
            ("Microsoft.DBforPostgreSQL/servers", "2017-12-01"),
        ]

    async def _list_servers_in_sub(sub: str, provider: str, api: str) -> list[dict]:
        url = f"https://management.azure.com/subscriptions/{sub}/providers/{provider}?api-version={api}"
        data = await _arm_get(token, url)
        return (data or {}).get("value", []) or []

    server_tasks = [
        _list_servers_in_sub(sub, p, api)
        for sub in sub_ids for (p, api) in provider_paths
    ]
    server_lists = await asyncio.gather(*server_tasks, return_exceptions=True)

    # Servers: returned as `{name, resourceGroup, location}` so the
    # Recover modal can auto-fill the RG field once the user picks a
    # server. Filtered by subscription + location only — the RG field
    # in the modal is "where the new DB will land", not a constraint
    # on which server to pick; narrowing by RG too would hide servers
    # the user is trying to restore into.
    server_objs: list[dict] = []
    seen_server_keys: set[str] = set()
    rgs_from_servers: set[str] = set()
    for payload in server_lists:
        if isinstance(payload, Exception) or not payload:
            continue
        for srv in payload:
            srv_id = srv.get("id", "")
            srv_rg_actual = _rg_from_id(srv_id)
            srv_loc = (srv.get("location") or "").lower()
            if loc_filter and srv_loc != loc_filter:
                continue
            name = srv.get("name")
            if not name:
                continue
            key = f"{srv_rg_actual.lower()}/{name}"
            if key in seen_server_keys:
                continue
            seen_server_keys.add(key)
            server_objs.append({
                "name": name,
                "resourceGroup": srv_rg_actual,
                "location": srv_loc,
            })
            if srv_rg_actual:
                rgs_from_servers.add(srv_rg_actual)
    if not sub_filter:
        rgs = rgs_from_servers
    server_objs.sort(key=lambda s: s["name"].lower())

    result = {
        "subscriptions": subs_out,
        "resourceGroups": sorted(rgs),
        # Locations are the full Azure catalogue — independent of which
        # regions currently host a server, since the user is creating a
        # NEW database and could land it anywhere their sub allows.
        "locations": sorted(AZURE_REGIONS, key=lambda x: x["displayName"].lower()),
        "servers": server_objs,
    }
    _AZURE_OPTIONS_CACHE[cache_key] = (result, time.time() + _AZURE_OPTIONS_TTL)
    return result
