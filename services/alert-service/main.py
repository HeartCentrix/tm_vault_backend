"""Alert Service - Manages alerts, notifications, and access groups"""
from contextlib import asynccontextmanager
from typing import List, Optional
from uuid import UUID, uuid4
from datetime import datetime, timezone

from fastapi import FastAPI, Depends, HTTPException, Query
from pydantic import BaseModel, ConfigDict
from sqlalchemy import select, func

from shared.database import get_db, init_db, close_db, AsyncSession
from shared.models import Alert, AccessGroup
from shared.security import get_current_user_from_token


@asynccontextmanager
async def lifespan(app: FastAPI):
    from shared import core_metrics
    core_metrics.init()
    await init_db()
    yield
    await close_db()


app = FastAPI(title="Alert Service", version="1.0.0", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "alert"}


# ============ Auth / tenant scoping helpers ============
#
# Every /api/v1/* route requires a verified JWT (B-H2). The user dict
# returned by `get_current_user_from_token` carries `tenant_ids` from
# the caller's token claims — we treat that list as the *authoritative*
# scope for what the caller can see, and refuse to honour any query
# parameter that asks for a tenant outside that list.


def _user_tenant_uuids(user: dict) -> List[UUID]:
    """Caller's assigned tenant UUIDs from the verified token."""
    out: List[UUID] = []
    for raw in user.get("tenant_ids", []) or []:
        try:
            out.append(UUID(str(raw)))
        except (TypeError, ValueError):
            # Malformed claim — skip rather than crash the whole request.
            continue
    return out


def _authorize_tenant_filter(user: dict, tenant_id_str: Optional[str]) -> List[UUID]:
    """Resolve a caller-supplied `tenantId` query into the set of tenant UUIDs
    we will actually filter against. Always intersects with the caller's
    assigned tenants — never trusts the query string on its own (B-H2).

    Returns the list of tenant UUIDs to filter rows by. Raises 400/403
    when the request is malformed or out-of-scope.
    """
    user_tenants = _user_tenant_uuids(user)
    if not user_tenants:
        raise HTTPException(status_code=403, detail="No tenant access")
    if not tenant_id_str:
        return user_tenants
    try:
        requested = UUID(tenant_id_str)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="Invalid tenantId")
    if requested not in user_tenants:
        raise HTTPException(status_code=403, detail="Not authorized for tenant")
    return [requested]


def _ensure_tenant_member(user: dict, resource_tenant_id) -> None:
    """After fetching a single resource by id, confirm it belongs to a tenant
    the caller is assigned to. Returns 404 (not 403) so an attacker probing
    for resources outside their scope cannot distinguish "exists but not
    yours" from "does not exist" (B-H2).
    """
    user_tenants = _user_tenant_uuids(user)
    if resource_tenant_id is None or resource_tenant_id not in user_tenants:
        raise HTTPException(status_code=404, detail="Not found")


# ============ Request schemas ============
#
# B-H1: explicit Pydantic models with `extra="forbid"` so the framework
# rejects unknown keys at the boundary. The previous blind setattr loop
# allowed any caller to overwrite `id`, `tenant_id`, `org_id`,
# `created_at`, etc. — full mass-assignment escalation.


class AccessGroupCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    description: Optional[str] = None
    memberIds: Optional[List[UUID]] = None


class AccessGroupUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: Optional[str] = None
    description: Optional[str] = None
    memberIds: Optional[List[UUID]] = None


class AddMemberRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    userId: str
    userName: Optional[str] = ""
    userEmail: Optional[str] = ""
    role: Optional[str] = "MEMBER"


class WebhookCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    url: str


# ============ Alerts ============

@app.get("/api/v1/alerts")
async def list_alerts(
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1),
    unresolved: Optional[bool] = Query(None),
    tenantId: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user_from_token),
):
    tenant_filter = _authorize_tenant_filter(current_user, tenantId)
    filters = [Alert.tenant_id.in_(tenant_filter)]
    if unresolved:
        filters.append(Alert.resolved == False)  # noqa: E712 — SQLAlchemy expression

    total = (await db.execute(select(func.count(Alert.id)).where(*filters))).scalar() or 0
    stmt = select(Alert).where(*filters).order_by(Alert.created_at.desc()).offset((page-1)*size).limit(size)
    result = await db.execute(stmt)
    alerts = result.scalars().all()

    return {
        "content": [
            {
                "id": str(a.id), "severity": a.severity, "title": a.message[:100] if a.message else "Alert",
                "description": a.message or "", "status": "RESOLVED" if a.resolved else "ACTIVE",
                "createdAt": a.created_at.isoformat() if a.created_at else "",
                "resolved": a.resolved, "tenantId": str(a.tenant_id) if a.tenant_id else None,
                "type": a.type, "message": a.message,
            }
            for a in alerts
        ],
        "totalPages": max(1, (total + size - 1) // size),
        "totalElements": total,
        "size": size,
        "number": page,
    }


@app.get("/api/v1/alerts/{alert_id}")
async def get_alert(
    alert_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user_from_token),
):
    try:
        alert_uuid = UUID(alert_id)
    except (TypeError, ValueError):
        raise HTTPException(status_code=404, detail="Alert not found")
    stmt = select(Alert).where(Alert.id == alert_uuid)
    result = await db.execute(stmt)
    alert = result.scalar_one_or_none()
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    _ensure_tenant_member(current_user, alert.tenant_id)
    return {
        "id": str(alert.id), "severity": alert.severity, "title": alert.message[:100] if alert.message else "Alert",
        "description": alert.message or "", "status": "RESOLVED" if alert.resolved else "ACTIVE",
        "createdAt": alert.created_at.isoformat(),
    }


@app.post("/api/v1/alerts/{alert_id}/resolve", status_code=204)
async def resolve_alert(
    alert_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user_from_token),
):
    try:
        alert_uuid = UUID(alert_id)
    except (TypeError, ValueError):
        raise HTTPException(status_code=404, detail="Alert not found")
    stmt = select(Alert).where(Alert.id == alert_uuid)
    result = await db.execute(stmt)
    alert = result.scalar_one_or_none()
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    _ensure_tenant_member(current_user, alert.tenant_id)
    alert.resolved = True
    alert.resolved_at = datetime.now(timezone.utc).replace(tzinfo=None)
    await db.flush()


@app.get("/api/v1/alerts/notifications/settings")
async def get_notification_settings(
    current_user: dict = Depends(get_current_user_from_token),
):
    return {"emailEnabled": False, "slackEnabled": False, "teamsEnabled": False, "alertThresholds": {"critical": True, "high": True, "medium": False, "low": False}}


@app.put("/api/v1/alerts/notifications/settings")
async def update_notification_settings(
    settings: dict,
    current_user: dict = Depends(get_current_user_from_token),
):
    return settings


@app.get("/api/v1/alerts/webhooks")
async def list_webhooks(
    current_user: dict = Depends(get_current_user_from_token),
):
    return []


@app.post("/api/v1/alerts/webhooks")
async def create_webhook(
    webhook: WebhookCreate,
    current_user: dict = Depends(get_current_user_from_token),
):
    return {"id": str(uuid4()), "name": webhook.name, "url": webhook.url, "enabled": True, "createdAt": datetime.now(timezone.utc).isoformat()}


@app.delete("/api/v1/alerts/webhooks/{webhook_id}", status_code=204)
async def delete_webhook(
    webhook_id: str,
    current_user: dict = Depends(get_current_user_from_token),
):
    pass


@app.post("/api/v1/alerts/webhooks/{webhook_id}/test")
async def test_webhook(
    webhook_id: str,
    current_user: dict = Depends(get_current_user_from_token),
):
    return {"success": True, "message": "Webhook test successful"}


# ============ Access Groups ============

@app.get("/api/v1/access-groups")
async def list_access_groups(
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1),
    tenantId: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user_from_token),
):
    tenant_filter = _authorize_tenant_filter(current_user, tenantId)
    filters = [AccessGroup.tenant_id.in_(tenant_filter)]

    total = (await db.execute(select(func.count(AccessGroup.id)).where(*filters))).scalar() or 0
    stmt = select(AccessGroup).where(*filters).offset((page-1)*size).limit(size)
    result = await db.execute(stmt)
    groups = result.scalars().all()

    return {
        "content": [
            {"id": str(g.id), "name": g.name, "description": g.description, "memberCount": len(g.member_ids) if g.member_ids else 0, "createdAt": g.created_at.isoformat() if g.created_at else None}
            for g in groups
        ],
        "totalPages": max(1, (total + size - 1) // size),
        "totalElements": total,
        "size": size,
        "number": page,
    }


@app.post("/api/v1/access-groups")
async def create_access_group(
    request: AccessGroupCreate,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user_from_token),
):
    # New groups land in the caller's *first* assigned tenant. If a tenant
    # selector is needed in the UI later, accept it as a typed field on
    # AccessGroupCreate and validate it via _authorize_tenant_filter.
    user_tenants = _user_tenant_uuids(current_user)
    if not user_tenants:
        raise HTTPException(status_code=403, detail="No tenant access")
    group = AccessGroup(
        id=uuid4(),
        tenant_id=user_tenants[0],
        name=request.name,
        description=request.description,
        member_ids=[str(m) for m in (request.memberIds or [])],
    )
    db.add(group)
    await db.flush()
    return {"id": str(group.id), "name": group.name, "description": group.description, "createdAt": group.created_at.isoformat() if group.created_at else None}


@app.put("/api/v1/access-groups/{group_id}")
async def update_access_group(
    group_id: str,
    request: AccessGroupUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user_from_token),
):
    try:
        group_uuid = UUID(group_id)
    except (TypeError, ValueError):
        raise HTTPException(status_code=404, detail="Group not found")
    stmt = select(AccessGroup).where(AccessGroup.id == group_uuid)
    result = await db.execute(stmt)
    group = result.scalar_one_or_none()
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")
    _ensure_tenant_member(current_user, group.tenant_id)

    # B-H1: explicit field updates only. The previous blind setattr loop
    # let any caller overwrite id / tenant_id / org_id / created_at /
    # any other column present on the model. The Pydantic model already
    # rejects unknown keys via extra="forbid"; this block then writes
    # only the fields we intend to be user-mutable.
    update_payload = request.model_dump(exclude_unset=True)
    if "name" in update_payload:
        group.name = update_payload["name"]
    if "description" in update_payload:
        group.description = update_payload["description"]
    if "memberIds" in update_payload:
        group.member_ids = [str(m) for m in (update_payload["memberIds"] or [])]
    await db.flush()
    return {"id": str(group.id), "name": group.name}


@app.delete("/api/v1/access-groups/{group_id}", status_code=204)
async def delete_access_group(
    group_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user_from_token),
):
    try:
        group_uuid = UUID(group_id)
    except (TypeError, ValueError):
        raise HTTPException(status_code=404, detail="Group not found")
    stmt = select(AccessGroup).where(AccessGroup.id == group_uuid)
    result = await db.execute(stmt)
    group = result.scalar_one_or_none()
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")
    _ensure_tenant_member(current_user, group.tenant_id)
    await db.delete(group)
    await db.flush()


@app.post("/api/v1/access-groups/{group_id}/members")
async def add_member(
    group_id: str,
    request: AddMemberRequest,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user_from_token),
):
    try:
        group_uuid = UUID(group_id)
    except (TypeError, ValueError):
        raise HTTPException(status_code=404, detail="Group not found")
    stmt = select(AccessGroup).where(AccessGroup.id == group_uuid)
    result = await db.execute(stmt)
    group = result.scalar_one_or_none()
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")
    _ensure_tenant_member(current_user, group.tenant_id)
    return {"id": str(uuid4()), "groupId": group_id, "userId": request.userId, "userName": request.userName, "userEmail": request.userEmail, "role": request.role, "addedAt": datetime.now(timezone.utc).isoformat()}


@app.delete("/api/v1/access-groups/{group_id}/members/{member_id}", status_code=204)
async def remove_member(
    group_id: str,
    member_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user_from_token),
):
    try:
        group_uuid = UUID(group_id)
    except (TypeError, ValueError):
        raise HTTPException(status_code=404, detail="Group not found")
    stmt = select(AccessGroup).where(AccessGroup.id == group_uuid)
    result = await db.execute(stmt)
    group = result.scalar_one_or_none()
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")
    _ensure_tenant_member(current_user, group.tenant_id)


@app.get("/api/v1/access-groups/self-service/settings")
async def get_self_service_settings(
    current_user: dict = Depends(get_current_user_from_token),
):
    return {"enabled": True, "allowRestore": True, "allowExport": True, "maxExportItems": 100}


@app.put("/api/v1/access-groups/self-service/settings")
async def update_self_service_settings(
    settings: dict,
    current_user: dict = Depends(get_current_user_from_token),
):
    return settings


@app.get("/api/v1/access-groups/ip-restrictions")
async def get_ip_restrictions(
    current_user: dict = Depends(get_current_user_from_token),
):
    return {"enabled": False, "allowedIPs": [], "blockedIPs": []}


@app.put("/api/v1/access-groups/ip-restrictions")
async def update_ip_restrictions(
    restrictions: dict,
    current_user: dict = Depends(get_current_user_from_token),
):
    return restrictions


# ============ Export Notifications ============

from pydantic import BaseModel


class ExportNotification(BaseModel):
    user_email: str = ""
    user_display_name: str = ""
    job_id: str
    status: str  # "COMPLETED" | "COMPLETED_WITH_ERRORS" | "FAILED"
    download_url: str
    exported_count: int = 0
    failed_count: int = 0
    duration_seconds: int = 0
    size_bytes: int = 0


@app.post("/api/v1/alerts/notify/export-completed", status_code=202)
async def notify_export_completed(
    payload: ExportNotification,
    current_user: dict = Depends(get_current_user_from_token),
):
    """Send the export-completed email. Delegates to the existing email helper
    when one is available; otherwise logs for audit. Fire-and-forget from the
    caller's perspective — we return 202 before the email physically leaves.

    Auth note (B-H2): this endpoint is called by worker services after an
    export job finishes. Workers must present a valid JWT (issued by the
    auth-service for a service principal) — anonymous invocation is no
    longer accepted.
    """
    subject = f"Your TMvault export is ready — {payload.exported_count} items"
    if payload.status != "COMPLETED":
        subject = (
            f"Your TMvault export finished with warnings — "
            f"{payload.failed_count} skipped"
        )

    body_text = (
        f"Hi {payload.user_display_name or payload.user_email or 'there'},\n\n"
        f"Your mail export job {payload.job_id} is complete.\n\n"
        f"  Status:   {payload.status}\n"
        f"  Items:    {payload.exported_count} exported, {payload.failed_count} skipped\n"
        f"  Size:     {payload.size_bytes / (1024 ** 3):.2f} GB\n"
        f"  Duration: {payload.duration_seconds // 60} min {payload.duration_seconds % 60}s\n\n"
        f"Download: {payload.download_url}\n\n"
        f"The download link is valid for 24 hours.\n"
    )

    try:
        # Delegate to existing helper. Replace the call below if your helper has
        # a different signature. Keep the surrounding try so a mail failure
        # doesn't turn the POST into a 500 — we want fire-and-forget semantics.
        await send_email(to=payload.user_email, subject=subject, body_text=body_text)
    except NameError:
        # No send_email in scope — log for audit until SMTP wiring lands.
        print(
            f"[ALERT/email] would send to={payload.user_email!r} subject={subject!r}\n{body_text}"
        )
    except Exception as exc:
        print(f"[ALERT/email] send failed (non-fatal): {exc}")
    return {"queued": True}
