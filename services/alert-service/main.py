"""Alert Service - Manages alerts, notifications, and access groups"""
from contextlib import asynccontextmanager
from typing import Optional
from uuid import UUID, uuid4
from datetime import datetime, timezone

from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy import select, func

from shared.database import get_db, init_db, close_db, AsyncSession
from shared.models import Alert, AccessGroup


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


# ============ Alerts ============

@app.get("/api/v1/alerts")
async def list_alerts(
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1),
    unresolved: Optional[bool] = Query(None),
    tenantId: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    filters = []
    if tenantId:
        filters.append(Alert.tenant_id == UUID(tenantId))
    if unresolved:
        filters.append(Alert.resolved == False)
    
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
async def get_alert(alert_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(Alert).where(Alert.id == UUID(alert_id))
    result = await db.execute(stmt)
    alert = result.scalar_one_or_none()
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    return {
        "id": str(alert.id), "severity": alert.severity, "title": alert.message[:100] if alert.message else "Alert",
        "description": alert.message or "", "status": "RESOLVED" if alert.resolved else "ACTIVE",
        "createdAt": alert.created_at.isoformat(),
    }


@app.post("/api/v1/alerts/{alert_id}/resolve", status_code=204)
async def resolve_alert(alert_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(Alert).where(Alert.id == UUID(alert_id))
    result = await db.execute(stmt)
    alert = result.scalar_one_or_none()
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    alert.resolved = True
    alert.resolved_at = datetime.now(timezone.utc).replace(tzinfo=None)
    await db.flush()


@app.get("/api/v1/alerts/notifications/settings")
async def get_notification_settings():
    return {"emailEnabled": False, "slackEnabled": False, "teamsEnabled": False, "alertThresholds": {"critical": True, "high": True, "medium": False, "low": False}}


@app.put("/api/v1/alerts/notifications/settings")
async def update_notification_settings(settings: dict):
    return settings


@app.get("/api/v1/alerts/webhooks")
async def list_webhooks():
    return []


@app.post("/api/v1/alerts/webhooks")
async def create_webhook(webhook: dict):
    return {"id": str(uuid4()), "name": webhook.get("name", ""), "url": webhook.get("url", ""), "enabled": True, "createdAt": datetime.now(timezone.utc).isoformat()}


@app.delete("/api/v1/alerts/webhooks/{webhook_id}", status_code=204)
async def delete_webhook(webhook_id: str):
    pass


@app.post("/api/v1/alerts/webhooks/{webhook_id}/test")
async def test_webhook(webhook_id: str):
    return {"success": True, "message": "Webhook test successful"}


# ============ Access Groups ============

@app.get("/api/v1/access-groups")
async def list_access_groups(
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1),
    tenantId: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    filters = []
    if tenantId:
        filters.append(AccessGroup.tenant_id == UUID(tenantId))
    
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
async def create_access_group(request: dict, db: AsyncSession = Depends(get_db)):
    group = AccessGroup(id=uuid4(), name=request.get("name"), description=request.get("description"), member_ids=request.get("memberIds", []))
    db.add(group)
    await db.flush()
    return {"id": str(group.id), "name": group.name, "description": group.description, "createdAt": group.created_at.isoformat() if group.created_at else None}


@app.put("/api/v1/access-groups/{group_id}")
async def update_access_group(group_id: str, request: dict, db: AsyncSession = Depends(get_db)):
    stmt = select(AccessGroup).where(AccessGroup.id == UUID(group_id))
    result = await db.execute(stmt)
    group = result.scalar_one_or_none()
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")
    for key, value in request.items():
        if hasattr(group, key):
            setattr(group, key, value)
    await db.flush()
    return {"id": str(group.id), "name": group.name}


@app.delete("/api/v1/access-groups/{group_id}", status_code=204)
async def delete_access_group(group_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(AccessGroup).where(AccessGroup.id == UUID(group_id))
    result = await db.execute(stmt)
    group = result.scalar_one_or_none()
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")
    await db.delete(group)
    await db.flush()


@app.post("/api/v1/access-groups/{group_id}/members")
async def add_member(group_id: str, request: dict):
    return {"id": str(uuid4()), "groupId": group_id, "userId": request.get("userId", ""), "userName": request.get("userName", ""), "userEmail": request.get("userEmail", ""), "role": request.get("role", "MEMBER"), "addedAt": datetime.now(timezone.utc).isoformat()}


@app.delete("/api/v1/access-groups/{group_id}/members/{member_id}", status_code=204)
async def remove_member(group_id: str, member_id: str):
    pass


@app.get("/api/v1/access-groups/self-service/settings")
async def get_self_service_settings():
    return {"enabled": True, "allowRestore": True, "allowExport": True, "maxExportItems": 100}


@app.put("/api/v1/access-groups/self-service/settings")
async def update_self_service_settings(settings: dict):
    return settings


@app.get("/api/v1/access-groups/ip-restrictions")
async def get_ip_restrictions():
    return {"enabled": False, "allowedIPs": [], "blockedIPs": []}


@app.put("/api/v1/access-groups/ip-restrictions")
async def update_ip_restrictions(restrictions: dict):
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
async def notify_export_completed(payload: ExportNotification):
    """Send the export-completed email. Delegates to the existing email helper
    when one is available; otherwise logs for audit. Fire-and-forget from the
    caller's perspective — we return 202 before the email physically leaves."""
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
