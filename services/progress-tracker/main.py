"""
Progress Tracking Service - Real-time Backup Progress
Port: 8011

Responsibilities:
- Track per-resource backup progress (bytes/items processed vs total)
- Pre-scan resources to estimate total size before backup
- Provide REST API for progress queries
- Provide SSE stream for real-time progress updates
"""
import asyncio
import uuid
from datetime import datetime
from typing import Dict, Optional
from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
import json

from shared.database import async_session_factory, get_db
from shared.models import Job, Resource, Snapshot, JobStatus
from shared.config import settings

app = FastAPI(title="Progress Tracking Service", version="1.0.0")
from shared.auth_enforcement import AuthMiddleware  # Ops/Data Check 17
app.add_middleware(AuthMiddleware, service_name="progress-tracker", enforce_tenant_scope=True)

# In-memory progress tracker (also persisted to DB)
# Key: resource_id, Value: progress dict
progress_cache: Dict[str, dict] = {}

# SSE subscribers for real-time updates
sse_subscribers: Dict[str, list] = {}


@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "progress-tracker"}


@app.get("/api/v1/progress/resource/{resource_id}")
async def get_resource_progress(resource_id: str):
    """
    Get backup progress for a specific resource.
    
    Response:
    {
        "resource_id": "uuid",
        "status": "RUNNING" | "QUEUED" | "COMPLETED" | "FAILED",
        "progress_pct": 65,
        "total_bytes": 10737418240,
        "processed_bytes": 6979321856,
        "total_items": 5000,
        "processed_items": 3250,
        "last_backup_at": "2026-04-10T10:00:00",
        "job_id": "uuid",
        "started_at": "2026-04-10T10:00:05",
        "eta_seconds": 120
    }
    """
    # Check cache first
    if resource_id in progress_cache:
        cached = progress_cache[resource_id]
        # Also fetch latest job info from DB
        async with async_session_factory() as db:
            job = await _get_latest_job(db, resource_id)
            if job:
                cached["status"] = job.status.value if hasattr(job.status, 'value') else str(job.status)
                cached["job_id"] = str(job.id)
                cached["progress_pct"] = job.progress_pct or cached.get("progress_pct", 0)
                cached["processed_items"] = job.items_processed or cached.get("processed_items", 0)
                cached["processed_bytes"] = job.bytes_processed or cached.get("processed_bytes", 0)
                cached["started_at"] = job.created_at.isoformat() if job.created_at else None
        return cached

    # Fetch from DB
    async with async_session_factory() as db:
        return await _compute_resource_progress(db, resource_id)


@app.get("/api/v1/progress/resources")
async def get_all_progress(tenant_id: Optional[str] = Query(None)):
    """Get active (QUEUED/RUNNING) job progress per resource, optionally filtered by tenant."""
    async with async_session_factory() as db:
        # Only join resources that have an active job to avoid computing idle resources
        stmt = (
            select(Resource, Job)
            .join(Job, Resource.id == Job.resource_id)
            .where(Job.status.in_([JobStatus.QUEUED, JobStatus.RUNNING]))
            .order_by(Job.created_at.desc())
        )

        if tenant_id:
            stmt = stmt.where(Resource.tenant_id == uuid.UUID(tenant_id))

        result = await db.execute(stmt)
        rows = result.all()

        # Deduplicate: keep only the latest active job per resource
        latest_jobs: Dict[str, tuple] = {}
        for resource, job in rows:
            rid = str(resource.id)
            if rid not in latest_jobs:
                latest_jobs[rid] = (resource, job)

        progresses = []
        for rid, (resource, job) in latest_jobs.items():
            progress = await _compute_resource_progress(db, rid, resource, job)
            progresses.append(progress)

        return {"resources": progresses, "total": len(progresses)}


@app.get("/api/v1/progress/resource/{resource_id}/stream")
async def stream_resource_progress(resource_id: str):
    """
    SSE stream for real-time progress updates.
    Client connects and receives progress updates as they happen.
    """
    async def event_generator():
        # Send initial state
        initial = await get_resource_progress(resource_id)
        yield f"data: {json.dumps(initial)}\n\n"

        # Subscribe to updates
        subscribers = sse_subscribers.setdefault(resource_id, [])
        queue: asyncio.Queue = asyncio.Queue()
        subscribers.append(queue)

        try:
            while True:
                # Poll DB for updates every 2 seconds
                async with async_session_factory() as db:
                    job = await _get_latest_job(db, resource_id)
                    if job:
                        progress = await _compute_resource_progress(db, resource_id, job=job)
                        yield f"data: {json.dumps(progress)}\n\n"

                        # If job is done, stop streaming
                        status = job.status.value if hasattr(job.status, 'value') else str(job.status)
                        if status in ("COMPLETED", "FAILED", "CANCELLED"):
                            yield f"data: {json.dumps({'done': True})}\n\n"
                            break

                await asyncio.sleep(2)
        finally:
            if queue in subscribers:
                subscribers.remove(queue)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.post("/api/v1/progress/update")
async def update_progress(request: dict):
    """
    Internal endpoint called by backup-worker to update progress.
    
    Body:
    {
        "resource_id": "uuid",
        "job_id": "uuid",
        "status": "RUNNING",
        "progress_pct": 65,
        "total_bytes": 10737418240,
        "processed_bytes": 6979321856,
        "total_items": 5000,
        "processed_items": 3250
    }
    """
    resource_id = request.get("resource_id")
    if not resource_id:
        raise HTTPException(status_code=400, detail="resource_id required")

    # Update cache
    progress_cache[resource_id] = {
        "resource_id": resource_id,
        "status": request.get("status", "RUNNING"),
        "progress_pct": request.get("progress_pct", 0),
        "total_bytes": request.get("total_bytes", 0),
        "processed_bytes": request.get("processed_bytes", 0),
        "total_items": request.get("total_items", 0),
        "processed_items": request.get("processed_items", 0),
        "updated_at": datetime.utcnow().isoformat(),
    }

    # Update job in DB
    job_id = request.get("job_id")
    if job_id:
        async with async_session_factory() as db:
            job = await db.get(Job, uuid.UUID(job_id))
            if job:
                job.progress_pct = request.get("progress_pct", job.progress_pct)
                job.items_processed = request.get("processed_items", job.items_processed)
                job.bytes_processed = request.get("processed_bytes", job.bytes_processed)
                status_val = request.get("status")
                if status_val:
                    try:
                        new_status = JobStatus(status_val)
                        # Never downgrade a terminal status back to a non-terminal one
                        terminal = {JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED}
                        if job.status not in terminal:
                            job.status = new_status
                    except ValueError:
                        pass
                await db.commit()

    return {"status": "updated"}


@app.post("/api/v1/progress/estimate/{resource_id}")
async def estimate_resource_size(resource_id: str):
    """
    Pre-scan a resource to estimate total backup size.
    This is called before backup starts to establish a baseline.
    """
    async with async_session_factory() as db:
        resource = await db.get(Resource, uuid.UUID(resource_id))
        if not resource:
            raise HTTPException(status_code=404, detail="Resource not found")

        estimate = await _estimate_size(db, resource)

        # Cache the estimate
        progress_cache.setdefault(resource_id, {})
        progress_cache[resource_id]["total_bytes"] = estimate.get("total_bytes", 0)
        progress_cache[resource_id]["total_items"] = estimate.get("total_items", 0)
        progress_cache[resource_id]["estimated_at"] = datetime.utcnow().isoformat()

        return estimate


async def _get_latest_job(db: AsyncSession, resource_id: str):
    """Get the latest job for a resource"""
    stmt = select(Job).where(
        Job.resource_id == uuid.UUID(resource_id)
    ).order_by(Job.created_at.desc()).limit(1)
    result = await db.execute(stmt)
    return result.scalar_one_or_none()


async def _compute_resource_progress(
    db: AsyncSession,
    resource_id: str,
    resource: Optional[Resource] = None,
    job: Optional[Job] = None
):
    """Compute progress for a resource"""
    if not job:
        job = await _get_latest_job(db, resource_id)

    if not resource:
        resource = await db.get(Resource, uuid.UUID(resource_id))

    # Get cached estimate if available
    cached = progress_cache.get(resource_id, {})
    total_items = cached.get("total_items", 0)

    status = "IDLE"
    progress_pct = 0
    processed_items = 0
    data_backed_up = 0
    total_data = 0
    job_id = None
    started_at = None

    if job:
        status = job.status.value if hasattr(job.status, 'value') else str(job.status)
        progress_pct = job.progress_pct or 0
        processed_items = job.items_processed or 0
        data_backed_up = job.bytes_processed or 0
        # total_data: prefer job.result["total_bytes"], then cache, then resource.storage_bytes
        total_data = (job.result.get("total_bytes", 0) if job.result else 0)
        job_id = str(job.id)
        started_at = job.created_at.isoformat() if job.created_at else None

    if not total_data:
        total_data = cached.get("total_bytes", 0)
    if not total_data and resource:
        total_data = resource.storage_bytes or 0

    # Compute progress_pct using data_backed_up/total_data formula
    if total_data > 0 and data_backed_up > 0:
        progress_pct = min(100, int((data_backed_up / total_data) * 100))
    elif total_items > 0 and processed_items > 0:
        progress_pct = min(100, int((processed_items / total_items) * 100))

    # Calculate ETA if running
    eta_seconds = None
    if status == "RUNNING" and progress_pct > 0 and started_at:
        try:
            start_time = datetime.fromisoformat(started_at)
            elapsed = (datetime.utcnow() - start_time).total_seconds()
            if elapsed > 0:
                eta_seconds = (elapsed / progress_pct) * (100 - progress_pct)
        except Exception:
            pass

    return {
        "resource_id": resource_id,
        "resource_name": resource.display_name if resource else None,
        "resource_type": resource.type.value if resource and hasattr(resource.type, 'value') else None,
        "status": status,
        "progress_pct": progress_pct,
        "data_backed_up": data_backed_up,
        "total_data": total_data,
        "total_items": total_items,
        "processed_items": processed_items,
        "last_backup_at": resource.last_backup_at.isoformat() if resource and resource.last_backup_at else None,
        "last_backup_status": resource.last_backup_status if resource else None,
        "job_id": job_id,
        "started_at": started_at,
        "eta_seconds": int(eta_seconds) if eta_seconds else None,
    }


async def _estimate_size(db: AsyncSession, resource: Resource) -> dict:
    """
    Estimate total backup size for a resource.
    In production: calls Graph API to get actual sizes.
    For now: uses resource.storage_bytes as estimate.
    """
    resource_type = resource.type.value if hasattr(resource.type, 'value') else resource.type

    return {
        "resource_id": str(resource.id),
        "resource_type": resource_type,
        "total_bytes": resource.storage_bytes or 0,
        "total_items": 0,  # Would require Graph API call to determine
        "estimated": True,
        "note": "Estimate based on resource.storage_bytes. Pre-scan via Graph API would be more accurate.",
    }
