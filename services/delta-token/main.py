"""
Delta Token Service - Centralized Delta Token Management
Port: 8010

Responsibilities:
- Store and retrieve delta tokens for all resource types
- Support per-folder delta tokens for Exchange
- Support per-resource delta tokens for OneDrive, SharePoint, Entra ID
- Invalidate tokens when full sync is required
- Track delta token history for debugging
"""
import hashlib
import logging
import secrets
import time
import uuid
from datetime import datetime
from typing import Dict, Optional, List
from fastapi import Depends, FastAPI, Header, HTTPException, Request, status
from pydantic import BaseModel
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from redis.asyncio import Redis

from shared.database import async_session_factory
from shared.models import Resource, Tenant, Snapshot
from shared.config import settings

app = FastAPI(title="Delta Token Service", version="2.0.0")

# Dedicated audit logger for invalidation events. Stays distinct from the
# default uvicorn / app logger so the audit trail is easy to ship to a SIEM
# without being drowned out by request access logs.
audit_log = logging.getLogger("delta_token.audit")

# Per-resource invalidation rate limit. Force-full-sync is destructive (one
# call can amplify into thousands of Graph API requests on the next backup),
# so we cap how often the same resource can be invalidated. The limit lives
# in Redis so it spans replicas; falls open if Redis isn't configured.
INVALIDATION_MIN_INTERVAL_SECONDS = 60


def require_internal_api_key(
    x_internal_api_key: Optional[str] = Header(default=None, alias="X-Internal-Api-Key"),
) -> None:
    """Reject any request that doesn't carry the internal-services shared secret.

    Delta tokens are tenant-scoped Microsoft Graph sync credentials — leaking or
    poisoning them lets an attacker exfiltrate incremental sync context or force
    expensive full resyncs. Fail closed when the secret isn't configured so a
    misconfigured deploy can't silently expose the routes.
    """
    expected = settings.INTERNAL_API_KEY
    if not expected:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="INTERNAL_API_KEY not configured",
        )
    if not x_internal_api_key or not secrets.compare_digest(
        x_internal_api_key, expected
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing internal API key",
        )

# Redis for fast token caching
redis_client: Optional[Redis] = None


class DeltaTokenRequest(BaseModel):
    """Request to save a delta token"""
    resource_id: str
    folder_id: Optional[str] = None
    delta_token: str
    timestamp: Optional[str] = None


class DeltaTokenResponse(BaseModel):
    """Delta token response"""
    resource_id: str
    folder_id: Optional[str]
    delta_token: Optional[str]
    last_updated: Optional[str]


class DeltaTokenHistory(BaseModel):
    """Delta token history entry"""
    snapshot_id: str
    delta_token: str
    created_at: str
    resource_type: str


@app.on_event("startup")
async def startup():
    """Initialize Redis connection"""
    from shared import core_metrics
    core_metrics.init()
    global redis_client

    if settings.REDIS_ENABLED:
        try:
            redis_client = Redis(
                host=settings.REDIS_HOST,
                port=settings.REDIS_PORT,
                db=settings.REDIS_DB,
                decode_responses=True
            )
            await redis_client.ping()
            print("[DELTA-TOKEN] Redis connected for token caching")
        except Exception as e:
            print(f"[DELTA-TOKEN] Redis connection failed: {e}. Running without cache.")
            redis_client = None
    
    print("[DELTA-TOKEN] Delta Token Service initialized")


@app.on_event("shutdown")
async def shutdown():
    """Cleanup"""
    if redis_client:
        await redis_client.close()


@app.get("/health")
async def health_check():
    """Health check"""
    return {"status": "healthy", "service": "delta-token"}


@app.get(
    "/delta-token/{resource_id}",
    response_model=DeltaTokenResponse,
    dependencies=[Depends(require_internal_api_key)],
)
async def get_delta_token(resource_id: str, folder_id: Optional[str] = None):
    """
    Get delta token for a resource
    
    Lookup order:
    1. Redis cache (fastest)
    2. Latest snapshot delta_token (database)
    3. Tenant graph_delta_tokens JSONB (fallback)
    """
    # Try Redis cache first
    cache_key = build_cache_key(resource_id, folder_id)
    if redis_client:
        cached_token = await redis_client.get(cache_key)
        if cached_token:
            return DeltaTokenResponse(
                resource_id=resource_id,
                folder_id=folder_id,
                delta_token=cached_token,
                last_updated=None
            )
    
    # Fallback to database - get latest snapshot
    async with async_session_factory() as session:
        snapshot_result = await session.execute(
            select(Snapshot)
            .where(Snapshot.resource_id == uuid.UUID(resource_id))
            .where(Snapshot.delta_token.isnot(None))
            .order_by(Snapshot.started_at.desc())
            .limit(1)
        )
        snapshot = snapshot_result.scalars().first()
        
        if snapshot and snapshot.delta_token:
            return DeltaTokenResponse(
                resource_id=resource_id,
                folder_id=folder_id,
                delta_token=snapshot.delta_token,
                last_updated=snapshot.started_at.isoformat() if snapshot.started_at else None
            )
    
    # No token found
    return DeltaTokenResponse(
        resource_id=resource_id,
        folder_id=folder_id,
        delta_token=None,
        last_updated=None
    )


@app.post("/delta-token", dependencies=[Depends(require_internal_api_key)])
async def save_delta_token(request: DeltaTokenRequest):
    """
    Save delta token after successful backup
    
    Saves to:
    1. Redis cache (for fast lookup)
    2. Snapshot record (persistent storage)
    """
    resource_id = request.resource_id
    folder_id = request.folder_id
    delta_token = request.delta_token
    
    # Save to Redis cache (30-day TTL)
    if redis_client:
        cache_key = build_cache_key(resource_id, folder_id)
        await redis_client.setex(cache_key, 30 * 24 * 3600, delta_token)  # 30 days
    
    # Note: Snapshot record is updated by backup-worker during completion
    # This endpoint is for out-of-band token updates
    
    return {
        "status": "saved",
        "resource_id": resource_id,
        "folder_id": folder_id,
    }


@app.delete(
    "/delta-token/{resource_id}",
    dependencies=[Depends(require_internal_api_key)],
)
async def invalidate_delta_token(
    resource_id: str,
    request: Request,
    folder_id: Optional[str] = None,
    confirm: Optional[str] = None,
    x_internal_api_key: Optional[str] = Header(default=None, alias="X-Internal-Api-Key"),
):
    """
    Invalidate delta token (forces full sync on next backup)

    Use cases:
    - Delta token corruption detected
    - Schema mismatch
    - Manual full sync requested

    Requires `?confirm=force-full-sync` to acknowledge the cost — a full
    Graph API resync of a large mailbox / SharePoint library can burn
    significant API quota. Loops that hit every resource still need to pass
    the flag per call, which makes accidental bulk invalidation harder.
    Per-resource rate-limited (one invalidation per
    INVALIDATION_MIN_INTERVAL_SECONDS) so a malicious caller who somehow
    obtained the internal API key can't spray-invalidate every tenant.
    """
    # Validate UUID before doing anything else — guards against `resource_id`
    # injection patterns leaking into Redis keys / log lines.
    try:
        resource_uuid = uuid.UUID(resource_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="resource_id must be a UUID",
        )

    if confirm != "force-full-sync":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "Confirmation required. Pass ?confirm=force-full-sync to "
                "acknowledge that this will trigger a full Graph API resync."
            ),
        )

    # Caller fingerprint for the audit log: don't log the API key itself
    # (it'd end up in log aggregators), but a hash prefix makes it possible
    # to correlate which credential is being used for invalidation storms.
    api_key_fp = (
        hashlib.sha256(x_internal_api_key.encode()).hexdigest()[:12]
        if x_internal_api_key
        else "none"
    )
    client_ip = request.client.host if request.client else "unknown"

    # Per-resource rate limit. Sliding-window simulated via SETEX: the key
    # exists for INVALIDATION_MIN_INTERVAL_SECONDS after the first hit, and
    # any second hit within that window is rejected. Falls open when Redis
    # isn't configured (legacy single-process dev / Redis-disabled deploys).
    rate_key = f"delta_token_rl:{resource_id}:{folder_id or '*'}"
    if redis_client:
        # SET … NX EX returns None when the key already existed (rate-limited).
        first = await redis_client.set(
            rate_key, "1", ex=INVALIDATION_MIN_INTERVAL_SECONDS, nx=True
        )
        if not first:
            audit_log.warning(
                "delta_token.invalidate.rate_limited "
                "resource_id=%s folder_id=%s caller_ip=%s api_key_fp=%s",
                resource_id, folder_id, client_ip, api_key_fp,
            )
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=(
                    f"Resource invalidated within the last "
                    f"{INVALIDATION_MIN_INTERVAL_SECONDS}s. Try again later."
                ),
            )

    audit_log.info(
        "delta_token.invalidate "
        "resource_id=%s folder_id=%s caller_ip=%s api_key_fp=%s",
        resource_id, folder_id, client_ip, api_key_fp,
    )

    # Remove from Redis cache.
    if redis_client:
        cache_key = build_cache_key(resource_id, folder_id)
        await redis_client.delete(cache_key)

    # Clear from latest snapshot.
    async with async_session_factory() as session:
        snapshot_result = await session.execute(
            select(Snapshot)
            .where(Snapshot.resource_id == resource_uuid)
            .order_by(Snapshot.started_at.desc())
            .limit(1)
        )
        snapshot = snapshot_result.scalars().first()

        if snapshot:
            snapshot.delta_token = None
            await session.commit()

    return {"status": "invalidated", "resource_id": resource_id}


@app.get(
    "/delta-token/history/{resource_id}",
    dependencies=[Depends(require_internal_api_key)],
)
async def get_delta_token_history(resource_id: str, limit: int = 10):
    """Get delta token history for a resource"""
    async with async_session_factory() as session:
        snapshots_result = await session.execute(
            select(Snapshot, Resource)
            .join(Resource, Snapshot.resource_id == Resource.id)
            .where(Snapshot.resource_id == uuid.UUID(resource_id))
            .where(Snapshot.delta_token.isnot(None))
            .order_by(Snapshot.started_at.desc())
            .limit(limit)
        )
        
        history = []
        for snapshot, resource in snapshots_result.all():
            history.append(DeltaTokenHistory(
                snapshot_id=str(snapshot.id),
                delta_token=snapshot.delta_token,
                created_at=snapshot.started_at.isoformat() if snapshot.started_at else None,
                resource_type=resource.type.value,
            ))
        
        return {"history": history, "total": len(history)}


@app.get("/delta-tokens/bulk", dependencies=[Depends(require_internal_api_key)])
async def get_bulk_delta_tokens(resource_ids: str):
    """
    Get delta tokens for multiple resources at once
    
    Query param: resource_ids (comma-separated list of UUIDs)
    """
    id_list = [rid.strip() for rid in resource_ids.split(",")]
    
    async with async_session_factory() as session:
        resources_result = await session.execute(
            select(Resource, Snapshot)
            .outerjoin(
                Snapshot,
                Resource.id == Snapshot.resource_id
            )
            .where(Resource.id.in_([uuid.UUID(rid) for rid in id_list]))
            .order_by(Snapshot.started_at.desc())
        )
        
        tokens = {}
        for resource, snapshot in resources_result.all():
            if resource.id not in tokens:
                tokens[str(resource.id)] = {
                    "resource_id": str(resource.id),
                    "resource_type": resource.type.value,
                    "delta_token": snapshot.delta_token if snapshot else None,
                }
        
        return {"tokens": list(tokens.values())}


def build_cache_key(resource_id: str, folder_id: Optional[str] = None) -> str:
    """Build Redis cache key"""
    if folder_id:
        return f"delta_token:{resource_id}:{folder_id}"
    return f"delta_token:{resource_id}"


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8010)
