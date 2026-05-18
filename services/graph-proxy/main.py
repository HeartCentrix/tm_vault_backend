"""
Graph Proxy Service - Microsoft Graph API Gateway with Batch Support
Port: 8009

Responsibilities:
- Proxy requests to Microsoft Graph API
- Support $batch endpoint for bulk operations (up to 20 requests per batch)
- Implement adaptive throttling to prevent 429 errors
- Token caching and rotation
- Request metrics and monitoring
"""
import asyncio
import time
import uuid
from datetime import datetime
from typing import Dict, List, Any, Optional
from fastapi import FastAPI, HTTPException, Request, Response
from pydantic import BaseModel
import httpx
from redis.asyncio import Redis

from shared.config import settings

app = FastAPI(title="Graph Proxy Service", version="2.0.0")

# Redis for token caching
redis_client: Optional[Redis] = None

# Throttle manager
throttle_manager = None


class GraphBatchRequest(BaseModel):
    """Graph API batch request model"""
    requests: List[Dict[str, Any]]


class GraphBatchResponse(BaseModel):
    """Graph API batch response model"""
    responses: List[Dict[str, Any]]


class ThrottleState(BaseModel):
    """Track throttle state per tenant"""
    request_count: int = 0
    window_start: float = 0.0
    backoff_until: float = 0.0
    concurrent_requests: int = 0


class AdaptiveThrottleManager:
    """
    Adaptive throttling for Graph API requests
    
    Graph API Limits:
    - 10,000 requests per 5 minutes per tenant
    - 20 concurrent requests
    - Rate limit headers: Retry-After, x-ms-ags-diagnostic
    """
    
    def __init__(self):
        self.tenant_states: Dict[str, ThrottleState] = {}
        self.MAX_REQUESTS_PER_5MIN = 10000
        self.MAX_CONCURRENT = 20
    
    def can_execute(self, tenant_id: str) -> bool:
        """Check if request can be executed"""
        now = time.time()
        
        # Initialize tenant state if needed
        if tenant_id not in self.tenant_states:
            self.tenant_states[tenant_id] = ThrottleState(window_start=now)
        
        state = self.tenant_states[tenant_id]
        
        # Reset counter if 5-minute window passed
        if now - state.window_start > 300:  # 5 minutes
            state.request_count = 0
            state.window_start = now
        
        # Check if in backoff
        if state.backoff_until > now:
            return False
        
        # Check concurrent limit
        if state.concurrent_requests >= self.MAX_CONCURRENT:
            return False
        
        # Check request count
        if state.request_count >= self.MAX_REQUESTS_PER_5MIN:
            return False
        
        # All checks passed
        state.request_count += 1
        state.concurrent_requests += 1
        return True
    
    def record_success(self, tenant_id: str):
        """Record successful request"""
        if tenant_id in self.tenant_states:
            self.tenant_states[tenant_id].concurrent_requests = max(
                0, self.tenant_states[tenant_id].concurrent_requests - 1
            )
    
    def record_throttle(self, tenant_id: str, retry_after: int):
        """Record throttling event and activate backoff"""
        if tenant_id not in self.tenant_states:
            self.tenant_states[tenant_id] = ThrottleState()
        
        self.tenant_states[tenant_id].backoff_until = time.time() + retry_after
        self.tenant_states[tenant_id].concurrent_requests = max(
            0, self.tenant_states[tenant_id].concurrent_requests - 1
        )
    
    def get_stats(self, tenant_id: str) -> Dict:
        """Get throttle stats for tenant"""
        if tenant_id not in self.tenant_states:
            return {"request_count": 0, "window_start": 0}
        
        state = self.tenant_states[tenant_id]
        return {
            "request_count": state.request_count,
            "max_requests": self.MAX_REQUESTS_PER_5MIN,
            "concurrent_requests": state.concurrent_requests,
            "backoff_until": state.backoff_until,
            "window_start": state.window_start,
        }


@app.on_event("startup")
async def startup():
    """Initialize services"""
    from shared import core_metrics
    core_metrics.init()
    global redis_client, throttle_manager

    # Initialize Redis if enabled
    if settings.REDIS_ENABLED:
        try:
            redis_client = Redis(
                host=settings.REDIS_HOST,
                port=settings.REDIS_PORT,
                db=settings.REDIS_DB,
                decode_responses=True
            )
            await redis_client.ping()
            print("[GRAPH-PROXY] Redis connected")
        except Exception as e:
            print(f"[GRAPH-PROXY] Redis connection failed: {e}. Running without cache.")
            redis_client = None
    
    # Initialize throttle manager
    throttle_manager = AdaptiveThrottleManager()
    print("[GRAPH-PROXY] Graph Proxy Service initialized")


@app.on_event("shutdown")
async def shutdown():
    """Cleanup"""
    if redis_client:
        await redis_client.close()


@app.get("/health")
async def health_check():
    """Health check"""
    return {
        "status": "healthy",
        "service": "graph-proxy",
        "timestamp": datetime.utcnow().isoformat()
    }


@app.post("/graph/batch")
async def execute_batch_request(batch_request: GraphBatchRequest, tenant_id: str):
    """
    Execute batch request to Graph API
    
    Graph API $batch endpoint accepts up to 20 requests per batch.
    This endpoint handles throttling and retry logic automatically.
    """
    # Validate batch size
    if len(batch_request.requests) > 20:
        raise HTTPException(
            status_code=400,
            detail="Batch request cannot contain more than 20 requests"
        )
    
    # Check throttle state
    if not throttle_manager.can_execute(tenant_id):
        raise HTTPException(
            status_code=429,
            detail="Throttle limit exceeded. Try again later."
        )
    
    # Get access token
    access_token = await get_graph_token(tenant_id)
    
    # Execute batch request
    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(
            "https://graph.microsoft.com/v1.0/$batch",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            },
            json={"requests": batch_request.requests}
        )
    
    # Handle response
    if response.status_code == 429:
        # Throttled - extract retry-after header
        retry_after = int(response.headers.get("Retry-After", "60"))
        throttle_manager.record_throttle(tenant_id, retry_after)
        
        raise HTTPException(
            status_code=429,
            detail=f"Graph API throttled. Retry after {retry_after} seconds.",
            headers={"Retry-After": str(retry_after)}
        )
    
    if response.status_code == 200:
        throttle_manager.record_success(tenant_id)
        return response.json()
    
    raise HTTPException(
        status_code=response.status_code,
        detail=f"Graph API error: {response.text}"
    )


@app.get("/graph/{path:path}")
async def proxy_get_request(path: str, tenant_id: str, request: Request):
    """Proxy GET request to Graph API with throttling"""
    return await proxy_request("GET", path, tenant_id, dict(request.query_params))


@app.post("/graph/{path:path}")
async def proxy_post_request(path: str, tenant_id: str, request: Request):
    """Proxy POST request to Graph API with throttling"""
    body = await request.json() if request.method == "POST" else None
    return await proxy_request("POST", path, tenant_id, body)


async def proxy_request(method: str, path: str, tenant_id: str, body: Optional[Dict] = None):
    """Generic Graph API proxy with throttling"""
    # Check throttle
    if not throttle_manager.can_execute(tenant_id):
        raise HTTPException(status_code=429, detail="Throttle limit exceeded")
    
    # Get token
    access_token = await get_graph_token(tenant_id)
    
    # Build Graph API URL
    graph_url = f"https://graph.microsoft.com/v1.0/{path}"
    
    # Execute request with retry logic
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.request(
            method=method,
            url=graph_url,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            },
            json=body if method == "POST" else None
        )
    
    # Handle throttling
    if response.status_code == 429:
        retry_after = int(response.headers.get("Retry-After", "60"))
        throttle_manager.record_throttle(tenant_id, retry_after)
        
        # Wait and retry once
        await asyncio.sleep(retry_after)
        
        if not throttle_manager.can_execute(tenant_id):
            raise HTTPException(status_code=429, detail="Still throttled after retry")
        
        response = await client.request(
            method=method,
            url=graph_url,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            },
            json=body if method == "POST" else None
        )
    
    throttle_manager.record_success(tenant_id)
    
    if response.status_code in [200, 201]:
        return response.json()
    
    raise HTTPException(
        status_code=response.status_code,
        detail=f"Graph API error: {response.text}"
    )


async def get_graph_token(tenant_id: str) -> str:
    """
    Get Graph API access token with Redis caching
    
    Token lifecycle:
    1. Check Redis cache
    2. If valid, return cached token
    3. If expired/missing, acquire new token from Microsoft
    4. Cache token in Redis with 5-minute buffer before expiry
    """
    cache_key = f"graph_token:{tenant_id}"
    
    # Check Redis cache
    if redis_client:
        cached = await redis_client.get(cache_key)
        if cached:
            return cached
    
    # Acquire new token
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    
    async with httpx.AsyncClient() as client:
        response = await client.post(
            token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": settings.AZURE_AD_CLIENT_ID,
                "client_secret": settings.AZURE_AD_CLIENT_SECRET,
                "scope": "https://graph.microsoft.com/.default",
            }
        )
        
        if response.status_code != 200:
            raise HTTPException(
                status_code=401,
                detail=f"Failed to acquire Graph token: {response.text}"
            )
        
        token_data = response.json()
        access_token = token_data["access_token"]
        expires_in = token_data.get("expires_in", 3600)
        
        # Cache in Redis (with 5-minute buffer)
        if redis_client:
            cache_ttl = max(300, expires_in - 300)  # At least 5 minutes
            await redis_client.setex(cache_key, cache_ttl, access_token)
    
    return access_token


@app.get("/throttle/stats/{tenant_id}")
async def get_throttle_stats(tenant_id: str):
    """Get throttle statistics for a tenant"""
    stats = throttle_manager.get_stats(tenant_id)
    return stats


@app.get("/throttle/stats")
async def get_all_throttle_stats():
    """Get throttle statistics for all tenants"""
    return {
        tenant_id: throttle_manager.get_stats(tenant_id)
        for tenant_id in throttle_manager.tenant_states
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8009)
