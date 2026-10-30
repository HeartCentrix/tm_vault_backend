"""API Gateway - Routes requests to microservices"""
import asyncio
import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from starlette.background import BackgroundTask
import httpx

from shared.config import settings
from shared.security import decode_token  # Ops/Data Check 6: verify, don't just detect
from shared.storage.startup import startup_router, shutdown_router

import sys as _sys, pathlib as _pl
_sys.path.insert(0, str(_pl.Path(__file__).parent))  # make `routes` importable
from routes import admin_storage  # noqa: E402


class _SilentPollFilter(logging.Filter):
    """Drop uvicorn.access log lines for requests tagged with
    `_silent=1`. The frontend appends that query param on the Protection
    / Activity auto-refresh fetches so live-progress polling doesn't
    flood docker logs while a backup is in flight."""

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            msg = record.getMessage()
        except Exception:
            return True
        return "_silent=1" not in msg


logging.getLogger("uvicorn.access").addFilter(_SilentPollFilter())

# Service URLs
SERVICES = {
    "auth": settings.AUTH_SERVICE_URL,
    "tenant": settings.TENANT_SERVICE_URL,
    "resource": settings.RESOURCE_SERVICE_URL,
    "job": settings.JOB_SERVICE_URL,
    "snapshot": settings.SNAPSHOT_SERVICE_URL,
    "dashboard": settings.DASHBOARD_SERVICE_URL,
    "alert": settings.ALERT_SERVICE_URL,
    "scheduler": settings.BACKUP_SCHEDULER_URL,
    "report": settings.REPORT_SERVICE_URL,
    "graph-proxy": settings.GRAPH_PROXY_URL,
    "delta-token": settings.DELTA_TOKEN_URL,
    "progress": settings.PROGRESS_TRACKER_URL,
    "audit": settings.AUDIT_SERVICE_URL,
}

GATEWAY_TIMEOUT = httpx.Timeout(connect=10.0, read=60.0, write=60.0, pool=10.0)
GATEWAY_LIMITS = httpx.Limits(max_keepalive_connections=20, max_connections=100)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.http_client = httpx.AsyncClient(timeout=GATEWAY_TIMEOUT, limits=GATEWAY_LIMITS)
    await startup_router()
    try:
        yield
    finally:
        await shutdown_router()
        await app.state.http_client.aclose()


app = FastAPI(title="TM Vault API Gateway", version="1.0.0", lifespan=lifespan)
app.include_router(admin_storage.router)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    # Content-Disposition carries the server-chosen filename for raw-single
    # OneDrive exports (e.g. Report.xlsx). Without exposing it, fetch()
    # in the browser can't read the header and falls back to a.download
    # which would force a .zip suffix.
    expose_headers=["Content-Disposition", "Content-Length"],
)


async def proxy_request(service: str, path: str, request: Request, timeout: httpx.Timeout = None):
    """Forward request to microservice with retry logic"""
    import asyncio

    service_url = SERVICES.get(service)
    if not service_url:
        raise HTTPException(status_code=502, detail="Service not found")

    url = f"{service_url}{path}"

    # An explicit timeout=None in httpx does NOT mean "use the client
    # default" — it disables connect/read/write/pool timeouts entirely,
    # so one hung downstream pins a pooled connection forever and ~100
    # of them wedge the whole gateway. Fall back to the module default.
    if timeout is None:
        timeout = GATEWAY_TIMEOUT

    # Build headers (forward auth)
    headers = {}
    # Cookie-first: the SPA stores its access token in an HttpOnly cookie, so
    # the browser sends the cookie automatically and the Authorization header
    # from the client is either absent or a stale "Bearer null" left over
    # from in-flight code paths. Translate cookie -> Bearer so upstream
    # services keep their existing HTTPBearer auth dependency unchanged.
    cookie_token = request.cookies.get("access_token")
    incoming_auth = request.headers.get("authorization", "")
    looks_junk = incoming_auth.lower() in (
        "",
        "bearer",
        "bearer ",
        "bearer null",
        "bearer undefined",
    )
    if cookie_token and looks_junk:
        headers["Authorization"] = f"Bearer {cookie_token}"
    elif incoming_auth and not looks_junk:
        headers["Authorization"] = incoming_auth
    elif cookie_token:
        # Both present and the header looks valid — prefer the explicit
        # Authorization header so service-to-service calls aren't overridden.
        headers["Authorization"] = incoming_auth or f"Bearer {cookie_token}"
    if "content-type" in request.headers:
        headers["Content-Type"] = request.headers["content-type"]
    # Forward the Cookie header too so the auth-service can read refresh_token
    # on POST /auth/refresh (cookie-first refresh path).
    if "cookie" in request.headers:
        headers["Cookie"] = request.headers["cookie"]

    # Request bodies here are small (JSON specs); buffering keeps them
    # replayable for the connect-failure retries below.
    body = await request.body()

    # Retry up to 3 times with exponential backoff. Only ConnectError /
    # ConnectTimeout retry for every method — those provably never reached
    # the service. ReadTimeout / RemoteProtocolError can fire AFTER the
    # downstream committed its work (pod rollover closing the socket
    # mid-response), so replaying a POST would duplicate restore/backup/
    # export jobs — double writes into live mailboxes. Idempotent-only.
    idempotent = request.method in ("GET", "HEAD")
    last_error = None
    client: httpx.AsyncClient = request.app.state.http_client
    for attempt in range(3):
        try:
            upstream_request = client.build_request(
                method=request.method,
                url=url,
                headers=headers,
                params=request.query_params,
                content=body,
                timeout=timeout,
            )
            # Stream the upstream body through instead of buffering it —
            # export/download artifacts run to multiple GB, and holding
            # one in memory OOMKills the 512Mi gateway pod.
            response = await client.send(upstream_request, stream=True)

            # Forward response headers the browser actually needs —
            # Content-Disposition is critical for downloads (browser uses
            # it to pick the saved filename). Skip hop-by-hop and
            # connection-managed headers that httpx/uvicorn will compute
            # themselves so we don't double-set them.
            _skip = {
                "content-type",  # set via media_type below
                "transfer-encoding",
                "connection",
                "keep-alive",
                "proxy-authenticate",
                "proxy-authorization",
                "te",
                "trailers",
                "upgrade",
            }
            async def _relay(upstream=response):
                # finally (not just BackgroundTask) so the pooled
                # connection is released even when the client disconnects
                # mid-download or the upstream dies mid-body.
                try:
                    async for chunk in upstream.aiter_raw():
                        yield chunk
                finally:
                    await upstream.aclose()

            proxied = StreamingResponse(
                _relay(),
                status_code=response.status_code,
                media_type=response.headers.get("content-type", "application/json"),
                background=BackgroundTask(response.aclose),
            )
            # multi_items(), not items(): httpx comma-joins repeated
            # headers in items(), which merged auth-service's three
            # Set-Cookie headers into one — the browser then stored only
            # access_token, and every session died unrefreshable at
            # access-token expiry because refresh_token never landed.
            for k, v in response.headers.multi_items():
                if k.lower() not in _skip:
                    proxied.headers.append(k, v)
            return proxied
        except (httpx.ConnectError, httpx.ConnectTimeout) as e:
            last_error = e
            if attempt < 2:
                await asyncio.sleep(1 * (attempt + 1))
        except httpx.ReadTimeout as e:
            last_error = e
            if idempotent and attempt < 1:
                await asyncio.sleep(1)
            else:
                return JSONResponse(
                    status_code=504,
                    content={"detail": f"Service '{service}' timed out"},
                )
        except httpx.RemoteProtocolError as e:
            last_error = e
            if idempotent and attempt < 2:
                await asyncio.sleep(1 * (attempt + 1))
            else:
                break
        except httpx.RequestError as e:
            last_error = e
            break

    # All retries failed
    print(f"[GATEWAY] Failed to reach {service} at {url} after 3 attempts: {last_error}")
    raise HTTPException(status_code=503, detail=f"Service '{service}' unavailable")


# Auth routes
@app.get("/api/v1/auth/microsoft/url")
@app.get("/api/v1/auth/microsoft/datasource/url")
@app.get("/api/v1/auth/azure/datasource/url")
@app.get("/api/v1/auth/power-bi/url")
async def auth_get(request: Request):
    return await proxy_request("auth", request.url.path, request)


@app.post("/api/v1/auth/callback")
@app.post("/api/v1/auth/refresh")
@app.post("/api/v1/auth/logout")
@app.post("/api/v1/auth/microsoft/datasource/callback")
@app.post("/api/v1/auth/azure/datasource/callback")
@app.post("/api/v1/auth/power-bi/callback")
async def auth_post(request: Request):
    return await proxy_request("auth", request.url.path, request)


@app.get("/api/v1/auth/me")
async def auth_me(request: Request):
    return await proxy_request("auth", request.url.path, request)


# Admin Consent status routes (existing datasource APIs handle URL/callback)
@app.get("/api/v1/admin-consent/m365/status")
@app.get("/api/v1/admin-consent/azure/status")
@app.get("/api/v1/admin-consent/power-bi/readiness")
async def admin_consent_status(request: Request):
    return await proxy_request("auth", request.url.path, request)


# Dashboard routes
@app.get("/api/v1/dashboard/{rest:path}")
async def dashboard(request: Request):
    return await proxy_request("dashboard", request.url.path, request)


# Tenant routes
@app.get("/api/v1/tenants")
@app.get("/api/v1/tenants/{tenant_id}")
@app.get("/api/v1/tenants/{tenant_id}/info")
@app.get("/api/v1/tenants/{tenant_id}/usage-report")
@app.post("/api/v1/tenants")
@app.put("/api/v1/tenants/{tenant_id}")
@app.delete("/api/v1/tenants/{tenant_id}")
@app.post("/api/v1/tenants/{tenant_id}/discover")
@app.post("/api/v1/tenants/{tenant_id}/discover-m365")
@app.post("/api/v1/tenants/{tenant_id}/discover-azure")
@app.post("/api/v1/tenants/{tenant_id}/users/{user_resource_id}/discover-content")
@app.post("/api/v1/tenants/{tenant_id}/users/{user_resource_id}/backup")
@app.get("/api/v1/tenants/{tenant_id}/discovery-status")
@app.get("/api/v1/tenants/{tenant_id}/storage-summary")
@app.get("/api/v1/tenants/{tenant_id}/secrets")
@app.post("/api/v1/tenants/{tenant_id}/secrets")
@app.get("/api/v1/tenants/{tenant_id}/secrets/{secret_id}")
@app.delete("/api/v1/tenants/{tenant_id}/secrets/{secret_id}")
@app.get("/api/v1/azure/tenants")
@app.get("/api/v1/azure/tenants/{tenant_id}/options")
@app.post("/api/v1/tenants/{tenant_id}/test-connection")
@app.get("/api/v1/organizations")
@app.get("/api/v1/organizations/{org_id}")
@app.get("/api/v1/encryption/config")
@app.put("/api/v1/encryption/config")
@app.post("/api/v1/encryption/rewrap")
@app.get("/api/v1/settings/app")
@app.put("/api/v1/settings/app")
async def tenant(request: Request):
    return await proxy_request("tenant", request.url.path, request)


# Resource routes
@app.get("/api/v1/resources/with-backups")
async def resources_with_backups(request: Request):
    return await proxy_request("snapshot", request.url.path, request)


@app.get("/api/v1/resources/{resource_id}/subsites")
async def resource_subsites(request: Request):
    # Live SharePoint subsite listing for the Recovery page's Subsites
    # panel. Handled by tenant-service because it has Graph auth wired.
    return await proxy_request("tenant", request.url.path, request)


@app.get("/api/v1/resources")
@app.get("/api/v1/resources/search")
@app.get("/api/v1/resources/by-type")
@app.get("/api/v1/resources/users")
@app.get("/api/v1/resources/{resource_id}")
@app.get("/api/v1/resources/{resource_id}/storage-history")
@app.post("/api/v1/resources/{resource_id}/assign-policy")
@app.post("/api/v1/resources/{resource_id}/unassign-policy")
@app.post("/api/v1/resources/{resource_id}/archive")
@app.post("/api/v1/resources/{resource_id}/unarchive")
@app.delete("/api/v1/resources/{resource_id}")
@app.post("/api/v1/resources/bulk-assign-policy")
@app.post("/api/v1/resources/bulk-archive")
async def resource(request: Request):
    return await proxy_request("resource", request.url.path, request)


# Job routes
@app.get("/api/v1/jobs")
@app.get("/api/v1/jobs/{job_id}")
@app.get("/api/v1/jobs/{job_id}/progress")
@app.post("/api/v1/jobs/{job_id}/cancel")
@app.post("/api/v1/jobs/{job_id}/retry")
@app.get("/api/v1/jobs/{job_id}/logs")
@app.post("/api/v1/backups/trigger")
@app.post("/api/v1/backups/trigger-user/{resource_id}")
@app.post("/api/v1/backups/trigger-bulk")
@app.post("/api/v1/backups/trigger-datasource")
@app.get("/api/v1/dlq/stats")
@app.post("/api/v1/dlq/{dlq_name}/purge")
@app.post("/api/v1/dlq/{dlq_name}/requeue")
async def job(request: Request):
    return await proxy_request("job", request.url.path, request)


# Snapshot routes
@app.get("/api/v1/resources/snapshots/folders")
# Per-content-type browsers — five fixed types replace the old
# /content-types lookup. /mail and /chats are aliases for /emails and
# /messages so legacy callers keep working during the cutover.
@app.get("/api/v1/resources/snapshots/{snapshot_id}/mail")
@app.get("/api/v1/resources/snapshots/{snapshot_id}/onedrive")
@app.get("/api/v1/resources/snapshots/{snapshot_id}/contacts")
@app.get("/api/v1/resources/snapshots/{snapshot_id}/chats")
@app.get("/api/v1/resources/snapshots/{snapshot_id}/chats/groups")
@app.get("/api/v1/resources/snapshots/{snapshot_id}/emails")
@app.get("/api/v1/resources/snapshots/{snapshot_id}/messages")
@app.get("/api/v1/resources/snapshots/{snapshot_id}/calendar")
# Power BI / generic "all items" listing — consumed by the Recovery page's
# Files view for resource kinds outside the five fixed tabs.
@app.get("/api/v1/resources/snapshots/{snapshot_id}/files")
@app.get("/api/v1/resources/snapshots/{snapshot_id}/onedrive/ids")
@app.get("/api/v1/resources/snapshots/{snapshot_id}/azure-db/table")
@app.get("/api/v1/resources/snapshots/{snapshot_id}/azure-db/export")
async def snapshot_folders(request: Request):
    return await proxy_request("snapshot", request.url.path, request)


@app.get("/api/v1/resources/{resource_id}/snapshots")
@app.get("/api/v1/resources/{resource_id}/content-snapshots")
@app.get("/api/v1/resources/{resource_id}/storage-summary")
@app.get("/api/v1/resources/{resource_id}/snapshots/by-date")
@app.get("/api/v1/resources/{resource_id}/snapshots/calendar")
@app.get("/api/v1/resources/snapshots/{snapshot_id}")
@app.get("/api/v1/resources/snapshots/{snapshot_id}/items")
@app.get("/api/v1/resources/snapshots/{snapshot_id}/items/{item_id}/content")
@app.get("/api/v1/resources/snapshots/{snapshot_id}/items/{item_id}/attachments")
@app.get("/api/v1/resources/snapshots/{snapshot_id}/items/{item_id}")
@app.get("/api/v1/resources/snapshots/{snapshot_id}/items/search")
@app.get("/api/v1/resources/snapshots/{snapshot_id}/items/{item_id}/preview")
@app.get("/api/v1/resources/snapshots/{snapshot_id}/diff")
@app.get("/api/v1/resources/{resource_id}/snapshots/{snapshot_id}/items")
@app.get("/api/v1/resources/{resource_id}/snapshots/changes")
# Recovery: search snapshot items
@app.get("/api/v1/resources/{resource_id}/snapshots/search")
# Azure VM live detail — hits ARM on-demand to enrich a SnapshotItem
# with the same fields the Azure Portal shows (IOPS, disk tier, etc).
@app.get("/api/v1/snapshot-items/{item_id}/azure-vm-detail")
# Volumes tab — live file listing via Azure Run Command on the VM.
@app.get("/api/v1/snapshot-items/{item_id}/vm-volume-files")
async def snapshot(request: Request):
    return await proxy_request("snapshot", request.url.path, request)


# Volumes tab download — accepts `{ paths: [...] }` and returns the
# raw file (for single-file requests) or a ZIP preserving folder
# structure (for multi-path or folder requests).
@app.post("/api/v1/snapshot-items/{item_id}/vm-volume-download")
# Client-callable audit emitter for frontend-only downloads (config JSON).
@app.post("/api/v1/snapshot-items/{item_id}/download-audit")
async def vm_volume_download(request: Request):
    return await proxy_request("snapshot", request.url.path, request)


# Restore routes
@app.post("/api/v1/jobs/restore")
@app.post("/api/v1/jobs/restore/mailbox")
@app.post("/api/v1/jobs/restore/onedrive")
@app.post("/api/v1/jobs/restore/sharepoint")
@app.post("/api/v1/jobs/restore/entra-object")
@app.get("/api/v1/jobs/restore/{job_id}/status")
@app.get("/api/v1/jobs/restore/history")
@app.post("/api/v1/jobs/export")
@app.get("/api/v1/jobs/export/{job_id}/status")
@app.get("/api/v1/exports/{job_id}/download")
@app.get("/api/v1/exports/{job_id}/download")
@app.get("/api/v1/jobs/export/{job_id}/download")
async def restore(request: Request):
    return await proxy_request("job", request.url.path, request)


# Chat export routes (v1 — single-thread / per-message, routed to job-service)
@app.post("/api/v1/exports/chat")
@app.post("/api/v1/exports/chat/estimate")
@app.get("/api/v1/exports/chat/{job_id}")
@app.post("/api/v1/exports/chat/{job_id}/cancel")
@app.delete("/api/v1/exports/chat/{job_id}")
async def chat_export(request: Request):
    return await proxy_request("job", request.url.path, request)


# Alert routes
@app.get("/api/v1/alerts")
@app.get("/api/v1/alerts/{alert_id}")
@app.post("/api/v1/alerts/{alert_id}/resolve")
@app.get("/api/v1/alerts/notifications/settings")
@app.put("/api/v1/alerts/notifications/settings")
@app.get("/api/v1/alerts/webhooks")
@app.post("/api/v1/alerts/webhooks")
@app.delete("/api/v1/alerts/webhooks/{webhook_id}")
@app.post("/api/v1/alerts/webhooks/{webhook_id}/test")
async def alert(request: Request):
    return await proxy_request("alert", request.url.path, request)


# SLA Policy routes
@app.get("/api/v1/policies")
@app.get("/api/v1/policies/{policy_id}")
@app.post("/api/v1/policies")
@app.put("/api/v1/policies/{policy_id}")
@app.delete("/api/v1/policies/{policy_id}")
@app.get("/api/v1/policies/{policy_id}/resources")
@app.post("/api/v1/policies/{policy_id}/auto-assign")
# Phase 2 — exclusions hang off a policy
@app.get("/api/v1/policies/{policy_id}/exclusions")
@app.post("/api/v1/policies/{policy_id}/exclusions")
@app.delete("/api/v1/policies/{policy_id}/exclusions/{exclusion_id}")
async def policy(request: Request):
    return await proxy_request("resource", request.url.path, request)


# Phase 2 — resource groups (afi-style auto-protection rules)
@app.get("/api/v1/resource-groups")
@app.post("/api/v1/resource-groups")
@app.get("/api/v1/resource-groups/{group_id}")
@app.put("/api/v1/resource-groups/{group_id}")
@app.delete("/api/v1/resource-groups/{group_id}")
@app.post("/api/v1/resource-groups/{group_id}/policies")
@app.delete("/api/v1/resource-groups/{group_id}/policies/{policy_id}")
async def resource_group(request: Request):
    return await proxy_request("resource", request.url.path, request)


# Report Configuration routes
@app.get("/api/v1/reports/config")
@app.post("/api/v1/reports/config")
@app.put("/api/v1/reports/config")
@app.get("/api/v1/reports/history")
@app.get("/api/v1/reports/history/{report_id}")
@app.post("/api/v1/reports/generate")
@app.post("/api/v1/reports/send-test")
async def report_proxy(request: Request):
    return await proxy_request("report", request.url.path, request,
                               timeout=httpx.Timeout(connect=10.0, read=120.0, write=30.0, pool=10.0))


# Access Control routes
@app.get("/api/v1/access-groups")
@app.post("/api/v1/access-groups")
@app.put("/api/v1/access-groups/{group_id}")
@app.delete("/api/v1/access-groups/{group_id}")
@app.post("/api/v1/access-groups/{group_id}/members")
@app.delete("/api/v1/access-groups/{group_id}/members/{member_id}")
@app.get("/api/v1/access-groups/self-service/settings")
@app.put("/api/v1/access-groups/self-service/settings")
@app.get("/api/v1/access-groups/ip-restrictions")
@app.put("/api/v1/access-groups/ip-restrictions")
async def access_control(request: Request):
    return await proxy_request("alert", request.url.path, request)


# ============ Progress Tracking ============

@app.get("/api/v1/progress/resource/{resource_id}")
@app.get("/api/v1/progress/resources")
@app.post("/api/v1/progress/update")
@app.post("/api/v1/progress/estimate/{resource_id}")
async def progress_proxy(request: Request):
    return await proxy_request("progress", request.url.path, request)


# ============ Audit Log ============

@app.get("/api/v1/activity")
@app.get("/api/v1/activity/export")
@app.get("/api/v1/activity/batches/{batch_id}/children")
@app.get("/api/v1/audit/events")
@app.get("/api/v1/audit/events/{event_id}")
@app.get("/api/v1/audit/risk-signals")
@app.get("/api/v1/audit/resource/{resource_id}")
@app.get("/api/v1/audit/export")
@app.get("/api/v1/audit/actions")
@app.get("/api/v1/audit/stats")
@app.get("/api/v1/audit/siem/stream")
@app.post("/api/v1/audit/siem/webhook")
@app.post("/api/v1/audit/siem/webhook/{webhook_id}/test")
@app.get("/api/v1/audit/graph-apps")
# POST /api/v1/audit/log is deliberately NOT proxied: it writes the audit
# trail from an unvalidated body and is gated on the X-Internal-Api-Key
# shared secret, which proxy_request does not forward. Publishing it here
# only exposed an anonymous audit-forgery endpoint at the ingress; every
# emitter posts to AUDIT_SERVICE_URL in-cluster instead.
@app.post("/api/v1/audit/ingest/graph/{tenant_id}")
async def audit_proxy(request: Request):
    return await proxy_request("audit", request.url.path, request)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "api-gateway"}


# ─── Aggregated application health ──────────────────────────────────────────
_RANK = {"healthy": 0, "degraded": 1, "unhealthy": 2}
# Short server-side cache so probes / the auto-refreshing Health page don't
# fan out to every service on each request.
_HEALTH_CACHE: dict = {"at": 0.0, "key": None, "value": None}
_HEALTH_CACHE_TTL_S = 8.0


def _is_authed(request: Request) -> bool:
    """Return True only for a cryptographically VALID access token.

    Ops/Data Check 6: presence of a cookie or a non-empty ``Authorization``
    header is not proof of identity — a forged ``Bearer x`` must not unlock the
    detailed /api/v1/health payload (queue depths, worker counts). Cookie-first,
    matching the proxy, then a Bearer header; the token is decoded and its
    signature + ``access`` type verified, and any failure falls back to the
    coarse public payload."""
    token = request.cookies.get("access_token")
    if not token:
        auth = request.headers.get("authorization", "")
        if auth[:7].lower() == "bearer ":
            token = auth[7:].strip()
    if not token or token.lower() in ("null", "undefined"):
        return False
    try:
        decode_token(token, expected_type="access")
        return True
    except Exception:
        return False


async def _probe_service(client: httpx.AsyncClient, name: str, base_url: str) -> dict:
    try:
        r = await client.get(f"{base_url}/health", timeout=httpx.Timeout(5.0))
        return {"name": name, "up": r.status_code == 200}
    except Exception as e:
        return {"name": name, "up": False, "error": f"{type(e).__name__}"}


@app.get("/api/v1/health")
async def app_health(request: Request):
    """Application-wide health: downstream service up/down matrix merged with
    dashboard-service's deep subsystem checks (DB, RabbitMQ, queues, workers,
    storage, leases). Overall = worst of all signals.

    Unauthenticated callers get a coarse summary (probe-friendly); an
    authenticated session additionally gets per-service and per-component
    detail."""
    detailed = _is_authed(request)
    now = time.monotonic()
    if (
        _HEALTH_CACHE["value"] is not None
        and _HEALTH_CACHE["key"] == detailed
        and (now - _HEALTH_CACHE["at"]) < _HEALTH_CACHE_TTL_S
    ):
        return _HEALTH_CACHE["value"]

    client: httpx.AsyncClient = request.app.state.http_client

    # 1) downstream service up/down matrix (concurrent)
    probes = await asyncio.gather(*[
        _probe_service(client, name, url) for name, url in SERVICES.items()
    ])
    services = {p["name"]: p for p in probes}
    up = sum(1 for p in probes if p["up"])
    down = [p["name"] for p in probes if not p["up"]]
    svc_status = "healthy" if not down else ("unhealthy" if up == 0 else "degraded")

    # 2) deep subsystem checks from dashboard-service
    components: dict = {}
    deep_status = "healthy"
    try:
        dr = await client.get(
            f"{SERVICES['dashboard']}/health/deep",
            params={"detailed": str(detailed).lower()},
            timeout=httpx.Timeout(12.0),
        )
        dr.raise_for_status()
        deep = dr.json()
        components = deep.get("components", {})
        deep_status = deep.get("status", "degraded")
    except Exception as e:
        deep_status = "unhealthy"
        components = {"subsystems": {"status": "unhealthy", "detail": f"deep check unreachable: {type(e).__name__}"}}

    overall = max([svc_status, deep_status], key=lambda s: _RANK.get(s, 1))

    if detailed:
        payload = {
            "status": overall,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "services": {"up": up, "down": len(down), "down_list": down, "detail": services},
            "components": components,
        }
    else:
        payload = {
            "status": overall,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "services": {"up": up, "down": len(down)},
            "components": {name: {"status": c.get("status", "degraded")} for name, c in components.items()},
        }

    _HEALTH_CACHE.update(at=now, key=detailed, value=payload)
    return payload
