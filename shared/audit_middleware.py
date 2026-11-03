"""Fire-and-forget audit middleware for state-changing HTTP requests.

Every mutating request (POST/PUT/PATCH/DELETE) that reaches a TM Vault service
produces an ``audit_events`` row answering who / when / what / to-which-thing,
so mutation-audit coverage is structural and complete rather than a per-handler
call that a new route can silently forget. Rich domain-specific emits
(``BACKUP_TRIGGERED``, ``RESTORE_TRIGGERED``, ...) stay where they are; this is
the backstop underneath them, and semantic auth events (``LOGIN_SUCCESS`` etc.)
are emitted directly by auth-service — which is why ``/api/v1/auth`` is excluded
here (no duplicate rows).

Why pure ASGI and not ``BaseHTTPMiddleware``: BaseHTTPMiddleware buffers the
response, which breaks streaming downloads (snapshot-service item content) and
Server-Sent Events (storage-toggle event stream). This middleware only reads the
request ``scope`` (method, path, headers) and the response's ``http.response.start``
message (status code); it never touches the request or response body, and it
schedules the audit emit AFTER the response has been delivered, so it adds no
latency to — and can never break — the request it describes.

Loop safety: the emit itself is a POST to ``/api/v1/audit/log`` on audit-service.
That path is in the default exclude list, so audit-service's own copy of this
middleware never re-audits an incoming audit write (which would recurse forever).
"""
from __future__ import annotations

import asyncio
import logging
import re
from typing import Iterable, Optional, Set

from shared import audit

log = logging.getLogger("tmvault.audit.mw")

_MUTATING = {"POST", "PUT", "PATCH", "DELETE"}

# Path prefixes never audited by the generic middleware:
#  - /api/v1/audit/log : the emit target — excluding exactly this path is the
#    recursion guard (a re-entrant emit still hits an excluded path, depth 1).
#    Scoped to /log so audit-service's OWN admin mutations (SIEM webhook,
#    graph-log ingest) are still audited.
#  - /api/v1/auth      : auth-service emits semantic LOGIN_*/LOGOUT/TOKEN_REFRESHED itself
#  - health/probe/metrics endpoints: not state changes, and high-volume
_DEFAULT_EXCLUDES = (
    "/api/v1/audit/log",
    "/api/v1/auth",
    "/health",
    "/healthz",
    "/metrics",
    "/ping",
)

_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)

# Strong refs so create_task'd emits are not garbage-collected mid-flight.
_BG_TASKS: Set["asyncio.Task"] = set()


def _fire(coro) -> None:
    """Schedule a fire-and-forget coroutine, keeping a strong reference.

    If there is no running loop (should never happen inside an ASGI call) the
    coroutine is closed to avoid a 'never awaited' warning."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        coro.close()
        return
    task = loop.create_task(coro)
    _BG_TASKS.add(task)
    task.add_done_callback(_BG_TASKS.discard)


def _template_and_target(path: str):
    """Return (templated_path, resource_type, resource_id, tenant_id).

    Templated path replaces UUID / long-numeric segments with ``{id}`` so the
    audit ``action`` has bounded cardinality (one value per route, not per row).
    resource_id is the first id-shaped segment; resource_type is the segment
    before it; tenant_id is the id that follows a ``tenants`` segment, if any."""
    segs = [s for s in path.split("/") if s]
    templated = []
    resource_id: Optional[str] = None
    resource_type: Optional[str] = None
    tenant_id: Optional[str] = None
    prev_word: Optional[str] = None
    for seg in segs:
        is_id = bool(_UUID_RE.match(seg)) or (seg.isdigit() and len(seg) >= 6)
        if is_id:
            templated.append("{id}")
            if resource_id is None:
                resource_id = seg
                resource_type = prev_word
            if prev_word == "tenants" and tenant_id is None:
                tenant_id = seg
        else:
            templated.append(seg)
            prev_word = seg
    return "/" + "/".join(templated), resource_type, resource_id, tenant_id


def _extract_token(headers: dict) -> Optional[str]:
    """Best-effort access-token extraction from the request headers.

    Cookie-first (the SPA's session lives in an HttpOnly ``access_token``
    cookie), then a ``Bearer`` header (the gateway forwards the cookie as one
    when proxying). Used only to attribute the actor of an audit row — never to
    authorize — so a malformed value simply yields an UNKNOWN actor."""
    cookie = headers.get("cookie", "")
    for part in cookie.split(";"):
        part = part.strip()
        if part.startswith("access_token="):
            val = part[len("access_token="):].strip()
            if val:
                return val
    auth = headers.get("authorization", "")
    if auth[:7].lower() == "bearer ":
        val = auth[7:].strip()
        if val and val.lower() not in ("null", "undefined"):
            return val
    return None


def _actor_from_token(token: Optional[str]):
    """(actor_id, actor_email, org_id, actor_type) — best-effort, never raises."""
    if not token:
        return None, None, None, "UNKNOWN"
    try:
        # Local import: shared.security pulls in jose lazily and some non-API
        # runtimes import this module without it.
        from shared.security import decode_token

        payload = decode_token(token, expected_type="access")
        return (
            payload.get("sub"),
            payload.get("email"),
            payload.get("orgId"),
            "USER",
        )
    except Exception:
        # Present-but-invalid token: still a real actor attempt, just not a
        # verifiable identity. Record that we saw a credential.
        return None, None, None, "UNKNOWN"


class AuditMiddleware:
    """Pure-ASGI middleware emitting one audit event per mutating request."""

    def __init__(
        self,
        app,
        *,
        service_name: str,
        exclude_prefixes: Optional[Iterable[str]] = None,
        extra_excludes: Iterable[str] = (),
    ) -> None:
        self.app = app
        self.service_name = service_name
        # extra_excludes unions with the defaults so a caller can suppress a
        # high-frequency machine endpoint (graph-proxy's /graph passthrough,
        # progress-tracker's per-tick /api/v1/progress writes) without having
        # to restate the whole default list.
        base = exclude_prefixes if exclude_prefixes is not None else _DEFAULT_EXCLUDES
        self.excludes = tuple(base) + tuple(extra_excludes)

    def _excluded(self, path: str) -> bool:
        # Match on a path boundary only — an exact hit or a genuine
        # sub-path. A bare startswith would over-exclude siblings like
        # "/api/v1/authorizations" (prefix "/api/v1/auth") or "/pingback"
        # (prefix "/ping"), silently dropping their audit rows.
        return any(path == p or path.startswith(p + "/") for p in self.excludes)

    async def __call__(self, scope, receive, send):
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return
        method = scope.get("method", "").upper()
        path = scope.get("path", "") or ""
        if method not in _MUTATING or self._excluded(path):
            await self.app(scope, receive, send)
            return

        status_holder = {"code": 0}

        async def send_wrapper(message):
            if message.get("type") == "http.response.start":
                status_holder["code"] = message.get("status", 0)
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        except Exception:
            # Never swallow the app's own error — record it as a failed
            # mutation attempt and re-raise so the normal 500 path runs. If the
            # response had already started (mid-stream failure) keep the status
            # the client actually saw rather than overwriting it with 500.
            _fire(self._emit(scope, method, path, status_holder["code"] or 500))
            raise
        _fire(self._emit(scope, method, path, status_holder["code"]))

    async def _emit(self, scope, method: str, path: str, status: int) -> None:
        try:
            headers = {}
            for k, v in scope.get("headers", []) or []:
                try:
                    headers[k.decode("latin-1").lower()] = v.decode("latin-1")
                except Exception:
                    continue
            token = _extract_token(headers)
            actor_id, actor_email, org_id, actor_type = _actor_from_token(token)
            templated, resource_type, resource_id, tenant_id = _template_and_target(path)
            client = scope.get("client") or (None, None)
            outcome = "SUCCESS" if 0 < status < 400 else "FAILURE"
            await audit.emit_audit_event(
                action=f"{method} {templated}",
                actor_type=actor_type,
                actor_id=actor_id,
                actor_email=actor_email,
                org_id=org_id,
                tenant_id=tenant_id,
                resource_type=resource_type,
                resource_id=resource_id,
                outcome=outcome,
                details={
                    "method": method,
                    "path": path,
                    "status": status,
                    "service": self.service_name,
                    "source_ip": client[0],
                    "via": "audit-middleware",
                },
            )
        except Exception as exc:  # pragma: no cover - defensive
            log.warning("audit middleware emit failed (%s %s): %s", method, path, exc)
