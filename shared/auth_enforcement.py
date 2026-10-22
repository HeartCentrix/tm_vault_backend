"""Service-tier authentication enforcement (Ops/Data Check 17).

The data tier historically "trusted the gateway": api-gateway forwarded the
session cookie/JWT without verifying it, and snapshot-/resource-/tenant-service
(and others) enforced no auth at all — so the whole backup vault was readable,
and destructible, by an unauthenticated caller who could reach the ingress.

This module closes that at the service tier with a pure-ASGI ``AuthMiddleware``
that rejects any request lacking a valid principal, plus small FastAPI
dependencies (``verified_user`` / ``require_admin``) that per-handler authz
builds on. Pure ASGI (never buffers the body) so streaming downloads and SSE
are unaffected — same rationale as shared/audit_middleware.py.

A valid principal is EITHER:
  * a cryptographically-valid access-token JWT (cookie ``access_token`` or a
    ``Bearer`` header) — the user path; the gateway forwards it, and it is the
    only way normal SPA traffic reaches a service; OR
  * a matching ``X-Internal-Api-Key`` — the machine path, for service-to-service
    and worker-to-service calls (audit ingest, etc.). Internal callers are
    trusted and treated as an admin-equivalent SYSTEM principal.

Public paths (health/probe/metrics) are exempt — a kubelet probe and the
gateway's own health matrix present no credential.

The verified principal is stashed on a private scope key (``tmvault_user``),
NOT ``scope['state']`` (which Starlette resets per request), so a downstream
``Depends(verified_user)`` can read it without re-decoding.
"""
from __future__ import annotations

import json
import logging
import os
import re
import secrets as _secrets
from typing import Iterable, Optional

log = logging.getLogger("tmvault.authz")

_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)


def _tenant_ids_in_request(path: str, query_string) -> set:
    """Collect tenant UUIDs named in the request — a UUID after a ``tenants``
    path segment, and any ``tenantId``/``tenant_id`` query param — so the
    middleware can enforce the caller only names tenants it is entitled to."""
    from urllib.parse import parse_qs

    out = set()
    segs = [s for s in path.split("/") if s]
    for i, seg in enumerate(segs):
        if seg == "tenants" and i + 1 < len(segs) and _UUID_RE.match(segs[i + 1]):
            out.add(segs[i + 1])
    try:
        qs = (query_string.decode("latin-1")
              if isinstance(query_string, (bytes, bytearray))
              else str(query_string or ""))
        for k, vals in parse_qs(qs).items():
            if k.lower() in ("tenantid", "tenant_id"):
                for v in vals:
                    if _UUID_RE.match(v):
                        out.add(v)
    except Exception:
        pass
    return out

_PUBLIC_DEFAULT = ("/health", "/healthz", "/metrics", "/ping")
_SCOPE_KEY = "tmvault_user"

# Roles that may perform administrative / destructive operations. Mirrors
# services/job-service/export_authz.py:ADMIN_ROLES (kept in sync; that module
# is service-local and cannot be imported cross-service).
ADMIN_ROLES = frozenset({"SUPER_ADMIN", "ORG_ADMIN", "TENANT_ADMIN", "tenant_admin"})


def _internal_key() -> str:
    return os.getenv("INTERNAL_API_KEY", "")


def _extract_token(headers: dict) -> Optional[str]:
    cookie = headers.get("cookie", "")
    for part in cookie.split(";"):
        part = part.strip()
        if part.startswith("access_token="):
            v = part[len("access_token="):].strip()
            if v:
                return v
    auth = headers.get("authorization", "")
    if auth[:7].lower() == "bearer ":
        v = auth[7:].strip()
        if v and v.lower() not in ("null", "undefined"):
            return v
    return None


def _authenticate(headers: dict) -> Optional[dict]:
    """Return a principal dict, or None if the request is not authenticated."""
    # Machine path: a matching internal key is an admin-equivalent SYSTEM principal.
    key = _internal_key()
    supplied = headers.get("x-internal-api-key", "")
    if key and supplied and _secrets.compare_digest(supplied, key):
        return {
            "id": None,
            "email": "internal-service",
            "roles": ["SUPER_ADMIN"],
            "org_id": None,
            "tenant_ids": [],
            "_principal": "internal",
        }
    # User path: a validly-signed access token.
    token = _extract_token(headers)
    if not token:
        return None
    try:
        from shared.security import decode_token

        payload = decode_token(token, expected_type="access")
    except Exception:
        return None
    sub = payload.get("sub")
    if not sub:
        return None
    return {
        "id": sub,
        "email": payload.get("email"),
        "roles": payload.get("roles", []) or [],
        "org_id": payload.get("orgId"),
        "tenant_ids": payload.get("tenantIds", []) or [],
        "_principal": "user",
    }


class AuthMiddleware:
    """Reject any non-public request that carries no valid principal."""

    #: HTTP methods considered state-changing.
    MUTATING = frozenset({"POST", "PUT", "PATCH", "DELETE"})

    def __init__(
        self,
        app,
        *,
        service_name: str,
        public_prefixes: Optional[Iterable[str]] = None,
        extra_public: Iterable[str] = (),
        require_admin_for_mutations: bool = False,
        enforce_tenant_scope: bool = False,
    ) -> None:
        self.app = app
        self.service_name = service_name
        base = public_prefixes if public_prefixes is not None else _PUBLIC_DEFAULT
        self.public = tuple(base) + tuple(extra_public)
        # When True, every non-public POST/PUT/PATCH/DELETE additionally
        # requires an admin principal. Used on tenant-/resource-service, whose
        # mutations (tenant + secret + policy + settings CRUD) are ALL
        # administrative — this closes the privilege-escalation angle without a
        # per-handler edit, while leaving GET reads open to any authenticated
        # user.
        self.require_admin_for_mutations = require_admin_for_mutations
        # When True, a request that names a tenant (a UUID after a ``tenants``
        # path segment, or a ``tenantId``/``tenant_id`` query param) must name
        # one the caller is entitled to — closes the authenticated cross-tenant
        # IDOR on the resource/tenant/dashboard read routes. No-op on requests
        # that name no tenant. Admin and internal principals are exempt.
        self.enforce_tenant_scope = enforce_tenant_scope

    def _is_public(self, path: str) -> bool:
        return any(path == p or path.startswith(p + "/") for p in self.public)

    async def __call__(self, scope, receive, send):
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return
        path = scope.get("path", "") or ""
        if self._is_public(path):
            await self.app(scope, receive, send)
            return
        headers = {}
        for k, v in scope.get("headers", []) or []:
            try:
                headers[k.decode("latin-1").lower()] = v.decode("latin-1")
            except Exception:
                continue
        principal = _authenticate(headers)
        if principal is None:
            await self._reject(send, 401, "Authentication required")
            return
        if (
            self.require_admin_for_mutations
            and scope.get("method", "").upper() in self.MUTATING
            and not is_admin(principal)
        ):
            await self._reject(send, 403, "Administrator role required")
            return
        if (
            self.enforce_tenant_scope
            and principal.get("_principal") != "internal"
            and not is_admin(principal)
        ):
            named = _tenant_ids_in_request(path, scope.get("query_string", b""))
            if named:
                entitled = set(str(t) for t in (principal.get("tenant_ids") or []))
                if not named.issubset(entitled):
                    await self._reject(send, 403, "Not entitled to the requested tenant")
                    return
        scope[_SCOPE_KEY] = principal
        await self.app(scope, receive, send)

    async def _reject(self, send, status: int, detail: str) -> None:
        body = json.dumps({"detail": detail}).encode()
        await send({
            "type": "http.response.start",
            "status": status,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(body)).encode()),
            ],
        })
        await send({"type": "http.response.body", "body": body})


# ── FastAPI dependencies (read the principal the middleware stashed) ──────────
# starlette is always present in a service runtime; the Request annotation is
# what lets FastAPI inject the request into these Depends().
from starlette.requests import Request  # noqa: E402


def verified_user(request: Request) -> dict:
    """The principal AuthMiddleware verified for this request.

    Raises 500 if the middleware is not mounted (a wiring bug — fail closed
    rather than silently treating the request as anonymous)."""
    from fastapi import HTTPException

    user = request.scope.get(_SCOPE_KEY)
    if user is None:
        raise HTTPException(status_code=500, detail="auth middleware not mounted")
    return user


def is_admin(user: dict) -> bool:
    return any(r in ADMIN_ROLES for r in (user.get("roles") or []))


def require_admin(request: Request) -> dict:
    """Dependency: 403 unless the verified principal holds an admin role.

    The internal SYSTEM principal is admin-equivalent (service-to-service calls
    are already trusted)."""
    from fastapi import HTTPException

    user = verified_user(request)
    if not is_admin(user):
        raise HTTPException(status_code=403, detail="Administrator role required")
    return user


def _principal_from_request(request: Request) -> Optional[dict]:
    headers = {k.lower(): v for k, v in request.headers.items()}
    return _authenticate(headers)


def require_principal(request: Request) -> dict:
    """Route dependency (for services NOT using AuthMiddleware): require a
    valid principal — a signed access-token JWT or the internal key. Attach via
    the route decorator `dependencies=[Depends(require_principal)]` so it guards
    an endpoint without a signature change and without colliding with an
    existing get_current_user_from_token override."""
    from fastapi import HTTPException

    p = _principal_from_request(request)
    if p is None:
        raise HTTPException(status_code=401, detail="Authentication required")
    return p


def require_admin_principal(request: Request) -> dict:
    """Like require_principal but additionally requires an admin role."""
    from fastapi import HTTPException

    p = require_principal(request)
    if not is_admin(p):
        raise HTTPException(status_code=403, detail="Administrator role required")
    return p


def confine_guest_path(root: str, candidate: str, is_windows: bool) -> str:
    """Confine an attacker-supplied guest-OS path under ``root`` (the volume
    mount point). Prevents an authenticated caller from reading arbitrary VM
    files (``C:\\Windows\\NTDS\\ntds.dit``, ``/etc/shadow``) via the
    vm-volume-download / vm-volume-files routes (Check 17 CRITICAL — path
    confinement is required even after auth). Raises HTTPException(400) on
    escape. An empty candidate resolves to the root itself."""
    import ntpath
    import posixpath
    from fastapi import HTTPException

    cand = (candidate or "").strip()
    if is_windows:
        if not cand:
            return ntpath.normpath(root)
        if cand.startswith("\\\\") or cand.startswith("//"):
            raise HTTPException(status_code=400, detail="UNC path not allowed")
        is_abs = (len(cand) >= 2 and cand[1] == ":") or cand.startswith("\\") or cand.startswith("/")
        full = ntpath.normpath(cand if is_abs else ntpath.join(root, cand))
        root_n = ntpath.normpath(root)
        f, r, sep = full.lower(), root_n.lower(), "\\"
    else:
        if not cand:
            return posixpath.normpath(root)
        full = posixpath.normpath(cand if cand.startswith("/") else posixpath.join(root, cand))
        root_n = posixpath.normpath(root)
        f, r, sep = full, root_n, "/"
    r_trim = r.rstrip(sep)
    if not (f == r or f == r_trim or f.startswith(r_trim + sep)):
        raise HTTPException(status_code=400, detail="path escapes the volume root")
    return full
