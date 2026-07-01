"""Shared security utilities."""
import asyncio
import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Tuple

try:
    from fastapi import Depends, HTTPException, Request, status
    from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
except ModuleNotFoundError:  # pragma: no cover - worker-only runtimes
    Depends = None
    HTTPException = None
    Request = None
    status = None
    HTTPBearer = None
    HTTPAuthorizationCredentials = Any

from shared.config import settings

if HTTPBearer is not None:
    class _HTTPBearer401(HTTPBearer):
        """HTTPBearer that 401s (not 403s) on missing/malformed header.

        Why: the SPA's global fetch shim auto-refreshes on 401 only. The
        default HTTPBearer raises 403 for "Not authenticated", which silently
        bypasses the refresh path and surfaces as a permission error to the
        user. 401 keeps the cookie-refresh flow working.
        """
        async def __call__(self, request: Request):  # type: ignore[override]
            try:
                return await super().__call__(request)
            except HTTPException as exc:
                if exc.status_code == 403:
                    raise HTTPException(
                        status_code=401,
                        detail="Not authenticated",
                        headers={"WWW-Authenticate": "Bearer"},
                    ) from exc
                raise

    security = _HTTPBearer401()
else:
    security = None


# ==================== Secret Encryption ====================

def _get_fernet():
    """Get Fernet cipher instance from ENCRYPTION_KEY env var.

    ENCRYPTION_KEY MUST be a separate random secret from JWT_SECRET. Deriving
    one from the other means a single env-var leak (SSRF, leaked .env, log
    capture) decrypts every stored secret. Generate with:
        python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
    """
    from cryptography.fernet import Fernet

    key = settings.ENCRYPTION_KEY
    if not key:
        raise RuntimeError(
            "ENCRYPTION_KEY must be set. Provision a separate random 32-byte "
            "Fernet key in every environment (including CI) — do NOT reuse or "
            "derive from JWT_SECRET."
        )
    try:
        return Fernet(key.encode() if isinstance(key, str) else key)
    except Exception:
        raise RuntimeError("Invalid ENCRYPTION_KEY. Must be a base64-encoded 32-byte Fernet key.")


def encrypt_secret(plaintext: str) -> bytes:
    """Encrypt a secret string using Fernet symmetric encryption.
    
    Returns ciphertext as bytes.
    """
    f = _get_fernet()
    return f.encrypt(plaintext.encode('utf-8'))


def decrypt_secret(ciphertext: bytes) -> str:
    """Decrypt a previously encrypted secret string.
    
    Returns the original plaintext string.
    """
    f = _get_fernet()
    return f.decrypt(ciphertext).decode('utf-8')


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    from jose import jwt

    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + (expires_delta or timedelta(hours=settings.JWT_EXPIRATION_HOURS))
    to_encode.update({"exp": expire, "jti": str(uuid.uuid4()), "type": "access"})
    return jwt.encode(to_encode, settings.ACCESS_TOKEN_SECRET, algorithm=settings.JWT_ALGORITHM)


def create_refresh_token(data: dict) -> str:
    from jose import jwt

    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(days=settings.JWT_REFRESH_EXPIRATION_DAYS)
    # `jti` lets us revoke an individual refresh token without rotating the
    # signing secret. The auth-service stamps the JTI into a Redis denylist
    # on rotation/logout; decode_token's caller checks the denylist on
    # refresh paths (see is_refresh_token_revoked / revoke_refresh_token).
    to_encode.update({"exp": expire, "type": "refresh", "jti": str(uuid.uuid4())})
    return jwt.encode(to_encode, settings.REFRESH_TOKEN_SECRET, algorithm=settings.JWT_ALGORITHM)


# ==================== Refresh-token revocation (denylist) ====================
#
# Refresh tokens carry a `jti` claim. On every successful /auth/refresh we
# add the *used* jti to a Redis-backed denylist with TTL = remaining token
# lifetime, then issue a new refresh token with a fresh jti. If the same
# refresh token is presented twice (legitimate user race or replay attack),
# the second use sees the jti in the denylist and is rejected. /auth/logout
# also revokes the current jti.
#
# The auth-service is the only caller; we keep these helpers async and the
# rest of decode_token sync so every authenticated request doesn't pay a
# Redis round-trip. Access tokens are short-lived (hours) and aren't checked
# against the denylist — use the secret rotation lever for mass revocation
# of access tokens.

_REVOCATION_KEY_PREFIX = "jwt_revoked:"
_revocation_redis: Optional[Any] = None


async def _get_revocation_redis():
    """Lazy async Redis client for the revocation denylist. None when disabled."""
    global _revocation_redis
    if _revocation_redis is not None:
        return _revocation_redis
    if not settings.REDIS_ENABLED:
        return None
    try:
        from redis.asyncio import Redis
        # Pass REDIS_PASSWORD when configured. Production Redis runs with
        # `--requirepass` (see D-C3); without this kwarg every revocation
        # check would NOAUTH-error and the denylist would be unreachable
        # — which combined with the fail-closed behaviour above would 503
        # the entire auth-service.
        _revocation_redis = Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            password=settings.REDIS_PASSWORD or None,
            decode_responses=True,
            socket_timeout=2,
            socket_connect_timeout=2,
        )
        return _revocation_redis
    except Exception:
        return None


async def revoke_refresh_token(jti: str, ttl_seconds: int) -> bool:
    """Atomically claim a refresh-token jti on the denylist.

    Returns True when this caller wrote the denylist entry (i.e. won the
    rotation race). Returns False when the key already existed — another
    concurrent /auth/refresh has already burned this jti and the caller
    must reject the request to prevent refresh-token replay (B-C2).

    Fails closed on Redis errors: if we cannot prove we wrote the entry,
    we cannot safely issue a new refresh token, so we raise 503 rather
    than letting a stolen token roll forward (B-C1 companion).
    """
    if not jti:
        return False
    client = await _get_revocation_redis()
    if client is None:
        # Revocation is a security-critical control. If Redis is disabled
        # or unreachable at startup we refuse the operation rather than
        # silently letting the same refresh token be replayed.
        if HTTPException is None or status is None:
            raise RuntimeError("Refresh-token revocation requires Redis")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Token revocation service unavailable",
        )
    try:
        # SET NX is the compare-and-swap that makes rotation atomic: the
        # first concurrent caller wins (result truthy), every other caller
        # for the same jti gets None and is rejected by the auth-service.
        result = await client.set(
            f"{_REVOCATION_KEY_PREFIX}{jti}",
            "1",
            nx=True,
            ex=max(1, ttl_seconds),
        )
        return result is not None
    except Exception:
        # Fail-closed: we cannot confirm the denylist write, so we must
        # not let the caller proceed to mint a new token pair.
        if HTTPException is None or status is None:
            raise RuntimeError("Refresh-token revocation backend unreachable")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Token revocation service unavailable",
        )


async def is_refresh_token_revoked(jti: str) -> bool:
    """Return True when the jti has been revoked.

    Fails closed: on Redis error we raise 503 rather than treating the
    token as valid. A network partition or Redis OOM at the moment of a
    /auth/refresh call would otherwise let a previously revoked token
    replay indefinitely (B-C1).
    """
    if not jti:
        return False
    client = await _get_revocation_redis()
    if client is None:
        if HTTPException is None or status is None:
            raise RuntimeError("Refresh-token revocation requires Redis")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Token validation service unavailable",
        )
    try:
        return bool(await client.exists(f"{_REVOCATION_KEY_PREFIX}{jti}"))
    except Exception:
        if HTTPException is None or status is None:
            raise RuntimeError("Refresh-token revocation backend unreachable")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Token validation service unavailable",
        )


# ── Refresh-token rotation grace ──────────────────────────────────────────
# Rotation is strict single-use (revoke_refresh_token burns the old jti). To
# tolerate CONCURRENT legitimate use of the same refresh token — two browser
# tabs each firing a proactive refresh, or a client retry after a lost response
# — the winner of the rotation caches the freshly-minted (access, refresh) pair
# keyed by the old jti for a short grace window. The loser of the race (or the
# retry) retrieves that same pair instead of being handed a 401 and logged out.
# After the window the cache is gone, so a genuinely replayed token still fails.
_ROTATION_KEY_PREFIX = "refresh_rotation:"


async def remember_rotated_tokens(
    old_jti: str, access_token: str, refresh_token: str, grace_ttl_s: int
) -> None:
    """Cache the token pair minted while rotating ``old_jti`` for ``grace_ttl_s``
    seconds so a concurrent/retried refresh presenting the same token gets the
    SAME pair back (idempotent rotation). Best-effort: on Redis unavailability we
    simply don't cache — the loser then falls back to the pre-existing 401, i.e.
    no worse than before."""
    if not old_jti or not access_token or not refresh_token:
        return
    client = await _get_revocation_redis()
    if client is None:
        return
    try:
        await client.set(
            f"{_ROTATION_KEY_PREFIX}{old_jti}",
            json.dumps({"a": access_token, "r": refresh_token}),
            ex=max(1, int(grace_ttl_s)),
        )
    except Exception:
        # Never let a caching hiccup break the (already-successful) rotation.
        return


async def get_rotated_tokens(old_jti: str) -> Optional[Tuple[str, str]]:
    """Return the (access, refresh) pair cached for ``old_jti`` within the grace
    window, or None. Fail-open to None so a lookup error degrades to the normal
    401 path rather than 503-ing a legitimate refresh."""
    if not old_jti:
        return None
    client = await _get_revocation_redis()
    if client is None:
        return None
    try:
        raw = await client.get(f"{_ROTATION_KEY_PREFIX}{old_jti}")
        if not raw:
            return None
        d = json.loads(raw)
        a, r = d.get("a"), d.get("r")
        if a and r:
            return a, r
        return None
    except Exception:
        return None


async def resolve_concurrent_refresh(
    old_jti: str, poll_attempts: int = 3, poll_interval_s: float = 0.15
) -> Optional[Tuple[str, str]]:
    """When a refresh LOSES the rotation race, return the winner's grace-cached
    token pair (idempotent) or None (genuine replay → caller must 401).

    Polls a few times to close the tiny window between the winner claiming the
    jti and the winner writing its cache entry (they are separate Redis ops)."""
    for attempt in range(max(1, poll_attempts)):
        pair = await get_rotated_tokens(old_jti)
        if pair:
            return pair
        if attempt < poll_attempts - 1:
            await asyncio.sleep(poll_interval_s)
    return None


def _unauthorized(detail: str):
    if HTTPException is None or status is None:
        raise RuntimeError("JWT decoding requires FastAPI runtime support")
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail=detail,
        headers={"WWW-Authenticate": "Bearer"},
    )


def decode_token(token: str, expected_type: str = "access") -> dict:
    from jose import JWTError, jwt

    if expected_type == "access":
        secret = settings.ACCESS_TOKEN_SECRET
    elif expected_type == "refresh":
        secret = settings.REFRESH_TOKEN_SECRET
    else:
        raise ValueError(f"Unknown token type: {expected_type!r}")

    try:
        payload = jwt.decode(token, secret, algorithms=[settings.JWT_ALGORITHM])
    except JWTError:
        raise _unauthorized("Invalid or expired token")

    if payload.get("type") != expected_type:
        raise _unauthorized("Invalid token type")

    return payload


def _get_user_from_token(token: str) -> dict:
    payload = decode_token(token, expected_type="access")

    user_id: str = payload.get("sub")
    if user_id is None:
        if HTTPException is None:
            raise RuntimeError("Token validation requires FastAPI runtime support")
        raise HTTPException(status_code=401, detail="Invalid token")

    return {
        "id": user_id,
        "email": payload.get("email"),
        "roles": payload.get("roles", []),
        "org_id": payload.get("orgId"),
        "tenant_ids": payload.get("tenantIds", []),
    }


if Depends is not None and security is not None:
    def get_current_user_from_token(
        credentials: HTTPAuthorizationCredentials = Depends(security),
    ) -> dict:
        """Extract user from JWT token (used by API Gateway)."""
        return _get_user_from_token(credentials.credentials)
else:  # pragma: no cover - non-API runtimes should not call this
    def get_current_user_from_token(credentials: Optional[Any] = None) -> dict:
        raise RuntimeError("FastAPI authentication helpers are unavailable in this runtime")
