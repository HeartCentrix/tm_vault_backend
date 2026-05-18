"""Microsoft Graph API client for resource discovery"""
import asyncio
import logging
import os
import httpx
from contextlib import contextmanager
from contextvars import ContextVar
from typing import AsyncGenerator, AsyncIterator, Iterator, List, Optional, Dict, Any, Set, Tuple, Union
from datetime import datetime, timedelta
import hashlib
import time

from shared.power_bi_client import PowerBIClient
from shared.graph_ratelimit import (
    RateLimitPolicy, GraphRetryExhaustedError,
)

logger = logging.getLogger(__name__)

# Task-scoped Graph-rate-limit priority. ContextVar because a single
# cached GraphClient often services multiple concurrent jobs (e.g.
# backup-worker with MAX_CONCURRENT_ONEDRIVE_BACKUPS_PER_WORKER>1) —
# each asyncio task gets its own context and its own priority.
# 0=NORMAL, 1=HIGH, 2=URGENT. See shared/graph_priority.py for mapping.
_current_priority: ContextVar[int] = ContextVar("graph_priority", default=0)


@contextmanager
def graph_priority(priority: int) -> Iterator[None]:
    """Set the Graph-rate-limit priority for all calls made inside the
    `with` block (and all asyncio tasks spawned from it). Reverts on
    exit, even on exception.

    Usage in a worker's job handler::

        from shared.graph_client import graph_priority
        from shared.graph_priority import priority_for_queue

        async def process(self, msg, queue_name):
            with graph_priority(priority_for_queue(queue_name)):
                # every Graph call here runs at the queue's priority
                ...

    No-op cost when the feature flag is off (the ContextVar is set but
    `GraphClient._effective_priority` short-circuits to 0).
    """
    token = _current_priority.set(max(0, int(priority)))
    try:
        yield
    finally:
        _current_priority.reset(token)

# Timeout constants — tuned for Graph API and token endpoint behavior
_DEFAULT_TIMEOUT = httpx.Timeout(connect=15.0, read=60.0, write=30.0, pool=10.0)
_TOKEN_TIMEOUT = httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=10.0)

# Module-level cache of GraphClient instances keyed by (app_client_id,
# tenant_id). Populated lazily by `get_messages_mime_concurrent` so a
# 12-app rotation against one mailbox doesn't reopen 12 token exchanges
# and httpx sessions on every call. Each cached client owns its own
# persistent http session (HTTP/2 multiplexed if GRAPHCLIENT_HTTP2=true),
# so 12 cached clients = 12 long-lived TCP connections to Graph,
# multiplexing up to 4 streams each per the per-app per-mailbox limit.
# Lifetime is process-scoped — clients are never evicted (12 × tenants
# worth of sockets is bounded and tiny for a single-tenant deployment).
_MULTI_APP_CLIENT_CACHE: Dict[Tuple[str, str], "GraphClient"] = {}


def _parse_retry_after(resp: httpx.Response, default: float = 30.0, cap: float = 120.0) -> float:
    """Parse Retry-After (seconds or HTTP-date) from a 429/503 response.

    Falls back to ``default`` when the header is missing or unparseable.
    Clamped to ``cap`` so a hostile Retry-After doesn't stall a worker
    for hours on a single throttle event.
    """
    raw = resp.headers.get("Retry-After") or resp.headers.get("retry-after")
    if not raw:
        return min(default, cap)
    try:
        return min(float(raw), cap)
    except ValueError:
        try:
            from email.utils import parsedate_to_datetime
            dt = parsedate_to_datetime(raw)
            delta = (dt - datetime.utcnow()).total_seconds()
            return min(max(delta, 1.0), cap)
        except Exception:
            return min(default, cap)


class GraphClient:
    """Client for Microsoft Graph API calls with multi-app support"""

    GRAPH_URL = "https://graph.microsoft.com/v1.0"
    TOKEN_URL = "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

    SCOPES = [
        "https://graph.microsoft.com/.default"
    ]

    def __init__(self, client_id: str, client_secret: str, tenant_id: str, power_bi_refresh_token: Optional[str] = None):
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.power_bi_refresh_token = power_bi_refresh_token
        self._access_token: Optional[str] = None
        self._token_expiry: Optional[datetime] = None
        # Persistent shared httpx.AsyncClient. Previously every Graph
        # call did `async with httpx.AsyncClient(...)` which forced a
        # fresh TCP + TLS handshake per request (~80-150ms on WAN to
        # graph.microsoft.com). For a 5K-user first-sync that's ~100M
        # handshakes worth of overhead. Persistent client with a 50-
        # keepalive pool collapses that to ~50 handshakes for the
        # lifetime of the worker. Lazy-created on first use so we
        # don't need a running event loop at construction time.
        self._http: Optional[httpx.AsyncClient] = None
        self._http_lock = asyncio.Lock()

    async def _get_shared_http(self) -> httpx.AsyncClient:
        """Return (lazy-create) the persistent httpx.AsyncClient.

        Locked so concurrent first-callers don't both build a client.
        Re-creates if the previous client was closed (defensive, in
        case some path called aclose() prematurely)."""
        if self._http is not None and not self._http.is_closed:
            return self._http
        async with self._http_lock:
            if self._http is not None and not self._http.is_closed:
                return self._http
            # Generous default timeout — individual call sites can
            # tighten via per-call `timeout=` kwarg on .get/.post.
            # connect=30 covers TLS handshake under WAN jitter.
            # read=600 covers large-attachment downloads (mail/chat).
            # max_keepalive_connections=50 sized so a worker with 10
            # parallel folder drains × 2-4 in-flight pages each fits
            # without recycling sockets.
            # HTTP/2: default ON (2026-05-17 prod tuning). The `h2` lib is
            # pinned in every worker's requirements.txt; if a custom image
            # lacks it the ImportError path below falls back to HTTP/1.1
            # with a warning so the worker keeps serving. HTTP/2 multiplexes
            # hundreds of in-flight requests on one TCP connection → no
            # per-request TLS handshake on the hot path. Critical for the
            # 12-replica × 20-app fleet where each replica may have
            # 8 concurrent USER_CHATS handlers × 32 in-flight HC fetches
            # = 256 concurrent Graph requests; HTTP/1.1 would force 256
            # parallel sockets per replica, blowing the pool budget.
            use_http2 = os.environ.get("GRAPHCLIENT_HTTP2", "true").lower() == "true"
            # HTTP/2 needs the `h2` lib. It's pinned in every worker's
            # requirements.txt — but if a custom image somehow lacks
            # it, httpx raises ImportError at construct time. Detect
            # and fall back to HTTP/1.1 with a loud warning so the
            # worker keeps serving instead of crash-looping.
            if use_http2:
                try:
                    import h2  # noqa: F401 — presence check only
                except ImportError:
                    print(
                        "[GraphClient] WARN: GRAPHCLIENT_HTTP2=true but "
                        "the `h2` package isn't installed. Falling back "
                        "to HTTP/1.1. Add `h2>=4.1.0,<5.0` to this "
                        "service's requirements.txt to enable multiplexing."
                    )
                    use_http2 = False
            # Pool sizing rationale (validated against the worker's
            # actual concurrency budget):
            #   - HTTP/2 OFF (legacy): each Graph call needs its own
            #     TCP socket; 50 keepalive covers 10 folders × 2-4
            #     in-flight pages + headroom.
            #   - HTTP/2 ON: one socket handles 100+ multiplexed
            #     streams, so max_connections matters less — the cap
            #     just bounds the number of distinct host:port pairs
            #     we keep open (graph.microsoft.com, login.* for the
            #     token service, plus the per-shard SharePoint hosts).
            # Bumped to 200/100 to accommodate higher per-worker
            # concurrency after the throughput overhaul (folder
            # parallelism + multi-app concurrent MIME fetch). No prior
            # commit documented a smaller value being load-bearing.
            self._http = httpx.AsyncClient(
                http2=use_http2,
                timeout=httpx.Timeout(
                    connect=30.0, read=600.0, write=300.0, pool=30.0,
                ),
                limits=httpx.Limits(
                    max_connections=200,
                    max_keepalive_connections=100,
                    keepalive_expiry=300.0,
                ),
                # Several SharePoint/CDN download sites need 3xx
                # redirect following (referenceAttachment shared URLs,
                # chat hosted content). Graph proper rarely returns
                # 3xx; safe default.
                follow_redirects=True,
            )
            return self._http

    def _http_session(self, timeout=None):
        """Drop-in replacement for `async with httpx.AsyncClient(timeout=X)`.

        Returns an async context manager that yields the SHARED
        persistent client without closing it on exit. The `timeout=`
        arg is honored at the persistent client's session-default level
        on first creation; per-call sites should pass `timeout=` to
        their .get/.post if they need a tighter cap than the generous
        client-wide default (read=600s) — this preserves behavior of
        the pre-pool sites without forcing every call site to be
        rewritten."""
        client_provider = self._get_shared_http

        class _SharedClientCM:
            async def __aenter__(self_inner):
                return await client_provider()

            async def __aexit__(self_inner, *exc):
                # Crucial: do NOT close the shared client here. It
                # outlives this `async with` block by design.
                return False

        return _SharedClientCM()

    async def aclose(self) -> None:
        """Dispose the persistent client on worker shutdown.

        Idempotent. Safe to call multiple times. After aclose() the
        next Graph call lazy-creates a fresh client — useful when
        the loop is being recycled."""
        client = self._http
        self._http = None
        if client is not None and not client.is_closed:
            try:
                await client.aclose()
            except Exception:
                pass

    def _effective_priority(self) -> int:
        """Return the priority for the current asyncio task.

        Uses a task-scoped ContextVar so concurrent jobs sharing this
        GraphClient instance (common pattern: worker caches one client
        per tenant and runs many jobs against it) each see their own
        priority without race conditions.

        Ignored — returns 0 — when the feature flag is off.
        """
        from shared.config import settings as _s
        if not getattr(_s, "GRAPH_PRIORITY_SCHEDULING_ENABLED", False):
            return 0
        return _current_priority.get()

    @property
    def app_client_id(self) -> str:
        """Return the app client ID for tracking purposes"""
        return self.client_id

    async def get_granted_scope_fingerprint(self) -> str:
        """Returns a short hash that uniquely identifies THIS app's currently-
        granted application permissions in the active tenant.

        Used by the chat-drain "smart skip" path: when a chat fails with 403,
        we record the fingerprint at failure-time. On subsequent backups we
        compare the current fingerprint to the recorded one — same value
        means "no permission change since failure, don't bother retrying";
        different value means an admin granted/revoked a permission, retry.

        Source of truth: GET /servicePrincipals(appId='<id>')/appRoleAssignments
        which lists every application-permission grant for this app in this
        tenant. Each grant has a stable `appRoleId` GUID (e.g. the GUID for
        Chat.Read.All). Sort the GUIDs, hash, return a 16-char hex digest —
        small enough to store cheaply on every chat failure record, stable
        across runs, changes only when an admin adds/removes a permission.

        Cached per-instance so repeated calls during one backup don't burn
        Graph budget. ~1 query per shard app per backup."""
        cached = getattr(self, "_granted_scope_fingerprint", None)
        if cached is not None:
            return cached
        try:
            token = await self._get_token()
            url = (
                f"{self.GRAPH_URL}/servicePrincipals(appId='{self.client_id}')"
                "/appRoleAssignments?$select=appRoleId&$top=200"
            )
            http = await self._get_shared_http()
            resp = await http.get(
                url, headers={"Authorization": f"Bearer {token}"}, timeout=15,
            )
            resp.raise_for_status()
            data = resp.json() or {}
            role_ids = sorted(
                str(item.get("appRoleId") or "")
                for item in (data.get("value") or [])
                if item.get("appRoleId")
            )
            fp = hashlib.sha256(",".join(role_ids).encode()).hexdigest()[:16]
            self._granted_scope_fingerprint = fp
            return fp
        except Exception as e:
            # Fail-open: empty string compares != to any real fingerprint
            # so callers will retry rather than skip. Better to waste one
            # Graph call than to permanently skip a chat we could have
            # backed up.
            log = logging.getLogger("tmvault.graph_client")
            log.warning(
                "get_granted_scope_fingerprint failed for app %s: %s",
                self.client_id, e,
            )
            return ""

    async def _get_token(self) -> str:
        """Get or refresh access token using client credentials with retry."""
        if self._access_token and self._token_expiry and datetime.utcnow() < self._token_expiry:
            return self._access_token

        last_exc = None
        for attempt in range(1, 4):  # 3 attempts
            try:
                async with self._http_session() as client:
                    resp = await client.post(
                        self.TOKEN_URL.format(tenant_id=self.tenant_id),
                        data={
                            "grant_type": "client_credentials",
                            "client_id": self.client_id,
                            "client_secret": self.client_secret,
                            "scope": "https://graph.microsoft.com/.default",
                        },
                    )
                    resp.raise_for_status()
                    data = resp.json()
                    self._access_token = data["access_token"]
                    # Token expires in ~1 hour, refresh 5 min early
                    expires_in = data.get("expires_in", 3600)
                    self._token_expiry = datetime.utcnow() + timedelta(seconds=expires_in - 300)
                    return self._access_token
            except (httpx.ReadTimeout, httpx.ConnectTimeout) as e:
                last_exc = e
                wait = 2 ** attempt
                print(f"[GraphClient] Token fetch timeout (attempt {attempt}/3), retry in {wait}s: {e}")
                await asyncio.sleep(wait)
        raise RuntimeError(f"Could not acquire token after 3 attempts: {last_exc}")

    async def _try_migrate_app(
        self, throttled_app_id: str,
    ) -> Tuple[Optional[str], Optional[str]]:
        """Pick a healthy alternate app and fetch a fresh token for it.

        Called by 429/503 retry sites to swap apps **instead of sleeping**.
        With N apps registered (currently 20), most 429s only apply to the
        app that just sent the request — the other N-1 apps still have full
        per-app budget. Burning 30s of Retry-After sleep is pure waste when
        we could immediately retry on a different app's token.

        The migration cost is one token-fetch round-trip (~150-300ms)
        amortized against avoiding the Retry-After sleep (~5-60s). Net win
        in 100% of cases where at least one other app is healthy.

        Args:
            throttled_app_id: the client_id whose request just hit 429/503.
                Used to ensure we don't migrate back to the same app.

        Returns:
            (token, new_app_id) — caller uses this token in retry's
            Authorization header and reports mark_success against new_app_id.
            (None, None) when no other healthy app is available
            (single-app deployment OR all apps throttled simultaneously);
            caller should fall back to sleep+retry-on-same-app.

        Tokens are NOT cached here; the migration is rare and the multi_app
        manager already does adaptive ban-ladder bookkeeping. Caching would
        invite stale-token bugs on the slow path.
        """
        from shared.multi_app_manager import multi_app_manager
        if multi_app_manager.app_count <= 1:
            return None, None
        # get_next_app() applies round-robin admission filtered by
        # throttle state; it returns the chosen AppRegistry even when all
        # apps are throttled (least-loaded fallback) so we double-check
        # is_throttled + client_id distinct here.
        pick = multi_app_manager.get_next_app()
        if not pick or pick.client_id == throttled_app_id or pick.is_throttled:
            return None, None
        try:
            async with self._http_session() as client:
                resp = await client.post(
                    self.TOKEN_URL.format(tenant_id=pick.tenant_id),
                    data={
                        "grant_type": "client_credentials",
                        "client_id": pick.client_id,
                        "client_secret": pick.client_secret,
                        "scope": "https://graph.microsoft.com/.default",
                    },
                )
                resp.raise_for_status()
                token = resp.json().get("access_token")
                if not token:
                    return None, None
                return token, pick.client_id
        except Exception as exc:
            print(
                f"[GraphClient] migration token fetch failed "
                f"({pick.client_id[:8]}): {type(exc).__name__}: {exc}"
            )
            return None, None
    
    @property
    def _policy(self) -> RateLimitPolicy:
        """Lazy per-client RateLimitPolicy; rebuilt if settings change."""
        existing = getattr(self, "__policy_cached", None)
        if existing is not None:
            return existing
        from shared.config import settings as s
        p = RateLimitPolicy(
            stream_rate=s.GRAPH_STREAM_PACE_REQS_PER_SEC,
            app_rate=s.GRAPH_APP_PACE_REQS_PER_SEC,
            throttle_sequence=s.GRAPH_THROTTLE_BACKOFF_SECONDS,
            transient_sequence=s.GRAPH_TRANSIENT_BACKOFF_SECONDS,
            jitter_ratio=s.GRAPH_JITTER_RATIO,
            cumulative_cap_s=s.GRAPH_MAX_CUMULATIVE_WAIT_SECONDS,
        )
        self.__policy_cached = p
        return p

    async def _get(self, url: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """Make authenticated GET request with pagination, throttling, and timeout retry.

        Branches on GRAPH_HARDENING_ENABLED: when off (default), runs the
        legacy path preserved verbatim. When on, runs the policy-driven
        hardened path with per-app pacing, Retry-After parsing,
        cumulative cap, and GraphRetryExhaustedError on exhaustion.

        Preserves @odata.deltaLink for incremental sync and handles
        single-object responses (e.g. /users/{id}) that have no 'value' array.
        """
        from shared.config import settings as _s
        if _s.GRAPH_HARDENING_ENABLED:
            return await self._get_hardened(url, params)
        return await self._get_legacy(url, params)

    async def _get_legacy(self, url: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """Pre-hardening _get — preserved verbatim as the kill-switch path."""
        token = await self._get_token()
        # Track which app's token is currently in use. Starts as self
        # but may migrate on 429 to a healthier app — see _try_migrate_app
        # docstring for why this beats sleep+retry-on-same-app.
        current_app_id = self.client_id
        all_items = []
        next_url = url
        max_retries = 5
        retry_count = 0
        delta_link = None
        last_data = {}

        from shared.graph_rate_limiter import graph_rate_limiter
        while next_url:
            try:
                async with self._http_session() as client:
                    # ConsistencyLevel: eventual is only valid with $count queries
                    if params and params.get("$count") == "true":
                        headers = {"Authorization": f"Bearer {token}", "ConsistencyLevel": "eventual"}
                    else:
                        headers = {"Authorization": f"Bearer {token}"}
                    await graph_rate_limiter.acquire(reason="graph_get_legacy")
                    resp = await client.get(next_url, headers=headers, params=params if not next_url.startswith("http") else None)

                    # Handle 429 throttling
                    if resp.status_code == 429:
                        retry_after = int(resp.headers.get("Retry-After", "30"))
                        from shared.multi_app_manager import multi_app_manager
                        multi_app_manager.mark_throttled(current_app_id, retry_after)
                        if retry_count < max_retries:
                            retry_count += 1
                            # Try to swap to a different healthy app FIRST.
                            # If one's available, immediate retry — no
                            # sleep, no wasted minutes burning through
                            # Retry-After when 19 other apps could serve.
                            new_token, new_app = await self._try_migrate_app(current_app_id)
                            if new_token and new_app:
                                token = new_token
                                current_app_id = new_app
                                # Skip the Retry-After sleep entirely;
                                # the new app's per-app budget hasn't
                                # been throttled — Graph's per-app caps
                                # are independent.
                                continue
                            # All other apps throttled (or single-app
                            # deployment) — fall back to honoring
                            # Retry-After on the same app.
                            await __import__('asyncio').sleep(retry_after)
                            continue
                        resp.raise_for_status()

                    resp.raise_for_status()
                    # Adaptive circuit-breaker feedback: tell the
                    # multi-app manager this app served a clean 2xx so
                    # it can exit probation + recover its rate cap.
                    try:
                        from shared.multi_app_manager import multi_app_manager
                        _lat_ms = float(resp.elapsed.total_seconds() * 1000) if getattr(resp, "elapsed", None) else 0.0
                        # Credit the app that actually served the request
                        # (may have migrated from self.client_id on prior 429).
                        multi_app_manager.mark_success(current_app_id, _lat_ms)
                    except Exception:
                        pass
                    data = resp.json()
                    last_data = data
                    retry_count = 0  # Reset on success

                    # Single-object response (e.g. /users/{id}, /users/{id}/drive)
                    # These have no "value" array — return the object directly
                    if "value" not in data and "@odata.nextLink" not in data:
                        return data

                    all_items.extend(data.get("value", []))

                    # Capture delta link for incremental sync
                    if "@odata.deltaLink" in data:
                        delta_link = data["@odata.deltaLink"]

                    next_url = data.get("@odata.nextLink")
                    params = None  # params only on first request

            except (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.RemoteProtocolError) as e:
                if retry_count < max_retries:
                    retry_count += 1
                    wait = min(5 * retry_count, 30)
                    print(f"[GraphClient] Timeout on {next_url} (attempt {retry_count}/{max_retries}), retrying in {wait}s: {e}")
                    await __import__('asyncio').sleep(wait)
                    # Refresh token in case it expired during the wait
                    token = await self._get_token()
                    continue
                raise

        result = {
            "value": all_items,
            "@odata.count": last_data.get("@odata.count", len(all_items)),
        }
        # Preserve delta link so callers can save it for incremental backups
        if delta_link:
            result["@odata.deltaLink"] = delta_link
        return result

    async def _get_hardened(
        self, url: str, params: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Policy-driven GET: pacing + Retry-After + backoff + cumulative cap.

        Same result shape as _get_legacy so callers are unaffected when the
        feature flag flips.
        """
        from shared.config import settings as s
        from shared.multi_app_manager import multi_app_manager
        policy = self._policy
        token = await self._get_token()
        all_items: List[Dict[str, Any]] = []
        next_url: Optional[str] = url
        delta_link: Optional[str] = None
        last_data: Dict[str, Any] = {}

        from shared.graph_rate_limiter import graph_rate_limiter
        async with self._http_session() as client:
            while next_url:
                prio = self._effective_priority()
                await policy.stream_bucket.acquire(priority=prio)
                await multi_app_manager.acquire_app_token(
                    self.client_id, priority=prio
                )
                if params and params.get("$count") == "true":
                    headers = {"Authorization": f"Bearer {token}",
                               "ConsistencyLevel": "eventual"}
                else:
                    headers = {"Authorization": f"Bearer {token}"}
                try:
                    await graph_rate_limiter.acquire(reason="graph_get_hardened")
                    resp = await client.get(
                        next_url, headers=headers,
                        params=params if not next_url.startswith("http") else None,
                    )
                except (httpx.ReadTimeout, httpx.ConnectTimeout,
                        httpx.RemoteProtocolError) as exc:
                    action = policy.decide_transient_error()
                    if action.exhausted:
                        raise GraphRetryExhaustedError(
                            f"transient cap hit on {next_url}: {type(exc).__name__}"
                        )
                    print(f"[GraphClient/hardened] transient {type(exc).__name__} "
                          f"on {next_url[:80]}; sleep {action.sleep_seconds:.1f}s")
                    await asyncio.sleep(action.sleep_seconds)
                    token = await self._get_token()
                    continue

                if resp.status_code in (429, 503):
                    action = policy.decide(
                        status_code=resp.status_code,
                        retry_after=resp.headers.get("Retry-After"),
                    )
                    if action.exhausted:
                        raise GraphRetryExhaustedError(
                            f"cumulative cap hit on {next_url}: {action.reason}"
                        )
                    multi_app_manager.mark_throttled(
                        self.client_id, int(action.sleep_seconds),
                    )
                    # Multi-app rotation: same reasoning as the legacy
                    # path — sleeping ~30s of Retry-After is wasteful
                    # when other apps have full budget. We don't track
                    # `current_app` locally here because the hardened
                    # `_get` is a single-URL call (not paginated); the
                    # next iteration of this while-loop just retries
                    # next_url with the swapped token.
                    new_token, _new_app = await self._try_migrate_app(self.client_id)
                    if new_token:
                        token = new_token
                        print(
                            f"[GraphClient/hardened] {resp.status_code} on "
                            f"{next_url[:80]} — migrating app "
                            f"(skipping {action.sleep_seconds:.1f}s sleep)"
                        )
                        continue
                    print(f"[GraphClient/hardened] {resp.status_code} on "
                          f"{next_url[:80]} — {action.reason}")
                    await asyncio.sleep(action.sleep_seconds)
                    if s.GRAPH_POST_THROTTLE_BRAKE_MS > 0:
                        await asyncio.sleep(
                            s.GRAPH_POST_THROTTLE_BRAKE_MS / 1000.0
                        )
                    continue

                resp.raise_for_status()
                policy.reset_on_success()
                # Adaptive circuit-breaker feedback for the hardened path.
                try:
                    _lat_ms = float(resp.elapsed.total_seconds() * 1000) if getattr(resp, "elapsed", None) else 0.0
                    multi_app_manager.mark_success(self.client_id, _lat_ms)
                except Exception:
                    pass
                data = resp.json() or {}
                last_data = data

                if "value" not in data and "@odata.nextLink" not in data:
                    return data

                all_items.extend(data.get("value", []))
                if "@odata.deltaLink" in data:
                    delta_link = data["@odata.deltaLink"]
                next_url = data.get("@odata.nextLink")
                params = None

        result: Dict[str, Any] = {
            "value": all_items,
            "@odata.count": last_data.get("@odata.count", len(all_items)),
        }
        if delta_link:
            result["@odata.deltaLink"] = delta_link
        return result

    async def _iter_pages(
        self, url: str, params: Optional[Dict] = None,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Streaming variant of _get: yields each page as it arrives.

        Branches on GRAPH_HARDENING_ENABLED: legacy path preserved for the
        kill-switch, hardened path adds per-page pacing + sticky app
        rotation with failover.

        Overlaps downstream work (upload/DB) with Graph pagination — callers
        can fire upload tasks per page and continue pulling instead of waiting
        for the full response to materialize in RAM. Preserves 429 Retry-After
        + timeout retry semantics.
        """
        from shared.config import settings as _s
        if _s.GRAPH_HARDENING_ENABLED:
            async for p in self._iter_pages_hardened(url, params):
                yield p
            return
        async for p in self._iter_pages_legacy(url, params):
            yield p

    async def _iter_pages_legacy(
        self, url: str, params: Optional[Dict] = None,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Pre-hardening _iter_pages — preserved verbatim as the kill-switch path."""
        next_url = url
        max_retries = 5
        retry_count = 0
        # Reset the captured delta link at the start of each stream so a
        # stale value from a previous call never leaks into the caller's
        # `getattr(graph_client, "_last_delta_link", None)` read. Without
        # this the legacy path silently breaks incremental backups (every
        # run re-fetches full history because the token is never stored).
        self._last_delta_link: Optional[str] = None
        # Tracks which app's token is currently in use for this stream;
        # migrates on 429 (see _try_migrate_app). Streams stay on the
        # failover app for the remainder of pagination — re-pinning to
        # self.client_id every page would defeat the migration.
        token = await self._get_token()
        current_app_id = self.client_id

        while next_url:
            try:
                async with self._http_session() as client:
                    if params and params.get("$count") == "true":
                        headers = {"Authorization": f"Bearer {token}", "ConsistencyLevel": "eventual"}
                    else:
                        headers = {"Authorization": f"Bearer {token}"}
                    resp = await client.get(
                        next_url, headers=headers,
                        params=params if not next_url.startswith("http") else None,
                    )
                    if resp.status_code == 429:
                        retry_after = int(resp.headers.get("Retry-After", "30"))
                        from shared.multi_app_manager import multi_app_manager
                        multi_app_manager.mark_throttled(current_app_id, retry_after)
                        if retry_count < max_retries:
                            retry_count += 1
                            # Try app migration before falling back to sleep.
                            new_token, new_app = await self._try_migrate_app(current_app_id)
                            if new_token and new_app:
                                print(
                                    f"[GraphClient] 429 on {next_url[:80]} — "
                                    f"migrating app {current_app_id[:8]} → "
                                    f"{new_app[:8]} (attempt {retry_count}/{max_retries})"
                                )
                                token = new_token
                                current_app_id = new_app
                                continue
                            print(
                                f"[GraphClient] 429 on {next_url[:100]} — sleeping {retry_after}s "
                                f"(attempt {retry_count}/{max_retries}, no healthy alt-app)"
                            )
                            await asyncio.sleep(retry_after)
                            continue
                        resp.raise_for_status()

                    resp.raise_for_status()
                    # Adaptive circuit-breaker feedback (legacy stream path).
                    try:
                        from shared.multi_app_manager import multi_app_manager as _mam
                        _lat_ms = float(resp.elapsed.total_seconds() * 1000) if getattr(resp, "elapsed", None) else 0.0
                        _mam.mark_success(current_app_id, _lat_ms)
                    except Exception:
                        pass
                    data = resp.json()
                    retry_count = 0
                    yield data
                    # Graph returns @odata.deltaLink on the FINAL page of a
                    # delta stream (mutually exclusive with @odata.nextLink).
                    # Persist it so callers can store and resume next run.
                    if "@odata.deltaLink" in data:
                        self._last_delta_link = data["@odata.deltaLink"]
                    next_url = data.get("@odata.nextLink")
                    params = None  # params only on the first request

            except (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.RemoteProtocolError) as e:
                if retry_count < max_retries:
                    retry_count += 1
                    wait = min(5 * retry_count, 30)
                    print(
                        f"[GraphClient] Timeout on {next_url} "
                        f"(attempt {retry_count}/{max_retries}), retrying in {wait}s: {e}"
                    )
                    await asyncio.sleep(wait)
                    continue
                raise

    async def _iter_pages_hardened(
        self, url: str, params: Optional[Dict] = None,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Policy-paced page iteration with sticky app rotation.

        Stream starts pinned to self.client_id. On 429, the policy reports
        throttle + marks the app in multi_app_manager. The stream migrates
        to the next healthy app and stays on the failover for
        GRAPH_STICKY_PAGES_BEFORE_RETURN pages; after that window, checks
        is_app_throttled(original) and returns if clean, else stays put.

        Captures @odata.deltaLink on the terminal page into self._last_delta_link
        so callers that want the cursor (chats / mail / onedrive delta) can
        read it post-iteration.
        """
        from shared.config import settings as s
        from shared.multi_app_manager import multi_app_manager
        policy = self._policy
        original_app = self.client_id
        current_app = original_app
        pages_on_failover = 0
        next_url: Optional[str] = url
        token = await self._get_token()
        self._last_delta_link: Optional[str] = None

        async with self._http_session() as client:
            while next_url:
                prio = self._effective_priority()
                await policy.stream_bucket.acquire(priority=prio)
                await multi_app_manager.acquire_app_token(
                    current_app, priority=prio
                )
                if params and params.get("$count") == "true":
                    headers = {"Authorization": f"Bearer {token}",
                               "ConsistencyLevel": "eventual"}
                else:
                    headers = {"Authorization": f"Bearer {token}"}
                try:
                    resp = await client.get(
                        next_url, headers=headers,
                        params=params if not next_url.startswith("http") else None,
                    )
                except (httpx.ReadTimeout, httpx.ConnectTimeout,
                        httpx.RemoteProtocolError) as exc:
                    action = policy.decide_transient_error()
                    if action.exhausted:
                        raise GraphRetryExhaustedError(
                            f"transient cap hit on {next_url}: {type(exc).__name__}"
                        )
                    print(f"[GraphClient/hardened iter] transient "
                          f"{type(exc).__name__}; sleep {action.sleep_seconds:.1f}s")
                    await asyncio.sleep(action.sleep_seconds)
                    token = await self._get_token()
                    continue

                if resp.status_code in (429, 503):
                    action = policy.decide(
                        status_code=resp.status_code,
                        retry_after=resp.headers.get("Retry-After"),
                    )
                    if action.exhausted:
                        raise GraphRetryExhaustedError(
                            f"cumulative cap hit on {next_url}: {action.reason}"
                        )
                    multi_app_manager.mark_throttled(
                        current_app, int(action.sleep_seconds),
                    )
                    # Try to migrate AND obtain a fresh token for the
                    # new app. If successful, skip the sleep entirely —
                    # the new app's per-app budget is independent of
                    # the throttled one. The prior implementation
                    # migrated `current_app` but kept the OLD token,
                    # so the retry still hit the throttled app's
                    # tenant-wide cap; this version actually swaps the
                    # token used for the next request.
                    new_token, new_app = await self._try_migrate_app(current_app)
                    if new_token and new_app:
                        token = new_token
                        current_app = new_app
                        pages_on_failover = 0
                        print(
                            f"[GraphClient/hardened iter] migrating "
                            f"{resp.status_code} -> app={current_app[:8]} "
                            f"(skipping {action.sleep_seconds:.1f}s sleep)"
                        )
                        # Skip the Retry-After sleep and the post-
                        # throttle brake; the new app needs no cooldown.
                        continue
                    await asyncio.sleep(action.sleep_seconds)
                    if s.GRAPH_POST_THROTTLE_BRAKE_MS > 0:
                        await asyncio.sleep(
                            s.GRAPH_POST_THROTTLE_BRAKE_MS / 1000.0
                        )
                    continue

                resp.raise_for_status()
                policy.reset_on_success()
                # Adaptive circuit-breaker feedback (hardened stream path).
                try:
                    _lat_ms = float(resp.elapsed.total_seconds() * 1000) if getattr(resp, "elapsed", None) else 0.0
                    # Mark on the CURRENT app (post-failover) not self.client_id,
                    # since hardened iter migrates apps mid-stream.
                    multi_app_manager.mark_success(current_app, _lat_ms)
                except Exception:
                    pass
                data = resp.json() or {}
                yield data

                next_url = data.get("@odata.nextLink")
                params = None
                if "@odata.deltaLink" in data:
                    self._last_delta_link = data["@odata.deltaLink"]
                    if not next_url:
                        break

                # Sticky-return check: if on failover for long enough and
                # the original app has cooled down, switch back.
                if current_app != original_app:
                    pages_on_failover += 1
                    if pages_on_failover >= s.GRAPH_STICKY_PAGES_BEFORE_RETURN:
                        if not multi_app_manager.is_app_throttled(original_app):
                            current_app = original_app
                            pages_on_failover = 0
                            print(f"[GraphClient/hardened iter] returning "
                                  f"to original app={original_app}")
                        else:
                            pages_on_failover = 0

    async def batch(self, requests):
        """Convenience: run a Graph $batch through the hardened policy.

        See shared.graph_batch.BatchClient for semantics. Paginated
        endpoints (delta, skiptoken, top) are rejected at submission.
        """
        from shared.graph_batch import BatchClient
        return await BatchClient(self).batch(requests)

    async def _post(self, url: str, payload: Dict[str, Any], headers: Optional[Dict] = None) -> Dict[str, Any]:
        """Make authenticated POST request"""
        token = await self._get_token()
        async with self._http_session() as client:
            req_headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
            if headers:
                req_headers.update(headers)
            resp = await client.post(url, headers=req_headers, json=payload)
            resp.raise_for_status()
            return resp.json()

    async def _put(self, url: str, content: Any, headers: Optional[Dict] = None) -> Dict[str, Any]:
        """Make authenticated PUT request (for file uploads)"""
        token = await self._get_token()
        async with self._http_session() as client:
            req_headers = {"Authorization": f"Bearer {token}"}
            if headers:
                req_headers.update(headers)
            else:
                req_headers["Content-Type"] = "application/octet-stream"

            if isinstance(content, str):
                content = content.encode('utf-8')

            resp = await client.put(url, headers=req_headers, content=content)
            resp.raise_for_status()
            return resp.json()

    async def _patch(self, url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Make authenticated PATCH request"""
        token = await self._get_token()
        async with self._http_session() as client:
            req_headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
            resp = await client.patch(url, headers=req_headers, json=payload)
            resp.raise_for_status()
            return resp.json()

    async def _delete(self, url: str) -> None:
        """Make authenticated DELETE request"""
        token = await self._get_token()
        async with self._http_session() as client:
            headers = {"Authorization": f"Bearer {token}"}
            resp = await client.delete(url, headers=headers)
            resp.raise_for_status()
    
    # System mailbox display-name prefixes Microsoft creates and never wants backed up.
    # Matches afi.ai's exclusion list — these are tenant infrastructure, not user data.
    _SYSTEM_MAILBOX_PREFIXES = (
        "DiscoverySearchMailbox",
        "FederatedEmail.",
        "SystemMailbox{",
        "Microsoft Office 365 portal",
        "MicrosoftSupport",
        "MicrosoftCustomerSupport",
        "Spam Quarantine",
    )

    @classmethod
    def _is_system_mailbox(cls, display_name: Optional[str], upn: Optional[str]) -> bool:
        for needle in (display_name or "", upn or ""):
            for prefix in cls._SYSTEM_MAILBOX_PREFIXES:
                if needle.startswith(prefix):
                    return True
        return False

    async def discover_users(self) -> List[Dict[str, Any]]:
        """Fetch all users from Entra ID. Skips Guest users and system mailboxes —
        afi.ai treats these as out-of-scope for backup."""
        result = await self._get(
            f"{self.GRAPH_URL}/users",
            params={
                "$top": "999",
                "$count": "true",
                "$select": "id,displayName,mail,userPrincipalName,jobTitle,department,accountEnabled,createdDateTime,userType",
            },
        )
        all_value = result.get("value", [])
        while "@odata.nextLink" in result:
            result = await self._get(result["@odata.nextLink"])
            all_value.extend(result.get("value", []))

        users = []
        skipped_guest = 0
        skipped_system = 0
        for u in all_value:
            user_type = (u.get("userType") or "").lower()
            display_name = u.get("displayName") or u.get("mail") or u.get("userPrincipalName") or "Unknown"
            upn = u.get("userPrincipalName")
            if user_type == "guest":
                skipped_guest += 1
                continue
            if self._is_system_mailbox(display_name, upn):
                skipped_system += 1
                continue
            is_enabled = u.get("accountEnabled", True)
            users.append({
                "external_id": u.get("id"),
                "display_name": display_name,
                "email": u.get("mail") or upn,
                "type": "ENTRA_USER",
                "metadata": {
                    "user_principal_name": upn,
                    "job_title": u.get("jobTitle"),
                    "department": u.get("department"),
                    "account_enabled": is_enabled,
                    "user_type": u.get("userType"),
                    "created_at": u.get("createdDateTime"),
                },
                "_account_enabled": is_enabled,  # For discovery worker to filter
            })
        if skipped_guest or skipped_system:
            print(f"[GraphClient] discover_users: skipped {skipped_guest} guest(s), {skipped_system} system account(s)")
        return users
    
    @staticmethod
    def _classify_group(g: Dict[str, Any]) -> str:
        """Map Entra group flags to a canonical classification.

        Microsoft splits groups across three flags (groupTypes, mailEnabled,
        securityEnabled) which don't form an obvious taxonomy. afi.ai surfaces
        a single 'kind' to the user — we mirror that:
          M365_GROUP            — groupTypes contains 'Unified' (a.k.a. modern group)
          DISTRIBUTION_LIST     — mail-enabled, NOT security, no Unified flag
          MAIL_ENABLED_SECURITY — both mail- and security-enabled
          SECURITY_GROUP        — security-only (not mail-enabled)
        Anything else falls back to UNKNOWN — typically dynamic groups or
        provisioning artifacts. The caller decides whether to back it up."""
        group_types = [t.lower() for t in (g.get("groupTypes") or [])]
        mail_enabled = bool(g.get("mailEnabled"))
        security_enabled = bool(g.get("securityEnabled"))
        if "unified" in group_types:
            return "M365_GROUP"
        if mail_enabled and security_enabled:
            return "MAIL_ENABLED_SECURITY"
        if mail_enabled and not security_enabled:
            return "DISTRIBUTION_LIST"
        if security_enabled and not mail_enabled:
            return "SECURITY_GROUP"
        return "UNKNOWN"

    async def discover_groups(self) -> List[Dict[str, Any]]:
        """Fetch all groups from Entra ID and classify each one.

        Unified (M365) groups are emitted as type=M365_GROUP so a single resource
        row represents the group's mailbox + SharePoint site + (optional) Team —
        matching afi.ai's UX. Distribution Lists and security groups stay as
        ENTRA_GROUP rows but carry a `group_classification` so the UI can label
        them and backup handlers can decide what to fetch.
        """
        result = await self._get(
            f"{self.GRAPH_URL}/groups",
            params={
                "$top": "999",
                "$count": "true",
                # Pull resourceProvisioningOptions so we know if a Unified group
                # has a Team attached (caller can skip Team-scan if absent).
                "$select": "id,displayName,mail,mailEnabled,securityEnabled,groupTypes,description,resourceProvisioningOptions,visibility,createdDateTime",
            },
        )
        all_value = result.get("value", [])
        while "@odata.nextLink" in result:
            result = await self._get(result["@odata.nextLink"])
            all_value.extend(result.get("value", []))

        groups = []
        counts: Dict[str, int] = {}
        for g in all_value:
            classification = self._classify_group(g)
            counts[classification] = counts.get(classification, 0) + 1
            provisioning = [p.lower() for p in (g.get("resourceProvisioningOptions") or [])]
            group_types_lc = [t.lower() for t in (g.get("groupTypes") or [])]
            is_dynamic = "dynamicmembership" in group_types_lc
            metadata = {
                "mail_enabled": g.get("mailEnabled"),
                "security_enabled": g.get("securityEnabled"),
                "group_types": g.get("groupTypes", []),
                "description": g.get("description"),
                "visibility": g.get("visibility"),
                "created_at": g.get("createdDateTime"),
                "group_classification": classification,
                "has_team": "team" in provisioning,
                "resource_provisioning_options": g.get("resourceProvisioningOptions") or [],
                # Auto-protection bucket flags — surfaced for the Protection
                # page so the user can target dynamic / Entra-only groups
                # without needing the UI to re-derive these from groupTypes.
                "is_dynamic_group": is_dynamic,
                "auto_protect_eligible": classification in ("M365_GROUP", "SECURITY_GROUP") or is_dynamic,
            }
            groups.append({
                "external_id": g.get("id"),
                "display_name": g.get("displayName", "Unknown"),
                "email": g.get("mail"),
                # Unified → first-class M365_GROUP row; everything else stays as
                # ENTRA_GROUP and the classification metadata distinguishes them.
                "type": "M365_GROUP" if classification == "M365_GROUP" else "ENTRA_GROUP",
                "metadata": metadata,
            })
        if counts:
            summary = ", ".join(f"{k}={v}" for k, v in sorted(counts.items()))
            print(f"[GraphClient] discover_groups: {summary}")
        return groups
    
    # ------------------------------------------------------------------
    # Mailbox discovery — simple & direct:
    #   1. Fetch all users
    #   2. Enrich each with mailboxSettings.userPurpose
    #   3. Build resource records based on userPurpose value
    # ------------------------------------------------------------------

    async def discover_mailboxes(self, kinds: Optional[set] = None) -> List[Dict[str, Any]]:
        """
        Discover all mailboxes by enriching users with userPurpose.

        userPurpose → resource type mapping:
          "user"      → MAILBOX
          "shared"    → SHARED_MAILBOX
          "room"      → ROOM_MAILBOX
          "equipment" → ROOM_MAILBOX
          None/other  → skipped (no mailbox)

        `kinds`, if supplied, restricts the result to those resource type
        strings — e.g. {"SHARED_MAILBOX","ROOM_MAILBOX"} for Tier 1, which
        excludes per-user MAILBOX rows (those are emitted as USER_MAIL Tier 2
        children later, on demand).
        """
        # Step 1: Fetch all users (no $filter — get everyone). Add userType so we
        # can drop guests; afi.ai never backs up guest mailboxes (they live in their
        # home tenant).
        users_result = await self._get(
            f"{self.GRAPH_URL}/users",
            params={"$top": "999", "$count": "true",
                    "$select": "id,displayName,mail,userPrincipalName,jobTitle,department,accountEnabled,createdDateTime,userType"},
        )
        all_users_raw = users_result.get("value", [])
        while "@odata.nextLink" in users_result:
            users_result = await self._get(users_result["@odata.nextLink"])
            all_users_raw.extend(users_result.get("value", []))
        # Drop guests + system mailboxes BEFORE the per-user mailboxSettings round-trip
        # to avoid wasted API calls on tenant infrastructure accounts.
        all_users = []
        skipped_guest = 0
        skipped_system = 0
        for u in all_users_raw:
            if (u.get("userType") or "").lower() == "guest":
                skipped_guest += 1
                continue
            if self._is_system_mailbox(u.get("displayName"), u.get("userPrincipalName")):
                skipped_system += 1
                continue
            all_users.append(u)
        if skipped_guest or skipped_system:
            print(f"[GraphClient] discover_mailboxes: skipped {skipped_guest} guest(s), {skipped_system} system account(s) before enrichment")
        mailboxes = []

        # Step 2: Enrich each user with userPurpose
        semaphore = asyncio.Semaphore(10)

        async def _enrich_one_user(user: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            async with semaphore:
                email = user.get("mail")
                if not email:
                    return None

                # Try mailboxSettings.userPurpose first — gives us the precise
                # mailbox type (user / shared / room / equipment). Common cause
                # of failure: app lacks MailboxSettings.Read.All. We fall back
                # to a /messages probe in that case so a missing scope doesn't
                # silently drop every mailbox in the tenant.
                purpose: Optional[str] = None
                try:
                    result = await self._get(
                        f"{self.GRAPH_URL}/users/{user['id']}/mailboxSettings",
                        params={"$select": "userPurpose"},
                    )
                    purpose = result.get("userPurpose") if result else None
                except Exception:
                    purpose = None

                if purpose == "user":
                    rtype = "MAILBOX"
                elif purpose == "shared":
                    rtype = "SHARED_MAILBOX"
                elif purpose in ("room", "equipment"):
                    rtype = "ROOM_MAILBOX"
                else:
                    # Fallback: probe /messages directly. This is the actual
                    # endpoint backup_mailbox uses, so success here proves the
                    # mailbox is backup-able regardless of the userPurpose
                    # field's accessibility. Cost: one extra HEAD-style GET
                    # per user-without-purpose, which is acceptable for the
                    # ~1x/discovery-cycle frequency.
                    try:
                        probe = await self._get(
                            f"{self.GRAPH_URL}/users/{user['id']}/messages",
                            params={"$top": "1", "$select": "id"},
                        )
                        if probe and "value" in probe:
                            # Mailbox reachable. Without userPurpose we can't
                            # distinguish user vs shared vs room — assume MAILBOX
                            # (user) since that's the dominant case. UI can let
                            # users reclassify as needed.
                            rtype = "MAILBOX"
                            purpose = "user (probed)"
                        else:
                            return None
                    except Exception:
                        return None  # truly no mailbox

                print(f"[GraphClient] {email} → userPurpose={purpose} → {rtype}")

                return {
                    "external_id": user.get("id"),
                    "display_name": user.get("displayName", email),
                    "email": email,
                    "type": rtype,
                    "metadata": {
                        "user_principal_name": user.get("userPrincipalName"),
                        "job_title": user.get("jobTitle"),
                        "department": user.get("department"),
                        "account_enabled": user.get("accountEnabled", True),
                        "created_at": user.get("createdDateTime"),
                        "mailbox_purpose": purpose,
                    },
                    "_account_enabled": user.get("accountEnabled", True),  # For discovery worker
                }

        tasks = [_enrich_one_user(u) for u in all_users]
        results = await asyncio.gather(*tasks, return_exceptions=False)

        for r in results:
            if r:
                if kinds and r["type"] not in kinds:
                    continue
                mailboxes.append(r)

        print(f"[GraphClient] discover_mailboxes: found {len(mailboxes)} mailboxes "
              f"({[m['type'] for m in mailboxes]})")
        return mailboxes

    async def discover_onedrive(self) -> List[Dict[str, Any]]:
        """Discover OneDrive sites for all users in parallel (bounded by Semaphore(10)).

        Previously serial — one GET /users/{id}/drive per user awaited in a loop —
        which turned into ~N × round-trip-latency wall time for tenants with many
        users. Matches the pattern used by discover_mailboxes and discover_teams."""
        users = await self.discover_users()
        semaphore = asyncio.Semaphore(10)

        async def _fetch_drive(u: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            if not u.get("email"):
                return None
            user_id = u["external_id"]
            async with semaphore:
                try:
                    drive_result = await self._get(f"{self.GRAPH_URL}/users/{user_id}/drive")
                except Exception as e:
                    msg = str(e)
                    if "404" in msg or "423" in msg:
                        # Not found / locked — discovery worker will stale-mark later
                        return None
                    print(f"Error discovering OneDrive for user {u.get('email')}: {e}")
                    return None

            if not drive_result or not drive_result.get("id"):
                return None
            return {
                "external_id": drive_result["id"],
                "display_name": drive_result.get("name", f"OneDrive - {u['display_name']}"),
                "email": u["email"],
                "type": "ONEDRIVE",
                "metadata": {
                    "user_id": user_id,
                    "user_email": u["email"],
                    "drive_id": drive_result["id"],
                    "web_url": drive_result.get("webUrl"),
                    "quota": drive_result.get("quota", {}),
                },
                "_account_enabled": u.get("_account_enabled", True),
            }

        results = await asyncio.gather(
            *[_fetch_drive(u) for u in users],
            return_exceptions=True,
        )
        drives: List[Dict[str, Any]] = []
        for r in results:
            if isinstance(r, dict):
                drives.append(r)
        return drives
    
    async def discover_sharepoint(self) -> List[Dict[str, Any]]:
        """Discover SharePoint sites via the tenant-admin REST API.

        The SharePoint Admin API at ``{tenant}-admin.sharepoint.com/_api/v2.1/sites``
        is the authoritative site-list endpoint — same source the
        SharePoint Admin Center uses. Returns every team site (not
        personal OneDrives) with richer metadata than Graph's /sites:
        ``template`` (GROUP#0, STS#3, SITEPAGEPUBLISHING#0, etc.),
        ``lockState``, ``archiveStatus``, ``isHubSite``, ``hubSiteId``,
        ``owner``, ``storageUsage``/``storageQuota``,
        ``lastContentModifiedDate``, and ``isPersonalSite`` as a
        first-class flag (no URL heuristics needed).

        Requires:
          1. App-registration with ``Sites.FullControl.All`` on the
             **SharePoint Online** API (admin-scope).
          2. Admin consent granted for that permission.
          3. A self-signed cert uploaded to the app registration,
             matching the PEM mounted at SHAREPOINT_CERT_PATH. SharePoint
             REST refuses client-secret-minted tokens outright.

        Raises on failure (no silent fallback) — the previous
        Graph-based fallback returned personal OneDrives dressed up as
        team sites, so a misconfigured tenant would appear to "work"
        while discovering the wrong data.
        """
        admin_host = await self._resolve_sharepoint_admin_host()
        if not admin_host:
            raise RuntimeError(
                "SharePoint admin host could not be resolved from Graph /sites/root; "
                "tenant may have SharePoint disabled or Graph connectivity is down"
            )

        token = await self._get_sharepoint_token(admin_host)

        sites: List[Dict[str, Any]] = []
        next_url: Optional[str] = f"https://{admin_host}/_api/v2.1/sites?$top=500"
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json;odata=nometadata",
        }
        async with self._http_session() as client:
            while next_url:
                resp = await client.get(next_url, headers=headers)
                if resp.status_code in (401, 403):
                    raise RuntimeError(
                        f"SharePoint admin API rejected the cert-signed token ({resp.status_code}): "
                        f"{resp.text[:300]}. Verify the cert's public key is uploaded to the AAD app "
                        f"AND the app has Sites.FullControl.All on the SharePoint API with admin consent granted."
                    )
                resp.raise_for_status()
                data = resp.json()
                for s in (data.get("value") or []):
                    if s.get("isPersonalSite") is True:
                        continue
                    # Compose the Graph-style composite id
                    # "host/siteCollectionId/webId" so every other SP
                    # helper in this module (subsites/lists/drives)
                    # keeps working without changes.
                    ext_id = s.get("id") or s.get("siteId") or ""
                    ext_id = ext_id.replace(",", "/") if ext_id else ""
                    if ext_id and "/" not in ext_id and s.get("webUrl"):
                        from urllib.parse import urlparse as _up
                        host = _up(s["webUrl"]).netloc
                        web_id = s.get("webId") or ""
                        if host and web_id:
                            ext_id = f"{host}/{ext_id}/{web_id}"

                    owner_val = s.get("owner") or s.get("ownerEmail")
                    owner_email = s.get("ownerEmail")
                    if isinstance(owner_val, dict):
                        owner_email = owner_val.get("email") or owner_email

                    sites.append({
                        "external_id": ext_id,
                        "display_name": s.get("title") or s.get("displayName") or s.get("name") or "Unknown Site",
                        "email": owner_email,
                        "type": "SHAREPOINT_SITE",
                        "metadata": {
                            "web_url": s.get("webUrl"),
                            "template": s.get("template"),
                            "lock_state": s.get("lockState"),
                            "archive_status": s.get("archiveStatus"),
                            "is_hub_site": s.get("isHubSite"),
                            "hub_site_id": s.get("hubSiteId"),
                            "owner": owner_val,
                            "storage_usage_mb": s.get("storageUsage"),
                            "storage_quota_mb": s.get("storageQuota"),
                            "last_content_modified": s.get("lastContentModifiedDate"),
                            "time_created": s.get("timeCreated"),
                            "source": "sp_admin_api",
                        },
                    })
                # v2.1 uses @odata.nextLink with a skipToken in the URL.
                next_url = data.get("@odata.nextLink") or data.get("@nextLink")

        print(f"[GraphClient] discover_sharepoint: {len(sites)} site(s) via admin API")
        return sites

    async def _resolve_sharepoint_admin_host(self) -> Optional[str]:
        """Derive ``{tenant}-admin.sharepoint.com`` from the tenant's
        SharePoint root site. Cached per-instance so we don't re-hit
        Graph for every discovery pass.

        The admin hostname is always the tenant name with ``-admin``
        appended — e.g. ``qfion.sharepoint.com`` → ``qfion-admin.sharepoint.com``.
        We just need to know the tenant name, which Graph's /sites/root
        exposes via its siteCollection.hostname.
        """
        if hasattr(self, "_sp_admin_host") and self._sp_admin_host is not None:
            return self._sp_admin_host
        try:
            root = await self._get(f"{self.GRAPH_URL}/sites/root", params={"$select": "siteCollection,webUrl"})
            host = (root.get("siteCollection") or {}).get("hostname") or ""
            if not host:
                from urllib.parse import urlparse as _up
                host = _up(root.get("webUrl") or "").netloc
            if not host or ".sharepoint.com" not in host:
                self._sp_admin_host = None
                return None
            # "qfion.sharepoint.com" → "qfion-admin.sharepoint.com"
            # Already-admin hosts (belt-and-suspenders) pass through.
            if "-admin.sharepoint.com" in host:
                admin_host = host
            else:
                prefix = host.split(".sharepoint.com")[0]
                admin_host = f"{prefix}-admin.sharepoint.com"
            self._sp_admin_host = admin_host
            return admin_host
        except Exception as exc:
            print(f"[GraphClient] _resolve_sharepoint_admin_host failed: {exc}")
            self._sp_admin_host = None
            return None

    async def discover_teams(self, include_chats: bool = True) -> List[Dict[str, Any]]:
        """Discover Teams groups (for channels) and, when ``include_chats`` is
        True, every 1:1 / group chat across the tenant.

        Tier 1 callers pass ``include_chats=False`` because per-user chat
        enumeration is deferred to Tier 2 (`discover_user_content`). The chat
        scan is the slowest part of this method (one /users/{id}/chats round-
        trip per user) so skipping it is a meaningful speedup."""
        resources = []

        # 1. Discover Teams groups (for channel backups)
        result = await self._get(
            f"{self.GRAPH_URL}/groups",
            params={"$filter": "resourceProvisioningOptions/Any(x:x eq 'Team')", "$top": "999"}
        )
        all_teams = result.get("value", [])
        while "@odata.nextLink" in result:
            result = await self._get(result["@odata.nextLink"])
            all_teams.extend(result.get("value", []))

        for g in all_teams:
            resources.append({
                "external_id": g.get("id"),
                "display_name": g.get("displayName", "Unknown Team"),
                "email": g.get("mail"),
                "type": "TEAMS_CHANNEL",
                "metadata": {
                    "description": g.get("description"),
                    "mail_enabled": g.get("mailEnabled"),
                    "visibility": g.get("visibility"),
                },
            })

        # 2. Discover all chats (1-on-1 and group chats)
        # Note: GET /chats (global) does NOT support app-only auth.
        # We use GET /users/{id}/chats per-user, which DOES support app-only with Chat.Read.All.
        if not include_chats:
            return resources
        try:
            import time

            # Fetch all users first
            users_result = await self._get(
                f"{self.GRAPH_URL}/users",
                params={"$top": "999", "$select": "id,userPrincipalName,displayName"}
            )
            all_users = users_result.get("value", [])
            # Follow pagination
            while users_result.get("@odata.nextLink"):
                users_result = await self._get(users_result["@odata.nextLink"])
                all_users.extend(users_result.get("value", []))

            _chat_semaphore = asyncio.Semaphore(10)  # Max 10 concurrent Graph API calls

            async def _fetch_chat_members(chat_id: str) -> tuple:
                """Fetch members for a single chat (with semaphore)."""
                async with _chat_semaphore:
                    try:
                        members_result = await self._get(
                            f"{self.GRAPH_URL}/chats/{chat_id}/members"
                        )
                        emails = []
                        names = []
                        for m in members_result.get("value", []):
                            email = m.get("email")
                            display_name = m.get("displayName")
                            if email:
                                emails.append(email)
                            if display_name:
                                names.append(display_name)
                        return emails, names
                    except Exception:
                        return [], []

            def _build_chat_resource(chat: Dict) -> Dict:
                """Build a TEAMS_CHAT resource dict from a chat object."""
                chat_id = chat.get("id")
                chat_type = chat.get("chatType", "unknown")
                topic = chat.get("topic")
                return {
                    "chat_id": chat_id,
                    "chat_type": chat_type,
                    "topic": topic,
                    "createdDateTime": chat.get("createdDateTime"),
                    "lastUpdatedDateTime": chat.get("lastUpdatedDateTime"),
                }

            async def _process_user_chats(user: Dict):
                """Fetch and process all chats for a single user."""
                user_id = user.get("id")
                if not user_id:
                    return []

                user_chats_raw = []
                try:
                    async with _chat_semaphore:
                        chats_result = await self._get(
                            f"{self.GRAPH_URL}/users/{user_id}/chats",
                            params={"$top": "999"}
                        )
                    user_chats_raw.extend(chats_result.get("value", []))

                    # Follow pagination
                    while chats_result.get("@odata.nextLink"):
                        async with _chat_semaphore:
                            chats_result = await self._get(chats_result["@odata.nextLink"])
                        user_chats_raw.extend(chats_result.get("value", []))
                except Exception:
                    pass  # Skip users where we can't access chats

                return user_chats_raw

            # Phase 1: Fetch all user chats in parallel (bounded concurrency)
            logger.info("Discovering Teams chats for %d users...", len(all_users))
            start_time = time.time()
            user_chat_tasks = [_process_user_chats(u) for u in all_users]
            user_chat_results = await asyncio.gather(*user_chat_tasks, return_exceptions=True)

            # Collect all unique chats
            all_chats: Dict[str, Dict] = {}  # chat_id -> chat object
            for result in user_chat_results:
                if isinstance(result, Exception):
                    continue
                for chat in result:
                    chat_id = chat.get("id")
                    if chat_id and chat_id not in all_chats:
                        all_chats[chat_id] = chat

            elapsed1 = time.time() - start_time
            logger.info("Found %d unique chats across users in %.1fs", len(all_chats), elapsed1)

            # Phase 2: Fetch members for all chats in parallel (bounded concurrency)
            start_time2 = time.time()
            all_chat_ids = list(all_chats.keys())
            member_tasks = [_fetch_chat_members(cid) for cid in all_chat_ids]
            member_results = await asyncio.gather(*member_tasks, return_exceptions=True)

            chat_members: Dict[str, tuple] = {}
            for i, chat_id in enumerate(all_chat_ids):
                result = member_results[i]
                if isinstance(result, Exception):
                    chat_members[chat_id] = ([], [])
                else:
                    chat_members[chat_id] = result

            elapsed2 = time.time() - start_time2
            logger.info("Fetched members for %d chats in %.1fs", len(all_chats), elapsed2)

            # Phase 3: Build resource dicts (CPU-bound, no network)
            for chat_id, chat in all_chats.items():
                chat_type = chat.get("chatType", "unknown")
                topic = chat.get("topic")
                member_emails, member_names = chat_members.get(chat_id, ([], []))

                # Build display name
                if chat_type == "oneOnOne":
                    if topic:
                        display_name = topic
                    elif member_names:
                        display_name = " | ".join(member_names)
                    else:
                        display_name = f"1-on-1 Chat ({chat_id[:8]})"
                else:
                    if topic:
                        display_name = topic
                    elif member_names:
                        display_name = f"Group: {', '.join(member_names[:3])}"
                        if len(member_names) > 3:
                            display_name += f" +{len(member_names) - 3} more"
                    else:
                        display_name = f"Group Chat ({chat_id[:8]})"

                resources.append({
                    "external_id": chat_id,
                    "display_name": display_name,
                    "email": None,
                    "type": "TEAMS_CHAT",
                    "metadata": {
                        "chatType": chat_type,
                        "topic": topic,
                        "memberCount": len(member_names),
                        "memberEmails": member_emails,
                        "memberNames": member_names,
                        "createdDateTime": chat.get("createdDateTime"),
                        "lastUpdatedDateTime": chat.get("lastUpdatedDateTime"),
                    },
                })

            total_elapsed = time.time() - start_time
            logger.info("Teams chat discovery complete: %d chats in %.1fs", len(all_chats), total_elapsed)

            # Phase 4: emit per-user TEAMS_CHAT_EXPORT shards.
            # One resource per Graph user who has any chats — the backup worker
            # issues a single /users/{id}/chats/getAllMessages/delta call per
            # shard instead of one call per chat (Graph caps $top=50, so a heavy
            # user was previously stuck paying that full-export cost once per
            # chat job). Delta token lives on this row's extra_data.
            for user, user_chats in zip(all_users, user_chat_results):
                if isinstance(user_chats, Exception):
                    continue
                chat_ids = [c.get("id") for c in user_chats if c.get("id")]
                if not chat_ids:
                    continue
                user_id = user.get("id")
                if not user_id:
                    continue
                display = user.get("displayName") or user.get("userPrincipalName") or user_id
                resources.append({
                    "external_id": user_id,
                    "display_name": f"Chat export — {display}",
                    "email": user.get("userPrincipalName"),
                    "type": "TEAMS_CHAT_EXPORT",
                    "metadata": {
                        "userPrincipalName": user.get("userPrincipalName"),
                        "userDisplayName": user.get("displayName"),
                        "chatIds": chat_ids,
                        "chatCount": len(chat_ids),
                    },
                })
            logger.info("Teams chat-export shards emitted: %d users", sum(
                1 for r in resources if r.get("type") == "TEAMS_CHAT_EXPORT"
            ))

        except Exception as e:
            logger.warning(f"Failed to discover Teams chats: {e}")

        return resources

    async def discover_power_platform(self) -> List[Dict[str, Any]]:
        """Discover Power Platform resources via PowerPlatformClient (correct audience).

        Previously this code hand-rolled HTTP against api.bap.microsoft.com using
        a Graph-scoped token, which Microsoft rejects with 401
        InvalidAuthenticationAudience (the BAP admin endpoints require a
        service.powerapps.com-scoped token). PowerPlatformClient gets the right
        token audience, so environments / apps / flows / DLP policies actually
        come back here.

        Power BI is orthogonal and still uses PowerBIClient (different REST
        surface, different scope)."""
        from shared.power_platform_client import PowerPlatformClient
        resources = []

        pp = PowerPlatformClient(
            client_id=self.client_id,
            client_secret=self.client_secret,
            tenant_id=self.tenant_id,
        )

        # 1. Environments
        try:
            envs_data = await pp.list_environments()
            environments = envs_data.get("value", []) if isinstance(envs_data, dict) else []
        except Exception as e:
            print(f"[discover_power_platform] list_environments failed: {e}")
            environments = []

        for env in environments:
            env_id = env.get("name")
            env_props = env.get("properties", {})
            env_name = env_props.get("displayName", env_id)
            env_type = env_props.get("environmentType", "Unknown")
            env_region = (env_props.get("location", {}) or {}).get("name", "Unknown") \
                if isinstance(env_props.get("location"), dict) else env_props.get("location", "Unknown")
            has_dataverse = (env_props.get("linkedEnvironmentMetadata", {}) or {}).get("CommonDataService") is not None

            # Environment itself as a POWER_APPS row (external_id prefixed "env_"
            # so the Recovery RestoreModal can distinguish environments from apps)
            resources.append({
                "external_id": f"env_{env_id}",
                "display_name": f"{env_name} (Environment)",
                "email": None,
                "type": "POWER_APPS",
                "metadata": {
                    "environment_id": env_id,
                    "environment_type": env_type,
                    "region": env_region,
                    "has_dataverse": has_dataverse,
                    "created_time": env_props.get("createdTime"),
                },
            })

            # 2. Power Apps in this environment
            try:
                apps_data = await pp.list_apps(env_id)
                for app in (apps_data.get("value", []) if isinstance(apps_data, dict) else []):
                    app_props = app.get("properties", {})
                    resources.append({
                        "external_id": f"app_{app.get('id', app.get('name'))}",
                        "display_name": app_props.get("displayName", app.get("name", "Unknown App")),
                        "email": None,
                        "type": "POWER_APPS",
                        "metadata": {
                            "app_id": app.get("name"),
                            "environment_id": env_id,
                            "environment_name": env_name,
                            "app_type": app_props.get("appType"),
                            "created_by": (app_props.get("createdBy", {}) or {}).get("displayName"),
                            "created_time": app_props.get("createdTime"),
                            "modified_time": app_props.get("lastModifiedTime"),
                        },
                    })
            except Exception as e:
                print(f"[discover_power_platform] list_apps failed for env {env_id}: {e}")

            # 3. Power Automate flows in this environment
            try:
                flows_data = await pp.list_flows(env_id)
                for flow in (flows_data.get("value", []) if isinstance(flows_data, dict) else []):
                    flow_props = flow.get("properties", {})
                    resources.append({
                        "external_id": f"flow_{flow.get('id', flow.get('name'))}",
                        "display_name": flow_props.get("displayName", flow.get("name", "Unknown Flow")),
                        "email": None,
                        "type": "POWER_AUTOMATE",
                        "metadata": {
                            "flow_id": flow.get("name"),
                            "environment_id": env_id,
                            "environment_name": env_name,
                            "state": flow_props.get("state"),
                            "created_by": (flow_props.get("createdBy", {}) or {}).get("displayName"),
                            "created_time": flow_props.get("createdTime"),
                            "modified_time": flow_props.get("lastModifiedTime"),
                        },
                    })
            except Exception as e:
                print(f"[discover_power_platform] list_flows failed for env {env_id}: {e}")

        # 4. Tenant-level DLP policies
        try:
            dlp_data = await pp.list_dlp_policies()
            for policy in (dlp_data.get("value", []) if isinstance(dlp_data, dict) else []):
                policy_id = policy.get("name") or policy.get("id")
                policy_props = policy.get("properties", {})
                if not policy_id:
                    continue
                resources.append({
                    "external_id": f"dlp_{policy_id}",
                    "display_name": policy_props.get("displayName", policy_id),
                    "email": None,
                    "type": "POWER_DLP",
                    "metadata": {
                        "policy_id": policy_id,
                        "policy_type": policy_props.get("policyType"),
                        "environment_type": policy_props.get("environmentType"),
                        "created_time": policy_props.get("createdTime"),
                        "modified_time": policy_props.get("lastModifiedTime"),
                    },
                })
        except Exception as e:
            print(f"[discover_power_platform] list_dlp_policies failed: {e}")

        # 5. Discover Power BI workspaces via Power BI REST API
        try:
            power_bi_client = PowerBIClient(
                tenant_id=self.tenant_id,
                client_id=self.client_id,
                client_secret=self.client_secret,
                refresh_token=self.power_bi_refresh_token,
            )
            workspaces = await power_bi_client.list_workspaces()
            self.power_bi_refresh_token = power_bi_client.refresh_token
            for workspace in workspaces:
                workspace_id = workspace.get("id")
                if not workspace_id:
                    continue
                resources.append({
                    "external_id": f"pbi_ws_{workspace_id}",
                    "display_name": workspace.get("name", "Unknown Power BI Workspace"),
                    "email": None,
                    "type": "POWER_BI",
                    "metadata": {
                        "workspace_id": workspace_id,
                        "workspace_type": workspace.get("type"),
                        "is_on_dedicated_capacity": workspace.get("isOnDedicatedCapacity"),
                        "capacity_id": workspace.get("capacityId"),
                        "description": workspace.get("description"),
                        "state": workspace.get("state"),
                        "default_dataset_storage_format": workspace.get("defaultDatasetStorageFormat"),
                    },
                })
        except Exception as e:
            print(f"Error discovering Power BI workspaces: {e}")

        return resources

    async def discover_planner(self) -> List[Dict[str, Any]]:
        """Discover Planner-capable group containers that actually have plans."""
        resources = []
        groups = await self.discover_groups()
        semaphore = asyncio.Semaphore(8)

        async def _probe_group(group: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            group_id = group.get("external_id")
            if not group_id:
                return None

            metadata = group.get("metadata") or {}
            if not (
                metadata.get("mail_enabled")
                or "Unified" in (metadata.get("group_types") or [])
            ):
                return None

            async with semaphore:
                try:
                    plans = await self.get_planner_plans_for_group(group_id)
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code in (403, 404):
                        return None
                    raise
                except Exception:
                    return None

            plan_list = plans.get("value", [])
            if not plan_list:
                return None

            return {
                "external_id": group_id,
                "display_name": f"{group.get('display_name', 'Unknown')} Planner",
                "email": group.get("email"),
                "type": "PLANNER",
                "metadata": {
                    "group_id": group_id,
                    "group_display_name": group.get("display_name"),
                    "group_email": group.get("email"),
                    "plan_count": len(plan_list),
                    "plan_ids": [plan.get("id") for plan in plan_list if plan.get("id")],
                },
            }

        results = await asyncio.gather(
            *[_probe_group(group) for group in groups],
            return_exceptions=True,
        )
        for result in results:
            if isinstance(result, dict):
                resources.append(result)
        return resources

    async def discover_todo(self) -> List[Dict[str, Any]]:
        """Discover users whose To Do workload is accessible."""
        resources = []
        users = await self.discover_users()
        semaphore = asyncio.Semaphore(10)

        async def _probe_user(user: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            user_id = user.get("external_id")
            if not user_id:
                return None

            async with semaphore:
                try:
                    lists = await self.get_user_todo_lists(user_id)
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code in (403, 404):
                        return None
                    raise
                except Exception:
                    return None

            list_items = lists.get("value", [])
            if not list_items:
                return None

            return {
                "external_id": user_id,
                "display_name": f"{user.get('display_name', 'Unknown')} To Do",
                "email": user.get("email"),
                "type": "TODO",
                "metadata": {
                    "user_id": user_id,
                    "user_email": user.get("email"),
                    "list_count": len(list_items),
                    "wellknown_lists": [
                        item.get("wellknownListName")
                        for item in list_items
                        if item.get("wellknownListName")
                    ],
                },
                "_account_enabled": user.get("_account_enabled", True),
            }

        results = await asyncio.gather(
            *[_probe_user(user) for user in users],
            return_exceptions=True,
        )
        for result in results:
            if isinstance(result, dict):
                resources.append(result)
        return resources
    
    async def discover_all(self) -> List[Dict[str, Any]]:
        """Tier 1 discovery — runs on datasource add and on the protection
        page's "refresh" button. Discovers only top-level container resources
        (Users, Shared Mailboxes, Rooms, SharePoint sites, Groups & Teams,
        Power Platform). Per-user content (Mail/OneDrive/Contacts/Calendar/
        Chats) is deferred to Tier 2 and runs on demand via
        `discover_user_content()` when a user is selected for backup.

        Excluded from Tier 1 (still callable individually as scoped scans):
          discover_onedrive (per-user → Tier 2)
          discover_planner / discover_todo (low-frequency, opt-in)
          discover_conditional_access / discover_bitlocker_keys (Entra security
            singletons — opt-in)
          chats inside discover_teams (per-user → Tier 2; channels stay)
          user mailboxes inside discover_mailboxes (per-user → Tier 2; shared
            + room mailboxes stay since they're tenant-level)"""
        all_resources = []

        async def _safe_discover(name, coro):
            try:
                return await coro
            except Exception as e:
                print(f"Error discovering {name}: {e}")
                return []

        tasks = [
            _safe_discover("users", self.discover_users()),
            _safe_discover("groups", self.discover_groups()),
            _safe_discover("mailboxes (shared+rooms)", self.discover_mailboxes(kinds={"SHARED_MAILBOX", "ROOM_MAILBOX"})),
            _safe_discover("sharepoint", self.discover_sharepoint()),
            _safe_discover("teams (channels)", self.discover_teams(include_chats=False)),
            _safe_discover("power_platform", self.discover_power_platform()),
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                print(f"Discovery task failed: {result}")
            elif isinstance(result, list):
                all_resources.extend(result)

        # Deduplicate: same external_id AND same type
        seen = set()
        unique = []
        for r in all_resources:
            key = f"{r.get('external_id')}:{r.get('type')}"
            if key and key not in seen:
                seen.add(key)
                unique.append(r)

        return unique

    # ── Tier 2 per-user content discovery ───────────────────────────────────
    # Runs on demand when the user clicks "back up" on a specific Entra user.
    # Returns five fixed buckets (Mail, OneDrive, Contacts, Calendar, Chats),
    # each a self-describing dict with type=USER_*, parent_external_id set to
    # the Entra user ID, and a metadata blob carrying counts/IDs the backup
    # worker can use to plan its run. We deliberately do NOT enumerate every
    # message/file here — that's the backup worker's job. The summary counts
    # exist so the Protection / Recovery UI can show the user "you have X
    # mailbox folders, Y OneDrive items, …" without a second discovery pass.

    async def discover_user_content(
        self,
        user_external_id: str,
        user_principal_name: Optional[str] = None,
        user_display_name: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Discover the five fixed content categories for one Entra user.

        Returns up to 5 dicts (type ∈ USER_MAIL, USER_ONEDRIVE, USER_CONTACTS,
        USER_CALENDAR, USER_CHATS). For categories where Graph returns
        404/403 (e.g. user has no mailbox / OneDrive not provisioned — i.e.
        no Exchange Online / OneDrive license), we emit a marker row with
        metadata.license_missing=True so the UI can render a small
        "no license" badge on the workload instead of silently hiding it.
        """
        display = user_display_name or user_principal_name or user_external_id
        email = user_principal_name

        # Map of per-category licensing hints surfaced in the UI when
        # Graph returns 403/404/423 (not-licensed / locked / absent).
        _LICENSE_HINT = {
            "mail": "Exchange Online",
            "onedrive": "OneDrive for Business",
            "calendar": "Exchange Online",
            "contacts": "Exchange Online",
            "chats": "Microsoft Teams",
        }
        _KIND = {
            "mail": ("USER_MAIL", f"Mail — {display}"),
            "onedrive": ("USER_ONEDRIVE", f"OneDrive — {display}"),
            "calendar": ("USER_CALENDAR", f"Calendar — {display}"),
            "contacts": ("USER_CONTACTS", f"Contacts — {display}"),
            "chats": ("USER_CHATS", f"Chats — {display}"),
        }

        # Transient errors from Graph or the network are surfaced
        # here as `httpx.HTTPStatusError` (5xx), `httpx.TimeoutException`,
        # or `httpx.ConnectError`. Permanent errors (404/403/423) carry
        # license / consent meaning and must NOT be retried — they
        # produce a stable INACCESSIBLE marker row instead.
        _PERMANENT_STATUSES = ("400", "401", "403", "404", "410", "423")
        _TRANSIENT_STATUSES = ("500", "502", "503", "504")

        def _classify(err_msg: str) -> str:
            """Return 'permanent', 'transient', or 'unknown' based on
            the error text. We grep for the status code rather than
            isinstance-check because Graph wraps responses at multiple
            layers — the string remains the most reliable signal."""
            for s in _PERMANENT_STATUSES:
                if s in err_msg:
                    return "permanent"
            for s in _TRANSIENT_STATUSES:
                if s in err_msg:
                    return "transient"
            low = err_msg.lower()
            if "timeout" in low or "timed out" in low or "connecterror" in low \
               or "remoteprotocolerror" in low or "connection" in low:
                return "transient"
            return "unknown"

        async def _safe(name: str, coro_factory):
            """Run a discovery probe with bounded retry on transients.

            `coro_factory` is a zero-arg async callable that returns a
            FRESH coroutine each call — we cannot retry the same
            coroutine object once it has been awaited. Backoff: 0.5s,
            1.5s (cumulative <3s before final failure) — enough to ride
            past most 504 blips without dragging out a 5,000-user
            discovery sweep. Permanent 4xx exits immediately so a
            no-license user doesn't pay the retry budget.
            """
            import asyncio as _aio
            kind, label = _KIND.get(name, ("USER_UNKNOWN", name))
            backoffs = (0.5, 1.5)  # 2 retries -> 3 attempts total
            last_err = None
            for attempt in range(len(backoffs) + 1):
                try:
                    return await coro_factory()
                except Exception as e:
                    last_err = e
                    cls = _classify(str(e))
                    if cls == "permanent":
                        return {
                            "external_id": f"{user_external_id}:{name}",
                            "display_name": label,
                            "email": email,
                            "type": kind,
                            "parent_external_id": user_external_id,
                            "metadata": {
                                "user_id": user_external_id,
                                "license_missing": True,
                                "license_hint": _LICENSE_HINT.get(name),
                                "probe_status": next(
                                    (s for s in _PERMANENT_STATUSES if s in str(e)),
                                    "4xx",
                                ),
                            },
                        }
                    if attempt < len(backoffs):
                        await _aio.sleep(backoffs[attempt])
                        continue
                    break

            # All retries exhausted on a transient (or unknown) error.
            # Do NOT silently drop the resource — emit a pending marker
            # so the row still appears in the resource list. The persist
            # layer treats `discovery_pending` as INACCESSIBLE (backup
            # skips it), and the next discovery cycle will overwrite this
            # marker with the real metadata once Graph recovers.
            print(
                f"[GraphClient] discover_user_content {name} failed for "
                f"{display} after {len(backoffs)+1} attempts: {last_err}"
            )
            return {
                "external_id": f"{user_external_id}:{name}",
                "display_name": label,
                "email": email,
                "type": kind,
                "parent_external_id": user_external_id,
                "metadata": {
                    "user_id": user_external_id,
                    "discovery_pending": True,
                    "last_discovery_error": str(last_err)[:300],
                },
            }

        # Mail — folder count + storage estimate.
        async def _mail():
            folders = await self._get(
                f"{self.GRAPH_URL}/users/{user_external_id}/mailFolders",
                params={"$top": "999", "$select": "id,displayName,totalItemCount,unreadItemCount"},
            )
            folder_list = folders.get("value", []) or []
            total_items = sum(int(f.get("totalItemCount") or 0) for f in folder_list)
            return {
                "external_id": f"{user_external_id}:mail",
                "display_name": f"Mail — {display}",
                "email": email,
                "type": "USER_MAIL",
                "parent_external_id": user_external_id,
                "metadata": {
                    "user_id": user_external_id,
                    "user_principal_name": user_principal_name,
                    "folder_count": len(folder_list),
                    "item_count": total_items,
                },
            }

        # OneDrive — drive id + quota.
        async def _onedrive():
            drive = await self._get(f"{self.GRAPH_URL}/users/{user_external_id}/drive")
            if not drive or not drive.get("id"):
                return None
            return {
                "external_id": f"{user_external_id}:onedrive",
                "display_name": f"OneDrive — {display}",
                "email": email,
                "type": "USER_ONEDRIVE",
                "parent_external_id": user_external_id,
                "metadata": {
                    "user_id": user_external_id,
                    "drive_id": drive.get("id"),
                    "web_url": drive.get("webUrl"),
                    "quota": drive.get("quota", {}),
                },
            }

        # Contacts — folder + count. Emits an entry unconditionally so the
        # Recovery tab and the backup fan-out always have a USER_CONTACTS
        # row to point at, even when the user has zero contacts. Quirky
        # tenants where /contactFolders 404s or returns an unparsable body
        # still get an empty row instead of being dropped.
        async def _contacts():
            folder_count = 0
            try:
                folders = await self._get(
                    f"{self.GRAPH_URL}/users/{user_external_id}/contactFolders",
                    params={"$top": "100", "$select": "id,displayName"},
                )
                folder_count = len((folders or {}).get("value", []) or [])
            except Exception as e:
                # Non-fatal — fall through with folder_count=0.
                print(f"[GraphClient] contactFolders probe failed for {display}: {e}")
            return {
                "external_id": f"{user_external_id}:contacts",
                "display_name": f"Contacts — {display}",
                "email": email,
                "type": "USER_CONTACTS",
                "parent_external_id": user_external_id,
                "metadata": {
                    "user_id": user_external_id,
                    "folder_count": folder_count,
                },
            }

        # Calendar — list of calendars.
        async def _calendar():
            cals = await self._get(
                f"{self.GRAPH_URL}/users/{user_external_id}/calendars",
                params={"$top": "100", "$select": "id,name,canEdit,owner"},
            )
            cal_list = cals.get("value", []) or []
            return {
                "external_id": f"{user_external_id}:calendar",
                "display_name": f"Calendar — {display}",
                "email": email,
                "type": "USER_CALENDAR",
                "parent_external_id": user_external_id,
                "metadata": {
                    "user_id": user_external_id,
                    "calendar_count": len(cal_list),
                    "calendar_names": [c.get("name") for c in cal_list[:10]],
                },
            }

        # Chats — chat IDs for this user (1:1 + group).
        async def _chats():
            chats = await self._get(
                f"{self.GRAPH_URL}/users/{user_external_id}/chats",
                params={"$top": "999", "$select": "id,chatType,topic,lastUpdatedDateTime"},
            )
            chat_list = chats.get("value", []) or []
            return {
                "external_id": f"{user_external_id}:chats",
                "display_name": f"Chats — {display}",
                "email": email,
                "type": "USER_CHATS",
                "parent_external_id": user_external_id,
                "metadata": {
                    "user_id": user_external_id,
                    "chat_count": len(chat_list),
                    "chat_ids": [c.get("id") for c in chat_list],
                },
            }

        # Pass factories (not awaited coroutines) so _safe can retry
        # transient failures with a fresh coroutine each attempt — you
        # cannot await the same coroutine twice.
        results = await asyncio.gather(
            _safe("mail", _mail),
            _safe("onedrive", _onedrive),
            _safe("contacts", _contacts),
            _safe("calendar", _calendar),
            _safe("chats", _chats),
        )
        return [r for r in results if r]

    # ── Conditional Access policies ─────────────────────────────────────────
    # Tenant-singleton resources — small in number, high in security value.
    # afi backs these up so a misconfiguration or tenant-takeover incident can
    # be reverted by re-applying the captured definitions.

    async def discover_conditional_access(self) -> List[Dict[str, Any]]:
        """List all CA policies as discovery rows. Each row's external_id is the
        policy ID; full definition lives in metadata so the backup handler can
        re-dump it without a second round-trip."""
        url = f"{self.GRAPH_URL}/identity/conditionalAccess/policies"
        try:
            result = await self._get(url, params={"$top": "200"})
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (401, 403):
                # Tenant lacks Policy.Read.All or doesn't have Entra ID P1+
                print(f"[GraphClient] CA policies inaccessible (HTTP {e.response.status_code}) — skipping")
                return []
            raise
        all_value = result.get("value", []) or []
        while "@odata.nextLink" in result:
            result = await self._get(result["@odata.nextLink"])
            all_value.extend(result.get("value", []))
        rows = []
        for p in all_value:
            rows.append({
                "external_id": p.get("id"),
                "display_name": p.get("displayName") or "(unnamed CA policy)",
                "email": None,
                "type": "ENTRA_CONDITIONAL_ACCESS",
                "metadata": {
                    "state": p.get("state"),
                    "created_at": p.get("createdDateTime"),
                    "modified_at": p.get("modifiedDateTime"),
                    "raw": p,  # full definition cached for backup handler
                },
            })
        return rows

    async def get_conditional_access_policy(self, policy_id: str) -> Optional[Dict[str, Any]]:
        """Re-fetch a single CA policy by ID — used by the backup handler when
        the cached metadata is stale or missing."""
        url = f"{self.GRAPH_URL}/identity/conditionalAccess/policies/{policy_id}"
        try:
            return await self._get(url)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            raise

    # ── BitLocker recovery keys ─────────────────────────────────────────────
    # The list endpoint returns key metadata (id, deviceId, createdDateTime)
    # WITHOUT the key value. Reading the key value requires a separate GET to
    # /informationProtection/bitlocker/recoveryKeys/{id}?$select=key — and the
    # caller must have BitlockerKey.Read.All. We capture metadata at discovery
    # and pull the key bytes during backup so a least-privileged discovery
    # token still works.

    async def discover_bitlocker_keys(self) -> List[Dict[str, Any]]:
        """List BitLocker recovery key metadata across the tenant."""
        url = f"{self.GRAPH_URL}/informationProtection/bitlocker/recoveryKeys"
        try:
            result = await self._get(url, params={"$top": "200"})
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (401, 403, 404):
                print(f"[GraphClient] BitLocker keys inaccessible (HTTP {e.response.status_code}) — skipping")
                return []
            raise
        all_value = result.get("value", []) or []
        while "@odata.nextLink" in result:
            result = await self._get(result["@odata.nextLink"])
            all_value.extend(result.get("value", []))
        rows = []
        for k in all_value:
            kid = k.get("id")
            device_id = k.get("deviceId")
            volume_type = k.get("volumeType")
            rows.append({
                "external_id": kid,
                "display_name": f"BitLocker key — device {device_id} ({volume_type})" if device_id else (kid or "BitLocker key"),
                "email": None,
                "type": "ENTRA_BITLOCKER_KEY",
                "metadata": {
                    "device_id": device_id,
                    "volume_type": volume_type,
                    "created_at": k.get("createdDateTime"),
                },
            })
        return rows

    async def get_bitlocker_key_value(self, key_id: str) -> Optional[Dict[str, Any]]:
        """Fetch the actual recovery key bytes for a single BitLocker entry.
        Requires BitlockerKey.Read.All — separate from the metadata-only
        BitlockerKey.ReadBasic.All used by the list endpoint."""
        url = f"{self.GRAPH_URL}/informationProtection/bitlocker/recoveryKeys/{key_id}"
        try:
            # $select=key promotes the actual recovery key into the response
            return await self._get(url, params={"$select": "id,createdDateTime,deviceId,volumeType,key"})
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (403, 404):
                return None
            raise

    async def get_directory_audit_logs(self, filter_expr: str = None, top: int = 100) -> List[Dict[str, Any]]:
        """
        Get Microsoft Entra directory audit logs.
        Graph API: GET /auditLogs/directoryAudits
        Permission: AuditLog.Read.All
        """
        params = {"$top": min(top, 999)}
        if filter_expr:
            params["$filter"] = filter_expr

        return await self._paginated_get("/auditLogs/directoryAudits", params=params)

    async def get_sign_in_logs(self, filter_expr: str = None, top: int = 100) -> List[Dict[str, Any]]:
        """
        Get sign-in logs.
        Graph API: GET /auditLogs/signIns
        Permission: AuditLog.Read.All
        """
        params = {"$top": min(top, 999)}
        if filter_expr:
            params["$filter"] = filter_expr

        return await self._paginated_get("/auditLogs/signIns", params=params)

    # ==================== Backup-Specific Graph API Methods ====================

    async def get_sharepoint_site_drives(self, site_id: str, delta_token: str = None) -> Dict[str, Any]:
        """
        Get drive items from a SharePoint site using delta API.
        Graph API: GET /sites/{site-id}/drive/root/delta
        site_id format in DB: hostname/site-collection-id/site-id
        Graph API requires: hostname,site-collection-id,site-id

        NOTE: buffers the full drive into memory — only safe for small sites.
        For enterprise-scale sites, use ``iter_sharepoint_site_drive_items``.
        """
        # Convert slashes to commas for Graph API
        graph_site_id = site_id.replace("/", ",")
        url = f"{self.GRAPH_URL}/sites/{graph_site_id}/drive/root/delta"
        if delta_token:
            url = delta_token

        # No $select or $expand — delta endpoint ignores them
        params = {"$top": "999"}
        result = await self._get(url, params=params)
        all_value = result.get("value", [])

        # Follow pagination
        while "@odata.nextLink" in result:
            next_url = result["@odata.nextLink"]
            result = await self._get(next_url)
            all_value.extend(result.get("value", []))

        result["value"] = all_value
        return result

    async def iter_sharepoint_site_drive_items(
        self,
        site_id: str,
        delta_token: Optional[str] = None,
        delta_holder: Optional[Dict[str, Optional[str]]] = None,
    ):
        """Page-by-page async iterator over drive items of a SharePoint site.

        Yields raw drive-item dicts as each delta page arrives; the final
        ``@odata.deltaLink`` is written into ``delta_holder['deltaLink']``
        if provided so callers can persist the checkpoint after consuming
        the stream. Internal Graph pacing (``self._get``) already handles
        Retry-After, so we don't duplicate that here.

        NOTE: targets ``/sites/{id}/drive`` (singular) — the site's
        default document library only. Sites with multiple libraries need
        ``iter_sharepoint_site_all_drive_items`` instead; this method is
        kept for back-compat and single-drive callers.
        """
        graph_site_id = site_id.replace("/", ",")
        url = f"{self.GRAPH_URL}/sites/{graph_site_id}/drive/root/delta"
        if delta_token:
            url = delta_token
        params = {"$top": "999"}
        while url:
            page = await self._get(url, params=params)
            params = None
            for item in (page.get("value") or []):
                yield item
            if "@odata.nextLink" in page:
                url = page["@odata.nextLink"]
                continue
            if delta_holder is not None:
                delta_holder["deltaLink"] = page.get("@odata.deltaLink")
            break

    async def list_sharepoint_site_drives(self, site_id: str) -> List[Dict[str, Any]]:
        """Enumerate every drive (document library) attached to a
        SharePoint site. Returns raw drive dicts (at minimum each has
        ``id``, ``name``, ``driveType``). Paginated.

        Needed to back up sites whose content lives outside the default
        ``/sites/{id}/drive`` singleton — e.g. Team Sites with multiple
        libraries, or Communication Sites whose ``Pages`` library isn't
        the default one."""
        graph_site_id = site_id.replace("/", ",")
        url = f"{self.GRAPH_URL}/sites/{graph_site_id}/drives"
        params: Optional[Dict[str, str]] = {"$top": "200"}
        out: List[Dict[str, Any]] = []
        while url:
            page = await self._get(url, params=params)
            params = None
            out.extend(page.get("value") or [])
            url = page.get("@odata.nextLink")
        return out

    async def iter_drive_items_by_id(
        self,
        drive_id: str,
        delta_token: Optional[str] = None,
        delta_holder: Optional[Dict[str, Optional[str]]] = None,
    ):
        """Delta-iterate items of a specific drive. Same shape as
        ``iter_sharepoint_site_drive_items`` but targets ``/drives/{id}``
        directly so multi-library SharePoint sites can walk each drive
        independently."""
        url = f"{self.GRAPH_URL}/drives/{drive_id}/root/delta"
        if delta_token:
            url = delta_token
        params = {"$top": "999"}
        while url:
            page = await self._get(url, params=params)
            params = None
            for item in (page.get("value") or []):
                # Tag with drive id so the backup-worker's file-row
                # builder can partition blob paths correctly even when
                # the upstream producer aggregates drives.
                if "_drive_id" not in item:
                    item["_drive_id"] = drive_id
                yield item
            if "@odata.nextLink" in page:
                url = page["@odata.nextLink"]
                continue
            if delta_holder is not None:
                delta_holder["deltaLink"] = page.get("@odata.deltaLink")
            break

    async def iter_sharepoint_site_all_drive_items(
        self,
        site_id: str,
        drive_delta_tokens: Optional[Dict[str, str]] = None,
        drive_delta_holder: Optional[Dict[str, Dict[str, Optional[str]]]] = None,
        allowed_drive_ids: Optional[Set[str]] = None,
    ):
        """Enumerate every drive on the site, then delta-iterate each.
        Yields the same drive-item dicts as
        ``iter_sharepoint_site_drive_items`` plus ``_drive_id`` and
        ``_drive_name`` so callers can tell which library an item came
        from.

        ``drive_delta_tokens`` — optional ``{drive_id: deltaLink}`` read
        from the previous run to resume per-drive.

        ``drive_delta_holder`` — optional ``{drive_id: {"deltaLink": ...}}``
        written during iteration so callers can persist the new tokens
        after consuming the stream.

        ``allowed_drive_ids`` — optional drive_id allowlist. When set,
        drives not in the set are skipped entirely (per-shard scoping
        for the cross-replica partition split). Default ``None`` means
        iterate every drive on the site (full-site backup path).
        """
        drive_delta_tokens = drive_delta_tokens or {}
        drives = await self.list_sharepoint_site_drives(site_id)
        for drv in drives:
            drive_id = drv.get("id")
            drive_name = drv.get("name") or drive_id
            if not drive_id:
                continue
            if allowed_drive_ids is not None and drive_id not in allowed_drive_ids:
                continue
            local_holder: Dict[str, Optional[str]] = {"deltaLink": None}
            async for item in self.iter_drive_items_by_id(
                drive_id, drive_delta_tokens.get(drive_id), local_holder,
            ):
                item["_drive_name"] = drive_name
                yield item
            if drive_delta_holder is not None and local_holder.get("deltaLink"):
                drive_delta_holder[drive_id] = local_holder

    async def get_sharepoint_subsites(self, site_id: str) -> Dict[str, Any]:
        """
        Get subsites for a SharePoint site.
        Graph API: GET /sites/{site-id}/sites
        """
        graph_site_id = site_id.replace("/", ",")
        result = await self._get(f"{self.GRAPH_URL}/sites/{graph_site_id}/sites", params={"$top": "999"})
        all_value = result.get("value", [])
        while "@odata.nextLink" in result:
            result = await self._get(result["@odata.nextLink"])
            all_value.extend(result.get("value", []))
        result["value"] = all_value
        return result

    async def create_communication_site(
        self,
        *,
        title: str,
        alias: str,
        owner_email: Optional[str] = None,
        lcid: int = 1033,
    ) -> str:
        """Provision a fresh SharePoint Communication Site and return its
        Graph site id (``hostname/site-collection-id/site-id``).

        Uses SPO tenant-admin REST ``_api/SPSiteManager/create`` — the same
        endpoint used by the SharePoint admin UI and SPO CLI. Requires the
        app to have ``Sites.FullControl.All`` (or a tenant-admin-consented
        scope). We block on ``SiteStatus=2`` (provisioned); SiteStatus=1 is
        still creating, SiteStatus=3 is failed.
        """
        tenant_host = await self._get_default_sharepoint_hostname()
        tenant_name = tenant_host.split(".")[0]
        admin_host = f"{tenant_name}-admin.sharepoint.com"
        site_url = f"https://{tenant_host}/sites/{alias}"

        token = await self._get_sharepoint_token(admin_host)
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json;odata=nometadata",
            "Content-Type": "application/json;odata=nometadata",
        }
        payload: Dict[str, Any] = {
            "request": {
                "Title": title,
                "Url": site_url,
                "Lcid": lcid,
                "ShareByEmailEnabled": False,
                "WebTemplate": "SITEPAGEPUBLISHING#0",
                # Built-in "Topic" site design — stable GUID documented by MS.
                "SiteDesignId": "f6cc5403-0d63-442e-96c0-285923709ffc",
            }
        }
        if owner_email:
            payload["request"]["Owner"] = owner_email

        url = f"https://{admin_host}/_api/SPSiteManager/create"
        async with self._http_session() as client:
            # Wait up to ~5 min for provisioning — communication sites are
            # usually ready in <60s, but a cold tenant can lag.
            deadline = time.monotonic() + 300
            site_status = 0
            while time.monotonic() < deadline:
                resp = await client.post(url, headers=headers, json=payload)
                if resp.status_code in (429, 503):
                    await asyncio.sleep(_parse_retry_after(resp))
                    continue
                if resp.status_code >= 400:
                    raise RuntimeError(
                        f"create_communication_site {resp.status_code}: {resp.text[:300]}"
                    )
                body = resp.json()
                site_status = int(body.get("SiteStatus") or 0)
                if site_status == 2:
                    break
                if site_status == 3:
                    raise RuntimeError(f"Site provisioning failed: {body}")
                await asyncio.sleep(5)
            if site_status != 2:
                raise RuntimeError(f"Site provisioning timed out after 300s (status={site_status})")

        # Resolve to Graph's composite site id via path lookup.
        site_meta = await self._get(
            f"{self.GRAPH_URL}/sites/{tenant_host}:/sites/{alias}"
        )
        site_id_raw = site_meta.get("id") or ""
        return site_id_raw.replace(",", "/")

    async def _get_default_sharepoint_hostname(self) -> str:
        """Derive the tenant's default SharePoint hostname
        (``{tenant}.sharepoint.com``) from the root site."""
        root = await self._get(f"{self.GRAPH_URL}/sites/root")
        web_url = root.get("webUrl") or ""
        from urllib.parse import urlparse as _up
        parsed = _up(web_url)
        if not parsed.netloc:
            raise RuntimeError(f"Could not resolve SharePoint hostname from root site: {root}")
        return parsed.netloc

    async def get_sharepoint_site_lists(self, site_id: str) -> Dict[str, Any]:
        """
        Get SharePoint site lists.
        Graph API: GET /sites/{site-id}/lists

        Normalises the DB-stored ``hostname/site-collection-id/site-id``
        back to Graph's required ``hostname,site-collection-id,site-id``.
        Without this conversion Graph returns 400 (interprets the slashes
        as additional URL segments).

        Graph's /lists endpoint only returns user-facing content lists —
        it filters out hidden system catalogs like _catalogs/masterpage,
        _catalogs/theme, TaxonomyHiddenList, User Information List, etc.
        For full parity we ALSO call SharePoint REST API at
        ``{siteUrl}/_api/web/lists`` which returns every list including
        the _catalogs/*, and merge the two results de-duped by id. That
        requires a SharePoint-scoped token (different audience from
        Graph) minted via ``_get_sharepoint_token``.
        """
        graph_site_id = site_id.replace("/", ",")
        # Graph pass — user-facing content lists.
        graph_result = await self._get(
            f"{self.GRAPH_URL}/sites/{graph_site_id}/lists",
            params={
                "$top": "999",
                "$select": "id,name,displayName,description,webUrl,createdDateTime,lastModifiedDateTime,lastModifiedBy,system,list",
            },
        )
        graph_lists = graph_result.get("value") or []
        seen_ids = {l.get("id") for l in graph_lists if l.get("id")}

        # SharePoint REST pass — picks up system / _catalogs lists that
        # Graph silently filters. Best-effort: if auth/site-URL lookup
        # fails we still return whatever Graph gave us.
        sp_lists: List[Dict[str, Any]] = []
        try:
            # Derive site URL from the composite id:
            #   "qfion.sharepoint.com,<scid>,<webid>" → "qfion.sharepoint.com"
            parts = site_id.replace(",", "/").split("/")
            hostname = parts[0] if parts else ""
            if hostname:
                # The site's webUrl lives on the Graph response itself.
                # First look for the root site in the existing Graph
                # result; otherwise fetch it.
                site_web_url = None
                for l in graph_lists:
                    # parentReference.siteUrl isn't a standard field but
                    # webUrl of any list lets us derive the site root.
                    web_url = l.get("webUrl") or ""
                    if web_url:
                        # https://<host>/... → strip any /Lists/... suffix
                        from urllib.parse import urlparse
                        parsed = urlparse(web_url)
                        site_web_url = f"{parsed.scheme}://{parsed.netloc}"
                        # Include site path if present (e.g. /sites/Foo).
                        path_parts = parsed.path.strip("/").split("/")
                        if len(path_parts) >= 2 and path_parts[0] == "sites":
                            site_web_url += f"/sites/{path_parts[1]}"
                        break
                # Fallback to Graph's /sites/{id}?$select=webUrl
                if not site_web_url:
                    site_meta = await self._get(
                        f"{self.GRAPH_URL}/sites/{graph_site_id}",
                        params={"$select": "webUrl"},
                    )
                    site_web_url = site_meta.get("webUrl")

                if site_web_url:
                    sp_lists = await self._get_sharepoint_lists_via_rest(hostname, site_web_url)
        except Exception as exc:
            print(f"[GraphClient] SP REST lists fetch failed for {site_id}: {type(exc).__name__}: {exc}")

        # Merge — prefer Graph's shape when both sources have the same id.
        merged = list(graph_lists)
        for rl in sp_lists:
            rl_id = rl.get("id")
            if rl_id and rl_id not in seen_ids:
                merged.append(rl)
                seen_ids.add(rl_id)

        graph_result["value"] = merged
        return graph_result

    async def _get_sharepoint_token(self, hostname: str) -> str:
        """Mint a SharePoint-scoped access token using the same app creds.

        SharePoint REST requires a token with audience
        ``https://<tenant>.sharepoint.com`` AND — unlike Graph — it
        refuses client-secret-based app-only tokens outright
        ("Unsupported app only token"). The only way to get a token
        SharePoint REST will accept is to sign a JWT assertion with a
        certificate whose public key is registered on the AAD app.

        Auth path selection:
          • If SHAREPOINT_CERT_PATH (or SHAREPOINT_CERT_PEM_B64) and
            SHAREPOINT_CERT_THUMBPRINT are configured → JWT assertion
            flow (accepted by SP REST).
          • Otherwise → client-secret flow (Graph accepts, SP REST
            rejects — only useful for diagnostics).

        Tokens cached per-hostname for ~55 minutes. Cert loading is
        also cached so every token refresh doesn't re-parse the PEM.
        """
        if not hasattr(self, "_sp_tokens"):
            self._sp_tokens = {}
        cached = self._sp_tokens.get(hostname)
        if cached and cached[1] > datetime.utcnow():
            return cached[0]

        scope = f"https://{hostname}/.default"
        token_url = self.TOKEN_URL.format(tenant_id=self.tenant_id)

        # Prefer cert-based assertion when configured — this is the
        # only path that yields SP-REST-acceptable tokens.
        assertion = self._build_sharepoint_client_assertion(token_url)
        if assertion:
            data = {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "scope": scope,
                "client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
                "client_assertion": assertion,
            }
        else:
            # Fallback — won't work against SP REST but useful for
            # development and Graph-only call sites.
            data = {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "scope": scope,
            }

        async with self._http_session() as client:
            resp = await client.post(token_url, data=data)
            if resp.status_code >= 400:
                # Surface AAD's error text — it's the only way to tell
                # cert-thumbprint-mismatch from missing-permission.
                print(f"[GraphClient] SP token POST failed ({resp.status_code}): {resp.text[:400]}")
            resp.raise_for_status()
            payload = resp.json()
            token = payload["access_token"]
            expires_in = payload.get("expires_in", 3600)
            self._sp_tokens[hostname] = (token, datetime.utcnow() + timedelta(seconds=expires_in - 300))
            return token

    # ------------------------------------------------------------------
    # Cert-based client-assertion helpers
    # ------------------------------------------------------------------
    _sp_cert_cache: Optional[tuple] = None  # (pem_bytes, thumbprint_b64url_bytes)

    @classmethod
    def _load_sharepoint_cert(cls) -> Optional[tuple]:
        """Load the PEM + derive the base64url-encoded SHA-1 thumbprint
        that Azure AD expects in the JWT header ``x5t`` claim.

        Reads SHAREPOINT_CERT_PEM_B64 first (base64-encoded PEM for
        env-var deployments), falls back to SHAREPOINT_CERT_PATH (file
        path for local dev with a mounted PEM). Returns None if neither
        is set — caller falls back to the secret flow.
        """
        if cls._sp_cert_cache is not None:
            return cls._sp_cert_cache

        import base64 as _b64
        import os as _os
        from cryptography import x509 as _x509
        from cryptography.hazmat.primitives import hashes as _hashes
        from cryptography.hazmat.primitives import serialization as _ser

        pem_b64 = (_os.getenv("SHAREPOINT_CERT_PEM_B64") or "").strip()
        pem_path = (_os.getenv("SHAREPOINT_CERT_PATH") or "").strip()
        explicit_thumbprint = (_os.getenv("SHAREPOINT_CERT_THUMBPRINT") or "").strip().replace(":", "").lower()

        pem_bytes: Optional[bytes] = None
        if pem_b64:
            try:
                pem_bytes = _b64.b64decode(pem_b64)
            except Exception as e:
                print(f"[GraphClient] SHAREPOINT_CERT_PEM_B64 invalid base64: {e}")
        elif pem_path:
            try:
                with open(pem_path, "rb") as f:
                    pem_bytes = f.read()
            except Exception as e:
                print(f"[GraphClient] SHAREPOINT_CERT_PATH read failed: {e}")
        if not pem_bytes:
            return None

        # Extract the public cert to derive the thumbprint. The PEM
        # file may also contain the private key — load_pem_x509_certificates
        # tolerates that by picking up the CERTIFICATE block.
        try:
            cert = _x509.load_pem_x509_certificate(pem_bytes)
        except Exception:
            # Fallback: some writers emit just the private key; Azure AD
            # requires the thumbprint from the public cert. If there's
            # no cert in the bundle we can't continue.
            try:
                certs = _x509.load_pem_x509_certificates(pem_bytes)
                cert = certs[0] if certs else None
            except Exception as e:
                print(f"[GraphClient] SHAREPOINT_CERT_* PEM does not contain a usable X.509 cert: {e}")
                return None
        if cert is None:
            return None

        thumbprint_hex = cert.fingerprint(_hashes.SHA1()).hex().lower()
        if explicit_thumbprint and explicit_thumbprint != thumbprint_hex:
            print(f"[GraphClient] Warning: SHAREPOINT_CERT_THUMBPRINT ({explicit_thumbprint}) doesn't match cert fingerprint ({thumbprint_hex}); using the cert's real fingerprint.")
        # Azure AD expects the SHA-1 fingerprint in base64url of the
        # raw bytes (NOT hex) in the JWT header's `x5t` claim.
        thumbprint_b64url = _b64.urlsafe_b64encode(cert.fingerprint(_hashes.SHA1())).rstrip(b"=")

        cls._sp_cert_cache = (pem_bytes, thumbprint_b64url)
        return cls._sp_cert_cache

    def _build_sharepoint_client_assertion(self, token_url: str) -> Optional[str]:
        """Build the signed JWT assertion that proves we control the cert
        whose public key is registered on the AAD app. Returns None when
        no cert is configured — caller falls back to the secret flow."""
        loaded = self._load_sharepoint_cert()
        if not loaded:
            return None
        pem_bytes, thumbprint_b64url = loaded

        import uuid as _uuid
        import jwt as _jwt

        now = int(datetime.utcnow().timestamp())
        payload = {
            "aud": token_url,
            "iss": self.client_id,
            "sub": self.client_id,
            "jti": str(_uuid.uuid4()),
            "nbf": now,
            "exp": now + 10 * 60,  # 10 minutes; AAD caps at 10
        }
        # The `x5t` header is what tells AAD which uploaded cert
        # matches the signature on this assertion.
        headers = {"alg": "RS256", "typ": "JWT", "x5t": thumbprint_b64url.decode()}
        try:
            return _jwt.encode(payload, pem_bytes, algorithm="RS256", headers=headers)
        except Exception as e:
            print(f"[GraphClient] Failed to build SP client assertion: {type(e).__name__}: {e}")
            return None

    async def _get_sharepoint_lists_via_rest(self, hostname: str, site_web_url: str) -> List[Dict[str, Any]]:
        """Hit SharePoint REST ``_api/web/lists`` to pick up every list
        including system catalogs that Graph hides.

        Returns list objects normalised to roughly the Graph shape so
        callers don't need to know which source each list came from.
        """
        token = await self._get_sharepoint_token(hostname)
        url = f"{site_web_url.rstrip('/')}/_api/web/lists"
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json;odata=nometadata",
        }
        # `$select` reduces response size + matches what Graph gives us.
        params = {
            "$top": "5000",
            "$select": "Id,Title,Description,DefaultViewUrl,Created,LastItemModifiedDate,BaseTemplate,Hidden,IsCatalog,IsSystemList",
        }

        async with self._http_session() as client:
            resp = await client.get(url, headers=headers, params=params)
            if resp.status_code == 403 or resp.status_code == 401:
                print(f"[GraphClient] SP REST denied for {site_web_url} (status={resp.status_code}); is AllSites.Read granted on THIS app registration?")
                return []
            resp.raise_for_status()
            data = resp.json()

        raw = data.get("value") or data.get("d", {}).get("results") or []
        out: List[Dict[str, Any]] = []
        # Derive a Graph-style composite site id from the webUrl so callers
        # that key on list.id can still join this back to the site row.
        for lst in raw:
            lst_id = (lst.get("Id") or "").lower().strip("{}")
            if not lst_id:
                continue
            default_view = lst.get("DefaultViewUrl") or ""
            list_web_url = f"https://{hostname}{default_view}" if default_view.startswith("/") else default_view
            # Strip the trailing /AllItems.aspx or /AllListItems.aspx
            if list_web_url:
                if "/Forms/" in list_web_url:
                    list_web_url = list_web_url.split("/Forms/")[0]
                elif list_web_url.endswith("/AllItems.aspx"):
                    list_web_url = list_web_url[: -len("/AllItems.aspx")]
            out.append({
                "id": lst_id,
                "name": lst.get("Title"),
                "displayName": lst.get("Title"),
                "description": lst.get("Description") or "",
                "webUrl": list_web_url,
                "createdDateTime": lst.get("Created"),
                "lastModifiedDateTime": lst.get("LastItemModifiedDate"),
                "lastModifiedBy": None,
                "system": bool(lst.get("IsSystemList")),
                "list": {
                    "template": lst.get("BaseTemplate"),
                    "hidden": bool(lst.get("Hidden")),
                    "contentTypesEnabled": None,
                },
                # Mark the source so callers (and debugging) can tell.
                "_source": "sp_rest",
                "_is_catalog": bool(lst.get("IsCatalog")),
            })
        return out

    async def get_sharepoint_list_items_via_rest(self, hostname: str, site_web_url: str, list_id: str) -> List[Dict[str, Any]]:
        """Fetch ALL items from a SharePoint list via SP REST.

        Thin wrapper over ``iter_sharepoint_list_items_via_rest`` for callers
        that want the materialised list. Enterprise-scale callers (bounded
        producer/consumer queues) should use the async iterator directly so
        pagination streams instead of buffering the whole list in memory.
        """
        out: List[Dict[str, Any]] = []
        async for row in self.iter_sharepoint_list_items_via_rest(hostname, site_web_url, list_id):
            out.append(row)
        return out

    async def iter_sharepoint_list_items_via_rest(
        self,
        hostname: str,
        site_web_url: str,
        list_id: str,
        page_size: int = 1000,
        all_fields: bool = True,
    ):
        """Streaming page-by-page iterator over SP REST list items.

        Why: libraries on a heavy tenant can hold millions of rows; the old
        buffered wrapper kept every row in memory before returning. This
        iterator yields row-by-row as pages arrive and honours 429 /
        Retry-After so we cooperate with SP throttling instead of stampeding
        it on restart.

        ``all_fields=True`` drops the narrow $select and asks SP to $expand
        FieldValuesAsText so every custom column comes back as a readable
        string. Needed for catalog lists (Composed Looks, Master Page
        Gallery, Theme Gallery, …) where the interesting data lives in
        columns like MasterPageUrl / ThemeUrl / ImageUrl / FontSchemeUrl,
        not the underlying file bytes.
        """
        token = await self._get_sharepoint_token(hostname)
        url = f"{site_web_url.rstrip('/')}/_api/web/lists(guid'{list_id}')/items"
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json;odata=nometadata",
        }
        # File-reference columns (FileRef / FileLeafRef / FileSystemObjectType)
        # aren't always returned by the default projection of SP REST's
        # /items endpoint — some tenants drop them, leaving the backup
        # worker's "do I stream bytes for this row?" check false for
        # every row, producing 0-byte snapshots for SitePages and
        # other list-hosted content.
        #
        # $select is strict-all-or-nothing: if ANY field doesn't exist
        # on a given list type, SP REST 400s the whole request. Only
        # include the file-reference fields — they're defined on every
        # list type. Extras like Length / Title / Modified have
        # fire-tested as absent on catalog / hidden / tasks lists.
        if all_fields:
            # Catalog lists: drop $select so every declared column comes
            # back, and $expand FieldValuesAsText so lookup/url columns
            # render as strings (not OData shells).
            params: Optional[Dict[str, str]] = {
                "$top": str(page_size),
                "$expand": "FieldValuesAsText",
            }
        else:
            params = {
                "$top": str(page_size),
                "$select": "Id,FileRef,FileLeafRef,FileSystemObjectType",
            }
        next_url: Optional[str] = url
        backoff = 1.0  # seconds; doubled per consecutive transient failure
        async with self._http_session() as client:
            while next_url:
                try:
                    resp = await client.get(
                        next_url,
                        headers=headers,
                        params=params if next_url == url else None,
                    )
                except (httpx.ConnectError, httpx.ReadTimeout, httpx.RemoteProtocolError) as exc:
                    if backoff > 60:
                        print(f"[GraphClient] SP REST items transport giving up after {backoff}s: {exc}")
                        return
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 60.0)
                    continue

                if resp.status_code == 429 or resp.status_code == 503:
                    retry_after = _parse_retry_after(resp)
                    print(f"[GraphClient] SP REST 429/503 for list {list_id}, sleeping {retry_after:.1f}s")
                    await asyncio.sleep(retry_after)
                    continue  # retry same URL
                if resp.status_code in (401, 403):
                    print(f"[GraphClient] SP REST items denied for list {list_id} ({resp.status_code}): {resp.text[:180]}")
                    return
                if resp.status_code >= 400:
                    print(f"[GraphClient] SP REST items {resp.status_code} for list {list_id}: {resp.text[:180]}")
                    return
                backoff = 1.0  # reset after a good page
                data = resp.json()
                for row in (data.get("value") or []):
                    yield row
                next_url = data.get("odata.nextLink") or data.get("@odata.nextLink")
                params = None  # next_url already includes query

    async def get_sharepoint_file_metadata_via_rest(self, hostname: str, site_web_url: str, server_relative_url: str) -> Optional[Dict[str, Any]]:
        """HEAD-style metadata for a SharePoint file (Length, Name,
        TimeLastModified, UIVersionLabel) without downloading bytes.
        Returns None on any failure (treated as non-existent / not a file).
        """
        token = await self._get_sharepoint_token(hostname)
        safe_path = server_relative_url.replace("'", "''")
        url = f"{site_web_url.rstrip('/')}/_api/web/GetFileByServerRelativeUrl('{safe_path}')"
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json;odata=nometadata",
        }
        async with self._http_session() as client:
            resp = await client.get(url, headers=headers)
            if resp.status_code >= 400:
                return None
            try:
                return resp.json()
            except Exception:
                return None

    async def download_sharepoint_file_via_rest(self, hostname: str, site_web_url: str, server_relative_url: str) -> bytes:
        """Buffered download of a SharePoint file by its server-relative URL.

        NOTE: reads the whole body into memory — only safe for small catalog
        entries. Enterprise-scale callers backing up real document libraries
        should use ``stream_sharepoint_file_via_rest`` to avoid OOM on large
        files.
        """
        token = await self._get_sharepoint_token(hostname)
        safe_path = server_relative_url.replace("'", "''")
        url = f"{site_web_url.rstrip('/')}/_api/web/GetFileByServerRelativeUrl('{safe_path}')/$value"
        async with self._http_session() as client:
            resp = await client.get(url, headers={"Authorization": f"Bearer {token}"})
            resp.raise_for_status()
            return resp.content

    async def stream_sharepoint_file_via_rest(
        self,
        hostname: str,
        site_web_url: str,
        server_relative_url: str,
        chunk_size: int = 1 * 1024 * 1024,
    ):
        """Stream a SharePoint file's bytes via SP REST ``$value`` endpoint.

        Yields raw chunks so callers can pipe directly into a temp file /
        block-blob uploader without buffering the full body. Honours 429 /
        503 Retry-After by retrying the same URL after the indicated delay.
        Falls through to a single retry on connection errors.
        """
        token = await self._get_sharepoint_token(hostname)
        safe_path = server_relative_url.replace("'", "''")
        url = f"{site_web_url.rstrip('/')}/_api/web/GetFileByServerRelativeUrl('{safe_path}')/$value"
        headers = {"Authorization": f"Bearer {token}"}
        attempt = 0
        while True:
            attempt += 1
            try:
                async with self._http_session() as client:
                    async with client.stream("GET", url, headers=headers) as resp:
                        if resp.status_code in (429, 503):
                            retry_after = _parse_retry_after(resp)
                            print(f"[GraphClient] SP REST stream 429/503 for {server_relative_url}, sleeping {retry_after:.1f}s")
                            await asyncio.sleep(retry_after)
                            continue
                        resp.raise_for_status()
                        async for chunk in resp.aiter_bytes(chunk_size):
                            if chunk:
                                yield chunk
                        return
            except (httpx.ConnectError, httpx.ReadTimeout, httpx.RemoteProtocolError) as exc:
                if attempt >= 3:
                    raise
                await asyncio.sleep(min(2 ** attempt, 30))

    async def get_sharepoint_site_list_items(self, site_id: str, list_id: str, delta_token: str = None) -> Dict[str, Any]:
        """
        Get items from a SharePoint list using delta API.
        Graph API: GET /sites/{site-id}/lists/{list-id}/items/delta

        Same slash→comma normalisation as the other SharePoint helpers.
        """
        graph_site_id = site_id.replace("/", ",")
        url = f"{self.GRAPH_URL}/sites/{graph_site_id}/lists/{list_id}/items/delta"
        if delta_token:
            url = delta_token

        # Expand both fields (list-row columns) AND driveItem so library
        # items come back with the drive/item id needed to download bytes.
        params = {"$expand": "fields,driveItem", "$top": "999"}
        result = await self._get(url, params=params)
        all_value = result.get("value", [])

        # Follow pagination
        while "@odata.nextLink" in result:
            next_url = result["@odata.nextLink"]
            result = await self._get(next_url)
            all_value.extend(result.get("value", []))

        result["value"] = all_value
        return result

    async def download_drive_item_bytes(self, drive_id: str, item_id: str) -> bytes:
        """Stream a SharePoint/OneDrive driveItem's content as raw bytes.
        Graph API: GET /drives/{drive-id}/items/{item-id}/content
        """
        token = await self._get_token()
        url = f"{self.GRAPH_URL}/drives/{drive_id}/items/{item_id}/content"
        async with self._http_session() as client:
            resp = await client.get(url, headers={"Authorization": f"Bearer {token}"})
            resp.raise_for_status()
            return resp.content

    async def get_site_permissions(self, site_id: str) -> Dict[str, Any]:
        """
        Get SharePoint site permissions.
        Graph API: GET /sites/{site-id}/permissions
        """
        return await self._get(f"{self.GRAPH_URL}/sites/{site_id}/permissions")

    async def get_teams_channels(self, team_id: str) -> Dict[str, Any]:
        """
        Get channels in a Teams team.
        Graph API: GET /teams/{team-id}/channels
        """
        result = await self._get(f"{self.GRAPH_URL}/teams/{team_id}/channels", params={"$top": "999"})
        all_value = result.get("value", [])

        # Follow pagination
        while "@odata.nextLink" in result:
            next_url = result["@odata.nextLink"]
            result = await self._get(next_url)
            all_value.extend(result.get("value", []))

        result["value"] = all_value
        return result

    async def get_channel_messages(self, team_id: str, channel_id: str, delta_token: str = None) -> Dict[str, Any]:
        """
        Get messages from a Teams channel using delta API.
        Graph API: GET /teams/{team-id}/channels/{channel-id}/messages/delta
        """
        url = f"{self.GRAPH_URL}/teams/{team_id}/channels/{channel_id}/messages/delta"
        if delta_token:
            url = delta_token

        params = {"$top": "999"}
        result = await self._get(url, params=params)
        all_value = result.get("value", [])

        # Follow pagination
        while "@odata.nextLink" in result:
            next_url = result["@odata.nextLink"]
            result = await self._get(next_url)
            all_value.extend(result.get("value", []))

        result["value"] = all_value
        return result

    async def get_channel_messages_replies(self, team_id: str, channel_id: str, message_id: str) -> Dict[str, Any]:
        """
        Get replies to a Teams channel message.
        Graph API: GET /teams/{team-id}/channels/{channel-id}/messages/{message-id}/replies
        """
        return await self._get(
            f"{self.GRAPH_URL}/teams/{team_id}/channels/{channel_id}/messages/{message_id}/replies",
            params={"$top": "999"}
        )

    async def get_teams_chats(self, delta_token: str = None) -> Dict[str, Any]:
        """Get all Teams chats accessible to the app.

        /chats/delta is NOT in the v1.0 Graph reference (was previously called here
        but never documented). We now scope by user: /users/{id}/chats. For
        organization-wide chat export the documented approach is
        /users/{id}/chats/getAllMessages.

        Kept API-compatible: callers may still pass delta_token from a previous
        nextLink response and it'll be used verbatim."""
        if delta_token:
            url = delta_token
        else:
            url = f"{self.GRAPH_URL}/chats"
        params = {"$top": "999", "$expand": "members,permission"}
        result = await self._get(url, params=params)
        all_value = result.get("value", [])

        # Follow pagination
        while "@odata.nextLink" in result:
            next_url = result["@odata.nextLink"]
            result = await self._get(next_url)
            all_value.extend(result.get("value", []))

        result["value"] = all_value
        return result

    async def get_all_chat_messages_for_user(self, user_id: str) -> Dict[str, Any]:
        """Export all chat messages a user is part of.

        Graph API: GET /users/{id}/chats/getAllMessages
        Permission: Chat.Read.All (or ChatMessage.Read.All). This is the documented
        replacement for the undocumented /chats/delta used previously."""
        url = f"{self.GRAPH_URL}/users/{user_id}/chats/getAllMessages"
        params = {"$top": "50"}
        result = await self._get(url, params=params)
        all_value = result.get("value", [])
        while "@odata.nextLink" in result:
            result = await self._get(result["@odata.nextLink"])
            all_value.extend(result.get("value", []))
        result["value"] = all_value
        return result

    async def get_all_chat_messages_for_user_delta(
        self, user_id: str, delta_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """Incremental chat export via delta query.

        Graph API: GET /users/{id}/chats/getAllMessages/delta
        Permission: Chat.Read.All (same as the non-delta endpoint).

        First call (no delta_token): full sync, returns every message and an
        @odata.deltaLink. Subsequent calls (delta_token = previous deltaLink):
        only messages added/changed since the last sync.

        Hard limit: delta only covers the last 8 months. If the token is
        expired or too old, Graph returns 410/400 and callers should fall
        back to get_all_chat_messages_for_user() for a full reseed.
        """
        if delta_token:
            # A deltaLink IS the full URL — use it verbatim, no extra params.
            url = delta_token
            params = None
        else:
            url = f"{self.GRAPH_URL}/users/{user_id}/chats/getAllMessages/delta"
            params = {"$top": "50"}
        # _get already paginates via @odata.nextLink and preserves @odata.deltaLink.
        return await self._get(url, params=params)

    async def iter_all_chat_messages_for_user_delta(
        self, user_id: str, delta_token: Optional[str] = None,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Streaming counterpart to get_all_chat_messages_for_user_delta.

        Yields one page dict at a time ({"value": [...], "@odata.deltaLink"?,
        "@odata.nextLink"?, ...}), letting callers pipeline uploads and DB
        writes mid-stream instead of buffering every message in RAM. This is
        the single biggest speedup for heavy users: Azure uploads can fire
        concurrently while the next Graph page is still in flight.
        """
        if delta_token:
            url = delta_token
            params = None
        else:
            url = f"{self.GRAPH_URL}/users/{user_id}/chats/getAllMessages/delta"
            params = {"$top": "50"}
        async for page in self._iter_pages(url, params=params):
            yield page

    async def get_chat_messages(self, chat_id: str, delta_token: str = None) -> Dict[str, Any]:
        """
        Get messages from a Teams chat.
        Note: /messages/delta is NOT supported for chat messages (MS Graph limitation).
        Graph API: GET /chats/{chat-id}/messages
        """
        url = f"{self.GRAPH_URL}/chats/{chat_id}/messages"
        params = {"$top": "999"}
        result = await self._get(url, params=params)
        all_value = result.get("value", [])

        # Follow pagination
        while "@odata.nextLink" in result:
            next_url = result["@odata.nextLink"]
            result = await self._get(next_url)
            all_value.extend(result.get("value", []))

        result["value"] = all_value
        return result

    async def count_chat_messages(self, chat_id: str) -> Optional[int]:
        """Return Graph's authoritative message count for a chat thread.

        Used by the audit-service's nightly integrity verifier (plan P4)
        to compare against `chat_thread_messages` row count and detect
        silent drops from a prior drain. Returns None if Graph rejects
        the call (permission, 404, throttle) so the caller skips.

        Graph's chat-messages endpoint does NOT support `$count=true`, so
        we paginate and tally. With `$top=999` this is typically 1-3
        round trips per chat — acceptable for a once-a-day sweep.
        """
        try:
            url = f"{self.GRAPH_URL}/chats/{chat_id}/messages"
            params = {"$top": "999"}
            total = 0
            result = await self._get(url, params=params)
            total += len(result.get("value", []) or [])
            while "@odata.nextLink" in result:
                next_url = result["@odata.nextLink"]
                result = await self._get(next_url)
                total += len(result.get("value", []) or [])
            return total
        except Exception:
            return None

    async def get_group_profile(self, group_id: str) -> Dict[str, Any]:
        """
        Get Entra ID group profile.
        Graph API: GET /groups/{id}
        """
        return await self._get(f"{self.GRAPH_URL}/groups/{group_id}")

    async def get_user_profile(self, user_id: str) -> Dict[str, Any]:
        """
        Get detailed user profile.
        Graph API: GET /users/{id}
        """
        return await self._get(f"{self.GRAPH_URL}/users/{user_id}")

    async def get_user_manager(self, user_id: str) -> Dict[str, Any]:
        """
        Get user's manager.
        Graph API: GET /users/{id}/manager
        """
        try:
            return await self._get(f"{self.GRAPH_URL}/users/{user_id}/manager")
        except Exception:
            return {}

    async def get_user_direct_reports(self, user_id: str) -> Dict[str, Any]:
        """
        Get user's direct reports.
        Graph API: GET /users/{id}/directReports
        """
        return await self._get(f"{self.GRAPH_URL}/users/{user_id}/directReports", params={"$top": "999"})

    async def get_user_group_memberships(self, user_id: str) -> Dict[str, Any]:
        """
        Get user's group memberships.
        Graph API: GET /users/{id}/memberOf
        """
        return await self._get(f"{self.GRAPH_URL}/users/{user_id}/memberOf", params={"$top": "999"})

    async def get_group_members(self, group_id: str) -> Dict[str, Any]:
        """
        Get group members.
        Graph API: GET /groups/{id}/members
        """
        return await self._get(f"{self.GRAPH_URL}/groups/{group_id}/members", params={"$top": "999"})

    async def get_group_owners(self, group_id: str) -> Dict[str, Any]:
        """
        Get group owners.
        Graph API: GET /groups/{id}/owners
        """
        return await self._get(f"{self.GRAPH_URL}/groups/{group_id}/owners", params={"$top": "999"})

    async def get_entra_apps(self) -> Dict[str, Any]:
        """
        Get Entra ID application registrations.
        Graph API: GET /applications
        """
        result = await self._get(f"{self.GRAPH_URL}/applications", params={"$top": "999"})
        all_value = result.get("value", [])
        while "@odata.nextLink" in result:
            result = await self._get(result["@odata.nextLink"])
            all_value.extend(result.get("value", []))
        result["value"] = all_value
        return result

    async def get_entra_service_principals(self) -> Dict[str, Any]:
        """
        Get service principals.
        Graph API: GET /servicePrincipals
        """
        result = await self._get(f"{self.GRAPH_URL}/servicePrincipals", params={"$top": "999"})
        all_value = result.get("value", [])
        while "@odata.nextLink" in result:
            result = await self._get(result["@odata.nextLink"])
            all_value.extend(result.get("value", []))
        result["value"] = all_value
        return result

    async def get_entra_devices(self) -> Dict[str, Any]:
        """
        Get registered devices.
        Graph API: GET /devices
        """
        result = await self._get(f"{self.GRAPH_URL}/devices", params={"$top": "999"})
        all_value = result.get("value", [])
        while "@odata.nextLink" in result:
            result = await self._get(result["@odata.nextLink"])
            all_value.extend(result.get("value", []))
        result["value"] = all_value
        return result

    async def get_user_mailbox_settings(self, user_id: str) -> Dict[str, Any]:
        """
        Get user mailbox settings.
        Graph API: GET /users/{id}/mailboxSettings
        """
        return await self._get(f"{self.GRAPH_URL}/users/{user_id}/mailboxSettings")

    # ── Entra restore helpers ────────────────────────────────────────────────
    # Each follows PATCH-if-exists, POST-if-missing. Graph mints a new id on
    # POST, so restoring a hard-deleted object produces a new external id; the
    # caller is responsible for keeping resource.external_id in sync if desired.

    _ENTRA_APP_WRITE_FIELDS = (
        "displayName", "description", "signInAudience", "tags", "notes",
        "identifierUris", "api", "web", "spa", "publicClient",
        "requiredResourceAccess", "optionalClaims", "appRoles", "keyCredentials",
        "passwordCredentials",
    )
    _ENTRA_SP_WRITE_FIELDS = (
        "displayName", "description", "accountEnabled", "tags", "notes",
        "servicePrincipalType", "appRoleAssignmentRequired", "loginUrl",
        "logoutUrl", "homepage", "replyUrls", "preferredSingleSignOnMode",
    )
    _ENTRA_DEVICE_WRITE_FIELDS = (
        "displayName", "accountEnabled", "operatingSystem",
        "operatingSystemVersion", "profileType", "isManaged", "isCompliant",
    )
    _ENTRA_CA_WRITE_FIELDS = (
        "displayName", "state", "conditions", "grantControls", "sessionControls",
    )

    @staticmethod
    def _pick_fields(payload: Dict[str, Any], allowed: tuple) -> Dict[str, Any]:
        return {k: v for k, v in (payload or {}).items() if k in allowed and v is not None}

    async def restore_entra_app(self, app_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """PATCH /applications/{id} if it exists, else POST /applications."""
        clean = self._pick_fields(payload, self._ENTRA_APP_WRITE_FIELDS)
        url = f"{self.GRAPH_URL}/applications/{app_id}"
        try:
            await self._get(url)
            return await self._patch(url, clean)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return await self._post(f"{self.GRAPH_URL}/applications", clean)
            raise

    async def restore_service_principal(self, sp_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """PATCH /servicePrincipals/{id} if it exists, else POST /servicePrincipals.

        Creation requires an `appId` — if the SP was hard-deleted but the parent
        application still exists, Graph auto-provisions the SP by appId.
        """
        clean = self._pick_fields(payload, self._ENTRA_SP_WRITE_FIELDS)
        url = f"{self.GRAPH_URL}/servicePrincipals/{sp_id}"
        try:
            await self._get(url)
            return await self._patch(url, clean)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                app_id = (payload or {}).get("appId")
                if not app_id:
                    raise ValueError("Cannot recreate service principal without appId in backup payload") from e
                return await self._post(f"{self.GRAPH_URL}/servicePrincipals", {"appId": app_id, **clean})
            raise

    async def restore_entra_device(self, device_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """PATCH /devices/{id}. Devices can't be created via Graph without MDM
        enrollment — if the device is hard-deleted, we report a skip rather
        than attempting a meaningless POST."""
        clean = self._pick_fields(payload, self._ENTRA_DEVICE_WRITE_FIELDS)
        url = f"{self.GRAPH_URL}/devices/{device_id}"
        try:
            await self._get(url)
            return await self._patch(url, clean)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise ValueError(
                    "Device object no longer exists and cannot be re-created via Graph API "
                    "(device records are provisioned by MDM enrollment, not by tenant admins)."
                ) from e
            raise

    async def restore_conditional_access_policy(self, policy_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """PATCH /identity/conditionalAccess/policies/{id} if it exists, else POST."""
        clean = self._pick_fields(payload, self._ENTRA_CA_WRITE_FIELDS)
        url = f"{self.GRAPH_URL}/identity/conditionalAccess/policies/{policy_id}"
        try:
            await self._get(url)
            return await self._patch(url, clean)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return await self._post(f"{self.GRAPH_URL}/identity/conditionalAccess/policies", clean)
            raise

    async def get_user_contacts(self, user_id: str) -> Dict[str, Any]:
        """
        Get user contacts.
        Graph API: GET /users/{id}/contacts
        """
        return await self._get(f"{self.GRAPH_URL}/users/{user_id}/contacts", params={"$top": "999"})

    async def get_calendar_events_delta(self, user_id: str, delta_token: str = None) -> Dict[str, Any]:
        """Get calendar events using the documented delta API.

        Graph v1.0 documents /users/{id}/calendarView/delta (with startDateTime /
        endDateTime bounds); the previously-used /calendar/events/delta is not in
        the v1.0 reference — it may still respond today but isn't guaranteed.

        Window is 10 years back / 1 year forward by default, which covers almost
        every realistic retention need without paginating the full multi-decade
        history of recurring meetings."""
        if delta_token:
            # delta token contains the full next URL including the preserved window
            url = delta_token
            params = {"$top": "999"}
        else:
            url = f"{self.GRAPH_URL}/users/{user_id}/calendarView/delta"
            now = datetime.utcnow()
            start = (now - timedelta(days=365 * 10)).replace(microsecond=0).isoformat() + "Z"
            end = (now + timedelta(days=365)).replace(microsecond=0).isoformat() + "Z"
            params = {
                "$top": "999",
                "startDateTime": start,
                "endDateTime": end,
            }
        result = await self._get(url, params=params)
        all_value = result.get("value", [])

        # Follow pagination
        while "@odata.nextLink" in result:
            next_url = result["@odata.nextLink"]
            result = await self._get(next_url)
            all_value.extend(result.get("value", []))

        result["value"] = all_value
        return result

    async def get_messages_delta(self, user_id: str, delta_token: str = None) -> Dict[str, Any]:
        """
        Get mailbox messages with full pagination.
        NOTE: Graph API does NOT support delta/change tracking on messages with app-only auth.
        Falls back to regular /messages endpoint with $top pagination.
        Graph API: GET /users/{id}/messages
        """
        url = f"{self.GRAPH_URL}/users/{user_id}/messages"
        params = {"$top": "999"}
        result = await self._get(url, params=params)
        all_value = result.get("value", [])

        # Follow pagination
        while "@odata.nextLink" in result:
            next_url = result["@odata.nextLink"]
            result = await self._get(next_url)
            all_value.extend(result.get("value", []))

        result["value"] = all_value
        return result

    # ── Attachment endpoints ────────────────────────────────────────────────
    # Mailbox messages and calendar events both expose /attachments collections.
    # Three attachment types exist:
    #   #microsoft.graph.fileAttachment       — binary file, content via /$value
    #   #microsoft.graph.itemAttachment       — embedded item (msg/event/contact);
    #                                           expand inline at list time
    #   #microsoft.graph.referenceAttachment  — link only (OneDrive URL etc.) —
    #                                           no content, just metadata
    # afi.ai captures fileAttachments inline as separate blobs; we mirror that.

    async def list_message_attachments(self, user_id: str, message_id: str) -> List[Dict[str, Any]]:
        """List attachments on a single mailbox message. Returns the raw list
        (no $value blobs) — caller fetches binary content separately for
        fileAttachments. Empty list on 404 (message gone) or 403 (no access)."""
        url = f"{self.GRAPH_URL}/users/{user_id}/messages/{message_id}/attachments"
        try:
            result = await self._get(url, params={"$top": "100"})
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (403, 404):
                return []
            raise
        items = result.get("value", []) or []
        # Some tenants paginate even for /attachments — follow nextLink defensively.
        while "@odata.nextLink" in result:
            result = await self._get(result["@odata.nextLink"])
            items.extend(result.get("value", []))
        return items

    async def get_message_attachment_content(
        self, user_id: str, message_id: str, attachment_id: str
    ) -> bytes:
        """Download a fileAttachment's binary content via /$value."""
        url = f"{self.GRAPH_URL}/users/{user_id}/messages/{message_id}/attachments/{attachment_id}/$value"
        token = await self._get_token()
        async with self._http_session() as client:
            resp = await client.get(url, headers={"Authorization": f"Bearer {token}"})
            resp.raise_for_status()
            return resp.content

    async def get_message_mime_source(
        self, user_id: str, message_id: str
    ) -> bytes:
        """Fetch a message as RFC822 MIME source bytes.

        GET /v1.0/users/{user_id}/messages/{message_id}/$value

        Used by the backup-worker's MIME inline-image fallback: for
        emails whose body references inline images via cid: but whose
        /attachments endpoint returns empty (Teams activity
        notifications, OneDrive share emails, some Planner
        notifications), the bytes only live in the RFC822 envelope.
        Python's email.parser walks the multipart tree and recovers
        each inline part keyed by Content-ID.

        Retry contract: 429/503 honor Retry-After (capped at 120s),
        refresh token between attempts, mark the app throttled in
        multi_app_manager so other apps in the rotation absorb load.
        Transient httpx errors (read/connect timeout, RemoteProtocolError)
        also retry with capped exponential backoff. Permanent 4xx
        (400/401/403/404/410/423) raise immediately — they will not
        succeed on retry and the caller needs to record the failure.
        Without this, every 429 silently dropped the inline image for
        that email; at TM scale (heavy mailboxes + tenant-wide throttle)
        that meant most enterprise inline logos/signatures vanished.
        """
        from shared.graph_rate_limiter import graph_rate_limiter
        url = f"{self.GRAPH_URL}/users/{user_id}/messages/{message_id}/$value"
        token = await self._get_token()
        max_attempts = 5
        backoff_s = 2.0
        last_err: Optional[Exception] = None
        for attempt in range(max_attempts):
            try:
                async with self._http_session() as client:
                    await graph_rate_limiter.acquire(reason="graph_mime_single")
                    resp = await client.get(
                        url, headers={"Authorization": f"Bearer {token}"},
                    )
                    if resp.status_code in (429, 503):
                        retry_after = _parse_retry_after(resp, default=30.0, cap=120.0)
                        try:
                            from shared.multi_app_manager import multi_app_manager
                            multi_app_manager.mark_throttled(
                                self.client_id, int(retry_after),
                            )
                        except Exception:
                            pass
                        if attempt < max_attempts - 1:
                            await asyncio.sleep(retry_after)
                            # Refresh token — it may have expired during sleep
                            # and a stale token would 401 the retry.
                            token = await self._get_token()
                            continue
                        resp.raise_for_status()
                    resp.raise_for_status()
                    try:
                        from shared.multi_app_manager import multi_app_manager
                        _lat_ms = (
                            float(resp.elapsed.total_seconds() * 1000)
                            if getattr(resp, "elapsed", None) else 0.0
                        )
                        multi_app_manager.mark_success(self.client_id, _lat_ms)
                    except Exception:
                        pass
                    return resp.content
            except httpx.HTTPStatusError as he:
                # Permanent 4xx — no retry, raise to caller for failure
                # recording. 408 is treated transient (request timeout).
                status = he.response.status_code if he.response is not None else 0
                if status in (400, 401, 403, 404, 410, 423):
                    raise
                last_err = he
                if attempt < max_attempts - 1:
                    await asyncio.sleep(backoff_s)
                    backoff_s = min(30.0, backoff_s * 2)
                    token = await self._get_token()
                    continue
                raise
            except (
                httpx.ReadTimeout,
                httpx.ConnectTimeout,
                httpx.RemoteProtocolError,
                httpx.PoolTimeout,
            ) as e:
                last_err = e
                if attempt < max_attempts - 1:
                    await asyncio.sleep(backoff_s)
                    backoff_s = min(30.0, backoff_s * 2)
                    token = await self._get_token()
                    continue
                raise
        # Should be unreachable — loop always returns or raises.
        if last_err is not None:
            raise last_err
        raise RuntimeError("get_message_mime_source: exhausted retries")

    async def get_messages_mime_concurrent(
        self,
        user_id: str,
        message_ids: List[str],
        *,
        per_app_concurrency: int = 4,
    ) -> Dict[str, Union[bytes, Exception]]:
        """Concurrently fetch RFC822 MIME bodies for many messages of ONE
        mailbox, spreading load across the entire 12-app pool to escape
        the per-mailbox-per-app concurrency ceiling.

        Microsoft Graph enforces a hard limit of **4 concurrent requests
        per mailbox per app** on Outlook endpoints (documented at
        https://learn.microsoft.com/en-us/graph/throttling-limits). A
        single GraphClient hitting `/messages/{id}/$value` for one mailbox
        bottlenecks at ~4 in-flight no matter how much asyncio.gather()
        fan-out the caller adds — every fifth request queues server-side.

        BUT — and this is the unlock — the limit is **per-app**. With 12
        registered apps in `multi_app_manager`, the same mailbox can sustain
        12 × 4 = 48 truly parallel `/$value` GETs at once. That's the only
        legal way to escape the per-mailbox ceiling without paying for
        Microsoft 365 Backup Storage API or hitting tenant-wide aggregate
        throttle.

        Approach:
          1. For each app in the pool, build (or reuse a cached) GraphClient.
          2. Round-robin assign message_ids to apps.
          3. Each app has its own asyncio.Semaphore capped at
             `per_app_concurrency` (default 4 — Microsoft's hard limit).
          4. Each fetch reuses the persistent single-call retry chain in
             `get_message_mime_source` (handles 429 / 503 / transients).
          5. Returns dict[msg_id, bytes | Exception]. Bulk caller decides
             per-id whether to skip or surface the error — we don't raise
             on partial failure, so 1 bad msg doesn't sink the batch.

        Why this beats `$batch` for MIME:
          - `$batch` on Outlook serializes 4 sub-requests-at-a-time PER
            MAILBOX (documented at shared/graph_batch.py:9-12). So 20
            sub-requests for the same mailbox = 5 serial waves of 4.
            Wall-time equals 4-concurrent serial GETs ≈ no win over
            asyncio.gather + 4 in-flight.
          - This method instead exploits the per-APP scope of the
            concurrency limit. With HTTP/2 enabled, each app's persistent
            client multiplexes its 4 streams over ONE TCP connection
            (negligible TLS overhead), and 12 apps = 12 connections =
            48 streams in flight against ONE mailbox.

        Caller guidance:
          - Use ONLY for MIME fetches against a single mailbox per call.
            Mixing mailboxes per-call breaks the per-mailbox isolation
            model and risks burning all apps' quota on one user.
          - For 100s of msg_ids, call in batches of 48-96 to bound
            unwrapped task fan-out.
          - Token refresh per-app happens inside each per-message retry
            (existing get_message_mime_source logic) — no token storm.

        Returns:
          dict mapping each requested message_id to either the bytes of
          its RFC822 MIME source on success, or the Exception that the
          per-message retry chain ultimately raised on failure. Missing
          msg_ids in the result indicate caller bug (we always populate
          every requested id).
        """
        if not message_ids:
            return {}
        # Lazy import to avoid circular-import at module load.
        from shared.multi_app_manager import multi_app_manager

        apps = multi_app_manager.apps
        # Single-app deployment (or test mocks) — fall back to the
        # serial path through this same GraphClient. No semaphore games;
        # at most 4 in-flight per Outlook's own rule.
        if not apps or len(apps) <= 1:
            return await self._fetch_mime_serial(
                user_id, message_ids,
                concurrency=per_app_concurrency,
            )

        # Per-app semaphores cap concurrency at Microsoft's documented
        # ceiling. Going above 4 just queues server-side and burns
        # cycles in retry loops — it does NOT increase throughput.
        sem_per_app = max(1, min(per_app_concurrency, 4))
        app_sems: Dict[str, asyncio.Semaphore] = {
            app.client_id: asyncio.Semaphore(sem_per_app) for app in apps
        }
        # Cache per-app GraphClient for this tenant so we don't reopen
        # token exchanges + http sessions per batch. Lifetime = caller
        # scope; weak global cache below absorbs reuse across calls.
        clients_by_app: Dict[str, GraphClient] = {}
        for app in apps:
            cached = _MULTI_APP_CLIENT_CACHE.get((app.client_id, self.tenant_id))
            if cached is None:
                cached = GraphClient(
                    client_id=app.client_id,
                    client_secret=app.client_secret,
                    tenant_id=self.tenant_id,
                )
                _MULTI_APP_CLIENT_CACHE[(app.client_id, self.tenant_id)] = cached
            clients_by_app[app.client_id] = cached

        results: Dict[str, Union[bytes, Exception]] = {}

        async def _one(msg_id: str, app_client_id: str) -> None:
            client = clients_by_app[app_client_id]
            sem = app_sems[app_client_id]
            async with sem:
                try:
                    data = await client.get_message_mime_source(
                        user_id, msg_id,
                    )
                    results[msg_id] = data
                except Exception as e:
                    # Don't raise — bulk semantics. Caller inspects per-id.
                    results[msg_id] = e

        # Round-robin distribution across apps. If 7 messages and 12 apps,
        # apps 0-6 each handle one; apps 7-11 idle. With more messages,
        # apps cycle: msg[12] goes back to app[0]'s semaphore, queueing
        # behind whatever app[0] is doing.
        tasks = []
        for i, msg_id in enumerate(message_ids):
            chosen_app = apps[i % len(apps)]
            tasks.append(_one(msg_id, chosen_app.client_id))
        # gather with return_exceptions=False — our _one() catches all
        # exceptions and stuffs them into results, so gather always
        # completes cleanly. (return_exceptions=True would swallow our
        # already-caught errors twice and complicate the result map.)
        await asyncio.gather(*tasks)
        return results

    async def _fetch_mime_serial(
        self,
        user_id: str,
        message_ids: List[str],
        *,
        concurrency: int = 4,
    ) -> Dict[str, Union[bytes, Exception]]:
        """Single-app fallback for get_messages_mime_concurrent when only
        one Graph app is configured. Caps in-flight at `concurrency`
        (default 4 — Microsoft's per-mailbox-per-app limit). Same bulk
        semantics: each msg_id maps to bytes or Exception, never raises."""
        if not message_ids:
            return {}
        sem = asyncio.Semaphore(max(1, min(concurrency, 4)))
        out: Dict[str, Union[bytes, Exception]] = {}

        async def _one(msg_id: str) -> None:
            async with sem:
                try:
                    out[msg_id] = await self.get_message_mime_source(
                        user_id, msg_id,
                    )
                except Exception as e:
                    out[msg_id] = e

        await asyncio.gather(*(_one(mid) for mid in message_ids))
        return out

    async def get_hosted_content(
        self, chat_id: str, message_id: str, hc_id: str, chunk_size: int = 1024 * 1024
    ) -> Tuple[AsyncGenerator[bytes, None], str, int]:
        """Stream the raw bytes of a chat message's hostedContent.

        Graph API: GET /chats/{cid}/messages/{mid}/hostedContents/{hid}/$value

        Opens an httpx streaming response, captures Content-Type and
        Content-Length headers, then returns an async generator that yields
        chunks of ``chunk_size`` bytes. The underlying httpx client + stream
        contexts stay open until the returned generator is exhausted or closed
        — callers MUST fully consume (or ``aclose()``) the stream to release
        the connection. Used by the backup-worker hostedContents capture and
        by the Teams-chat backfill script.

        Returns: (async_iter_bytes, content_type, content_length).

        Retry contract: 429/503 honor Retry-After (capped at 120s),
        refresh token between attempts, mark the app throttled. Stream
        + client contexts are torn down between attempts and rebuilt
        fresh on retry (httpx stream cannot be replayed). Permanent
        4xx (400/401/403/404/410/423) raise immediately. Without this
        retry, every 429 during a hostedContents drain silently dropped
        the inline image (sticker / emoji / pasted screenshot) from
        the chat — same durability bug as get_message_mime_source.
        """
        url = f"{self.GRAPH_URL}/chats/{chat_id}/messages/{message_id}/hostedContents/{hc_id}/$value"
        token = await self._get_token()
        max_attempts = 5
        backoff_s = 2.0
        last_err: Optional[Exception] = None
        # PERF (Item A): route streaming HC fetch through the SHARED httpx
        # client (HTTP/2 + persistent pool). Previously each HC fetch built
        # a per-request AsyncClient → fresh TCP + TLS handshake on every
        # inline image. With HTTP/2 on (GRAPHCLIENT_HTTP2=true) multiple
        # HC streams now multiplex on the same connection.
        #
        # Item A-followup (2026-05-17): on 429/503, try to migrate to a
        # healthy app's token BEFORE sleeping Retry-After. Without this,
        # all 5 retries hit the same throttled app and one transient
        # 429-storm on app X drops one inline image. With 20 apps and
        # the BatchClient pattern (see _send_chunk_with_retry), one
        # app's 429 should swap to another app's bucket immediately.
        # Caught in prod: "hc interleave failed msg=17751087 chat=19:6d29d:
        # HTTPStatusError: throttled after 5 attempts".
        current_app_id: str = self.client_id
        for attempt in range(max_attempts):
            client = await self._get_shared_http()
            client_cm = None  # shared client must NOT be closed per-request
            stream_cm = None
            resp = None
            try:
                stream_cm = client.stream(
                    "GET", url, headers={"Authorization": f"Bearer {token}"},
                )
                resp = await stream_cm.__aenter__()
                # 429/503: tear down stream + client, honor Retry-After, retry.
                if resp.status_code in (429, 503):
                    retry_after = _parse_retry_after(resp, default=30.0, cap=120.0)
                    try:
                        from shared.multi_app_manager import multi_app_manager
                        multi_app_manager.mark_throttled(
                            current_app_id, int(retry_after),
                        )
                    except Exception:
                        pass
                    await stream_cm.__aexit__(None, None, None)
                    if client_cm is not None:
                        await client_cm.__aexit__(None, None, None)
                    if attempt < max_attempts - 1:
                        # Try app migration FIRST — no sleep needed if we
                        # find a healthy alt. Matches BatchClient retry.
                        new_token, new_app = await self._try_migrate_app(current_app_id)
                        if new_token and new_app:
                            token = new_token
                            current_app_id = new_app
                            continue
                        # No healthy app available → fall back to sleep.
                        await asyncio.sleep(retry_after)
                        token = await self._get_token()
                        continue
                    # Surface the throttle to the caller after exhausting.
                    raise httpx.HTTPStatusError(
                        f"throttled after {max_attempts} attempts",
                        request=resp.request, response=resp,
                    )
                resp.raise_for_status()
                ctype = resp.headers.get("Content-Type", "application/octet-stream")
                size = int(resp.headers.get("Content-Length", "0") or 0)
            except httpx.HTTPStatusError as he:
                # Permanent 4xx — no retry.
                if stream_cm is not None:
                    await stream_cm.__aexit__(None, None, None)
                if client_cm is not None:
                    await client_cm.__aexit__(None, None, None)
                status = he.response.status_code if he.response is not None else 0
                if status in (400, 401, 403, 404, 410, 423):
                    raise
                last_err = he
                if attempt < max_attempts - 1:
                    await asyncio.sleep(backoff_s)
                    backoff_s = min(30.0, backoff_s * 2)
                    token = await self._get_token()
                    continue
                raise
            except (
                httpx.ReadTimeout,
                httpx.ConnectTimeout,
                httpx.RemoteProtocolError,
                httpx.PoolTimeout,
            ) as e:
                if stream_cm is not None:
                    await stream_cm.__aexit__(None, None, None)
                if client_cm is not None:
                    await client_cm.__aexit__(None, None, None)
                last_err = e
                if attempt < max_attempts - 1:
                    await asyncio.sleep(backoff_s)
                    backoff_s = min(30.0, backoff_s * 2)
                    token = await self._get_token()
                    continue
                raise
            except BaseException:
                if stream_cm is not None:
                    await stream_cm.__aexit__(None, None, None)
                if client_cm is not None:
                    await client_cm.__aexit__(None, None, None)
                raise

            try:
                from shared.multi_app_manager import multi_app_manager
                _lat_ms = (
                    float(resp.elapsed.total_seconds() * 1000)
                    if getattr(resp, "elapsed", None) else 0.0
                )
                multi_app_manager.mark_success(self.client_id, _lat_ms)
            except Exception:
                pass

            # Capture context managers in the closure so they outlive this
            # function. The caller MUST fully consume / aclose() the
            # generator to release them. With the shared client we no
            # longer own client_cm — only the stream needs tearing down.
            _stream_cm = stream_cm
            _client_cm = client_cm
            _resp = resp

            async def _iter() -> AsyncGenerator[bytes, None]:
                try:
                    async for chunk in _resp.aiter_bytes(chunk_size):
                        yield chunk
                finally:
                    await _stream_cm.__aexit__(None, None, None)
                    if _client_cm is not None:
                        await _client_cm.__aexit__(None, None, None)

            return _iter(), ctype, size

        # Should be unreachable — loop always returns or raises.
        if last_err is not None:
            raise last_err
        raise RuntimeError("get_hosted_content: exhausted retries")

    # Worker-lifetime cache of source URLs that resolved to a permanent
    # 4xx (400/401/403/404/410/423). Backed by a plain dict so subsequent
    # references to the same URL — same chat, different chat, different
    # user, different resource type — short-circuit at O(1) instead of
    # burning a Graph round-trip + a Retry-After to "discover" it's
    # still unreachable. Bounded so the dict can't grow unbounded on a
    # tenant with thousands of broken references; evicts oldest on
    # insert past _UNREACHABLE_URL_CACHE_MAX. Set at the CLASS level
    # rather than self.* so it spans every GraphClient instance the
    # worker creates (one per tenant).
    _unreachable_urls: Dict[str, float] = {}
    _UNREACHABLE_URL_CACHE_MAX = 50_000

    @classmethod
    def _mark_url_unreachable(cls, url: str) -> None:
        if not url:
            return
        # Eviction: when cache is full, drop the oldest 5% in one pass.
        if len(cls._unreachable_urls) >= cls._UNREACHABLE_URL_CACHE_MAX:
            cutoff = sorted(cls._unreachable_urls.values())[
                len(cls._unreachable_urls) // 20
            ]
            cls._unreachable_urls = {
                u: t for u, t in cls._unreachable_urls.items() if t > cutoff
            }
        cls._unreachable_urls[url] = time.time()

    @classmethod
    def _is_url_unreachable(cls, url: str) -> bool:
        return bool(url) and url in cls._unreachable_urls

    async def resolve_share_to_drive_item(self, source_url: str) -> Optional[Dict[str, Any]]:
        """Convert a OneDrive / SharePoint share URL into a driveItem dict.

        Split out from fetch_shared_url_content so callers can put the
        share-resolve (Graph-rate-limited) and the byte-download
        (SharePoint-CDN, different rate limits) under SEPARATE
        semaphores — letting the two phases overlap instead of
        sharing one bottleneck.

        Returns the driveItem dict on success, None on permanent 4xx.
        Marks the URL as unreachable on permanent 4xx so subsequent
        references short-circuit without burning a Graph round-trip."""
        if not source_url:
            return None
        if self._is_url_unreachable(source_url):
            return None
        import base64 as _b64
        try:
            share_id = "u!" + _b64.urlsafe_b64encode(source_url.encode("utf-8")).decode("ascii").rstrip("=")
        except Exception:
            return None
        try:
            return await self._get(
                f"{self.GRAPH_URL}/shares/{share_id}/driveItem",
            )
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (400, 401, 403, 404, 410, 423):
                self._mark_url_unreachable(source_url)
                return None
            raise
        except Exception:
            return None

    async def resolve_shares_batch(
        self, source_urls: List[str],
    ) -> Dict[str, Optional[Dict[str, Any]]]:
        """Bulk variant of resolve_share_to_drive_item via /v1.0/$batch.

        Mirrors the OneDrive ``get_download_urls_batch`` pattern: bundle
        up to 20 GET /shares/{u!base64}/driveItem requests per HTTP call
        instead of issuing N serial round-trips against the per-app
        rate-limit budget. On the chat-attachment path, a single user
        with 37 unique attachment URLs was observed taking 35s of pure
        resolve wall-time; bulked, that becomes ~3-5s (one batch wave
        of two chunks).

        Returns a map ``{source_url: driveItem|None}``.
          * ``dict``  — driveItem resolved (caller should download bytes)
          * ``None``  — permanent 4xx (400/401/403/404/410/423) OR the
                       URL was already in the unreachable cache; caller
                       should treat as a known-broken reference.

        URLs absent from the returned map indicate a transient batch
        failure (network, 5xx after retries). Callers should fall back
        to the single-URL ``resolve_share_to_drive_item`` path for
        anything missing — that path's own retry budget will absorb
        the transient, and per-URL failures don't leak into the bulk
        unreachable cache (only true permanent 4xx do).
        """
        from shared.graph_batch import BatchRequest
        import base64 as _b64
        out: Dict[str, Optional[Dict[str, Any]]] = {}
        if not source_urls:
            return out

        # Pre-filter URLs already known-broken — those are deterministic
        # Nones and don't deserve a batch slot.
        url_by_id: Dict[str, str] = {}
        reqs: List[BatchRequest] = []
        for u in source_urls:
            if not u:
                continue
            if self._is_url_unreachable(u):
                out[u] = None
                continue
            try:
                share_id = (
                    "u!"
                    + _b64.urlsafe_b64encode(u.encode("utf-8"))
                    .decode("ascii")
                    .rstrip("=")
                )
            except Exception:
                out[u] = None
                continue
            # Batch sub-request id must be unique within the batch and
            # short. Hash the URL to keep it bounded — collision chance
            # at 20-per-batch with sha256[:16] is astronomical.
            req_id = hashlib.sha256(u.encode("utf-8")).hexdigest()[:16]
            url_by_id[req_id] = u
            reqs.append(
                BatchRequest(
                    id=req_id,
                    method="GET",
                    # $batch sub-request URLs are root-relative (no host).
                    url=f"/shares/{share_id}/driveItem",
                )
            )

        if not reqs:
            return out

        try:
            responses = await self.batch(reqs)
        except Exception as exc:
            # Whole-batch failure is rare — graph_batch already retries
            # 429/503 internally with the Retry-After header honored.
            # If we still bubble out here, surface nothing so callers
            # fall back to the single-URL path rather than incorrectly
            # caching every URL as broken.
            print(
                f"[GraphClient] resolve_shares_batch failed for "
                f"{len(reqs)} URLs: {type(exc).__name__}: {exc}"
            )
            return out

        for req_id, resp in responses.items():
            u = url_by_id.get(req_id)
            if not u:
                continue
            status = getattr(resp, "status", 0)
            body = getattr(resp, "body", None) or {}
            if status == 200 and isinstance(body, dict) and body.get("id"):
                out[u] = body
            elif status in (400, 401, 403, 404, 410, 423):
                # Same permanent-4xx contract as the single-URL path —
                # cache the failure so future runs short-circuit.
                self._mark_url_unreachable(u)
                out[u] = None
            else:
                # 429/503 after graph_batch's own retry budget, network
                # errors, or unexpected 5xx — leave URL absent so the
                # caller falls back to the single-URL path with its own
                # retry policy.
                continue

        return out

    async def download_drive_item_bytes(
        self,
        drive_item: Dict[str, Any],
        source_url_for_cache: Optional[str] = None,
    ) -> Optional[bytes]:
        """Stream the bytes of a resolved driveItem via its
        @microsoft.graph.downloadUrl. Range-resumes on transport drops.

        Pass `source_url_for_cache` if you'd like a permanent-4xx
        result to be added to the worker-lifetime unreachable cache
        (so a subsequent reference to the same source URL fails fast).
        """
        download_url = drive_item.get("@microsoft.graph.downloadUrl")
        if not download_url:
            return None
        expected_size = int(drive_item.get("size") or 0)
        return await self._stream_download_url(
            download_url, expected_size,
            source_url_for_cache=source_url_for_cache,
        )

    async def fetch_shared_url_content(self, source_url: str) -> Optional[bytes]:
        """Resolve a OneDrive / SharePoint share URL to its actual file bytes.

        Back-compat wrapper around resolve_share_to_drive_item +
        download_drive_item_bytes. New callers should prefer the two
        split methods so they can isolate Graph-resolve concurrency
        from SharePoint-CDN download concurrency.

        Used for referenceAttachments on mail (and later chat) messages — the
        attachment payload only has a `sourceUrl`, not content. Graph's
        `/shares/{id}/driveItem` endpoint converts a sharing URL into a
        driveItem we can then download via `@microsoft.graph.downloadUrl`.

        Encoding: the share id is `u!` + URL-safe base64 of the URL, with
        trailing `=` stripped — per Microsoft's docs:
        https://learn.microsoft.com/graph/api/shares-get

        Returns None when the URL isn't resolvable (external link, missing
        grant, deleted item, 403/404 on shares endpoint). Callers should
        degrade to metadata-only storage in that case."""
        if not source_url:
            return None
        drive_item = await self.resolve_share_to_drive_item(source_url)
        if drive_item is None:
            return None
        return await self.download_drive_item_bytes(
            drive_item, source_url_for_cache=source_url,
        )

    async def _stream_download_url(
        self,
        download_url: str,
        expected_size: int = 0,
        source_url_for_cache: Optional[str] = None,
    ) -> Optional[bytes]:
        """Internal: stream a (resolved) download URL with Range-resume.
        Caches permanent 4xx into the unreachable-URL cache when
        source_url_for_cache is provided."""
        max_resumes = int(os.getenv("SHARED_URL_STREAM_MAX_RESUMES", "6"))
        label = (source_url_for_cache or download_url)[:60]
        buf = bytearray()
        resume_attempt = 0
        try:
            async with self._http_session() as client:
                while True:
                    headers: Dict[str, str] = {}
                    if buf:
                        headers["Range"] = f"bytes={len(buf)}-"
                    try:
                        async with client.stream("GET", download_url, headers=headers) as resp:
                            # Bug #172 fix: SharePoint CDN can 429/503
                            # under partition-shard concurrency. Honor
                            # `Retry-After` header AND the JSON body's
                            # `retryAfterSeconds` (Graph quirk on shared-
                            # link path), sleep, then resume — within the
                            # existing max_resumes budget so a hostile
                            # remote can't stall the worker forever.
                            if resp.status_code in (429, 503):
                                body = await resp.aread()
                                retry_secs = 10.0
                                ra = resp.headers.get("Retry-After") or resp.headers.get("retry-after")
                                if ra:
                                    try:
                                        retry_secs = float(int(ra.strip()))
                                    except (ValueError, AttributeError):
                                        pass
                                else:
                                    try:
                                        import json as _json
                                        parsed_body = _json.loads(body)
                                        if isinstance(parsed_body, dict):
                                            err = parsed_body.get("error") or {}
                                            cand = (
                                                err.get("retryAfterSeconds")
                                                or parsed_body.get("retryAfterSeconds")
                                            )
                                            if cand is not None:
                                                retry_secs = float(cand)
                                    except Exception:
                                        pass
                                retry_secs = min(max(retry_secs, 1.0), 120.0)
                                if resume_attempt >= max_resumes:
                                    print(
                                        f"[GraphClient] shared URL download "
                                        f"HTTP {resp.status_code} ({label}…) "
                                        f"— exhausted {max_resumes} throttle "
                                        f"retries; giving up"
                                    )
                                    return None
                                resume_attempt += 1
                                print(
                                    f"[GraphClient] shared URL throttled "
                                    f"HTTP {resp.status_code} ({label}…), "
                                    f"sleeping {retry_secs:.0f}s "
                                    f"(retry {resume_attempt}/{max_resumes})"
                                )
                                await asyncio.sleep(retry_secs)
                                continue
                            if not buf:
                                if resp.status_code not in (200, 206):
                                    body = await resp.aread()
                                    print(
                                        f"[GraphClient] shared URL download HTTP "
                                        f"{resp.status_code} ({label}…): "
                                        f"{body[:160]!r}"
                                    )
                                    # Permanent 4xx — record in worker-
                                    # lifetime unreachable cache so the
                                    # next reference to this URL fails
                                    # fast without burning Graph budget.
                                    if (
                                        source_url_for_cache
                                        and resp.status_code in (400, 401, 403, 404, 410, 423)
                                    ):
                                        self._mark_url_unreachable(source_url_for_cache)
                                    return None
                            else:
                                if resp.status_code != 206:
                                    # CDN ignored Range — restart from zero to
                                    # avoid concatenating duplicate bytes.
                                    print(
                                        f"[GraphClient] shared URL range "
                                        f"rejected (HTTP {resp.status_code}); "
                                        f"restarting from offset 0 "
                                        f"({label}…)"
                                    )
                                    buf = bytearray()
                                    continue
                            async for chunk in resp.aiter_bytes(1 << 20):
                                if chunk:
                                    buf.extend(chunk)
                        return bytes(buf)
                    except (httpx.RemoteProtocolError, httpx.ReadError, httpx.ReadTimeout) as exc:
                        # Connection dropped mid-stream. If size is known and
                        # we already received everything, accept what we have
                        # (some CDNs close without proper framing on the last
                        # byte). Otherwise resume from current offset.
                        if expected_size > 0 and len(buf) >= expected_size:
                            return bytes(buf)
                        if resume_attempt >= max_resumes:
                            print(
                                f"[GraphClient] shared URL download exhausted "
                                f"{max_resumes} resumes at "
                                f"{len(buf)}/{expected_size or '?'} bytes "
                                f"({label}…): {type(exc).__name__}"
                            )
                            return None
                        resume_attempt += 1
                        backoff = min(8.0, 1.0 * (2 ** (resume_attempt - 1)))
                        print(
                            f"[GraphClient] shared URL stream dropped at "
                            f"{len(buf)}/{expected_size or '?'} bytes "
                            f"({type(exc).__name__}); resume "
                            f"{resume_attempt}/{max_resumes} after "
                            f"{backoff:.1f}s ({label}…)"
                        )
                        await asyncio.sleep(backoff)
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (400, 401, 403, 404, 410, 423):
                if source_url_for_cache:
                    self._mark_url_unreachable(source_url_for_cache)
                return None
            print(f"[GraphClient] shared URL download failed ({label}…): HTTP {e.response.status_code}")
            return None
        except Exception as e:
            print(f"[GraphClient] shared URL download failed ({label}…): {type(e).__name__}: {e}")
            return None

    async def list_event_attachments(self, user_id: str, event_id: str) -> List[Dict[str, Any]]:
        """List attachments on a calendar event."""
        url = f"{self.GRAPH_URL}/users/{user_id}/events/{event_id}/attachments"
        try:
            result = await self._get(url, params={"$top": "100"})
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (403, 404):
                return []
            raise
        items = result.get("value", []) or []
        while "@odata.nextLink" in result:
            result = await self._get(result["@odata.nextLink"])
            items.extend(result.get("value", []))
        return items

    async def get_event_attachment_content(
        self, user_id: str, event_id: str, attachment_id: str
    ) -> bytes:
        """Download a calendar event fileAttachment's binary content via /$value."""
        url = f"{self.GRAPH_URL}/users/{user_id}/events/{event_id}/attachments/{attachment_id}/$value"
        token = await self._get_token()
        async with self._http_session() as client:
            resp = await client.get(url, headers={"Authorization": f"Bearer {token}"})
            resp.raise_for_status()
            return resp.content

    # ── File version endpoints ──────────────────────────────────────────────
    # OneDrive/SharePoint files retain a version history when versioning is
    # enabled (default for SP, opt-in for OD personal). Graph exposes:
    #   GET /drives/{did}/items/{iid}/versions          — list metadata
    #   GET /drives/{did}/items/{iid}/versions/{vid}/content  — binary

    async def list_file_versions(self, drive_id: str, item_id: str) -> List[Dict[str, Any]]:
        """List historical versions of a drive item. Returns newest-first.
        The first entry is the current version (same content as the live file)."""
        url = f"{self.GRAPH_URL}/drives/{drive_id}/items/{item_id}/versions"
        try:
            result = await self._get(url, params={"$top": "200"})
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (403, 404):
                return []
            raise
        items = result.get("value", []) or []
        while "@odata.nextLink" in result:
            result = await self._get(result["@odata.nextLink"])
            items.extend(result.get("value", []))
        return items

    async def list_file_versions_batch(
        self,
        drive_id: str,
        item_ids: List[str],
        chunk_size: int = 20,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Bulk-list /versions for many items via Graph /$batch.

        Returns {item_id → versions_list}, newest-first per item.
        Missing items (403/404) map to []. Items that exhaust the batch
        retry budget map to None so the caller can fall back to a
        per-item call.

        Why this exists: each item needs its own GET /versions which costs
        a TCP/TLS/auth round-trip; on a 5k-user, millions-of-files corpus
        that overhead dominates list-phase wall time. $batch bundles up to
        20 sub-requests behind one auth handshake.

        Note: $batch does not follow @odata.nextLink. The /versions
        endpoint defaults to ~50 results; we cap callers at MAX_FILE_VERSIONS
        which is well under that, so single-page is sufficient.
        """
        if not item_ids:
            return {}

        from shared.graph_batch import BatchRequest

        # Build one BatchRequest per item. Sub-request URLs use the relative
        # path Microsoft Graph expects inside $batch (no host prefix). We
        # avoid $top= because graph_batch.validate_requests rejects it.
        requests: List[BatchRequest] = [
            BatchRequest(
                id=str(i),
                method="GET",
                url=f"/drives/{drive_id}/items/{item_id}/versions",
            )
            for i, item_id in enumerate(item_ids)
        ]
        # Honor caller-provided chunk_size if smaller than the Graph hard
        # cap of 20. BatchClient handles its own chunking via the global
        # GRAPH_BATCH_MAX_SIZE setting.
        responses = await self.batch(requests)

        out: Dict[str, List[Dict[str, Any]]] = {}
        for i, item_id in enumerate(item_ids):
            resp = responses.get(str(i))
            if resp is None:
                out[item_id] = []
                continue
            if resp.status in (403, 404):
                out[item_id] = []
                continue
            if 200 <= resp.status < 300:
                out[item_id] = (resp.body or {}).get("value", []) or []
                continue
            # Unrecoverable (429 after retries, 5xx). Signal None so the
            # caller can fall back to a single-shot list_file_versions().
            out[item_id] = None  # type: ignore
        return out

    async def stream_file_version_3tier(
        self,
        drive_id: str,
        item_id: str,
        version_id: str,
        size_hint: int,
        inline_buffer_mb: int = 32,
        disk_threshold_mb: int = 256,
        chunk_mb: int = 16,
    ) -> Tuple[Optional[bytes], Optional[str], int]:
        """Three-tier version content fetch with policy-driven 429/503 retry.

        - size ≤ inline_buffer_mb  → single GET into memory, return (bytes, None, size)
        - size ≤ disk_threshold_mb → chunked streaming into memory, return (bytes, None, size)
        - else                     → streaming download to disk-temp, return (None, path, size)

        Caller is responsible for cleaning up the disk-temp path if returned.

        Throttle handling: routes through the same per-app pacing bucket +
        multi-app throttle marker as the rest of GraphClient. On 429/503
        the worker sleeps for the policy-decided Retry-After and retries
        from the start of whichever tier was active — re-issuing a single
        idempotent GET, not duplicating bytes. On cumulative-cap exhaustion
        raises GraphRetryExhaustedError so the caller's best-effort
        try/except can log and move to the next version.
        """
        from shared.config import settings as s
        from shared.multi_app_manager import multi_app_manager

        url = f"{self.GRAPH_URL}/drives/{drive_id}/items/{item_id}/versions/{version_id}/content"
        inline_cap = inline_buffer_mb * 1024 * 1024
        disk_cap = disk_threshold_mb * 1024 * 1024
        chunk_bytes = max(chunk_mb * 1024 * 1024, 1024 * 1024)
        policy = self._policy

        async with self._http_session() as client:
            while True:
                prio = self._effective_priority()
                await policy.stream_bucket.acquire(priority=prio)
                await multi_app_manager.acquire_app_token(self.client_id, priority=prio)
                token = await self._get_token()
                headers = {"Authorization": f"Bearer {token}"}

                try:
                    # Tier 1 — small files: single non-streaming GET, simplest path.
                    if size_hint and size_hint <= inline_cap:
                        resp = await client.get(url, headers=headers)
                        if resp.status_code in (429, 503):
                            action = policy.decide(
                                status_code=resp.status_code,
                                retry_after=resp.headers.get("Retry-After"),
                            )
                            if action.exhausted:
                                raise GraphRetryExhaustedError(
                                    f"version content cap hit on {url[:80]}: {action.reason}"
                                )
                            multi_app_manager.mark_throttled(
                                self.client_id, int(action.sleep_seconds),
                            )
                            print(f"[GraphClient/version] {resp.status_code} on "
                                  f"{url[:80]} — {action.reason}")
                            await asyncio.sleep(action.sleep_seconds)
                            if s.GRAPH_POST_THROTTLE_BRAKE_MS > 0:
                                await asyncio.sleep(s.GRAPH_POST_THROTTLE_BRAKE_MS / 1000.0)
                            continue
                        resp.raise_for_status()
                        policy.reset_on_success()
                        try:
                            _lat_ms = float(resp.elapsed.total_seconds() * 1000) if getattr(resp, "elapsed", None) else 0.0
                            multi_app_manager.mark_success(self.client_id, _lat_ms)
                        except Exception:
                            pass
                        return resp.content, None, len(resp.content)

                    # Tier 2/3 — stream the response. Decide memory-vs-disk based
                    # on actual content-length once response headers arrive
                    # (size_hint may be stale or 0 for unknown sizes).
                    async with client.stream("GET", url, headers=headers) as resp:
                        if resp.status_code in (429, 503):
                            action = policy.decide(
                                status_code=resp.status_code,
                                retry_after=resp.headers.get("Retry-After"),
                            )
                            if action.exhausted:
                                raise GraphRetryExhaustedError(
                                    f"version content cap hit on {url[:80]}: {action.reason}"
                                )
                            multi_app_manager.mark_throttled(
                                self.client_id, int(action.sleep_seconds),
                            )
                            print(f"[GraphClient/version] {resp.status_code} on "
                                  f"{url[:80]} — {action.reason}")
                            # Drain so the connection can be reused.
                            try:
                                await resp.aread()
                            except Exception:
                                pass
                            await asyncio.sleep(action.sleep_seconds)
                            if s.GRAPH_POST_THROTTLE_BRAKE_MS > 0:
                                await asyncio.sleep(s.GRAPH_POST_THROTTLE_BRAKE_MS / 1000.0)
                            continue
                        resp.raise_for_status()
                        policy.reset_on_success()
                        try:
                            _lat_ms = float(resp.elapsed.total_seconds() * 1000) if getattr(resp, "elapsed", None) else 0.0
                            multi_app_manager.mark_success(self.client_id, _lat_ms)
                        except Exception:
                            pass
                        cl_header = resp.headers.get("Content-Length")
                        actual_size = int(cl_header) if cl_header and cl_header.isdigit() else (size_hint or 0)
                        if actual_size and actual_size <= disk_cap:
                            # Tier 2 — buffer in memory, chunked.
                            buf = bytearray()
                            async for chunk in resp.aiter_bytes(chunk_size=chunk_bytes):
                                buf.extend(chunk)
                            return bytes(buf), None, len(buf)
                        # Tier 3 — spill to disk, bounded RAM.
                        import tempfile
                        fd, tmp_path = tempfile.mkstemp(prefix="ver_", suffix=".bin")
                        total = 0
                        try:
                            with os.fdopen(fd, "wb") as fh:
                                async for chunk in resp.aiter_bytes(chunk_size=chunk_bytes):
                                    fh.write(chunk)
                                    total += len(chunk)
                            return None, tmp_path, total
                        except BaseException:
                            try:
                                os.unlink(tmp_path)
                            except OSError:
                                pass
                            raise
                except (httpx.ReadTimeout, httpx.ConnectTimeout,
                        httpx.RemoteProtocolError) as exc:
                    action = policy.decide_transient_error()
                    if action.exhausted:
                        raise GraphRetryExhaustedError(
                            f"version content transient cap hit on {url[:80]}: "
                            f"{type(exc).__name__}"
                        )
                    print(f"[GraphClient/version] transient {type(exc).__name__} "
                          f"on {url[:80]}; sleep {action.sleep_seconds:.1f}s")
                    await asyncio.sleep(action.sleep_seconds)
                    continue

    async def get_file_version_content(
        self, drive_id: str, item_id: str, version_id: str
    ) -> bytes:
        """Download the binary content of a specific historical version.

        Same 429/503 Retry-After handling as stream_file_version_3tier, so a
        throttled tenant won't burn this call on the first attempt.
        """
        from shared.config import settings as s
        from shared.multi_app_manager import multi_app_manager

        url = f"{self.GRAPH_URL}/drives/{drive_id}/items/{item_id}/versions/{version_id}/content"
        policy = self._policy

        async with self._http_session() as client:
            while True:
                prio = self._effective_priority()
                await policy.stream_bucket.acquire(priority=prio)
                await multi_app_manager.acquire_app_token(self.client_id, priority=prio)
                token = await self._get_token()
                try:
                    resp = await client.get(
                        url, headers={"Authorization": f"Bearer {token}"},
                    )
                except (httpx.ReadTimeout, httpx.ConnectTimeout,
                        httpx.RemoteProtocolError) as exc:
                    action = policy.decide_transient_error()
                    if action.exhausted:
                        raise GraphRetryExhaustedError(
                            f"version content transient cap hit on {url[:80]}: "
                            f"{type(exc).__name__}"
                        )
                    print(f"[GraphClient/version] transient {type(exc).__name__} "
                          f"on {url[:80]}; sleep {action.sleep_seconds:.1f}s")
                    await asyncio.sleep(action.sleep_seconds)
                    continue
                if resp.status_code in (429, 503):
                    action = policy.decide(
                        status_code=resp.status_code,
                        retry_after=resp.headers.get("Retry-After"),
                    )
                    if action.exhausted:
                        raise GraphRetryExhaustedError(
                            f"version content cap hit on {url[:80]}: {action.reason}"
                        )
                    multi_app_manager.mark_throttled(
                        self.client_id, int(action.sleep_seconds),
                    )
                    print(f"[GraphClient/version] {resp.status_code} on "
                          f"{url[:80]} — {action.reason}")
                    await asyncio.sleep(action.sleep_seconds)
                    if s.GRAPH_POST_THROTTLE_BRAKE_MS > 0:
                        await asyncio.sleep(s.GRAPH_POST_THROTTLE_BRAKE_MS / 1000.0)
                    continue
                resp.raise_for_status()
                policy.reset_on_success()
                try:
                    _lat_ms = float(resp.elapsed.total_seconds() * 1000) if getattr(resp, "elapsed", None) else 0.0
                    multi_app_manager.mark_success(self.client_id, _lat_ms)
                except Exception:
                    pass
                return resp.content

    # ── File / item permissions ─────────────────────────────────────────────
    # Graph's `permissions` collection on a drive item lists every grant —
    # direct sharing, SP groups, link-based access, inheritance markers. afi
    # captures these so restored files re-establish the exact same ACL set.

    async def list_file_permissions(self, drive_id: str, item_id: str) -> List[Dict[str, Any]]:
        """List ACL grants on a OneDrive/SharePoint item. Empty list on 404
        (item gone) or 403 (no permissions to read permissions — uncommon)."""
        url = f"{self.GRAPH_URL}/drives/{drive_id}/items/{item_id}/permissions"
        try:
            result = await self._get(url, params={"$top": "200"})
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (403, 404):
                return []
            raise
        items = result.get("value", []) or []
        while "@odata.nextLink" in result:
            result = await self._get(result["@odata.nextLink"])
            items.extend(result.get("value", []))
        return items

    # ── Event creation + attachment (re)attach ──────────────────────────────
    # Used by restore-worker. Event creation returns the new event_id; the
    # attachment endpoint accepts inline base64 (no /$value upload step needed
    # for restore — the event is freshly created so size limits aren't an issue
    # in practice for typical attachments under 3MB).

    async def create_calendar_event(self, user_id: str, event_payload: Dict[str, Any]) -> Dict[str, Any]:
        """POST /users/{id}/events. Strips server-set read-only fields
        so Graph re-mints them on create.

        The restore-worker's ``_afi_transform_event_for_restore`` is the
        layer that handles identity-bound stripping (organizer /
        isOrganizer / responseStatus / attendees) and re-renders those
        fields into a provenance banner inside ``body.content``. That
        transformation is restore-specific and stays out of this
        generic Graph helper so other callers (e.g. future
        programmatic-create paths) aren't forced into restore
        semantics.
        """
        url = f"{self.GRAPH_URL}/users/{user_id}/events"
        readonly = {
            "id", "createdDateTime", "lastModifiedDateTime", "changeKey",
            "iCalUId", "webLink", "onlineMeeting", "transactionId",
            "@odata.etag", "@odata.context",
        }
        clean = {k: v for k, v in event_payload.items() if k not in readonly}
        return await self._post(url, clean)

    CONTACT_READONLY_FIELDS = {
        "id", "createdDateTime", "lastModifiedDateTime", "changeKey",
        "@odata.etag", "@odata.context", "parentFolderId",
    }

    @classmethod
    def clean_contact_payload(cls, contact_payload: Dict[str, Any]) -> Dict[str, Any]:
        """Strip server-minted fields so Graph re-derives them on POST."""
        return {
            k: v for k, v in contact_payload.items()
            if k not in cls.CONTACT_READONLY_FIELDS
        }

    async def create_user_contact(self, user_id: str, contact_payload: Dict[str, Any]) -> Dict[str, Any]:
        """POST /users/{id}/contacts — restore a personal contact to the
        default Contacts folder. For folder-aware routing use
        ``create_user_contact_in_folder``."""
        url = f"{self.GRAPH_URL}/users/{user_id}/contacts"
        return await self._post(url, self.clean_contact_payload(contact_payload))

    async def create_user_contact_in_folder(
        self, user_id: str, folder_id: str, contact_payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        """POST /users/{id}/contactFolders/{fid}/contacts — folder-scoped create."""
        url = f"{self.GRAPH_URL}/users/{user_id}/contactFolders/{folder_id}/contacts"
        return await self._post(url, self.clean_contact_payload(contact_payload))

    async def list_contact_folders(self, user_id: str) -> List[Dict[str, Any]]:
        """GET /users/{id}/contactFolders — used by the restore engine to
        map snapshot `folder_path` values to folder ids. Single page is
        always enough in practice (Outlook caps custom contact folders at
        128 per mailbox); the $top=100 limit follows the discovery
        probe."""
        resp = await self._get(
            f"{self.GRAPH_URL}/users/{user_id}/contactFolders",
            params={"$top": "100", "$select": "id,displayName,parentFolderId"},
        )
        return (resp or {}).get("value", []) or []

    async def create_contact_folder(
        self, user_id: str, name: str, parent_folder_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """POST /users/{id}/contactFolders — idempotency is the caller's
        responsibility (check list_contact_folders first). Nested folders
        are supported via parent_folder_id → /contactFolders/{pid}/childFolders."""
        if parent_folder_id:
            url = (
                f"{self.GRAPH_URL}/users/{user_id}"
                f"/contactFolders/{parent_folder_id}/childFolders"
            )
        else:
            url = f"{self.GRAPH_URL}/users/{user_id}/contactFolders"
        return await self._post(url, {"displayName": name})

    async def attach_file_to_event(
        self, user_id: str, event_id: str, name: str,
        content_bytes: bytes, content_type: Optional[str] = None,
        is_inline: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """POST /users/{id}/events/{eid}/attachments — inline base64 upload."""
        import base64 as _b64
        url = f"{self.GRAPH_URL}/users/{user_id}/events/{event_id}/attachments"
        payload = {
            "@odata.type": "#microsoft.graph.fileAttachment",
            "name": name,
            "contentType": content_type or "application/octet-stream",
            "contentBytes": _b64.b64encode(content_bytes).decode("ascii"),
            "isInline": is_inline,
        }
        try:
            return await self._post(url, payload)
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (403, 404):
                return None
            raise

    # ── Permission RESTORE ──────────────────────────────────────────────────
    # Replay sharing grants captured by list_file_permissions onto a freshly
    # restored file. Two grant shapes:
    #   - User/group invite — POST /items/{id}/invite  with recipients + roles
    #   - Anonymous / org link — POST /items/{id}/createLink  with type + scope
    # Inherited permissions can't be re-created via API (they come from the
    # parent folder), so callers should filter them out before calling.

    async def invite_to_drive_item(
        self, drive_id: str, item_id: str, recipients: List[Dict[str, str]],
        roles: List[str], require_signin: bool = True, send_invitation: bool = False,
        message: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """POST /drives/{did}/items/{iid}/invite — grants users specific roles."""
        url = f"{self.GRAPH_URL}/drives/{drive_id}/items/{item_id}/invite"
        payload = {
            "recipients": recipients,
            "roles": roles,
            "requireSignIn": require_signin,
            "sendInvitation": send_invitation,
        }
        if message:
            payload["message"] = message
        try:
            return await self._post(url, payload)
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (403, 404):
                return None
            raise

    async def create_drive_item_link(
        self, drive_id: str, item_id: str, link_type: str, scope: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """POST /drives/{did}/items/{iid}/createLink — recreates a sharing link.
        link_type: 'view' | 'edit' | 'embed'. scope: 'anonymous' | 'organization' | 'users'."""
        url = f"{self.GRAPH_URL}/drives/{drive_id}/items/{item_id}/createLink"
        payload: Dict[str, Any] = {"type": link_type}
        if scope:
            payload["scope"] = scope
        try:
            return await self._post(url, payload)
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (403, 404):
                return None
            raise

    # ── Mailbox folder tree ─────────────────────────────────────────────────
    # Each message's `parentFolderId` is opaque; to reconstruct "/Inbox/Project X"
    # we must walk the folder tree once per user. afi rebuilds the hierarchy on
    # restore — without the full path we can only restore items to a flat root.

    async def get_mail_folder_tree(
        self,
        user_id: str,
        well_known_root: Optional[str] = None,
        with_stats: bool = False,
    ):
        """Return a flat map: folder_id → full path like "/Inbox/Subfolder".

        If well_known_root is provided (e.g., 'archive', 'recoverableitemsroot'),
        starts the walk at that special folder instead of the primary mailbox.
        Returns empty dict if the root folder doesn't exist (no archive license,
        no Exchange mailbox, etc.).

        When ``with_stats=True`` the returned shape switches to
        ``Dict[fid, {"path": str, "totalItemCount": int,
        "unreadItemCount": int, "sizeInBytes": int}]`` — same HTTP cost
        (Graph returns these fields for free, we just add them to
        ``$select``), but lets callers build a fingerprint per folder
        for incremental skip decisions.
        """
        sel_base = "id,displayName,childFolderCount"
        sel_stats = (
            sel_base + ",totalItemCount,unreadItemCount,sizeInBytes"
            if with_stats else sel_base
        )
        if well_known_root:
            root_url = f"{self.GRAPH_URL}/users/{user_id}/mailFolders/{well_known_root}"
            try:
                root = await self._get(root_url, params={"$select": sel_stats})
            except httpx.HTTPStatusError as e:
                if e.response.status_code in (403, 404):
                    return {}
                raise
            roots = [root]
        else:
            try:
                top = await self._get(
                    f"{self.GRAPH_URL}/users/{user_id}/mailFolders",
                    params={"$top": "200", "$select": sel_stats},
                )
            except httpx.HTTPStatusError as e:
                if e.response.status_code in (403, 404):
                    return {}
                raise
            roots = top.get("value", []) or []

        tree: Dict[str, Any] = {}

        def _entry(folder: Dict[str, Any], path: str) -> Any:
            if not with_stats:
                return path
            return {
                "path": path,
                "totalItemCount": int(folder.get("totalItemCount") or 0),
                "unreadItemCount": int(folder.get("unreadItemCount") or 0),
                "sizeInBytes": int(folder.get("sizeInBytes") or 0),
            }

        # PERF #13: prewarm folder paths breadth-first with parallel fetches
        # at each level instead of serial DFS. For mailboxes with deep folder
        # trees (e.g. legal/archive tenants) this collapses N sequential
        # GET roundtrips into one parallel burst per level. We try $batch
        # first (up to 20 parents per round-trip) and fall back to
        # asyncio.gather of individual GETs on batch failure.
        try:
            from shared.graph_batch import BatchClient as _BC, BatchRequest as _BR
            _batch = _BC(self)
        except Exception:
            _batch, _BR = None, None

        async def _fetch_children_serial(parents: List[Dict[str, Any]],
                                         parent_paths: Dict[str, str]) -> List[Dict[str, Any]]:
            next_level: List[Dict[str, Any]] = []

            async def _one(p: Dict[str, Any]):
                fid = p.get("id")
                base = parent_paths.get(fid, "")
                try:
                    resp = await self._get(
                        f"{self.GRAPH_URL}/users/{user_id}/mailFolders/{fid}/childFolders",
                        params={"$top": "200", "$select": sel_stats},
                    )
                except httpx.HTTPStatusError as e:
                    if e.response.status_code in (403, 404):
                        return []
                    raise
                children = resp.get("value", []) or []
                for c in children:
                    name = c.get("displayName") or "(unnamed)"
                    cpath = f"{base}/{name}"
                    cid = c.get("id")
                    if cid:
                        tree[cid] = _entry(c, cpath)
                        parent_paths[cid] = cpath
                return children

            results = await asyncio.gather(
                *[_one(p) for p in parents], return_exceptions=True,
            )
            for r in results:
                if isinstance(r, list):
                    for c in r:
                        if c.get("childFolderCount", 0) > 0:
                            next_level.append(c)
                elif isinstance(r, Exception):
                    # logged in inner _one fallthrough is missing; surface for visibility
                    pass
            return next_level

        async def _fetch_children_batched(parents: List[Dict[str, Any]],
                                          parent_paths: Dict[str, str]) -> List[Dict[str, Any]]:
            """Bundle parent childFolders fetches via /$batch (20 at a time).
            Bails to serial-gather on any batch failure."""
            if not _batch or not _BR:
                return await _fetch_children_serial(parents, parent_paths)

            reqs: List[Any] = []
            id_to_parent: Dict[str, Dict[str, Any]] = {}
            for i, p in enumerate(parents):
                pid = p.get("id")
                if not pid:
                    continue
                rid = f"cf::{i}"
                id_to_parent[rid] = p
                # IMPORTANT: $batch sub-request urls are PATH-only, no host.
                reqs.append(_BR(
                    id=rid,
                    method="GET",
                    url=f"/users/{user_id}/mailFolders/{pid}/childFolders?$top=200&$select={sel_stats}",
                ))

            try:
                resp_map = await _batch.batch(reqs)
            except Exception:
                return await _fetch_children_serial(parents, parent_paths)

            next_level: List[Dict[str, Any]] = []
            for rid, sub in resp_map.items():
                if not (200 <= sub.status < 300):
                    # 403/404 silently skip (matches DFS behavior). Other
                    # errors: drop this parent's children; tree stays
                    # partial but caller already tolerates that.
                    continue
                parent = id_to_parent.get(rid)
                if not parent:
                    continue
                base = parent_paths.get(parent.get("id"), "")
                children = (sub.body or {}).get("value", []) or []
                for c in children:
                    name = c.get("displayName") or "(unnamed)"
                    cpath = f"{base}/{name}"
                    cid = c.get("id")
                    if cid:
                        tree[cid] = _entry(c, cpath)
                        parent_paths[cid] = cpath
                    if c.get("childFolderCount", 0) > 0:
                        next_level.append(c)
            return next_level

        # Seed level 0 from the roots.
        level0: List[Dict[str, Any]] = []
        parent_paths: Dict[str, str] = {}
        for r in roots:
            rid = r.get("id")
            rname = r.get("displayName") or "(unnamed)"
            rpath = f"/{rname}"
            if rid:
                tree[rid] = _entry(r, rpath)
                parent_paths[rid] = rpath
            if r.get("childFolderCount", 0) > 0 or "childFolderCount" not in r:
                level0.append(r)

        # BFS: at each level, fetch all parents' childFolders in parallel.
        # Depth cap of 12 — Outlook UI itself caps at much less; this is just
        # a safety stop against pathological loops.
        current_level = level0
        for _depth in range(12):
            if not current_level:
                break
            current_level = await _fetch_children_batched(current_level, parent_paths)

        return tree

    async def resolve_mail_folder_path(
        self, user_id: str, folder_id: str, max_depth: int = 20,
    ) -> Optional[str]:
        """Resolve a single mailFolder id to a path like "/Inbox/Subfolder"
        by walking up via parentFolderId.

        Why: `get_mail_folder_tree` uses the list endpoint
        (`/users/{id}/mailFolders`) which 403s on shared and room
        mailboxes under common Application Access Policy scopes, even
        when the same token can still read an individual folder by id.
        Backup-worker falls back to this per-message resolver when the
        top-down tree comes back empty so mail still lands with a
        meaningful folder_path.

        Results are cached per-instance keyed by (user_id, folder_id)
        so a mailbox with N messages across K folders does K lookups,
        not N. Returns None when even the single-id read is forbidden.
        """
        if not hasattr(self, "_mail_folder_idpath_cache") or self._mail_folder_idpath_cache is None:
            self._mail_folder_idpath_cache = {}
        root_key = (user_id, folder_id)
        if root_key in self._mail_folder_idpath_cache:
            return self._mail_folder_idpath_cache[root_key]

        segments: List[str] = []
        current: Optional[str] = folder_id
        visited: set = set()
        for _ in range(max_depth):
            if not current or current in visited:
                break
            visited.add(current)
            ck = (user_id, current)
            if ck in self._mail_folder_idpath_cache:
                # Hit on an ancestor — prepend its resolved path and stop.
                ancestor = self._mail_folder_idpath_cache[ck]
                if ancestor:
                    return "/".join([ancestor.rstrip("/")] + segments) if segments else ancestor
                break
            try:
                info = await self._get(
                    f"{self.GRAPH_URL}/users/{user_id}/mailFolders/{current}",
                    params={"$select": "id,displayName,parentFolderId"},
                )
            except httpx.HTTPStatusError as e:
                # Tenant policy blocks even per-id reads, or folder
                # doesn't exist. Cache the failure so we don't hammer
                # Graph for every message in the same dead chain.
                if e.response.status_code in (403, 404):
                    self._mail_folder_idpath_cache[root_key] = None
                    return None
                raise
            name = (info.get("displayName") or "").strip()
            parent = info.get("parentFolderId")
            if name:
                segments.insert(0, name)
            # Graph reports the hidden root folder ("Top of Information
            # Store") as having no parentFolderId, or parentFolderId
            # pointing back at itself. Either way: stop walking.
            if not parent or parent == current:
                break
            current = parent

        path = ("/" + "/".join(segments)) if segments else None
        self._mail_folder_idpath_cache[root_key] = path
        return path

    # Tier label → mailFolders well-known root used as the starting
    # container for path resolution.
    _MAIL_TIER_ROOTS = {
        "primary": "mailFolders",
        "archive": "mailFolders('archive')",
        "recoverable": "mailFolders('recoverableitemsroot')",
    }

    async def ensure_mail_folder_path(
        self,
        user_id: str,
        tier: str,
        path: str,
    ) -> Optional[str]:
        """Resolve or create a `/Segment1/Segment2/...` path under the
        given mailbox tier; return the final folder id. Caches hits for
        the lifetime of this GraphClient instance.

        Returns None when the tier root itself isn't provisioned on the
        target mailbox (404 on the first hop). Caller must handle.

        `tier` ∈ {"primary", "archive", "recoverable"}. Unknown tiers
        fall back to primary so callers that lose the tier label still
        restore somewhere useful.
        """
        if not hasattr(self, "_mail_folder_path_cache") or self._mail_folder_path_cache is None:
            self._mail_folder_path_cache = {}
        cache_key = (user_id, tier, path)
        cached = self._mail_folder_path_cache.get(cache_key)
        if cached is not None:
            return cached

        root_segment = self._MAIL_TIER_ROOTS.get(tier, self._MAIL_TIER_ROOTS["primary"])
        segments = [s for s in (path or "").split("/") if s]
        if not segments:
            # Empty path → return the tier root's id by fetching the
            # well-known folder descriptor. Cache on its own key.
            try:
                resp = await self._get(f"{self.GRAPH_URL}/users/{user_id}/{root_segment}")
                root_id = resp.get("id") if isinstance(resp, dict) else None
            except Exception:
                return None
            self._mail_folder_path_cache[cache_key] = root_id
            return root_id

        # Walk segments: at each step look up the child by displayName
        # under the current parent; create if missing.
        #
        # Graph's URL shape for the tier root is asymmetric:
        #   primary      → /users/{u}/mailFolders             (a collection)
        #   archive      → /users/{u}/mailFolders('archive')  (a singleton)
        #   recoverable  → /users/{u}/mailFolders('recoverableitemsroot')
        #
        # Listing children of the primary collection is just a GET on the
        # collection; listing children of a singleton needs /childFolders.
        # Creating a top-level folder is the opposite: POST the collection
        # directly, or POST /childFolders on a singleton. Once we have a
        # concrete parent_id, both tiers use the same /mailFolders/{id}/
        # childFolders endpoint.
        is_primary_root = tier == "primary" or tier not in self._MAIL_TIER_ROOTS
        if is_primary_root:
            root_list_path = f"/users/{user_id}/{root_segment}"
            root_create_path = f"/users/{user_id}/{root_segment}"
        else:
            root_list_path = f"/users/{user_id}/{root_segment}/childFolders"
            root_create_path = f"/users/{user_id}/{root_segment}/childFolders"

        parent_id: Optional[str] = None
        for i, name in enumerate(segments):
            if parent_id is None:
                lookup_url = f"{self.GRAPH_URL}{root_list_path}"
            else:
                lookup_url = f"{self.GRAPH_URL}/users/{user_id}/mailFolders/{parent_id}/childFolders"
            # $filter on displayName; Graph matches exactly (case-sensitive).
            safe_name = name.replace("'", "''")
            list_url = f"{lookup_url}?$filter=displayName eq '{safe_name}'&$top=1"
            try:
                resp = await self._get(list_url)
            except Exception:
                return None
            values = resp.get("value", []) if isinstance(resp, dict) else []
            if values:
                parent_id = values[0].get("id")
            else:
                # Create under the current parent.
                if parent_id is None:
                    create_url = f"{self.GRAPH_URL}{root_create_path}"
                else:
                    create_url = f"{self.GRAPH_URL}/users/{user_id}/mailFolders/{parent_id}/childFolders"
                created = await self._post(create_url, {"displayName": name})
                parent_id = created.get("id") if isinstance(created, dict) else None
                if parent_id is None:
                    return None
            # Cache incremental paths so partial overlaps of future calls
            # get free hits (e.g. /Inbox/A then /Inbox/B).
            partial = "/" + "/".join(segments[: i + 1])
            self._mail_folder_path_cache[(user_id, tier, partial)] = parent_id
        return parent_id

    async def list_folder_internet_message_ids(
        self,
        user_id: str,
        folder_id: str,
    ) -> Dict[str, str]:
        """Return `{internetMessageId → graphMessageId}` for every
        message currently in the folder. Used as the dedup sieve for
        overwrite-mode restores: if an IMID matches, we PATCH the
        existing message instead of creating a duplicate.

        Follows @odata.nextLink; skips rows with no internetMessageId.
        """
        out: Dict[str, str] = {}
        url = (
            f"{self.GRAPH_URL}/users/{user_id}/mailFolders/{folder_id}/messages"
            f"?$select=id,internetMessageId&$top=1000"
        )
        while url:
            resp = await self._get(url)
            if not isinstance(resp, dict):
                break
            for row in resp.get("value", []) or []:
                imid = row.get("internetMessageId")
                gid = row.get("id")
                if imid and gid:
                    out[imid] = gid
            url = resp.get("@odata.nextLink")
        return out

    async def create_message_in_folder(
        self,
        user_id: str,
        folder_id: str,
        payload: Dict[str, Any],
    ) -> Optional[str]:
        """Create a message inside a specific folder. Returns the new
        Graph message id, or None if the response didn't include one.

        NOTE: Graph always sets ``isDraft=true`` on messages created via
        JSON POST, and it silently overwrites ``from`` / ``sender`` with
        the mailbox owner. Use ``create_mime_message`` for true restore.
        """
        url = f"{self.GRAPH_URL}/users/{user_id}/mailFolders/{folder_id}/messages"
        resp = await self._post(url, payload)
        return resp.get("id") if isinstance(resp, dict) else None

    async def create_mime_message(
        self,
        user_id: str,
        mime_bytes: bytes,
        folder_id: Optional[str] = None,
    ) -> Optional[str]:
        """Import an RFC-822 MIME message via Graph.

        Graph has a quirk that isn't obvious from the summary docs: POST
        to ``/users/{id}/messages`` with ``Content-Type: text/plain``
        ALWAYS lands the message in Drafts with ``isDraft=true`` — no
        amount of MIME header tweaking changes that. The only way to
        create a non-draft message via MIME is to POST to the per-folder
        endpoint ``/users/{id}/mailFolders/{folder-id}/messages`` with
        the same content type. Graph respects the folder choice there and
        imports the MIME with ``isDraft=false`` when the target isn't
        Drafts.

        Pass ``folder_id`` when known (normal restore path). When it's
        omitted we fall back to the mailbox root which preserves the old
        behaviour (imports as draft) — callers that want a non-draft
        restore must supply a target folder.

        The MIME itself preserves ``From`` / ``Sender`` / ``Date`` /
        ``Message-ID`` / attachments / inline CIDs exactly as captured.
        """
        import base64 as _b64
        if folder_id:
            url = f"{self.GRAPH_URL}/users/{user_id}/mailFolders/{folder_id}/messages"
        else:
            url = f"{self.GRAPH_URL}/users/{user_id}/messages"
        token = await self._get_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "text/plain",
        }
        body = _b64.b64encode(mime_bytes)
        async with self._http_session() as client:
            resp = await client.post(url, headers=headers, content=body)
            if resp.status_code == 429 or resp.status_code == 503:
                await asyncio.sleep(_parse_retry_after(resp))
                resp = await client.post(url, headers=headers, content=body)
            if resp.status_code >= 400:
                raise RuntimeError(
                    f"create_mime_message {resp.status_code}: {resp.text[:300]}"
                )
            data = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
            return data.get("id") if isinstance(data, dict) else None

    async def move_message(
        self,
        user_id: str,
        message_id: str,
        destination_folder_id: str,
    ) -> Optional[str]:
        """Move a message to another mail folder. Returns the new message
        id (Graph rewrites the id on move)."""
        url = f"{self.GRAPH_URL}/users/{user_id}/messages/{message_id}/move"
        resp = await self._post(url, {"destinationId": destination_folder_id})
        return resp.get("id") if isinstance(resp, dict) else None

    async def upload_small_file_to_drive(
        self,
        drive_id: str,
        drive_path: str,
        body: bytes,
        conflict_behavior: str = "rename",
    ) -> Dict[str, Any]:
        """Single-PUT upload for files < 4 MB (Graph's simple-upload cap).

        Targets ``/drives/{drive_id}/root:/{path}:/content`` — this works
        for any driveItem host (personal OneDrive, SharePoint document
        library, Group drive) because drive_id is the canonical Graph
        identifier. conflict_behavior = "replace" | "rename" | "fail" —
        passed as a URL query parameter because httpx (RFC 7230) rejects
        the ``@`` character in HTTP header names, so the
        ``@microsoft.graph.conflictBehavior`` header form Graph also
        accepts isn't usable here.
        Returns the created driveItem dict.
        """
        from urllib.parse import quote as _q
        # URL-escape each path segment (spaces / unicode / `#` are valid
        # in OneDrive filenames but break Graph routing unescaped). Keep
        # `/` as a literal separator by quoting each segment then
        # rejoining.
        quoted_path = "/".join(_q(seg, safe="") for seg in drive_path.split("/") if seg)
        url = (
            f"{self.GRAPH_URL}/drives/{drive_id}/root:/"
            f"{quoted_path}:/content"
            f"?@microsoft.graph.conflictBehavior={_q(conflict_behavior)}"
        )
        token = await self._get_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/octet-stream",
        }
        async with self._http_session() as c:
            resp = await c.put(url, headers=headers, content=body)
            if resp.status_code in (429, 503):
                await asyncio.sleep(_parse_retry_after(resp))
                resp = await c.put(url, headers=headers, content=body)
            if resp.status_code >= 400:
                raise RuntimeError(
                    f"upload_small_file_to_drive {resp.status_code}: "
                    f"{resp.text[:300]} · url={url}"
                )
            return resp.json()

    async def upload_large_file_to_drive(
        self,
        drive_id: str,
        drive_path: str,
        body: bytes,
        total_size: int,
        chunk_size: int = 10 * 1024 * 1024,
        conflict_behavior: str = "rename",
    ) -> Dict[str, Any]:
        """Resumable upload for files >= 4 MB via Graph's uploadSession.

        Splits body into chunk_size chunks (MS mandates multiples of 320
        KiB; 10 MiB is safe). Each chunk is PUT with a Content-Range:
        bytes X-Y/total header; the final chunk's response returns the
        created driveItem with status 201.

        Retries each chunk up to 3x on 500/503/connection reset — the
        uploadSession URL stays valid so retries never re-upload earlier
        chunks on a mid-file failure.
        """
        from urllib.parse import quote as _qseg
        quoted_path_lg = "/".join(_qseg(seg, safe="") for seg in drive_path.split("/") if seg)
        create_url = (
            f"{self.GRAPH_URL}/drives/{drive_id}/root:/"
            f"{quoted_path_lg}:/createUploadSession"
        )
        token = await self._get_token()
        create_headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        create_payload = {
            "item": {
                "@microsoft.graph.conflictBehavior": conflict_behavior,
            }
        }
        async with self._http_session() as c:
            session_resp = await c.post(create_url, headers=create_headers, json=create_payload)
            if session_resp.status_code >= 400:
                raise RuntimeError(
                    f"createUploadSession {session_resp.status_code}: {session_resp.text[:300]}"
                )
            upload_url = session_resp.json().get("uploadUrl")
            if not upload_url:
                raise RuntimeError("createUploadSession returned no uploadUrl")

            offset = 0
            last_json: Dict[str, Any] = {}
            while offset < total_size:
                end = min(offset + chunk_size, total_size) - 1
                chunk = body[offset:end + 1]
                put_headers = {
                    "Content-Length": str(len(chunk)),
                    "Content-Range": f"bytes {offset}-{end}/{total_size}",
                }
                attempt = 0
                while True:
                    attempt += 1
                    try:
                        resp = await c.put(upload_url, headers=put_headers, content=chunk)
                    except (httpx.ConnectError, httpx.ReadTimeout, httpx.RemoteProtocolError):
                        if attempt >= 3:
                            raise
                        await asyncio.sleep(min(2 ** attempt, 30))
                        continue
                    if resp.status_code in (429, 503):
                        await asyncio.sleep(_parse_retry_after(resp))
                        continue
                    if resp.status_code >= 500 and attempt < 3:
                        await asyncio.sleep(min(2 ** attempt, 30))
                        continue
                    if resp.status_code >= 400:
                        raise RuntimeError(
                            f"upload chunk {offset}-{end} {resp.status_code}: {resp.text[:300]}"
                        )
                    if resp.status_code in (200, 201):
                        last_json = resp.json()
                    break
                offset = end + 1
            return last_json

    async def upload_large_file_stream_to_drive(
        self,
        drive_id: str,
        drive_path: str,
        byte_iter: "AsyncIterator[bytes]",
        total_size: int,
        chunk_size: int = 10 * 1024 * 1024,
        conflict_behavior: str = "rename",
    ) -> Dict[str, Any]:
        """Streaming analogue of upload_large_file_to_drive: takes an
        async byte iterator instead of a fully-buffered `bytes`.
        Pipes the iterator directly into Graph's uploadSession so a
        100 GB restore never materialises 100 GB of Python heap.

        Buffering: the caller's iterator may yield arbitrary-sized
        chunks (S3 range reads produce variable pieces), but the
        uploadSession requires each PUT to be a multiple of 320 KiB
        *except the last*. We accumulate into a bytearray and flush
        exactly `chunk_size` at a time, with whatever remains going
        out as the tail PUT.

        Retries: per-PUT retry on 429/5xx/transient TCP with the
        uploadSession URL staying valid across attempts — identical
        semantics to the buffered variant. If the iterator raises,
        we propagate; Graph garbage-collects the stale uploadSession
        within ~24h.
        """
        from urllib.parse import quote as _qseg
        quoted_path_lg = "/".join(
            _qseg(seg, safe="") for seg in drive_path.split("/") if seg
        )
        create_url = (
            f"{self.GRAPH_URL}/drives/{drive_id}/root:/"
            f"{quoted_path_lg}:/createUploadSession"
        )
        token = await self._get_token()
        create_headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        create_payload = {
            "item": {
                "@microsoft.graph.conflictBehavior": conflict_behavior,
            }
        }

        # Enforce Graph's 320 KiB alignment requirement. Round the
        # caller's chunk_size down to the nearest multiple.
        CHUNK_ALIGN = 320 * 1024
        aligned = max(CHUNK_ALIGN, (chunk_size // CHUNK_ALIGN) * CHUNK_ALIGN)

        async with self._http_session() as c:
            session_resp = await c.post(
                create_url, headers=create_headers, json=create_payload,
            )
            if session_resp.status_code >= 400:
                raise RuntimeError(
                    f"createUploadSession {session_resp.status_code}: "
                    f"{session_resp.text[:300]}"
                )
            upload_url = session_resp.json().get("uploadUrl")
            if not upload_url:
                raise RuntimeError(
                    "createUploadSession returned no uploadUrl"
                )

            async def _put_one(chunk_bytes: bytes, offset: int, is_last: bool) -> Dict[str, Any]:
                end = offset + len(chunk_bytes) - 1
                put_headers = {
                    "Content-Length": str(len(chunk_bytes)),
                    "Content-Range": (
                        f"bytes {offset}-{end}/{total_size}"
                    ),
                }
                attempt = 0
                while True:
                    attempt += 1
                    try:
                        resp = await c.put(
                            upload_url, headers=put_headers,
                            content=chunk_bytes,
                        )
                    except (httpx.ConnectError, httpx.ReadTimeout,
                            httpx.RemoteProtocolError):
                        if attempt >= 3:
                            raise
                        await asyncio.sleep(min(2 ** attempt, 30))
                        continue
                    if resp.status_code in (429, 503):
                        await asyncio.sleep(_parse_retry_after(resp))
                        continue
                    if resp.status_code >= 500 and attempt < 3:
                        await asyncio.sleep(min(2 ** attempt, 30))
                        continue
                    if resp.status_code >= 400:
                        raise RuntimeError(
                            f"upload chunk {offset}-{end} "
                            f"{resp.status_code}: {resp.text[:300]}"
                        )
                    if resp.status_code in (200, 201):
                        return resp.json()
                    return {}

            buf = bytearray()
            offset = 0
            bytes_seen = 0
            last_json: Dict[str, Any] = {}

            async for piece in byte_iter:
                if not piece:
                    continue
                bytes_seen += len(piece)
                buf.extend(piece)
                while len(buf) >= aligned:
                    chunk = bytes(buf[:aligned])
                    del buf[:aligned]
                    is_last_put = (
                        offset + len(chunk) >= total_size and not buf
                    )
                    last_json = await _put_one(chunk, offset, is_last_put)
                    offset += len(chunk)

            # Flush tail (may be 0..aligned-1 bytes). If total_size
            # was mis-specified smaller than what the iterator
            # produced, we still send what we have; Graph will 400
            # and raise.
            if buf:
                tail = bytes(buf)
                last_json = await _put_one(tail, offset, True)
                offset += len(tail)

            if offset != total_size:
                raise RuntimeError(
                    f"upload stream size mismatch: sent {offset}, "
                    f"declared {total_size} (iter yielded {bytes_seen})"
                )
            return last_json

    async def patch_drive_item_file_system_info(
        self,
        drive_id: str,
        drive_item_id: str,
        created_iso: Optional[str] = None,
        modified_iso: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Restore captured creation + modification timestamps on a
        driveItem. Without this, Explorer shows every restored file as
        created / modified "today". Normalises "+00:00" into the Z form
        Graph accepts; skips the call entirely when both inputs are
        empty.
        """
        def _norm(ts: Optional[str]) -> Optional[str]:
            if not ts:
                return None
            t = ts.strip()
            if t.endswith("+00:00"):
                t = t[:-6] + "Z"
            elif "+" not in t and not t.endswith("Z"):
                t = t + "Z"
            return t

        created = _norm(created_iso)
        modified = _norm(modified_iso)
        if not (created or modified):
            return None
        fsi: Dict[str, str] = {}
        if created:
            fsi["createdDateTime"] = created
        if modified:
            fsi["lastModifiedDateTime"] = modified
        url = f"{self.GRAPH_URL}/drives/{drive_id}/items/{drive_item_id}"
        return await self._patch(url, {"fileSystemInfo": fsi})

    async def json_create_non_draft_message(
        self,
        user_id: str,
        folder_id: str,
        payload: Dict[str, Any],
    ) -> Optional[str]:
        """Create a message via JSON POST in a specific folder, with
        ``PR_MESSAGE_FLAGS`` preset so it lands as non-draft.

        This is the hybrid technique backup vendors (AFI, Veeam, Spanning)
        use because Graph's MIME import is Drafts-only and its
        PATCH-level write to ``PR_MESSAGE_FLAGS`` is server-ignored. At
        CREATE time Exchange respects ``singleValueExtendedProperties``
        including the normally-read-only MSGFLAG_UNSENT bit, so injecting
        ``Integer 0x0E07 = 1`` (READ, not UNSENT) at create produces a
        message that Outlook renders as real (received/sent) mail.

        Caller should PATCH sender ``singleValueExtendedProperties``
        afterwards to restore the original From / Sender — JSON POST
        silently overrides those with the mailbox owner.
        """
        props = list(payload.get("singleValueExtendedProperties") or [])
        props.append({"id": "Integer 0x0E07", "value": "1"})
        payload = dict(payload)
        payload["singleValueExtendedProperties"] = props
        url = f"{self.GRAPH_URL}/users/{user_id}/mailFolders/{folder_id}/messages"
        resp = await self._post(url, payload)
        return resp.get("id") if isinstance(resp, dict) else None

    async def patch_original_timestamps(
        self,
        user_id: str,
        message_id: str,
        sent_iso: Optional[str] = None,
        received_iso: Optional[str] = None,
    ) -> None:
        """Restore the message's original send / receive timestamps via
        MAPI extended properties.

        Graph ignores ``sentDateTime`` / ``receivedDateTime`` on JSON
        create and stamps server-now instead, so the restored mail would
        otherwise show "today" in Outlook. Writing the underlying MAPI
        tags directly fixes the displayed date columns:

          * ``SystemTime 0x0039`` PR_CLIENT_SUBMIT_TIME  — Sent column
          * ``SystemTime 0x0E06`` PR_MESSAGE_DELIVERY_TIME — Received column
          * ``SystemTime 0x3007`` PR_CREATION_TIME
          * ``SystemTime 0x3008`` PR_LAST_MODIFICATION_TIME

        Timestamps must be ISO-8601 UTC (``2024-01-15T10:30:00Z`` form);
        we normalise the common ``…+00:00`` form Graph emits into the
        trailing-``Z`` form Exchange expects.
        """
        def _norm(ts: Optional[str]) -> Optional[str]:
            if not ts:
                return None
            t = ts.strip()
            if t.endswith("+00:00"):
                t = t[:-6] + "Z"
            elif "+" not in t and not t.endswith("Z"):
                t = t + "Z"
            return t

        sent = _norm(sent_iso)
        recv = _norm(received_iso)
        if not (sent or recv):
            return
        props: List[Dict[str, str]] = []
        if sent:
            props.append({"id": "SystemTime 0x0039", "value": sent})
            props.append({"id": "SystemTime 0x3007", "value": sent})
        if recv:
            props.append({"id": "SystemTime 0x0E06", "value": recv})
            props.append({"id": "SystemTime 0x3008", "value": recv})
        url = f"{self.GRAPH_URL}/users/{user_id}/messages/{message_id}"
        await self._patch(url, {"singleValueExtendedProperties": props})

    async def patch_sender_extended_properties(
        self,
        user_id: str,
        message_id: str,
        sender_name: Optional[str],
        sender_address: Optional[str],
    ) -> None:
        """Overwrite a restored message's sender to the original
        From/Sender captured in the snapshot.

        Graph's JSON create silently rewrites ``from`` and ``sender`` to
        the mailbox owner, so we have to come back via MAPI tags:
          * ``String 0x0042`` PR_SENT_REPRESENTING_NAME
          * ``String 0x0065`` PR_SENT_REPRESENTING_EMAIL_ADDRESS
          * ``String 0x0064`` PR_SENT_REPRESENTING_ADDRTYPE = "SMTP"
          * ``String 0x0C1A`` PR_SENDER_NAME
          * ``String 0x0C1F`` PR_SENDER_EMAIL_ADDRESS
          * ``String 0x0C1E`` PR_SENDER_ADDRTYPE = "SMTP"
        Outlook's From column is computed from the PR_SENT_REPRESENTING_*
        pair, with PR_SENDER_* as fallback. Setting both avoids edge
        cases where one bag is preferred over the other.
        """
        if not (sender_name or sender_address):
            return
        props: List[Dict[str, str]] = []
        if sender_name:
            props.append({"id": "String 0x0042", "value": sender_name})
            props.append({"id": "String 0x0C1A", "value": sender_name})
        if sender_address:
            props.append({"id": "String 0x0065", "value": sender_address})
            props.append({"id": "String 0x0C1F", "value": sender_address})
        props.append({"id": "String 0x0064", "value": "SMTP"})
        props.append({"id": "String 0x0C1E", "value": "SMTP"})
        url = f"{self.GRAPH_URL}/users/{user_id}/messages/{message_id}"
        await self._patch(url, {"singleValueExtendedProperties": props})

    async def clear_draft_flag(self, user_id: str, message_id: str) -> None:
        """Force a restored message out of Drafts into a normal
        sent/received state by patching the MAPI PR_MESSAGE_FLAGS
        property (tag ``Integer 0x0E07``).

        Bit meanings (from MS-OXCMSG):
            0x00000001  MSGFLAG_READ     — message has been read
            0x00000008  MSGFLAG_UNSENT   — message is a draft
            0x00000010  MSGFLAG_UNMODIFIED
            0x00000020  MSGFLAG_SUBMIT
            0x00000040  MSGFLAG_HASATTACH

        Graph's MIME import often leaves ``UNSENT`` set, which is what
        makes Outlook render the restored mail as a draft with "sender
        unknown" cues even though the mailbox is correct. Writing the
        flags to ``1`` clears UNSENT + keeps READ — matching what
        Veeam/AFI do for immutable mail restore. We also set
        ``PR_MSG_EDITOR_FORMAT`` (``Integer 0x5909``) to ``2`` (HTML)
        as a belt-and-braces hint so clients pick the HTML part for
        display.
        """
        url = f"{self.GRAPH_URL}/users/{user_id}/messages/{message_id}"
        body = {
            "singleValueExtendedProperties": [
                {"id": "Integer 0x0E07", "value": "1"},
                {"id": "Integer 0x5909", "value": "2"},
            ],
        }
        await self._patch(url, body)

    # Fields AFI patches on an already-existing matched message. Body,
    # recipients, subject, and dates are immutable per Graph and AFI
    # does not attempt them.
    _PATCHABLE_FIELDS = ("isRead", "flag", "importance", "categories")

    async def patch_message_metadata(
        self,
        user_id: str,
        message_id: str,
        snapshot_raw: Dict[str, Any],
    ) -> None:
        """PATCH mutable metadata from a snapshot payload onto an
        already-existing message. Silently drops any fields outside the
        `_PATCHABLE_FIELDS` whitelist."""
        patch: Dict[str, Any] = {}
        for field in self._PATCHABLE_FIELDS:
            if field in snapshot_raw:
                patch[field] = snapshot_raw[field]
        if not patch:
            return
        url = f"{self.GRAPH_URL}/users/{user_id}/messages/{message_id}"
        await self._patch(url, patch)

    async def post_small_attachment(
        self,
        user_id: str,
        message_id: str,
        attachment_payload: Dict[str, Any],
    ) -> None:
        """Single POST for attachments < MAIL_RESTORE_ATTACH_LARGE_MB.
        `attachment_payload` must already carry the `@odata.type`
        discriminator and `contentBytes` (base64) for fileAttachment or
        `item` for itemAttachment. Caller shapes the payload."""
        url = f"{self.GRAPH_URL}/users/{user_id}/messages/{message_id}/attachments"
        await self._post(url, attachment_payload)

    async def upload_large_attachment(
        self,
        user_id: str,
        message_id: str,
        name: str,
        size: int,
        content_bytes: bytes,
        content_type: Optional[str] = None,
        is_inline: bool = False,
    ) -> None:
        """Chunked upload for attachments >= MAIL_RESTORE_ATTACH_LARGE_MB
        using Graph's uploadSession endpoint."""
        create_url = (
            f"{self.GRAPH_URL}/users/{user_id}/messages/{message_id}"
            f"/attachments/createUploadSession"
        )
        session = await self._post(create_url, {
            "AttachmentItem": {
                "attachmentType": "file",
                "name": name,
                "size": size,
                "contentType": content_type or "application/octet-stream",
                "isInline": is_inline,
            }
        })
        upload_url = session.get("uploadUrl") if isinstance(session, dict) else None
        if not upload_url:
            raise RuntimeError("uploadSession: Graph did not return uploadUrl")

        chunk_size = 4 * 1024 * 1024  # 4 MiB per Microsoft's guidance.
        total = len(content_bytes)
        async with self._http_session() as client:
            start = 0
            while start < total:
                end = min(start + chunk_size, total) - 1
                chunk = content_bytes[start:end + 1]
                headers = {
                    "Content-Length": str(len(chunk)),
                    "Content-Range": f"bytes {start}-{end}/{total}",
                }
                r = await client.put(upload_url, content=chunk, headers=headers)
                if r.status_code not in (200, 201, 202):
                    raise RuntimeError(
                        f"uploadSession chunk failed at {start}-{end}: HTTP {r.status_code}"
                    )
                start = end + 1

    async def list_messages_in_folder(
        self, user_id: str, folder_id: str, top: int = 999,
    ) -> List[Dict[str, Any]]:
        """Fetch all messages directly inside a single mail folder. Used for
        pulling Online Archive / Recoverable Items content where the top-level
        /messages endpoint doesn't reach."""
        url = f"{self.GRAPH_URL}/users/{user_id}/mailFolders/{folder_id}/messages"
        try:
            result = await self._get(url, params={"$top": str(top)})
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (403, 404):
                return []
            raise
        items = result.get("value", []) or []
        while "@odata.nextLink" in result:
            result = await self._get(result["@odata.nextLink"])
            items.extend(result.get("value", []))
        return items

    async def get_drive_items_delta(self, drive_id: str, delta_token: str = None) -> Dict[str, Any]:
        """
        Get drive items using delta API.
        Works with both user drives and SharePoint drives.
        Graph API: GET /drives/{drive-id}/root/delta

        NOTE: The delta endpoint does NOT support $select. It returns a fixed
        set of properties (id, name, size, file, folder, deleted, eTag,
        lastModifiedDateTime, @microsoft.graph.downloadUrl, etc.).
        """
        # Use /drives/{drive-id}/root/delta — works for any drive type
        url = f"{self.GRAPH_URL}/drives/{drive_id}/root/delta"
        if delta_token:
            url = delta_token

        # No $select or $expand — delta endpoint ignores them and returns empty
        params = {"$top": "999"}
        result = await self._get(url, params=params)
        all_value = result.get("value", [])

        # Follow pagination
        while "@odata.nextLink" in result:
            next_url = result["@odata.nextLink"]
            result = await self._get(next_url)
            all_value.extend(result.get("value", []))

        result["value"] = all_value
        return result

    async def get_user_onedrive_root(self, user_id: str) -> Dict[str, Any]:
        """
        Get user's OneDrive root drive info.
        Graph API: GET /users/{id}/drive
        """
        return await self._get(f"{self.GRAPH_URL}/users/{user_id}/drive")

    async def get_download_urls_batch(
        self,
        drive_id: str,
        item_ids: "List[str]",
    ) -> "Dict[str, Tuple[Optional[str], int, Optional[str]]]":
        """Bulk-fetch downloadUrl + size + quickXorHash for up to N items
        via /v1.0/$batch. Returns a map {item_id: (download_url, size,
        quickXorHash)} — download_url may be None when the item is a
        cloud-native object (Whiteboard, OneNote, Loop component) with
        no downloadable bytes.

        Enterprise win: the per-file GET /drives/{}/items/{} call is
        the serial bottleneck on many-small-files drives. For 10k
        files at ~200 ms/GET that's ~33 min of wall time just for
        URL-fetching even with worker-side concurrency, because each
        GET counts against the per-app RPS ceiling. $batch bundles
        20 requests per HTTP call and each sub-request is still billed
        separately — but wire-time drops ~20×, freeing worker
        connection slots for actual file downloads.

        Errors per-item (404 deleted, 403 restricted) are surfaced as
        `(None, 0, None)` entries rather than aborting the whole batch,
        so one bad file doesn't poison the drive's URL map.
        """
        from shared.graph_batch import BatchRequest
        if not item_ids:
            return {}
        reqs = [
            BatchRequest(
                id=iid, method="GET",
                url=f"/drives/{drive_id}/items/{iid}",
            )
            for iid in item_ids
        ]
        out: Dict[str, Tuple[Optional[str], int, Optional[str]]] = {}
        try:
            responses = await self.batch(reqs)
        except Exception as exc:
            # Batch call itself failed — fall back to per-item GETs
            # so throughput degrades gracefully instead of erroring
            # the whole drive. Caller keeps working.
            print(
                f"[GraphClient] get_download_urls_batch failed: "
                f"{type(exc).__name__}: {exc}; caller should fall "
                f"back to per-item get_download_url",
            )
            return {}
        for iid in item_ids:
            resp = responses.get(iid)
            if resp is None or resp.status != 200:
                out[iid] = (None, 0, None)
                continue
            body = resp.body or {}
            du = body.get("@microsoft.graph.downloadUrl")
            size = int(body.get("size", 0) or 0)
            qxh = (
                (body.get("file") or {})
                .get("hashes", {})
                .get("quickXorHash")
            )
            out[iid] = (du, size, qxh)
        return out

    async def get_download_url(self, drive_id: str, item_id: str,
                               max_attempts: int = 3) -> Tuple[str, int, Optional[str]]:
        """
        Reliably obtain a fresh @microsoft.graph.downloadUrl for a drive item.

        Why this is non-trivial:
          - Delta responses don't always include @microsoft.graph.downloadUrl.
          - When you $select it explicitly, Graph computes it on-the-fly.
          - The URL is short-lived (~1 hour) and must be used promptly.
          - For files just modified, the URL may briefly 404; retry helps.
          - Some file types (whiteboards, notebooks, packages) have NO download URL
            even though they have a 'file' facet — these are cloud-native objects.

        Returns: (download_url, size_bytes, quick_xor_hash_or_none)
        Raises: RuntimeError if no URL can be obtained after retries.
        Raises: RuntimeError("no_download_url") if item is not downloadable at all.
        """
        # For app-only access, do NOT use $select - Graph ignores it or strips downloadUrl
        # for application permission tokens. Get the full item and extract what we need.
        url = f"{self.GRAPH_URL}/drives/{drive_id}/items/{item_id}"
        last_error = None
        for attempt in range(max_attempts):
            try:
                item = await self._get(url)
                download_url = item.get("@microsoft.graph.downloadUrl")
                size = item.get("size", 0)
                qxh = (item.get("file") or {}).get("hashes", {}).get("quickXorHash")
                file_facet = item.get("file")
                if download_url:
                    return download_url, size, qxh
                # DEBUG: log what Graph returned
                keys = list(item.keys()) if isinstance(item, dict) else "not a dict"
                print(f"[GraphClient] get_download_url attempt {attempt+1}/{max_attempts} for "
                      f"item {item_id}: download_url={'present' if download_url else 'MISSING'}, "
                      f"file_facet={'yes' if file_facet else 'no'}, "
                      f"size={size}, keys={keys}")
                if not file_facet:
                    raise RuntimeError(f"Item {item_id} has no 'file' facet — not downloadable")
                last_error = "downloadUrl missing despite $select; retrying"
            except Exception as e:
                last_error = str(e)
                if attempt == 0:
                    print(f"[GraphClient] get_download_url attempt {attempt+1}/{max_attempts} for "
                          f"item {item_id}: EXCEPTION: {e}")
            if attempt < max_attempts - 1:
                await asyncio.sleep(2 ** attempt)
        # Final attempt failed — check if this item type is fundamentally non-downloadable
        raise RuntimeError(f"Could not obtain downloadUrl for item {item_id}: {last_error}")

    async def get_group_mailbox_messages(self, group_id: str, delta_token: str = None) -> Dict[str, Any]:
        """
        Get group mailbox messages using delta API.
        Graph API: GET /groups/{id}/messages/delta
        """
        url = f"{self.GRAPH_URL}/groups/{group_id}/messages/delta"
        if delta_token:
            url = delta_token

        params = {"$top": "999"}
        result = await self._get(url, params=params)
        all_value = result.get("value", [])

        # Follow pagination
        while "@odata.nextLink" in result:
            next_url = result["@odata.nextLink"]
            result = await self._get(next_url)
            all_value.extend(result.get("value", []))

        result["value"] = all_value
        return result

    async def get_group_threads(self, group_id: str) -> Dict[str, Any]:
        """
        Get group conversation threads.
        Graph API: GET /groups/{id}/threads
        """
        result = await self._get(f"{self.GRAPH_URL}/groups/{group_id}/threads", params={"$top": "999"})
        all_value = result.get("value", [])
        while "@odata.nextLink" in result:
            result = await self._get(result["@odata.nextLink"])
            all_value.extend(result.get("value", []))
        result["value"] = all_value
        return result

    async def get_group_thread_posts(self, group_id: str, thread_id: str) -> Dict[str, Any]:
        """
        Get posts for a specific group thread.
        Graph API: GET /groups/{id}/threads/{thread-id}/posts
        """
        result = await self._get(
            f"{self.GRAPH_URL}/groups/{group_id}/threads/{thread_id}/posts",
            params={"$top": "999"},
        )
        all_value = result.get("value", [])
        while "@odata.nextLink" in result:
            result = await self._get(result["@odata.nextLink"])
            all_value.extend(result.get("value", []))
        result["value"] = all_value
        return result

    # GROUP_MAILBOX conversation restore — same identity-bound problem as
    # calendar / chat: the sender of a group conversation post is a
    # mailbox-owner attribute Graph won't let an app-only token
    # impersonate. On create, Graph stamps the post's `from` as the
    # calling service principal. Any `from` / `sender` field we send
    # back is ignored at best, 403'd at worst, so we strip them —
    # same afi-parity tactic as the calendar restore helper. Provenance
    # (original sender, attendees, conversation subject) is preserved in
    # body.content as a banner by the caller.
    _GROUP_POST_READONLY_FIELDS = {
        "id", "createdDateTime", "lastModifiedDateTime", "changeKey",
        "conversationId", "conversationThreadId", "receivedDateTime",
        "hasAttachments", "newParticipants",
        "@odata.etag", "@odata.context", "@odata.type",
        "from", "sender",
    }

    async def create_group_thread(
        self, group_id: str, topic: str, post_body: Dict[str, Any],
    ) -> Dict[str, Any]:
        """POST /groups/{id}/threads — start a new conversation thread.

        `post_body` should match Graph's `post` resource: at minimum
        `body: {contentType, content}`. The server mints the thread id,
        post id, createdDateTime, and sets `from` to the calling SP.

        Needs Group.ReadWrite.All (Application permission)."""
        url = f"{self.GRAPH_URL}/groups/{group_id}/threads"
        clean_post = {
            k: v for k, v in (post_body or {}).items()
            if k not in self._GROUP_POST_READONLY_FIELDS
        }
        payload = {
            "topic": topic,
            "posts": [clean_post] if clean_post else [{"body": {"contentType": "html", "content": ""}}],
        }
        return await self._post(url, payload)

    async def get_planner_tasks(self, user_id: str = None, plan_id: str = None) -> Dict[str, Any]:
        """
        Get Planner tasks.
        Graph API: GET /users/{id}/planner/tasks or /planner/plans/{id}/tasks
        """
        if plan_id:
            url = f"{self.GRAPH_URL}/planner/plans/{plan_id}/tasks"
        elif user_id:
            url = f"{self.GRAPH_URL}/users/{user_id}/planner/tasks"
        else:
            return {"value": []}

        result = await self._get(url, params={"$top": "999"})
        all_value = result.get("value", [])

        # Follow pagination
        while "@odata.nextLink" in result:
            next_url = result["@odata.nextLink"]
            result = await self._get(next_url)
            all_value.extend(result.get("value", []))

        result["value"] = all_value
        return result

    async def get_power_bi_workspaces(self) -> Dict[str, Any]:
        """
        Get Power BI workspaces via Power BI REST API.
        """
        power_bi_client = PowerBIClient(
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
            refresh_token=self.power_bi_refresh_token,
        )
        workspaces = await power_bi_client.list_workspaces()
        self.power_bi_refresh_token = power_bi_client.refresh_token
        return {"value": workspaces}

    async def get_onenote_notebooks(self, user_id: str) -> Dict[str, Any]:
        """
        Get user's OneNote notebooks.
        Graph API: GET /users/{id}/onenote/notebooks
        Permission: Notes.Read.All
        """
        return await self._get(f"{self.GRAPH_URL}/users/{user_id}/onenote/notebooks", params={"$top": "999"})

    async def get_onenote_sections(self, user_id: str, notebook_id: str) -> Dict[str, Any]:
        """
        Get sections in a OneNote notebook.
        Graph API: GET /users/{id}/onenote/notebooks/{nb-id}/sections
        """
        return await self._get(
            f"{self.GRAPH_URL}/users/{user_id}/onenote/notebooks/{notebook_id}/sections",
            params={"$top": "999"}
        )

    async def get_onenote_pages(self, user_id: str, section_id: str) -> Dict[str, Any]:
        """
        Get pages in a OneNote section.
        Graph API: GET /users/{id}/onenote/sections/{section-id}/pages
        """
        return await self._get(
            f"{self.GRAPH_URL}/users/{user_id}/onenote/sections/{section_id}/pages",
            params={"$top": "999"}
        )

    async def get_user_todo_lists(self, user_id: str) -> Dict[str, Any]:
        """
        Get user's To Do task lists.
        Graph API: GET /users/{id}/todo/lists
        Permission: Tasks.Read.All
        """
        return await self._get(f"{self.GRAPH_URL}/users/{user_id}/todo/lists", params={"$top": "999"})

    async def get_user_todo_tasks(self, user_id: str, list_id: str) -> Dict[str, Any]:
        """
        Get tasks in a To Do list.
        Graph API: GET /users/{id}/todo/lists/{list-id}/tasks
        """
        return await self._get(
            f"{self.GRAPH_URL}/users/{user_id}/todo/lists/{list_id}/tasks",
            params={"$top": "999"}
        )

    async def get_planner_plans_for_group(self, group_id: str) -> Dict[str, Any]:
        """
        Get Planner plans for a group/team.
        Graph API: GET /groups/{id}/planner/plans
        Permission: Tasks.Read.All
        """
        return await self._get(f"{self.GRAPH_URL}/groups/{group_id}/planner/plans", params={"$top": "999"})

    async def get_planner_task_details(self, task_id: str) -> Dict[str, Any]:
        """Task details: description, checklist, references, previewType.
        Graph API: GET /planner/tasks/{task-id}/details
        Permission: Tasks.Read.All"""
        return await self._get(f"{self.GRAPH_URL}/planner/tasks/{task_id}/details")

    async def get_user_todo_task_checklist(self, user_id: str, list_id: str, task_id: str) -> Dict[str, Any]:
        """Checklist items nested under a To Do task.
        Graph API: GET /users/{id}/todo/lists/{list-id}/tasks/{task-id}/checklistItems"""
        return await self._get(
            f"{self.GRAPH_URL}/users/{user_id}/todo/lists/{list_id}/tasks/{task_id}/checklistItems",
            params={"$top": "999"},
        )

    async def get_user_todo_task_linked_resources(self, user_id: str, list_id: str, task_id: str) -> Dict[str, Any]:
        """Linked resources (attached URLs / apps) on a To Do task.
        Graph API: GET /users/{id}/todo/lists/{list-id}/tasks/{task-id}/linkedResources"""
        return await self._get(
            f"{self.GRAPH_URL}/users/{user_id}/todo/lists/{list_id}/tasks/{task_id}/linkedResources",
            params={"$top": "999"},
        )

    async def _get_bytes(self, url: str) -> bytes:
        """Authenticated GET that returns the raw response body as bytes — for non-JSON
        endpoints like OneNote page content (text/html) or resource $value (binary).
        Follows 302 redirects implicitly via httpx."""
        from shared.graph_rate_limiter import graph_rate_limiter
        token = await self._get_token()
        async with self._http_session() as client:
            await graph_rate_limiter.acquire(reason="graph_get_bytes")
            resp = await client.get(url, headers={"Authorization": f"Bearer {token}"})
            resp.raise_for_status()
            return resp.content

    async def get_onenote_page_content(self, user_id: str, page_id: str) -> bytes:
        """Get the HTML body of a OneNote page (returns bytes — the endpoint emits text/html).
        Graph API: GET /users/{id}/onenote/pages/{page-id}/content?includeinkML=true"""
        url = f"{self.GRAPH_URL}/users/{user_id}/onenote/pages/{page_id}/content?includeinkML=true"
        return await self._get_bytes(url)

    async def get_onenote_resource(self, url: str) -> bytes:
        """Fetch a OneNote resource (image or attachment) by its fully-qualified Graph URL.
        URL is taken verbatim from the page HTML's data-fullres-src or src attribute."""
        return await self._get_bytes(url)

    async def _paginated_get(self, path: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """Helper for paginated GET requests"""
        return await self._get(f"{self.GRAPH_URL}{path}", params=params)
