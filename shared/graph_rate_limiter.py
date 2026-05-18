"""Process-global token-bucket rate limiter for Microsoft Graph calls.

Sits *above* the per-app throttle in :mod:`shared.multi_app_manager`.

Why this exists
---------------
Microsoft enforces several limits on Graph API traffic, layered:

1. **Per-app** — each application registration has its own quota
   (managed inside ``multi_app_manager``).
2. **Per-tenant** — across all apps belonging to a tenant. As of
   Mar 2026 the documented cap is roughly **150,000 requests per
   10-minute sliding window** (~250 req/s). All 12 of our app
   registrations share this pool. Per-app rotation does NOT lift it.
3. **Per-mailbox** — at most 4 concurrent calls per mailbox per app.

This module enforces (2). With single-tenant TMvault, the limiter is
one process-global (or Redis-coordinated, when ``REDIS_URL`` is set)
token bucket whose refill rate matches the Microsoft-side per-tenant
budget. Callers ``await rate_limiter.acquire()`` before each Graph
HTTP call; the call blocks until a token is available.

Design choices
--------------
- **Token bucket, not leaky bucket.** Bursts are allowed up to the
  configured bucket capacity so that short spikes from coordinator
  fan-out don't get punished — sustained rate still cannot exceed
  the refill.
- **Process-local fast path.** When ``REDIS_URL`` is unset (typical
  for tests + local dev) the limiter is per-process. With multiple
  workers in prod, each worker holds its own share — divide the
  per-tenant budget across worker replicas via
  ``GRAPH_GLOBAL_RPS / replica_count`` env on deploy.
- **Redis-coordinated mode.** When ``REDIS_URL`` is set, tokens are
  drawn from a Redis-hosted bucket using a tiny Lua script so that
  N replicas share a single budget. Failure-open: if Redis is down
  the limiter degrades to process-local (logs warning).
- **Single-tenant simplification.** Today there is exactly one tenant.
  The bucket key is ``graph:bucket:global`` so the same limiter
  works unchanged when we go multi-tenant — the key just becomes
  ``graph:bucket:{tenant_id}`` and ``acquire(tenant_id=...)`` is
  routed accordingly.
- **No-op when prometheus_client missing** — calls
  ``core_metrics.inc_graph_rate_limit_wait`` only when the metric
  is registered, so the limiter has no hard dep on observability.

Configuration (env)
-------------------
- ``GRAPH_GLOBAL_RPS`` (default 200) — sustained rate, requests/sec.
  Microsoft's documented cap is ~250 r/s per tenant; default leaves
  headroom for retries on top.
- ``GRAPH_GLOBAL_BURST`` (default 400) — max bucket capacity. Short
  bursts up to this size are allowed before the limiter kicks in.
- ``GRAPH_RATE_LIMITER_ENABLED`` (default ``true``) — kill switch.

Usage
-----
::

    from shared.graph_rate_limiter import graph_rate_limiter

    async def call_graph(client, url):
        await graph_rate_limiter.acquire()
        return await client.get(url)
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Optional

logger = logging.getLogger(__name__)


class _LocalTokenBucket:
    """Pure-Python async-safe token bucket.

    Refills continuously at ``rate`` tokens/sec up to ``capacity``.
    ``acquire`` blocks until a token is available and returns the
    wait duration (so callers can record latency).
    """

    def __init__(self, rate: float, capacity: float) -> None:
        self.rate = float(rate)
        self.capacity = float(capacity)
        self._tokens = float(capacity)
        self._last = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, n: float = 1.0) -> float:
        waited = 0.0
        # Sleep MUST be outside the lock or we serialise every Graph
        # caller in the worker — one task sleeping would block every
        # other caller from observing the bucket state. Hold the lock
        # only during the refill + decrement math, release before
        # awaiting refill time. Concurrent callers may take a token
        # out of order under burst (no strict FIFO), but the long-run
        # rate is still exactly self.rate.
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self._last
                if elapsed > 0:
                    self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
                    self._last = now
                if self._tokens >= n:
                    self._tokens -= n
                    return waited
                deficit = n - self._tokens
                sleep_s = deficit / self.rate
                waited += sleep_s
            await asyncio.sleep(sleep_s)


class _RedisTokenBucket:
    """Redis-coordinated token bucket for cross-replica sharing.

    Uses a small Lua script that atomically refills + draws tokens,
    keyed by ``key``. Failures fall back to a process-local bucket
    so a Redis outage doesn't cascade into the backup pipeline.
    """

    # Atomic refill+take. Returns wait-seconds (0 if immediate) and
    # new token count.
    _LUA = """
    local now = tonumber(ARGV[1])
    local rate = tonumber(ARGV[2])
    local cap = tonumber(ARGV[3])
    local n = tonumber(ARGV[4])
    local last = tonumber(redis.call('HGET', KEYS[1], 'last') or now)
    local tokens = tonumber(redis.call('HGET', KEYS[1], 'tokens') or cap)
    local elapsed = math.max(0, now - last)
    tokens = math.min(cap, tokens + elapsed * rate)
    local wait = 0
    if tokens < n then
      wait = (n - tokens) / rate
      tokens = 0
    else
      tokens = tokens - n
    end
    redis.call('HSET', KEYS[1], 'last', now, 'tokens', tokens)
    redis.call('EXPIRE', KEYS[1], 600)
    return {tostring(wait), tostring(tokens)}
    """

    def __init__(self, redis_client, key: str, rate: float, capacity: float, fallback: _LocalTokenBucket) -> None:
        self._redis = redis_client
        self._key = key
        self.rate = rate
        self.capacity = capacity
        self._script = None  # lazy-loaded handle
        self._fallback = fallback

    async def acquire(self, n: float = 1.0) -> float:
        if self._script is None:
            try:
                self._script = self._redis.register_script(self._LUA)
            except Exception as exc:
                logger.warning("[graph_rate_limiter] Redis register_script failed (%s) — falling back to local", exc)
                return await self._fallback.acquire(n)
        try:
            wait_s, _tokens = await self._script(
                keys=[self._key],
                args=[str(time.time()), str(self.rate), str(self.capacity), str(n)],
            )
            wait = float(wait_s)
            if wait > 0:
                await asyncio.sleep(wait)
            return wait
        except Exception as exc:
            logger.warning("[graph_rate_limiter] Redis bucket draw failed (%s) — falling back to local", exc)
            return await self._fallback.acquire(n)


class GraphRateLimiter:
    """Public facade used by Graph callers."""

    def __init__(self) -> None:
        self._enabled = (os.environ.get("GRAPH_RATE_LIMITER_ENABLED", "true").lower() == "true")
        rate = float(os.environ.get("GRAPH_GLOBAL_RPS", "200"))
        burst = float(os.environ.get("GRAPH_GLOBAL_BURST", "400"))
        self._local = _LocalTokenBucket(rate=rate, capacity=burst)
        self._bucket = self._local  # may be replaced by Redis bucket in ``maybe_init_redis``
        self._rate = rate
        self._burst = burst

    async def maybe_init_redis(self) -> None:
        """Promote to Redis-backed bucket if ``REDIS_URL`` is configured.

        Safe to call multiple times; subsequent calls are no-ops.
        Designed to be invoked from each worker entry point AFTER
        the Redis client has been imported.
        """
        if isinstance(self._bucket, _RedisTokenBucket):
            return
        url = os.environ.get("REDIS_URL")
        if not url:
            return
        try:
            from redis.asyncio import Redis
        except ImportError:
            logger.info("[graph_rate_limiter] redis package missing — staying local")
            return
        try:
            client = Redis.from_url(url, decode_responses=True)
            await client.ping()
        except Exception as exc:
            logger.warning("[graph_rate_limiter] Redis ping failed (%s) — staying local", exc)
            return
        self._bucket = _RedisTokenBucket(
            client, "graph:bucket:global", self._rate, self._burst, fallback=self._local,
        )
        logger.info("[graph_rate_limiter] Redis-coordinated bucket active at %s", url)

    async def acquire(self, n: float = 1.0, reason: str = "graph_call") -> float:
        """Block until ``n`` tokens are available. Returns wait-seconds.

        ``reason`` is a free-form label used purely for metrics so we
        can tell which call sites are hot."""
        if not self._enabled:
            return 0.0
        waited = await self._bucket.acquire(n)
        if waited > 0:
            try:
                from shared import core_metrics
                core_metrics.inc_graph_rate_limit_wait(reason, waited)
            except Exception:
                pass
        return waited

    @property
    def enabled(self) -> bool:
        return self._enabled

    @property
    def configured_rate(self) -> float:
        return self._rate


# Module-level singleton — import this from anywhere.
graph_rate_limiter = GraphRateLimiter()
