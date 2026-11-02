"""Configured Graph admission with optional cross-process Redis coordination.

Limits are deployment policy, not a universal Microsoft quota. Local mode is
appropriate only when its independently configured process allowance is safe.
Configured Redis admission fails closed during coordinator outages.
"""
from __future__ import annotations

import asyncio
import logging
import math
import os
import time
from contextlib import asynccontextmanager
from typing import Optional

from shared.provider_read import ProviderReadError

logger = logging.getLogger(__name__)

def _validate_cost(cost, capacity):
    if not math.isfinite(cost) or cost <= 0 or cost > capacity:
        raise ValueError("admission cost must be positive, finite and no larger than capacity")


class _LocalTokenBucket:
    """Pure-Python async-safe token bucket.

    Refills continuously at ``rate`` tokens/sec up to ``capacity``.
    ``acquire`` blocks until a token is available and returns the
    wait duration (so callers can record latency).
    """

    def __init__(self, rate: float, capacity: float) -> None:
        if not math.isfinite(rate) or rate <= 0 or not math.isfinite(capacity) or capacity <= 0:
            raise ValueError("admission rate and capacity must be positive and finite")
        self.rate = float(rate)
        self.capacity = float(capacity)
        self._tokens = float(capacity)
        self._last = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, n: float = 1.0) -> float:
        _validate_cost(n, self.capacity)
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

    Uses a server-clock Lua grant. Denied callers must redraw after sleeping;
    no future capacity is reserved, so cancellation leaves no reservation.
    """

    # Atomic refill+take. Returns wait-seconds (0 if immediate) and
    # new token count.
    _LUA = """
    local clock = redis.call('TIME')
    local now = tonumber(clock[1]) + tonumber(clock[2]) / 1000000
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
        _validate_cost(n, self.capacity)
        waited = 0.0
        while True:
            try:
                if self._script is None:
                    self._script = self._redis.register_script(self._LUA)
                wait_s, _tokens = await self._script(
                    keys=[self._key],
                    args=["0", str(self.rate), str(self.capacity), str(n)],
                )
                wait = float(wait_s)
                if not math.isfinite(wait) or wait < 0:
                    raise ValueError("invalid coordinator delay")
            except Exception as exc:
                logger.warning("Graph admission coordinator unavailable: %s", type(exc).__name__)
                raise ProviderReadError("coordinator_unavailable", retry_after=5) from None
            if wait == 0:
                return waited
            await asyncio.sleep(wait)
            waited += wait


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

    @property
    def scoped_enabled(self):
        return os.environ.get("PROVIDER_SCOPED_ADMISSION_ENABLED", "false").lower() == "true"

    async def _scoped_coordinator(self, context):
        from shared.provider_admission import AdmissionLimits, ProviderDeferred, ScopedProviderAdmission
        existing = getattr(self, "_scoped", None)
        if existing is not None:
            return existing
        try:
            await self.maybe_init_redis()
        except ProviderReadError:
            raise ProviderDeferred(context, 5, stage="coordinator_unavailable") from None
        if not isinstance(self._bucket, _RedisTokenBucket):
            raise ProviderDeferred(context, 5, stage="coordinator_unconfigured")
        self._scoped = ScopedProviderAdmission(self._bucket._redis,
            limits=AdmissionLimits(rate=self._rate, burst=self._burst,
                global_concurrency=int(os.environ.get("PROVIDER_GLOBAL_CONCURRENCY", "16")),
                tenant_concurrency=int(os.environ.get("PROVIDER_TENANT_CONCURRENCY", "8")),
                resource_concurrency=int(os.environ.get("PROVIDER_RESOURCE_CONCURRENCY", "4"))),
            namespace=os.environ.get("PROVIDER_ADMISSION_NAMESPACE", "tmvault"))
        return self._scoped

    @asynccontextmanager
    async def admit(self, context, *, cost=1, wait_budget=1):
        if not self.scoped_enabled:
            yield
            return
        coordinator = await self._scoped_coordinator(context)
        async with coordinator.admit(context, cost=cost, wait_budget=wait_budget):
            yield

    async def cooldown(self, context, seconds):
        if self.scoped_enabled:
            coordinator = await self._scoped_coordinator(context)
            return await coordinator.cooldown(context, seconds)

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
            client = Redis.from_url(url, decode_responses=True,
                                    socket_connect_timeout=2, socket_timeout=2)
            self._bucket = _RedisTokenBucket(
                client, "graph:bucket:global", self._rate, self._burst, fallback=self._local,
            )
            await client.ping()
        except Exception as exc:
            logger.warning("Graph admission coordinator unavailable: %s", type(exc).__name__)
            raise ProviderReadError("coordinator_unavailable", retry_after=5) from None
        # Log host:port only — REDIS_URL can carry the auth password in URL
        # form, and worker pods now route shared.* INFO to stdout.
        from urllib.parse import urlparse
        _loc = urlparse(url)
        logger.info(
            "[graph_rate_limiter] Redis-coordinated bucket active at %s:%s",
            _loc.hostname, _loc.port,
        )

    async def acquire(self, n: float = 1.0, reason: str = "graph_call") -> float:
        """Block until ``n`` tokens are available. Returns wait-seconds.

        ``reason`` is a free-form label used purely for metrics so we
        can tell which call sites are hot."""
        if not self._enabled:
            return 0.0
        await self.maybe_init_redis()
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
