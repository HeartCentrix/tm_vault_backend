"""
Azure API Rate Limiter

Azure subscription limits:
- 1,200 write operations per 5-minute window per subscription
- 12,000 read operations per 5-minute window per subscription
- High-cost operations (snapshots, VMs) share a sub-pool (~100 per 3 min)

For 1000 VMs backing up simultaneously, we must:
1. Queue operations to stay within limits
2. Use longer polling intervals for LROs
3. Cache repeated calls (container exists, VM config)
4. Parallelize only where safe (disk copies after grant)

Counters live in Redis so all worker processes / Kubernetes replicas share one
budget. With N processes each tracking their own in-process counts, every worker
believes it has the full quota and the cluster blows past Azure's limits — the
incident this module was rewritten to prevent. The check-and-add is performed by
a Lua script so the read-then-write is atomic across the cluster.
"""
import asyncio
import logging
import os
import time
import uuid
from collections import deque
from typing import Deque, Dict, Optional

try:
    from redis.asyncio import Redis
except ModuleNotFoundError:  # pragma: no cover - redis is required at deploy time
    Redis = None  # type: ignore[assignment]

from shared.config import settings

logger = logging.getLogger("azure.rate_limiter")


# Atomic sliding-window check-and-add.
#
# KEYS[1] = sorted-set key (one per limit-type + subscription_id)
# ARGV[1] = now (unix seconds, float)
# ARGV[2] = cutoff (now - window_seconds)
# ARGV[3] = limit (max entries allowed in the window)
# ARGV[4] = member (unique value, prevents ZADD collisions)
# ARGV[5] = key TTL in seconds (window_seconds + slack)
#
# Returns -1 if acquired (added), or the current count when rejected.
_ACQUIRE_LUA = """
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[2])
local count = redis.call('ZCARD', KEYS[1])
if count >= tonumber(ARGV[3]) then
    return count
end
redis.call('ZADD', KEYS[1], ARGV[1], ARGV[4])
redis.call('EXPIRE', KEYS[1], ARGV[5])
return -1
"""


class AzureApiRateLimiter:
    """
    Cluster-wide rate limiter for Azure API calls.

    Tracks API call counts per subscription in Redis and throttles when
    approaching Azure's limits. All worker processes share one counter set, so
    the budget is enforced across replicas (not per-process).

    Limits (per subscription):
    - Write operations: 1,200 / 5 min (we use 800 for safety margin)
    - Read operations: 12,000 / 5 min (we use 8,000 for safety margin)
    - High-cost (snapshots/VMs): 100 / 3 min (we use 60 for safety margin)
    """

    # Limits (with ~33% safety margin against Azure's published ceilings)
    MAX_WRITES_PER_5MIN = 800
    MAX_READS_PER_5MIN = 8000
    MAX_HIGH_COST_PER_3MIN = 60

    # Window seconds
    WRITE_WINDOW = 300
    READ_WINDOW = 300
    HIGH_COST_WINDOW = 180

    # Sleep cadence when waiting for the window to roll
    WRITE_BACKOFF = 5
    READ_BACKOFF = 2
    HIGH_COST_BACKOFF = 10

    def __init__(self):
        self._redis: Optional[Redis] = None
        self._redis_init_lock = asyncio.Lock()
        self._redis_warned = False

        # Per-process safety net for high-cost concurrency. The cluster-wide
        # rate is enforced via Redis; this caps how many in-flight acquire
        # calls a single process can hold open at once.
        self._high_cost_semaphore = asyncio.Semaphore(5)

        # In-process fallback counters (used only when Redis is disabled —
        # e.g. local single-process dev). NOT safe across processes.
        # `deque` instead of `list` so cleanup is amortized O(1) per acquire
        # — popleft() drops one expired entry without realloc — versus the
        # O(n) list comprehension the previous implementation ran on every
        # acquire (which built GC pressure and stalled under sustained load).
        self._fallback_windows: Dict[str, Deque[float]] = {}
        # B-M1: track last-write per key so we can evict stale keys whose
        # subscription stopped calling. Without this the dict grows
        # unboundedly — one entry per unique subscription_id × limit-type
        # ever observed — and bleeds memory in long-running workers.
        self._fallback_last_active: Dict[str, float] = {}
        self._fallback_acquire_count = 0
        # Sweep stale keys every N successful in-process acquires. The
        # work per sweep is O(len(dict)); N=128 amortises that to ~1%
        # overhead even if every acquire used the fallback path.
        self._fallback_evict_every = 128
        self._fallback_lock = asyncio.Lock()

    @staticmethod
    def _is_production() -> bool:
        """True when the worker is running in a non-dev environment.

        Checks ENVIRONMENT (canonical) and RAILWAY_ENVIRONMENT_NAME (the
        var Railway sets automatically on the production environment).
        Both must explicitly say so — absence is treated as dev.
        """
        env = os.environ.get("ENVIRONMENT", "").strip().lower()
        if env in ("production", "prod"):
            return True
        railway_env = os.environ.get("RAILWAY_ENVIRONMENT_NAME", "").strip().lower()
        return railway_env == "production"

    async def _get_redis(self) -> Optional[Redis]:
        """Lazy-init the Redis client. Returns None when Redis isn't configured."""
        if self._redis is not None:
            return self._redis
        if Redis is None or not settings.REDIS_ENABLED:
            # B-M1: in production the in-process fallback would cause
            # every replica to track its own counters, which collectively
            # blast through Azure's per-subscription API ceiling and
            # trigger subscription-wide throttling. Fail fast instead.
            if self._is_production():
                raise RuntimeError(
                    "AzureApiRateLimiter requires Redis in production "
                    "(REDIS_ENABLED=true, reachable Redis). The in-process "
                    "fallback is NOT safe across multiple worker replicas "
                    "and will exceed Azure's per-subscription API limits. "
                    "Set ENVIRONMENT=dev / RAILWAY_ENVIRONMENT_NAME=dev "
                    "for explicit local-development opt-out."
                )
            if not self._redis_warned:
                logger.warning(
                    "[RateLimiter] Redis disabled — falling back to in-process "
                    "counters. This is NOT safe for multi-process / multi-replica "
                    "deployments and WILL exceed Azure's per-subscription limits."
                )
                self._redis_warned = True
            return None
        async with self._redis_init_lock:
            if self._redis is None:
                self._redis = Redis(
                    host=settings.REDIS_HOST,
                    port=settings.REDIS_PORT,
                    db=settings.REDIS_DB,
                    decode_responses=True,
                )
        return self._redis

    @staticmethod
    def _key(prefix: str, subscription_id: str) -> str:
        return f"azure_rl:{prefix}:{subscription_id}"

    async def _try_acquire(
        self, key: str, window_seconds: int, limit: int
    ) -> int:
        """Atomic check-and-add. Returns -1 on success, or current count when full.

        Falls back to in-process tracking when Redis is unavailable.
        """
        client = await self._get_redis()
        now = time.time()
        cutoff = now - window_seconds
        member = f"{now}:{uuid.uuid4().hex}"
        ttl = window_seconds + 10

        if client is not None:
            try:
                result = await client.eval(
                    _ACQUIRE_LUA, 1, key, now, cutoff, limit, member, ttl
                )
                return int(result)
            except Exception as exc:
                # Redis unreachable / script error — fail closed-ish: log loudly
                # and fall through to the in-process counter for this call so
                # we don't take down all backups when Redis flaps. Operators
                # should alert on this log line.
                logger.error(
                    "[RateLimiter] Redis eval failed (%s); falling back to "
                    "in-process counter for this call. Multi-process budget is "
                    "NOT enforced while Redis is unavailable.",
                    exc,
                )

        # In-process fallback — pure-Python sliding window. Sorted by
        # insertion order (timestamps grow monotonically per key under the
        # lock), so expired entries are always at the front; popleft drops
        # them in O(1) each instead of rebuilding the whole list. Each
        # entry is popped at most once, so amortized cost per acquire is
        # O(1). Deque size is bounded by `limit` because we reject the add
        # when count >= limit, so memory per key is O(limit) not unbounded.
        async with self._fallback_lock:
            entries = self._fallback_windows.setdefault(key, deque())
            while entries and entries[0] <= cutoff:
                entries.popleft()
            if len(entries) >= limit:
                # Rejected → still record that the key saw activity so
                # the sweep doesn't evict it under contention.
                self._fallback_last_active[key] = now
                return len(entries)
            entries.append(now)
            self._fallback_last_active[key] = now

            # B-M1: opportunistic eviction. Every N acquires, drop keys
            # whose deques are empty AND haven't been touched in two
            # full windows. Bounds memory at O(active subscriptions ×
            # limit_types) instead of unbounded growth across the
            # worker's lifetime.
            self._fallback_acquire_count += 1
            if self._fallback_acquire_count % self._fallback_evict_every == 0:
                stale_cutoff = now - 2 * window_seconds
                stale_keys = [
                    k for k, ts in self._fallback_last_active.items()
                    if ts < stale_cutoff and not self._fallback_windows.get(k)
                ]
                for k in stale_keys:
                    self._fallback_windows.pop(k, None)
                    self._fallback_last_active.pop(k, None)
            return -1

    async def _acquire(
        self,
        prefix: str,
        subscription_id: str,
        window: int,
        limit: int,
        backoff: int,
        label: str,
    ) -> None:
        key = self._key(prefix, subscription_id)
        while True:
            result = await self._try_acquire(key, window, limit)
            if result == -1:
                return
            logger.warning(
                "[RateLimiter] %s limit reached for %s (%d/%ds), waiting %ds",
                label,
                subscription_id[:8],
                result,
                window,
                backoff,
            )
            await asyncio.sleep(backoff)

    async def acquire_write(self, subscription_id: str) -> None:
        """Acquire permission for a write API call."""
        await self._acquire(
            "write",
            subscription_id,
            self.WRITE_WINDOW,
            self.MAX_WRITES_PER_5MIN,
            self.WRITE_BACKOFF,
            "Write",
        )

    async def acquire_read(self, subscription_id: str) -> None:
        """Acquire permission for a read API call."""
        await self._acquire(
            "read",
            subscription_id,
            self.READ_WINDOW,
            self.MAX_READS_PER_5MIN,
            self.READ_BACKOFF,
            "Read",
        )

    async def acquire_high_cost(self, subscription_id: str) -> None:
        """Acquire permission for a high-cost operation (snapshot, VM create/delete).

        Uses a per-process concurrency semaphore *and* the cluster-wide rate
        budget. The semaphore caps in-flight acquire calls per worker; the
        rate budget caps the cluster-wide call rate over the 3-min window.
        """
        await self._high_cost_semaphore.acquire()
        try:
            await self._acquire(
                "high_cost",
                subscription_id,
                self.HIGH_COST_WINDOW,
                self.MAX_HIGH_COST_PER_3MIN,
                self.HIGH_COST_BACKOFF,
                "High-cost",
            )
        finally:
            self._high_cost_semaphore.release()

    async def _count(self, key: str, window_seconds: int) -> int:
        """Best-effort current count for status reporting. Doesn't mutate."""
        client = await self._get_redis()
        cutoff = time.time() - window_seconds
        if client is not None:
            try:
                await client.zremrangebyscore(key, "-inf", cutoff)
                return int(await client.zcard(key))
            except Exception as exc:
                logger.warning("[RateLimiter] Redis status read failed: %s", exc)
        async with self._fallback_lock:
            entries = self._fallback_windows.get(key)
            if entries is None:
                return 0
            while entries and entries[0] <= cutoff:
                entries.popleft()
            return len(entries)

    async def get_status(self, subscription_id: str) -> Dict[str, int]:
        """Get current API usage stats for a subscription.

        Async because the counts live in Redis. Callers must await.
        """
        return {
            "writes_5min": await self._count(
                self._key("write", subscription_id), self.WRITE_WINDOW
            ),
            "reads_5min": await self._count(
                self._key("read", subscription_id), self.READ_WINDOW
            ),
            "high_cost_3min": await self._count(
                self._key("high_cost", subscription_id), self.HIGH_COST_WINDOW
            ),
            "write_limit": self.MAX_WRITES_PER_5MIN,
            "read_limit": self.MAX_READS_PER_5MIN,
            "high_cost_limit": self.MAX_HIGH_COST_PER_3MIN,
        }


rate_limiter = AzureApiRateLimiter()
