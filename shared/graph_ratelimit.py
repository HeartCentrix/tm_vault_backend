"""Graph API rate-limit policy — central retry / backoff / pacing brain.

One `RateLimitPolicy` instance per GraphClient. Owns the decision of how
long to sleep after a 429/503 and when to give up and raise so the
RabbitMQ redeliver path can resume.

Spec: docs/superpowers/specs/2026-04-19-graph-api-throttle-hardening-design.md
"""
from __future__ import annotations

import asyncio
import email.utils
import math
from collections import deque
import random
import time
from dataclasses import dataclass, field
from typing import List, Optional


def retry_wait(status: Optional[int], retry_after: Optional[float], attempt: int) -> float:
    """Throttling (429, or 503 carrying Retry-After) honours the server and backs off.
    A Microsoft server error is not load: retry after a short fixed wait, never above 10 s."""
    if status == 429 or (status == 503 and retry_after is not None):
        # Never faster than the backoff floor: Teams sends Retry-After 0 and throttles again.
        return float(max(retry_after or 0, 2 ** min(attempt, 5)))
    return min(float(retry_after if retry_after is not None else 2), 10.0)


def parse_retry_after(header_value: Optional[str]) -> Optional[float]:
    """Parse a Retry-After header value (seconds-int OR HTTP-date).

    Returns seconds-to-wait as float, or None if the value is missing /
    unparseable. Microsoft Graph always sends seconds today, but the HTTP
    spec allows dates — we handle both so a future format change doesn't
    silently crash the worker.
    """
    if header_value is None:
        return None
    raw = header_value.strip()
    if not raw:
        return None
    try:
        return float(int(raw))
    except ValueError:
        pass
    # Try HTTP-date.
    try:
        dt = email.utils.parsedate_to_datetime(raw)
        if dt is None:
            return None
        delta = dt.timestamp() - time.time()
        return max(delta, 0.0)
    except (TypeError, ValueError):
        return None


def jittered(base_seconds: float, ratio: float) -> float:
    """Apply uniform ±ratio jitter.

    Result is `base * uniform(1 - ratio, 1 + ratio)`. Caps at 0 so we never
    return negative sleep.
    """
    if ratio <= 0:
        return base_seconds
    factor = 1.0 + random.uniform(-ratio, ratio)
    return max(base_seconds * factor, 0.0)


@dataclass
class BackoffWalker:
    """Walks a configured sleep-sequence, tracks cumulative wait, loops on exhaustion.

    Used when 429/503 arrives without a Retry-After header. Each call to
    `.next()` returns the next jittered sleep duration and records it so
    callers can check `.exceeded_cumulative_cap()` before sleeping again.
    """

    sequence: List[int]
    jitter_ratio: float = 0.2
    _index: int = field(default=0, init=False)
    _cumulative: float = field(default=0.0, init=False)

    def next(self) -> float:
        if not self.sequence:
            return 0.0
        base = float(self.sequence[self._index % len(self.sequence)])
        self._index += 1
        wait = jittered(base, self.jitter_ratio)
        self._cumulative += wait
        return wait

    def cumulative_wait(self) -> float:
        return self._cumulative

    def exceeded_cumulative_cap(self, cap_seconds: float) -> bool:
        return self._cumulative >= cap_seconds

    def reset(self) -> None:
        self._index = 0
        self._cumulative = 0.0


class AsyncTokenBucket:
    """Token bucket pacing, asyncio-safe, priority-aware.

    Tokens accrue at `rate_per_sec`. `acquire()` blocks until one is
    available. `capacity` allows short bursts up to that many tokens.

    rate_per_sec=0 disables pacing — `acquire()` always returns
    immediately. This is the degraded-mode fallback so turning a pace
    knob to 0 in env disables it without code changes.

    Waiting priorities receive positive weighted round-robin shares (1..8).
    Higher priority receives more turns; sustained urgent traffic cannot
    starve a normal waiter. Within each priority, arrivals remain FIFO.
    """

    def __init__(self, rate_per_sec: float, capacity: int = 1):
        self._rate = max(rate_per_sec, 0.0)
        self._capacity = max(capacity, 1)
        self._tokens = float(self._capacity)
        self._last_refill = time.monotonic()
        # Condition wraps an internal Lock; all state under _cond.
        self._cond = asyncio.Condition()
        self._waiting = {}
        self._turns = tuple(p for p in range(7, -1, -1) for _ in range(p + 1))
        self._turn = 0

    def _next_waiter(self):
        for offset in range(len(self._turns)):
            turn = (self._turn + offset) % len(self._turns)
            queue = self._waiting.get(self._turns[turn])
            if queue:
                return queue[0], turn
        return None, self._turn

    def _refill_tokens(self) -> None:
        """Advance the token count based on monotonic time elapsed.
        Caller MUST hold the condition lock."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(
            self._capacity, self._tokens + elapsed * self._rate
        )
        self._last_refill = now

    async def acquire(self, cost: float = 1.0, priority: int = 0) -> None:
        """Wait for a real token and a fair turn; cancellation leaves no waiter."""
        if not math.isfinite(cost) or cost <= 0 or cost > self._capacity:
            raise ValueError("pacing cost must fit capacity")
        if self._rate <= 0:
            return
        priority = min(7, max(0, int(priority)))
        ticket = object()
        async with self._cond:
            queue = self._waiting.setdefault(priority, deque())
            queue.append(ticket)
            try:
                while True:
                    self._refill_tokens()
                    selected, turn = self._next_waiter()
                    if selected is ticket and self._tokens >= cost:
                        self._tokens -= cost
                        self._turn = (turn + 1) % len(self._turns)
                        return
                    wait = max((cost - self._tokens) / self._rate, 0.01)
                    try:
                        await asyncio.wait_for(self._cond.wait(), timeout=wait)
                    except asyncio.TimeoutError:
                        pass
            finally:
                queue.remove(ticket)
                if not queue:
                    del self._waiting[priority]
                self._cond.notify_all()

    def rate(self) -> float:
        return self._rate


@dataclass
class PolicyAction:
    """Decision returned by RateLimitPolicy for one response."""
    should_sleep: bool
    sleep_seconds: float
    exhausted: bool
    reason: str


class RateLimitPolicy:
    """Per-stream retry + pacing state machine.

    Call `.decide(status_code, retry_after)` after every Graph response.
    Returns a `PolicyAction` telling the caller whether to sleep, for
    how long, or to raise because the cumulative cap is exhausted.

    Pacing is enforced separately via the bucket properties. Callers are
    expected to `await policy.stream_bucket.acquire()` and
    `await policy.app_bucket.acquire()` BEFORE every request.
    """

    def __init__(
        self,
        *,
        stream_rate: float,
        app_rate: float,
        throttle_sequence: List[int],
        transient_sequence: List[int],
        jitter_ratio: float,
        cumulative_cap_s: float,
        stream_capacity: int = 1,
        app_capacity: int = 1,
    ):
        self.stream_bucket = AsyncTokenBucket(stream_rate, stream_capacity)
        self.app_bucket = AsyncTokenBucket(app_rate, app_capacity)
        self._throttle_walker = BackoffWalker(throttle_sequence, jitter_ratio)
        self._transient_walker = BackoffWalker(transient_sequence, jitter_ratio)
        self._cumulative_cap = cumulative_cap_s

    def for_operation(self):
        """Share pacing buckets, but never retry counters between operations."""
        import copy
        operation = copy.copy(self)
        operation._throttle_walker = BackoffWalker(
            list(self._throttle_walker.sequence), self._throttle_walker.jitter_ratio)
        operation._transient_walker = BackoffWalker(
            list(self._transient_walker.sequence), self._transient_walker.jitter_ratio)
        return operation

    def decide(
        self, *, status_code: int, retry_after: Optional[str]
    ) -> PolicyAction:
        if status_code in (429, 503):
            parsed = parse_retry_after(retry_after)
            if parsed is not None:
                # Trust the server. Still count it toward the cumulative cap.
                self._throttle_walker._cumulative += parsed
                if self._throttle_walker.exceeded_cumulative_cap(self._cumulative_cap):
                    return PolicyAction(
                        should_sleep=False, sleep_seconds=0.0,
                        exhausted=True,
                        reason=f"cumulative-cap-hit-after-retry-after-{parsed}s",
                    )
                return PolicyAction(
                    should_sleep=True, sleep_seconds=parsed,
                    exhausted=False, reason=f"retry-after-{parsed}s",
                )
            wait = self._throttle_walker.next()
            if self._throttle_walker.exceeded_cumulative_cap(self._cumulative_cap):
                return PolicyAction(
                    should_sleep=False, sleep_seconds=0.0,
                    exhausted=True, reason=f"cumulative-cap-hit-at-{wait:.0f}s",
                )
            return PolicyAction(
                should_sleep=True, sleep_seconds=wait,
                exhausted=False, reason=f"backoff-{wait:.0f}s",
            )
        return PolicyAction(
            should_sleep=False, sleep_seconds=0.0,
            exhausted=False, reason=f"no-sleep-for-{status_code}",
        )

    def decide_transient_error(self) -> PolicyAction:
        wait = self._transient_walker.next()
        if self._transient_walker.exceeded_cumulative_cap(self._cumulative_cap):
            return PolicyAction(
                should_sleep=False, sleep_seconds=0.0,
                exhausted=True, reason="cumulative-cap-hit-transient",
            )
        return PolicyAction(
            should_sleep=True, sleep_seconds=wait,
            exhausted=False, reason=f"transient-backoff-{wait:.0f}s",
        )

    def reset_on_success(self) -> None:
        """Reset walkers after a clean response. Cumulative cap stays in place."""
        self._throttle_walker._index = 0
        self._transient_walker._index = 0


class GraphRetryExhaustedError(Exception):
    """Cumulative-wait cap hit on a stream. Caller should raise, let
    RabbitMQ redeliver, and the next worker resumes from checkpoint."""
