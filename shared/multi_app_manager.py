"""Multi-App Registration Manager for Microsoft Graph API
Distributes requests across multiple app registrations to avoid throttling.

"""
import time
import threading
import hashlib
from collections import deque
from typing import Dict, List, Optional
from shared.config import settings
from shared.graph_ratelimit import AsyncTokenBucket


# Circuit breaker tuning. Documented inline so an SRE can dial these
# at runtime via env without re-reading the algorithm.
#
# 429_WINDOW_S × 429_THRESHOLD: count 429s in this sliding window;
# crossing the threshold escalates the ban instead of honoring just
# Retry-After.
_WINDOW_S = float(__import__("os").getenv("GRAPH_APP_429_WINDOW_S", "60"))
# 2026-05-17 prod tuning: 3 → 5. With 20 apps the rotator absorbs
# transient 429s by migrating to a healthy app on the next attempt —
# we don't need to ban an app after just 3 hits in a 60s window, that's
# over-aggressive and wastes ~5% of our app fleet during a normal burst.
_WINDOW_429_THRESHOLD = int(__import__("os").getenv("GRAPH_APP_429_THRESHOLD", "5"))

# Escalating ban ladder (seconds). Each consecutive triggering window
# moves one step up. Resets after RECOVERY_SUCCESS_COUNT consecutive
# successes — see mark_success.
# 2026-05-17 prod tuning: 5,30,300,1800 → 10,60,300,900.
#   - First step 5s → 10s so a brief ban is meaningful (5s could be
#     consumed entirely by token-refresh).
#   - Top step 1800s → 900s so a deeply-throttled app rejoins the
#     fleet within 15min instead of half an hour — with 20 apps the
#     fleet survives one bad apple, but rejoining faster helps when
#     2-3 are simultaneously banned.
_BAN_LADDER = [
    int(x) for x in
    (__import__("os").getenv("GRAPH_APP_BAN_LADDER", "10,60,300,900")).split(",")
]
_PROBATION_OK_COUNT = int(__import__("os").getenv("GRAPH_APP_PROBATION_OK", "3"))
# 2026-05-17 prod tuning: 50 → 30. Faster reset means a recovered app
# climbs back to full rate sooner — important for the 50-user manual
# burst use case where one momentarily-banned app shouldn't sit at
# probation rate for half a minute.
_RECOVERY_SUCCESS_COUNT = int(__import__("os").getenv("GRAPH_APP_RESET_AFTER_OK", "30"))

# Adaptive rate: multiplicative decrease on 429, additive increase on
# sustained quiet. Floor prevents rate dropping to zero on persistent
# throttling — at the floor we just lean on Retry-After delays.
_RATE_DECREASE_FACTOR = float(__import__("os").getenv("GRAPH_APP_RATE_DEC", "0.5"))
_RATE_INCREASE_FACTOR = float(__import__("os").getenv("GRAPH_APP_RATE_INC", "1.5"))
# 2026-05-17 prod tuning: 30s → 15s quiet period before bumping rate
# back up. Apps recover faster from transient throttle bursts.
_RATE_RECOVERY_QUIET_S = float(__import__("os").getenv("GRAPH_APP_RATE_RECOVER_S", "15"))
_RATE_FLOOR = float(__import__("os").getenv("GRAPH_APP_RATE_FLOOR", "0.25"))


class AppRegistry:
    """Tracks usage of a single Graph app registration with adaptive
    health state. State transitions:

        HEALTHY ──429──► THROTTLED (timed ban)
        THROTTLED ──ban expires──► PROBATION (limited admission)
        PROBATION ──N successes──► HEALTHY
        PROBATION ──429──► THROTTLED (escalated ban)
    """
    def __init__(self, app: dict):
        self.index = app["index"]
        self.client_id = app["client_id"]
        self.client_secret = app["client_secret"]
        self.tenant_id = app["tenant_id"]
        self.request_count = 0
        self.last_request_time = 0.0
        self.throttled_until = 0.0
        # Per-app token bucket — aggregate rate cap per (app, tenant).
        # Gates every Graph call so no single app blows past
        # GRAPH_APP_PACE_REQS_PER_SEC regardless of how many streams
        # pile on. rate=0 disables pacing (kill switch). Adaptive
        # rate-adjust below mutates self.bucket._rate at runtime.
        self.bucket = AsyncTokenBucket(
            rate_per_sec=settings.GRAPH_APP_PACE_REQS_PER_SEC,
            capacity=1,
        )
        # Adaptive-throttling state. All written under MultiAppManager._lock.
        self._original_rate: float = float(settings.GRAPH_APP_PACE_REQS_PER_SEC)
        self._recent_429s: deque = deque(maxlen=64)
        self._consecutive_bans: int = 0
        self._probation_remaining: int = 0
        self._last_success_at: float = 0.0
        self._success_streak: int = 0
        self._last_latency_ms: float = 0.0

    @property
    def is_throttled(self) -> bool:
        return time.time() < self.throttled_until

    @property
    def in_probation(self) -> bool:
        """True if app is out of ban but hasn't yet proven N successes."""
        return self._probation_remaining > 0 and not self.is_throttled

    @property
    def is_admissible(self) -> bool:
        """Admissibility check for round-robin. Probation apps ARE
        admissible — they just get fewer slots. Throttled apps aren't."""
        return not self.is_throttled

    @property
    def load_score(self) -> float:
        """Lower = better choice for next request. Weighted by:
        - hard fail: throttled → inf
        - soft penalty: probation → +200 (deprioritized but available)
        - recent-error penalty: count of 429s in last window × 50
        - latency penalty: last response time / 100ms
        - utilization: total request count
        """
        if self.is_throttled:
            return float("inf")
        now = time.time()
        recent_429 = sum(1 for ts in self._recent_429s if now - ts < _WINDOW_S)
        score = float(self.request_count)
        score += recent_429 * 50.0
        score += min(self._last_latency_ms / 100.0, 10.0)
        if self.in_probation:
            score += 200.0
        return score

    def health(self) -> dict:
        """Inspectable snapshot for ops / dashboards."""
        now = time.time()
        return {
            "index": self.index,
            "client_id": self.client_id,
            "is_throttled": self.is_throttled,
            "throttled_until": self.throttled_until,
            "throttle_remaining_s": max(0.0, self.throttled_until - now),
            "in_probation": self.in_probation,
            "probation_remaining": self._probation_remaining,
            "consecutive_bans": self._consecutive_bans,
            "rate_per_sec": self.bucket._rate,
            "rate_original": self._original_rate,
            "recent_429s_in_window": sum(
                1 for ts in self._recent_429s if now - ts < _WINDOW_S
            ),
            "success_streak": self._success_streak,
            "last_latency_ms": self._last_latency_ms,
            "request_count": self.request_count,
        }


class MultiAppManager:
    """Manages multiple Graph app registrations with round-robin + load
    balancing + adaptive circuit breaker."""

    def __init__(self):
        self.apps: List[AppRegistry] = [
            AppRegistry(app) for app in settings.GRAPH_APPS
        ]
        # 2026-05-17 prod tuning: stagger the rotation starting index
        # across replicas so a cold-boot fleet of 12 doesn't synchronize
        # on app #1. Without this, every replica's first 10-100 Graph
        # calls land on the same app (until the round-robin lap completes
        # to apps 2..N), spike-loading that app's token bucket and
        # producing a 429 storm during the manual-burst use case. Hash
        # the worker hostname/PID into the apps list length so different
        # replicas pick different starting apps deterministically (same
        # replica across restarts picks the same app — easier to debug).
        import os as _os
        seed = (
            _os.environ.get("HOSTNAME", "")
            or _os.environ.get("RAILWAY_REPLICA_ID", "")
            or str(_os.getpid())
        )
        if self.apps:
            self._current_index = (
                int(hashlib.sha256(seed.encode()).hexdigest()[:8], 16)
                % len(self.apps)
            )
        else:
            self._current_index = 0
        self._lock = threading.Lock()
        self._app_map: Dict[str, AppRegistry] = {
            app.client_id: app for app in self.apps
        }

    @property
    def app_count(self) -> int:
        return len(self.apps)

    def get_next_app(self) -> AppRegistry:
        """Get the next app using round-robin with throttling awareness.

        Round-robin admits HEALTHY apps first; PROBATION apps are tried
        only when no healthy app exists in this rotation pass; THROTTLED
        apps are skipped entirely. Falls back to least-loaded across
        admissible apps when nothing is fresh. Triggers lazy rate
        recovery during selection so we don't need a separate ticker.
        """
        if len(self.apps) == 1:
            app = self.apps[0]
            app.request_count += 1
            app.last_request_time = time.time()
            return app

        with self._lock:
            self._recover_rates_locked()
            # Pass 1: prefer healthy non-probation apps in round-robin
            for _ in range(len(self.apps)):
                app = self.apps[self._current_index % len(self.apps)]
                self._current_index += 1
                if not app.is_throttled and not app.in_probation:
                    app.request_count += 1
                    app.last_request_time = time.time()
                    return app
            # Pass 2: accept probationary apps (still better than
            # throttled ones — natural traffic acts as the probe)
            for _ in range(len(self.apps)):
                app = self.apps[self._current_index % len(self.apps)]
                self._current_index += 1
                if not app.is_throttled:
                    app.request_count += 1
                    app.last_request_time = time.time()
                    return app
            # All apps throttled → least-loaded fallback. Caller will
            # still hit the Retry-After sleep on its own.
            return min(self.apps, key=lambda a: a.load_score)

    def get_app_by_client_id(self, client_id: str) -> Optional[AppRegistry]:
        return self._app_map.get(client_id)

    def mark_throttled(self, client_id: str, retry_after_seconds: int):
        """Called by graph_client after a 429/503. Window-counts
        recent 429s; if threshold crossed, escalates the ban via the
        ladder and halves the per-app rate (multiplicative decrease).
        Otherwise honors Retry-After only."""
        with self._lock:
            app = self._app_map.get(client_id)
            if not app:
                return
            now = time.time()
            app._recent_429s.append(now)
            app._success_streak = 0

            window_count = sum(
                1 for ts in app._recent_429s if now - ts < _WINDOW_S
            )
            if window_count >= _WINDOW_429_THRESHOLD:
                # Escalate. Step into ban ladder; honor server Retry-
                # After as a FLOOR (don't return faster than server
                # asked even if our ladder is lower).
                ladder_idx = min(app._consecutive_bans, len(_BAN_LADDER) - 1)
                ladder_ban = float(_BAN_LADDER[ladder_idx])
                effective_ban = max(ladder_ban, float(retry_after_seconds))
                app._consecutive_bans += 1
                # Multiplicative decrease — halve rate, floor at _RATE_FLOOR.
                new_rate = max(_RATE_FLOOR, app.bucket._rate * _RATE_DECREASE_FACTOR)
                app.bucket._rate = new_rate
            else:
                # Single 429 — trust server hint, don't escalate
                effective_ban = float(retry_after_seconds)
            app.throttled_until = now + effective_ban
            # Schedule probation when ban expires: N successes required
            # before fully re-admitting.
            app._probation_remaining = _PROBATION_OK_COUNT

    def mark_success(self, client_id: str, latency_ms: float = 0.0):
        """Called by graph_client after a 2xx response. Drives
        probation exit + additive-increase rate recovery + escalation
        reset after sustained success."""
        with self._lock:
            app = self._app_map.get(client_id)
            if not app:
                return
            now = time.time()
            app._last_success_at = now
            app._last_latency_ms = latency_ms
            app._success_streak += 1
            if app._probation_remaining > 0:
                app._probation_remaining -= 1
            # Reset consecutive-ban escalation after a long success run
            if app._success_streak >= _RECOVERY_SUCCESS_COUNT:
                app._consecutive_bans = 0

    def _recover_rates_locked(self):
        """Caller MUST hold _lock. Additive-increase: any app that
        has been free of 429s for _RATE_RECOVERY_QUIET_S grows its
        rate by _RATE_INCREASE_FACTOR (capped at the original)."""
        now = time.time()
        for app in self.apps:
            if app.is_throttled or app.bucket._rate >= app._original_rate:
                continue
            quiet_for = now - (app._recent_429s[-1] if app._recent_429s else 0)
            if quiet_for >= _RATE_RECOVERY_QUIET_S:
                new_rate = min(
                    app._original_rate,
                    app.bucket._rate * _RATE_INCREASE_FACTOR,
                )
                if new_rate > app.bucket._rate:
                    app.bucket._rate = new_rate

    def reset_throttle(self, client_id: str):
        """Reset throttle state for an app (thread-safe)."""
        with self._lock:
            app = self._app_map.get(client_id)
            if app:
                app.throttled_until = 0.0
                app._probation_remaining = 0
                app._consecutive_bans = 0
                app.bucket._rate = app._original_rate

    def is_app_throttled(self, client_id: str) -> bool:
        app = self._app_map.get(client_id)
        return bool(app and app.is_throttled)

    async def acquire_app_token(
        self, client_id: str, cost: float = 1.0, priority: int = 0
    ) -> None:
        """Block on this app's per-app pace bucket until a token is available.

        priority=0 (NORMAL) is the default — matches pre-priority
        behaviour. priority>0 (HIGH/URGENT) jumps ahead of NORMAL
        callers waiting on the same bucket. See shared/graph_priority.py
        for the priority constants and queue→priority mapping.

        Unknown client_id is a silent no-op so fallback / legacy paths
        keep working.
        """
        app = self._app_map.get(client_id)
        if app is None:
            return
        await app.bucket.acquire(cost, priority=priority)

    def get_stats(self) -> List[dict]:
        """Full inspectable health snapshot — used by /admin/graph-health
        endpoint (if wired) and ops dashboards."""
        return [app.health() for app in self.apps]


# Global instance
multi_app_manager = MultiAppManager()
