"""2026-08-24 pooler-disturbance fix: carry-forward slot waiters must not
pin a pgbouncer server connection while queued.

The original blocking ``SELECT pg_advisory_xact_lock`` sat inside the
caller's transaction, so under transaction-mode pgbouncer every waiter held
a pooler server slot for its whole (possibly >1h) wait — one giant copy
starved the pool and query_wait_timeout=120s killed every other TM Vault
connection. ``carry_forward_slot_session`` must therefore:

  * probe with ``pg_try_advisory_xact_lock`` (never the blocking form),
  * END its transaction (rollback) and CLOSE the session before sleeping
    on a miss — that is what releases the pooled server connection,
  * sleep OUTSIDE any session/transaction,
  * tick ``on_wait`` once per miss (stall guards count queueing as
    liveness),
  * yield the session that won the lock (the lock is xact-scoped: the
    caller's commit releases it),
  * propagate DB exceptions immediately (a killed connection must fail
    fast, not hang for the stall guard to notice).
"""
from __future__ import annotations

import hashlib
import os

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

import shared.database as db  # noqa: E402


class _FakeResult:
    def __init__(self, value):
        self._value = value

    def scalar(self):
        return self._value


class _FakeSession:
    """Minimal AsyncSession stand-in: async context manager + execute/rollback."""

    def __init__(self, grant: bool, events: list, tag: int):
        self._grant = grant
        self._events = events
        self._tag = tag
        self.closed = False
        self.rolled_back = False
        self.statements = []

    async def __aenter__(self):
        self._events.append(("enter", self._tag))
        return self

    async def __aexit__(self, *exc):
        self.closed = True
        self._events.append(("close", self._tag))
        return False

    async def execute(self, stmt, params=None):
        self.statements.append((str(stmt), params))
        self._events.append(("execute", self._tag))
        return _FakeResult(self._grant)

    async def rollback(self):
        self.rolled_back = True
        self._events.append(("rollback", self._tag))


def _wire(monkeypatch, grants):
    """Patch the session factory to hand out fake sessions granting/denying
    the lock per ``grants``, and asyncio.sleep to record instead of waiting."""
    events: list = []
    sessions: list = []

    def factory():
        s = _FakeSession(grants[len(sessions)], events, len(sessions))
        sessions.append(s)
        return s

    sleeps: list = []

    async def fake_sleep(secs):
        events.append(("sleep", secs))
        sleeps.append(secs)

    monkeypatch.setattr(db, "async_session_factory", factory)
    monkeypatch.setattr(db.asyncio, "sleep", fake_sleep)
    return events, sessions, sleeps


async def test_miss_releases_transaction_before_sleeping(monkeypatch):
    events, sessions, sleeps = _wire(monkeypatch, [False, False, True])
    ticks = []

    async with db.carry_forward_slot_session(
        "res-1", on_wait=lambda: ticks.append(1), what="test"
    ) as s:
        got = s

    # Third session won the lock and is the one yielded.
    assert got is sessions[2]
    # Every miss was rolled back AND closed BEFORE the sleep that follows it.
    sleep_is = [i for i, e in enumerate(events) if e[0] == "sleep"]
    for tag in (0, 1):
        assert sessions[tag].rolled_back
        assert sessions[tag].closed
        close_i = events.index(("close", tag))
        assert any(i > close_i for i in sleep_is), (
            "session must be closed (pooler slot released) before sleeping"
        )
    assert len(sleeps) == 2
    # on_wait ticked once per miss — queue time counts as liveness.
    assert ticks == [1, 1]


async def test_uses_try_lock_with_stable_bucket(monkeypatch):
    _, sessions, _ = _wire(monkeypatch, [True])

    async with db.carry_forward_slot_session("bucket-key"):
        pass

    stmt, params = sessions[0].statements[0]
    assert "pg_try_advisory_xact_lock" in stmt, (
        "blocking pg_advisory_xact_lock pins a pooler slot per waiter — "
        "the probe must be the try_ form"
    )
    assert "pg_advisory_xact_lock(" not in stmt.replace(
        "pg_try_advisory_xact_lock(", ""
    )
    assert params["cls"] == db.CARRY_FORWARD_LOCK_CLASS == 420018
    expected_bucket = int(
        hashlib.md5(b"bucket-key").hexdigest(), 16
    ) % max(1, db.CARRY_FORWARD_BUCKETS)
    assert params["b"] == expected_bucket == db.carry_forward_bucket("bucket-key")


async def test_immediate_grant_never_sleeps(monkeypatch):
    _, sessions, sleeps = _wire(monkeypatch, [True])

    async with db.carry_forward_slot_session("k"):
        pass

    assert sleeps == []
    assert len(sessions) == 1


async def test_db_exception_propagates_immediately(monkeypatch):
    events: list = []

    class _Boom(_FakeSession):
        async def execute(self, stmt, params=None):
            raise RuntimeError("connection was closed in the middle of operation")

    monkeypatch.setattr(db, "async_session_factory", lambda: _Boom(False, events, 0))

    with pytest.raises(RuntimeError, match="connection was closed"):
        async with db.carry_forward_slot_session("k"):
            pass


async def test_wait_ceiling_raises(monkeypatch):
    _wire(monkeypatch, [False] * 50)
    monkeypatch.setattr(db, "CARRY_FORWARD_WAIT_MAX_S", 0.0001)

    with pytest.raises(RuntimeError, match="CARRY_FORWARD_WAIT_MAX_S"):
        async with db.carry_forward_slot_session("k"):
            pass


async def test_per_site_wait_schedule_override(monkeypatch):
    # MICRO copy sites (per-file version carry-forward, P8 per-chat copy)
    # acquire thousands of times per walk — they pass a sub-second cadence;
    # a 5-10s default sleep per miss would add hours of wall clock.
    _, _, sleeps = _wire(monkeypatch, [False, True])

    async with db.carry_forward_slot_session(
        "k", wait_base_s=0.25, wait_jitter_s=0.5
    ):
        pass

    assert len(sleeps) == 1
    assert 0.25 <= sleeps[0] <= 0.75


def test_micro_sites_use_fast_poll():
    import pathlib
    main_src = (pathlib.Path(__file__).resolve().parents[2]
                / "workers" / "backup-worker" / "main.py").read_text()
    for marker in ('what="file-version carry-forward"',
                   'what="P8 attachment copy"'):
        i = main_src.find(marker)
        assert i != -1
        assert "wait_base_s=0.25" in main_src[i:i + 300], (
            f"micro-copy site {marker} must poll fast — coarse polling "
            "adds hours to per-item walks (2026-08-24 review finding)"
        )


def test_caller_exception_closes_winning_session(monkeypatch):
    # Exceptions raised in the caller's block must propagate AND close the
    # yielded session (releasing the xact-scoped lock).
    import asyncio as _aio

    events, sessions, _ = _wire(monkeypatch, [True])

    async def run():
        async with db.carry_forward_slot_session("k"):
            raise ValueError("caller blew up")

    with pytest.raises(ValueError, match="caller blew up"):
        _aio.run(run())
    assert sessions[0].closed


async def test_fast_retries_back_off_and_cap_under_sustained_contention(monkeypatch):
    _, _, sleeps = _wire(monkeypatch, [False] * 7 + [True])
    monkeypatch.setattr(db, "CARRY_FORWARD_WAIT_BASE_S", 2.0)
    monkeypatch.setattr(db, "CARRY_FORWARD_WAIT_JITTER_S", 0.0)
    monkeypatch.setattr(db, "CARRY_FORWARD_WAIT_CAP_S", 4.0, raising=False)

    async with db.carry_forward_slot_session("busy-bucket"):
        pass

    assert sleeps[:2] == [0.25, 0.25]
    assert sleeps[2:] == [2.0, 4.0, 4.0, 4.0, 4.0]


async def test_retry_jitter_stays_inside_backoff_cap(monkeypatch):
    _, _, sleeps = _wire(monkeypatch, [False] * 6 + [True])
    monkeypatch.setattr(db, "CARRY_FORWARD_WAIT_CAP_S", 2.0, raising=False)
    monkeypatch.setattr(db.random, "uniform", lambda low, high: high)

    async with db.carry_forward_slot_session(
        "busy-bucket", wait_base_s=0.25, wait_jitter_s=0.5
    ):
        pass

    assert sleeps[0] == 0.75  # Existing micro override stays fast on first miss.
    assert sleeps[-1] == 2.0
    assert all(0 < delay <= 2.0 for delay in sleeps)


async def test_wait_deadline_clamps_sleep_and_prevents_next_probe(monkeypatch):
    _, sessions, sleeps = _wire(monkeypatch, [False] * 10)
    now = [100.0]

    async def sleep(seconds):
        sleeps.append(seconds)
        now[0] += seconds

    monkeypatch.setattr(db, "monotonic", lambda: now[0])
    monkeypatch.setattr(db.asyncio, "sleep", sleep)
    monkeypatch.setattr(db.random, "uniform", lambda low, high: 0.0)
    monkeypatch.setattr(db, "CARRY_FORWARD_WAIT_MAX_S", 0.35)

    with pytest.raises(RuntimeError, match="CARRY_FORWARD_WAIT_MAX_S"):
        async with db.carry_forward_slot_session("deadline-key"):
            pytest.fail("expired waiter must not acquire")

    assert now[0] - 100.0 == pytest.approx(0.35)
    assert sleeps == pytest.approx([0.25, 0.10])
    assert len(sessions) == 2
    assert all(session.closed and session.rolled_back for session in sessions)


async def test_cancelled_waiter_never_opens_session(monkeypatch):
    _, sessions, _ = _wire(monkeypatch, [])

    with pytest.raises(db.asyncio.CancelledError):
        async with db.carry_forward_slot_session(
            "private-customer-key", should_cancel=lambda: True
        ):
            pytest.fail("cancelled waiter must not acquire")

    assert sessions == []


async def test_async_cancellation_interrupts_long_backoff_after_session_close(monkeypatch):
    _, sessions, sleeps = _wire(monkeypatch, [False, True])
    cancelled = False

    async def sleep(seconds):
        nonlocal cancelled
        assert sessions[0].closed and sessions[0].rolled_back
        sleeps.append(seconds)
        cancelled = True

    async def should_cancel():
        return cancelled

    monkeypatch.setattr(db.asyncio, "sleep", sleep)
    monkeypatch.setattr(db, "CARRY_FORWARD_FAST_RETRIES", 0, raising=False)

    with pytest.raises(db.asyncio.CancelledError):
        async with db.carry_forward_slot_session(
            "private-customer-key", should_cancel=should_cancel,
            wait_base_s=30.0, wait_jitter_s=0.0,
        ):
            pytest.fail("cancelled waiter must not acquire")

    assert len(sessions) == 1
    assert 0 < sum(sleeps) <= 0.5


async def test_local_bucket_wait_deadline_never_opens_another_session(monkeypatch):
    import asyncio

    events, sessions = [], []

    def factory():
        session = _FakeSession(True, events, len(sessions))
        sessions.append(session)
        return session

    monkeypatch.setattr(db, "async_session_factory", factory)
    monkeypatch.setattr(db, "CARRY_FORWARD_WAIT_MAX_S", 0.01)

    async with db.carry_forward_slot_session("same-slot"):
        with pytest.raises(RuntimeError, match="CARRY_FORWARD_WAIT_MAX_S"):
            async with db.carry_forward_slot_session("same-slot"):
                pytest.fail("local waiter must not bypass active contender")
        assert len(sessions) == 1
        assert not sessions[0].closed

    # Deadline/cancellation must not leak the local permit.
    async with asyncio.timeout(1):
        async with db.carry_forward_slot_session("same-slot"):
            pass
    assert len(sessions) == 2


async def test_local_bucket_wait_can_be_cancelled_without_db_probe(monkeypatch):
    import asyncio

    events, sessions = [], []
    cancelled = False

    def factory():
        session = _FakeSession(True, events, len(sessions))
        sessions.append(session)
        return session

    def cancel():
        nonlocal cancelled
        cancelled = True

    monkeypatch.setattr(db, "async_session_factory", factory)
    monkeypatch.setattr(db, "_CARRY_FORWARD_CANCEL_POLL_S", 0.001, raising=False)

    async with db.carry_forward_slot_session("same-slot"):
        asyncio.get_running_loop().call_soon(cancel)
        with pytest.raises(asyncio.CancelledError):
            async with db.carry_forward_slot_session(
                "same-slot", should_cancel=lambda: cancelled,
            ):
                pytest.fail("local waiter must honor cancellation")
        assert len(sessions) == 1


async def test_local_contender_bound_is_per_lane_and_bucket(monkeypatch):
    events, sessions = [], []

    def factory():
        session = _FakeSession(True, events, len(sessions))
        sessions.append(session)
        return session

    monkeypatch.setattr(db, "async_session_factory", factory)
    # k0 and k2 map to different default micro buckets (1 and 0).
    async with db.carry_forward_slot_session("k0"):
        async with db.carry_forward_slot_session("k2"):
            async with db.carry_forward_slot_session("k0", lane="giant"):
                assert len(sessions) == 3


def test_local_contender_state_does_not_bind_later_event_loops(monkeypatch):
    import asyncio

    events = []
    monkeypatch.setattr(
        db, "async_session_factory", lambda: _FakeSession(True, events, 0)
    )

    async def run():
        entered = asyncio.Event()
        release = asyncio.Event()

        async def holder():
            async with db.carry_forward_slot_session("same-slot"):
                entered.set()
                await release.wait()

        async def follower():
            async with db.carry_forward_slot_session("same-slot"):
                pass

        task = asyncio.create_task(holder())
        await entered.wait()
        waiting = asyncio.create_task(follower())
        await asyncio.sleep(0)
        release.set()
        await asyncio.gather(task, waiting)

    asyncio.run(run())
    asyncio.run(run())


async def test_wait_logs_safe_structured_outcome_and_wait_fields(monkeypatch, caplog):
    import logging

    _wire(monkeypatch, [False, True])
    monkeypatch.setattr(db, "_CARRY_FORWARD_LOG_INTERVAL_S", 0)
    with caplog.at_level(logging.INFO, logger="shared.database"):
        async with db.carry_forward_slot_session(
            "private-customer-key", what="private-customer-operation"
        ):
            pass

    records = [record for record in caplog.records if "CARRY_FORWARD" in record.message]
    assert records
    assert {record.copy_slot_outcome for record in records} == {"waiting", "acquired"}
    for record in records:
        assert record.copy_slot_lane == "micro"
        assert isinstance(record.copy_slot_bucket, int)
        assert record.copy_slot_attempts == 1
        assert record.copy_slot_wait_s >= 0
        assert record.copy_slot_wait_kind == "database"
        assert "private-customer" not in record.message
        assert "private-customer" not in str(record.__dict__)


async def test_saturated_backoff_keeps_jitter_to_avoid_synchronized_probes(monkeypatch):
    _, _, sleeps = _wire(monkeypatch, [False] * 7 + [True])
    monkeypatch.setattr(db, "CARRY_FORWARD_WAIT_CAP_S", 2.0)
    monkeypatch.setattr(db.random, "uniform", lambda low, high: low)

    async with db.carry_forward_slot_session(
        "busy-bucket", wait_base_s=1.0, wait_jitter_s=0.5,
    ):
        pass

    assert 1.0 <= sleeps[-1] < 2.0, "capped retries must still have random spread"


async def test_wait_ceiling_closes_a_probe_stuck_acquiring_db_connection(monkeypatch):
    import asyncio

    events = []

    class StuckSession(_FakeSession):
        async def execute(self, stmt, params=None):
            await asyncio.Event().wait()

    session = StuckSession(False, events, 0)
    monkeypatch.setattr(db, "async_session_factory", lambda: session)
    monkeypatch.setattr(db, "CARRY_FORWARD_WAIT_MAX_S", 0.01)

    with pytest.raises(RuntimeError, match="CARRY_FORWARD_WAIT_MAX_S"):
        async with db.carry_forward_slot_session("stuck-pool"):
            pytest.fail("a stuck connection checkout must be bounded")
    assert session.closed


async def test_wait_ceiling_does_not_limit_the_winning_copy(monkeypatch):
    import asyncio

    events = []
    session = _FakeSession(True, events, 0)
    monkeypatch.setattr(db, "async_session_factory", lambda: session)
    monkeypatch.setattr(db, "CARRY_FORWARD_WAIT_MAX_S", 0.01)

    async with db.carry_forward_slot_session("copy-key"):
        await asyncio.sleep(0.02)
        assert not session.closed
    assert session.closed


async def test_local_gate_prevents_followers_from_probing_during_backoff(monkeypatch):
    import asyncio

    events, sessions = [], []
    sleeping = asyncio.Event()
    resume = asyncio.Event()
    real_sleep = asyncio.sleep

    def factory():
        session = _FakeSession(bool(sessions), events, len(sessions))
        sessions.append(session)
        return session

    async def sleep(seconds):
        assert sessions[0].closed and sessions[0].rolled_back
        sleeping.set()
        await resume.wait()

    async def copy():
        async with db.carry_forward_slot_session("same-slot"):
            pass

    monkeypatch.setattr(db, "async_session_factory", factory)
    monkeypatch.setattr(db.asyncio, "sleep", sleep)
    first = asyncio.create_task(copy())
    await sleeping.wait()
    follower = asyncio.create_task(copy())
    await real_sleep(0)
    assert len(sessions) == 1, "only the local leader may probe the DB"
    resume.set()
    await asyncio.gather(first, follower)
    assert len(sessions) == 3


async def test_cancelled_local_task_does_not_steal_or_leak_permit(monkeypatch):
    import asyncio

    events, sessions = [], []

    def factory():
        session = _FakeSession(True, events, len(sessions))
        sessions.append(session)
        return session

    async def copy():
        async with db.carry_forward_slot_session("same-slot"):
            pass

    monkeypatch.setattr(db, "async_session_factory", factory)
    async with db.carry_forward_slot_session("same-slot"):
        cancelled = asyncio.create_task(copy())
        await asyncio.sleep(0)
        cancelled.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cancelled
        follower = asyncio.create_task(copy())
        await asyncio.sleep(0)
        assert len(sessions) == 1
    async with asyncio.timeout(1):
        await follower
    assert len(sessions) == 2
