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
