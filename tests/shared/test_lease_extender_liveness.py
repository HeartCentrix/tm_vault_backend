"""LeaseExtender: bounded bumps, engine reset, and handler-scoped renewal.

Two defects from the 2026-10-13/14 incidents:

  1. ``LeaseExtender._loop`` awaited ``_bump_once()`` with no timeout, so a
     DB round-trip that never returned (black-holed socket after the CNPG
     failover) hung the renewal loop for good in 3 of 20 pods.

  2. ``_bump_once`` renewed EVERY snapshots / snapshot_partitions / jobs
     row whose lease_owner_id was this pod — including rows whose handler
     had died hours earlier (terminal write + release both lost during
     the 10/14 outage: "row will strand until this pod exits"). The
     reconciler needs an expired lease, so those rows were zombies for
     12 h. Renewal is now scoped to rows a live handler registered, with
     the owner-wide statement kept ONLY while nothing has ever registered
     in the process (other LeaseExtender users keep working).
"""
from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from typing import Dict, List, Optional, Tuple

import pytest

from shared import lease as lease_mod
from shared.lease import (
    LeaseExtender,
    _reset_registry,
    register,
    registered_ids,
    registry_active,
    run_bounded,
    unregister,
)


def _run(coro):
    try:
        loop = asyncio.get_event_loop_policy().get_event_loop()
        if loop.is_closed():
            raise RuntimeError("closed")
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)


@pytest.fixture(autouse=True)
def _clean_registry():
    _reset_registry()
    yield
    _reset_registry()


# ─── run_bounded ──────────────────────────────────────────────────────


def test_run_bounded_returns_the_value_and_propagates_exceptions():
    async def ok():
        return 42

    async def bad():
        raise ValueError("nope")

    async def body():
        assert await run_bounded(ok(), 1.0) == 42
        with pytest.raises(ValueError):
            await run_bounded(bad(), 1.0)

    _run(body())


def test_run_bounded_returns_promptly_even_when_the_hung_call_swallows_cancel():
    """The shape of the real hang: the DB call ignores cancellation
    because its cleanup (session close → ROLLBACK) is itself stuck on the
    dead socket. ``asyncio.wait_for`` would wait for that cleanup forever
    (negative control below); run_bounded must not."""
    release = asyncio.Event()

    async def stuck():
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await release.wait()  # "cleanup" that never returns on its own

    async def body():
        t0 = time.monotonic()
        with pytest.raises(asyncio.TimeoutError):
            await run_bounded(stuck(), 0.05)
        bounded_elapsed = time.monotonic() - t0

        # Negative control: plain wait_for on the same coroutine is still
        # pending long after its timeout, because it awaits the cancelled
        # task's unwinding.
        control = asyncio.create_task(asyncio.wait_for(stuck(), 0.05))
        await asyncio.sleep(0.3)
        control_hung = not control.done()

        release.set()  # let both abandoned coroutines finish
        await asyncio.sleep(0.05)
        with contextlib.suppress(BaseException):
            await control
        return bounded_elapsed, control_hung

    bounded_elapsed, control_hung = _run(body())
    assert bounded_elapsed < 1.0
    assert control_hung, "negative control did not reproduce the wait_for hang"


# ─── loop: bounded bump + consecutive failures + engine reset ─────────


def _extender(**kw) -> LeaseExtender:
    kw.setdefault("interval_s", 0.01)
    kw.setdefault("bump_timeout_s", 0.05)
    kw.setdefault("reset_after", 0)
    return LeaseExtender(owner_id=uuid.uuid4(), session_factory=None, **kw)


async def _drive(ext: LeaseExtender, until, budget_s: float = 5.0):
    task = asyncio.create_task(ext._loop())
    loop = asyncio.get_running_loop()
    deadline = loop.time() + budget_s
    try:
        while not until(ext):
            if task.done():
                task.result()
                raise AssertionError("extender loop exited early")
            assert loop.time() < deadline, "condition not reached within budget"
            await asyncio.sleep(0.005)
    finally:
        ext._stop.set()
        with contextlib.suppress(BaseException):
            await asyncio.wait_for(task, timeout=2.0)


def test_default_bump_timeout_is_twice_the_interval_with_a_10s_floor():
    assert LeaseExtender(owner_id=uuid.uuid4(), session_factory=None, interval_s=120).bump_timeout_s == 240
    assert LeaseExtender(owner_id=uuid.uuid4(), session_factory=None, interval_s=3).bump_timeout_s == 10


def test_a_hung_bump_times_out_and_the_loop_keeps_ticking(capsys):
    ext = _extender()

    async def hang():
        await asyncio.Event().wait()

    ext._bump_once = hang
    t0 = time.monotonic()
    _run(_drive(ext, lambda e: e.consecutive_failures >= 2))
    assert time.monotonic() - t0 < 2.0
    out = capsys.readouterr().out
    assert "[LEASE_EXTENDER] bump timed out (1 consecutive): TimeoutError" in out
    assert "[LEASE_EXTENDER] bump timed out (2 consecutive)" in out


def test_failing_bumps_reset_the_engine_every_n(capsys):
    resets = []

    async def fake_reset(tag):
        resets.append(tag)
        return True

    ext = _extender(reset_after=2, engine_reset=fake_reset)

    async def boom():
        raise RuntimeError("boom")

    ext._bump_once = boom
    _run(_drive(ext, lambda e: e.consecutive_failures >= 4))
    out = capsys.readouterr().out
    assert "[LEASE_EXTENDER] bump failed (2 consecutive): RuntimeError: boom" in out
    assert "2 consecutive failed bumps" in out
    assert resets == ["LEASE_EXTENDER", "LEASE_EXTENDER"]


def test_a_successful_bump_resets_the_counter():
    calls = {"n": 0}

    async def flaky():
        calls["n"] += 1
        if calls["n"] <= 2:
            raise RuntimeError("not yet")

    ext = _extender()
    ext._bump_once = flaky
    _run(_drive(ext, lambda e: calls["n"] >= 3 and e.consecutive_failures == 0))
    assert ext.consecutive_failures == 0


def test_default_engine_reset_is_the_shared_helper():
    assert _extender()._engine_reset is lease_mod.reset_db_engine


# ─── registry ─────────────────────────────────────────────────────────


def test_register_unregister_are_idempotent_and_canonicalise_ids():
    rid = uuid.uuid4()
    assert registry_active() is False
    register("snapshots", rid)
    register("snapshots", str(rid))
    register("snapshots", str(rid).upper())
    assert registered_ids("snapshots") == [str(rid)]
    assert registry_active() is True
    unregister("snapshots", str(rid).upper())
    unregister("snapshots", rid)  # second time is a no-op
    assert registered_ids("snapshots") == []
    # Once used, stays scoped: the owner-wide fallback never comes back.
    assert registry_active() is True


def test_registry_rejects_unknown_tables():
    with pytest.raises(ValueError):
        register("resources", uuid.uuid4())
    with pytest.raises(ValueError):
        unregister("resources", uuid.uuid4())


# ─── _bump_once: owner-wide fallback vs registered ids ────────────────


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return list(self._rows)

    def first(self):
        return self._rows[0] if self._rows else None


class _Session:
    """Records every statement; RETURNING rows are staged per table."""

    def __init__(self, returning: Optional[Dict[str, List[str]]] = None):
        self.executed: List[Tuple[str, Dict]] = []
        self.committed = False
        self._returning = returning or {}

    async def execute(self, statement, binds: Optional[Dict] = None):
        sql = str(statement)
        self.executed.append((sql, dict(binds or {})))
        table = sql.split("UPDATE ", 1)[1].split()[0]
        return _Result([(rid,) for rid in self._returning.get(table, [])])

    async def commit(self):
        self.committed = True


class _Factory:
    def __init__(self, session):
        self._session = session

    def __call__(self):
        session = self._session

        class _Ctx:
            async def __aenter__(self):
                return session

            async def __aexit__(self, *exc):
                return False

        return _Ctx()


def _tables(session: _Session) -> List[str]:
    return [sql.split("UPDATE ", 1)[1].split()[0] for sql, _ in session.executed]


def test_bump_is_owner_wide_while_nothing_has_ever_registered():
    owner = uuid.uuid4()
    session = _Session()
    ext = LeaseExtender(owner_id=owner, session_factory=_Factory(session))
    _run(ext._bump_once())
    assert _tables(session) == ["snapshots", "snapshot_partitions", "jobs"]
    for sql, binds in session.executed:
        assert "ANY(" not in sql and "ids" not in binds, sql
        assert "lease_owner_id = cast(:owner AS uuid)" in sql
        assert binds["owner"] == str(owner)
    assert session.committed


def test_bump_renews_only_registered_rows_once_the_registry_is_used():
    owner = uuid.uuid4()
    pid = uuid.uuid4()
    register("snapshot_partitions", pid)
    session = _Session(returning={"snapshot_partitions": [str(pid)]})
    ext = LeaseExtender(owner_id=owner, session_factory=_Factory(session))
    _run(ext._bump_once())
    # Only the table with a registered row is touched — a dead handler's
    # snapshot / job rows are no longer kept alive by this process.
    assert _tables(session) == ["snapshot_partitions"]
    sql, binds = session.executed[0]
    assert "id = ANY(cast(:ids AS uuid[]))" in sql
    assert "lease_owner_id = cast(:owner AS uuid)" in sql
    assert "status::text = :sts" in sql
    assert "RETURNING id" in sql
    assert binds["ids"] == [str(pid)]
    assert binds["owner"] == str(owner)
    assert binds["sts"] == "IN_PROGRESS"
    assert registered_ids("snapshot_partitions") == [str(pid)]
    assert session.committed


def test_bump_prunes_rows_the_update_did_not_renew():
    keep, gone = uuid.uuid4(), uuid.uuid4()
    register("snapshots", keep)
    register("snapshots", gone)
    # The DB renewed only `keep`: `gone` is terminal / released / re-leased.
    session = _Session(returning={"snapshots": [str(keep)]})
    ext = LeaseExtender(owner_id=uuid.uuid4(), session_factory=_Factory(session))
    _run(ext._bump_once())
    assert registered_ids("snapshots") == [str(keep)]


def test_bump_issues_nothing_when_every_registered_row_was_dropped():
    pid = uuid.uuid4()
    register("snapshot_partitions", pid)
    unregister("snapshot_partitions", pid)
    session = _Session()
    ext = LeaseExtender(owner_id=uuid.uuid4(), session_factory=_Factory(session))
    _run(ext._bump_once())
    assert session.executed == [], "owner-wide fallback must not return after the registry was used"


def test_a_failed_bump_does_not_prune():
    pid = uuid.uuid4()
    register("snapshot_partitions", pid)

    class _Boom(_Session):
        async def execute(self, statement, binds=None):
            raise ConnectionError("socket gone")

    ext = LeaseExtender(owner_id=uuid.uuid4(), session_factory=_Factory(_Boom()))
    with pytest.raises(ConnectionError):
        _run(ext._bump_once())
    assert registered_ids("snapshot_partitions") == [str(pid)]
