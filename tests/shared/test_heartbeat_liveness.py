"""The heartbeat loop must stay alive, heal its pool, and give up loudly.

2026-10-13 18:10Z CNPG failover: 6 of 20 backup-worker pods had their
heartbeat task hung on a DB connect / UPDATE that never returned —
``HeartbeatThread._loop`` awaited ``_beat_once()`` with no timeout. Their
``worker_heartbeats`` rows went stale, so every sweep treated the leases
those pods held as dead while the pods kept consuming, and nothing ever
restarted them.

Pinned here:
  * a beat is bounded (``beat_timeout_s``, default max(2×interval, 10 s))
    and a hung or failing beat is logged with its consecutive count;
  * every ``reset_after``-th consecutive failure disposes the shared DB
    engine so a wedged pool is rebuilt;
  * no SUCCESSFUL beat for ``fatal_s`` seconds exits the process
    non-zero (``HEARTBEAT_FATAL_S=0`` opts out), via ``os._exit`` so a
    wedged shutdown sequence cannot swallow the exit.
"""
from __future__ import annotations

import asyncio
import contextlib
import os
import pathlib
import sys
import time
import uuid

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from shared import heartbeat as hb  # noqa: E402
from shared import lease as lease_mod  # noqa: E402


def _run(coro):
    """Run on a live loop, installing one if the suite left none behind."""
    try:
        loop = asyncio.get_event_loop_policy().get_event_loop()
        if loop.is_closed():
            raise RuntimeError("closed")
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)


class _Exit(BaseException):
    """What a fake exit_fn raises so the loop really stops, as os._exit would."""


def _make(**kw) -> hb.HeartbeatThread:
    kw.setdefault("interval_s", 0.01)
    kw.setdefault("beat_timeout_s", 0.05)
    kw.setdefault("reset_after", 0)
    kw.setdefault("fatal_s", 0)
    return hb.HeartbeatThread(
        worker_id=str(uuid.uuid4()), service_name="unit-test", **kw,
    )


async def _drive(thread: hb.HeartbeatThread, until, budget_s: float = 5.0):
    """Run ``thread._loop()`` until ``until(thread)`` holds, then stop it."""
    task = asyncio.create_task(thread._loop())
    loop = asyncio.get_running_loop()
    deadline = loop.time() + budget_s
    try:
        while not until(thread):
            if task.done():
                task.result()  # surface a crashed loop
                raise AssertionError("heartbeat loop exited before the condition held")
            assert loop.time() < deadline, "condition not reached within budget"
            await asyncio.sleep(0.005)
    finally:
        thread._stop.set()
        with contextlib.suppress(BaseException):
            await asyncio.wait_for(task, timeout=2.0)


async def _hang() -> None:
    await asyncio.Event().wait()


# ─── beat timeout ─────────────────────────────────────────────────────


def test_default_beat_timeout_is_twice_the_interval_with_a_10s_floor():
    assert hb.HeartbeatThread(worker_id="w", service_name="s", interval_s=10).beat_timeout_s == 20
    assert hb.HeartbeatThread(worker_id="w", service_name="s", interval_s=2).beat_timeout_s == 10
    assert hb.HeartbeatThread(worker_id="w", service_name="s", interval_s=30).beat_timeout_s == 60


def test_a_hung_beat_times_out_and_the_loop_keeps_ticking(capsys):
    thread = _make()
    thread._beat_once = _hang  # the 10/13 shape: a DB call that never returns

    async def body():
        t0 = time.monotonic()
        await _drive(thread, lambda t: t.consecutive_failures >= 2)
        return time.monotonic() - t0

    elapsed = _run(body())
    out = capsys.readouterr().out
    assert "[HEARTBEAT] beat timed out (1 consecutive): TimeoutError" in out
    assert "[HEARTBEAT] beat timed out (2 consecutive)" in out
    # Two bounded beats at 50 ms each — not two hangs.
    assert elapsed < 2.0


# ─── consecutive failures + engine reset ──────────────────────────────


def test_failing_beats_count_consecutively_and_reset_the_engine_every_n(capsys):
    resets = []

    async def fake_reset(tag):
        resets.append(tag)
        return True

    thread = _make(reset_after=3, engine_reset=fake_reset)

    async def boom():
        raise RuntimeError("boom")

    thread._beat_once = boom
    _run(_drive(thread, lambda t: t.consecutive_failures >= 6))

    out = capsys.readouterr().out
    assert "[HEARTBEAT] beat failed (1 consecutive): RuntimeError: boom" in out
    assert "[HEARTBEAT] beat failed (3 consecutive): RuntimeError: boom" in out
    assert "3 consecutive failed beats" in out
    # Exactly at the 3rd and 6th consecutive failure — a pool that wedges
    # again is rebuilt again.
    assert resets == ["HEARTBEAT", "HEARTBEAT"]


def test_an_engine_reset_that_raises_does_not_kill_the_loop(capsys):
    async def bad_reset(tag):
        raise OSError("dispose exploded")

    thread = _make(reset_after=1, engine_reset=bad_reset)

    async def boom():
        raise RuntimeError("boom")

    thread._beat_once = boom
    _run(_drive(thread, lambda t: t.consecutive_failures >= 2))
    assert "engine reset raised: OSError: dispose exploded" in capsys.readouterr().out


def test_a_successful_beat_resets_the_consecutive_counter():
    calls = {"n": 0}

    async def flaky():
        calls["n"] += 1
        if calls["n"] <= 2:
            raise RuntimeError("not yet")

    thread = _make()
    thread._beat_once = flaky
    _run(_drive(thread, lambda t: t.last_success_at is not None))
    assert thread.consecutive_failures == 0
    assert calls["n"] >= 3


def test_default_engine_reset_is_the_shared_lease_helper():
    assert _make()._engine_reset is lease_mod.reset_db_engine


# ─── fatal stale-own-heartbeat guard ──────────────────────────────────


def test_no_successful_beat_for_fatal_s_exits_non_zero(capsys):
    exits = []

    def fake_exit(code):
        exits.append(code)
        raise _Exit()

    thread = _make(fatal_s=0.15, exit_fn=fake_exit)

    async def boom():
        raise RuntimeError("db down")

    thread._beat_once = boom

    async def body():
        t0 = time.monotonic()
        task = asyncio.create_task(thread._loop())
        with pytest.raises(_Exit):
            await asyncio.wait_for(task, timeout=5.0)
        return time.monotonic() - t0

    elapsed = _run(body())
    assert exits == [1]
    assert elapsed >= 0.15, "exited before the fatal window elapsed"
    out = capsys.readouterr().out
    assert "[HEARTBEAT] FATAL: no successful beat for" in out
    assert "exiting" in out


def test_fatal_clock_runs_from_the_last_success_not_loop_start(capsys):
    exits = []

    def fake_exit(code):
        exits.append(code)
        raise _Exit()

    thread = _make(fatal_s=0.15, exit_fn=fake_exit)
    calls = {"n": 0}

    async def ok_then_dead():
        calls["n"] += 1
        if calls["n"] <= 3:
            return  # healthy for a while
        raise RuntimeError("db down")

    thread._beat_once = ok_then_dead

    async def body():
        task = asyncio.create_task(thread._loop())
        with pytest.raises(_Exit):
            await asyncio.wait_for(task, timeout=5.0)

    _run(body())
    assert thread.last_success_at is not None
    assert exits == [1]
    assert "[HEARTBEAT] FATAL: no successful beat for" in capsys.readouterr().out


def test_fatal_guard_is_opt_out_with_zero(capsys):
    exits = []

    def fake_exit(code):
        exits.append(code)
        raise _Exit()

    thread = _make(fatal_s=0, exit_fn=fake_exit)

    async def boom():
        raise RuntimeError("db down")

    thread._beat_once = boom

    async def body():
        task = asyncio.create_task(thread._loop())
        await asyncio.sleep(0.3)
        assert not task.done(), "loop ended although the guard is disabled"
        thread._stop.set()
        await asyncio.wait_for(task, timeout=2.0)

    _run(body())
    assert exits == []
    assert "FATAL" not in capsys.readouterr().out


def test_fatal_exit_flushes_and_uses_os_exit(monkeypatch):
    seen = []
    monkeypatch.setattr(hb.os, "_exit", lambda code: seen.append(code))
    hb._fatal_exit(1)
    assert seen == [1]


def test_fatal_default_is_300s_and_env_overridable():
    # Module constant is read at import; the instance takes it as default.
    assert hb.HEARTBEAT_FATAL_S == int(os.getenv("HEARTBEAT_FATAL_S", "300"))
    assert hb.HeartbeatThread(worker_id="w", service_name="s").fatal_s == float(hb.HEARTBEAT_FATAL_S)
    assert hb.HEARTBEAT_RESET_AFTER == int(os.getenv("HEARTBEAT_RESET_AFTER", "6"))


# ─── stop() tombstone is bounded too ──────────────────────────────────


def test_stop_tombstone_is_bounded(monkeypatch, capsys):
    class _HangingSession:
        async def execute(self, *a, **k):
            await asyncio.Event().wait()

        async def commit(self):
            pass

    class _Factory:
        def __call__(self):
            session = _HangingSession()

            class _Ctx:
                async def __aenter__(self):
                    return session

                async def __aexit__(self, *exc):
                    return False

            return _Ctx()

    monkeypatch.setattr(hb, "async_session_factory", _Factory())
    thread = _make(beat_timeout_s=0.05)

    async def body():
        t0 = time.monotonic()
        await thread.stop()
        return time.monotonic() - t0

    elapsed = _run(body())
    assert elapsed < 2.0
    assert "[HEARTBEAT] tombstone failed: TimeoutError" in capsys.readouterr().out
