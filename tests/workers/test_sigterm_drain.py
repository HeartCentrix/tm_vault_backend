"""SIGTERM drain: stop intake and provider/copy claims at once, bound in-flight handlers."""
from __future__ import annotations

import asyncio
import importlib.util
import os
from pathlib import Path
import signal
import sys
import time

import pytest

_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def bw():
    path = _ROOT / "workers/backup-worker/main.py"
    spec = importlib.util.spec_from_file_location("sigterm_drain_worker", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class Runner:
    def __init__(self, events, name):
        self.events, self.name, self.closes = events, name, 0

    async def close(self):
        self.closes += 1
        self.events.append(f"{self.name}_closed")


def worker(bw, events, handler_s):
    class Worker:
        _close_work_runners = bw.BackupWorker._close_work_runners

        def __init__(self):
            self._provider_work_runner = Runner(events, "provider")
            self._copy_work_runner = Runner(events, "copy")
            self.runners = (self._provider_work_runner, self._copy_work_runner)

        async def start(self):
            # Mirrors start()/consume_queue: cancelled intake waits for the
            # in-flight handler, then the runners close in start()'s finally.
            async def handler():
                try:
                    await asyncio.sleep(handler_s)
                    events.append("handler_finished")
                except asyncio.CancelledError:
                    events.append("handler_cancelled")
                    raise
            in_flight = asyncio.create_task(handler())
            try:
                try:
                    await asyncio.Event().wait()
                finally:
                    await asyncio.gather(in_flight, return_exceptions=True)
            finally:
                await self._close_work_runners()
    return Worker()


async def _drain(bw, w, drain_s):
    run = asyncio.create_task(bw._run_until_drained(w, drain_s))
    await asyncio.sleep(0.05)
    os.kill(os.getpid(), signal.SIGTERM)
    started = time.monotonic()
    await asyncio.wait_for(run, 5)
    return time.monotonic() - started


async def test_sigterm_closes_claims_first_then_lets_inflight_handler_finish(bw):
    events = []
    w = worker(bw, events, handler_s=0.3)
    await _drain(bw, w, drain_s=5)
    assert events[-1] == "handler_finished"
    assert {"provider_closed", "copy_closed"} == set(events[:2])
    assert [r.closes for r in w.runners] == [1, 1]


async def test_sigterm_drain_is_bounded(bw):
    events = []
    w = worker(bw, events, handler_s=60)
    elapsed = await _drain(bw, w, drain_s=0.3)
    assert elapsed < 3
    assert "handler_cancelled" in events and "handler_finished" not in events
    assert [r.closes for r in w.runners] == [1, 1]


async def test_worker_crash_without_sigterm_still_raises(bw):
    class Crashing:
        async def start(self):
            raise RuntimeError("boom")
    with pytest.raises(RuntimeError, match="boom"):
        await bw._run_until_drained(Crashing(), 1)
    assert not asyncio.get_running_loop().remove_signal_handler(signal.SIGTERM)
