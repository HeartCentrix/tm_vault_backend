"""Regression tests for azure-workload-worker await_lro.

azure.core's AsyncLROPoller only advances polling inside result()/wait().
An earlier await_lro awaited result() as a readiness "probe" on the first
loop iteration, which blocks until the LRO terminates — timeout_seconds
was dead code and a wedged Azure operation parked the worker's serial
queue forever. These tests pin the contract: the timeout fires, success
returns the value, and a real LRO failure surfaces immediately (not one
poll_interval later).
"""
import asyncio
import importlib.util
import os
import time

import pytest

_LRO_PATH = os.path.abspath(os.path.join(
    os.path.dirname(__file__), "..", "..",
    "workers", "azure-workload-worker", "lib", "lro.py",
))

_spec = importlib.util.spec_from_file_location("_azure_lro", _LRO_PATH)
_lro = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_lro)
await_lro = _lro.await_lro


class _FakeAsyncPoller:
    """Mimics azure.core AsyncLROPoller: result() returns a coroutine,
    done() is False until the operation terminates."""

    def __init__(self, coro_factory):
        self._coro_factory = coro_factory
        self._done = False

    def done(self):
        return self._done

    def result(self):
        return self._coro_factory()


def test_timeout_is_enforced_on_stuck_lro():
    async def _stuck():
        # Simulates Azure's "InProgress forever" failure mode.
        await asyncio.sleep(60)

    poller = _FakeAsyncPoller(_stuck)
    start = time.monotonic()
    with pytest.raises(TimeoutError):
        asyncio.run(await_lro(poller, "stuck-op", timeout_seconds=1, poll_interval=1))
    # Must fire around the deadline, not after the LRO "finishes".
    assert time.monotonic() - start < 10


def test_success_returns_result():
    async def _ok():
        await asyncio.sleep(0)
        return "sentinel"

    poller = _FakeAsyncPoller(_ok)
    out = asyncio.run(await_lro(poller, "ok-op", timeout_seconds=30, poll_interval=1))
    assert out == "sentinel"


def test_lro_failure_surfaces_immediately():
    async def _boom():
        raise RuntimeError("provisioning failed")

    poller = _FakeAsyncPoller(_boom)
    start = time.monotonic()
    with pytest.raises(RuntimeError, match="provisioning failed"):
        # Long poll_interval: the old code swallowed the probe error and
        # only re-raised a full poll_interval later via the done() branch.
        asyncio.run(await_lro(poller, "boom-op", timeout_seconds=30, poll_interval=15))
    assert time.monotonic() - start < 5
