"""The Tier-1 mailbox handler must not fan out one coroutine per batch.

2026-08-07: `Corp_Esplanade` (ROOM_MAILBOX, 22,605 messages) SIGKILLed a
backup-worker on every Gold tick. `backup_mailbox` built
`batches = [items[i:i+50] ...]` (main.py:22388) and then
`asyncio.gather(*[backup_batch(b) for b in batches])` (main.py:22435) —
~453 batch coroutines live at once, each doing `json.dumps(msg).encode()`
per message and awaiting its own `asyncio.gather` of uploads. That put
3-4 simultaneous copies of the whole mailbox on the heap (~2.5-4 GB) on a
pod sharing 8Gi with BACKUP_CONCURRENCY=20 siblings.

The crash then became permanent: the redelivery was ACK-dropped by the
in-flight guard, so nobody re-ran it, and the reconciler settled the
snapshot FAILED — every 8 hours, forever.

Bounding the fan-out caps peak at (concurrency x batch_size) messages in
flight regardless of mailbox size. `mailbox_batch_concurrency` is the
pure knob-reader; it must never return 0 or a negative (that would
deadlock the gather or restore unbounded behaviour).
"""
from __future__ import annotations

import asyncio
import importlib.util
import os
import pathlib
import sys

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "workers" / "backup-worker" / "main.py"
_spec = importlib.util.spec_from_file_location("bw_mailbox_bound", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_mailbox_bound"] = _bw
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(_bw)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(
        f"backup-worker module failed to import: {exc}",
        allow_module_level=True,
    )

gather_bounded = _bw.gather_bounded
mailbox_batch_concurrency = _bw.mailbox_batch_concurrency


class TestMailboxBatchConcurrencyKnob:
    def test_default_when_unset(self):
        assert mailbox_batch_concurrency(None) == 4

    def test_explicit_value_wins(self):
        assert mailbox_batch_concurrency("8") == 8

    @pytest.mark.parametrize("raw", ["0", "-1", "-100"])
    def test_never_returns_a_deadlocking_or_unbounded_value(self, raw):
        """0 would hang the gather forever; negatives raise in Semaphore."""
        assert mailbox_batch_concurrency(raw) == 1

    @pytest.mark.parametrize("raw", ["", "   ", "abc", "4.5", "None"])
    def test_garbage_falls_back_to_the_default(self, raw):
        assert mailbox_batch_concurrency(raw) == 4

    def test_whitespace_is_tolerated(self):
        assert mailbox_batch_concurrency("  6  ") == 6


class TestGatherBounded:
    @pytest.mark.asyncio
    async def test_never_exceeds_the_limit(self):
        live = 0
        peak = 0

        async def work(_):
            nonlocal live, peak
            live += 1
            peak = max(peak, live)
            await asyncio.sleep(0.01)
            live -= 1
            return 1

        # 40 units of work, limit 4 — the shape that used to be 453-wide.
        results = await gather_bounded([lambda i=i: work(i) for i in range(40)], 4)

        assert peak <= 4, f"fan-out reached {peak}, bound was 4"
        assert results == [1] * 40

    @pytest.mark.asyncio
    async def test_preserves_result_order(self):
        async def work(i):
            # Reverse the sleep so completion order != submission order.
            await asyncio.sleep((10 - i) * 0.002)
            return i

        results = await gather_bounded([lambda i=i: work(i) for i in range(10)], 3)
        assert results == list(range(10))

    @pytest.mark.asyncio
    async def test_exceptions_are_returned_not_raised(self):
        """Matches the existing return_exceptions=True contract at
        main.py:22435 — one bad batch must not abort the mailbox."""

        async def ok():
            return (1, 10)

        async def boom():
            raise RuntimeError("upload died")

        results = await gather_bounded([ok, boom, ok], 2)

        assert results[0] == (1, 10)
        assert isinstance(results[1], RuntimeError)
        assert results[2] == (1, 10)

    @pytest.mark.asyncio
    async def test_a_failing_unit_still_releases_its_slot(self):
        """A raise inside the semaphore must not leak the permit, or the
        mailbox stalls after `limit` failures."""

        async def boom():
            raise RuntimeError("x")

        async def ok():
            return "done"

        results = await gather_bounded([boom, boom, ok], 2)
        assert results[2] == "done"

    @pytest.mark.asyncio
    async def test_empty_input(self):
        assert await gather_bounded([], 4) == []
