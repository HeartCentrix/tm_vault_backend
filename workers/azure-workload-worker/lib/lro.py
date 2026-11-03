"""Long-running operation polling helper for Azure ARM operations."""
import asyncio
import time
import logging
from typing import Any

logger = logging.getLogger("azure.lro")


async def await_lro(poller, name: str, timeout_seconds: int = 3600,
                    poll_interval: int = 15) -> Any:
    """
    Await an Azure Long-Running Operation (LRO) with a real timeout and
    progress logging.

    azure.core's AsyncLROPoller only advances its polling inside
    result()/wait() — an earlier version of this helper awaited
    result() directly as a readiness "probe", which blocks until the
    LRO terminates and made timeout_seconds dead code. With every
    queue in this worker consumed serially, one Azure operation
    wedged in 'InProgress' parked the whole queue until pod restart.
    Drive result() as a task and enforce the deadline around it
    instead.
    """
    start = time.monotonic()

    async def _result():
        result = poller.result()
        if asyncio.iscoroutine(result):
            result = await result
        return result

    task = asyncio.ensure_future(_result())
    try:
        while True:
            elapsed = time.monotonic() - start
            remaining = timeout_seconds - elapsed
            if remaining <= 0:
                raise TimeoutError(
                    f"LRO '{name}' exceeded {timeout_seconds}s timeout (ran {elapsed:.0f}s)"
                )

            done, _ = await asyncio.wait({task}, timeout=min(poll_interval, remaining))
            if done:
                # Completed — returns the result, or re-raises the LRO's
                # real failure immediately (no probe-swallowing delay).
                return task.result()

            logger.info("[LRO] %s in progress (%.0fs elapsed)", name, time.monotonic() - start)
    finally:
        if not task.done():
            # Stop waiting; the Azure-side operation keeps running but
            # we surface the timeout to the caller instead of hanging.
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
