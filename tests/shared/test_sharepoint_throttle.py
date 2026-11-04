"""SharePoint throttles the app across the whole tenant: one 429 must pause every request to
that host, and concurrency must adapt. Railway 19-site run 2026-11-04 (fbf5904): 19 sites x 8
REST readers with no shared pause -> 12/19 sites PARTIAL, files dropped after 8 x 429."""
import asyncio
import time

import pytest

from shared.graph_ratelimit import HostThrottle


async def test_a_throttle_pauses_every_request_to_the_host():
    throttle = HostThrottle(4)
    await throttle.throttled(0.3)
    started = time.monotonic()
    async with throttle.slot():
        pass
    assert time.monotonic() - started >= 0.25


async def test_concurrency_halves_once_per_throttle_window_and_recovers_on_success():
    throttle = HostThrottle(8)
    await throttle.throttled(0.05)
    await throttle.throttled(0.05)          # same burst: no second cut
    assert throttle.limit == 4
    await asyncio.sleep(0.06)
    await throttle.throttled(0.01)
    assert throttle.limit == 2
    for _ in range(100):
        throttle.succeeded()
    assert throttle.limit == 8               # never above the configured ceiling
    for _ in range(10):
        await asyncio.sleep(0.02)
        await throttle.throttled(0.01)
    assert throttle.limit == 1               # never below one request


async def test_requests_beyond_the_limit_wait_for_a_free_slot():
    throttle = HostThrottle(2)
    inside, peak = 0, 0

    async def request():
        nonlocal inside, peak
        async with throttle.slot():
            inside += 1
            peak = max(peak, inside)
            await asyncio.sleep(0.02)
            inside -= 1

    await asyncio.gather(*(request() for _ in range(6)))
    assert peak == 2


class FakeShared:
    def __init__(self):
        self.deadline = None

    async def get(self):
        return self.deadline

    async def put(self, deadline):
        self.deadline = deadline


async def test_a_throttle_seen_by_one_process_pauses_the_others():
    shared = FakeShared()
    first, second = HostThrottle(4, shared=shared), HostThrottle(4, shared=shared)
    await first.throttled(0.3)
    started = time.monotonic()
    async with second.slot():
        pass
    assert time.monotonic() - started >= 0.25


async def test_shared_store_failure_falls_back_to_the_local_pause():
    class Broken:
        async def get(self):
            raise ConnectionError("redis down")

        async def put(self, deadline):
            raise ConnectionError("redis down")

    throttle = HostThrottle(4, shared=Broken())
    await throttle.throttled(0.2)
    started = time.monotonic()
    async with throttle.slot():
        pass
    assert time.monotonic() - started >= 0.15
