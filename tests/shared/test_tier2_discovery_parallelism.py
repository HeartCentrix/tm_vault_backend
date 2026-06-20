import asyncio
import os

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")

from shared.tier2_discovery import chunk_user_ids, run_bounded_user_tasks


def test_tier2_chunking_default_shape_for_4k_users():
    chunks = chunk_user_ids(range(4000), chunk_size=25)

    assert len(chunks) == 160
    assert all(len(chunk) <= 25 for chunk in chunks)
    assert chunks[0] == [str(i) for i in range(25)]
    assert chunks[-1] == [str(i) for i in range(3975, 4000)]


def test_tier2_chunking_never_uses_zero_or_negative_sizes():
    assert chunk_user_ids(["a", "b", "c"], chunk_size=0) == [["a"], ["b"], ["c"]]
    assert chunk_user_ids(["a", "b", "c"], chunk_size=-10) == [["a"], ["b"], ["c"]]


@pytest.mark.asyncio
async def test_bounded_user_tasks_respects_concurrency():
    active = 0
    max_active = 0

    async def worker(user_id: int) -> int:
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        await asyncio.sleep(0.01)
        active -= 1
        return user_id * 2

    results = await run_bounded_user_tasks(range(10), concurrency=3, worker=worker)

    assert results == [i * 2 for i in range(10)]
    assert max_active == 3
