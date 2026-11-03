"""Legacy live bumps cannot overwrite published catalogue counters."""
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tests.workers.test_merged_chat_sparse_paths import drain_code
from tests.workers.test_chat_stream_persistence_honesty import streaming_drain


@pytest.mark.parametrize("catalogue", [False, True])
async def test_streaming_live_bump_respects_catalogue_counter_ownership(drain_code, catalogue):
    namespace, _, _, _ = streaming_drain(drain_code, cursor=None)
    snapshot = SimpleNamespace(item_count=4, bytes_total=100, new_item_count=0,
        extra_data={"chat_inventory_format": 1} if catalogue else {})
    session = SimpleNamespace(get=AsyncMock(return_value=snapshot), commit=AsyncMock())

    @asynccontextmanager
    async def factory():
        yield session

    namespace["async_session_factory"] = factory
    await namespace["_drain_one_chat"]("chat")
    assert (snapshot.item_count, snapshot.bytes_total, snapshot.new_item_count) == (
        (4, 100, 0) if catalogue else (1, 5, 1))
