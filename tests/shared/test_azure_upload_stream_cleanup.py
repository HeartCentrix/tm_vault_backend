import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock
import pytest
from shared.storage.azure_blob import AzureBlobStore

async def test_cancel_during_block_upload_closes_provider_source_without_committing():
    gate, started = asyncio.Event(), asyncio.Event()
    closed = False
    async def stage(*args):
        started.set()
        await gate.wait()
    shard = SimpleNamespace(_ensure_container=AsyncMock(), stage_block=stage,
        commit_block_list_manual=AsyncMock(), get_blob_properties=AsyncMock())
    store = AzureBlobStore('backend', 'test', [shard])
    async def source():
        nonlocal closed
        try:
            yield b'1234'
            await gate.wait()
        finally:
            closed = True
    stream = source()
    task = asyncio.create_task(store.upload_stream('c', 'p', stream, 8, chunk_size=4))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    try:
        assert closed
        shard.commit_block_list_manual.assert_not_called()
    finally:
        await stream.aclose()
