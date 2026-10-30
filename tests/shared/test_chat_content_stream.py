"""Streaming capture must not buffer whole files or hide incomplete bytes."""
import asyncio
import importlib
from pathlib import Path
import tracemalloc

import pytest


def api():
    return importlib.import_module("shared.chat_content_stream")


async def chunks(*values):
    for value in values:
        yield value


async def test_spool_records_exact_bytes_checksum_and_removes_owned_file(tmp_path):
    async with api().spool_chat_content(chunks(b"abc", b"", b"def"), expected_size=6,
                                        directory=tmp_path) as saved:
        path = Path(saved.path)
        assert path.read_bytes() == b"abcdef"
        assert saved.size == 6
        assert saved.sha256 == "bef57ec7f53a6d40beb640a780a639c83bc29ac8a9816f1fc6c5c6dcd93c4721"
        assert path.stat().st_mode & 0o077 == 0
    assert not path.exists()


@pytest.mark.parametrize("expected_size", [2, 9])
async def test_size_mismatch_never_returns_a_complete_file(tmp_path, expected_size):
    with pytest.raises(api().ChatContentCaptureError, match="size"):
        async with api().spool_chat_content(chunks(b"abc"), expected_size=expected_size,
                                            directory=tmp_path):
            pytest.fail("a size mismatch was accepted")
    assert list(tmp_path.iterdir()) == []


async def test_content_limit_is_an_explicit_failure_and_closes_source(tmp_path):
    closed = False

    async def source():
        nonlocal closed
        try:
            yield b"abc"
            yield b"def"
        finally:
            closed = True

    with pytest.raises(api().ChatContentCaptureError, match="limit"):
        async with api().spool_chat_content(source(), max_bytes=5, directory=tmp_path):
            pytest.fail("oversized content was accepted")
    assert closed
    assert list(tmp_path.iterdir()) == []


async def test_consumer_failure_preserves_error_and_cleans_spool(tmp_path):
    with pytest.raises(RuntimeError, match="upload failed"):
        async with api().spool_chat_content(chunks(b"abc"), directory=tmp_path):
            raise RuntimeError("upload failed")
    assert list(tmp_path.iterdir()) == []


async def test_source_failure_preserves_error_and_cleans_partial_spool(tmp_path):
    async def broken():
        yield b"abc"
        raise ConnectionError("interrupted download")

    with pytest.raises(ConnectionError, match="interrupted download"):
        async with api().spool_chat_content(broken(), directory=tmp_path):
            pytest.fail("interrupted download was accepted")
    assert list(tmp_path.iterdir()) == []


async def test_cancellation_closes_source_and_removes_partial_spool(tmp_path):
    entered = asyncio.Event()
    closed = asyncio.Event()

    async def source():
        try:
            yield b"first"
            entered.set()
            await asyncio.Event().wait()
        finally:
            closed.set()

    async def consume():
        async with api().spool_chat_content(source(), directory=tmp_path):
            pytest.fail("cancelled download completed")

    task = asyncio.create_task(consume())
    await asyncio.wait_for(entered.wait(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert closed.is_set()
    assert list(tmp_path.iterdir()) == []


async def test_large_stream_memory_does_not_grow_with_total_content(tmp_path):
    chunk = b"x" * (1024 * 1024)

    async def source():
        for _ in range(32):
            yield chunk

    tracemalloc.start()
    try:
        async with api().spool_chat_content(source(), expected_size=32 * len(chunk),
                                            directory=tmp_path) as saved:
            assert Path(saved.path).stat().st_size == 32 * len(chunk)
            assert tracemalloc.get_traced_memory()[1] < 8 * 1024 * 1024
    finally:
        tracemalloc.stop()
    assert list(tmp_path.iterdir()) == []


async def test_genuinely_empty_file_is_valid(tmp_path):
    async with api().spool_chat_content(chunks(), expected_size=0, max_bytes=0,
                                        directory=tmp_path) as saved:
        assert saved.size == 0
        assert saved.sha256 == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

