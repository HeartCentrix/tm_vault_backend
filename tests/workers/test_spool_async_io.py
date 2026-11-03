"""Disk spooling must yield the event loop and retain ownership on cancellation."""
import asyncio
import builtins
import hashlib
import threading

import httpx
import pytest

from .test_provider_file_streams import worker


async def test_download_spool_opens_and_writes_off_event_loop(worker, monkeypatch, tmp_path):
    obj, transport = worker
    monkeypatch.setattr("tempfile.tempdir", str(tmp_path))
    transport(lambda request: httpx.Response(200, content=b"abc"))
    loop_thread = threading.get_ident()
    real_open = builtins.open
    operations = []

    class CheckedFile:
        def __init__(self, file):
            self.file = file

        def write(self, data):
            assert threading.get_ident() != loop_thread
            operations.append("write")
            return self.file.write(data)

        def close(self):
            assert threading.get_ident() != loop_thread
            operations.append("close")
            self.file.close()

    def checked_open(path, mode="r", *args, **kwargs):
        if str(path).startswith(str(tmp_path)):
            assert threading.get_ident() != loop_thread
            operations.append("open")
            return CheckedFile(real_open(path, mode, *args, **kwargs))
        return real_open(path, mode, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", checked_open)
    path, digest = await obj._download_to_temp_resumable(
        "https://tenant.sharepoint.com/file", 3, "name", chunk_size=1,
    )
    assert digest == hashlib.sha256(b"abc").hexdigest()
    assert operations == ["open", "write", "write", "write", "close"]
    with real_open(path, "rb") as captured:
        assert captured.read() == b"abc"


async def test_spool_cancellation_joins_pending_write_before_close(worker, monkeypatch, tmp_path):
    from shared.provider_runtime import execution_budget

    obj, transport = worker
    monkeypatch.setattr("tempfile.tempdir", str(tmp_path))
    transport(lambda request: httpx.Response(200, content=b"abc"))
    loop = asyncio.get_running_loop()
    entered = asyncio.Event()
    release = threading.Event()
    operations = []
    real_open = builtins.open

    class SlowFile:
        def __init__(self, file):
            self.file = file

        def write(self, data):
            loop.call_soon_threadsafe(entered.set)
            assert release.wait(5), "test did not release owned disk write"
            operations.append("write")
            return self.file.write(data)

        def close(self):
            operations.append("close")
            self.file.close()

    def slow_open(path, mode="r", *args, **kwargs):
        file = real_open(path, mode, *args, **kwargs)
        return SlowFile(file) if str(path).startswith(str(tmp_path)) else file

    monkeypatch.setattr(builtins, "open", slow_open)
    task = asyncio.create_task(obj._download_to_temp_resumable(
        "https://tenant.sharepoint.com/file", 3, "name",
    ))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        assert execution_budget().used_bytes > 0
        assert list(tmp_path.iterdir())
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert operations == ["write", "close"]
    assert execution_budget().used_bytes == 0
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("blocked_operation", ["open", "close"])
async def test_spool_cancellation_joins_pending_open_or_close(
    worker, monkeypatch, tmp_path, blocked_operation,
):
    from shared.provider_runtime import execution_budget

    obj, transport = worker
    monkeypatch.setattr("tempfile.tempdir", str(tmp_path))
    transport(lambda request: httpx.Response(200, content=b"abc"))
    loop = asyncio.get_running_loop()
    entered, finished = asyncio.Event(), asyncio.Event()
    release = threading.Event()
    files = []
    real_open = builtins.open

    def wait_for_release():
        loop.call_soon_threadsafe(entered.set)
        assert release.wait(5), "test did not release owned disk operation"

    class SlowCloseFile:
        def __init__(self, file):
            self.file = file

        def write(self, data):
            return self.file.write(data)

        def close(self):
            try:
                wait_for_release()
                self.file.close()
            finally:
                loop.call_soon_threadsafe(finished.set)

    def controlled_open(path, mode="r", *args, **kwargs):
        file = real_open(path, mode, *args, **kwargs)
        if not str(path).startswith(str(tmp_path)):
            return file
        files.append(file)
        if blocked_operation == "open":
            try:
                wait_for_release()
            finally:
                loop.call_soon_threadsafe(finished.set)
            return file
        return SlowCloseFile(file)

    monkeypatch.setattr(builtins, "open", controlled_open)
    task = asyncio.create_task(obj._download_to_temp_resumable(
        "https://tenant.sharepoint.com/file", 3, "name",
    ))
    try:
        try:
            await asyncio.wait_for(entered.wait(), 3)
            for _ in range(2):
                task.cancel()
                turn = loop.create_future()
                loop.call_soon(turn.set_result, None)
                await turn
            assert not task.done()
            assert execution_budget().used_bytes > 0
            assert list(tmp_path.iterdir())
            assert not files[0].closed
        finally:
            release.set()
            try:
                with pytest.raises(asyncio.CancelledError):
                    await task
            finally:
                await asyncio.wait_for(finished.wait(), 3)
        assert len(files) == 1
        assert files[0].closed
        assert execution_budget().used_bytes == 0
        assert not list(tmp_path.iterdir())
    finally:
        for file in files:
            file.close()
