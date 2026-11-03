"""Threaded file scopes that retain ownership until cancelled I/O finishes."""
import asyncio
from contextlib import asynccontextmanager

from shared.storage.file_upload import finish_io


@asynccontextmanager
async def open_spool_file(path, mode):
    opening = asyncio.create_task(asyncio.to_thread(open, path, mode))
    try:
        file = await finish_io(opening)
    except BaseException:
        # Cancellation can arrive after open succeeds; its handle still needs closing.
        if not opening.cancelled() and opening.exception() is None:
            await finish_io(asyncio.to_thread(opening.result().close))
        raise
    try:
        yield file
    finally:
        await finish_io(asyncio.to_thread(file.close))
