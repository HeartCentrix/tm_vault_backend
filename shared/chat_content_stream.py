"""Spool chat content with bounded memory and explicit completeness errors."""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
import hashlib
import os
import tempfile


class ChatContentCaptureError(RuntimeError):
    """Required chat bytes could not be captured completely."""


@dataclass(frozen=True)
class SpooledChatContent:
    path: str
    size: int
    sha256: str


@asynccontextmanager
async def spool_chat_content(chunks, *, expected_size=None, max_bytes=None, directory=None):
    """Yield a private complete file; remove it and close the source on exit.

    The caller keeps upload/retry work inside this context. Neither a failed
    upload nor an interrupted download may publish this as durable content.
    Limits are actual byte limits; None means no configured limit.
    """
    for name, value in (("expected_size", expected_size), ("max_bytes", max_bytes)):
        if value is not None and (type(value) is not int or value < 0):
            raise ValueError(f"{name} must be a nonnegative integer or None")
    handle = tempfile.NamedTemporaryFile(prefix="chat-capture-", dir=directory, delete=False)
    size = 0
    digest = hashlib.sha256()
    try:
        async for chunk in chunks:
            if not isinstance(chunk, bytes):
                raise ChatContentCaptureError("content stream yielded non-byte data")
            size += len(chunk)
            if max_bytes is not None and size > max_bytes:
                raise ChatContentCaptureError("content exceeded the configured byte limit")
            if expected_size is not None and size > expected_size:
                raise ChatContentCaptureError("content size exceeded the expected size")
            if chunk:
                digest.update(chunk)
                await asyncio.to_thread(handle.write, chunk)
        if expected_size is not None and size != expected_size:
            raise ChatContentCaptureError("content size did not match the expected size")
        await asyncio.to_thread(handle.flush)
        yield SpooledChatContent(handle.name, size, digest.hexdigest())
    finally:
        try:
            close = getattr(chunks, "aclose", None)
            if close is not None:
                await close()
        finally:
            handle.close()
            try:
                os.unlink(handle.name)
            except FileNotFoundError:
                pass
