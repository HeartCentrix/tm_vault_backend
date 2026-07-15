"""TDD — the streaming read seam: runtime.decrypt_stream.

The single-shot download() path already auto-decrypts, but the STREAMING path
(download_stream → mail restore, mail/chat export, ZIP export) yielded raw
ciphertext for TMVE-framed blobs. This is the gap that kept encryption OFF.

decrypt_stream(raw_iter, chunk_size) consumes raw S3 chunks and yields PLAINTEXT:
  * TMVE-framed blob  -> reassemble + decrypt, re-chunk to chunk_size
  * legacy plaintext  -> pass straight through (re-chunked), zero migration
Reassembly must survive arbitrary S3 chunk boundaries (the framer's internal
chunks do NOT align with the 4 MiB read size).
"""
from __future__ import annotations

from typing import AsyncIterator, Dict, Optional

import pytest

from shared.crypto import blob_cipher, kek, runtime
from shared.crypto.dek_manager import DekManager, DekRecord, DekStore

MASTER = b"\x55" * 32


class MemStore(DekStore):
    def __init__(self):
        self.by_kid: Dict[bytes, DekRecord] = {}
        self.active: Dict[str, bytes] = {}

    async def get_active(self, tenant_id):
        kid = self.active.get(tenant_id)
        return self.by_kid.get(kid) if kid else None

    async def get_by_key_id(self, key_id):
        return self.by_kid.get(key_id)

    async def insert(self, rec):
        self.by_kid[rec.key_id] = rec
        self.active[rec.tenant_id] = rec.key_id


async def _feed(data: bytes, size: int) -> AsyncIterator[bytes]:
    for i in range(0, len(data), size):
        yield data[i : i + size]


async def _collect(aiter) -> bytes:
    out = bytearray()
    async for c in aiter:
        out += c
    return bytes(out)


@pytest.fixture
def mgr():
    m = DekManager(kek.EnvKek(MASTER, kek_id="e1"), MemStore())
    runtime.configure(m, write_enabled=True, verify_readback=False)
    yield m
    runtime.configure(None)


async def _seal(mgr, plaintext: bytes) -> bytes:
    key_id, dek = await mgr.active_dek("t1")
    return blob_cipher.seal(plaintext, dek, key_id)


@pytest.mark.parametrize(
    "plaintext",
    [b"", b"hi", b"A" * 2_100_000],  # last one > framer's 1 MiB chunk -> 3 GCM chunks
    ids=["empty", "tiny", "multi_frame_chunk"],
)
@pytest.mark.parametrize("feed_size", [3, 64, 4 * 1024 * 1024], ids=["drib", "small", "s3size"])
async def test_encrypted_roundtrips_across_arbitrary_chunk_boundaries(mgr, plaintext, feed_size):
    envelope_bytes = await _seal(mgr, plaintext)
    out = await _collect(runtime.decrypt_stream(_feed(envelope_bytes, feed_size), 4 * 1024 * 1024))
    assert out == plaintext


async def test_legacy_plaintext_passes_through(mgr):
    raw = b"legacy-not-a-frame-" + b"x" * 100_000
    out = await _collect(runtime.decrypt_stream(_feed(raw, 7), 4096))
    assert out == raw


async def test_re_chunks_to_requested_size(mgr):
    plaintext = b"Z" * 10_000
    envelope_bytes = await _seal(mgr, plaintext)
    chunks = [c async for c in runtime.decrypt_stream(_feed(envelope_bytes, 4096), 1024)]
    assert b"".join(chunks) == plaintext
    assert all(len(c) <= 1024 for c in chunks)
    assert len(chunks) == 10  # 10_000 / 1024 -> 10 chunks


async def test_empty_input_yields_nothing(mgr):
    out = await _collect(runtime.decrypt_stream(_feed(b"", 4096), 4096))
    assert out == b""
