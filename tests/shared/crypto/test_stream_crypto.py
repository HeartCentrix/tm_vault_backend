"""TDD — STREAMING envelope encrypt/decrypt (large-file path, no full-buffer).

OneDrive/SQL backups stream GB-scale bytes and must never be buffered whole.
These add:
  * envelope.encrypt_stream(plaintext_aiter, dek, key_id) -> ciphertext chunks
  * envelope.decrypt_stream(cipher_aiter, resolve) -> plaintext chunks
built on framer.encrypt_stream / framer.decrypt_stream.

INTEROP IS THE HARD REQUIREMENT: the stream format must be byte-identical to the
single-shot framer, so a blob written one way opens the other way. A blob sealed
today by single-shot upload() must decrypt via the streaming read path, and a
big blob sealed by the streaming upload path must decrypt via single-shot
download(). Both directions are tested here.
"""
from __future__ import annotations

import os
from typing import AsyncIterator

import pytest

from shared.crypto import envelope, framer

DEK = b"\x33" * 32
KID = b"\x44" * 16
BLOB_KEY = b"\x99" * 32


async def _feed(data: bytes, size: int) -> AsyncIterator[bytes]:
    for i in range(0, len(data), size):
        yield data[i : i + size]
    if not data:
        return


async def _collect(aiter) -> bytes:
    out = bytearray()
    async for c in aiter:
        out += c
    return bytes(out)


async def _resolve(_key_id):
    return DEK


SIZES = [0, 1, 100, (1 << 20), (1 << 20) + 7, 3 * (1 << 20), 5_000_000]
FEEDS = [1, 4096, 1 << 20, 8 * (1 << 20)]


# ---- framer-level streaming: identical bytes to single-shot ------------------

@pytest.mark.parametrize("n", SIZES)
async def test_framer_stream_encrypt_matches_single_shot(n):
    pt = os.urandom(n)
    streamed = await _collect(framer.encrypt_stream(_feed(pt, 4096), BLOB_KEY, chunk_size=1 << 20))
    # streamed frame decrypts with the plain single-shot decryptor
    assert framer.decrypt(streamed, BLOB_KEY) == pt


@pytest.mark.parametrize("n", SIZES)
async def test_framer_single_shot_decrypts_via_stream(n):
    pt = os.urandom(n)
    frame = framer.encrypt(pt, BLOB_KEY, chunk_size=1 << 20)
    assert await _collect(framer.decrypt_stream(_feed(frame, 3), BLOB_KEY)) == pt


# ---- envelope-level streaming ------------------------------------------------

@pytest.mark.parametrize("n", SIZES)
@pytest.mark.parametrize("feed", FEEDS)
async def test_envelope_stream_roundtrip(n, feed):
    pt = os.urandom(n)
    ct = await _collect(envelope.encrypt_stream(_feed(pt, feed), DEK, KID, chunk_size=1 << 20))
    assert envelope.is_encrypted(ct)
    assert envelope.read_key_id(ct) == KID
    got = await _collect(envelope.decrypt_stream(_feed(ct, feed), _resolve))
    assert got == pt


@pytest.mark.parametrize("n", [0, 100, (1 << 20) + 5, 4_000_000])
async def test_stream_written_opens_single_shot(n):
    """Big blob written by the streaming path decrypts via single-shot download."""
    pt = os.urandom(n)
    ct = await _collect(envelope.encrypt_stream(_feed(pt, 64 * 1024), DEK, KID, chunk_size=1 << 20))
    assert envelope.decrypt_blob(ct, DEK) == pt


@pytest.mark.parametrize("n", [0, 100, (1 << 20) + 5, 4_000_000])
async def test_single_shot_written_opens_stream(n):
    """Blob written single-shot decrypts via the streaming read path."""
    pt = os.urandom(n)
    ct = envelope.encrypt_blob(pt, DEK, KID, chunk_size=1 << 20)
    assert await _collect(envelope.decrypt_stream(_feed(ct, 7), _resolve)) == pt


# ---- tamper / truncation must fail authentication ----------------------------

async def test_stream_decrypt_rejects_tamper():
    pt = os.urandom(2_000_000)
    ct = bytearray(await _collect(envelope.encrypt_stream(_feed(pt, 65536), DEK, KID, chunk_size=1 << 20)))
    ct[-1] ^= 0x01  # flip a byte in the last chunk's tag
    with pytest.raises(Exception):
        await _collect(envelope.decrypt_stream(_feed(bytes(ct), 4096), _resolve))


async def test_stream_decrypt_rejects_truncation():
    pt = os.urandom(3 * (1 << 20))  # 3 framer chunks
    ct = await _collect(envelope.encrypt_stream(_feed(pt, 65536), DEK, KID, chunk_size=1 << 20))
    # drop the final chunk -> the new "last" chunk was authenticated as non-final
    truncated = ct[: len(ct) // 2]
    with pytest.raises(Exception):
        await _collect(envelope.decrypt_stream(_feed(truncated, 4096), _resolve))
