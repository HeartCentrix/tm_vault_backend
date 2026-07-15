"""AES-256-GCM chunked streaming frame — the crypto core of backup
encryption-at-rest.

A blob is encrypted as a self-describing frame so it can be streamed (GB-scale
OneDrive files) and verified without buffering the whole plaintext, while
staying tamper-, truncation- and reorder-evident.

Frame layout (all integers big-endian)::

    header (18 bytes):
        magic       4  b"TMV1"
        version     1  = 1
        flags       1  reserved, 0
        chunk_size  4  plaintext bytes per chunk
        nonce_pfx   8  random, per-frame
    then one or more chunks:
        ct_len      4  length of the following ciphertext (incl. 16-byte tag)
        ct        ct_len  AES-256-GCM(key, nonce, plaintext_chunk, aad)

Per chunk *i*:
    nonce = nonce_pfx (8) || i (4)                     -> 12-byte GCM nonce
    aad   = header || i (4) || final_flag (1)          binds position + finality

Because ``aad`` includes the chunk index and whether the chunk is the last one,
dropping, duplicating, or reordering chunks changes the AAD and fails GCM
authentication — so truncation and reorder attacks are detected, not silently
accepted. The per-frame random ``nonce_pfx`` makes ciphertext non-deterministic;
callers additionally use a unique per-blob key, so GCM nonce reuse across blobs
is impossible.

This module is pure (only ``os`` + ``cryptography``) so it is trivially
unit-testable and imports without any service/config side effects.
"""
from __future__ import annotations

import os
import struct
from typing import AsyncIterator

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

MAGIC = b"TMV1"
VERSION = 1
_HEADER_LEN = 18  # magic(4) + version(1) + flags(1) + chunk_size(4) + nonce_pfx(8)
_DEFAULT_CHUNK = 1 << 20  # 1 MiB
_TAG_LEN = 16
_MAX_CHUNKS = 1 << 32  # nonce counter is 4 bytes


class DecryptError(Exception):
    """Raised when a frame is malformed, tampered, truncated, or the key is
    wrong. Callers must treat this as 'cannot recover' — never fall back to
    returning the raw bytes."""


def is_encrypted(data: bytes) -> bool:
    """True if ``data`` is one of our frames (starts with the magic + a known
    version). Legacy plaintext blobs have no magic and read as False, so the
    read path can pass them through untouched."""
    return (
        len(data) >= _HEADER_LEN
        and data[:4] == MAGIC
        and data[4] == VERSION
    )


def _header(chunk_size: int, nonce_pfx: bytes) -> bytes:
    return MAGIC + bytes([VERSION, 0]) + struct.pack(">I", chunk_size) + nonce_pfx


def _aad(header: bytes, index: int, final: bool) -> bytes:
    return header + struct.pack(">I", index) + (b"\x01" if final else b"\x00")


def encrypt(plaintext: bytes, key: bytes, *, chunk_size: int = _DEFAULT_CHUNK) -> bytes:
    """Encrypt ``plaintext`` (bytes) into a frame. ``key`` must be 32 bytes
    (AES-256). Empty input produces a valid frame that decrypts back to b""."""
    if len(key) != 32:
        raise ValueError("key must be 32 bytes (AES-256)")
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")

    nonce_pfx = os.urandom(8)
    header = _header(chunk_size, nonce_pfx)
    aes = AESGCM(key)

    # Split into plaintext chunks; a single empty chunk for empty input so the
    # frame still round-trips to b"".
    chunks = [plaintext[i : i + chunk_size] for i in range(0, len(plaintext), chunk_size)] or [b""]
    if len(chunks) >= _MAX_CHUNKS:
        raise ValueError("plaintext too large for the 32-bit chunk counter")

    out = bytearray(header)
    last = len(chunks) - 1
    for i, chunk in enumerate(chunks):
        nonce = nonce_pfx + struct.pack(">I", i)
        ct = aes.encrypt(nonce, chunk, _aad(header, i, i == last))
        out += struct.pack(">I", len(ct)) + ct
    return bytes(out)


def _seal_chunk(aes: AESGCM, nonce_pfx: bytes, header: bytes, index: int, chunk: bytes, final: bool) -> bytes:
    nonce = nonce_pfx + struct.pack(">I", index)
    ct = aes.encrypt(nonce, chunk, _aad(header, index, final))
    return struct.pack(">I", len(ct)) + ct


def _open_chunk(aes: AESGCM, nonce_pfx: bytes, header: bytes, index: int, ct: bytes, final: bool) -> bytes:
    nonce = nonce_pfx + struct.pack(">I", index)
    try:
        return aes.decrypt(nonce, ct, _aad(header, index, final))
    except InvalidTag as e:
        raise DecryptError(f"authentication failed at chunk {index}") from e


async def encrypt_stream(
    plaintext: AsyncIterator[bytes], key: bytes, *, chunk_size: int = _DEFAULT_CHUNK
) -> AsyncIterator[bytes]:
    """Streaming form of :func:`encrypt` — yields the SAME bytes a single-shot
    call would, without ever holding the whole plaintext. Re-chunks the incoming
    (arbitrarily-sized) pieces into ``chunk_size`` plaintext chunks; a chunk is
    emitted only once we know another follows, so the final chunk is correctly
    marked ``final`` in its AAD. Empty input yields one empty final chunk, exactly
    like :func:`encrypt`."""
    if len(key) != 32:
        raise ValueError("key must be 32 bytes (AES-256)")
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")

    nonce_pfx = os.urandom(8)
    header = _header(chunk_size, nonce_pfx)
    yield header
    aes = AESGCM(key)

    buf = bytearray()
    index = 0
    # `held` is a full plaintext chunk we cannot emit yet (might be the last).
    held = None
    async for piece in plaintext:
        buf += piece
        while len(buf) > chunk_size:  # strict: >chunk_size guarantees more follows
            chunk = bytes(buf[:chunk_size])
            del buf[:chunk_size]
            if held is not None:
                yield _seal_chunk(aes, nonce_pfx, header, index, held, False)
                index += 1
            held = chunk
    # stream ended: the remainder in buf is the tail (0..chunk_size bytes)
    if held is not None:
        yield _seal_chunk(aes, nonce_pfx, header, index, held, False)
        index += 1
    yield _seal_chunk(aes, nonce_pfx, header, index, bytes(buf), True)


async def decrypt_stream(cipher: AsyncIterator[bytes], key: bytes) -> AsyncIterator[bytes]:
    """Streaming form of :func:`decrypt` — yields plaintext chunks as ciphertext
    chunks arrive, holding exactly one chunk back so the true final chunk gets
    ``final=True`` in its AAD (finality is only known when the stream ends).
    Raises :class:`DecryptError` on malformation, tamper, truncation, reorder, or
    wrong key — never yields unauthenticated plaintext."""
    if len(key) != 32:
        raise ValueError("key must be 32 bytes (AES-256)")

    ait = cipher.__aiter__()
    buf = bytearray()

    async def _fill(n: int) -> bool:
        while len(buf) < n:
            try:
                buf.extend(await ait.__anext__())
            except StopAsyncIteration:
                return False
        return True

    if not await _fill(_HEADER_LEN):
        raise DecryptError("truncated header")
    header = bytes(buf[:_HEADER_LEN])
    if header[:4] != MAGIC or header[4] != VERSION:
        raise DecryptError("not a TMV1 frame")
    nonce_pfx = header[10:18]
    del buf[:_HEADER_LEN]

    aes = AESGCM(key)
    index = 0
    held = None          # (index, ct) parsed but finality unknown
    while True:
        if not await _fill(4):
            break  # clean end at a chunk boundary
        (ct_len,) = struct.unpack(">I", buf[:4])
        if ct_len < _TAG_LEN:
            raise DecryptError("invalid chunk length")
        if not await _fill(4 + ct_len):
            raise DecryptError("truncated chunk")
        ct = bytes(buf[4 : 4 + ct_len])
        del buf[: 4 + ct_len]
        if held is not None:
            yield _open_chunk(aes, nonce_pfx, header, held[0], held[1], False)
        held = (index, ct)
        index += 1
    if held is None:
        raise DecryptError("no chunks")
    yield _open_chunk(aes, nonce_pfx, header, held[0], held[1], True)


def decrypt(frame: bytes, key: bytes) -> bytes:
    """Decrypt a frame produced by :func:`encrypt`. Raises :class:`DecryptError`
    on any malformation, tamper, truncation, reorder, or wrong key — never
    returns partial or unauthenticated plaintext."""
    if len(key) != 32:
        raise ValueError("key must be 32 bytes (AES-256)")
    if not is_encrypted(frame):
        raise DecryptError("not a TMV1 frame")

    header = frame[:_HEADER_LEN]
    nonce_pfx = header[10:18]
    aes = AESGCM(key)

    # Pre-scan the chunk boundaries so we know which chunk is genuinely last
    # (needed for the AAD final-flag). A malformed length or overrun => reject.
    spans = []  # (ct_start, ct_end)
    pos = _HEADER_LEN
    n = len(frame)
    while pos < n:
        if pos + 4 > n:
            raise DecryptError("truncated chunk length prefix")
        (ct_len,) = struct.unpack(">I", frame[pos : pos + 4])
        pos += 4
        if ct_len < _TAG_LEN or pos + ct_len > n:
            raise DecryptError("truncated or invalid chunk")
        spans.append((pos, pos + ct_len))
        pos += ct_len
    if not spans:
        raise DecryptError("no chunks")

    out = bytearray()
    last = len(spans) - 1
    for i, (start, end) in enumerate(spans):
        nonce = nonce_pfx + struct.pack(">I", i)
        try:
            out += aes.decrypt(nonce, frame[start:end], _aad(header, i, i == last))
        except InvalidTag as e:
            raise DecryptError(f"authentication failed at chunk {i}") from e
    return bytes(out)
