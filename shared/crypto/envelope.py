"""Envelope layer for backup blob encryption.

Owns per-blob key management so :mod:`shared.crypto.framer` stays a pure AEAD
primitive. A tenant holds one Data Encryption Key (DEK, 32 bytes); each blob is
sealed under a UNIQUE key derived from that DEK and a random per-blob salt::

    blob_key = HKDF-SHA256(dek, salt=<random 16B>, info="tmvault-blob-v1")

On-disk envelope layout::

    magic    4   b"TMVE"
    version  1   = 1
    key_id  16   which DEK/version sealed this blob (read path resolves it)
    salt    16   random HKDF salt for this blob
    frame   ...  framer.encrypt(plaintext, blob_key)

The salt is random and stored here — NOT the plaintext content hash, which would
be circular (you'd need the plaintext to derive the key that decrypts the
plaintext). Dedup is unaffected: callers hash the plaintext and only reach
encryption on a dedup miss. Tampering the salt or key_id yields a wrong derived
key, so the framer's GCM check fails — no separate authentication of this header
is needed.

Pure module (``os`` + ``cryptography`` + framer) — unit-testable, no service
imports. DEK resolution (which DEK for a key_id, and unwrapping it) lives one
layer up so this stays free of DB/KEK dependencies.
"""
from __future__ import annotations

import os
from typing import AsyncIterator, Awaitable, Callable

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

from . import framer

ENV_MAGIC = b"TMVE"
ENV_VERSION = 1
_KEY_ID_LEN = 16
_SALT_LEN = 16
_HEADER_LEN = 4 + 1 + _KEY_ID_LEN + _SALT_LEN  # 37
_INFO = b"tmvault-blob-v1"


class EnvelopeError(Exception):
    """Malformed envelope, wrong DEK, or tampered header/frame."""


def new_dek() -> bytes:
    """Mint a fresh 32-byte (AES-256) Data Encryption Key."""
    return os.urandom(32)


def _derive(dek: bytes, salt: bytes) -> bytes:
    if len(dek) != 32:
        raise ValueError("dek must be 32 bytes")
    return HKDF(algorithm=hashes.SHA256(), length=32, salt=salt, info=_INFO).derive(dek)


def is_encrypted(data: bytes) -> bool:
    """True if ``data`` is a TMVE envelope. Legacy plaintext (and bare framer
    frames) read as False so the read path passes them through."""
    return len(data) >= _HEADER_LEN and data[:4] == ENV_MAGIC and data[4] == ENV_VERSION


def read_key_id(data: bytes) -> bytes:
    """The 16-byte DEK id that sealed this blob, for DEK resolution."""
    if not is_encrypted(data):
        raise EnvelopeError("not a TMVE envelope")
    return data[5 : 5 + _KEY_ID_LEN]


def encrypt_blob(plaintext: bytes, dek: bytes, key_id: bytes, *, chunk_size: int = 1 << 20) -> bytes:
    """Seal ``plaintext`` under a per-blob key derived from ``dek``. ``key_id``
    (16 bytes) records which DEK version this is, so the read path can resolve
    the right DEK later."""
    if len(key_id) != _KEY_ID_LEN:
        raise ValueError("key_id must be 16 bytes")
    salt = os.urandom(_SALT_LEN)
    blob_key = _derive(dek, salt)
    frame = framer.encrypt(plaintext, blob_key, chunk_size=chunk_size)
    return ENV_MAGIC + bytes([ENV_VERSION]) + key_id + salt + frame


def decrypt_blob(data: bytes, dek: bytes) -> bytes:
    """Open a TMVE envelope with the DEK its ``key_id`` refers to. Raises on any
    malformation, wrong DEK, or tamper — never returns partial plaintext."""
    if not is_encrypted(data):
        raise EnvelopeError("not a TMVE envelope")
    salt = data[5 + _KEY_ID_LEN : _HEADER_LEN]
    blob_key = _derive(dek, salt)
    try:
        return framer.decrypt(data[_HEADER_LEN:], blob_key)
    except framer.DecryptError as e:
        raise EnvelopeError(str(e)) from e


async def encrypt_stream(
    plaintext: AsyncIterator[bytes], dek: bytes, key_id: bytes, *, chunk_size: int = 1 << 20
) -> AsyncIterator[bytes]:
    """Streaming form of :func:`encrypt_blob` — yields envelope-framed ciphertext
    for a plaintext that is never held whole (GB-scale OneDrive/SQL blobs). The
    output is byte-identical to :func:`encrypt_blob`, so a stream-written blob
    opens with the single-shot :func:`decrypt_blob`."""
    if len(key_id) != _KEY_ID_LEN:
        raise ValueError("key_id must be 16 bytes")
    salt = os.urandom(_SALT_LEN)
    blob_key = _derive(dek, salt)
    yield ENV_MAGIC + bytes([ENV_VERSION]) + key_id + salt
    async for piece in framer.encrypt_stream(plaintext, blob_key, chunk_size=chunk_size):
        yield piece


async def decrypt_stream(
    cipher: AsyncIterator[bytes], resolve: Callable[[bytes], Awaitable[bytes]]
) -> AsyncIterator[bytes]:
    """Streaming form of :func:`decrypt_blob`. Reads the envelope header, resolves
    the DEK for its ``key_id`` via the async ``resolve`` callable, then streams the
    framer chunks as plaintext. Raises :class:`EnvelopeError` on any malformation,
    wrong DEK, or tamper."""
    ait = cipher.__aiter__()
    buf = bytearray()
    while len(buf) < _HEADER_LEN:
        try:
            buf.extend(await ait.__anext__())
        except StopAsyncIteration:
            break
    if not is_encrypted(bytes(buf)):
        raise EnvelopeError("not a TMVE envelope")
    key_id = bytes(buf[5 : 5 + _KEY_ID_LEN])
    salt = bytes(buf[5 + _KEY_ID_LEN : _HEADER_LEN])
    dek = await resolve(key_id)
    blob_key = _derive(dek, salt)
    rest = bytes(buf[_HEADER_LEN:])

    async def _frame_bytes():
        if rest:
            yield rest
        async for piece in ait:
            yield piece

    try:
        async for pt in framer.decrypt_stream(_frame_bytes(), blob_key):
            yield pt
    except framer.DecryptError as e:
        raise EnvelopeError(str(e)) from e
