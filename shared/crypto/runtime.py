"""Process-global encryption hook the storage layer calls.

Keeps ``SeaweedStore`` decoupled from DB/KEK construction: a worker installs a
:class:`~shared.crypto.dek_manager.DekManager` at startup via :func:`configure`;
until then everything is a no-op passthrough, so encryption is OFF by default and
current backups are unaffected.

Split of concerns:
  * ``write_enabled`` gates WRITES (whether new blobs get encrypted).
  * READS always attempt decryption when a blob is TMVE-framed — regardless of
    ``write_enabled`` — so already-encrypted blobs keep opening even if writes
    are later turned off. That requires the DEK manager to stay installed.
"""
from __future__ import annotations

from typing import AsyncIterator, Optional, Tuple

from . import blob_cipher, envelope
from .dek_manager import DekManager

# Enough bytes to recognise a TMVE envelope header (magic + version + key_id +
# salt); the streaming read path buffers at least this much before deciding
# whether a blob is framed.
_PEEK = envelope._HEADER_LEN

_dek_manager: Optional[DekManager] = None
_write_enabled: bool = False
_verify_readback: bool = True


def configure(dek_manager: Optional[DekManager], *, write_enabled: bool = False, verify_readback: bool = True) -> None:
    """Install (or clear) the DEK manager and flags. Call once at worker start."""
    global _dek_manager, _write_enabled, _verify_readback
    _dek_manager = dek_manager
    _write_enabled = bool(write_enabled)
    _verify_readback = bool(verify_readback)


def is_write_enabled() -> bool:
    return _write_enabled and _dek_manager is not None


def verify_readback_enabled() -> bool:
    return _verify_readback


async def maybe_encrypt(content: bytes, tenant_id: Optional[str]) -> Tuple[bytes, bool]:
    """Return ``(bytes_to_store, was_encrypted)``. No-op unless writes are
    enabled, a DEK manager is installed, and a tenant_id is known."""
    if not is_write_enabled() or tenant_id is None:
        return content, False
    key_id, dek = await _dek_manager.active_dek(tenant_id)  # type: ignore[union-attr]
    return blob_cipher.seal(content, dek, key_id), True


async def maybe_decrypt(data: Optional[bytes]) -> Optional[bytes]:
    """Decrypt a TMVE-framed blob (key_id resolves the DEK); pass legacy
    plaintext through unchanged. Raises if a framed blob is seen but no DEK
    manager is installed (can't recover — better than returning ciphertext)."""
    if data is None or not envelope.is_encrypted(data):
        return data
    if _dek_manager is None:
        raise blob_cipher.BlobCipherError("encrypted blob but no DEK manager configured")
    dek = await _dek_manager.dek_for_key_id(envelope.read_key_id(data))
    return blob_cipher.open_blob(data, dek)


async def _rechunk(src: AsyncIterator[bytes], chunk_size: int) -> AsyncIterator[bytes]:
    """Yield ``src`` re-packed into <= chunk_size pieces (never buffers more than
    one chunk)."""
    buf = bytearray()
    async for piece in src:
        buf += piece
        while len(buf) >= chunk_size:
            yield bytes(buf[:chunk_size])
            del buf[:chunk_size]
    if buf:
        yield bytes(buf)


async def maybe_encrypt_stream(
    plaintext: AsyncIterator[bytes], tenant_id: Optional[str], *, chunk_size: int = 1 << 20
) -> Tuple[AsyncIterator[bytes], bool]:
    """Return ``(byte_stream, will_encrypt)`` for the STREAMING write path (large
    OneDrive/SQL blobs). When writes are enabled and a tenant is known, the stream
    is TMVE-framed on the fly (no full buffering); otherwise the original stream is
    returned unchanged. Byte-identical to the single-shot seal, so a stream-written
    blob opens with the single-shot read path and vice-versa."""
    if not is_write_enabled() or tenant_id is None:
        return plaintext, False
    key_id, dek = await _dek_manager.active_dek(tenant_id)  # type: ignore[union-attr]
    return envelope.encrypt_stream(plaintext, dek, key_id, chunk_size=chunk_size), True


async def decrypt_stream(raw: AsyncIterator[bytes], chunk_size: int) -> AsyncIterator[bytes]:
    """Wrap a raw object-store byte stream so callers always see PLAINTEXT.

    Legacy (unframed) blobs pass straight through, re-chunked to ``chunk_size``.
    TMVE-framed blobs are decrypted INCREMENTALLY (via the streaming framer) so
    GB-scale blobs are never buffered whole — the key_id in the envelope header
    resolves the DEK. Raises if a framed blob is seen with no DEK manager
    installed (cannot recover — better than emitting ciphertext)."""
    ait = raw.__aiter__()
    head = bytearray()
    while len(head) < _PEEK:
        try:
            head.extend(await ait.__anext__())
        except StopAsyncIteration:
            break

    async def _restream() -> AsyncIterator[bytes]:
        if head:
            yield bytes(head)
        async for piece in ait:
            yield piece

    if not envelope.is_encrypted(bytes(head)):
        async for chunk in _rechunk(_restream(), chunk_size):
            yield chunk
        return

    if _dek_manager is None:
        raise blob_cipher.BlobCipherError("encrypted blob but no DEK manager configured")
    plaintext = envelope.decrypt_stream(_restream(), _dek_manager.dek_for_key_id)
    async for chunk in _rechunk(plaintext, chunk_size):
        yield chunk
