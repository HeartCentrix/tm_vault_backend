"""Storage-boundary seam: the thin layer the object-store put/get paths call.

Keeps the crypto decision in one place:
  * ``seal`` encrypts a plaintext blob into an envelope (given the tenant DEK).
  * ``open_blob`` decrypts an envelope, and passes LEGACY plaintext blobs (no
    envelope magic) straight through — so backups written before encryption was
    enabled keep reading with zero migration.
  * ``verify_readback`` is the write-then-verify rail: after uploading, the
    caller downloads the bytes back and calls this to prove they decrypt to the
    original. An item is only recorded once this passes — we never persist a
    blob we can't get back.
"""
from __future__ import annotations

from typing import Optional

from . import envelope


class BlobCipherError(Exception):
    """Write-then-verify failed — the stored bytes did not decrypt back to the
    original plaintext. Caller must abort (do not record the item)."""


def seal(plaintext: bytes, dek: bytes, key_id: bytes) -> bytes:
    """Encrypt ``plaintext`` into a TMVE envelope under the tenant DEK."""
    return envelope.encrypt_blob(plaintext, dek, key_id)


def open_blob(data: bytes, dek: Optional[bytes]) -> bytes:
    """Decrypt an envelope, or return legacy plaintext unchanged. ``dek`` may be
    ``None`` for the legacy path (nothing to decrypt)."""
    if not envelope.is_encrypted(data):
        return data
    if dek is None:
        raise BlobCipherError("encrypted blob but no DEK provided")
    return envelope.decrypt_blob(data, dek)


def verify_readback(downloaded: bytes, expected_plaintext: bytes, dek: bytes) -> None:
    """Prove the just-uploaded bytes decrypt back to ``expected_plaintext``.
    Raises :class:`BlobCipherError` on any mismatch, corruption, or truncation."""
    try:
        recovered = envelope.decrypt_blob(downloaded, dek)
    except Exception as e:  # envelope/framer auth failure, truncation, etc.
        raise BlobCipherError(f"read-back did not decrypt: {e}") from e
    if recovered != expected_plaintext:
        raise BlobCipherError("read-back plaintext mismatch")
