"""TDD — the storage-boundary seam.

  * seal(plaintext, dek, key_id) → ciphertext envelope
  * open_blob(data, dek) → plaintext; LEGACY plaintext (no envelope) passes
    through untouched so pre-encryption backups still read
  * verify_readback(downloaded, expected, dek) → the write-then-verify rail:
    after upload we download + decrypt + compare, and REFUSE to record an item
    whose stored bytes don't decrypt back to the original (raises).
"""
from __future__ import annotations

import os

import pytest

from shared.crypto import blob_cipher, envelope

DEK = b"\x66" * 32
KID = b"\x77" * 16


def test_seal_then_open_roundtrip():
    for data in [b"", b"hi", os.urandom(2048)]:
        ct = blob_cipher.seal(data, DEK, KID)
        assert envelope.is_encrypted(ct)
        assert blob_cipher.open_blob(ct, DEK) == data


def test_open_passes_through_legacy_plaintext():
    legacy = b"a pre-encryption backup blob"
    assert blob_cipher.open_blob(legacy, DEK) == legacy
    assert blob_cipher.open_blob(legacy, None) == legacy


def test_verify_readback_ok():
    data = os.urandom(500)
    ct = blob_cipher.seal(data, DEK, KID)
    blob_cipher.verify_readback(ct, data, DEK)  # must not raise


def test_verify_readback_detects_corruption():
    data = os.urandom(500)
    ct = bytearray(blob_cipher.seal(data, DEK, KID))
    ct[-1] ^= 0x01  # storage returned a corrupted byte
    with pytest.raises(blob_cipher.BlobCipherError):
        blob_cipher.verify_readback(bytes(ct), data, DEK)


def test_verify_readback_detects_wrong_plaintext():
    ct = blob_cipher.seal(b"real", DEK, KID)
    with pytest.raises(blob_cipher.BlobCipherError):
        blob_cipher.verify_readback(ct, b"different", DEK)
