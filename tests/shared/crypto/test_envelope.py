"""TDD — envelope layer: per-blob key derivation + key_id/salt header around a
framer frame.

The envelope owns key management so the framer stays a pure AEAD primitive:
  * new_dek() mints a per-tenant 32-byte data key
  * encrypt_blob derives a UNIQUE per-blob key = HKDF(dek, random salt) and
    writes [ENV magic | version | key_id | salt | framer-frame]
  * the salt is random and STORED in the header (not the content hash — that
    would be circular: you'd need the plaintext to derive the key to get the
    plaintext), so decrypt is self-contained given the right DEK
  * key_id lets the read path pick the right DEK version without trial
"""
from __future__ import annotations

import os

import pytest

from shared.crypto import envelope

KID = b"\xab" * 16


def test_new_dek_is_32_random_bytes():
    a, b = envelope.new_dek(), envelope.new_dek()
    assert len(a) == 32 and len(b) == 32
    assert a != b


def test_roundtrip():
    dek = envelope.new_dek()
    for data in [b"", b"hi", os.urandom(3000)]:
        blob = envelope.encrypt_blob(data, dek, KID, chunk_size=64)
        assert envelope.decrypt_blob(blob, dek) == data


def test_is_encrypted_and_key_id_readable():
    dek = envelope.new_dek()
    blob = envelope.encrypt_blob(b"payload", dek, KID)
    assert envelope.is_encrypted(blob)
    assert not envelope.is_encrypted(b"legacy plaintext")
    assert not envelope.is_encrypted(b"")
    assert envelope.read_key_id(blob) == KID


def test_random_salt_makes_ciphertext_nondeterministic():
    dek = envelope.new_dek()
    a = envelope.encrypt_blob(b"same", dek, KID)
    b = envelope.encrypt_blob(b"same", dek, KID)
    assert a != b
    assert envelope.decrypt_blob(a, dek) == envelope.decrypt_blob(b, dek) == b"same"


def test_wrong_dek_fails():
    blob = envelope.encrypt_blob(b"payload", envelope.new_dek(), KID)
    with pytest.raises(Exception):
        envelope.decrypt_blob(blob, envelope.new_dek())


def test_tampered_salt_fails():
    dek = envelope.new_dek()
    blob = bytearray(envelope.encrypt_blob(b"payload", dek, KID))
    # salt sits right after magic(4)+version(1)+key_id(16) = offset 21
    blob[22] ^= 0x01
    with pytest.raises(Exception):
        envelope.decrypt_blob(bytes(blob), dek)


def test_key_id_must_be_16_bytes():
    with pytest.raises(ValueError):
        envelope.encrypt_blob(b"x", envelope.new_dek(), b"short")
