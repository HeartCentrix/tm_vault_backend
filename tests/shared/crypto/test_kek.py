"""TDD — KEK providers that wrap/unwrap the per-tenant DEK.

Three interchangeable providers (client picks one from the UI):
  * EnvKek        — AES Key Wrap (RFC 3394) under a 32-byte master key (env)
  * AwsKmsKek     — AWS KMS Encrypt/Decrypt
  * AzureKeyVaultKek — Azure Key Vault wrapKey/unwrapKey

EnvKek is tested end-to-end (real crypto). AWS/Azure are tested with injected
fake clients so the wrap/unwrap call wiring is verified without live endpoints.
"""
from __future__ import annotations

import pytest

from shared.crypto import kek

DEK = b"\x33" * 32
MASTER = b"\x44" * 32


# ── EnvKek: real AES-KW round-trip ──────────────────────────────────────────
def test_envkek_roundtrip():
    p = kek.EnvKek(MASTER, kek_id="env-1")
    wrapped = p.wrap_dek(DEK)
    assert wrapped != DEK
    assert p.unwrap_dek(wrapped) == DEK
    assert p.kek_id == "env-1"
    assert p.provider_name == "env"


def test_envkek_wrong_master_fails():
    wrapped = kek.EnvKek(MASTER, kek_id="k").wrap_dek(DEK)
    with pytest.raises(Exception):
        kek.EnvKek(b"\x99" * 32, kek_id="k").unwrap_dek(wrapped)


def test_envkek_master_must_be_32():
    with pytest.raises(ValueError):
        kek.EnvKek(b"short", kek_id="k")


# ── AwsKmsKek: fake KMS client ──────────────────────────────────────────────
class _FakeKms:
    def encrypt(self, KeyId, Plaintext):
        return {"CiphertextBlob": b"aws|" + KeyId.encode() + b"|" + Plaintext[::-1]}

    def decrypt(self, CiphertextBlob):
        _tag, _kid, body = CiphertextBlob.split(b"|", 2)
        return {"Plaintext": body[::-1]}


def test_awskms_roundtrip_via_fake_client():
    p = kek.AwsKmsKek("arn:aws:kms:...:key/abc", client=_FakeKms())
    wrapped = p.wrap_dek(DEK)
    assert wrapped != DEK
    assert p.unwrap_dek(wrapped) == DEK
    assert p.provider_name == "aws_kms"
    assert "abc" in p.kek_id


# ── AzureKeyVaultKek: fake CryptographyClient ───────────────────────────────
class _Res:
    def __init__(self, **kw):
        self.__dict__.update(kw)


class _FakeKvCrypto:
    def wrap_key(self, algorithm, key):
        return _Res(encrypted_key=b"az|" + key[::-1])

    def unwrap_key(self, algorithm, encrypted_key):
        return _Res(key=encrypted_key[3:][::-1])


def test_azure_kv_roundtrip_via_fake_client():
    p = kek.AzureKeyVaultKek("https://vault/keys/k/ver", client=_FakeKvCrypto())
    wrapped = p.wrap_dek(DEK)
    assert wrapped != DEK
    assert p.unwrap_dek(wrapped) == DEK
    assert p.provider_name == "azure_key_vault"


# ── Factory ─────────────────────────────────────────────────────────────────
def test_factory_selects_provider():
    p = kek.build_kek({"provider": "env", "master_key_hex": MASTER.hex(), "kek_id": "e"})
    assert isinstance(p, kek.EnvKek)
    assert p.unwrap_dek(p.wrap_dek(DEK)) == DEK


def test_factory_rejects_unknown_provider():
    with pytest.raises(ValueError):
        kek.build_kek({"provider": "nope"})
