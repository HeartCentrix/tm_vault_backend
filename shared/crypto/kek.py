"""Key-Encryption-Key (KEK) providers — wrap/unwrap the per-tenant DEK.

Envelope encryption keeps the DEK in the DB only in **wrapped** form; the KEK
that wraps it lives outside the app (KMS/Key Vault) or in an env master key.
Three interchangeable providers, chosen from the UI because the client's
capability (on-prem vs AWS vs Azure) isn't known up front:

  * :class:`EnvKek`          — AES Key Wrap (RFC 3394) under a 32-byte env key
  * :class:`AwsKmsKek`       — AWS KMS Encrypt/Decrypt
  * :class:`AzureKeyVaultKek`— Azure Key Vault wrapKey/unwrapKey (RSA-OAEP-256)

All expose the same tiny contract so callers are provider-agnostic. Rotating the
KEK only re-wraps DEKs (cheap); blob ciphertext is never touched. Cloud clients
are injectable so the wiring is unit-testable without live endpoints.
"""
from __future__ import annotations

import abc
from typing import Any, Dict, Optional

from cryptography.hazmat.primitives.keywrap import aes_key_unwrap, aes_key_wrap


class KekProvider(abc.ABC):
    """Wrap/unwrap a DEK. Implementations must be deterministic enough that a
    DEK wrapped now unwraps to the same bytes later (across restarts)."""

    provider_name: str = "abstract"

    @property
    @abc.abstractmethod
    def kek_id(self) -> str:
        """Stable identifier of the wrapping key (stored beside the DEK)."""

    @abc.abstractmethod
    def wrap_dek(self, dek: bytes) -> bytes:
        """Return the wrapped (encrypted) DEK."""

    @abc.abstractmethod
    def unwrap_dek(self, wrapped: bytes) -> bytes:
        """Return the plaintext DEK from its wrapped form."""


class EnvKek(KekProvider):
    """AES Key Wrap under a 32-byte master key supplied via the environment.
    Simplest real option — protects the DB dump / object store against theft.
    Migrating to a cloud KEK later only re-wraps DEKs."""

    provider_name = "env"

    def __init__(self, master_key: bytes, *, kek_id: str = "env"):
        if len(master_key) != 32:
            raise ValueError("master_key must be 32 bytes (AES-256)")
        self._mk = master_key
        self._kek_id = kek_id

    @property
    def kek_id(self) -> str:
        return self._kek_id

    def wrap_dek(self, dek: bytes) -> bytes:
        return aes_key_wrap(self._mk, dek)

    def unwrap_dek(self, wrapped: bytes) -> bytes:
        return aes_key_unwrap(self._mk, wrapped)


class AwsKmsKek(KekProvider):
    """AWS KMS. ``wrap`` = kms:Encrypt, ``unwrap`` = kms:Decrypt (symmetric CMK).
    The KEK never leaves KMS."""

    provider_name = "aws_kms"

    def __init__(self, key_id: str, *, client: Optional[Any] = None, region_name: Optional[str] = None):
        self._key_id = key_id
        if client is None:  # pragma: no cover - needs AWS creds
            import boto3

            client = boto3.client("kms", region_name=region_name)
        self._client = client

    @property
    def kek_id(self) -> str:
        return self._key_id

    def wrap_dek(self, dek: bytes) -> bytes:
        return self._client.encrypt(KeyId=self._key_id, Plaintext=dek)["CiphertextBlob"]

    def unwrap_dek(self, wrapped: bytes) -> bytes:
        return self._client.decrypt(CiphertextBlob=wrapped)["Plaintext"]


class AzureKeyVaultKek(KekProvider):
    """Azure Key Vault. ``wrap`` = wrapKey, ``unwrap`` = unwrapKey (RSA-OAEP-256).
    The KEK stays in the vault/HSM."""

    provider_name = "azure_key_vault"

    def __init__(self, key_id: str, *, client: Optional[Any] = None, credential: Optional[Any] = None):
        self._key_id = key_id
        self._algorithm: Any = "RSA-OAEP-256"
        if client is None:  # pragma: no cover - needs azure-keyvault-keys + creds
            from azure.keyvault.keys.crypto import CryptographyClient, KeyWrapAlgorithm

            if credential is None:
                from azure.identity import DefaultAzureCredential

                credential = DefaultAzureCredential()
            client = CryptographyClient(key_id, credential=credential)
            self._algorithm = KeyWrapAlgorithm.rsa_oaep_256
        self._client = client

    @property
    def kek_id(self) -> str:
        return self._key_id

    def wrap_dek(self, dek: bytes) -> bytes:
        return self._client.wrap_key(self._algorithm, dek).encrypted_key

    def unwrap_dek(self, wrapped: bytes) -> bytes:
        return self._client.unwrap_key(self._algorithm, wrapped).key


def build_kek(config: Dict[str, Any]) -> KekProvider:
    """Construct the provider named by ``config['provider']``:
      env             → EnvKek(master_key_hex, kek_id)
      aws_kms         → AwsKmsKek(key_id, region_name?)
      azure_key_vault → AzureKeyVaultKek(key_id)
    """
    provider = (config or {}).get("provider")
    if provider == "env":
        return EnvKek(bytes.fromhex(config["master_key_hex"]), kek_id=config.get("kek_id", "env"))
    if provider == "aws_kms":
        return AwsKmsKek(config["key_id"], region_name=config.get("region_name"))
    if provider == "azure_key_vault":
        return AzureKeyVaultKek(config["key_id"])
    raise ValueError(f"unknown KEK provider: {provider!r}")
