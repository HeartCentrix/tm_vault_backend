"""Assemble the encryption stack from env config and install it (Phase-4 gate).

A worker calls :func:`install_from_env` once at startup. With no config it
installs nothing → :mod:`shared.crypto.runtime` stays a passthrough → backups
are unencrypted, exactly as before. Encryption turns on only when the operator
sets the env below AND ``BACKUP_ENCRYPTION_ENABLED=true``.

Env:
  BACKUP_ENCRYPTION_ENABLED       "true" to encrypt NEW writes (default off)
  BACKUP_ENCRYPTION_VERIFY        write-then-verify read-back (default on)
  BACKUP_ENCRYPTION_KEK_PROVIDER  env | aws_kms | azure_key_vault
    env:            BACKUP_ENCRYPTION_MASTER_KEY_HEX (64 hex chars = 32 bytes),
                    BACKUP_ENCRYPTION_KEK_ID (optional label)
    aws_kms:        BACKUP_ENCRYPTION_AWS_KMS_KEY_ID, AWS_REGION (optional)
    azure_key_vault:BACKUP_ENCRYPTION_AZURE_KEY_ID (full key identifier URL)

The KEK provider is env-driven for the canary; the UI provider selector persists
the same choice later without changing this contract.
"""
from __future__ import annotations

import os
from typing import Optional

from . import runtime
from .dek_manager import DekManager
from .kek import AwsKmsKek, AzureKeyVaultKek, EnvKek, KekProvider
from .store import SqlDekStore


def _truthy(v: Optional[str]) -> bool:
    return (v or "").strip().lower() in ("1", "true", "yes", "on")


def build_kek_from_env() -> Optional[KekProvider]:
    provider = os.getenv("BACKUP_ENCRYPTION_KEK_PROVIDER")
    if not provider:
        return None
    if provider == "env":
        mk_hex = os.environ["BACKUP_ENCRYPTION_MASTER_KEY_HEX"]
        return EnvKek(bytes.fromhex(mk_hex), kek_id=os.getenv("BACKUP_ENCRYPTION_KEK_ID", "env"))
    if provider == "aws_kms":
        return AwsKmsKek(os.environ["BACKUP_ENCRYPTION_AWS_KMS_KEY_ID"], region_name=os.getenv("AWS_REGION"))
    if provider == "azure_key_vault":
        return AzureKeyVaultKek(os.environ["BACKUP_ENCRYPTION_AZURE_KEY_ID"])
    raise ValueError(f"unknown BACKUP_ENCRYPTION_KEK_PROVIDER: {provider!r}")


def _config_dict_from(cfg: dict) -> dict:
    """Turn a stored encryption_config row (provider + non-secret key_reference)
    into the dict :func:`shared.crypto.kek.build_kek` expects. Secret material is
    pulled from env / managed identity here — never from the DB row:
      * env             → master key from BACKUP_ENCRYPTION_MASTER_KEY_HEX
      * aws_kms         → key_id = key_reference (the KMS key ARN), region from env
      * azure_key_vault → key_id = key_reference (the Key Vault key URL)
    """
    provider = cfg.get("provider")
    ref = cfg.get("key_reference")
    if provider == "env":
        return {
            "provider": "env",
            "master_key_hex": os.environ["BACKUP_ENCRYPTION_MASTER_KEY_HEX"],
            "kek_id": os.getenv("BACKUP_ENCRYPTION_KEK_ID", "env"),
        }
    if provider == "aws_kms":
        return {"provider": "aws_kms", "key_id": ref, "region_name": os.getenv("AWS_REGION")}
    if provider == "azure_key_vault":
        return {"provider": "azure_key_vault", "key_id": ref}
    raise ValueError(f"unknown encryption provider: {provider!r}")


def build_kek_from_config(cfg: dict) -> KekProvider:
    """Construct the KEK named by a stored config row (UI-selected provider)."""
    from .kek import build_kek

    return build_kek(_config_dict_from(cfg))


def configure_from_config(cfg: Optional[dict], session_factory) -> bool:
    """Install the crypto runtime from a stored config row (UI-selected), or fall
    back to env when there is no row. Returns True if a DEK manager was installed.

    A present config always installs the DEK manager (so existing encrypted blobs
    decrypt); ``cfg['enabled']`` only gates whether NEW writes are encrypted."""
    if not cfg or not cfg.get("provider"):
        return install_from_env(session_factory)
    try:
        kek = build_kek_from_config(cfg)
    except Exception as e:  # missing env secret / bad reference -> stay safe (off)
        print(f"[crypto] encryption_config present but KEK build failed ({e}); leaving encryption OFF")
        runtime.configure(None)
        return False
    mgr = DekManager(kek, SqlDekStore(session_factory))
    runtime.configure(
        mgr,
        write_enabled=bool(cfg.get("enabled")),
        verify_readback=bool(cfg.get("verify_readback", True)),
    )
    return True


def install_from_env(session_factory) -> bool:
    """Wire the crypto runtime from env. Returns True if a DEK manager was
    installed (encryption is reachable), False if left as passthrough."""
    kek = build_kek_from_env()
    if kek is None:
        runtime.configure(None)  # explicit off
        return False
    mgr = DekManager(kek, SqlDekStore(session_factory))
    runtime.configure(
        mgr,
        write_enabled=_truthy(os.getenv("BACKUP_ENCRYPTION_ENABLED")),
        verify_readback=_truthy(os.getenv("BACKUP_ENCRYPTION_VERIFY", "true")),
    )
    return True


async def install(session_factory) -> bool:
    """Preferred startup entry point: install the crypto runtime from the
    UI-selected DB config (``encryption_config`` singleton) when present,
    otherwise fall back to env. Returns True if a DEK manager was installed.

    Safe if the config table doesn't exist yet (fresh DB) — falls back to env."""
    cfg = None
    try:
        from . import config_store

        cfg = await config_store.get_config(session_factory)
    except Exception as e:
        print(f"[crypto] could not read encryption_config ({e}); falling back to env")
    return configure_from_config(cfg, session_factory)
