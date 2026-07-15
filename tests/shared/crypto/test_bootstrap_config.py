"""TDD — UI-selected provider is honored by the crypto bootstrap.

The gap: build_kek_from_env read ONLY env, so a provider picked in the UI had
zero effect. bootstrap now prefers a DB config (provider + non-secret key
reference); secret material still comes from env / managed identity.

Tested here (hermetically):
  * _config_dict_from(cfg) maps a stored config row -> the dict kek.build_kek
    expects, pulling env secret material for the env provider and using the
    key_reference as the KMS/Key Vault key id for cloud providers.
  * configure_from_config(cfg, session_factory) installs a write-enabled/-disabled
    hook per cfg.enabled, and falls back to env when cfg is None.
"""
from __future__ import annotations

import os

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "test-secret-" + "0" * 40)

import pytest

from shared.crypto import bootstrap, kek, runtime


def teardown_function():
    runtime.configure(None)


def test_config_dict_env_pulls_master_key_from_env(monkeypatch):
    monkeypatch.setenv("BACKUP_ENCRYPTION_MASTER_KEY_HEX", "ab" * 32)
    d = bootstrap._config_dict_from({"provider": "env", "key_reference": None})
    assert d["provider"] == "env"
    assert d["master_key_hex"] == "ab" * 32


def test_config_dict_aws_uses_key_reference(monkeypatch):
    monkeypatch.setenv("AWS_REGION", "eu-west-1")
    d = bootstrap._config_dict_from({"provider": "aws_kms", "key_reference": "arn:aws:kms:...:key/abc"})
    assert d == {"provider": "aws_kms", "key_id": "arn:aws:kms:...:key/abc", "region_name": "eu-west-1"}


def test_config_dict_azure_uses_key_reference():
    d = bootstrap._config_dict_from({"provider": "azure_key_vault", "key_reference": "https://v.vault.azure.net/keys/k/1"})
    assert d == {"provider": "azure_key_vault", "key_id": "https://v.vault.azure.net/keys/k/1"}


def test_config_dict_unknown_provider_raises():
    with pytest.raises(ValueError):
        bootstrap._config_dict_from({"provider": "gcp", "key_reference": "x"})


def test_configure_from_config_env_enabled(monkeypatch):
    monkeypatch.setenv("BACKUP_ENCRYPTION_MASTER_KEY_HEX", "cd" * 32)
    ok = bootstrap.configure_from_config(
        {"provider": "env", "key_reference": None, "enabled": True, "verify_readback": True},
        session_factory=lambda: None,
    )
    assert ok is True
    assert runtime.is_write_enabled() is True


def test_configure_from_config_present_but_disabled_installs_readonly(monkeypatch):
    monkeypatch.setenv("BACKUP_ENCRYPTION_MASTER_KEY_HEX", "cd" * 32)
    ok = bootstrap.configure_from_config(
        {"provider": "env", "key_reference": None, "enabled": False, "verify_readback": True},
        session_factory=lambda: None,
    )
    assert ok is True
    # manager installed (reads decrypt) but writes stay OFF
    assert runtime.is_write_enabled() is False
    assert runtime._dek_manager is not None


def test_configure_from_config_none_falls_back_to_env(monkeypatch):
    # no DB row, no env provider -> passthrough (nothing installed)
    monkeypatch.delenv("BACKUP_ENCRYPTION_KEK_PROVIDER", raising=False)
    ok = bootstrap.configure_from_config(None, session_factory=lambda: None)
    assert ok is False
    assert runtime.is_write_enabled() is False
