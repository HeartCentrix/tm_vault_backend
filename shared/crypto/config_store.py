"""Read/write the singleton ``encryption_config`` row (UI-selected provider).

Kept separate from :mod:`shared.crypto.bootstrap` so bootstrap stays importable
without a DB. Stores only NON-SECRET fields (provider + key reference); secret
material lives in env / managed identity.
"""
from __future__ import annotations

from typing import Optional

from sqlalchemy import select

from shared.models import EncryptionConfig

_SINGLETON = "singleton"


def _to_dict(row: EncryptionConfig) -> dict:
    return {
        "provider": row.provider,
        "key_reference": row.key_reference,
        "enabled": bool(row.enabled),
        "verify_readback": bool(row.verify_readback),
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        "updated_by": row.updated_by,
    }


async def get_config(session_factory) -> Optional[dict]:
    async with session_factory() as s:
        row = (
            await s.execute(select(EncryptionConfig).where(EncryptionConfig.id == _SINGLETON))
        ).scalar_one_or_none()
        return _to_dict(row) if row else None


async def set_config(
    session_factory,
    *,
    provider: str,
    key_reference: Optional[str],
    enabled: bool,
    verify_readback: bool = True,
    updated_by: Optional[str] = None,
) -> dict:
    if provider not in ("env", "aws_kms", "azure_key_vault"):
        raise ValueError(f"unknown provider: {provider!r}")
    async with session_factory() as s:
        row = (
            await s.execute(select(EncryptionConfig).where(EncryptionConfig.id == _SINGLETON))
        ).scalar_one_or_none()
        if row is None:
            row = EncryptionConfig(id=_SINGLETON)
            s.add(row)
        row.provider = provider
        row.key_reference = key_reference
        row.enabled = enabled
        row.verify_readback = verify_readback
        row.updated_by = updated_by
        await s.commit()
        await s.refresh(row)
        return _to_dict(row)
