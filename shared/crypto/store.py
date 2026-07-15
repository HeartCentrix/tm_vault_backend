"""SQLAlchemy-backed :class:`~shared.crypto.dek_manager.DekStore`.

Persists wrapped DEKs in the ``tenant_dek`` table (see shared/models.TenantDek).
Kept out of :mod:`shared.crypto.dek_manager` so the manager stays DB-agnostic
and unit-testable with an in-memory store.
"""
from __future__ import annotations

from typing import Optional

from sqlalchemy import select

from shared.models import TenantDek

from .dek_manager import DekRecord, DekStore


def _to_record(row: TenantDek) -> DekRecord:
    return DekRecord(
        tenant_id=row.tenant_id,
        key_id=bytes(row.key_id),
        wrapped_dek=bytes(row.wrapped_dek),
        kek_id=row.kek_id,
        provider=row.provider,
        state=row.state,
    )


class SqlDekStore(DekStore):
    def __init__(self, session_factory):
        # session_factory() must yield an AsyncSession context manager.
        self._sf = session_factory

    async def get_active(self, tenant_id: str) -> Optional[DekRecord]:
        async with self._sf() as s:
            row = (
                await s.execute(
                    select(TenantDek)
                    .where(TenantDek.tenant_id == tenant_id, TenantDek.state == "active")
                    .limit(1)
                )
            ).scalar_one_or_none()
            return _to_record(row) if row else None

    async def get_by_key_id(self, key_id: bytes) -> Optional[DekRecord]:
        async with self._sf() as s:
            row = (
                await s.execute(select(TenantDek).where(TenantDek.key_id == key_id).limit(1))
            ).scalar_one_or_none()
            return _to_record(row) if row else None

    async def insert(self, rec: DekRecord) -> None:
        async with self._sf() as s:
            s.add(
                TenantDek(
                    tenant_id=rec.tenant_id,
                    key_id=rec.key_id,
                    wrapped_dek=rec.wrapped_dek,
                    kek_id=rec.kek_id,
                    provider=rec.provider,
                    state=rec.state,
                )
            )
            await s.commit()
