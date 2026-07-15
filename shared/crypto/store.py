"""SQLAlchemy-backed :class:`~shared.crypto.dek_manager.DekStore`.

Persists wrapped DEKs in the ``tenant_dek`` table (see shared/models.TenantDek).
Kept out of :mod:`shared.crypto.dek_manager` so the manager stays DB-agnostic
and unit-testable with an in-memory store.
"""
from __future__ import annotations

from typing import List, Optional

from sqlalchemy import select, update

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


def _to_row(rec: DekRecord) -> TenantDek:
    return TenantDek(
        tenant_id=rec.tenant_id,
        key_id=rec.key_id,
        wrapped_dek=rec.wrapped_dek,
        kek_id=rec.kek_id,
        provider=rec.provider,
        state=rec.state,
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
            s.add(_to_row(rec))
            await s.commit()

    async def rotate(self, tenant_id: str, new_rec: DekRecord) -> None:
        # Retire the current active row and insert the new one in ONE transaction
        # so uq_tenant_dek_active (partial unique on state='active') never sees
        # two active rows for the tenant.
        async with self._sf() as s:
            await s.execute(
                update(TenantDek)
                .where(TenantDek.tenant_id == tenant_id, TenantDek.state == "active")
                .values(state="retired")
            )
            s.add(_to_row(new_rec))
            await s.commit()

    async def all_records(self) -> List[DekRecord]:
        async with self._sf() as s:
            rows = (await s.execute(select(TenantDek))).scalars().all()
            return [_to_record(r) for r in rows]

    async def update_wrapping(self, key_id: bytes, wrapped_dek: bytes, kek_id: str, provider: str) -> None:
        async with self._sf() as s:
            await s.execute(
                update(TenantDek)
                .where(TenantDek.key_id == key_id)
                .values(wrapped_dek=wrapped_dek, kek_id=kek_id, provider=provider)
            )
            await s.commit()
