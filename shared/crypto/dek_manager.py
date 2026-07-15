"""Per-tenant DEK lifecycle over a KEK provider and a pluggable store.

The DEK is the AES-256 key that seals a tenant's blobs (via
:mod:`shared.crypto.envelope`). It is persisted only in **wrapped** form (a KEK
wraps it); the manager unwraps it once per process and caches the plaintext in
memory. Storage-agnostic: the store is an injected async interface so the logic
is unit-testable and the real SQLAlchemy-backed store lives elsewhere.

Key invariant: a DEK wrapped now must unwrap to the same bytes forever, so a
blob sealed today decrypts after restarts and KEK rotations (rotation re-wraps
the DEK; the plaintext DEK is unchanged).
"""
from __future__ import annotations

import abc
import uuid
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

from . import envelope
from .kek import KekProvider


@dataclass
class DekRecord:
    tenant_id: str
    key_id: bytes          # 16 bytes, also stamped into every envelope
    wrapped_dek: bytes     # DEK wrapped by the KEK — safe to persist
    kek_id: str            # which KEK wrapped it (rotation bookkeeping)
    provider: str          # KEK provider name (env / aws_kms / azure_key_vault)
    state: str = "active"  # active | retired


class DekStore(abc.ABC):
    """Persistence for wrapped DEKs. One 'active' record per tenant; historical
    records are kept (state='retired') so older blobs still resolve."""

    @abc.abstractmethod
    async def get_active(self, tenant_id: str) -> Optional[DekRecord]: ...

    @abc.abstractmethod
    async def get_by_key_id(self, key_id: bytes) -> Optional[DekRecord]: ...

    @abc.abstractmethod
    async def insert(self, rec: DekRecord) -> None: ...

    # --- rotation support (default: unsupported; rotation-capable stores override) ---

    async def rotate(self, tenant_id: str, new_rec: DekRecord) -> None:
        """Atomically retire the tenant's current active DEK and insert
        ``new_rec`` as the new active one. Single transaction so there is never
        more than one active row per tenant (uq_tenant_dek_active)."""
        raise NotImplementedError

    async def all_records(self) -> "list[DekRecord]":
        """Every DEK record (active + retired) — used by KEK re-wrap."""
        raise NotImplementedError

    async def update_wrapping(self, key_id: bytes, wrapped_dek: bytes, kek_id: str, provider: str) -> None:
        """Replace the wrapped form of one DEK (KEK rotation). The plaintext DEK
        is unchanged, so blob ciphertext is untouched and still decrypts."""
        raise NotImplementedError


class DekManager:
    def __init__(self, kek: KekProvider, store: DekStore):
        self._kek = kek
        self._store = store
        self._by_kid: Dict[bytes, bytes] = {}     # key_id -> plaintext DEK (mem only)
        self._active_kid: Dict[str, bytes] = {}   # tenant_id -> active key_id

    async def active_dek(self, tenant_id: str) -> Tuple[bytes, bytes]:
        """Return ``(key_id, dek)`` for sealing this tenant's new blobs, minting
        and persisting a wrapped DEK on first use."""
        cached = self._active_kid.get(tenant_id)
        if cached is not None:
            return cached, self._by_kid[cached]

        rec = await self._store.get_active(tenant_id)
        if rec is None:
            dek = envelope.new_dek()
            rec = DekRecord(
                tenant_id=tenant_id,
                key_id=uuid.uuid4().bytes,
                wrapped_dek=self._kek.wrap_dek(dek),
                kek_id=self._kek.kek_id,
                provider=self._kek.provider_name,
            )
            await self._store.insert(rec)
        else:
            dek = self._kek.unwrap_dek(rec.wrapped_dek)

        self._by_kid[rec.key_id] = dek
        self._active_kid[tenant_id] = rec.key_id
        return rec.key_id, dek

    async def dek_for_key_id(self, key_id: bytes) -> bytes:
        """Return the plaintext DEK a blob was sealed with (read path)."""
        if key_id in self._by_kid:
            return self._by_kid[key_id]
        rec = await self._store.get_by_key_id(key_id)
        if rec is None:
            raise KeyError(f"no DEK for key_id {key_id.hex()}")
        dek = self._kek.unwrap_dek(rec.wrapped_dek)
        self._by_kid[key_id] = dek
        return dek

    async def rotate_dek(self, tenant_id: str) -> bytes:
        """Mint a fresh active DEK for ``tenant_id`` and retire the old one.
        New blobs seal under the new DEK; blobs already written keep decrypting
        via their ``key_id`` (which resolves the now-retired DEK). Returns the
        new key_id."""
        dek = envelope.new_dek()
        rec = DekRecord(
            tenant_id=tenant_id,
            key_id=uuid.uuid4().bytes,
            wrapped_dek=self._kek.wrap_dek(dek),
            kek_id=self._kek.kek_id,
            provider=self._kek.provider_name,
            state="active",
        )
        await self._store.rotate(tenant_id, rec)
        self._by_kid[rec.key_id] = dek          # keep old kids cached too (old blobs)
        self._active_kid[tenant_id] = rec.key_id
        return rec.key_id

    async def rewrap_all(self, new_kek: KekProvider) -> int:
        """Re-wrap every DEK under ``new_kek`` and adopt it. The DEK plaintext is
        unchanged so blob ciphertext is never touched. Call this with the manager
        still holding the CURRENT KEK and pass the NEW one — i.e. BEFORE retiring
        the old master key / KMS key — otherwise the existing wrapped bytes can no
        longer be unwrapped (the master-key-change data-loss trap). Returns the
        number of DEKs re-wrapped."""
        n = 0
        for rec in await self._store.all_records():
            dek = self._by_kid.get(rec.key_id) or self._kek.unwrap_dek(rec.wrapped_dek)
            await self._store.update_wrapping(
                rec.key_id, new_kek.wrap_dek(dek), new_kek.kek_id, new_kek.provider_name
            )
            self._by_kid[rec.key_id] = dek
            n += 1
        self._kek = new_kek
        return n
