"""TDD — DekManager: per-tenant DEK lifecycle over a KEK + a pluggable store.

  * active_dek(tenant) → mints a DEK on first use (wrapped by the KEK, persisted
    via the store), unwraps + caches it in memory thereafter
  * dek_for_key_id(key_id) → unwraps the DEK a given blob was sealed with (reads)
  * a cold manager (fresh process) reconstructs the SAME DEK from the stored
    wrapped bytes — so blobs stay decryptable across restarts

Tested with an in-memory async store + real EnvKek (so wrap/unwrap is real).
End-to-end test seals a blob and re-opens it through a cold manager.
"""
from __future__ import annotations

from typing import Dict, Optional

import pytest

from shared.crypto import envelope, kek
from shared.crypto.dek_manager import DekManager, DekRecord, DekStore

MASTER = b"\x55" * 32


class MemStore(DekStore):
    def __init__(self):
        self.by_kid: Dict[bytes, DekRecord] = {}
        self.active: Dict[str, bytes] = {}
        self.inserts = 0

    async def get_active(self, tenant_id: str) -> Optional[DekRecord]:
        kid = self.active.get(tenant_id)
        return self.by_kid.get(kid) if kid else None

    async def get_by_key_id(self, key_id: bytes) -> Optional[DekRecord]:
        return self.by_kid.get(key_id)

    async def insert(self, rec: DekRecord) -> None:
        self.by_kid[rec.key_id] = rec
        self.active[rec.tenant_id] = rec.key_id
        self.inserts += 1


def _mgr(store):
    return DekManager(kek.EnvKek(MASTER, kek_id="e1"), store)


async def test_active_dek_mints_and_persists_once():
    store = MemStore()
    mgr = _mgr(store)
    kid, dek = await mgr.active_dek("t1")
    assert len(kid) == 16 and len(dek) == 32
    assert store.inserts == 1
    # cached — no second insert
    kid2, dek2 = await mgr.active_dek("t1")
    assert (kid2, dek2) == (kid, dek)
    assert store.inserts == 1
    # persisted wrapped, not plaintext
    rec = store.by_kid[kid]
    assert rec.wrapped_dek != dek
    assert rec.provider == "env" and rec.kek_id == "e1"


async def test_cold_manager_recovers_same_dek():
    store = MemStore()
    kid, dek = await _mgr(store).active_dek("t1")
    # brand-new manager, empty cache, same store + kek
    cold = _mgr(store)
    kid2, dek2 = await cold.active_dek("t1")
    assert kid2 == kid and dek2 == dek
    assert store.inserts == 1  # reused, not re-minted


async def test_dek_for_key_id_reads():
    store = MemStore()
    mgr = _mgr(store)
    kid, dek = await mgr.active_dek("t1")
    assert await _mgr(store).dek_for_key_id(kid) == dek


async def test_dek_for_unknown_key_id_raises():
    with pytest.raises(KeyError):
        await _mgr(MemStore()).dek_for_key_id(b"\x00" * 16)


async def test_end_to_end_seal_and_reopen_cold():
    store = MemStore()
    kid, dek = await _mgr(store).active_dek("t1")
    blob = envelope.encrypt_blob(b"top secret mail", dek, kid)
    # read side: fresh manager resolves the DEK from key_id in the envelope
    cold = _mgr(store)
    reopened_dek = await cold.dek_for_key_id(envelope.read_key_id(blob))
    assert envelope.decrypt_blob(blob, reopened_dek) == b"top secret mail"


async def test_per_tenant_isolation():
    store = MemStore()
    mgr = _mgr(store)
    _, dek_a = await mgr.active_dek("tA")
    _, dek_b = await mgr.active_dek("tB")
    assert dek_a != dek_b
    assert store.inserts == 2
