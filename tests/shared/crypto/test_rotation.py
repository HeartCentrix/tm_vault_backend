"""TDD — key rotation + plaintext-blob migration.

Two rotation operations, both previously unimplemented:
  * rotate_dek(tenant)  — mint a NEW active DEK, retire the old (new writes use
    the new DEK; old blobs still open via their key_id -> retired DEK). Atomic:
    never two active rows per tenant.
  * rewrap_all(new_kek) — re-wrap every DEK under a new KEK. DEK plaintext is
    unchanged so blob ciphertext is untouched and still decrypts. This is the
    fix for the EnvKek master-key-change data-loss landmine: re-wrap BEFORE the
    old master key is retired.

Plus migrate.reencrypt_blob — the backfill primitive that seals existing
plaintext blobs, idempotently (skip already-TMVE), with the write-then-verify
upload rail.
"""
from __future__ import annotations

from dataclasses import replace
from typing import Dict, List, Optional

import pytest

from shared.crypto import blob_cipher, envelope, kek, migrate, runtime
from shared.crypto.dek_manager import DekManager, DekRecord, DekStore

MASTER_A = b"\xaa" * 32
MASTER_B = b"\xbb" * 32


class MemStore(DekStore):
    def __init__(self):
        self.by_kid: Dict[bytes, DekRecord] = {}
        self.active: Dict[str, bytes] = {}

    async def get_active(self, tenant_id):
        kid = self.active.get(tenant_id)
        return self.by_kid.get(kid) if kid else None

    async def get_by_key_id(self, key_id):
        return self.by_kid.get(key_id)

    async def insert(self, rec):
        self.by_kid[rec.key_id] = rec
        if rec.state == "active":
            self.active[rec.tenant_id] = rec.key_id

    async def rotate(self, tenant_id, new_rec):
        cur = self.active.get(tenant_id)
        if cur:
            self.by_kid[cur] = replace(self.by_kid[cur], state="retired")
        self.by_kid[new_rec.key_id] = new_rec
        self.active[tenant_id] = new_rec.key_id

    async def all_records(self):
        return list(self.by_kid.values())

    async def update_wrapping(self, key_id, wrapped_dek, kek_id, provider):
        self.by_kid[key_id] = replace(
            self.by_kid[key_id], wrapped_dek=wrapped_dek, kek_id=kek_id, provider=provider
        )

    def active_count(self, tenant_id):
        return sum(1 for r in self.by_kid.values() if r.tenant_id == tenant_id and r.state == "active")


def _mgr(store, master=MASTER_A, kek_id="A"):
    return DekManager(kek.EnvKek(master, kek_id=kek_id), store)


# ---- DEK rotation -----------------------------------------------------------

async def test_rotate_mints_new_active_and_retires_old():
    store = MemStore()
    mgr = _mgr(store)
    kid1, dek1 = await mgr.active_dek("t1")
    kid2 = await mgr.rotate_dek("t1")
    assert kid2 != kid1
    assert store.by_kid[kid1].state == "retired"
    assert store.by_kid[kid2].state == "active"
    assert store.active_count("t1") == 1  # never two active
    # new writes use the new DEK
    akid, _ = await mgr.active_dek("t1")
    assert akid == kid2


async def test_old_blob_still_opens_after_dek_rotation():
    store = MemStore()
    mgr = _mgr(store)
    kid1, dek1 = await mgr.active_dek("t1")
    sealed_old = blob_cipher.seal(b"secret-old", dek1, kid1)
    await mgr.rotate_dek("t1")
    # read path resolves the retired DEK by the blob's key_id
    dek_resolved = await mgr.dek_for_key_id(kid1)
    assert blob_cipher.open_blob(sealed_old, dek_resolved) == b"secret-old"


# ---- KEK rotation / re-wrap (the master-key data-loss landmine) --------------

async def test_rewrap_all_changes_wrapping_not_dek():
    store = MemStore()
    mgr = _mgr(store, MASTER_A, "A")
    kid, dek = await mgr.active_dek("t1")
    wrapped_before = store.by_kid[kid].wrapped_dek
    n = await mgr.rewrap_all(kek.EnvKek(MASTER_B, kek_id="B"))
    assert n == 1
    rec = store.by_kid[kid]
    assert rec.kek_id == "B" and rec.provider == "env"
    assert rec.wrapped_dek != wrapped_before          # re-wrapped
    assert kek.EnvKek(MASTER_B, kek_id="B").unwrap_dek(rec.wrapped_dek) == dek  # same DEK


async def test_master_key_change_without_dataloss():
    """Seal under master A, rotate KEK to master B, blob must still decrypt."""
    store = MemStore()
    mgr = _mgr(store, MASTER_A, "A")
    kid, dek = await mgr.active_dek("t1")
    sealed = blob_cipher.seal(b"payload", dek, kid)
    await mgr.rewrap_all(kek.EnvKek(MASTER_B, kek_id="B"))
    # a COLD manager that only knows master B reconstructs the same DEK
    cold = DekManager(kek.EnvKek(MASTER_B, kek_id="B"), store)
    dek_b = await cold.dek_for_key_id(kid)
    assert blob_cipher.open_blob(sealed, dek_b) == b"payload"


# ---- plaintext-blob migration / backfill ------------------------------------

class FakeShard:
    """Minimal in-memory stand-in for the storage facade the migrator drives."""

    def __init__(self, blobs: Dict[str, bytes]):
        self.blobs = dict(blobs)  # "container/path" -> stored bytes (plaintext or TMVE)
        self.dek = envelope.new_dek()
        self.kid = b"\x01" * 16

    def _k(self, c, p):
        return f"{c}/{p}"

    async def head_is_encrypted(self, container, path):
        b = self.blobs.get(self._k(container, path))
        return b is not None and envelope.is_encrypted(b)

    async def download_blob(self, container, path):
        b = self.blobs.get(self._k(container, path))
        if b is None:
            return None
        return envelope.decrypt_blob(b, self.dek) if envelope.is_encrypted(b) else b

    async def upload_blob(self, container, path, content, *, tenant_id=None, **kw):
        self.blobs[self._k(container, path)] = blob_cipher.seal(content, self.dek, self.kid)
        return {"success": True}

    async def list_blobs(self, container):
        pre = f"{container}/"
        for k in list(self.blobs):
            if k.startswith(pre):
                yield k[len(pre):]


@pytest.fixture
def write_on():
    # migration requires the write hook to be enabled, else it would loop.
    store = MemStore()
    mgr = _mgr(store)
    runtime.configure(mgr, write_enabled=True, verify_readback=False)
    yield
    runtime.configure(None)


async def test_reencrypt_blob_seals_plaintext(write_on):
    shard = FakeShard({"c/a.bin": b"plaintext-A"})
    res = await migrate.reencrypt_blob(shard, "c", "a.bin", "t1")
    assert res == "encrypted"
    assert envelope.is_encrypted(shard.blobs["c/a.bin"])
    assert await shard.download_blob("c", "a.bin") == b"plaintext-A"


async def test_reencrypt_blob_skips_already_encrypted(write_on):
    shard = FakeShard({})
    shard.blobs["c/a.bin"] = blob_cipher.seal(b"already", shard.dek, shard.kid)
    before = shard.blobs["c/a.bin"]
    res = await migrate.reencrypt_blob(shard, "c", "a.bin", "t1")
    assert res == "skipped"
    assert shard.blobs["c/a.bin"] == before  # untouched (idempotent)


async def test_reencrypt_blob_missing(write_on):
    shard = FakeShard({})
    assert await migrate.reencrypt_blob(shard, "c", "gone.bin", "t1") == "missing"


async def test_reencrypt_container_counts(write_on):
    shard = FakeShard({"c/a.bin": b"aaa", "c/b.bin": b"bbb"})
    shard.blobs["c/enc.bin"] = blob_cipher.seal(b"enc", shard.dek, shard.kid)
    stats = await migrate.reencrypt_container(shard, "c", "t1", concurrency=2)
    assert stats["encrypted"] == 2 and stats["skipped"] == 1 and stats["failed"] == 0


async def test_reencrypt_requires_write_enabled():
    runtime.configure(None)  # writes OFF
    shard = FakeShard({"c/a.bin": b"x"})
    with pytest.raises(RuntimeError):
        await migrate.reencrypt_container(shard, "c", "t1")
