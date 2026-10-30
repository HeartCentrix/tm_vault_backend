"""Exclusive catalogue GC against disposable PostgreSQL, with real references."""
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import delete, select, update

from test_chat_catalogue_postgres import store, parent_for
from shared.models import (ChatBlobObject, ChatCataloguePin, ChatCaptureAttempt,
                           ChatInventory, ChatItemVersion, Snapshot, SnapshotItem)
from shared.chat_catalogue_gc import catalogue_read_pin

pytestmark = pytest.mark.integration


class Storage:
    def __init__(self):
        self.deleted = []
        self.fail = False

    def get_store_by_id(self, backend_id):
        self.backend = backend_id
        return self

    async def delete(self, container, path):
        if self.fail:
            raise OSError("synthetic storage unavailable")
        self.deleted.append((self.backend, container, path))


async def capture(store):
    scope = await store.new_scope()
    lease = await store.begin(scope)
    child = await store.binary(lease)
    items = [parent_for(child), child]
    await store.stage(lease, items)
    await store.observe(scope, lease, items)
    result = await store.publish(lease)
    return SimpleNamespace(scope=scope, lease=lease, child=child, result=result)


async def remove(store, scope):
    async with store.factory.begin() as session:
        await session.execute(delete(SnapshotItem).where(SnapshotItem.snapshot_id == scope.snapshot_id))
        await session.execute(delete(Snapshot).where(Snapshot.id == scope.snapshot_id))


async def collect(store, storage=None, limit=100):
    from shared.chat_catalogue_gc import collect_chat_catalogue
    return await collect_chat_catalogue(session_factory=store.factory, storage=storage or Storage(), limit=limit)


async def test_surviving_snapshot_retains_shared_inventory_and_bytes(store):
    first = await capture(store)
    second_scope, _, second = await store.capture([parent_for(first.child), first.child])
    assert second.reused
    await remove(store, first.scope)
    storage = Storage()
    await collect(store, storage)
    assert storage.deleted == []
    async with store.factory() as session:
        assert await session.get(ChatInventory, first.result.inventory_id)
        assert await session.get(ChatBlobObject, first.child.content.object_id)
    await remove(store, second_scope)
    result = await collect(store, storage)
    assert result.objects_deleted == 1
    assert storage.deleted == [(str(store.backend), first.child.content.container, first.child.content.path)]


async def test_export_pin_retains_inventory_after_snapshot_is_deleted(store):
    first = await capture(store)
    storage = Storage()
    async with catalogue_read_pin(store.factory, snapshot_ids=[first.scope.snapshot_id], owner_kind="export"):
        await remove(store, first.scope)
        await collect(store, storage)
        assert storage.deleted == []
    assert (await collect(store, storage)).objects_deleted == 1


async def test_indefinite_legal_hold_pin_and_expired_read_pin(store):
    first = await capture(store)
    hold_id = uuid4()
    async with store.factory.begin() as session:
        session.add(ChatCataloguePin(id=hold_id, inventory_id=first.result.inventory_id,
            owner_kind="legal_hold", owner_id=uuid4(), generation=1, expires_at=None))
        session.add(ChatCataloguePin(inventory_id=first.result.inventory_id,
            owner_kind="read", owner_id=uuid4(), generation=1,
            expires_at=datetime.now(timezone.utc) - timedelta(days=1)))
    await remove(store, first.scope)
    storage = Storage()
    await collect(store, storage)
    assert not storage.deleted
    async with store.factory.begin() as session:
        await session.execute(delete(ChatCataloguePin).where(ChatCataloguePin.id == hold_id))
    assert (await collect(store, storage)).objects_deleted == 1


async def test_storage_failure_keeps_durable_retirement_task_and_retries(store):
    first = await capture(store)
    await remove(store, first.scope)
    storage = Storage()
    storage.fail = True
    result = await collect(store, storage)
    assert result.deletion_failures == 1
    async with store.factory() as session:
        obj = await session.get(ChatBlobObject, first.child.content.object_id)
        assert obj.state == "DELETING" and obj.deletion_task_id
        task_id = obj.deletion_task_id
    storage.fail = False
    assert (await collect(store, storage)).objects_deleted == 1
    async with store.factory() as session:
        obj = await session.get(ChatBlobObject, first.child.content.object_id)
        assert obj.state == "DELETED" and obj.deletion_task_id == task_id
        assert obj.deletion_attempts == 2


async def test_live_capture_and_unverifiable_staging_upload_are_retained(store):
    scope = await store.new_scope()
    lease = await store.begin(scope)
    child = await store.binary(lease, state="STAGING")
    await remove(store, scope)
    storage = Storage()
    await collect(store, storage)
    assert not storage.deleted
    async with store.factory.begin() as session:
        await session.execute(update(ChatCaptureAttempt).where(ChatCaptureAttempt.id == lease.attempt_id)
            .values(expires_at=datetime.now(timezone.utc) - timedelta(days=1)))
    await collect(store, storage)
    assert not storage.deleted  # No upload deadline / quiescence evidence.
    async with store.factory.begin() as session:
        await session.execute(update(ChatBlobObject).where(ChatBlobObject.id == child.content.object_id)
            .values(upload_deadline=datetime.now(timezone.utc)-timedelta(hours=2),
                    quiescent_after=datetime.now(timezone.utc)-timedelta(hours=1)))
    assert (await collect(store, storage)).objects_deleted == 1


async def test_collector_respects_limit_and_ignores_legacy_paths(store):
    first = await capture(store)
    await remove(store, first.scope)
    async with store.factory.begin() as session:
        await session.execute(update(ChatBlobObject).where(ChatBlobObject.id == first.child.content.object_id)
                              .values(path="legacy/snapshot/owned/path"))
    storage = Storage()
    await collect(store, storage, limit=1)
    assert not storage.deleted
    with pytest.raises(ValueError):
        await collect(store, storage, limit=0)


async def test_retired_generation_never_deletes_new_generation(store):
    first = await capture(store)
    await remove(store, first.scope)
    storage = Storage()
    await collect(store, storage)
    newer = await capture(store)
    assert newer.child.content.path != first.child.content.path
    await collect(store, storage)
    assert all(path != newer.child.content.path for _, _, path in storage.deleted)
    async with store.factory() as session:
        assert (await session.get(ChatBlobObject, newer.child.content.object_id)).state == "LIVE"


async def test_small_gc_batch_progresses_past_retained_objects(store):
    await capture(store)
    orphan = await capture(store)
    await remove(store, orphan.scope)
    storage = Storage()
    for _ in range(3):
        await collect(store, storage, limit=1)
    assert storage.deleted == [(str(store.backend), orphan.child.content.container, orphan.child.content.path)]


async def test_reuse_heir_promotion_preserves_exact_catalogue_links(store):
    from shared.retention_cleanup import _rehydrate_reuse_heir
    from shared.models import SnapshotChatInventory, SnapshotStatus
    first = await capture(store)
    heir = await store.new_scope()
    async with store.factory.begin() as session:
        await session.execute(update(Snapshot).where(Snapshot.id == first.scope.snapshot_id).values(status=SnapshotStatus.COMPLETED))
        await session.execute(update(Snapshot).where(Snapshot.id == heir.snapshot_id).values(
            status=SnapshotStatus.COMPLETED, reuse_of_snapshot_id=first.scope.snapshot_id,
            reuse_chain_root_id=first.scope.snapshot_id))
        promoted = await _rehydrate_reuse_heir(session, first.scope.snapshot_id, doomed_ids={first.scope.snapshot_id})
        assert promoted == heir.snapshot_id
    async with store.factory() as session:
        links = list((await session.scalars(select(SnapshotItem).where(SnapshotItem.snapshot_id == heir.snapshot_id))).all())
        assert links and all(link.chat_version_id is not None for link in links)
        association = await session.get(SnapshotChatInventory, (heir.snapshot_id, first.scope.chat_id))
        assert association.inventory_id == first.result.inventory_id
    await remove(store, first.scope)
    storage = Storage()
    await collect(store, storage)
    assert not storage.deleted


async def test_tombstone_reconciliation_removes_a_late_upload(store):
    first = await capture(store)
    await remove(store, first.scope)
    storage = Storage()
    await collect(store, storage)
    async with store.factory.begin() as session:
        await session.execute(update(ChatBlobObject).where(ChatBlobObject.id == first.child.content.object_id)
            .values(deleted_at=datetime.now(timezone.utc) - timedelta(days=2)))
    # A timed-out external upload could finish after the original delete.
    result = await collect(store, storage)
    assert result.objects_deleted == 1
    assert len(storage.deleted) == 2 and storage.deleted[0] == storage.deleted[1]


async def test_bounded_expired_pin_cleanup_does_not_abort_inventory_reclamation(store):
    retained = await capture(store)
    orphan = await capture(store)
    async with store.factory.begin() as session:
        for pin_id, captured in ((1, retained), (2, orphan)):
            from uuid import UUID
            session.add(ChatCataloguePin(id=UUID(int=pin_id), inventory_id=captured.result.inventory_id,
                owner_kind="read", owner_id=uuid4(), generation=1,
                expires_at=datetime.now(timezone.utc)-timedelta(days=1)))
    await remove(store, orphan.scope)
    storage = Storage()
    first = await collect(store, storage, limit=1)
    assert first.objects_deleted == 0
    second = await collect(store, storage, limit=1)
    assert second.objects_deleted == 1
