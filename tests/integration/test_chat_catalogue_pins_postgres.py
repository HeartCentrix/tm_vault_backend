"""Read/export pins use real catalogue ownership and database transactions."""
from datetime import datetime, timedelta, timezone
import importlib
from uuid import uuid4

import pytest
from sqlalchemy import delete, select, update

from test_chat_catalogue_postgres import store, parent_for
from shared.models import ChatBlobObject, ChatCataloguePin, Snapshot

pytestmark = pytest.mark.integration


def api():
    return importlib.import_module("shared.chat_catalogue_gc")


async def captured(store):
    scope = await store.new_scope()
    lease = await store.begin(scope)
    child = await store.binary(lease)
    items = [parent_for(child), child]
    await store.stage(lease, items)
    await store.observe(scope, lease, items)
    result = await store.publish(lease)
    return scope, child.content.object_id, result.inventory_id


async def test_pin_context_persists_owned_inventory_then_releases(store):
    scope, _, inventory_id = await captured(store)
    async with api().catalogue_read_pin(store.factory, snapshot_ids=[scope.snapshot_id]) as lease:
        lease.assert_valid()
        async with store.factory() as session:
            pins = list((await session.scalars(select(ChatCataloguePin))).all())
            assert len(pins) == 1
            assert pins[0].inventory_id == inventory_id
    async with store.factory() as session:
        assert list((await session.scalars(select(ChatCataloguePin))).all()) == []


async def test_exact_object_pin_does_not_pin_unrelated_snapshot_content(store):
    scope, object_id, _ = await captured(store)
    async with api().catalogue_read_pin(store.factory, snapshot_ids=[scope.snapshot_id],
                                       object_ids=[object_id]):
        async with store.factory() as session:
            pin = (await session.scalars(select(ChatCataloguePin))).one()
            assert pin.object_id == object_id
            assert pin.inventory_id is None
    unrelated = await store.new_scope()
    with pytest.raises(RuntimeError, match="referenc|scope|owned"):
        async with api().catalogue_read_pin(store.factory, snapshot_ids=[unrelated.snapshot_id],
                                           object_ids=[object_id]):
            pytest.fail("unrelated content was pinned")


async def test_pin_cannot_resurrect_retiring_object(store):
    scope, object_id, _ = await captured(store)
    async with store.factory.begin() as session:
        await session.execute(update(ChatBlobObject).where(ChatBlobObject.id == object_id).values(state="DELETING"))
    with pytest.raises(RuntimeError, match="LIVE|retir|available"):
        async with api().catalogue_read_pin(store.factory, snapshot_ids=[scope.snapshot_id],
                                           object_ids=[object_id]):
            pytest.fail("retiring object was pinned")


async def test_expired_pin_cannot_be_renewed(store):
    scope, _, _ = await captured(store)
    owner = uuid4()
    async with store.factory.begin() as session:
        ids = await api().acquire_catalogue_read_pins(session, snapshot_ids=[scope.snapshot_id],
                                                     owner_kind="export", owner_id=owner)
    async with store.factory.begin() as session:
        await session.execute(update(ChatCataloguePin).where(ChatCataloguePin.id.in_(ids)).values(
            expires_at=datetime.now(timezone.utc) - timedelta(seconds=1)))
    with pytest.raises(RuntimeError, match="expir|ownership"):
        async with store.factory.begin() as session:
            await api().renew_catalogue_read_pins(session, pin_ids=ids, owner_id=owner)


async def test_different_owner_cannot_release_another_read_pin(store):
    scope, _, _ = await captured(store)
    owner = uuid4()
    async with store.factory.begin() as session:
        ids = await api().acquire_catalogue_read_pins(session, snapshot_ids=[scope.snapshot_id],
                                                     owner_kind="read", owner_id=owner)
    async with store.factory.begin() as session:
        await api().release_catalogue_read_pins(session, pin_ids=ids, owner_id=uuid4())
    async with store.factory() as session:
        assert len(list((await session.scalars(select(ChatCataloguePin))).all())) == 1


async def test_deleted_snapshot_cannot_authorize_a_new_object_pin(store):
    scope, object_id, _ = await captured(store)
    async with store.factory.begin() as session:
        from shared.models import SnapshotItem
        await session.execute(delete(SnapshotItem).where(SnapshotItem.snapshot_id == scope.snapshot_id))
        await session.execute(delete(Snapshot).where(Snapshot.id == scope.snapshot_id))
    with pytest.raises(RuntimeError, match="snapshot|scope|referenc"):
        async with api().catalogue_read_pin(store.factory, snapshot_ids=[scope.snapshot_id],
                                           object_ids=[object_id]):
            pytest.fail("deleted snapshot authorized a new read")


async def test_guard_stops_stream_after_pin_renewal_fails(store, monkeypatch):
    import asyncio
    scope, _, _ = await captured(store)
    failed = asyncio.Event()

    async def renewal_failure(*args, **kwargs):
        failed.set()
        raise RuntimeError("pin database unavailable")

    monkeypatch.setattr(api(), "renew_catalogue_read_pins", renewal_failure)

    async def content():
        yield b"must not be emitted"

    with pytest.raises(RuntimeError, match="pin|lease|renew"):
        async with api().catalogue_read_pin(store.factory, snapshot_ids=[scope.snapshot_id],
                                           expires_seconds=1) as lease:
            await asyncio.wait_for(failed.wait(), 2)
            await asyncio.sleep(0)
            async for _ in lease.guard(content()):
                pytest.fail("data emitted without valid protection")


async def test_inventory_pin_rejects_a_retiring_member_object(store):
    scope, object_id, _ = await captured(store)
    async with store.factory.begin() as session:
        await session.execute(update(ChatBlobObject).where(ChatBlobObject.id == object_id).values(state="DELETING"))
    with pytest.raises(RuntimeError, match="LIVE|retir|available"):
        async with api().catalogue_read_pin(store.factory, snapshot_ids=[scope.snapshot_id]):
            pytest.fail("retired inventory content authorized")


async def test_pin_acquisition_serializes_against_snapshot_deletion(store):
    import asyncio
    scope, _, _ = await captured(store)
    deleting = None
    try:
        async with store.factory.begin() as session:
            await api().acquire_catalogue_read_pins(session, snapshot_ids=[scope.snapshot_id],
                owner_kind="read", owner_id=uuid4())

            async def remove_snapshot():
                from shared.models import SnapshotItem
                async with store.factory.begin() as other:
                    await other.execute(delete(SnapshotItem).where(SnapshotItem.snapshot_id == scope.snapshot_id))
                    await other.execute(delete(Snapshot).where(Snapshot.id == scope.snapshot_id))

            deleting = asyncio.create_task(remove_snapshot())
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(asyncio.shield(deleting), 0.15)
        await asyncio.wait_for(deleting, 2)
    finally:
        if deleting is not None and not deleting.done():
            deleting.cancel()
            try:
                await deleting
            except asyncio.CancelledError:
                pass
