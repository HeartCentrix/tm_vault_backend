"""Live catalogue progress counts committed publications, independently of final deltas."""
import asyncio
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import select

from test_chat_catalogue_postgres import store, message, parent_for
from test_chat_catalogue_entry_postgres import source_adapter, worker_entry
from shared import models, chat_catalogue, chat_catalogue_entry

pytestmark = pytest.mark.integration


async def counters(store, scope):
    async with store.factory() as session:
        snapshot = await session.get(models.Snapshot, scope.snapshot_id)
        return snapshot.item_count, snapshot.bytes_total, snapshot.new_item_count, snapshot.bytes_added


async def refresh(store, scope):
    scope = replace(scope, snapshot_lease_token=0, job_lease_token=0)
    async with store.factory.begin() as session:
        return await chat_catalogue_entry.refresh_catalogue_progress(session, scope)


async def test_capture_and_replay_expose_published_content_before_final_settlement(store, source_adapter):
    scope = await store.new_scope()
    result = await source_adapter.run(scope)
    assert await counters(store, scope) == (1, result["bytes_total"], 0, 0)
    assert (await source_adapter.run(scope))["item_count"] == result["item_count"]
    assert await counters(store, scope) == (1, result["bytes_total"], 0, 0)


async def test_reused_inventory_advances_each_snapshot_once(store, source_adapter):
    first, second = await store.new_scope(), await store.new_scope()
    result = await source_adapter.run(first)
    reused = await source_adapter.run(second)
    assert await counters(store, first) == await counters(store, second) == (1, result["bytes_total"], 0, 0)
    async with store.factory() as session:
        assert len(list((await session.scalars(select(models.ChatInventory).where(
            models.ChatInventory.state == "COMPLETE"))).all())) == 1
    assert reused["uploaded_bytes"] == 0


async def test_partial_publication_counts_primary_content_and_preserves_delta_fields(store):
    scope = await store.new_scope()
    lease = await store.begin(scope)
    child = await store.binary(lease)
    parent = parent_for(child)
    await store.stage(lease, [parent, child])
    await store.observe(scope, lease, [parent, child], enumeration_complete=False)
    async with store.factory.begin() as session:
        snapshot = await session.get(models.Snapshot, scope.snapshot_id)
        snapshot.new_item_count, snapshot.bytes_added = 7, 13
    assert await counters(store, scope) == (0, 0, 7, 13)
    result = await store.publish(lease)
    assert result.outcome == "partial" and result.item_count == 2
    await refresh(store, scope)
    assert await counters(store, scope) == (1, result.logical_bytes, 7, 13)
    await store.publish(lease)
    await refresh(store, scope)
    assert await counters(store, scope) == (1, result.logical_bytes, 7, 13)


async def test_unpublished_rows_never_enter_live_progress(store):
    scope, lease, published = await store.capture([message()])
    pending = replace(scope, chat_id="chat-beta")
    pending_lease = await store.begin(pending)
    await store.stage(pending_lease, [message("unpublished")])
    await refresh(store, scope)
    assert await counters(store, scope) == (1, published.logical_bytes, 0, 0)
    # A rolled-back publication cannot change either published membership or progress.
    await store.observe(pending, pending_lease, [message("unpublished")])
    async with store.factory() as session:
        await chat_catalogue.publish_inventory(session, lease=pending_lease)
        await session.rollback()
    await refresh(store, scope)
    assert await counters(store, scope) == (1, published.logical_bytes, 0, 0)


async def test_parallel_publication_and_refresh_have_no_lost_counts_or_deadlock(store):
    first = await store.new_scope()
    second = replace(first, chat_id="chat-beta")
    captures = []
    for index, scope in enumerate((first, second)):
        partition_id = uuid4()
        async with store.factory.begin() as session:
            snapshot = await session.get(models.Snapshot, scope.snapshot_id)
            session.add(models.SnapshotPartition(id=partition_id, snapshot_id=snapshot.id,
                job_id=snapshot.job_id, tenant_id=store.tenant, resource_id=store.resource,
                drive_id="", file_ids=[], partition_index=index, total_files=0, total_bytes_est=0,
                partition_type="CHATS", payload={"chat_ids": [scope.chat_id]},
                status="IN_PROGRESS", lease_token=1))
        scope = replace(scope, partition_id=partition_id, partition_lease_token=1)
        lease = await store.begin(scope)
        await store.stage(lease, [message()])
        await store.observe(scope, lease, [message()])
        captures.append((scope, lease))

    async def publish_and_refresh(scope, lease):
        result = await store.publish(lease)
        await refresh(store, scope)
        return result

    tasks = [asyncio.create_task(publish_and_refresh(*capture)) for capture in captures]
    try:
        results = await asyncio.wait_for(asyncio.gather(*tasks), 5)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    assert await counters(store, first) == (2, sum(result.logical_bytes for result in results), 0, 0)
    await asyncio.gather(*(refresh(store, scope) for scope, _ in captures))
    assert (await counters(store, first))[0] == 2


async def test_progress_refresh_refuses_replacement_parent_owner(store):
    scope, _, _ = await store.capture([message()])
    async with store.factory.begin() as session:
        (await session.get(models.Snapshot, scope.snapshot_id)).lease_token = 1
    with pytest.raises(chat_catalogue.ChatCatalogueWorkLeaseMoved):
        await refresh(store, scope)
    assert await counters(store, scope) == (0, 0, 0, 0)


async def test_partition_settlement_cannot_overwrite_newer_publication_progress(
        store, source_adapter, worker_entry, monkeypatch):
    scope = await store.new_scope()
    async with store.factory.begin() as session:
        snapshot = await session.get(models.Snapshot, scope.snapshot_id)
        snapshot.extra_data = {"chat_inventory_format": 1, "partitioned": True}
        snapshot.item_count, snapshot.bytes_total = 2, 200
    stale = {"chat_inventory_format": 1, "item_count": 1, "bytes_total": 100,
        "new_item_count": 1, "bytes_added": 100, "uploaded_bytes": 0,
        "partitioned": True, "outcome": "complete", "_snapshot_lease_token": 0, "_job_lease_token": 0}
    monkeypatch.setattr(chat_catalogue_entry, "run_chat_catalogue_resource", AsyncMock(return_value=stale))
    worker_entry.worker._settle_inline_snapshot_finalize = AsyncMock(return_value="deferred_partitioned")
    worker_entry.worker.update_resource_backup_info = AsyncMock()
    await worker_entry.run(scope)
    assert await counters(store, scope) == (2, 200, 0, 0)


async def test_failed_advisory_refresh_preserves_publication_and_replay_repairs_progress(
        store, source_adapter, monkeypatch):
    scope = await store.new_scope()
    original = chat_catalogue_entry.refresh_catalogue_progress
    monkeypatch.setattr(chat_catalogue_entry, "refresh_catalogue_progress",
        AsyncMock(side_effect=RuntimeError("synthetic progress outage")))
    first = await source_adapter.run(scope)
    assert first["outcome"] == "complete" and first["item_count"] == 1
    assert await counters(store, scope) == (0, 0, 0, 0)
    monkeypatch.setattr(chat_catalogue_entry, "refresh_catalogue_progress", original)
    await source_adapter.run(scope)
    assert await counters(store, scope) == (1, first["bytes_total"], 0, 0)
    assert source_adapter.source.message_calls == ["chat-alpha", "chat-alpha"]


async def test_live_new_count_is_only_changed_messages_and_ends_at_final_delta(store):
    # Activity showed the running total (34,951) then the final delta (20); users read it as a full re-backup.
    old = [message(graph_id="m1"), message(graph_id="m2")]
    beta = [message(graph_id="b1")]
    first, _, _ = await store.capture(old)
    await store.capture(beta, scope=replace(first, chat_id="chat-beta"))
    async with store.factory.begin() as session:
        (await session.get(models.Snapshot, first.snapshot_id)).status = models.SnapshotStatus.COMPLETED
    second = await store.new_scope()
    seen = []
    for scope, items in ((replace(second, chat_id="chat-beta"), beta),
                         (second, [*old, message("edited", graph_id="m3")])):
        await store.capture(items, scope=scope)
        await refresh(store, scope)
        await refresh(store, scope)  # replayed refresh never double counts
        async with store.factory() as session:
            snapshot = await session.get(models.Snapshot, second.snapshot_id)
            seen.append(((snapshot.extra_data or {}).get("chat_live_new_items"), snapshot.item_count))
    assert seen == [(0, 1), (1, 4)]
    async with store.factory() as session:
        assert (await chat_catalogue_entry.catalogue_snapshot_result(session, second.snapshot_id))["new_item_count"] == 1


async def test_first_capture_live_new_count_is_everything(store):
    scope, _, _ = await store.capture([message()])
    await refresh(store, scope)
    async with store.factory() as session:
        assert (await session.get(models.Snapshot, scope.snapshot_id)).extra_data["chat_live_new_items"] == 1
