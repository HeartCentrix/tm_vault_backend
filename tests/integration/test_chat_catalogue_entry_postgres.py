"""Resource-level cutover fencing against real disposable PostgreSQL."""
from __future__ import annotations

import asyncio
import importlib
import importlib.util
from contextlib import asynccontextmanager
from copy import deepcopy
import hashlib
from types import SimpleNamespace
from pathlib import Path
import sys
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import select, update

from test_chat_catalogue_postgres import store, message
from shared import models

pytestmark = pytest.mark.integration


def api():
    return importlib.import_module("shared.chat_catalogue_entry")


async def legacy_scope(store):
    scope = await store.new_scope()
    async with store.factory.begin() as session:
        await session.execute(update(models.Resource).where(models.Resource.id == scope.resource_id)
            .values(extra_data={"user_id": scope.principal_id}))
        await session.execute(update(models.Snapshot).where(models.Snapshot.id == scope.snapshot_id).values(extra_data={}))
    return scope


async def prepare(store, scope, enabled):
    async with store.factory.begin() as session:
        return await api().adopt_chat_catalogue(session, resource_id=scope.resource_id,
            snapshot_id=scope.snapshot_id, writer_enabled=enabled)


async def test_disabled_legacy_path_is_not_marked_or_converted(store):
    scope = await legacy_scope(store)
    assert await prepare(store, scope, False) is False
    async with store.factory.begin() as session:
        assert (await session.get(models.Resource, scope.resource_id)).extra_data == {"user_id": scope.principal_id}
        assert (await session.get(models.Snapshot, scope.snapshot_id)).extra_data.get("chat_inventory_format") is None


async def test_legacy_entry_reservation_prevents_same_snapshot_flag_toggle_race(store):
    scope = await legacy_scope(store)
    assert await prepare(store, scope, False) is False
    with pytest.raises(api().ChatCatalogueAdoptionError, match="legacy"):
        await prepare(store, scope, True)
    async with store.factory.begin() as session:
        snapshot = await session.get(models.Snapshot, scope.snapshot_id)
        assert snapshot.extra_data["chat_legacy_writer_entered"] is True
        assert snapshot.extra_data.get("chat_inventory_format") is None
        snapshot.status = models.SnapshotStatus.COMPLETED
    later = await store.new_scope()
    async with store.factory.begin() as session:
        snapshot = await session.get(models.Snapshot, later.snapshot_id)
        snapshot.extra_data = {}
    assert await prepare(store, later, True) is True


async def test_adoption_marks_resource_and_snapshot_before_capture(store):
    scope = await legacy_scope(store)
    assert await prepare(store, scope, True) is True
    async with store.factory.begin() as session:
        assert (await session.get(models.Resource, scope.resource_id)).extra_data["chat_inventory_format"] == 1
        snapshot = await session.get(models.Snapshot, scope.snapshot_id)
        assert snapshot.extra_data["chat_inventory_format"] == 1
        assert snapshot.type == models.SnapshotType.FULL
    with pytest.raises(api().ChatCatalogueSuspended):
        await prepare(store, scope, False)


async def test_failed_first_catalogue_attempt_does_not_create_incremental_baseline(store):
    first = await legacy_scope(store)
    assert await prepare(store, first, True)
    async with store.factory.begin() as session:
        (await session.get(models.Snapshot, first.snapshot_id)).status = models.SnapshotStatus.FAILED
    second = await store.new_scope()
    assert await prepare(store, second, True)
    async with store.factory() as session:
        assert (await session.get(models.Snapshot, second.snapshot_id)).type == models.SnapshotType.FULL


async def test_other_legacy_run_blocks_cutover_until_terminal(store):
    scope = await legacy_scope(store)
    other = await store.new_scope()
    async with store.factory.begin() as session:
        await session.execute(update(models.Snapshot).where(models.Snapshot.id == other.snapshot_id).values(extra_data={}))
    with pytest.raises(api().ChatCatalogueAdoptionError, match="legacy"):
        await prepare(store, scope, True)
    async with store.factory.begin() as session:
        await session.execute(update(models.Snapshot).where(models.Snapshot.id == other.snapshot_id)
            .values(status=models.SnapshotStatus.FAILED))
    assert await prepare(store, scope, True)


@pytest.mark.parametrize("kind", ["items", "partitions"])
async def test_existing_current_legacy_work_cannot_be_relabelled(store, kind):
    scope = await legacy_scope(store)
    async with store.factory.begin() as session:
        if kind == "items":
            session.add(models.SnapshotItem(snapshot_id=scope.snapshot_id, tenant_id=scope.tenant_id,
                backend_id=scope.backend_id, external_id="legacy", item_type="TEAMS_CHAT_MESSAGE", name="legacy"))
        else:
            session.add(models.SnapshotPartition(snapshot_id=scope.snapshot_id, tenant_id=scope.tenant_id,
                resource_id=scope.resource_id, job_id=await session.scalar(select(models.Snapshot.job_id)
                    .where(models.Snapshot.id == scope.snapshot_id)), partition_type="CHATS", partition_index=0,
                status="QUEUED", payload={"chat_ids": [scope.chat_id]}))
    with pytest.raises(api().ChatCatalogueAdoptionError, match="legacy"):
        await prepare(store, scope, True)
    async with store.factory.begin() as session:
        assert (await session.get(models.Resource, scope.resource_id)).extra_data.get("chat_inventory_format") is None


async def test_disabled_waiter_observes_committed_adoption_and_cannot_enter_legacy(store):
    scope = await legacy_scope(store)
    first = store.factory()
    await first.begin()
    try:
        assert await api().adopt_chat_catalogue(first, resource_id=scope.resource_id,
            snapshot_id=scope.snapshot_id, writer_enabled=True)
        started = asyncio.Event()
        async def disabled():
            async with store.factory.begin() as session:
                started.set()
                return await api().adopt_chat_catalogue(session, resource_id=scope.resource_id,
                    snapshot_id=scope.snapshot_id, writer_enabled=False)
        task = asyncio.create_task(disabled())
        await started.wait()
        await asyncio.sleep(0.05)
        assert not task.done()
        await first.commit()
        with pytest.raises(api().ChatCatalogueSuspended):
            await task
    finally:
        await first.close()


async def test_terminal_new_snapshot_resume_does_not_require_writer_flag(store):
    scope, _, _ = await store.capture([message()])
    async with store.factory.begin() as session:
        await session.execute(update(models.Snapshot).where(models.Snapshot.id == scope.snapshot_id)
            .values(status=models.SnapshotStatus.PARTIAL))
    assert await prepare(store, scope, False)


@pytest.fixture
def source_adapter(store, monkeypatch):
    from shared import chat_capture_graph
    class Source(chat_capture_graph.StrictChatGraphSource):
        def __init__(self):
            self.principal_id = "principal-alpha"
            self.permission_scope_hash = "a" * 64
            self.chats = [{"id": "chat-alpha", "topic": "Frozen name"}]
            self.messages = {"chat-alpha": [{"id": "message-alpha", "body": {"content": "body"}, "attachments": []}]}
            self.list_calls, self.message_calls, self.hosted_calls = 0, [], []
        async def iter_chats(self):
            self.list_calls += 1
            for chat in self.chats:
                yield deepcopy(chat)
        async def iter_message_pages(self, chat_id):
            self.message_calls.append(chat_id)
            yield deepcopy(self.messages.get(chat_id, []))
        async def iter_hosted_contents(self, chat_id, message_id):
            self.hosted_calls.append((chat_id, message_id))
            if False:
                yield {}
    source = Source()
    @asynccontextmanager
    async def pinned(_client, *, principal_id):
        assert principal_id == source.principal_id
        yield source
    monkeypatch.setattr(chat_capture_graph, "pinned_chat_source", pinned)
    class Storage:
        def active_backend_id(self):
            return str(store.backend)
        def get_store_by_id(self, _backend):
            raise AssertionError("unexpected binary storage access")
    async def run(scope, **changes):
        return await api().run_chat_catalogue_resource(resource_id=scope.resource_id,
            snapshot_id=scope.snapshot_id, writer_enabled=True, graph_client=object(),
            session_factory=store.factory, storage=Storage(), **changes)
    return SimpleNamespace(source=source, run=run, storage=Storage())


async def test_resource_capture_uses_full_source_and_records_complete_scope(store, source_adapter):
    scope = await legacy_scope(store)
    result = await source_adapter.run(scope)
    assert result["outcome"] == "complete" and result["item_count"] == 1
    assert source_adapter.source.message_calls == ["chat-alpha", "chat-alpha"]
    assert source_adapter.source.list_calls == 2
    async with store.factory.begin() as session:
        snapshot = await session.get(models.Snapshot, scope.snapshot_id)
        assert snapshot.status == models.SnapshotStatus.IN_PROGRESS  # worker owns terminal settle
        assert snapshot.extra_data["chat_catalogue_scopes"]["resource"]["complete"]
        assert snapshot.type == models.SnapshotType.FULL


async def test_exclusions_filter_messages_and_children_in_both_observations(store, source_adapter):
    scope = await legacy_scope(store)
    source_adapter.source.messages["chat-alpha"] = [
        {"id": "kept", "body": {"content": "kept"}},
        {"id": "excluded", "body": {"content": "excluded"}, "attachments": [
            {"id": "never-download", "contentType": "reference", "contentUrl": "https://invalid.example/file"}]}]
    filtered = []
    def excluded(raw):
        filtered.append((raw["id"], raw["_chat_folder_path"]))
        return raw["id"] == "excluded"
    result = await source_adapter.run(scope, exclude_message=excluded,
        scope_filter_hash=hashlib.sha256(b"configured filter").hexdigest())
    assert result["outcome"] == "complete" and result["item_count"] == 1
    assert filtered == [("kept", "chats/Frozen name"), ("excluded", "chats/Frozen name")] * 2
    assert source_adapter.source.hosted_calls == [("chat-alpha", "kept")] * 2
    async with store.factory.begin() as session:
        assert list((await session.scalars(select(models.ChatItemVersion.graph_id))).all()) == ["kept"]


async def test_resuming_published_chat_does_not_reenumerate_its_messages(store, source_adapter):
    scope = await legacy_scope(store)
    first = await source_adapter.run(scope)
    source_adapter.source.messages["chat-alpha"][0]["body"]["content"] = "later state"
    second = await source_adapter.run(scope)
    assert first["item_count"] == second["item_count"] == 1
    assert source_adapter.source.message_calls == ["chat-alpha", "chat-alpha"]
    async with store.factory.begin() as session:
        versions = list((await session.scalars(select(models.ChatItemVersion))).all())
        assert len(versions) == 1 and versions[0].payload["body"]["content"] == "body"
        await session.execute(update(models.Snapshot).where(models.Snapshot.id == scope.snapshot_id)
            .values(status=models.SnapshotStatus.PARTIAL, extra_data={"chat_inventory_format": 1,
                "failure_reason": "known partial coverage"}))
    resumed = await api().run_chat_catalogue_resource(resource_id=scope.resource_id,
        snapshot_id=scope.snapshot_id, writer_enabled=False, graph_client=object(),
        session_factory=store.factory, storage=source_adapter.storage)
    assert resumed["resumed_terminal"] and resumed["outcome"] == "partial"
    assert resumed["failure_reason"] == "known partial coverage"
    assert source_adapter.source.list_calls == 4


@pytest.mark.parametrize("protection", ["hold", "immutability", "customer_key"])
async def test_unsupported_native_protection_refuses_adoption_before_markers(store, protection):
    scope = await legacy_scope(store)
    policy_id = uuid4()
    async with store.factory.begin() as session:
        values = {"legal_hold_enabled": True} if protection == "hold" else (
            {"immutability_mode": "Locked"} if protection == "immutability" else {"encryption_mode": "CUSTOMER_KEY"})
        session.add(models.SlaPolicy(id=policy_id, tenant_id=scope.tenant_id, name="Protected", **values))
        await session.flush()
        await session.execute(update(models.Resource).where(models.Resource.id == scope.resource_id)
            .values(sla_policy_id=policy_id))
    with pytest.raises(api().ChatCatalogueProtectionUnsupported):
        await prepare(store, scope, True)
    assert await prepare(store, scope, False) is False
    async with store.factory.begin() as session:
        assert (await session.get(models.Resource, scope.resource_id)).extra_data.get("chat_inventory_format") is None


async def test_partition_capture_preserves_allowlist_and_original_fence(store, source_adapter):
    scope = await store.new_scope()
    partition_id = uuid4()
    async with store.factory.begin() as session:
        snapshot = await session.get(models.Snapshot, scope.snapshot_id)
        snapshot.extra_data = {"chat_inventory_format": 1, "partitioned": True}
        session.add(models.SnapshotPartition(id=partition_id, snapshot_id=scope.snapshot_id,
            resource_id=scope.resource_id, tenant_id=scope.tenant_id, job_id=snapshot.job_id,
            partition_type="CHATS", partition_index=0, status="IN_PROGRESS", lease_token=7,
            payload={"chat_ids": ["chat-beta"]}))
    source_adapter.source.chats.append({"id": "chat-beta", "topic": "Beta"})
    source_adapter.source.messages["chat-beta"] = [{"id": "beta-message", "body": {"content": "beta"}}]
    with pytest.raises(api().ChatCatalogueWorkLeaseMoved):
        await source_adapter.run(scope, partition_filter={"chatIds": ["chat-beta"]},
            partition_id=partition_id, partition_lease_token=6)
    assert source_adapter.source.list_calls == 0
    result = await source_adapter.run(scope, partition_filter={"chatIds": ["chat-beta"]},
        partition_id=partition_id, partition_lease_token=7)
    assert result["outcome"] == "complete" and result["partitioned"]
    assert source_adapter.source.message_calls == ["chat-beta", "chat-beta"]


@pytest.fixture
def worker_entry(store, source_adapter, monkeypatch):
    path = Path(__file__).resolve().parents[2] / "workers/backup-worker/main.py"
    spec = importlib.util.spec_from_file_location("catalogue_pg_worker", path)
    bw = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = bw
    spec.loader.exec_module(bw)
    monkeypatch.setattr(bw, "async_session_factory", store.factory)
    monkeypatch.setattr(bw, "_is_job_cancelled", AsyncMock(return_value=False))
    monkeypatch.setattr(bw.settings, "CHAT_CATALOGUE_WRITER_ENABLED", True)
    import shared.storage.router as router_module
    monkeypatch.setattr(router_module, "router", source_adapter.storage)
    worker = bw.BackupWorker.__new__(bw.BackupWorker)
    worker.worker_id = "catalogue-pg-test"
    worker.get_sla_policy = AsyncMock(return_value=None)
    worker.get_policy_exclusions = AsyncMock(return_value=[])
    worker._emit_backup_audit = AsyncMock()
    async def run(scope, **message_fields):
        async with store.factory() as session:
            snapshot = await session.get(models.Snapshot, scope.snapshot_id)
            resource = await session.get(models.Resource, scope.resource_id)
            tenant = await session.get(models.Tenant, scope.tenant_id)
            job_id = snapshot.job_id
        return await worker._maybe_backup_chat_catalogue(resource, snapshot, tenant, object(),
            {"jobId": str(job_id), **message_fields}, job_id)
    return SimpleNamespace(worker=worker, module=bw, run=run)


async def test_worker_publishes_and_settles_full_then_reused_incremental_with_honest_counts(
        store, source_adapter, worker_entry):
    scope = await legacy_scope(store)
    result = await worker_entry.run(scope)
    assert result["status"] == "COMPLETED" and result["item_count"] == result["new_item_count"] == 1
    assert result["bytes_total"] == result["bytes_added"] > 0
    async with store.factory() as session:
        snapshot = await session.get(models.Snapshot, scope.snapshot_id)
        resource = await session.get(models.Resource, scope.resource_id)
        assert snapshot.type == models.SnapshotType.FULL and snapshot.status == models.SnapshotStatus.COMPLETED
        assert resource.last_backup_status == "COMPLETED" and resource.last_backup_at is not None
    second = await store.new_scope()
    result2 = await worker_entry.run(second)
    assert result2["status"] == "COMPLETED" and result2["item_count"] == 1
    assert result2["new_item_count"] == 0 and result2["bytes_added"] == 0
    assert result2["bytes_total"] == result["bytes_total"]
    async with store.factory() as session:
        second_snapshot = await session.get(models.Snapshot, second.snapshot_id)
        assert second_snapshot.type == models.SnapshotType.INCREMENTAL
        assert second_snapshot.bytes_total == result["bytes_total"] and second_snapshot.bytes_added == 0
    async with store.factory.begin() as session:
        (await session.get(models.Snapshot, scope.snapshot_id)).status = models.SnapshotStatus.PENDING_DELETION
    resumed = await worker_entry.run(second)
    assert resumed["resumed_terminal"] and resumed["new_item_count"] == 0 and resumed["bytes_added"] == 0


async def test_worker_preserves_partial_and_failed_source_outcomes(store, source_adapter, worker_entry):
    scope = await legacy_scope(store)
    source_adapter.source.chats.append({"id": "unavailable", "topic": "Unavailable"})
    original = source_adapter.source.iter_message_pages
    async def fail_unavailable(chat_id):
        if chat_id == "unavailable":
            raise RuntimeError("source chat is unavailable")
        async for page in original(chat_id):
            yield page
    source_adapter.source.iter_message_pages = fail_unavailable
    result = await worker_entry.run(scope)
    assert result["status"] == "PARTIAL" and result["item_count"] == 1 and not result["failed"]
    async with store.factory() as session:
        snapshot = await session.get(models.Snapshot, scope.snapshot_id)
        assert snapshot.status == models.SnapshotStatus.PARTIAL and snapshot.extra_data["degraded_reason"]
        assert (await session.get(models.Resource, scope.resource_id)).last_backup_status == "PARTIAL"
    source_adapter.source.chats = [{"id": "unavailable"}]
    failed = await store.new_scope()
    result2 = await worker_entry.run(failed)
    assert result2["status"] == "FAILED" and result2["failed"]
    async with store.factory() as session:
        assert (await session.get(models.Resource, scope.resource_id)).last_backup_status == "FAILED"


async def test_parent_takeover_after_capture_cannot_settle_snapshot(store, source_adapter):
    scope = await legacy_scope(store)
    original = source_adapter.source.iter_chats
    calls = 0
    async def changed_owner():
        nonlocal calls
        calls += 1
        async for chat in original():
            yield chat
        if calls == 2:
            async with store.factory.begin() as session:
                await session.execute(update(models.Snapshot).where(models.Snapshot.id == scope.snapshot_id)
                    .values(lease_token=42))
    source_adapter.source.iter_chats = changed_owner
    with pytest.raises(api().ChatCatalogueWorkLeaseMoved):
        await source_adapter.run(scope)
    async with store.factory() as session:
        snapshot = await session.get(models.Snapshot, scope.snapshot_id)
        assert snapshot.status == models.SnapshotStatus.IN_PROGRESS and snapshot.lease_token == 42
        assert "chat_catalogue_scopes" not in snapshot.extra_data


async def test_newformat_partition_finalizer_uses_versions_and_never_carries_legacy_history(
        store, source_adapter, worker_entry):
    first = await legacy_scope(store)
    await worker_entry.run(first)
    second = await store.new_scope()
    partition_id = uuid4()
    async with store.factory.begin() as session:
        snapshot = await session.get(models.Snapshot, second.snapshot_id)
        snapshot.extra_data = {"chat_inventory_format": 1, "partitioned": True}
        session.add(models.SnapshotPartition(id=partition_id, snapshot_id=second.snapshot_id,
            resource_id=second.resource_id, tenant_id=second.tenant_id, job_id=snapshot.job_id,
            partition_type="CHATS", partition_index=0, status="IN_PROGRESS", lease_token=7,
            payload={"chat_ids": ["chat-alpha"]}))
    worker_entry.module._bind_partition_lease(partition_id, {"lease_token": 7, "snapshot_id": second.snapshot_id})
    source_adapter.source.messages["chat-alpha"][0]["body"]["content"] = "changed text"
    result = await worker_entry.run(second, partitionId=str(partition_id), partitionFilter={"chatIds": ["chat-alpha"]})
    assert result["status"] == "IN_PROGRESS" and result["new_item_count"] == 1
    async with store.factory.begin() as session:
        await session.execute(update(models.SnapshotPartition).where(models.SnapshotPartition.id == partition_id)
            .values(status="COMPLETED"))
    worker = worker_entry.worker
    worker._compute_snapshot_delta_from_prior = AsyncMock(side_effect=AssertionError("legacy id delta is unsafe"))
    worker._carry_forward_partitioned_user_mail = AsyncMock(side_effect=AssertionError("legacy carry forbidden"))
    assert await worker._finalize_partitioned_snapshot(second.snapshot_id) == "COMPLETED"
    worker._compute_snapshot_delta_from_prior.assert_not_awaited()
    worker._carry_forward_partitioned_user_mail.assert_not_awaited()
    async with store.factory() as session:
        snapshot = await session.get(models.Snapshot, second.snapshot_id)
        assert snapshot.new_item_count == 1 and snapshot.bytes_added == result["bytes_added"] > 0
        assert snapshot.bytes_total == result["bytes_total"]


async def test_partition_without_original_claim_token_refuses_before_graph(store, source_adapter, worker_entry):
    scope = await store.new_scope()
    partition_id = uuid4()
    async with store.factory.begin() as session:
        snapshot = await session.get(models.Snapshot, scope.snapshot_id)
        snapshot.extra_data = {"chat_inventory_format": 1, "partitioned": True}
        session.add(models.SnapshotPartition(id=partition_id, snapshot_id=scope.snapshot_id,
            resource_id=scope.resource_id, tenant_id=scope.tenant_id, job_id=snapshot.job_id,
            partition_type="CHATS", partition_index=0, status="IN_PROGRESS", lease_token=7,
            payload={"chat_ids": ["chat-alpha"]}))
    worker_entry.module._PARTITION_LEASE_CTX.set(None)
    with pytest.raises(api().ChatCatalogueWorkLeaseMoved):
        await worker_entry.run(scope, partitionId=str(partition_id), partitionFilter={"chatIds": ["chat-alpha"]})
    assert source_adapter.source.list_calls == 0
