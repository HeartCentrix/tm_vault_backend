"""Resource-level cutover fencing against real disposable PostgreSQL."""
from __future__ import annotations

import asyncio
import importlib
import importlib.util
from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import datetime, timezone
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
        latest_change = None
        async def latest_message_change(self, chat_id):
            # Default: always changed, so these tests exercise the full capture.
            return self.latest_change or datetime.now(timezone.utc)
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
        {"id": "kept", "body": {"content": "kept <img src=\"https://graph.microsoft.com/v1.0/chats/chat-alpha/messages/kept/hostedContents/aWQ9/$value\">"}},
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


async def test_capture_keeps_only_history_visible_to_the_principal(store, source_adapter):
    """A member invited with limited history sees messages from visibleHistoryStartDateTime on;
    their backup must hold exactly that (Divyansh: 11 of 364, 2026-11-04). Full-history
    members ("0001-01-01") keep everything."""
    scope = await legacy_scope(store)
    source_adapter.source.chats = [{"id": "chat-alpha", "topic": "Frozen name", "chatType": "group", "members": [
        {"userId": "principal-alpha", "displayName": "Me", "visibleHistoryStartDateTime": "2026-09-01T16:07:24.118Z"}]}]
    source_adapter.source.messages["chat-alpha"] = [
        {"id": "before", "createdDateTime": "2026-08-31T10:00:00Z", "body": {"content": "old"}, "attachments": []},
        {"id": "after", "createdDateTime": "2026-09-01T16:07:24.118Z", "body": {"content": "new"}, "attachments": []},
        {"id": "undated", "body": {"content": "kept when the source gives no creation time"}, "attachments": []}]
    result = await source_adapter.run(scope)
    assert result["outcome"] == "complete" and result["item_count"] == 2
    async with store.factory.begin() as session:
        assert sorted((await session.scalars(select(models.ChatItemVersion.graph_id))).all()) == ["after", "undated"]


async def test_full_history_member_keeps_every_message(store, source_adapter):
    scope = await legacy_scope(store)
    source_adapter.source.chats = [{"id": "chat-alpha", "topic": None, "chatType": "oneOnOne", "members": [
        {"userId": "principal-alpha", "displayName": "Me", "visibleHistoryStartDateTime": "0001-01-01T00:00:00Z"},
        {"userId": "other", "displayName": "Other", "visibleHistoryStartDateTime": "2030-01-01T00:00:00Z"}]}]
    source_adapter.source.messages["chat-alpha"] = [
        {"id": "ancient", "createdDateTime": "2020-01-01T00:00:00Z", "body": {"content": "old"}, "attachments": []}]
    result = await source_adapter.run(scope)
    assert result["outcome"] == "complete" and result["item_count"] == 1


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


async def test_chat_hosted_in_another_tenant_is_listed_not_partial(store, source_adapter):
    """Microsoft refuses app access (403) to chats hosted in another organization's tenant
    (external meeting chats). That is not our app or permission failure: the backup is not
    PARTIAL and the chat is listed as hosted elsewhere. A same-tenant 403 stays PARTIAL."""
    from shared.chat_capture_graph import ChatGraphError
    scope = await legacy_scope(store)
    source = source_adapter.source
    source.home_tenant_id = "home-tenant"
    source.chats.append({"id": "external-meeting", "topic": "Vendor call", "tenantId": "other-tenant"})
    # Railway 2026-11-04: Graph lists some cross-organization chats as chatType
    # unknownFutureValue with no tenantId; they answer 403 InsufficientPrivileges like the rest.
    source.chats.append({"id": "unlabelled-external", "topic": None, "chatType": "unknownFutureValue"})
    original = source.iter_message_pages
    async def denied(chat_id):
        if chat_id in ("external-meeting", "unlabelled-external", "same-tenant-denied"):
            raise ChatGraphError("Graph capture metadata failed with HTTP 403", status_code=403)
        async for page in original(chat_id):
            yield page
    source.iter_message_pages = denied
    result = await source_adapter.run(scope)
    assert result["outcome"] == "complete" and result["item_count"] == 1
    async with store.factory() as session:
        record = (await session.get(models.Snapshot, scope.snapshot_id)).extra_data["chat_catalogue_scopes"]["resource"]
    assert record["complete"] and record["external_tenant_chats"] == ["external-meeting", "unlabelled-external"]

    source.chats.append({"id": "same-tenant-denied", "topic": "Ours", "tenantId": "home-tenant"})
    second = await store.new_scope()
    assert (await source_adapter.run(second))["outcome"] == "partial"
    source.chats[-1] = {"id": "same-tenant-denied", "topic": "Ours", "chatType": "group"}  # no tenant, ordinary type
    third = await store.new_scope()
    assert (await source_adapter.run(third))["outcome"] == "partial"


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


async def test_requested_full_backup_reads_every_chat_even_when_unchanged(store, source_adapter):
    """Incremental reuse is for incremental runs; a full backup (forceFullBackup / type FULL)
    re-reads every chat from Graph."""
    source = source_adapter.source
    await source_adapter.run(await legacy_scope(store))
    source.latest_change = datetime(2020, 1, 1, tzinfo=timezone.utc)
    reads = len(source.message_calls)
    assert (await source_adapter.run(await store.new_scope(), full_backup=True))["outcome"] == "complete"
    assert len(source.message_calls) > reads
    reads = len(source.message_calls)
    assert (await source_adapter.run(await store.new_scope()))["outcome"] == "complete"
    assert len(source.message_calls) == reads


async def test_chat_hosted_elsewhere_is_settled_by_one_request_without_a_capture_attempt(store, source_adapter):
    """Railway 2026-11-05 incremental: Rohit's 92 chats hosted in other organizations' tenants were
    attempted again on every run, postponed behind other members and failed at the end (~7 of his
    25 minutes). One request now proves Microsoft refuses them: no capture attempt, nothing to wait on.
    A chat listed under another tenant that Microsoft does let us read is still captured."""
    from shared.chat_capture_graph import ChatGraphError
    source = source_adapter.source
    source.home_tenant_id = "home-tenant"
    source.chats += [{"id": "external-meeting", "topic": "Vendor call", "tenantId": "other-tenant"},
                     {"id": "unlabelled-external", "topic": None, "chatType": "unknownFutureValue"},
                     {"id": "readable-external", "topic": "Shared channel", "tenantId": "other-tenant"}]
    source.messages["readable-external"] = [{"id": "message-ext", "body": {"content": "hi"}, "attachments": []}]
    checks = []

    async def latest(chat_id):
        checks.append(chat_id)
        if chat_id in ("external-meeting", "unlabelled-external"):
            raise ChatGraphError("Graph capture metadata failed with HTTP 403", status_code=403)
        return datetime.now(timezone.utc)
    source.latest_message_change = latest
    scope = await legacy_scope(store)
    result = await source_adapter.run(scope)
    assert result["outcome"] == "complete" and result["item_count"] == 2
    assert checks.count("external-meeting") == checks.count("unlabelled-external") == 1
    assert {"external-meeting", "unlabelled-external"}.isdisjoint(source.message_calls)
    async with store.factory() as session:
        attempted = set((await session.scalars(select(models.ChatCaptureAttempt.chat_id).where(
            models.ChatCaptureAttempt.snapshot_id == scope.snapshot_id))).all())
        record = (await session.get(models.Snapshot, scope.snapshot_id)).extra_data["chat_catalogue_scopes"]["resource"]
    assert attempted == {"chat-alpha", "readable-external"}
    assert record["complete"] and record["external_tenant_chats"] == ["external-meeting", "unlabelled-external"]


async def test_chat_hosted_elsewhere_completes_its_provider_unit(store, source_adapter, monkeypatch):
    """Railway 2026-11-04: Divyansh's chat snapshot was COMPLETED but its provider work PARTIAL:
    the two chats Microsoft refuses (another organization's tenant) marked their units FAILED
    while the snapshot counted them as hosted elsewhere."""
    from shared import provider_runtime
    from shared.chat_capture_graph import ChatGraphError
    units = []

    async def record(**kwargs):
        units.append(kwargs.get("state"))

    monkeypatch.setattr(provider_runtime, "checkpoint_unit", record)
    source = source_adapter.source
    source.home_tenant_id = "home-tenant"
    source.chats.append({"id": "external-meeting", "topic": "Vendor call", "tenantId": "other-tenant"})
    original = source.iter_message_pages
    async def denied(chat_id):
        if chat_id == "external-meeting":
            raise ChatGraphError("Graph capture metadata failed with HTTP 403", status_code=403)
        async for page in original(chat_id):
            yield page
    source.iter_message_pages = denied
    assert (await source_adapter.run(await legacy_scope(store)))["outcome"] == "complete"
    assert units == ["COMPLETED", "COMPLETED"]


def delta_source(source, feed, rounds, checks, *, latest_at=None):
    """Fake per-user delta feed (/users/{id}/chats/getAllMessages/delta) and per-chat check."""
    async def delta(link, *, since=None):
        rounds.append(link)
        changed = feed.pop(0) if link else set()
        if isinstance(changed, Exception):
            raise changed
        return changed, f"link-{len(rounds)}"

    async def latest(chat_id):
        checks.append(chat_id)
        return latest_at or datetime.now(timezone.utc)
    source.message_delta, source.latest_message_change = delta, latest


async def chat_state(store, snapshot_id):
    async with store.factory() as session:
        links = sorted((await session.execute(select(models.SnapshotChatInventory.chat_id, models.SnapshotChatInventory.inventory_id)
            .where(models.SnapshotChatInventory.snapshot_id == snapshot_id))).all())
        rows = sorted((await session.execute(select(models.SnapshotItem.external_id, models.SnapshotItem.chat_version_id)
            .where(models.SnapshotItem.snapshot_id == snapshot_id))).all())
    return links, rows


async def delta_cursor(store, resource_id):
    async with store.factory() as session:
        return ((await session.get(models.Resource, resource_id)).extra_data or {}).get("chat_delta_cursor")


async def test_incremental_links_every_chat_the_delta_feed_shows_unchanged_at_once(store, source_adapter):
    """Codex 2026-11-05: Microsoft's per-user delta feed reports new, edited and deleted messages
    across all of a user's chats, group chats included. An incremental links every chat the feed
    shows unchanged in one step (Railway 2026-11-05: 1,107 chats linked one by one took 6-12 min
    per user); only chats the feed reports changed take the per-chat path."""
    source = source_adapter.source
    source.chats.append({"id": "chat-beta", "topic": "Beta"})
    source.messages["chat-beta"] = [{"id": "message-beta", "body": {"content": "b"}, "attachments": []}]
    feed, rounds, checks = [], [], []
    delta_source(source, feed, rounds, checks, latest_at=datetime(2020, 1, 1, tzinfo=timezone.utc))
    first = await legacy_scope(store)
    assert (await source_adapter.run(first))["outcome"] == "complete"
    assert rounds == [None] and (await delta_cursor(store, first.resource_id))["link"] == "link-1"
    reads = list(source.message_calls)

    feed.append(set())
    second = await store.new_scope()
    assert (await source_adapter.run(second))["outcome"] == "complete"
    assert rounds == [None, "link-1"] and sorted(checks) == ["chat-alpha", "chat-beta"] and source.message_calls == reads
    before, after = await chat_state(store, first.snapshot_id), await chat_state(store, second.snapshot_id)
    assert after == before and len(after[0]) == 2
    async with store.factory() as session:
        attempts = (await session.scalars(select(models.ChatCaptureAttempt).where(
            models.ChatCaptureAttempt.snapshot_id == second.snapshot_id))).all()
    assert {a.state for a in attempts} == {"PUBLISHED"} and all(a.reused_inventory for a in attempts)
    assert all(any("reused_unchanged_from_attempt" in e for e in a.evidence_history) for a in attempts)
    assert (await delta_cursor(store, first.resource_id)) == {"link": "link-2", "snapshot_id": str(second.snapshot_id),
        "principal": first.principal_id, "scope": source.permission_scope_hash}

    async def latest(chat_id):
        checks.append(chat_id)
        return datetime.now(timezone.utc) if chat_id == "chat-beta" else datetime(2020, 1, 1, tzinfo=timezone.utc)
    source.latest_message_change = latest
    feed.append({"chat-beta"})
    third = await store.new_scope()
    assert (await source_adapter.run(third))["outcome"] == "complete"
    assert rounds[-1] == "link-2" and sorted(checks[2:]) == ["chat-alpha", "chat-beta"]
    assert set(source.message_calls[len(reads):]) == {"chat-beta"}
    assert len((await chat_state(store, third.snapshot_id))[0]) == 2


async def test_delta_feed_silence_never_links_a_chat_whose_message_was_edited(store, source_adapter):
    """Railway 2026-11-05: a 13-hour-old message in a group chat was edited at 14:38; at 14:42 every
    member's delta feed reported no change and all five linked the old version. Each chat the feed
    shows unchanged is still checked with one request before it is linked."""
    source = source_adapter.source
    source.chats.append({"id": "chat-beta", "topic": "Beta"})
    source.messages["chat-beta"] = [{"id": "message-beta", "body": {"content": "b"}, "attachments": []}]
    old = datetime(2020, 1, 1, tzinfo=timezone.utc)
    feed, rounds, checks = [], [], []
    delta_source(source, feed, rounds, checks, latest_at=old)
    first = await legacy_scope(store)
    await source_adapter.run(first)
    before, reads = await chat_state(store, first.snapshot_id), len(source.message_calls)

    source.messages["chat-beta"] = [{"id": "message-beta", "body": {"content": "edited"}, "attachments": []}]
    async def latest(chat_id):
        checks.append(chat_id)
        return datetime.now(timezone.utc) if chat_id == "chat-beta" else old
    source.latest_message_change = latest
    feed.append(set())
    second = await store.new_scope()
    assert (await source_adapter.run(second))["outcome"] == "complete"
    assert {"chat-alpha", "chat-beta"} <= set(checks)
    assert set(source.message_calls[reads:]) == {"chat-beta"}
    after = await chat_state(store, second.snapshot_id)
    assert len(after[0]) == 2 and after[1] != before[1]
    assert await chat_state(store, first.snapshot_id) == before


async def test_bulk_link_uses_the_check_another_member_shared_for_this_run(store, source_adapter, monkeypatch):
    """A group chat is checked once per run for all its members (Teams: 1 request/s per chat): when
    another member already found it changed in this run, this member does not link it in bulk."""
    from shared import chat_capture
    from shared.graph_rate_limiter import graph_rate_limiter
    data = {}

    class Redis:
        async def mget(self, keys):
            return [data.get(k) for k in keys]
        def pipeline(self, transaction=False):
            class Pipe:
                async def __aenter__(self):
                    return self
                async def __aexit__(self, *exc):
                    return False
                def set(self, key, value, ex=None, nx=False):
                    data.setdefault(key, value)
                async def execute(self):
                    return None
            return Pipe()
    async def shared():
        return Redis()
    monkeypatch.setattr(graph_rate_limiter, "redis", shared)
    source = source_adapter.source
    source.chats.append({"id": "chat-beta", "topic": "Beta"})
    source.messages["chat-beta"] = [{"id": "message-beta", "body": {"content": "b"}, "attachments": []}]
    feed, rounds, checks = [set()], [], []
    delta_source(source, feed, rounds, checks, latest_at=datetime(2020, 1, 1, tzinfo=timezone.utc))
    first = await legacy_scope(store)
    await source_adapter.run(first)
    second = await store.new_scope()
    async with store.factory() as session:
        job = (await session.get(models.Snapshot, second.snapshot_id)).job_id
    data[chat_capture._probe_key(str(job), str(second.tenant_id), "chat-beta")] = datetime.now(timezone.utc).isoformat()
    assert (await source_adapter.run(second))["outcome"] == "complete"
    async with store.factory() as session:
        attempts = {a.chat_id: a for a in (await session.scalars(select(models.ChatCaptureAttempt).where(
            models.ChatCaptureAttempt.snapshot_id == second.snapshot_id))).all()}
    bulk = {cid for cid, a in attempts.items() if "delta_unchanged_since_snapshot" in str(a.evidence_history)}
    assert bulk == {"chat-alpha"} and attempts["chat-beta"].state == "PUBLISHED"
    assert checks.count("chat-alpha") == 1


async def test_delta_feed_failure_falls_back_to_checking_each_chat(store, source_adapter):
    from shared.chat_capture_graph import ChatGraphError
    source = source_adapter.source
    feed, rounds, checks = [ChatGraphError("Graph capture metadata failed with HTTP 410", status_code=410)], [], []
    delta_source(source, feed, rounds, checks, latest_at=datetime(2020, 1, 1, tzinfo=timezone.utc))
    first = await legacy_scope(store)
    await source_adapter.run(first)
    second = await store.new_scope()
    assert (await source_adapter.run(second))["outcome"] == "complete"
    assert rounds == [None, "link-1", None] and checks == ["chat-alpha"]
    assert (await delta_cursor(store, first.resource_id))["link"] == "link-3"


async def test_a_run_that_is_not_complete_does_not_advance_the_delta_cursor(store, source_adapter):
    from shared.chat_capture_graph import ChatGraphError
    source = source_adapter.source
    feed, rounds, checks = [], [], []
    delta_source(source, feed, rounds, checks)
    first = await legacy_scope(store)
    await source_adapter.run(first)
    source.chats.append({"id": "same-tenant-denied", "topic": "Ours"})
    original = source.iter_message_pages
    async def denied(chat_id):
        if chat_id == "same-tenant-denied":
            raise ChatGraphError("Graph capture metadata failed with HTTP 403", status_code=403)
        async for page in original(chat_id):
            yield page
    source.iter_message_pages = denied
    feed.append(set())
    assert (await source_adapter.run(await store.new_scope()))["outcome"] == "partial"
    assert await delta_cursor(store, first.resource_id) is None


async def test_delta_run_skips_known_hosted_elsewhere_chats_and_already_linked_chats(store, source_adapter, monkeypatch):
    """Railway 2026-11-05 branch incremental: Rohit's 243 chats were linked in 30 s, but his run went on
    for minutes: 92 chats hosted in other tenants were probed again, and a resumed turn walked the
    243 linked chats one by one. A chat hosted elsewhere at the cursor's snapshot that the feed shows
    unchanged is listed hosted elsewhere again without a request; linked chats are not walked again."""
    from shared import chat_catalogue_entry
    from shared.chat_capture_graph import ChatGraphError
    source = source_adapter.source
    source.home_tenant_id = "home-tenant"
    source.chats.append({"id": "external-meeting", "topic": "Vendor call", "tenantId": "other-tenant"})
    feed, rounds, checks = [set(), set()], [], []
    delta_source(source, feed, rounds, checks, latest_at=datetime(2020, 1, 1, tzinfo=timezone.utc))
    plain = source.latest_message_change

    async def latest(chat_id):
        if chat_id == "external-meeting":
            checks.append(chat_id)
            raise ChatGraphError("Graph capture metadata failed with HTTP 403", status_code=403)
        return await plain(chat_id)
    source.latest_message_change = latest
    first = await legacy_scope(store)
    await source_adapter.run(first)
    assert checks == ["external-meeting"]
    second = await store.new_scope()
    assert (await source_adapter.run(second))["outcome"] == "complete"
    assert checks.count("external-meeting") == 1
    async with store.factory() as session:
        record = (await session.get(models.Snapshot, second.snapshot_id)).extra_data["chat_catalogue_scopes"]["resource"]
    assert record["complete"] and record["external_tenant_chats"] == ["external-meeting"]

    walked = []
    published = chat_catalogue_entry._published_chat

    async def counted(session, scope):
        walked.append(scope.chat_id)
        return await published(session, scope)
    monkeypatch.setattr(chat_catalogue_entry, "_published_chat", counted)
    assert (await source_adapter.run(second))["outcome"] == "complete"   # a resumed turn of the same snapshot
    assert walked == [] and checks.count("external-meeting") == 1
