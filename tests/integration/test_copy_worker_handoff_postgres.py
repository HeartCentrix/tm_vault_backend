"""Legacy worker suspension and continuation use actual persisted leaf ownership."""
from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import select, update

from test_chat_catalogue_postgres import store
from shared import models
from shared import copy_workflow

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def bw():
    import sys
    path = Path(__file__).resolve().parents[2] / "workers/backup-worker/main.py"
    spec = importlib.util.spec_from_file_location("bw_copy_handoff_postgres", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


async def prepare(store, monkeypatch, bw, *, partition=False):
    monkeypatch.setenv("COPY_WORK_ENABLED", "true")
    monkeypatch.setattr(copy_workflow, "_COPY_GUARDS_ACTIVE", False)
    monkeypatch.setattr(copy_workflow, "async_session_factory", store.factory)
    monkeypatch.setattr(bw, "async_session_factory", store.factory)
    scope = await store.new_scope()
    pid = uuid4() if partition else None
    async with store.factory.begin() as session:
        snapshot = await session.get(models.Snapshot, scope.snapshot_id)
        snapshot.extra_data = {"partitioned": True} if partition else {}
        resource = await session.get(models.Resource, store.resource)
        resource.extra_data = {}
        if partition:
            session.add(models.SnapshotPartition(id=pid, snapshot_id=snapshot.id,
                tenant_id=store.tenant, resource_id=store.resource, job_id=snapshot.job_id,
                drive_id="", file_ids=[], partition_index=0, total_files=0, total_bytes_est=0,
                partition_type="CHATS", status="IN_PROGRESS", lease_token=1))
    return scope, pid


@pytest.mark.parametrize("failed", [False, True])
async def test_actual_worker_pending_guard_then_resume_preserves_capture_and_finalizes(store, monkeypatch, bw, failed):
    scope, _ = await prepare(store, monkeypatch, bw)
    async with store.factory.begin() as session:
        session.add(models.SnapshotItem(snapshot_id=scope.snapshot_id, tenant_id=store.tenant,
            external_id="captured", item_type="TEAMS_CHAT_MESSAGE", name="captured", content_size=7, backend_id=store.backend))
    with copy_workflow.copy_collection(scope.snapshot_id):
        deferred = await bw._carry_forward_prior_chat_snapshot_items(
            tenant_id=str(store.tenant), resource_id=str(store.resource), target_snapshot_id=str(scope.snapshot_id))
        waiting = await copy_workflow.seal_current_copy({"resume_kind": "user_content", "result": {}})
    assert waiting["copy_waiting"] and isinstance(deferred, copy_workflow.DeferredCopy)
    worker = bw.BackupWorker.__new__(bw.BackupWorker)
    worker.worker_id = "synthetic-copy-worker"
    worker._heartbeat = SimpleNamespace(worker_uuid=uuid4())
    worker.flush_version_rows = AsyncMock(return_value=0)
    worker._stamp_resource_from_snapshot_row = AsyncMock()
    worker._check_snapshot_anomaly = AsyncMock()
    worker.audit_logger = SimpleNamespace(log=AsyncMock())
    worker.get_graph_client = AsyncMock(side_effect=AssertionError("no Graph on resume"))
    async with store.factory() as session:
        snapshot = await session.get(models.Snapshot, scope.snapshot_id)
        resource = await session.get(models.Resource, store.resource)
        assert await worker._settle_inline_snapshot_finalize(session, snapshot, resource) == "deferred_copy"
        assert await worker.complete_snapshot(session, snapshot, {}) is False
    async with store.factory.begin() as session:
        await session.execute(update(models.CopyWorkTask).where(models.CopyWorkTask.id == deferred.task_id).values(state="FAILED" if failed else "COMPLETED"))
    assert await worker.resume_copy_workflow(deferred.workflow_id)
    async with store.factory() as session:
        snapshot = await session.get(models.Snapshot, scope.snapshot_id)
        workflow = await session.get(models.CopyWorkflow, deferred.workflow_id)
        assert snapshot.status == (models.SnapshotStatus.PARTIAL if failed else models.SnapshotStatus.COMPLETED)
        assert snapshot.item_count == 1 and snapshot.bytes_total == 7
        assert workflow.state == ("PARTIAL" if failed else "COMPLETED")
        assert await session.scalar(select(models.SnapshotItem.id).where(models.SnapshotItem.snapshot_id == snapshot.id))


async def test_partition_redelivery_cannot_claim_suspended_copy_leaf(store, monkeypatch, bw):
    scope, pid = await prepare(store, monkeypatch, bw, partition=True)
    with copy_workflow.copy_collection(scope.snapshot_id, pid):
        await bw._copy_chat_attachment_rows_to_snapshot(
            tenant_id=str(store.tenant), target_snapshot_id=str(scope.snapshot_id), chat_id="chat-alpha")
        await copy_workflow.seal_current_copy({"resume_kind": "partition", "partition_id": str(pid), "result": {}})
    worker = bw.BackupWorker.__new__(bw.BackupWorker)
    worker.worker_id = "synthetic-copy-worker"
    worker._heartbeat = SimpleNamespace(worker_uuid=uuid4())
    assert await worker._claim_partition(pid, worker.worker_id) is None
    async with store.factory() as session:
        partition = await session.get(models.SnapshotPartition, pid)
        assert partition.status == "IN_PROGRESS" and partition.lease_owner_id is None
        assert await copy_workflow.has_pending_copy(session, partition_id=pid)


async def test_mail_cursor_checkpoints_stay_staged_until_no_copy_capture_is_drained(store, monkeypatch, bw):
    scope, _ = await prepare(store, monkeypatch, bw)
    worker = bw.BackupWorker.__new__(bw.BackupWorker)
    worker.worker_id = "synthetic-copy-worker"
    worker.flush_version_rows = AsyncMock(return_value=0)
    async with store.factory() as session:
        snapshot = await session.get(models.Snapshot, scope.snapshot_id)
    with bw._worker_copy_collection(scope.snapshot_id, resource_id=store.resource,
                                    expected_snapshot_token=snapshot.lease_token):
        await worker._upsert_mail_folder_delta(store.resource, "inbox", "pending-cursor")
        await worker._persist_mail_delta_state(store.resource,
            new_tokens={"inbox": "pending-cursor"}, ok_token_keys={"inbox"},
            drained_fids={"inbox"}, fingerprints_now={"inbox": "1|0|7"})
        async with store.factory() as session:
            assert not list((await session.scalars(select(models.MailFolderDelta))).all())
            staged = await session.get(models.Snapshot, scope.snapshot_id)
            assert staged.extra_data["copy_mail_cursor_states"]["snapshot"]["new_tokens"] == {"inbox": "pending-cursor"}
        assert await worker._seal_copy_handoff(snapshot, {"resume_kind": "user_content"}) is None
    async with store.factory() as session:
        cursor = await session.scalar(select(models.MailFolderDelta))
        assert cursor.delta_token == "pending-cursor"
        snapshot = await session.get(models.Snapshot, scope.snapshot_id)
        assert snapshot.extra_data["copy_mail_cursor_states"] == {}


@pytest.mark.parametrize("failed,retained_item,expected", [
    (False, False, models.SnapshotStatus.COMPLETED),
    (True, False, models.SnapshotStatus.FAILED),
    (True, True, models.SnapshotStatus.PARTIAL),
])
async def test_partition_mail_union_is_queued_once_before_any_terminal_snapshot_write(
        store, monkeypatch, bw, failed, retained_item, expected):
    scope, pid = await prepare(store, monkeypatch, bw, partition=True)
    async with store.factory.begin() as session:
        resource = await session.get(models.Resource, store.resource)
        resource.type = models.ResourceType.USER_MAIL
        partition = await session.get(models.SnapshotPartition, pid)
        partition.status = "COMPLETED"
        partition.partition_type = "MAIL_FOLDERS"
        if retained_item:
            session.add(models.SnapshotItem(snapshot_id=scope.snapshot_id, tenant_id=store.tenant,
                external_id="captured", item_type="EMAIL", name="captured", content_size=7, backend_id=store.backend))
            partition.files_uploaded, partition.bytes_uploaded = 1, 7
        partition.payload = {"carry_forward": {
            "resumed_any": True, "excluded_paths": [], "tombstoned_ids": ["deleted-message"],
            "drained_keys": ["inbox"], "all_folders_fallback": False,
        }}
    worker = bw.BackupWorker.__new__(bw.BackupWorker)
    worker.worker_id = "synthetic-copy-worker"
    worker.flush_version_rows = AsyncMock(return_value=0)
    assert await worker._finalize_partitioned_snapshot(scope.snapshot_id) is None
    assert await worker._finalize_partitioned_snapshot(scope.snapshot_id) is None
    async with store.factory() as session:
        snapshot = await session.get(models.Snapshot, scope.snapshot_id)
        tasks = list((await session.scalars(select(models.CopyWorkTask))).all())
        workflows = list((await session.scalars(select(models.CopyWorkflow))).all())
        assert snapshot.status == models.SnapshotStatus.IN_PROGRESS
        assert snapshot.completed_at is None
        assert snapshot.extra_data["mail_carry_forward_pending"] is True
        assert len(tasks) == len(workflows) == 1
        assert tasks[0].kind == "MAIL_CARRY"
        assert tasks[0].params["tombstoned_ids"] == ["deleted-message"]
        assert workflows[0].state == "WAITING" and workflows[0].partition_id is None

    worker.audit_logger = SimpleNamespace(log=AsyncMock())
    worker._check_snapshot_anomaly = AsyncMock()
    worker._stamp_resource_from_snapshot_row = AsyncMock()
    async with store.factory.begin() as session:
        await session.execute(update(models.CopyWorkTask).where(models.CopyWorkTask.id == tasks[0].id)
                              .values(state="FAILED" if failed else "COMPLETED"))
    assert await worker.resume_copy_workflow(workflows[0].id)
    async with store.factory() as session:
        snapshot = await session.get(models.Snapshot, scope.snapshot_id)
        workflow = await session.get(models.CopyWorkflow, workflows[0].id)
        job = await session.get(models.Job, snapshot.job_id)
        assert snapshot.status == expected
        assert snapshot.extra_data["mail_carry_forward_pending"] is False
        assert workflow.state == ("PARTIAL" if failed else "COMPLETED")
        assert not workflow.parent_settle_pending and workflow.lease_owner_id is None
        assert job.status == (models.JobStatus.FAILED if expected == models.SnapshotStatus.FAILED
            else models.JobStatus.COMPLETED)
        if failed:
            assert snapshot.extra_data["copy_required_failed"] is True
            assert snapshot.extra_data["degraded_reason"] == "required database copy did not complete"
        if retained_item:
            assert await session.scalar(select(models.SnapshotItem.id)
                .where(models.SnapshotItem.snapshot_id == snapshot.id, models.SnapshotItem.external_id == "captured"))


async def test_replaced_union_resume_cannot_clear_pending_carry(store, monkeypatch, bw):
    scope, pid = await prepare(store, monkeypatch, bw, partition=True)
    async with store.factory.begin() as session:
        (await session.get(models.Resource, store.resource)).type = models.ResourceType.USER_MAIL
        partition = await session.get(models.SnapshotPartition, pid)
        partition.status, partition.partition_type = "COMPLETED", "MAIL_FOLDERS"
        partition.payload = {"carry_forward": {"resumed_any": True, "excluded_paths": [],
            "tombstoned_ids": [], "drained_keys": ["inbox"], "all_folders_fallback": False}}
    worker = bw.BackupWorker.__new__(bw.BackupWorker)
    worker.worker_id = "synthetic-stale-union-worker"
    worker.flush_version_rows = AsyncMock(return_value=0)
    assert await worker._finalize_partitioned_snapshot(scope.snapshot_id) is None
    async with store.factory.begin() as session:
        workflow = await session.scalar(select(models.CopyWorkflow))
        workflow_id = workflow.id
        await session.execute(update(models.CopyWorkTask).values(state="FAILED"))
    publish = worker._publish_copy_mail_state
    replacement_owner = uuid4()

    async def replace_after_publish(snapshot, *, failed=False):
        await publish(snapshot, failed=failed)
        async with store.factory.begin() as session:
            workflow = await session.get(models.CopyWorkflow, workflow_id)
            workflow.generation += 1
            workflow.lease_owner_id = replacement_owner

    monkeypatch.setattr(worker, "_publish_copy_mail_state", replace_after_publish)
    with pytest.raises(bw.ChatCatalogueWorkLeaseMoved):
        await worker.resume_copy_workflow(workflow_id)
    async with store.factory() as session:
        snapshot = await session.get(models.Snapshot, scope.snapshot_id)
        workflow = await session.get(models.CopyWorkflow, workflow_id)
        assert snapshot.status == models.SnapshotStatus.IN_PROGRESS
        assert snapshot.extra_data["mail_carry_forward_pending"] is True
        assert snapshot.extra_data["copy_required_failed"] is True
        assert workflow.lease_owner_id == replacement_owner


@pytest.mark.parametrize("staged", [False, True])
@pytest.mark.parametrize("fault", ["table", "mirror"])
async def test_failed_union_requires_verified_cursor_rewind_before_finishing(store, monkeypatch, bw, staged, fault):
    from contextlib import asynccontextmanager
    from datetime import datetime, timedelta, timezone

    scope, pid = await prepare(store, monkeypatch, bw, partition=True)
    keys = {"inbox", "fingerprint-only"} if staged else {"inbox"}
    async with store.factory.begin() as session:
        resource = await session.get(models.Resource, store.resource)
        resource.type = models.ResourceType.USER_MAIL
        resource.extra_data = {"mail_delta_tokens_by_folder": {k: "advanced" for k in keys},
            "mail_folder_fingerprints": {k: "1|0|7" for k in keys}}
        partition = await session.get(models.SnapshotPartition, pid)
        partition.status, partition.partition_type = "COMPLETED", "MAIL_FOLDERS"
        partition.files_uploaded, partition.bytes_uploaded = 1, 7
        partition.payload = {"carry_forward": {"resumed_any": True, "excluded_paths": [],
            "tombstoned_ids": [], "drained_keys": ["inbox"], "all_folders_fallback": False}}
        session.add(models.SnapshotItem(snapshot_id=scope.snapshot_id, tenant_id=store.tenant,
            external_id="retained", item_type="EMAIL", name="retained", content_size=7, backend_id=store.backend))
        for key in keys:
            session.add(models.MailFolderDelta(resource_id=store.resource, folder_id=key, delta_token="advanced"))
        if staged:
            snapshot = await session.get(models.Snapshot, scope.snapshot_id)
            snapshot.extra_data = {**snapshot.extra_data, "copy_mail_cursor_states": {"snapshot": {
                "resource_id": str(store.resource), "ok_token_keys": ["inbox"],
                "drained_fids": sorted(keys), "evicted_token_keys": [], "new_tokens": {"inbox": "advanced"},
                "fingerprints_now": {k: "1|0|7" for k in keys}, "write_legacy_mirror": True}}}
    worker = bw.BackupWorker.__new__(bw.BackupWorker)
    worker.worker_id = "rewind-recovery-worker"
    worker.flush_version_rows = AsyncMock(return_value=0)
    worker.audit_logger = SimpleNamespace(log=AsyncMock())
    worker._check_snapshot_anomaly = AsyncMock()
    worker._stamp_resource_from_snapshot_row = AsyncMock()
    await worker._finalize_partitioned_snapshot(scope.snapshot_id)
    async with store.factory.begin() as session:
        workflow_id = await session.scalar(select(models.CopyWorkflow.id))
        await session.execute(update(models.CopyWorkTask).values(state="FAILED"))
    failing = {"enabled": True}
    original_delete = worker._delete_mail_folder_delta
    original_bulk = worker._delete_mail_folder_state_bulk

    async def delete(*args):
        return False if fault == "table" and failing["enabled"] else await original_delete(*args)

    async def bulk(*args):
        return False if fault == "table" and failing["enabled"] else await original_bulk(*args)

    @asynccontextmanager
    async def factory():
        async with store.factory() as session:
            commit = session.commit

            async def guarded_commit():
                if fault == "mirror" and failing["enabled"] and any(isinstance(r, models.Resource) for r in session.dirty):
                    raise RuntimeError("synthetic mirror commit failure")
                await commit()

            session.commit = guarded_commit
            yield session

    factory.begin = store.factory.begin
    monkeypatch.setattr(worker, "_delete_mail_folder_delta", delete)
    monkeypatch.setattr(worker, "_delete_mail_folder_state_bulk", bulk)
    monkeypatch.setattr(bw, "async_session_factory", factory)
    with pytest.raises(RuntimeError, match="rewind"):
        await worker.resume_copy_workflow(workflow_id)
    async with store.factory() as session:
        snapshot = await session.get(models.Snapshot, scope.snapshot_id)
        assert snapshot.status == models.SnapshotStatus.IN_PROGRESS
        assert snapshot.extra_data["mail_carry_forward_pending"] is True
        assert snapshot.extra_data["copy_required_failed"] is True
    failing["enabled"] = False
    async with store.factory.begin() as session:
        workflow = await session.get(models.CopyWorkflow, workflow_id)
        workflow.lease_expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    assert await worker.resume_copy_workflow(workflow_id)
    async with store.factory() as session:
        snapshot = await session.get(models.Snapshot, scope.snapshot_id)
        resource = await session.get(models.Resource, store.resource)
        assert snapshot.status == models.SnapshotStatus.PARTIAL
        assert snapshot.extra_data["mail_carry_forward_pending"] is False
        assert snapshot.extra_data["mail_carry_forward_failed"] is True
        assert not list((await session.scalars(select(models.MailFolderDelta))).all())
        assert not resource.extra_data["mail_delta_tokens_by_folder"]
        assert not resource.extra_data["mail_folder_fingerprints"]


@pytest.mark.parametrize("partitioned", [False, True])
async def test_stale_source_capture_cannot_attach_first_intent_to_replacement_workflow(store, monkeypatch, bw, partitioned):
    import asyncio
    import contextvars
    import sys

    scope, pid = await prepare(store, monkeypatch, bw, partition=partitioned)
    async with store.factory() as session:
        captured_snapshot = await session.get(models.Snapshot, scope.snapshot_id)
    captured_token = captured_snapshot.lease_token
    leaf_model = models.SnapshotPartition if pid else models.Snapshot
    leaf_id = pid or scope.snapshot_id
    async with store.factory() as session:
        leaf = await session.get(leaf_model, leaf_id)
        captured_leaf_token = leaf.lease_token
    if pid:
        bw._bind_partition_lease(pid, {"snapshot_id": scope.snapshot_id,
                                     "lease_owner_id": leaf.lease_owner_id,
                                     "lease_token": captured_leaf_token})
    replacement_owner = uuid4()
    replacement = []

    async def replace_capture():
        async with store.factory.begin() as session:
            await session.execute(update(leaf_model).where(leaf_model.id == leaf_id)
                                  .values(lease_token=captured_leaf_token + 1, lease_owner_id=replacement_owner))
        # A different handler legitimately begins its own collection/workflow.
        with copy_workflow.copy_collection(scope.snapshot_id, pid):
            replacement.append(await copy_workflow.defer_current_copy("CHAT_CARRY", {
                "target_snapshot_id": str(scope.snapshot_id), "replacement_capture": True,
            }))

    async def source_phase(graph, resource, snapshot, tenant, **kwargs):
        assert snapshot.lease_token == captured_token
        await asyncio.create_task(replace_capture(), context=contextvars.Context())
        # The stale handler reaches its first copy only after the replacement claimed.
        return await bw._carry_forward_prior_chat_snapshot_items(
            tenant_id=str(store.tenant), resource_id=str(store.resource), target_snapshot_id=str(scope.snapshot_id))

    monkeypatch.setitem(sys.modules, "archive_backup", SimpleNamespace(run_archive_backup=source_phase))
    worker = bw.BackupWorker.__new__(bw.BackupWorker)
    worker.worker_id = "stale-capture-worker"
    try:
        with pytest.raises(copy_workflow.CopyWorkConflict):
            await worker.backup_archive(object(), SimpleNamespace(id=store.resource), captured_snapshot,
                                        SimpleNamespace(id=store.tenant), {"partitionId": str(pid)} if pid else {})
    finally:
        if pid:
            bw._bind_partition_lease(pid, None)
    async with store.factory() as session:
        workflow = await session.get(models.CopyWorkflow, replacement[0].workflow_id)
        tasks = list((await session.scalars(select(models.CopyWorkTask))).all())
        snapshot = await session.get(models.Snapshot, scope.snapshot_id)
        assert workflow.state == "DRAINING" and workflow.parent_lease_token == captured_leaf_token + 1
        assert len(tasks) == 1 and tasks[0].id == replacement[0].task_id
        leaf = await session.get(leaf_model, leaf_id)
        assert leaf.lease_token == captured_leaf_token + 1 and leaf.lease_owner_id == replacement_owner
        if pid:
            assert snapshot.lease_token == captured_token
