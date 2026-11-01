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
    with bw._worker_copy_collection(scope.snapshot_id, resource_id=store.resource):
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


@pytest.mark.parametrize("failed", [False, True])
async def test_partition_mail_union_is_queued_once_before_any_terminal_snapshot_write(store, monkeypatch, bw, failed):
    scope, pid = await prepare(store, monkeypatch, bw, partition=True)
    async with store.factory.begin() as session:
        resource = await session.get(models.Resource, store.resource)
        resource.type = models.ResourceType.USER_MAIL
        partition = await session.get(models.SnapshotPartition, pid)
        partition.status = "COMPLETED"
        partition.partition_type = "MAIL_FOLDERS"
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
        assert snapshot.status == (models.SnapshotStatus.FAILED if failed else models.SnapshotStatus.COMPLETED)
