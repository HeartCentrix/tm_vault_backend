"""Real legacy worker handoff and completion honor captured-label receipts."""
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import select

from test_chat_catalogue_postgres import store
from test_chat_presentation import legacy, drain_labels
from test_copy_worker_handoff_postgres import bw, prepare
from shared import models, copy_workflow

pytestmark = pytest.mark.integration


def worker(bw):
    instance = bw.BackupWorker.__new__(bw.BackupWorker)
    instance.worker_id = 'presentation-test'
    instance._heartbeat = SimpleNamespace(worker_uuid=uuid4())
    instance.flush_version_rows = AsyncMock(return_value=0)
    instance._stamp_resource_from_snapshot_row = AsyncMock()
    instance._check_snapshot_anomaly = AsyncMock()
    instance.audit_logger = SimpleNamespace(log=AsyncMock())
    instance.get_graph_client = AsyncMock(side_effect=AssertionError('no source refetch on resume'))
    return instance


@pytest.mark.parametrize('publication_fails', [False, True])
async def test_label_only_capture_gets_publication_task_and_resumes_without_source_refetch(store, monkeypatch, bw, publication_fails):
    scope, _ = await prepare(store, monkeypatch, bw)
    async with store.factory.begin() as db:
        db.add(models.SnapshotItem(snapshot_id=scope.snapshot_id, tenant_id=scope.tenant_id,
            backend_id=scope.backend_id, external_id='m', parent_external_id='chat-a',
            item_type='TEAMS_CHAT_MESSAGE', name='Message', folder_path='chats/Fallback'))
    instance = worker(bw)
    with copy_workflow.copy_collection(scope.snapshot_id):
        deferred = await copy_workflow.defer_current_copy('CHAT_LABELS', {'labels': [
            {'chat_id': 'chat-a', 'folder_path': 'chats/Frozen', 'quality': 3, 'provenance': 'current_topic'}]}, lane='micro')
        async with store.factory() as db:
            snapshot = await db.get(models.Snapshot, scope.snapshot_id)
        assert await instance._seal_copy_handoff(snapshot, {'resume_kind': 'user_content',
            'result': {'item_count': 1, 'bytes_added': 0}}, versions_flushed=True)
    async with store.factory() as db:
        tasks = list((await db.scalars(select(models.CopyWorkTask).where(
            models.CopyWorkTask.workflow_id == deferred.workflow_id))).all())
    assert {t.kind for t in tasks} == {'CHAT_LABELS', 'CHAT_PRESENTATION'}
    for kind in ['CHAT_LABELS', 'CHAT_PRESENTATION']:
        if kind == 'CHAT_PRESENTATION' and publication_fails:
            async with store.factory.begin() as db:
                (await db.get(models.CopyWorkTask, next(t.id for t in tasks if t.kind == kind))).state = 'FAILED'
            continue
        await drain_labels(store, next(t for t in tasks if t.kind == kind), terminal=False)
    assert await instance.resume_copy_workflow(deferred.workflow_id)
    async with store.factory() as db:
        snapshot = await db.get(models.Snapshot, scope.snapshot_id)
        assert snapshot.status == (models.SnapshotStatus.PARTIAL if publication_fails else models.SnapshotStatus.COMPLETED)
        if publication_fails:
            assert not snapshot.extra_data.get('chat_presentation_format')
            assert (await db.get(models.ChatPresentationPlan, scope.snapshot_id)).state == 'ABORTED'
        else:
            assert snapshot.extra_data['chat_presentation_format'] == 1
            assert (await db.get(models.SnapshotChatPresentation, (scope.snapshot_id, 'chat-a'))).folder_path == 'chats/Frozen'


async def test_persisted_incomplete_presentation_blocks_terminal_writes_even_with_producers_off(store, monkeypatch, bw):
    scope, _ = await prepare(store, monkeypatch, bw)
    monkeypatch.setenv('COPY_WORK_ENABLED', 'false')
    async with store.factory.begin() as db:
        db.add(models.ChatPresentationPlan(snapshot_id=scope.snapshot_id, state='PENDING'))
    instance = worker(bw)
    instance._compute_snapshot_delta_from_prior = AsyncMock(side_effect=AssertionError('incomplete names must block completion'))
    async with store.factory() as db:
        snapshot = await db.get(models.Snapshot, scope.snapshot_id)
        resource = await db.get(models.Resource, scope.resource_id)
        assert await instance.complete_snapshot(db, snapshot, {}) is False
        assert await instance._settle_inline_snapshot_finalize(db, snapshot, resource) == 'deferred_copy'
        assert snapshot.status == models.SnapshotStatus.IN_PROGRESS


@pytest.mark.parametrize('source_failed', [False, True])
async def test_partition_parent_queues_one_publication_after_source_settlement(store, monkeypatch, bw, source_failed):
    scope, pid = await prepare(store, monkeypatch, bw, partition=True)
    async with store.factory.begin() as db:
        partition = await db.get(models.SnapshotPartition, pid)
        partition.payload = {'chat_ids': ['chat-a']}
        db.add(models.SnapshotItem(snapshot_id=scope.snapshot_id, tenant_id=scope.tenant_id,
            backend_id=scope.backend_id, external_id='m', parent_external_id='chat-a',
            item_type='TEAMS_CHAT_MESSAGE', name='Message', folder_path='chats/Fallback'))
    with copy_workflow.copy_collection(scope.snapshot_id, pid, expected_partition_token=1):
        deferred = await copy_workflow.defer_current_copy('CHAT_LABELS', {'labels': [
            {'chat_id': 'chat-a', 'folder_path': 'chats/Frozen', 'quality': 3, 'provenance': 'current_topic'}]}, lane='micro')
        await copy_workflow.seal_current_copy({'resume_kind': 'partition', 'partition_id': str(pid)})
    async with store.factory() as db:
        task = await db.get(models.CopyWorkTask, deferred.task_id)
    await drain_labels(store, task)
    async with store.factory.begin() as db:
        (await db.get(models.SnapshotPartition, pid)).status = 'FAILED' if source_failed else 'COMPLETED'
    instance = worker(bw)
    instance._carry_forward_partitioned_user_mail = AsyncMock(return_value=0)
    assert await instance._finalize_partitioned_snapshot(scope.snapshot_id) is None
    assert await instance._finalize_partitioned_snapshot(scope.snapshot_id) is None
    async with store.factory() as db:
        published = list((await db.scalars(select(models.CopyWorkTask).join(models.CopyWorkflow,
            models.CopyWorkflow.id == models.CopyWorkTask.workflow_id).where(
            models.CopyWorkflow.target_snapshot_id == scope.snapshot_id,
            models.CopyWorkTask.kind == 'CHAT_PRESENTATION'))).all())
        assert len(published) == 1
        publication = published[0]
    await drain_labels(store, publication, terminal=False)
    assert await instance.resume_copy_workflow(publication.workflow_id)
    async with store.factory() as db:
        snapshot = await db.get(models.Snapshot, scope.snapshot_id)
        assert snapshot.status == (models.SnapshotStatus.FAILED if source_failed else models.SnapshotStatus.COMPLETED)
        assert snapshot.extra_data['chat_presentation_format'] == 1
