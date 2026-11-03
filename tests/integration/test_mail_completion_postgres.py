"""No-source mail carry requires durable coverage from this bootstrap capture."""
from datetime import datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import select

from test_chat_catalogue_postgres import store
from test_copy_worker_handoff_postgres import bw, prepare
from shared import copy_work, models

pytestmark = pytest.mark.integration


async def bootstrap(store, monkeypatch, bw, *, tokens=None):
    scope, _ = await prepare(store, monkeypatch, bw)
    worker = bw.BackupWorker.__new__(bw.BackupWorker)
    worker.worker_id = 'synthetic-mail-completion'
    async with store.factory.begin() as db:
        snapshot = await db.get(models.Snapshot, scope.snapshot_id)
        snapshot.extra_data = {}
        (await db.get(models.Resource, store.resource)).type = models.ResourceType.USER_MAIL
    await worker._record_mail_bootstrap_origin(snapshot, tokens or {}, ['inbox', 'sent'], cursor_read_ok=True)
    return scope, worker


async def finish_folders(store, scope, *, incomplete=False):
    async with store.factory.begin() as db:
        snapshot = await db.get(models.Snapshot, scope.snapshot_id)
        snapshot.extra_data = {**snapshot.extra_data, 'mail_union_carry_claimed': True,
                              'copy_mail_checkpoint_tokens': {'inbox': 'same-run-inbox', 'sent': 'same-run-sent'}}
        for index, folder in enumerate(['inbox', 'sent']):
            db.add(models.SnapshotPartition(snapshot_id=scope.snapshot_id,
                tenant_id=store.tenant, resource_id=store.resource, job_id=snapshot.job_id,
                partition_type='MAIL_FOLDERS', partition_index=index,
                status='FAILED' if incomplete and index else 'COMPLETED', lease_token=0,
                payload={'folder_ids': [folder], 'carry_forward': {
                    'resumed_any': True, 'excluded_paths': [], 'drained_keys': [folder],
                    'tombstoned_ids': [], 'all_folders_fallback': False}}))


async def enqueue(store, scope):
    async with store.factory.begin() as db:
        return await copy_work.enqueue(db, copy_work.CopyRequest(
            kind='MAIL_CARRY', lane='giant', target_snapshot_id=scope.snapshot_id))


async def test_no_prior_bootstrap_resumed_within_run_completes_without_execution(store, monkeypatch, bw):
    scope, worker = await bootstrap(store, monkeypatch, bw)
    await finish_folders(store, scope)
    # A redelivery observes cursors now; the original cursor-free proof survives.
    async with store.factory() as db:
        snapshot = await db.get(models.Snapshot, scope.snapshot_id)
    await worker._record_mail_bootstrap_origin(snapshot, {'inbox': 'resumed'}, ['inbox', 'sent'], cursor_read_ok=True)
    task = await enqueue(store, scope)
    assert task.state == 'COMPLETED' and task.prepared and task.attempts == 0
    assert task.row_count == task.bytes_count == task.checkpoint == 0
    async with store.factory() as db:
        snapshot = await db.get(models.Snapshot, scope.snapshot_id)
        assert snapshot.extra_data['mail_bootstrap_origin'] == {
            'cursor_free': True, 'folder_ids': ['inbox', 'sent']}
        assert snapshot.status == models.SnapshotStatus.IN_PROGRESS


@pytest.mark.parametrize('reason', ['prior-cursor', 'unknown-origin', 'failed-folder', 'missing-folder', 'cancelled'])
async def test_no_prior_never_completes_unproven_delta_coverage(store, monkeypatch, bw, reason):
    scope, _ = await bootstrap(store, monkeypatch, bw,
        tokens={'inbox': 'historical-cursor'} if reason == 'prior-cursor' else {})
    await finish_folders(store, scope, incomplete=reason == 'failed-folder')
    async with store.factory.begin() as db:
        snapshot = await db.get(models.Snapshot, scope.snapshot_id)
        extra = dict(snapshot.extra_data)
        if reason == 'unknown-origin':
            extra.pop('mail_bootstrap_origin')
        if reason == 'missing-folder':
            extra['copy_mail_checkpoint_tokens'] = {'inbox': 'same-run-inbox'}
            sent = await db.scalar(select(models.SnapshotPartition).where(
                models.SnapshotPartition.snapshot_id == snapshot.id, models.SnapshotPartition.partition_index == 1))
            sent.payload = {'folder_ids': ['sent'], 'carry_forward': {'resumed_any': True, 'drained_keys': []}}
        if reason == 'cancelled':
            extra['cancelled_at'] = 'synthetic-cancellation'
        snapshot.extra_data = extra
    task = await enqueue(store, scope)
    assert task.state == ('CANCELLED' if reason == 'cancelled' else 'FAILED')
    assert task.error == {'code': 'copy_parent_cancelled' if reason == 'cancelled' else 'mail_bootstrap_coverage_unproven'}
    assert not task.prepared and task.row_count == 0


async def test_prior_backed_incremental_remains_queued_for_copy(store, monkeypatch, bw):
    prior = await store.new_scope()
    async with store.factory.begin() as db:
        snapshot = await db.get(models.Snapshot, prior.snapshot_id)
        snapshot.status = models.SnapshotStatus.COMPLETED
        snapshot.created_at = datetime.utcnow() - timedelta(days=1)
        db.add(models.SnapshotItem(snapshot_id=snapshot.id, tenant_id=store.tenant,
            item_type='EMAIL', external_id='historical-mail', name='mail', content_size=7, backend_id=store.backend))
    scope, _ = await bootstrap(store, monkeypatch, bw, tokens={'inbox': 'historical-cursor'})
    await finish_folders(store, scope)
    task = await enqueue(store, scope)
    assert task.state == 'READY' and not task.prepared
    assert task.params['prior_snapshot_id'] == str(prior.snapshot_id)


async def test_bootstrap_origin_cannot_be_created_after_unattributed_mail_writes(store, monkeypatch, bw):
    scope, _ = await prepare(store, monkeypatch, bw)
    async with store.factory.begin() as db:
        snapshot = await db.get(models.Snapshot, scope.snapshot_id)
        db.add(models.SnapshotItem(snapshot_id=snapshot.id, tenant_id=store.tenant,
            item_type='EMAIL', external_id='unattributed', name='mail', content_size=7, backend_id=store.backend))
    worker = bw.BackupWorker.__new__(bw.BackupWorker)
    await worker._record_mail_bootstrap_origin(snapshot, {}, ['inbox'], cursor_read_ok=True)
    async with store.factory() as db:
        assert 'mail_bootstrap_origin' not in (await db.get(models.Snapshot, scope.snapshot_id)).extra_data


async def test_no_prior_with_durable_bootstrap_origin_is_noop(store, monkeypatch, bw):
    scope, _ = await prepare(store, monkeypatch, bw)
    async with store.factory.begin() as db:
        snapshot = await db.get(models.Snapshot, scope.snapshot_id)
        snapshot.extra_data = {'mail_bootstrap_origin': {'cursor_free': True, 'folder_ids': ['inbox', 'sent']}}
        (await db.get(models.Resource, store.resource)).type = models.ResourceType.USER_MAIL
    await finish_folders(store, scope)
    task = await enqueue(store, scope)
    assert task.state == 'COMPLETED' and task.prepared


async def test_validated_noop_uses_fenced_resume_to_finalize_mail_snapshot(store, monkeypatch, bw):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    scope, worker = await bootstrap(store, monkeypatch, bw)
    await finish_folders(store, scope)
    async with store.factory.begin() as db:
        snapshot = await db.get(models.Snapshot, scope.snapshot_id)
        snapshot.extra_data = {key: value for key, value in snapshot.extra_data.items()
                               if key != 'mail_union_carry_claimed'}
        snapshot.extra_data = {**snapshot.extra_data, 'partitioned': True}
        db.add(models.SnapshotItem(snapshot_id=snapshot.id, tenant_id=store.tenant,
            item_type='EMAIL', external_id='fresh', name='mail', content_size=7, backend_id=store.backend))
    worker.flush_version_rows = AsyncMock(return_value=0)
    worker.audit_logger = SimpleNamespace(log=AsyncMock())
    worker._check_snapshot_anomaly = AsyncMock()
    worker._stamp_resource_from_snapshot_row = AsyncMock()
    worker.get_graph_client = AsyncMock(side_effect=AssertionError('no Graph during no-op resume'))
    assert await worker._finalize_partitioned_snapshot(scope.snapshot_id) is None
    async with store.factory() as db:
        task = await db.scalar(select(models.CopyWorkTask))
        assert task.state == 'COMPLETED' and task.attempts == 0
        snapshot = await db.get(models.Snapshot, scope.snapshot_id)
        assert snapshot.status == models.SnapshotStatus.IN_PROGRESS
        assert snapshot.extra_data['mail_carry_forward_pending'] is True
    assert await worker.resume_copy_workflow(task.workflow_id)
    async with store.factory() as db:
        snapshot = await db.get(models.Snapshot, scope.snapshot_id)
        assert snapshot.status == models.SnapshotStatus.COMPLETED
        assert snapshot.item_count == 1 and snapshot.bytes_total == 7
        assert snapshot.extra_data['mail_carry_forward_pending'] is False
        assert not snapshot.extra_data.get('copy_required_failed')


async def test_cursor_free_capture_resumes_only_its_own_durable_checkpoint_tokens(store, monkeypatch, bw):
    scope, worker = await bootstrap(store, monkeypatch, bw)
    async with store.factory() as db:
        snapshot = await db.get(models.Snapshot, scope.snapshot_id)
    tokens = await worker._record_mail_bootstrap_origin(
        snapshot, {'inbox': 'other-job-cursor', 'sent': 'other-job-cursor'}, ['inbox', 'sent'],
        cursor_read_ok=True)
    assert tokens == {}  # No folder in this snapshot has completed its baseline.
    async with store.factory.begin() as db:
        live = await db.get(models.Snapshot, scope.snapshot_id)
        live.extra_data = {**live.extra_data, 'copy_mail_checkpoint_tokens': {'inbox': 'own-checkpoint'}}
    tokens = await worker._record_mail_bootstrap_origin(
        snapshot, {'inbox': 'other-job-cursor', 'sent': 'other-job-cursor'}, ['inbox', 'sent'],
        cursor_read_ok=True)
    assert tokens == {'inbox': 'own-checkpoint'}


async def test_failed_authoritative_cursor_read_cannot_prove_cursor_free_bootstrap(store, monkeypatch, bw):
    from sqlalchemy import text
    scope, _ = await prepare(store, monkeypatch, bw)
    worker = bw.BackupWorker.__new__(bw.BackupWorker)
    worker.worker_id = 'synthetic-mail-read-failure'
    async with store.factory.begin() as db:
        snapshot = await db.get(models.Snapshot, scope.snapshot_id)
        (await db.get(models.Resource, store.resource)).type = models.ResourceType.USER_MAIL
        db.add(models.MailFolderDelta(resource_id=store.resource, folder_id='inbox', delta_token='hidden-historical-cursor'))
    async with store.factory.begin() as db:
        await db.execute(text('ALTER TABLE mail_folder_delta RENAME TO unavailable_mail_folder_delta'))
    read_result = {}
    tokens = await worker._load_mail_folder_deltas(store.resource, read_result=read_result)
    assert tokens == {} and read_result == {'ok': False}
    await worker._record_mail_bootstrap_origin(snapshot, tokens, ['inbox', 'sent'],
        cursor_read_ok=read_result['ok'])
    async with store.factory() as db:
        snapshot = await db.get(models.Snapshot, scope.snapshot_id)
        assert snapshot.extra_data['mail_bootstrap_origin']['cursor_free'] is False
    await finish_folders(store, scope)
    task = await enqueue(store, scope)
    assert task.state == 'FAILED' and task.error == {'code': 'mail_bootstrap_coverage_unproven'}
