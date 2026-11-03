"""Worker settlement locks parents before dirty snapshot writes."""
import pytest
from sqlalchemy import event
from shared import models
from test_chat_catalogue_postgres import store
from test_copy_worker_handoff_postgres import bw

pytestmark = pytest.mark.integration



async def test_inline_settlement_locks_job_before_dirty_snapshot_flush(store, monkeypatch, bw):
    scope = await store.new_scope()
    monkeypatch.setattr(bw, 'async_session_factory', store.factory)
    worker = bw.BackupWorker.__new__(bw.BackupWorker)
    worker.worker_id = 'lock-order-test'
    statements = []

    def record(conn, cursor, statement, parameters, context, executemany):
        statements.append(statement.lower())

    async with store.factory.begin() as db:
        snapshot = await db.get(models.Snapshot, scope.snapshot_id)
        resource = await db.get(models.Resource, store.resource)
        resource.type = models.ResourceType.USER_MAIL
        await db.flush()
        snapshot.item_count = 7  # The actual caller dirties counters before settlement.
        event.listen(store.engine.sync_engine, 'before_cursor_execute', record)
        try:
            assert await worker._settle_inline_snapshot_finalize(db, snapshot, resource) == 'COMPLETED'
            await db.flush()
        finally:
            event.remove(store.engine.sync_engine, 'before_cursor_execute', record)
    first_write = next(i for i, sql in enumerate(statements) if sql.startswith('update snapshots'))
    parent_locks = [i for i, sql in enumerate(statements) if 'from jobs' in sql and ('for share' in sql or 'for key share' in sql)]
    assert parent_locks and parent_locks[0] < first_write, 'Dirty counters must not invert Job -> Snapshot order'
    async with store.factory() as db:
        saved = await db.get(models.Snapshot, scope.snapshot_id)
        assert saved.item_count == 7 and saved.status == models.SnapshotStatus.COMPLETED


@pytest.mark.parametrize('terminal', [models.SnapshotStatus.FAILED, models.SnapshotStatus.PENDING_DELETION])
async def test_inline_settlement_preserves_concurrently_terminal_snapshot(store, monkeypatch, bw, terminal):
    scope = await store.new_scope()
    monkeypatch.setattr(bw, 'async_session_factory', store.factory)
    worker = bw.BackupWorker.__new__(bw.BackupWorker)
    worker.worker_id = 'stale-settlement-test'
    async with store.factory.begin() as setup:
        resource = await setup.get(models.Resource, store.resource)
        resource.type = models.ResourceType.USER_MAIL
    async with store.factory() as db:
        snapshot = await db.get(models.Snapshot, scope.snapshot_id)
        resource = await db.get(models.Resource, store.resource)
        async with store.factory.begin() as other:
            live = await other.get(models.Snapshot, scope.snapshot_id)
            live.status = terminal
            live.extra_data = {'cancelled_at': '2026-11-03T00:00:00Z'}
        snapshot.item_count = 7
        outcome = await worker._settle_inline_snapshot_finalize(db, snapshot, resource)
        await db.commit()
    assert outcome == 'skipped_terminal'
    async with store.factory() as db:
        saved = await db.get(models.Snapshot, scope.snapshot_id)
        assert saved.status == terminal
        assert saved.extra_data['cancelled_at'] == '2026-11-03T00:00:00Z'


async def test_inline_settlement_sees_degradation_written_while_status_stays_running(store, monkeypatch, bw):
    scope = await store.new_scope()
    monkeypatch.setattr(bw, 'async_session_factory', store.factory)
    worker = bw.BackupWorker.__new__(bw.BackupWorker)
    worker.worker_id = 'degraded-settlement-test'
    async with store.factory.begin() as setup:
        resource = await setup.get(models.Resource, store.resource)
        resource.type = models.ResourceType.USER_MAIL
    async with store.factory() as db:
        snapshot = await db.get(models.Snapshot, scope.snapshot_id)
        resource = await db.get(models.Resource, store.resource)
        async with store.factory.begin() as other:
            live = await other.get(models.Snapshot, scope.snapshot_id)
            live.extra_data = {**live.extra_data, 'degraded_reason': 'concurrent attachment failure'}
        snapshot.item_count = 7
        assert await worker._settle_inline_snapshot_finalize(db, snapshot, resource) == 'PARTIAL'
        await db.commit()
    async with store.factory() as db:
        saved = await db.get(models.Snapshot, scope.snapshot_id)
        assert saved.status == models.SnapshotStatus.PARTIAL and saved.item_count == 7
        assert saved.extra_data['failure_reason'] == 'concurrent attachment failure'
