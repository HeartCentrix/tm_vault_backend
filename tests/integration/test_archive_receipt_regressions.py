"""Archive receipt and settlement regressions reproduced by independent review."""
import asyncio
from uuid import uuid4
from unittest.mock import AsyncMock
import pytest
from sqlalchemy import select, update
from test_chat_catalogue_postgres import store
from test_archive_capture_postgres import capture, observed_item, content_row, BASE
import importlib.util
from pathlib import Path
_spec = importlib.util.spec_from_file_location('archive_worker_regression_fixture', Path(__file__).parents[1] / 'workers/test_durable_copy_worker_handoff.py')
_fixture = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_fixture)
worker_module = _fixture.worker_module
from shared import models
from shared.archive_capture import ArchiveCapture, archive_final_gate

pytestmark = pytest.mark.integration

async def test_review_finalizer_self_blocks_on_its_own_snapshot_lock(store, monkeypatch, worker_module):
    from shared import copy_workflow, chat_presentation
    ledger = await capture(store)
    root = await ledger.claim_scope()
    await ledger.page(root, {'value': []}, BASE)
    async with store.factory() as db:
        snapshot = await db.get(models.Snapshot, ledger.fence.snapshot_id)
    monkeypatch.setattr(worker_module, 'async_session_factory', store.factory)
    monkeypatch.setattr(copy_workflow, 'has_pending_copy', AsyncMock(return_value=False))
    monkeypatch.setattr(copy_workflow, 'verify_copy_resume', AsyncMock(return_value=True))
    monkeypatch.setattr(chat_presentation, 'presentation_ready', AsyncMock(return_value=True))
    async def coerce(snapshot, proposed): return proposed
    monkeypatch.setattr(worker_module, '_coerce_snapshot_terminal_on_cancel', coerce)
    worker = worker_module.BackupWorker.__new__(worker_module.BackupWorker)
    worker.worker_id = 'private-archive-review'
    worker.flush_version_rows = AsyncMock(return_value=0)
    result = {'item_count': 0, 'new_item_count': 0, 'bytes_total': 0, 'bytes_added': 0,
              'archive_snapshot_token': 0, 'archive_job_token': 0,
              'archive_capture_generation': str(ledger.generation)}
    async with store.factory() as caller:
        async with asyncio.timeout(3):
            await worker.complete_snapshot(caller, snapshot, result)
    async with store.factory() as db:
        current = await db.get(models.Snapshot, snapshot.id)
        assert current.status == models.SnapshotStatus.COMPLETED
        assert current.item_count == 0

async def test_review_export_accepts_conflicting_backup_version(store):
    ledger = await capture(store)
    await observed_item(ledger)
    first = content_row(store, ledger)
    first['backup_version'] = 2
    await ledger.commit_rows([first])
    replay = content_row(store, ledger)
    assert replay['backup_version'] == 1
    receipts = await ledger.commit_rows([replay])
    assert receipts == []
    assert (await ledger.summary())['files_failed'] == 1
    async with store.factory() as db:
        actual = await db.get(models.SnapshotItem, first['id'])
        assert actual.backup_version == 2

async def test_review_location_cap_does_not_prevent_auxiliary_child_claim(store):
    ledger = await capture(store)
    ledger.max_locations = 1
    root = await ledger.claim_scope()
    aux = BASE.replace('Primary', 'BeyondCap')
    await ledger.page(root, {'value': [{'id': 'f', 'childFolderCount': 0, 'parentMailboxUrl': aux[:-8]}]}, BASE)
    scope = await ledger.claim_scope()
    assert scope is None
    async with store.factory() as db:
        capped = await db.scalar(select(models.ArchiveFolderWork).where(models.ArchiveFolderWork.base_url == aux, models.ArchiveFolderWork.phase == 'root'))
        assert capped.gap_kind == 'location_cap'

async def test_review_carry_gate_ignores_conflicting_attachment(store):
    prior = await capture(store)
    await observed_item(prior)
    await prior.commit_rows([content_row(store, prior)])
    current = await capture(store)
    await observed_item(current)
    async with store.factory.begin() as db:
        snap = await db.get(models.Snapshot, prior.fence.snapshot_id)
        snap.status = models.SnapshotStatus.COMPLETED
        snap.item_count = 1
        for sid, checksum in [(snap.id, 'expected'), (current.fence.snapshot_id, 'conflicting')]:
            db.add(models.SnapshotItem(id=uuid4(), snapshot_id=sid, tenant_id=store.tenant,
                external_id='attachment', parent_external_id='i', item_type='ARCHIVE_ATTACHMENT', name='file',
                content_hash=checksum, blob_path=checksum+'-path', backend_id=store.backend, content_size=4))
    await current.classify(prior.fence.snapshot_id, copy_collection=None)
    async with store.factory.begin() as db:
        snapshot = await db.get(models.Snapshot, current.fence.snapshot_id)
        summary = await archive_final_gate(db, snapshot, generation=current.generation)
        attachment = await db.scalar(select(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == snapshot.id, models.SnapshotItem.item_type == 'ARCHIVE_ATTACHMENT'))
        assert attachment.content_hash == 'conflicting'
        assert summary['files_failed'] == 1

async def test_review_null_legacy_copy_version_becomes_permanent_gap(store, monkeypatch):
    from shared import copy_workflow
    from shared.copy_executor import prepare_manifest, copy_manifest_batch
    monkeypatch.setenv('COPY_WORK_ENABLED', 'true')
    monkeypatch.setattr(copy_workflow, 'async_session_factory', store.factory)
    prior = await capture(store)
    await observed_item(prior)
    original = content_row(store, prior)
    await prior.commit_rows([original])
    async with store.factory.begin() as db:
        snap = await db.get(models.Snapshot, prior.fence.snapshot_id)
        snap.status = models.SnapshotStatus.COMPLETED
        snap.item_count = 1
        await db.execute(update(models.SnapshotItem).where(models.SnapshotItem.id == original['id']).values(backup_version=None))
    current = await capture(store)
    await observed_item(current, more=[{'id': 'new', 'changeKey': 'n'}])   # one new item: copy, not reuse
    with copy_workflow.copy_collection(current.fence.snapshot_id, expected_snapshot_token=0, expected_job_token=0) as collection:
        await current.classify(prior.fence.snapshot_id, copy_collection=collection)
        await copy_workflow.seal_current_copy({'resume_kind': 'archive', 'archive_capture_generation': str(current.generation)})
    async with store.factory.begin() as db:
        task = await db.scalar(select(models.CopyWorkTask))
        await prepare_manifest(db, task)
        outcome = await copy_manifest_batch(db, task, limit=20)
        assert outcome['done']
        task.state = 'COMPLETED'
    async with store.factory.begin() as db:
        snap = await db.get(models.Snapshot, current.fence.snapshot_id)
        summary = await archive_final_gate(db, snap, generation=current.generation)
        copied = await db.scalar(select(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == snap.id))
        assert copied.backup_version == 1
        assert summary['files_failed'] == 1 and summary['files_succeeded'] == 1   # the new item awaits export

async def test_review_baseline_drops_existing_row_count_floor(store, monkeypatch):
    from test_archive_runner_postgres import archive
    from shared import database
    prior = await capture(store)
    async with store.factory.begin() as db:
        snap = await db.get(models.Snapshot, prior.fence.snapshot_id)
        snap.status = models.SnapshotStatus.PARTIAL
        snap.item_count = 1  # Legacy cached counter lower than its retained content.
        for n in range(2):
            row = content_row(store, prior, iid=f'i{n}')
            row['extra_data'] = row.pop('metadata')
            db.add(models.SnapshotItem(**row))
    current = await capture(store)
    monkeypatch.setattr(database, 'async_session_factory', store.factory)
    chosen, actual_previous_map = await archive._default_load_prev(str(store.resource), str(current.fence.snapshot_id))
    new_id, new_baseline = await current.baseline()
    assert chosen == str(new_id) == str(prior.fence.snapshot_id)
    assert len(actual_previous_map) == 2 and new_baseline == 2

async def test_review_missing_location_probes_lose_folder_diversity(store, monkeypatch):
    from test_archive_runner_postgres import archive
    from shared import database
    prior = await capture(store)
    async with store.factory.begin() as db:
        for n in range(61):
            row = content_row(store, prior, iid=f'a{n:02}' if n < 60 else 'z-other-location')
            row['folder_path'] = 'A' if n < 60 else 'B'
            row['extra_data'] = row.pop('metadata')
            db.add(models.SnapshotItem(**row))
    current = await capture(store)
    samples = await current.missing_samples(prior.fence.snapshot_id, 60)
    monkeypatch.setattr(database, 'async_session_factory', store.factory)
    old_samples = await archive._default_load_prior_paths(str(prior.fence.snapshot_id))
    assert len(samples) == 60 and 'z-other-location' in samples
    assert old_samples['B'] == ['z-other-location'] and len(old_samples['A']) == 3

async def test_ordinary_export_row_uses_database_default_backup_version(store):
    ledger = await capture(store)
    await observed_item(ledger)
    row = content_row(store, ledger)
    row.pop('backup_version')
    assert len(await ledger.commit_rows([row])) == 1
    assert (await ledger.summary())['files_failed'] == 0

async def test_capped_response_context_stops_graph_before_following_next_link(store):
    from types import SimpleNamespace
    from test_archive_runner_postgres import archive
    ledger = await capture(store)
    ledger.max_locations = 1
    aux = BASE.replace('Primary', 'BeyondCap')
    calls = []
    class Graph:
        async def _get(self, url, params, *, on_activity, on_page):
            calls.append(url)
            await on_page({'value': [], '@odata.context': aux.split('/admin/')[0] + '/$metadata#admin/' + aux.split('/admin/')[1],
                '@odata.nextLink': aux + '?page=2'}, url)
            calls.append(aux + '?page=2')
    await archive._DurableArchiveWalker(Graph(), ledger, SimpleNamespace(ARCHIVE_FOLDER_CONCURRENCY=1), lambda: None).run()
    assert calls == [BASE]
    assert (await ledger.summary())['folders_failed'] >= 1

async def test_attachment_changed_after_inline_copy_is_detected_by_final_gate(store):
    prior = await capture(store)
    await observed_item(prior)
    await prior.commit_rows([content_row(store, prior)])
    async with store.factory.begin() as db:
        snap = await db.get(models.Snapshot, prior.fence.snapshot_id)
        snap.status = models.SnapshotStatus.COMPLETED
        db.add(models.SnapshotItem(id=uuid4(), snapshot_id=snap.id, tenant_id=store.tenant,
            external_id='attachment', parent_external_id='i', item_type='ARCHIVE_ATTACHMENT', name='file',
            content_hash='expected', blob_path='bytes', backend_id=store.backend, content_size=4))
    current = await capture(store)
    await observed_item(current, more=[{'id': 'new', 'changeKey': 'n'}])   # one new item: copy, not reuse
    await current.classify(prior.fence.snapshot_id)
    async with store.factory.begin() as db:
        await db.execute(update(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == current.fence.snapshot_id,
            models.SnapshotItem.item_type == 'ARCHIVE_ATTACHMENT').values(content_hash='changed'))
        snap = await db.get(models.Snapshot, current.fence.snapshot_id)
        assert (await archive_final_gate(db, snap, generation=current.generation))['files_failed'] == 2   # + the new item

async def test_archive_finalizer_preserves_required_copy_failure(store, monkeypatch, worker_module):
    ledger = await capture(store)
    root = await ledger.claim_scope()
    await ledger.page(root, {'value': []}, BASE)
    async with store.factory.begin() as db:
        snap = await db.get(models.Snapshot, ledger.fence.snapshot_id)
        snap.extra_data = {**snap.extra_data, 'copy_required_failed': True}
    monkeypatch.setattr(worker_module, 'async_session_factory', store.factory)
    worker = worker_module.BackupWorker.__new__(worker_module.BackupWorker)
    worker.worker_id = 'provider-archive-finalizer'
    assert await worker._complete_archive_snapshot(snap, {'archive_snapshot_token': 0, 'archive_job_token': 0,
        'archive_capture_generation': str(ledger.generation)})
    assert snap.status == models.SnapshotStatus.PARTIAL

async def test_pending_carry_resolution_does_not_rescan_prior_batches(store, monkeypatch):
    from shared import copy_workflow
    from shared.copy_executor import prepare_manifest, copy_manifest_batch
    from sqlalchemy import event
    monkeypatch.setenv('COPY_WORK_ENABLED', 'true')
    monkeypatch.setattr(copy_workflow, 'async_session_factory', store.factory)
    prior = await capture(store)
    async with store.factory.begin() as db:
        snap = await db.get(models.Snapshot, prior.fence.snapshot_id)
        snap.status = models.SnapshotStatus.COMPLETED
        for n in range(512):
            row = content_row(store, prior, iid=f'i{n:04}')
            row['extra_data'] = row.pop('metadata')
            db.add(models.SnapshotItem(**row))
    current = await capture(store)
    root = await current.claim_scope()
    await current.page(root, {'value': [{'id': 'f', 'childFolderCount': 0}]}, BASE)
    items = await current.claim_scope()
    await current.page(items, {'value': [{'id': f'i{n:04}', 'changeKey': 'a'} for n in range(512)]}, items['next_url'])
    page_queries = []
    def observe(conn, cursor, statement, parameters, context, executemany):
        if 'FROM archive_item_work' in statement and 'ORDER BY archive_item_work.external_id' in statement and 'LIMIT' in statement:
            page_queries.append(statement)
    event.listen(store.engine.sync_engine, 'before_cursor_execute', observe)
    try:
        with copy_workflow.copy_collection(current.fence.snapshot_id, expected_snapshot_token=0, expected_job_token=0) as collection:
            await current.classify(prior.fence.snapshot_id, copy_collection=collection)
    finally:
        event.remove(store.engine.sync_engine, 'before_cursor_execute', observe)
    # Four128-ID batches each perform at most one ready-copy query while SQL
    # copies are DRAINING. A scan of the whole growing backlog used fourteen.
    assert len(page_queries) <= 4
