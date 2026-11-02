"""Archive checkpoints use actual transactions in a unique disposable schema."""
from dataclasses import replace
import pytest
from sqlalchemy import select, update
from test_chat_catalogue_postgres import store
from shared import models
from shared.archive_capture import ArchiveCapture, ArchiveFence, ArchiveConflict

pytestmark = pytest.mark.integration
BASE = 'https://graph.microsoft.com/beta/admin/exchange/mailboxes/MBX:Primary/folders'


async def capture(store):
    scope = await store.new_scope()
    async with store.factory() as db:
        snap = await db.get(models.Snapshot, scope.snapshot_id)
    fence = ArchiveFence(snap.id, snap.job_id, store.resource, store.tenant, 0, 0)
    ledger = ArchiveCapture(store.factory, fence)
    await ledger.begin(BASE, 'visibility-one')
    return ledger


async def test_page_commit_replay_and_resume_cursor_are_atomic(store):
    ledger = await capture(store)
    scope = await ledger.claim_scope()
    page = {'value': [{'id': 'f', 'displayName': 'Inbox', 'childFolderCount': 0}], '@odata.nextLink': BASE + '?p=2'}
    await ledger.page(scope, page, BASE)
    await ledger.page(scope, page, BASE)  # replay must not double page receipt
    await ledger.recover()
    resumed = await ledger.claim_scope()
    assert resumed['scope_key'] == scope['scope_key']
    assert resumed['next_url'] == BASE + '?p=2'
    await ledger.page(resumed, {'value': []}, resumed['next_url'])
    items = await ledger.claim_scope()
    assert items['phase'] == 'items'
    await ledger.page(items, {'value': [{'id': 'i', 'changeKey': 'a'}]}, items['next_url'])
    state = await ledger.summary()
    assert state['items_enumerated'] == 1
    assert state['files_failed'] == 1  # no durable content receipt
    assert state['folders_failed'] == 0


async def test_stale_owner_cancel_and_changed_replay_cannot_commit(store):
    ledger = await capture(store)
    scope = await ledger.claim_scope()
    await ledger.page(scope, {'value': []}, BASE)
    with pytest.raises(ArchiveConflict, match='replay'):
        await ledger.page(scope, {'value': [{'id': 'new'}]}, BASE)
    async with store.factory.begin() as db:
        await db.execute(update(models.Snapshot).where(models.Snapshot.id == ledger.fence.snapshot_id).values(lease_token=1))
    with pytest.raises(ArchiveConflict, match='generation'):
        await ledger.recover()
    fresh = ArchiveCapture(store.factory, replace(ledger.fence, snapshot_token=1))
    await fresh.begin(BASE, 'visibility-one')
    async with store.factory.begin() as db:
        await db.execute(update(models.Job).where(models.Job.id == ledger.fence.job_id).values(status=models.JobStatus.CANCELLED))
    with pytest.raises(ArchiveConflict, match='active'):
        await fresh.recover()


async def test_scope_restart_discards_stale_census_and_cycle_is_gap(store):
    ledger = await capture(store)
    root = await ledger.claim_scope()
    await ledger.page(root, {'value': [{'id': 'f', 'childFolderCount': 0}]}, BASE)
    items = await ledger.claim_scope()
    next_url = items['next_url'] + '?p=2'
    await ledger.page(items, {'value': [{'id': 'gone', 'changeKey': 'a'}], '@odata.nextLink': next_url}, items['next_url'])
    await ledger.restart(items)
    items = await ledger.claim_scope()
    await ledger.page(items, {'value': [{'id': 'now', 'changeKey': 'b'}]}, items['next_url'])
    assert (await ledger.summary())['items_enumerated'] == 1
    assert [x['external_id'] for x in await ledger.manifest_batch()] == ['now']


async def test_same_names_and_changed_identity_do_not_heal_gaps(store):
    ledger = await capture(store)
    root = await ledger.claim_scope()
    await ledger.page(root, {'value': [{'id': 'a', 'displayName': 'Same', 'childFolderCount': 2}, {'id': 'b', 'displayName': 'Same', 'childFolderCount': 0}]}, BASE)
    while (scope := await ledger.claim_scope()):
        if scope['phase'] == 'items':
            await ledger.page(scope, {'value': [{'id': 'collision', 'changeKey': scope['folder_id']}]}, scope['next_url'])
        else:
            await ledger.page(scope, {'value': []}, scope['next_url'])
    summary = await ledger.summary()
    assert summary['folders_failed'] == 1
    assert summary['items_enumerated'] == 1
    await ledger.classify(None, force_full=False)
    assert (await ledger.manifest_batch())[0]['state'] == 'failed'


async def observed_item(ledger, *, iid='i', ck='a'):
    root = await ledger.claim_scope()
    await ledger.page(root, {'value': [{'id': 'f', 'childFolderCount': 0}]}, BASE)
    scope = await ledger.claim_scope()
    await ledger.page(scope, {'value': [{'id': iid, 'changeKey': ck}]}, scope['next_url'])


def content_row(store, ledger, *, iid='i', ck='a', checksum='hash', path='opaque'):
    from uuid import uuid4
    return dict(id=uuid4(), snapshot_id=ledger.fence.snapshot_id, tenant_id=store.tenant,
        external_id=iid, item_type='ARCHIVE_ITEM', parent_external_id='f', name='Email',
        content_hash=checksum, content_checksum=checksum, content_size=3, blob_path=path,
        backend_id=store.backend, metadata={'changeKey': ck}, backup_version=1)


async def test_receipt_replay_and_conflicting_existing_row_never_count_success(store):
    ledger = await capture(store)
    await observed_item(ledger)
    await ledger.classify(None)
    first = await ledger.commit_rows([content_row(store, ledger)])
    assert len(first) == 1
    assert len(await ledger.commit_rows([content_row(store, ledger)])) == 1
    assert (await ledger.summary())['files_failed'] == 0
    assert await ledger.commit_rows([content_row(store, ledger, checksum='different')]) == []
    assert (await ledger.summary())['files_failed'] == 1


async def test_changed_export_key_and_missing_target_receipt_are_incomplete(store):
    ledger = await capture(store)
    await observed_item(ledger)
    await ledger.classify(None)
    assert await ledger.commit_rows([content_row(store, ledger, ck='changed')]) == []
    assert (await ledger.summary())['files_failed'] == 1


async def test_copy_pins_and_attachments_resolve_only_after_actual_copy(store, monkeypatch):
    from shared import copy_workflow
    from shared.copy_executor import prepare_manifest, copy_manifest_batch
    from shared.archive_capture import archive_final_gate
    from uuid import uuid4
    monkeypatch.setenv('COPY_WORK_ENABLED', 'true')
    monkeypatch.setattr(copy_workflow, 'async_session_factory', store.factory)
    prior = await capture(store)
    await observed_item(prior)
    await prior.commit_rows([content_row(store, prior)])
    async with store.factory.begin() as db:
        snap = await db.get(models.Snapshot, prior.fence.snapshot_id)
        snap.status = models.SnapshotStatus.COMPLETED
        snap.item_count = 1
        db.add(models.SnapshotItem(id=uuid4(), snapshot_id=snap.id, tenant_id=store.tenant,
            external_id='attachment', parent_external_id='i', item_type='ARCHIVE_ATTACHMENT', name='file',
            content_hash='att', blob_path='att-path', backend_id=store.backend, content_size=4))
    current = await capture(store)
    await observed_item(current)
    with copy_workflow.copy_collection(current.fence.snapshot_id, expected_snapshot_token=0, expected_job_token=0) as collection:
        await current.classify(prior.fence.snapshot_id, copy_collection=collection)
        assert (await current.summary())['files_failed'] == 1
        async with store.factory() as db:
            task = await db.scalar(select(models.CopyWorkTask))
            assert (await db.scalar(select(models.CopyWorkItem).where(models.CopyWorkItem.task_id == task.id))).source_snapshot_id == prior.fence.snapshot_id
        await copy_workflow.seal_current_copy({'resume_kind': 'archive', 'archive_capture_generation': str(current.generation)})
    async with store.factory.begin() as db:
        task = await db.get(models.CopyWorkTask, task.id)
        await prepare_manifest(db, task)
        while True:
            outcome = await copy_manifest_batch(db, task, limit=20)
            task.checkpoint = outcome['cursor']
            if outcome['done']:
                break
        task.state = 'COMPLETED'
    async with store.factory.begin() as db:
        snap = await db.get(models.Snapshot, current.fence.snapshot_id)
        result = await archive_final_gate(db, snap, generation=current.generation)
        assert result['files_failed'] == 0
        assert result['bytes_total'] == 7
        assert await db.scalar(select(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == snap.id,
            models.SnapshotItem.item_type == 'ARCHIVE_ATTACHMENT'))


async def test_repaired_children_invalidate_old_subtree_observations(store):
    ledger = await capture(store)
    root = await ledger.claim_scope()
    await ledger.page(root, {'value': [{'id': 'parent', 'childFolderCount': 2}]}, BASE)
    parent_items = await ledger.claim_scope()
    await ledger.page(parent_items, {'value': []}, parent_items['next_url'])
    children = await ledger.claim_scope()
    await ledger.page(children, {'value': [{'id': 'child', 'childFolderCount': 0}]}, children['next_url'])
    child_items = await ledger.claim_scope()
    await ledger.page(child_items, {'value': [{'id': 'stale', 'changeKey': 'old'}]}, child_items['next_url'])
    assert (await ledger.summary())['items_enumerated'] == 1
    await ledger.restart(children)
    children = await ledger.claim_scope()
    await ledger.page(children, {'value': []}, children['next_url'])
    assert (await ledger.summary())['items_enumerated'] == 0


async def test_cyclic_page_cursor_rolls_back_descriptors(store):
    from shared.archive_capture import ArchivePageError
    ledger = await capture(store)
    root = await ledger.claim_scope()
    with pytest.raises(ArchivePageError, match='cycle'):
        await ledger.page(root, {'value': [{'id': 'f'}], '@odata.nextLink': BASE}, BASE)
    assert (await ledger.summary())['scopes_planned'] == 1
    assert not await ledger.manifest_batch()
