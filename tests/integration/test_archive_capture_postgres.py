"""Archive checkpoints use actual transactions in a unique disposable schema."""
from dataclasses import replace
import pytest
from sqlalchemy import select, text, update
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


async def observed_item(ledger, *, iid='i', ck='a', more=()):
    root = await ledger.claim_scope()
    await ledger.page(root, {'value': [{'id': 'f', 'childFolderCount': 0}]}, BASE)
    scope = await ledger.claim_scope()
    await ledger.page(scope, {'value': [{'id': iid, 'changeKey': ck}, *more]}, scope['next_url'])


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
    await observed_item(current, more=[{'id': 'new', 'changeKey': 'n'}])   # one new item: copy, not reuse
    with copy_workflow.copy_collection(current.fence.snapshot_id, expected_snapshot_token=0, expected_job_token=0) as collection:
        await current.classify(prior.fence.snapshot_id, copy_collection=collection)
        assert (await current.summary())['files_failed'] == 2
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
        assert result['files_failed'] == 1   # the new item still awaits its export
        assert result['bytes_total'] == 7
        assert await db.scalar(select(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == snap.id,
            models.SnapshotItem.item_type == 'ARCHIVE_ATTACHMENT'))


async def _completed_prior(store, *, ck='a'):
    from uuid import uuid4
    prior = await capture(store)
    await observed_item(prior, ck=ck)
    await prior.commit_rows([content_row(store, prior, ck=ck)])
    async with store.factory.begin() as db:
        snap = await db.get(models.Snapshot, prior.fence.snapshot_id)
        snap.status, snap.item_count, snap.bytes_total = models.SnapshotStatus.COMPLETED, 1, 7
        db.add(models.SnapshotItem(id=uuid4(), snapshot_id=snap.id, tenant_id=store.tenant,
            external_id='attachment', parent_external_id='i', item_type='ARCHIVE_ATTACHMENT', name='file',
            content_hash='att', blob_path='att-path', backend_id=store.backend, content_size=4))
    return prior


async def test_unchanged_archive_points_at_its_prior_instead_of_copying(store):
    """Every incremental copied the whole archive forward (ARCHIVE_CARRY), even unchanged: TM's largest
    is 1.1 M items. A complete census whose every item is unchanged and one-to-one with the prior
    points at it; every item is stored through the pointer and nothing counts as new."""
    from shared.archive_capture import archive_final_gate
    prior = await _completed_prior(store)
    current = await capture(store)
    await observed_item(current)
    await current.classify(prior.fence.snapshot_id)
    async with store.factory.begin() as db:
        snap = await db.get(models.Snapshot, current.fence.snapshot_id)
        result = await archive_final_gate(db, snap, generation=current.generation)
        assert (snap.reuse_of_snapshot_id, snap.reuse_chain_root_id) == (prior.fence.snapshot_id,) * 2
        own = await db.scalar(select(models.SnapshotItem.id).where(models.SnapshotItem.snapshot_id == snap.id))
        tasks = await db.scalar(select(models.CopyWorkTask.id))
    assert own is None and tasks is None
    assert (result['files_succeeded'], result['files_failed'], result['new_item_count'], result['bytes_total']) == (1, 0, 0, 7)


async def test_changed_archive_item_is_not_reused(store):
    prior = await _completed_prior(store)
    current = await capture(store)
    await observed_item(current, ck='changed')
    await current.classify(prior.fence.snapshot_id)
    async with store.factory() as db:
        assert (await db.get(models.Snapshot, current.fence.snapshot_id)).reuse_of_snapshot_id is None


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


async def test_migration_creates_additive_tables_and_refuses_unsafe_downgrade(store):
    import importlib.util
    from pathlib import Path
    from sqlalchemy import text
    from alembic.migration import MigrationContext
    from alembic.operations import Operations
    path = Path(__file__).resolve().parents[2] / 'alembic/versions/0016_archive_capture.py'
    spec = importlib.util.spec_from_file_location('archive_migration_test', path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    def apply(conn):
        with Operations.context(MigrationContext.configure(conn)):
            migration.upgrade()
            migration.upgrade()
            with pytest.raises(RuntimeError, match='forward migration'):
                migration.downgrade()
    async with store.engine.begin() as conn:
        await conn.run_sync(models.ArchiveItemWork.__table__.drop)
        await conn.run_sync(models.ArchiveFolderWork.__table__.drop)
        await conn.run_sync(apply)
        assert await conn.scalar(text("SELECT to_regclass('archive_item_work') IS NOT NULL"))


async def test_page_context_and_explicit_hint_keep_opaque_case_and_exact_scope(store):
    from shared.archive_capture import canonical_base, ArchivePageError
    ledger = await capture(store)
    root = await ledger.claim_scope()
    aux = BASE.replace('MBX:Primary', 'MBX:CaseSensitive')
    context = 'https://graph.microsoft.com/beta/$metadata#admin/Exchange/Mailboxes/MBX:CaseSensitive/folders'
    await ledger.page(root, {'value': [{'id': 'F', 'childFolderCount': 0, 'parentMailboxUrl': aux[:-8]}], '@odata.context': context}, BASE)
    async with store.factory() as db:
        item_scope = await db.scalar(select(models.ArchiveFolderWork).where(models.ArchiveFolderWork.phase == 'items'))
        assert item_scope.base_url == aux
        assert item_scope.folder_id == 'F'
    assert canonical_base(aux) != canonical_base(aux.replace('CaseSensitive', 'casesensitive'))
    for unsafe in ('https://evil.invalid/beta/admin/exchange/mailboxes/X/folders',
                   'https://graph.microsoft.com/beta/users/X/folders'):
        with pytest.raises(ArchivePageError):
            canonical_base(unsafe)


async def test_invalid_shape_rolls_back_page_and_location_cap_is_explicit(store):
    from shared.archive_capture import ArchivePageError
    ledger = await capture(store)
    scope = await ledger.claim_scope()
    with pytest.raises(ArchivePageError):
        await ledger.page(scope, {'value': [{'id': 'good'}, {}]}, BASE)
    assert (await ledger.summary())['scopes_planned'] == 1
    for i in range(10):
        await ledger.add_location(BASE.replace('Primary', f'Aux{i}'))
    async with store.factory() as db:
        over = list((await db.scalars(select(models.ArchiveFolderWork).where(models.ArchiveFolderWork.gap_kind == 'location_cap'))).all())
        assert len(over) == 3


@pytest.mark.slow
async def test_lazy_large_archive_census_has_bounded_descriptor_pages(store):
    import os
    import resource
    import time
    count = int(os.environ.get('ARCHIVE_SCALE_ROWS', '0'))
    if not count:
        pytest.skip('explicit ARCHIVE_SCALE_ROWS controls the large disposable census')
    ledger = await capture(store)
    root = await ledger.claim_scope()
    await ledger.page(root, {'value': [{'id': 'f', 'childFolderCount': 0}]}, BASE)
    scope = await ledger.claim_scope()
    started = time.monotonic()
    pages = 0
    for start in range(0, count, 1000):
        request = scope['next_url'] if start == 0 else scope['next_url'] + f'?page={start}'
        page = {'value': [{'id': f'i-{i:09}', 'changeKey': 'stable', 'size': 3}
            for i in range(start, min(start + 1000, count))]}
        if start + 1000 < count:
            page['@odata.nextLink'] = scope['next_url'] + f'?page={start + 1000}'
        await ledger.page(scope, page, request)
        pages += 1
        del page
    summary = await ledger.summary()
    assert summary['items_enumerated'] == count and summary['folders_failed'] == 0
    assert summary['files_failed'] == count  # Enumeration is not content capture.
    assert len(await ledger.manifest_batch(limit=20)) == min(20, count)
    print({'archive_scale_rows': count, 'pages': pages, 'max_page_descriptors': 1000,
        'rss_peak_bytes': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        'elapsed_seconds': round(time.monotonic() - started, 2)})


async def _short_children(store, name, declared, listed):
    ledger = await capture(store)
    root = await ledger.claim_scope()
    await ledger.page(root, {'value': [{'id': 'f1', 'displayName': name, 'childFolderCount': declared}]}, BASE)
    items = await ledger.claim_scope()
    await ledger.page(items, {'value': []}, items['next_url'])
    children = await ledger.claim_scope()
    await ledger.page(children, {'value': [{'id': f'c{i}', 'childFolderCount': 0} for i in range(listed)]},
                      children['next_url'])
    return ledger


async def _children_row(store):
    async with store.factory() as db:
        return await db.scalar(select(models.ArchiveFolderWork).where(
            models.ArchiveFolderWork.phase == 'children', models.ArchiveFolderWork.folder_id == 'f1'))


def _graph(hidden_children):
    calls = []
    async def read_folder(url, params=None, **_):
        calls.append(url)
        if 'includeHiddenFolders=true' in url:
            return {'value': [{'id': f'h{i}'} for i in range(hidden_children)]}
        return {'id': 'f1', 'childFolderCount': 7}
    return read_folder, calls


async def test_hidden_system_child_folders_are_not_a_coverage_loss(store):
    # Live 2026-11-06: Contacts declares 7 child folders, plain childFolders returns 0,
    # includeHiddenFolders=true returns all 7 (Recipient Cache, GAL Contacts, ...).
    ledger = await _short_children(store, 'Contacts', 7, 0)
    assert (await _children_row(store)).gap_kind == 'count_shortfall'
    read_folder, calls = _graph(7)
    await ledger.repair(read_folder=read_folder)
    row = await _children_row(store)
    assert (row.state, row.gap_kind, row.evidence['hidden_children']) == ('complete', None, 7)
    assert any('includeHiddenFolders=true' in c for c in calls)
    assert (await ledger.summary())['items_enumerated'] == 0  # hidden folders are counted, not backed up


async def test_conversation_history_child_graph_never_lists_is_not_a_loss(store):
    # Live 2026-11-06: Conversation History declares 1 child; v1.0 and beta, with or without
    # includeHiddenFolders, return none (Teams' system folder).
    ledger = await _short_children(store, 'Conversation History', 1, 0)
    await ledger.repair(read_folder=_graph(0)[0])
    row = await _children_row(store)
    assert (row.state, row.gap_kind, row.evidence['unlisted_system_children']) == ('complete', None, 1)


async def test_child_folders_missing_even_with_hidden_ones_stay_a_gap(store):
    ledger = await _short_children(store, 'Projects', 5, 3)
    await ledger.repair(read_folder=_graph(3)[0])
    row = await _children_row(store)
    assert row.state != 'complete' and 'hidden_children' not in (row.evidence or {})


async def _with_reuse_rule(store):
    import re
    from pathlib import Path
    source = (Path(__file__).parents[2] / "shared" / "database.py").read_text()
    function = re.search(r'"""(CREATE OR REPLACE FUNCTION validate_snapshot_reuse_target\(\).*?)"""', source, re.S).group(1)
    async with store.factory.begin() as db:
        await db.execute(text(function))
        await db.execute(text("DROP TRIGGER IF EXISTS snapshots_reuse_validate ON snapshots"))
        await db.execute(text("CREATE TRIGGER snapshots_reuse_validate BEFORE INSERT OR UPDATE OF reuse_of_snapshot_id, "
                              "reuse_chain_root_id ON snapshots FOR EACH ROW EXECUTE FUNCTION validate_snapshot_reuse_target()"))


async def _walk_with_failed_folder(ledger, *, failing='bad', ck='a'):
    """One readable folder holding item 'i' and one folder whose item listing fails like TM's
    Microsoft HTTP 500 folders."""
    from shared.provider_read import ProviderReadError
    root = await ledger.claim_scope()
    await ledger.page(root, {'value': [{'id': 'f', 'displayName': 'Inbox', 'childFolderCount': 0},
                                      {'id': failing, 'displayName': failing, 'childFolderCount': 0}]}, BASE)
    while (scope := await ledger.claim_scope()) is not None:
        if scope['folder_id'] == failing:
            await ledger.fail_scope(scope, ProviderReadError('items_page', status=500))
        else:
            await ledger.page(scope, {'value': [{'id': 'i', 'changeKey': ck}]}, scope['next_url'])


async def _partial_prior(store, *, failing='bad'):
    prior = await capture(store)
    await _walk_with_failed_folder(prior, failing=failing)
    await prior.classify(None)
    await prior.commit_rows([content_row(store, prior)])
    async with store.factory.begin() as db:
        snap = await db.get(models.Snapshot, prior.fence.snapshot_id)
        snap.status, snap.item_count, snap.bytes_total = models.SnapshotStatus.PARTIAL, 1, 3
    return prior


async def test_archive_with_the_same_microsoft_failures_points_at_its_partial_prior(store):
    """TM's archive is PARTIAL every run (the same 4 folders return Microsoft HTTP 500), so the pointer
    never fired and ~857k rows were copied daily. Same failed folders + unchanged items: point at the
    PARTIAL prior (marker names it), keep the same gap, and copy nothing."""
    from shared.archive_capture import archive_final_gate
    await _with_reuse_rule(store)
    prior = await _partial_prior(store)
    current = await capture(store)
    await _walk_with_failed_folder(current)
    await current.classify(prior.fence.snapshot_id)
    async with store.factory.begin() as db:
        snap = await db.get(models.Snapshot, current.fence.snapshot_id)
        assert (snap.reuse_of_snapshot_id, snap.reuse_chain_root_id) == (prior.fence.snapshot_id,) * 2
        assert snap.extra_data['archive_reused_partial_prior'] == str(prior.fence.snapshot_id)
        result = await archive_final_gate(db, snap, generation=current.generation)
        tasks = await db.scalar(select(models.CopyWorkTask.id))
    assert tasks is None   # nothing copied
    # Unchanged item stored through the pointer; the same folder gap stays visible (PARTIAL).
    assert (result['files_succeeded'], result['items_failed'], result['folders_failed'], result['new_item_count']) == (1, 0, 1, 0)


async def test_archive_with_different_failures_is_copied_not_reused(store):
    await _with_reuse_rule(store)
    prior = await _partial_prior(store, failing='bad')
    current = await capture(store)
    await _walk_with_failed_folder(current, failing='other')
    await current.classify(prior.fence.snapshot_id)
    async with store.factory() as db:
        assert (await db.get(models.Snapshot, current.fence.snapshot_id)).reuse_of_snapshot_id is None


async def test_reuse_rule_rejects_a_partial_target_without_the_archive_marker(store):
    from shared.snapshot_reuse import point_at
    await _with_reuse_rule(store)
    prior = await _partial_prior(store)
    current = await capture(store)
    with pytest.raises(Exception, match='must be COMPLETED'):
        async with store.factory.begin() as db:
            await point_at(db, target_snapshot_id=current.fence.snapshot_id,
                           prior_snapshot_id=prior.fence.snapshot_id, marker='mail_reused_prior')
