"""A changed in-place archive is an OVERLAY of a full base snapshot: it owns the items exported (or
carried from the prior overlay) this run and records as removals the base items the census no longer
holds, instead of ARCHIVE_CARRY copying every unchanged row (TM's largest archive: 1.1 M items)."""
from uuid import uuid4

import pytest
from sqlalchemy import select, text

from test_chat_catalogue_postgres import store  # noqa: F401
from test_archive_capture_postgres import capture, BASE
from shared import models
from shared.archive_capture import archive_final_gate

pytestmark = pytest.mark.integration
ITEMS = {f'm{n:02d}': '1' for n in range(40)}


async def _observe(ledger, items):
    root = await ledger.claim_scope()
    await ledger.page(root, {'value': [{'id': 'f', 'childFolderCount': 0}]}, BASE)
    scope = await ledger.claim_scope()
    await ledger.page(scope, {'value': [{'id': i, 'changeKey': ck} for i, ck in sorted(items.items())]}, scope['next_url'])


async def _run(store, items, *, attachments=()):
    """One archive backup the way _run_durable_archive drives the ledger (legacy carry protocol)."""
    ledger = await capture(store)
    await _observe(ledger, items)
    prior, _ = await ledger.baseline()
    await ledger.classify(prior)
    pending = [r['external_id'] for r in await ledger.manifest_batch(state='export_pending', limit=256)]
    for iid in pending:
        ck = items[iid]
        await ledger.commit_rows([dict(id=uuid4(), snapshot_id=ledger.fence.snapshot_id, tenant_id=store.tenant,
            external_id=iid, item_type='ARCHIVE_ITEM', parent_external_id='f', name=iid,
            content_hash=f'{iid}-{ck}', content_checksum=f'{iid}-{ck}', content_size=10 + int(ck),
            blob_path=f'blob/{iid}/{ck}', backend_id=store.backend, metadata={'changeKey': ck}, backup_version=1)])
        if iid in attachments:
            async with store.factory.begin() as db:
                db.add(models.SnapshotItem(id=uuid4(), snapshot_id=ledger.fence.snapshot_id, tenant_id=store.tenant,
                    external_id=f'{iid}::att', parent_external_id=iid, item_type='ARCHIVE_ATTACHMENT', name='file',
                    content_hash=f'att-{ck}', blob_path=f'att/{iid}/{ck}', backend_id=store.backend, content_size=4))
    async with store.factory.begin() as db:
        snap = await db.get(models.Snapshot, ledger.fence.snapshot_id)
        summary = await archive_final_gate(db, snap, generation=ledger.generation)
        snap.status = models.SnapshotStatus.COMPLETED
        snap.item_count, snap.bytes_total = summary['files_succeeded'], summary['bytes_total']
    return ledger.fence.snapshot_id, summary


async def _inventory(store, sid):
    async with store.factory() as db:
        return sorted(tuple(r) for r in (await db.execute(text(
            "SELECT item_type, external_id, blob_path, content_hash, content_size "
            "FROM snapshot_items_effective(:s)"), {'s': sid})).all())


async def _shape(store, sid):
    async with store.factory() as db:
        return tuple((await db.execute(text(
            "SELECT s.overlay_base_id, (SELECT count(*) FROM snapshot_items i WHERE i.snapshot_id = s.id), "
            "(SELECT count(*) FROM snapshot_overlay_removals r WHERE r.snapshot_id = s.id) "
            "FROM snapshots s WHERE s.id = :s"), {'s': sid})).one())


def _expected(items, attachments):
    rows = [('ARCHIVE_ITEM', i, f'blob/{i}/{ck}', f'{i}-{ck}', 10 + int(ck)) for i, ck in items.items()]
    rows += [('ARCHIVE_ATTACHMENT', f'{i}::att', f'att/{i}/{ck}', f'att-{ck}', 4)
             for i, ck in attachments.items() if i in items]
    return sorted(rows)


@pytest.mark.parametrize('overlay', [True, False])
async def test_changed_archive_holds_exactly_what_the_full_copy_holds(store, monkeypatch, overlay):
    monkeypatch.setenv('ARCHIVE_OVERLAY_ENABLED', 'true' if overlay else 'false')
    run1, _ = await _run(store, ITEMS, attachments=('m00', 'm01'))
    atts = {'m00': '1', 'm01': '1'}
    assert await _inventory(store, run1) == _expected(ITEMS, atts)

    # m01 changed (its old attachment must not survive), m02 deleted, n1 new
    items2 = {**{k: v for k, v in ITEMS.items() if k != 'm02'}, 'm01': '2', 'n1': '1'}
    run2, summary2 = await _run(store, items2)
    assert await _inventory(store, run2) == _expected(items2, {'m00': '1'})
    assert (summary2['files_succeeded'], summary2['files_failed'], summary2['new_item_count']) == (40, 0, 2)

    # m00 changed, m03 deleted, n2 new; m01 / n1 stay as run 2 captured them
    items3 = {**{k: v for k, v in items2.items() if k != 'm03'}, 'm00': '2', 'n2': '1'}
    run3, summary3 = await _run(store, items3)
    assert await _inventory(store, run3) == _expected(items3, {})
    assert (summary3['files_succeeded'], summary3['files_failed'], summary3['new_item_count']) == (40, 0, 2)
    assert summary3['bytes_total'] == sum(r[4] for r in _expected(items3, {}))

    if overlay:
        # run 2 owns its 2 exports; hides m02 + m01 and m01's attachment
        assert await _shape(store, run2) == (run1, 2, 3)
        # run 3 owns its 2 exports + run 2's 2 still-current exports; still 1 base, never a copy of 20
        base, own, removed = await _shape(store, run3)
        assert (base, own) == (run1, 4)
    else:
        assert (await _shape(store, run3))[:2] == (None, 40)


async def test_compaction_copies_an_overlay_priors_whole_inventory(store, monkeypatch):
    """Past the churn threshold the run is a full copy again - of the prior's inventory (base + own),
    not of the overlay's own rows only."""
    monkeypatch.setenv('ARCHIVE_OVERLAY_ENABLED', 'true')
    run1, _ = await _run(store, ITEMS, attachments=('m00',))
    items2 = {**ITEMS, 'm01': '2'}
    run2, _ = await _run(store, items2)
    assert (await _shape(store, run2))[0] == run1
    monkeypatch.setenv('ARCHIVE_OVERLAY_MAX_RATIO', '0')
    items3 = {**items2, 'n1': '1'}
    run3, summary3 = await _run(store, items3)
    assert await _shape(store, run3) == (None, 42, 0)   # 41 items + m00's attachment
    assert await _inventory(store, run3) == _expected(items3, {'m00': '1'})
    assert (summary3['files_succeeded'], summary3['files_failed'], summary3['new_item_count']) == (41, 0, 1)


async def test_overlay_keeps_its_base_out_of_retention_and_survives_the_base_retiring(store, monkeypatch):
    from shared.retention_cleanup import _delete_snapshots
    monkeypatch.setenv('ARCHIVE_OVERLAY_ENABLED', 'true')
    run1, _ = await _run(store, ITEMS, attachments=('m00', 'm01'))
    items2 = {**{k: v for k, v in ITEMS.items() if k != 'm02'}, 'm01': '2', 'n0': '1'}
    run2, _ = await _run(store, items2)
    # n0 (only ever in run 2's own rows) is gone again: once run 2 is the base, run 3 must hide it
    items3 = {**{k: v for k, v in items2.items() if k not in ('m05', 'n0')}, 'n1': '1'}
    run3, _ = await _run(store, items3)
    assert (await _shape(store, run3))[0] == run1
    before = [await _inventory(store, s) for s in (run2, run3)]
    async with store.factory.begin() as db:
        assert await _delete_snapshots(db, {run1}) == 1
    assert [await _inventory(store, s) for s in (run2, run3)] == before


async def test_durable_copy_compaction_reads_an_overlay_priors_whole_inventory(store, monkeypatch):
    """The production carry (ARCHIVE_CARRY copy task) from an overlay prior copies base + own rows."""
    from shared import copy_workflow
    from shared.copy_executor import prepare_manifest, copy_manifest_batch
    monkeypatch.setenv('ARCHIVE_OVERLAY_ENABLED', 'true')
    monkeypatch.setenv('COPY_WORK_ENABLED', 'true')
    monkeypatch.setattr(copy_workflow, 'async_session_factory', store.factory)
    run1, _ = await _run(store, ITEMS, attachments=('m00',))
    items2 = {**ITEMS, 'm01': '2'}
    run2, _ = await _run(store, items2)
    assert (await _shape(store, run2))[0] == run1
    monkeypatch.setenv('ARCHIVE_OVERLAY_MAX_RATIO', '0')
    items3 = {k: v for k, v in items2.items() if k != 'm05'}
    ledger = await capture(store)
    await _observe(ledger, items3)
    prior, _ = await ledger.baseline()
    assert prior == run2
    with copy_workflow.copy_collection(ledger.fence.snapshot_id, expected_snapshot_token=0,
                                       expected_job_token=0) as collection:
        await ledger.classify(prior, copy_collection=collection)
        await copy_workflow.seal_current_copy({'resume_kind': 'archive',
                                               'archive_capture_generation': str(ledger.generation)})
    async with store.factory.begin() as db:
        task = await db.scalar(select(models.CopyWorkTask).where(models.CopyWorkTask.kind == 'ARCHIVE_CARRY'))
        await prepare_manifest(db, task)
        while not (outcome := await copy_manifest_batch(db, task, limit=50))['done']:
            task.checkpoint = outcome['cursor']
        task.state = 'COMPLETED'
    async with store.factory.begin() as db:
        snap = await db.get(models.Snapshot, ledger.fence.snapshot_id)
        summary = await archive_final_gate(db, snap, generation=ledger.generation)
    assert await _inventory(store, ledger.fence.snapshot_id) == _expected(items3, {'m00': '1'})
    assert (summary['files_succeeded'], summary['files_failed']) == (39, 0)
