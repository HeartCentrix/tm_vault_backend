"""Archive coverage reconciliation uses received identities and stable counts."""
import pytest
from sqlalchemy import select
from test_chat_catalogue_postgres import store
from test_archive_capture_postgres import capture, BASE
from test_archive_runner_postgres import archive, settings
from shared import models

pytestmark = pytest.mark.integration

@pytest.mark.parametrize('mode', ['stable', 'drifting', 'unavailable'])
async def test_parent_count_refresh_requires_bounded_stable_metadata(store, mode):
    ledger = await capture(store)
    calls = []
    class Graph:
        async def _get(self, url, params=None, on_activity=None, *, on_page=None):
            calls.append(url)
            if url == BASE + '/P':
                if mode == 'unavailable':
                    from shared.provider_read import ProviderReadError
                    raise ProviderReadError('metadata_denied', status=403)
                return {'id': 'P', 'childFolderCount': 3 if mode == 'drifting' and calls.count(url) > 1 else 1}
            if url == BASE:
                page = {'value': [{'id': 'P', 'childFolderCount': 2}]}
            elif url.endswith('/P/childFolders'):
                page = {'value': [{'id': 'C', 'childFolderCount': 0}]}
            else:
                page = {'value': []}
            await on_page(page, url)
    graph = Graph()
    conf = settings()
    conf.ARCHIVE_FOLDER_CONCURRENCY = 1
    walker = archive._DurableArchiveWalker(graph, ledger, conf, lambda: None)
    await walker.run()
    await ledger.repair(read_folder=graph._get)
    await walker.run()
    await ledger.verify_repair_counts(graph._get)
    async with store.factory() as db:
        scope = await db.scalar(select(models.ArchiveFolderWork).where(
            models.ArchiveFolderWork.snapshot_id == ledger.fence.snapshot_id,
            models.ArchiveFolderWork.folder_id == 'P', models.ArchiveFolderWork.phase == 'children'))
        assert scope.observed_count == 1
        assert scope.state == ('complete' if mode == 'stable' else 'unresolved')
    assert calls.count(BASE + '/P/childFolders') == 2
    assert calls.count(BASE + '/P') == (1 if mode == 'unavailable' else 2)
    # Restart/repeated repair may not reset the stored reconciliation allowance.
    await ledger.recover()
    before = len(calls)
    await ledger.repair(read_folder=graph._get)
    await walker.run()
    await ledger.verify_repair_counts(graph._get)
    assert len(calls) == before


async def test_crashes_cannot_exhaust_metadata_reads_then_certify_unverified_coverage(store):
    ledger = await capture(store)
    root = await ledger.claim_scope()
    await ledger.page(root, {'value': [{'id': 'P', 'childFolderCount': 0}]}, BASE)
    scope = await ledger.claim_scope()
    await ledger.page(scope, {'value': []}, scope['next_url'])
    async with store.factory.begin() as db:
        row = await db.get(models.ArchiveFolderWork, (ledger.fence.snapshot_id, ledger.generation, scope['scope_key']))
        row.restart_count = 1
        row.evidence = {**row.evidence, 'count_reconciliation': {'before_reads': 1, 'before_done': True,
            'refreshed_count': 0, 'refreshed_route': BASE, 'after_reads': 2}}
    async def forbidden_read(url):
        pytest.fail('crash recovery may not reset exhausted metadata read allowance')
    # Final gate itself must reject incomplete proof, even before verifier runs.
    assert (await ledger.summary())['folders_failed'] == 1
    await ledger.verify_repair_counts(forbidden_read)
    assert (await ledger.summary())['folders_failed'] == 1


async def test_count_metadata_context_qualifies_the_actual_auxiliary_relist(store):
    ledger = await capture(store)
    root = await ledger.claim_scope()
    await ledger.page(root, {'value': [{'id': 'P', 'childFolderCount': 0, 'totalItemCount': 1}]}, BASE)
    scope = await ledger.claim_scope()
    await ledger.page(scope, {'value': []}, scope['next_url'])
    context = 'https://graph.microsoft.com/beta/$metadata#admin/exchange/mailboxes/MBX:Aux/folders/$entity'
    aux = BASE.replace('Primary', 'Aux')
    async def metadata(url):
        assert url == BASE + '/P'
        return {'id': 'P', 'totalItemCount': 0, '@odata.context': context}
    await ledger._refresh_count(scope, metadata)
    await ledger.restart(scope)
    relist = await ledger.claim_scope()
    assert relist['scope_key'] == scope['scope_key']
    await ledger.page(relist, {'value': [], '@odata.context': context}, relist['next_url'])
    await ledger._refresh_count(relist, metadata, after=True)
    async with store.factory() as db:
        row = await db.get(models.ArchiveFolderWork, (ledger.fence.snapshot_id, ledger.generation, scope['scope_key']))
        proof = row.evidence['count_reconciliation']
        assert proof['refreshed_route'] == proof['final_route'] == aux
        assert proof['stable'] is True
        assert row.state == 'complete'

async def test_auxiliary_child_membership_is_distinct_per_received_listing(store):
    ledger = await capture(store)
    aux = BASE.replace('Primary', 'Aux')
    context = 'https://graph.microsoft.com/beta/$metadata#admin/exchange/mailboxes/MBX:Aux/folders'
    class Graph:
        async def _get(self, url, params=None, on_activity=None, *, on_page=None):
            if url in (BASE, aux):
                page = {'value': [{'id': 'P', 'childFolderCount': 1}]}
            elif url.endswith('/P/childFolders'):
                page = {'value': [{'id': 'C', 'childFolderCount': 0}, {'id': 'C', 'childFolderCount': 0}], '@odata.context': context}
            else:
                page = {'value': [], '@odata.context': context}
            await on_page(page, url)
    conf = settings()
    conf.ARCHIVE_FOLDER_CONCURRENCY = 1
    await archive._DurableArchiveWalker(Graph(), ledger, conf, lambda: None).run()
    async with store.factory() as db:
        scopes = list((await db.scalars(select(models.ArchiveFolderWork).where(
            models.ArchiveFolderWork.snapshot_id == ledger.fence.snapshot_id,
            models.ArchiveFolderWork.folder_id == 'P', models.ArchiveFolderWork.phase == 'children'))).all())
        assert len(scopes) == 2
        assert [scope.observed_count for scope in scopes] == [1, 1]
        assert all(scope.state == 'complete' for scope in scopes)
    assert (await ledger.summary())['folders_failed'] == 0
    async with store.factory.begin() as db:
        target = await db.scalar(select(models.ArchiveFolderWork).where(
            models.ArchiveFolderWork.snapshot_id == ledger.fence.snapshot_id,
            models.ArchiveFolderWork.folder_id == 'C', models.ArchiveFolderWork.phase == 'items'))
        target.state = 'excluded'
    assert (await ledger.summary())['folders_failed'] > 0
