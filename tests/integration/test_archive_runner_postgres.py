"""Active archive writer: paged synthetic providers, real durable receipts."""
import asyncio
import base64
import importlib.util
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID
import pytest
from sqlalchemy import select
from test_chat_catalogue_postgres import store
from test_archive_capture_postgres import ArchiveFence, ArchiveCapture, content_row
from shared import models

spec = importlib.util.spec_from_file_location('durable_archive_test', Path(__file__).resolve().parents[2] / 'workers/backup-worker/archive_backup.py')
archive = importlib.util.module_from_spec(spec)
spec.loader.exec_module(archive)
pytestmark = pytest.mark.integration


def settings():
    return SimpleNamespace(ARCHIVE_GRAPH_VERSION='beta', ARCHIVE_IMPORT_TIMEOUT_S=30,
        ARCHIVE_FOLDER_CONCURRENCY=3, ARCHIVE_EXPORT_CONCURRENCY=2,
        ARCHIVE_LOCATION_PROBE_CALLS=0, ARCHIVE_MAX_FOLDER_FAILURES=100,
        ARCHIVE_LIVENESS_PROBE_S=30, ARCHIVE_ITEM_PAGE_SIZE=50, ARCHIVE_FOLDER_PAGE_SIZE=50)


async def setup(store):
    scope = await store.new_scope()
    async with store.factory.begin() as db:
        resource = await db.get(models.Resource, store.resource)
        resource.external_id = 'MBX:Primary'
        resource.type = models.ResourceType.USER_ARCHIVE
        resource.extra_data = {}
        snapshot = await db.get(models.Snapshot, scope.snapshot_id)
        snapshot.extra_data = {}
        tenant = await db.get(models.Tenant, store.tenant)
    fence = ArchiveFence(snapshot.id, snapshot.job_id, resource.id, tenant.id, 0, 0)
    return resource, snapshot, tenant, fence


class Source:
    client_id = 'synthetic'
    def __init__(self, *, count=45, gaps=0):
        self.count, self.gaps = count, gaps
        self.active = self.peak = self.calls = 0
    async def _get(self, url, params=None, on_activity=None, *, on_page=None):
        self.active += 1
        self.peak = max(self.peak, self.active)
        self.calls += 1
        try:
            await asyncio.sleep(0)
            if url.endswith('/folders'):
                values = [{'id': f'f{x}', 'displayName': 'Duplicate name', 'childFolderCount': 1 if self.gaps else 0}
                    for x in range(self.gaps or 1)]
            elif url.endswith('/childFolders'):
                values = []
            else:
                folder = url.split('/')[-2]
                values = [{'id': f'{folder}-{i:08}', 'changeKey': 'a', 'size': 3, 'type': 'IPM.Note'}
                    for i in range(1 if self.gaps else self.count)]
            for start in range(0, len(values), 50):
                current_url = url if start == 0 else url + f'?page={start}'
                page = {'value': values[start:start + 50]}
                if start + 50 < len(values):
                    page['@odata.nextLink'] = url + f'?page={start + 50}'
                await on_page(page, current_url)
            if not values:
                await on_page({'value': []}, url)
        finally:
            self.active -= 1


async def run(store, monkeypatch, source, *, pause=0):
    resource, snapshot, tenant, fence = await setup(store)
    stats = {'export_active': 0, 'peak': 0, 'batches': []}
    async def export(graph, url, ids, **kwargs):
        assert len(ids) <= 20
        stats['batches'].append(len(ids))
        stats['export_active'] += 1
        stats['peak'] = max(stats['peak'], stats['export_active'])
        return {'value': [{'itemId': iid, 'changeKey': 'a', 'data': base64.b64encode(b'abc').decode()} for iid in ids]}
    monkeypatch.setattr('shared.archive_provider.export_items', export)
    async def persist(pairs):
        await asyncio.sleep(pause)
        ledger = ArchiveCapture(store.factory, fence)
        ledger.generation = UUID(snapshot.extra_data['archive_capture']['generation'])
        receipts = await ledger.commit_rows([content_row(store, ledger, iid=item['id']) for item, _ in pairs])
        stats['export_active'] -= 1
        return receipts
    result = await archive.run_archive_backup(source, resource, snapshot, tenant, settings=settings(),
        archive_fence=fence, capture_factory=store.factory, persist_batch=persist)
    return result, snapshot, stats


async def test_fixed_pool_holds_slots_through_slow_upload_and_receipt(store, monkeypatch):
    source = Source(count=205)
    result, snapshot, stats = await run(store, monkeypatch, source, pause=.02)
    assert result['item_count'] == 205 and result['files_failed'] == 0
    assert result['bytes_total'] == 615 and result['bytes_added'] == 615
    assert stats['peak'] <= 2 and source.peak <= 3
    assert max(stats['batches']) == 20 and sum(stats['batches']) == 205
    assert snapshot.delta_tokens_json['folders_failed'] == 0


async def test_36_known_folder_gaps_remain_partial_at_full_item_count(store, monkeypatch):
    result, snapshot, stats = await run(store, monkeypatch, Source(gaps=36))
    assert result['item_count'] == 36
    assert result['files_failed'] == 36
    assert snapshot.delta_tokens_json['folders_failed'] == 36
    assert len(snapshot.delta_tokens_json['folder_failures']) == 20
    assert snapshot.delta_tokens_json['folder_failures_truncated']


async def test_page_commit_survives_process_cancel_without_false_completion(store, monkeypatch):
    resource, snapshot, tenant, fence = await setup(store)
    class Interrupted(Source):
        async def _get(self, url, params=None, on_activity=None, *, on_page=None):
            await on_page({'value': [{'id': 'f', 'childFolderCount': 0}], '@odata.nextLink': url + '?page=2'}, url)
            raise asyncio.CancelledError
    with pytest.raises(asyncio.CancelledError):
        await archive.run_archive_backup(Interrupted(), resource, snapshot, tenant, settings=settings(),
            archive_fence=fence, capture_factory=store.factory)
    async with store.factory() as db:
        snap = await db.get(models.Snapshot, snapshot.id)
        assert snap.status == models.SnapshotStatus.IN_PROGRESS
    ledger = ArchiveCapture(store.factory, fence)
    await ledger.begin('https://graph.microsoft.com/beta/admin/exchange/mailboxes/MBX:Primary/folders', 'synthetic')
    scope = await ledger.claim_scope()
    assert scope['phase'] == 'root' and scope['next_url'].endswith('?page=2')
