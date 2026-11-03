"""Archive transfers retain receipts and release spool budgets before redirects."""
import asyncio
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import select

from test_chat_catalogue_postgres import store
from test_archive_runner_postgres import archive, setup, settings, Source
from test_archive_capture_postgres import ArchiveCapture, content_row
from shared import models, provider_runtime as runtime
from shared.provider_admission import ProviderDeferred

pytestmark = pytest.mark.integration


@pytest.mark.parametrize('stop', ['concurrency', 'cooldown', 'cancellation'])
async def test_archive_upload_receipts_follow_provider_handoff_semantics(store, stop):
    resource, snapshot, tenant, fence = await setup(store)
    source = Source(count=21)
    upload_started, upload_release, congestion_seen = [asyncio.Event() for _ in range(3)]
    cancelled, committed, requests = [], [], []
    execution = runtime.ProviderExecution(store.factory, SimpleNamespace(work_id=uuid4()))

    async def token():
        return 'synthetic-only'

    async def transport(request):
        ids = json.loads(request.content)['itemIds']
        return httpx.Response(200, json={'value': [
            {'itemId': iid, 'changeKey': 'a', 'data': 'YWJj'} for iid in ids]})

    @asynccontextmanager
    async def admission(url, **kwargs):
        async with runtime.provider_request_scope(), runtime.provider_transfer_scope(implicit=True):
            requests.append(url)
            if len(requests) > 1:
                await upload_started.wait()
                congestion_seen.set()
                if stop == 'cooldown':
                    runtime.defer_provider(ProviderDeferred(None, 60, reason='cooldown'))
                if stop == 'cancellation':
                    await asyncio.Future()
                await runtime.wait_provider_concurrency(ProviderDeferred(None, .001, reason='concurrency'))
            assert runtime.provider_admission_granted()
            yield

    async def persist(pairs):
        upload_started.set()
        try:
            await upload_release.wait()
        except asyncio.CancelledError:
            cancelled.append(True)
            raise
        ledger = ArchiveCapture(store.factory, fence)
        ledger.generation = UUID(snapshot.extra_data['archive_capture']['generation'])
        receipts = await ledger.commit_rows([content_row(store, ledger, iid=item['id']) for item, _ in pairs])
        committed.extend(receipts)
        return receipts

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
        @asynccontextmanager
        async def session():
            yield client
        source._get_token = token
        source._http_session = session
        source._admit_data_request = admission
        marker = runtime._EXECUTION.set(execution)
        task = asyncio.create_task(archive.run_archive_backup(source, resource, snapshot, tenant,
            settings=settings(), archive_fence=fence, capture_factory=store.factory, persist_batch=persist))
        try:
            await asyncio.wait_for(congestion_seen.wait(), 3)
            if stop == 'concurrency':
                await asyncio.sleep(.05)
                assert not task.done(), f'archive turn exited before upload/receipt: cancelled={cancelled}, committed={len(committed)}'
                assert not cancelled
                upload_release.set()
            elif stop == 'cancellation':
                task.cancel()
            error = asyncio.CancelledError if stop == 'cancellation' else runtime.ProviderTurnDeferred
            with pytest.raises(error):
                await asyncio.wait_for(task, 3)
            assert len(committed) == (20 if stop == 'concurrency' else 0)
            assert bool(cancelled) == (stop != 'concurrency')
            async with store.factory() as db:
                rows = list((await db.scalars(select(models.SnapshotItem).where(
                    models.SnapshotItem.snapshot_id == snapshot.id))).all())
            assert len(rows) == len(committed)
        finally:
            upload_release.set()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await runtime._join_provider_tasks(execution)
            runtime._EXECUTION.reset(marker)


async def test_parallel_archive_redirects_release_response_budget_before_reissue(store, monkeypatch):
    resource, snapshot, tenant, fence = await setup(store)
    source = Source(count=21)
    initial_bodies = 0
    both_responses_reserved = asyncio.Event()
    budget = runtime.ExecutionBudget(resident_bytes=128 * 1024 * 1024)
    monkeypatch.setattr(runtime, '_budget', budget)
    target = 'https://graph.microsoft.com/beta/admin/exchange/mailboxes/MBX:Aux/exportItems'
    calls, committed = [], []

    class InitialBody(httpx.AsyncByteStream):
        def __init__(self, ids):
            self.ids = ids

        async def __aiter__(self):
            nonlocal initial_bodies
            initial_bodies += 1
            if initial_bodies == 2:
                both_responses_reserved.set()
            yield json.dumps({'value': [
                {'itemId': iid, 'changeKey': 'a', 'data': 'YWJj'} if iid.endswith('00000000') else
                {'itemId': iid, 'Error': {'code': 'ErrorArchiveFolderMovedPermanently', 'message': target}}
                for iid in self.ids]}).encode()
            await both_responses_reserved.wait()

    async def token():
        return 'synthetic-only'

    async def transport(request):
        ids = json.loads(request.content)['itemIds']
        calls.append(str(request.url))
        if str(request.url) != target:
            return httpx.Response(200, stream=InitialBody(ids))
        return httpx.Response(200, json={'value': [
            {'itemId': iid, 'changeKey': 'a', 'data': 'YWJj'} for iid in ids]})

    @asynccontextmanager
    async def admission(url, **kwargs):
        yield

    async def persist(pairs):
        ledger = ArchiveCapture(store.factory, fence)
        ledger.generation = UUID(snapshot.extra_data['archive_capture']['generation'])
        receipts = await ledger.commit_rows([content_row(store, ledger, iid=item['id']) for item, _ in pairs])
        committed.extend(receipts)
        return receipts

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
        @asynccontextmanager
        async def session():
            yield client
        source._get_token = token
        source._http_session = session
        source._admit_data_request = admission
        result = await asyncio.wait_for(archive.run_archive_backup(source, resource, snapshot, tenant,
            settings=settings(), archive_fence=fence, capture_factory=store.factory, persist_batch=persist), 3)
    assert result['files_succeeded'] == 21 and result['files_failed'] == 0
    assert len(committed) == 21 and calls.count(target) == 2
    assert budget.used_bytes == 0
