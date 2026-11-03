"""Concurrency pressure drains admitted bodies/files before durable handoff."""
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import select

from shared import models, provider_runtime as runtime, provider_work as work
from shared.provider_admission import AdmissionLimits, ScopedProviderAdmission
from shared.provider_http import AdmittedHttpClient, request_context
from shared.range_download import ordered_ranges
from test_chat_catalogue_postgres import store
from test_graph_admission_redis import redis_client
from test_provider_work_postgres import enqueue, claim

pytestmark = pytest.mark.integration
OWNER = SimpleNamespace(tenant_id='synthetic-tenant', client_id='synthetic-app')
URL = 'https://synthetic.sharepoint.com/file'


class ImmediateAdmission(ScopedProviderAdmission):
    @asynccontextmanager
    async def admit(self, context, **kwargs):
        async with super().admit(context, wait_budget=0):
            yield


def admission(redis_client):
    return ImmediateAdmission(redis_client, namespace=uuid4().hex,
        limits=AdmissionLimits(rate=10000, burst=10000, global_concurrency=1,
            tenant_concurrency=1, resource_concurrency=1, lease_seconds=.3))


async def wait_congestion(executions):
    async with asyncio.timeout(3):
        while not executions or executions[0].congestion is None:
            await asyncio.sleep(.001)


async def join(tasks):
    for task in tasks:
        if not task.done():
            task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


async def test_body_storage_and_verified_receipt_finish_before_concurrency_defer(store, redis_client):
    receipt = await enqueue(store)
    limiter = admission(redis_client)
    body_started, body_release, storage_started, storage_release = [asyncio.Event() for _ in range(4)]
    executions, sent, buffered = [], [], []

    class Body(httpx.AsyncByteStream):
        async def __aiter__(self):
            body_started.set()
            yield b'abc'
            await body_release.wait()
            yield b'def'

    async def transport(request):
        sent.append(request.url.path)
        return httpx.Response(200, stream=Body()) if request.url.path == '/file' else httpx.Response(200, json={'etag': 'v1'})

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as raw:
        client = AdmittedHttpClient(raw, OWNER, limiter=limiter)

        async def flush():
            with runtime.provider_receipt_scope():
                if not buffered:
                    return
                assert runtime.current_execution().deferred is None
                assert (await client.get(URL.replace('/file', '/proof'))).status_code == 200
                async with store.factory.begin() as db:
                    await work.record_unit_checkpoint(db, work.current_claim(), unit_key='file', kind='file',
                        state='COMPLETED', receipt_id='verified-blob')
                buffered.clear()

        async def transfer():
            async with runtime.provider_transfer_scope(timeout=10):
                assert (await client.get(URL)).content == b'abcdef'
                storage_started.set()
                await storage_release.wait()
                buffered.append('file')

        async def resume(owner):
            executions.append(runtime.current_execution())
            with runtime.provider_drain_callback(flush):
                tasks = [asyncio.create_task(transfer())]
                await body_started.wait()
                tasks.append(asyncio.create_task(client.get(URL.replace('/file', '/new'))))
                try:
                    await asyncio.gather(*tasks)
                finally:
                    await join(tasks)
                    await flush()

        runner = runtime.ProviderWorkRunner(store.factory, resume, None)
        task = asyncio.create_task(runner.run_once())
        try:
            await wait_congestion(executions)
            assert not task.done() and executions[0].deferred is None
            body_release.set()
            await asyncio.wait_for(storage_started.wait(), 3)
            assert not task.done()
            _, slots, _ = limiter._keys(request_context(OWNER, URL))
            assert await redis_client.zcard(slots[0]) == 0
            storage_release.set()
            assert (await asyncio.wait_for(task, 3))['provider_waiting']
        finally:
            body_release.set()
            storage_release.set()
            await join([task])
    assert sent == ['/file', '/proof']
    async with store.factory() as db:
        unit = await db.scalar(select(models.ProviderWorkUnit).where(models.ProviderWorkUnit.unit_key == 'file'))
        assert unit.state == 'COMPLETED' and unit.receipt_id == 'verified-blob'
        row = await db.get(models.ProviderWork, receipt.work_id)
        assert row.state == 'WAITING' and row.last_reason == 'provider_wait'
    assert not executions[0].drain_callbacks


async def test_range_gap_keeps_the_same_admitted_logical_transfer(store, redis_client):
    await enqueue(store)
    limiter = admission(redis_client)
    gap, proceed, start_new = asyncio.Event(), asyncio.Event(), asyncio.Event()
    executions, ranges, data = [], [], bytearray()

    async def transport(request):
        ranges.append(request.headers.get('Range'))
        assert request.url.path == '/file'
        lo, hi = map(int, request.headers['Range'].split('=')[1].split('-'))
        return httpx.Response(206, headers={'ETag': '"version"', 'Content-Range': f'bytes {lo}-{hi}/6'},
            content=b'abcdef'[lo:hi + 1])

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as raw:
        client = AdmittedHttpClient(raw, OWNER, limiter=limiter)

        async def transfer():
            async with runtime.provider_transfer_scope(timeout=10):
                async with ordered_ranges(client, URL, 6, segment_size=3, concurrency=1) as source:
                    async for block in source:
                        data.extend(block)
                        if len(data) == 3:
                            gap.set()
                            await proceed.wait()

        async def new_file():
            await start_new.wait()
            await client.get(URL.replace('/file', '/new'))

        async def resume(owner):
            executions.append(runtime.current_execution())
            tasks = [asyncio.create_task(transfer()), asyncio.create_task(new_file())]
            try:
                await asyncio.gather(*tasks)
            finally:
                await join(tasks)

        task = asyncio.create_task(runtime.ProviderWorkRunner(store.factory, resume, None).run_once())
        try:
            await asyncio.wait_for(gap.wait(), 3)
            async with limiter.admit(request_context(OWNER, URL)):
                start_new.set()
                await wait_congestion(executions)
                assert not task.done()
            proceed.set()
            assert (await asyncio.wait_for(task, 3))['provider_waiting']
        finally:
            proceed.set()
            await join([task])
    assert data == b'abcdef' and ranges == ['bytes=0-2', 'bytes=3-5']


@pytest.mark.parametrize('file_timeout', [0, .06])
async def test_absolute_drain_deadline_does_not_reset_on_body_progress(store, redis_client, monkeypatch, file_timeout):
    monkeypatch.setattr(runtime, 'CONCURRENCY_DRAIN_SECONDS', .12, raising=False)
    receipt = await enqueue(store)
    limiter = admission(redis_client)
    started, closed = asyncio.Event(), asyncio.Event()
    executions, chunks = [], []

    class Body(httpx.AsyncByteStream):
        async def __aiter__(self):
            started.set()
            try:
                for _ in range(1000):
                    chunks.append(1)
                    yield b'x'
                    await asyncio.sleep(.01)
            finally:
                closed.set()

    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200, stream=Body()))) as raw:
        client = AdmittedHttpClient(raw, OWNER, limiter=limiter)

        async def transfer():
            async with runtime.provider_transfer_scope(timeout=file_timeout):
                await client.get(URL)

        async def resume(owner):
            executions.append(runtime.current_execution())
            tasks = [asyncio.create_task(transfer())]
            await started.wait()
            tasks.append(asyncio.create_task(client.get(URL.replace('/file', '/new'))))
            try:
                await asyncio.gather(*tasks)
            finally:
                await join(tasks)

        task = asyncio.create_task(runtime.ProviderWorkRunner(store.factory, resume, None).run_once())
        try:
            await started.wait()
            others = [await enqueue(store, weight=weight) for weight in (8, 4, 2)]
            result = await asyncio.wait_for(task, 3)
        finally:
            await join([task])
    assert result['provider_waiting'] and closed.is_set() and 3 <= len(chunks) < 100
    assert executions[0].congestion is not None
    async with store.factory() as db:
        row = await db.get(models.ProviderWork, receipt.work_id)
        assert row.state == 'WAITING' and row.last_reason != 'worker_interrupted'
    assert (await claim(store)).work_id in {other.work_id for other in others}
    _, slots, _ = limiter._keys(request_context(OWNER, URL))
    assert await redis_client.zcard(slots[0]) == 0


@pytest.mark.parametrize('stop', ['cancel', 'lease', 'cooldown', 'rate', 'outage'])
async def test_cancel_lease_loss_and_cooldown_interrupt_body_without_drain_wait(store, redis_client, monkeypatch, stop):
    receipt = await enqueue(store)
    limiter = admission(redis_client)
    if stop == 'rate':
        limiter.limits = AdmissionLimits(rate=.01, burst=1, global_concurrency=1,
            tenant_concurrency=1, resource_concurrency=1, lease_seconds=.3)
    started, closed = asyncio.Event(), asyncio.Event()
    executions = []

    class Body(httpx.AsyncByteStream):
        async def __aiter__(self):
            started.set()
            try:
                yield b'a'
                await asyncio.Event().wait()
            finally:
                closed.set()

    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200, stream=Body()))) as raw:
        client = AdmittedHttpClient(raw, OWNER, limiter=limiter)

        async def resume(owner):
            executions.append(runtime.current_execution())
            tasks = [asyncio.create_task(client.get(URL))]
            await started.wait()
            if stop == 'cooldown':
                await limiter.cooldown(request_context(OWNER, URL), 10)
            if stop == 'outage':
                async def unavailable(**kwargs):
                    raise ConnectionError('synthetic coordinator outage')
                monkeypatch.setattr(limiter, '_grant_script', unavailable)
            tasks.append(asyncio.create_task(client.get(URL.replace('/file', '/new'))))
            try:
                await asyncio.gather(*tasks)
            finally:
                await join(tasks)

        task = asyncio.create_task(runtime.ProviderWorkRunner(store.factory, resume, None, renew_s=.02).run_once())
        await asyncio.wait_for(started.wait(), 3)
        try:
            if stop == 'cancel':
                await wait_congestion(executions)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await asyncio.wait_for(task, 2)
            elif stop == 'lease':
                await wait_congestion(executions)
                async with store.factory.begin() as db:
                    await work.cancel_provider_work(db, snapshot_id=receipt.snapshot_id)
                assert (await asyncio.wait_for(task, 2))['provider_lease_lost']
            else:
                assert (await asyncio.wait_for(task, 2))['provider_waiting']
                async with store.factory() as db:
                    row = await db.get(models.ProviderWork, receipt.work_id)
                    assert (row.next_eligible_at - datetime.now(timezone.utc)).total_seconds() > (4 if stop == 'outage' else 8)
                assert executions[0].congestion is None
        finally:
            await join([task])
    assert closed.is_set()


@pytest.mark.parametrize('release_failure', [False, True])
async def test_grant_returning_after_congestion_is_released_before_new_io(store, redis_client, monkeypatch, release_failure):
    await enqueue(store)
    limiter = admission(redis_client)
    limiter.limits = AdmissionLimits(rate=10000, burst=10000, global_concurrency=2,
        tenant_concurrency=2, resource_concurrency=2, lease_seconds=1)
    started, release_body, reserved, release_reply, released = [asyncio.Event() for _ in range(5)]
    executions, late_tokens, sent = [], [], []
    grant, release = limiter._grant_script, limiter._release_script
    grants = 0

    async def delayed_grant(**kwargs):
        nonlocal grants
        result = await grant(**kwargs)
        if int(result[0]) == 1:
            grants += 1
            if grants == 2:
                late_tokens.append(kwargs['args'][2])
                reserved.set()
                await release_reply.wait()
        return result

    async def observe_release(**kwargs):
        result = await release(**kwargs)
        if kwargs['args'][0] in late_tokens:
            released.set()
            if release_failure:
                raise ConnectionError('synthetic release acknowledgement lost')
        return result

    monkeypatch.setattr(limiter, '_grant_script', delayed_grant)
    monkeypatch.setattr(limiter, '_release_script', observe_release)

    class Body(httpx.AsyncByteStream):
        async def __aiter__(self):
            started.set()
            yield b'a'
            await release_body.wait()
            yield b'b'

    async def transport(request):
        sent.append(request.url.path)
        return httpx.Response(200, stream=Body())

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as raw:
        client = AdmittedHttpClient(raw, OWNER, limiter=limiter)

        async def resume(owner):
            executions.append(runtime.current_execution())
            tasks = [asyncio.create_task(client.get(URL))]
            await started.wait()
            tasks.append(asyncio.create_task(client.get(URL.replace('/file', '/late'))))
            await reserved.wait()
            tasks.append(asyncio.create_task(client.get(URL.replace('/file', '/denied'))))
            try:
                await asyncio.gather(*tasks)
            finally:
                await join(tasks)

        task = asyncio.create_task(runtime.ProviderWorkRunner(store.factory, resume, None).run_once())
        try:
            await wait_congestion(executions)
            release_reply.set()
            await asyncio.wait_for(released.wait(), 3)
            if release_failure:
                assert (await asyncio.wait_for(task, 3))['provider_waiting']
                async with store.factory() as db:
                    row = await db.scalar(select(models.ProviderWork))
                    assert row.last_reason == 'coordinator_unavailable'
                return
            _, slots, _ = limiter._keys(request_context(OWNER, URL))
            assert await redis_client.zcard(slots[0]) == 1
            assert sent == ['/file']
            release_body.set()
            assert (await asyncio.wait_for(task, 3))['provider_waiting']
        finally:
            release_reply.set()
            release_body.set()
            await join([task])
    assert sent == ['/file']


async def test_receipt_timeout_is_not_misreported_as_the_drain_deadline(store, redis_client):
    receipt = await enqueue(store)
    limiter = admission(redis_client)

    async def flush():
        raise TimeoutError('receipt proof timed out')

    async def resume(owner):
        with runtime.provider_drain_callback(flush):
            from shared.provider_http import admitted
            async with admitted(limiter, request_context(OWNER, URL)):
                pytest.fail('contended request must not transmit')

    async with limiter.admit(request_context(OWNER, URL)):
        runner = runtime.ProviderWorkRunner(store.factory, resume, None)
        with pytest.raises(TimeoutError, match='receipt proof timed out'):
            await runner.run_once()
    async with store.factory() as db:
        row = await db.get(models.ProviderWork, receipt.work_id)
        assert row.state == 'RESUMING'


async def test_returning_last_transfer_waits_for_pending_drain_before_finishing(store, redis_client, monkeypatch):
    receipt = await enqueue(store)
    limiter = admission(redis_client)
    gap, next_range, returned, release_drain = [asyncio.Event() for _ in range(4)]
    executions = []
    drain = runtime._drain_congestion

    async def delayed_drain(execution):
        await release_drain.wait()
        await drain(execution)

    monkeypatch.setattr(runtime, '_drain_congestion', delayed_drain)

    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200, content=b'abc'))) as raw:
        client = AdmittedHttpClient(raw, OWNER, limiter=limiter)

        async def resume(owner):
            executions.append(runtime.current_execution())
            async with runtime.provider_transfer_scope(timeout=10):
                await client.get(URL)
                gap.set()
                await next_range.wait()
                await client.get(URL)
            returned.set()
            return {'finished': True}

        async def settle(work_id):
            pass

        task = asyncio.create_task(runtime.ProviderWorkRunner(store.factory, resume, settle).run_once())
        try:
            await asyncio.wait_for(gap.wait(), 3)
            async with limiter.admit(request_context(OWNER, URL)):
                next_range.set()
                await wait_congestion(executions)
            await asyncio.wait_for(returned.wait(), 3)
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(asyncio.shield(task), .1)
            async with store.factory() as db:
                row = await db.get(models.ProviderWork, receipt.work_id)
                assert row.state == 'RESUMING'
            release_drain.set()
            assert (await asyncio.wait_for(task, 3))['provider_waiting']
        finally:
            next_range.set()
            release_drain.set()
            await join([task])
