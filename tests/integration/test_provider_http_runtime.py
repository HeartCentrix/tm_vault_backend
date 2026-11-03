"""Attempt checkpoints must never occupy provider network permits."""
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import event, select

from test_chat_catalogue_postgres import store
from test_graph_admission_redis import redis_client
from test_provider_work_postgres import claim, enqueue
from shared import models, provider_runtime as runtime, provider_work as work
from shared.provider_admission import AdmissionLimits, ProviderRequestContext, ScopedProviderAdmission
from shared.provider_http import AdmittedHttpClient

pytestmark = pytest.mark.integration
OWNER = SimpleNamespace(tenant_id='tenant', client_id='app')
URL = 'https://graph.microsoft.com/v1.0/chats/chat-A/messages'


def admission(redis_client):
    return ScopedProviderAdmission(redis_client, namespace=uuid4().hex,
        limits=AdmissionLimits(rate=10000, burst=10000, global_concurrency=2,
            tenant_concurrency=1, resource_concurrency=4))


@asynccontextmanager
async def execution_scope(store, owner):
    execution = runtime.ProviderExecution(store.factory, owner)
    marker = runtime._EXECUTION.set(execution)
    try:
        with work.work_scope(owner):
            yield execution
    finally:
        await runtime._join_provider_tasks(execution)
        runtime._EXECUTION.reset(marker)


async def send(client, mode):
    if mode == 'batch':
        return await client.post('https://graph.microsoft.com/v1.0/$batch', json={
            'requests': [{'id': 'one', 'method': 'GET', 'url': '/chats/chat-A/messages'}]})
    if mode == 'stream':
        async with client.stream('GET', URL) as response:
            await response.aread()
            return response
    return await client.get(URL)


@pytest.mark.parametrize('mode', ['request', 'stream', 'batch'])
@pytest.mark.parametrize('existing_unit', [False, True])
async def test_checkpoint_wait_leaves_tenant_slot_available(store, redis_client, mode, existing_unit):
    await enqueue(store)
    owner = await claim(store)
    limiter, sent, at_parent = admission(redis_client), [], asyncio.Event()

    def trace(conn, cursor, statement, parameters, context, executemany):
        if 'FROM jobs' in statement and 'FOR SHARE' in statement:
            at_parent.set()

    async def transport(request):
        sent.append(request.url.path)
        return httpx.Response(200, json={'responses': [{'id': 'one', 'status': 200}]})

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as raw:
        client = AdmittedHttpClient(raw, OWNER, limiter=limiter)
        async with execution_scope(store, owner):
            unit_marker = None
            if existing_unit:
                async with runtime.unit_scope('chat', 'chat') as unit:
                    pass
                unit_marker = runtime._UNIT.set(unit)
            task = None
            event.listen(store.engine.sync_engine, 'before_cursor_execute', trace)
            try:
                async with store.factory.begin() as blocker:
                    await blocker.scalar(select(models.Job).where(models.Job.id == owner.job_id).with_for_update())
                    task = asyncio.create_task(send(client, mode))
                    await asyncio.wait_for(at_parent.wait(), 2)
                    assert sent == []
                    assert await redis_client.zcard(limiter.prefix + 'active') == 0
                    peer = ProviderRequestContext('tenant', 'other-app', 'sharepoint', 'drive-B')
                    async with limiter.admit(peer, wait_budget=0):
                        assert sent == []
                assert (await asyncio.wait_for(task, 3)).status_code == 200
                assert len(sent) == 1
                assert await redis_client.zcard(limiter.prefix + 'active') == 0
            finally:
                event.remove(store.engine.sync_engine, 'before_cursor_execute', trace)
                if task is not None:
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                if unit_marker is not None:
                    runtime._UNIT.reset(unit_marker)


async def test_cancelled_admission_waits_reuse_unspent_attempts(store, redis_client):
    await enqueue(store)
    owner = await claim(store)
    limiter, sent = admission(redis_client), []

    async def transport(request):
        sent.append(request)
        return httpx.Response(200, json={'ok': True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as raw:
        client = AdmittedHttpClient(raw, OWNER, limiter=limiter)
        async with execution_scope(store, owner):
            async with runtime.unit_scope('chat', 'chat') as unit, runtime.provider_live_wait_scope():
                async with limiter.admit(ProviderRequestContext('tenant', 'holder', 'sharepoint'), wait_budget=0):
                    for _ in range(20):
                        task = asyncio.create_task(client.get(URL))
                        try:
                            async with asyncio.timeout(2):
                                while not await redis_client.hlen(limiter.prefix + 'fair:pending'):
                                    await asyncio.sleep(.001)
                        finally:
                            task.cancel()
                            with pytest.raises(asyncio.CancelledError):
                                await task
                        assert sent == []
                    assert unit.remaining == 16
                    async with store.factory() as db:
                        assert await db.scalar(select(models.ProviderWorkUnit.attempts).where(
                            models.ProviderWorkUnit.work_id == owner.work_id)) == 16
                assert (await client.get(URL)).status_code == 200
                assert len(sent) == 1
                assert unit.remaining == 15
                assert await redis_client.zcard(limiter.prefix + 'active') == 0



async def test_concurrent_planning_requests_share_allowance(store, redis_client):
    await enqueue(store)
    owner = await claim(store)
    limiter, sent = admission(redis_client), []

    async def transport(request):
        sent.append(request)
        return httpx.Response(200, json={'ok': True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as raw:
        client = AdmittedHttpClient(raw, OWNER, limiter=limiter)
        async with execution_scope(store, owner) as execution:
            async with runtime.provider_live_wait_scope():
                responses = await asyncio.gather(*(client.get(URL) for _ in range(33)))
            assert all(response.status_code == 200 for response in responses)
            assert len(sent) == 33
            async with store.factory() as db:
                reserved = await db.scalar(select(models.ProviderWorkUnit.attempts).where(
                    models.ProviderWorkUnit.work_id == owner.work_id))
            assert reserved == 48
            assert execution.planning_unit.remaining == 15
    assert await redis_client.zcard(limiter.prefix + 'active') == 0


async def test_redirect_refill_releases_previous_hop_before_checkpoint(store, redis_client):
    await enqueue(store)
    owner = await claim(store)
    limiter, sent, at_parent = admission(redis_client), [], asyncio.Event()

    def trace(conn, cursor, statement, parameters, context, executemany):
        if 'FROM jobs' in statement and 'FOR SHARE' in statement:
            at_parent.set()

    async def transport(request):
        sent.append(str(request.url))
        if len(sent) == 1:
            return httpx.Response(302, headers={'Location': 'https://tenant.sharepoint.com/file'})
        return httpx.Response(200, content=b'file')

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport), follow_redirects=True) as raw:
        client = AdmittedHttpClient(raw, OWNER, limiter=limiter)
        async with execution_scope(store, owner):
            async with runtime.unit_scope('chat', 'chat') as unit:
                for _ in range(15):
                    await runtime.account_provider_attempt()
                event.listen(store.engine.sync_engine, 'before_cursor_execute', trace)
                task = None
                try:
                    async with store.factory.begin() as blocker:
                        await blocker.scalar(select(models.Job).where(models.Job.id == owner.job_id).with_for_update())
                        task = asyncio.create_task(client.get(URL))
                        await asyncio.wait_for(at_parent.wait(), 2)
                        assert sent == [URL]
                        assert await redis_client.zcard(limiter.prefix + 'active') == 0
                        async with limiter.admit(ProviderRequestContext('tenant', 'peer', 'sharepoint'), wait_budget=0):
                            assert sent == [URL]
                    assert (await asyncio.wait_for(task, 3)).content == b'file'
                    assert len(sent) == 2
                    assert unit.remaining == 15
                finally:
                    event.remove(store.engine.sync_engine, 'before_cursor_execute', trace)
                    if task is not None:
                        task.cancel()
                        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize('stop', ['task_cancel', 'parent_cancel', 'defer', 'deadline'])
async def test_stopped_checkpoint_cannot_transmit_after_wait(store, redis_client, stop):
    await enqueue(store)
    owner = await claim(store)
    limiter, sent, at_parent = admission(redis_client), [], asyncio.Event()

    def trace(conn, cursor, statement, parameters, context, executemany):
        if 'FROM jobs' in statement and 'FOR SHARE' in statement:
            at_parent.set()

    async def transport(request):
        sent.append(request)
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as raw:
        client = AdmittedHttpClient(raw, OWNER, limiter=limiter)
        async with execution_scope(store, owner) as execution:
            async with runtime.unit_scope('chat', 'chat') as unit:
                event.listen(store.engine.sync_engine, 'before_cursor_execute', trace)
                task = None
                try:
                    async with store.factory.begin() as blocker:
                        job = await blocker.scalar(select(models.Job).where(models.Job.id == owner.job_id).with_for_update())
                        task = asyncio.create_task(client.get(URL))
                        await asyncio.wait_for(at_parent.wait(), 2)
                        assert sent == []
                        if stop == 'task_cancel':
                            task.cancel()
                        elif stop == 'parent_cancel':
                            job.status = models.JobStatus.CANCELLED
                        elif stop == 'defer':
                            from shared.provider_admission import ProviderDeferred
                            execution.defer(ProviderDeferred(None, 1))
                        else:
                            unit.deadline_at = datetime.now(timezone.utc) - timedelta(seconds=1)
                    expected = {'task_cancel': asyncio.CancelledError, 'parent_cancel': work.ProviderWorkConflict,
                        'defer': runtime.ProviderTurnDeferred, 'deadline': runtime.ProviderBudgetExhausted}[stop]
                    with pytest.raises(expected):
                        await asyncio.wait_for(task, 3)
                    assert sent == []
                    assert await redis_client.zcard(limiter.prefix + 'active') == 0
                finally:
                    event.remove(store.engine.sync_engine, 'before_cursor_execute', trace)
                    if task is not None:
                        task.cancel()
                        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize('stop', ['transport_failure', 'task_cancel'])
async def test_started_transmission_keeps_its_attempt_charge(store, redis_client, stop):
    await enqueue(store)
    owner = await claim(store)
    limiter, sent, entered = admission(redis_client), [], asyncio.Event()

    async def transport(request):
        sent.append(request)
        entered.set()
        if len(sent) == 1:
            if stop == 'transport_failure':
                raise httpx.WriteError('transmission outcome unknown')
            await asyncio.Event().wait()
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as raw:
        client = AdmittedHttpClient(raw, OWNER, limiter=limiter)
        async with execution_scope(store, owner):
            async with runtime.unit_scope('chat', 'chat') as unit:
                task = asyncio.create_task(client.get(URL))
                try:
                    await asyncio.wait_for(entered.wait(), 2)
                    if stop == 'task_cancel':
                        task.cancel()
                    with pytest.raises(asyncio.CancelledError if stop == 'task_cancel' else httpx.WriteError):
                        await task
                finally:
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                assert len(sent) == 1
                assert unit.remaining == 15
                assert (await client.get(URL)).status_code == 200
                assert len(sent) == 2
                assert unit.remaining == 14
                assert await redis_client.zcard(limiter.prefix + 'active') == 0
