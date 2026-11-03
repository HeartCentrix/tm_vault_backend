"""Strict chat requests share pacing at the atomic Redis network grant."""
import asyncio
from dataclasses import replace
import time
from uuid import uuid4

import pytest
from test_graph_admission_redis import redis_client

from shared.provider_admission import (
    AdmissionLimits, ProviderDeferred, ProviderRequestContext, ScopedProviderAdmission,
)

pytestmark = pytest.mark.integration


@pytest.mark.parametrize('resource', ['chat:chat-A', 'user_chats:user-A'])
def test_chat_pacing_accepts_only_explicit_teams_resource_scopes(resource):
    assert ProviderRequestContext('tenant', 'app', 'teams', resource, paced_chat=True).paced_chat


@pytest.mark.parametrize('workload,resource,paced_chat', [
    ('teams', 'chat:A', 1), ('teams', 'chat:A', 'true'), ('teams', 'chat:A', None),
    ('teams', None, True), ('teams', '', True), ('teams', 'chat:', True),
    ('teams', 'user_chats:', True), ('teams', 'channel:A', True),
    ('sharepoint', 'chat:A', True), ('exchange', 'user_chats:A', True),
])
def test_chat_pacing_rejects_invalid_opt_in(workload, resource, paced_chat):
    with pytest.raises(ValueError, match='chat pacing'):
        ProviderRequestContext('tenant', 'app', workload, resource, paced_chat=paced_chat)


def chat_scope(**changes):
    return replace(ProviderRequestContext('tenant', 'app', 'teams', 'chat:A', paced_chat=True), **changes)


@pytest.mark.parametrize('resource', ['chat:A', 'user_chats:A'])
async def test_pacing_is_shared_across_coordinators_and_app_ids(redis_client, resource):
    namespace = uuid4().hex
    coordinators = [ScopedProviderAdmission(redis_client, namespace=namespace) for _ in range(3)]
    granted = []

    async def request(index):
        context = chat_scope(resource=resource, app_id=f'app-{index}')
        async with coordinators[index].admit(context, wait_budget=4):
            granted.append(time.monotonic())

    await asyncio.gather(*(request(index) for index in range(3)))
    assert all(later - earlier >= .95 for earlier, later in zip(granted, granted[1:]))


async def test_chat_pacing_isolated_by_tenant_resource_and_generic_bucket(redis_client):
    admission = ScopedProviderAdmission(redis_client, namespace=uuid4().hex)
    first = chat_scope()
    async with admission.admit(first, wait_budget=0):
        pass
    for peer in [replace(first, tenant_id='other'), replace(first, resource='chat:B'),
            replace(first, resource='user_chats:A'), replace(first, paced_chat=False)]:
        async with admission.admit(peer, wait_budget=0):
            pass
    with pytest.raises(ProviderDeferred) as denied:
        async with admission.admit(replace(first, app_id='other'), wait_budget=0):
            pytest.fail('a generic request refilled the chat pacing bucket')
    assert (denied.value.reason, denied.value.scope) == ('rate', 'resource')
    assert denied.value.retry_after > .8


async def test_rate_waiter_holds_no_network_slot_or_future_capacity(redis_client):
    admission = ScopedProviderAdmission(redis_client, namespace=uuid4().hex,
        limits=AdmissionLimits(global_concurrency=1, tenant_concurrency=1))
    context = chat_scope()
    async with admission.admit(context, wait_budget=0):
        pass
    entered = asyncio.Event()

    async def waiting_request():
        async with admission.admit(context, wait_budget=3):
            entered.set()

    task = asyncio.create_task(waiting_request())
    try:
        async with asyncio.timeout(.5):
            while not await redis_client.hlen(admission.prefix + 'fair:pending'):
                await asyncio.sleep(.001)
        assert not entered.is_set()
        _, slots, _ = admission._keys(context)
        assert [await redis_client.zcard(key) for key in slots] == [0, 0, 0]
        for workload in ['exchange', 'sharepoint']:
            async with admission.admit(ProviderRequestContext('tenant', 'app', workload), wait_budget=0):
                pass
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert not entered.is_set()
    assert await redis_client.hlen(admission.prefix + 'fair:pending') == 0
    assert await redis_client.zcard(admission.prefix + 'fair:expiry') == 0
    # A cancelled waiter must leave the next refill available for a fresh request.
    async with admission.admit(context, wait_budget=1.1):
        pass


@pytest.mark.parametrize('duplicate,cost', [(True, 1), (False, 2), (False, .5)])
async def test_impossible_paced_request_fails_before_queueing(redis_client, duplicate, cost):
    admission = ScopedProviderAdmission(redis_client, namespace=uuid4().hex)
    requirements = [(chat_scope(), cost)] * (2 if duplicate else 1)
    with pytest.raises(ValueError, match='cost|burst'):
        async with asyncio.timeout(.2):
            async with admission.admit_many(requirements, wait_budget=30):
                pytest.fail('impossible paced cost was accepted')
    assert await redis_client.hlen(admission.prefix + 'fair:pending') == 0


async def test_generic_chat_batch_preserves_existing_cost_and_burst(redis_client):
    admission = ScopedProviderAdmission(redis_client, namespace=uuid4().hex)
    generic = chat_scope(paced_chat=False)
    async with admission.admit_many([(generic, 1), (generic, 1)], wait_budget=0):
        pass
    async with admission.admit(generic, cost=3, wait_budget=0):
        pass


async def test_slot_denial_does_not_spend_chat_rate_token(redis_client):
    admission = ScopedProviderAdmission(redis_client, namespace=uuid4().hex,
        limits=AdmissionLimits(global_concurrency=1))
    async with admission.admit(ProviderRequestContext('other', 'app', 'sharepoint'), wait_budget=0):
        with pytest.raises(ProviderDeferred) as denied:
            async with admission.admit(chat_scope(), wait_budget=0):
                pytest.fail('network slot limit was bypassed')
        assert denied.value.reason == 'concurrency'
    async with admission.admit(chat_scope(), wait_budget=0):
        pass


async def test_live_chat_rate_wait_keeps_execution_after_short_admission_budget(redis_client):
    from shared import provider_runtime as runtime
    admission = ScopedProviderAdmission(redis_client, namespace=uuid4().hex)
    execution = runtime.ProviderExecution(None, None)
    marker = runtime._EXECUTION.set(execution)
    progress = []
    try:
        async with runtime.provider_live_wait_scope():
            async with admission.admit(chat_scope(), wait_budget=0):
                progress.append('page-one')
            async with admission.admit(chat_scope(), wait_budget=0):
                progress.append('page-two')
        assert progress == ['page-one', 'page-two']
        assert execution.deferred is None
        assert execution.congestion is None
        assert execution.drain_task is None
    finally:
        runtime._EXECUTION.reset(marker)


async def test_chat_pacing_fails_closed_when_redis_is_unavailable(redis_client):
    from redis.asyncio import Redis
    # The fixture owns this directory; the absent socket cannot reach a live service.
    socket = redis_client.connection_pool.connection_kwargs['path'] + '.missing'
    unavailable = Redis(unix_socket_path=socket, decode_responses=True, socket_timeout=.1)
    try:
        admission = ScopedProviderAdmission(unavailable, namespace=uuid4().hex)
        with pytest.raises(ProviderDeferred) as denied:
            async with admission.admit(chat_scope(), wait_budget=0):
                pytest.fail('uncoordinated request was admitted')
        assert denied.value.stage == 'coordinator_unavailable'
    finally:
        await unavailable.aclose()


async def test_strict_sources_share_network_pacing_across_pages_and_hosted_content(redis_client, monkeypatch):
    from datetime import datetime, timedelta, timezone
    import importlib
    import httpx
    from shared import provider_runtime as runtime
    from shared.chat_capture_graph import pinned_chat_source
    from shared.config import settings
    from shared.graph_client import GraphClient
    from shared.graph_rate_limiter import GraphRateLimiter, _LocalTokenBucket, _RedisTokenBucket

    monkeypatch.setenv('PROVIDER_SCOPED_ADMISSION_ENABLED', 'true')
    monkeypatch.setenv('PROVIDER_ADMISSION_WAIT_SECONDS', '0')
    monkeypatch.setattr(settings, 'GRAPH_STREAM_PACE_REQS_PER_SEC', 0)
    monkeypatch.setattr(settings, 'GRAPH_APP_PACE_REQS_PER_SEC', 0)
    namespace = uuid4().hex
    admission = ScopedProviderAdmission(redis_client, namespace=namespace,
        limits=AdmissionLimits(global_concurrency=1, tenant_concurrency=1))
    limiter = GraphRateLimiter()
    limiter._scoped = admission
    limiter._bucket = _RedisTokenBucket(redis_client, namespace + ':legacy', 10000, 10000,
        _LocalTokenBucket(10000, 10000))
    monkeypatch.setattr(importlib.import_module('shared.graph_rate_limiter'), 'graph_rate_limiter', limiter)
    sent, first_send = [], asyncio.Event()

    async def transport(request):
        if request.url.host == 'login.microsoftonline.com':
            return httpx.Response(200, json={'access_token': 'synthetic-token', 'expires_in': 3600})
        assert await redis_client.zcard(admission.prefix + 'active') == 1
        sent.append((time.monotonic(), request.url.path))
        first_send.set()
        if request.url.path.endswith('/$value'):
            return httpx.Response(200, content=b'hosted-bytes')
        if request.url.path.endswith('/hostedContents'):
            return httpx.Response(200, json={'value': [{'id': 'hosted'}]})
        if request.url.params.get('$skiptoken') == 'next':
            return httpx.Response(200, json={'value': [{'id': 'message-two'}]})
        return httpx.Response(200, json={'value': [{'id': 'message-one'}],
            '@odata.nextLink': str(request.url.copy_with(query=b'$skiptoken=next'))})

    original_client = httpx.AsyncClient

    def http_client(*args, **kwargs):
        return original_client(*args, **kwargs, transport=httpx.MockTransport(transport))

    monkeypatch.setattr(httpx, 'AsyncClient', http_client)

    async def capture(source, *, messages):
        execution = runtime.ProviderExecution(None, None)
        now = datetime.now(timezone.utc)
        unit = runtime.UnitExecution('chat:A', 'chat', now + timedelta(seconds=15), now, 0, remaining=16)
        execution_marker, unit_marker = runtime._EXECUTION.set(execution), runtime._UNIT.set(unit)
        try:
            async with runtime.provider_live_wait_scope():
                if messages:
                    result = [page async for page in source.iter_message_pages('A')]
                    assert result == [[{'id': 'message-one'}], [{'id': 'message-two'}]]
                else:
                    assert [item async for item in source.iter_hosted_contents('A', 'message')] == [{'id': 'hosted'}]
                    async with source.open_hosted('A', 'message', 'hosted') as (body, _, _):
                        assert b''.join([chunk async for chunk in body]) == b'hosted-bytes'
                        assert await redis_client.zcard(admission.prefix + 'active') == 1
                assert runtime.current_execution() is execution
                assert runtime.current_unit() is unit
                assert unit.remaining == 14
                assert execution.deferred is None and execution.drain_task is None
        finally:
            runtime._UNIT.reset(unit_marker)
            runtime._EXECUTION.reset(execution_marker)

    async with pinned_chat_source(GraphClient('app-one', 'secret', 'tenant'), principal_id='alice') as first:
        async with pinned_chat_source(GraphClient('app-two', 'secret', 'tenant'), principal_id='bob') as second:
            tasks = [asyncio.create_task(capture(first, messages=True)),
                asyncio.create_task(capture(second, messages=False))]
            try:
                await asyncio.wait_for(first_send.wait(), 1)
                async with asyncio.timeout(.8):
                    while await redis_client.hlen(admission.prefix + 'fair:pending') != 2:
                        await asyncio.sleep(.001)
                assert len(sent) == 1
                assert await redis_client.zcard(admission.prefix + 'active') == 0
                async with admission.admit(ProviderRequestContext('tenant', 'mail-app', 'exchange'), wait_budget=0):
                    assert len(sent) == 1
                await asyncio.wait_for(asyncio.gather(*tasks), 6)
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
    assert len(sent) == 4
    assert all(later[0] - earlier[0] >= .95 for earlier, later in zip(sent, sent[1:]))
    assert {path.split('/')[3] for _, path in sent} == {'alice', 'bob'}
    assert await redis_client.zcard(admission.prefix + 'active') == 0
    assert await redis_client.hlen(admission.prefix + 'fair:pending') == 0
