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
