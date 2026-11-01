"""Dynamic fair queues must not bank credit while empty or source-draining."""
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest
from shared import copy_work, models
from test_chat_catalogue_postgres import store


pytestmark = pytest.mark.integration
QUEUE_COLUMNS = {'TENANT': 'tenant_queue_key', 'POLICY': 'queue_key', 'RESOURCE': 'resource_queue_key'}


def entity_key(store, scope, entity):
    if scope == 'TENANT':
        return copy_work._key(scope, entity, 'giant')
    if scope == 'POLICY':
        return copy_work._key(scope, store.tenant, entity, 'giant')
    return copy_work._key(scope, store.tenant, None, entity, 'giant')


def entity(store, scope, *, above=None, below=None, used=None):
    for value in range(1, 10001):
        candidate = UUID(int=value)
        key = entity_key(store, scope, candidate)
        if (used is None or candidate not in used) and (above is None or key > above) and (below is None or key < below):
            if used is not None:
                used.add(candidate)
            return candidate
    pytest.fail('could not construct adversarial queue ordering')


async def request(store, scope='POLICY', *, identity=None, weight=1, lane='giant'):
    identity = identity or uuid4()
    tenant, resource, policy = store.tenant, store.resource, None
    async with store.factory.begin() as session:
        if scope == 'TENANT':
            tenant, resource = identity, uuid4()
            if await session.get(models.Tenant, tenant) is None:
                session.add(models.Tenant(id=tenant, org_id=store.org.id, display_name='Synthetic tenant'))
                await session.flush()
        elif scope == 'POLICY':
            policy = identity
            if await session.get(models.SlaPolicy, policy) is None:
                session.add(models.SlaPolicy(id=policy, tenant_id=tenant, name='Synthetic policy'))
                await session.flush()
        else:
            resource = identity
        if await session.get(models.Resource, resource) is None:
            session.add(models.Resource(id=resource, tenant_id=tenant, type=models.ResourceType.USER_CHATS,
                                        external_id=uuid4().hex, display_name='Synthetic resource'))
            await session.flush()
        job = models.Job(type=models.JobType.BACKUP, tenant_id=tenant, resource_id=resource,
                         status=models.JobStatus.RUNNING, priority=1 if weight > 1 else 5)
        session.add(job)
        await session.flush()
        snapshot = models.Snapshot(resource_id=resource, job_id=job.id, backend_id=store.backend)
        session.add(snapshot)
        await session.flush()
        return await copy_work.enqueue(session, copy_work.CopyRequest(
            kind='CHAT_CARRY', lane=lane, target_snapshot_id=snapshot.id,
            sla_policy_id=policy, weight=weight,
        ))


async def activate(store, task):
    async with store.factory.begin() as session:
        await copy_work.align_ready_workflow(session, task.workflow_id)
        (await session.get(models.CopyWorkflow, task.workflow_id)).state = 'WAITING'


async def score(store, task, scope, value=None):
    async with store.factory.begin() as session:
        queue = await session.get(models.CopyWorkQueue, getattr(task, QUEUE_COLUMNS[scope]))
        if value is not None:
            queue.virtual_finish = value
        return queue.virtual_finish


@pytest.mark.parametrize('scope', ['TENANT', 'POLICY', 'RESOURCE'])
async def test_established_custom_queue_progresses_with_continuously_new_smaller_key_arrivals(store, scope):
    used = set()
    custom_id = entity(store, scope, above='8' * 64, used=used)
    custom = await request(store, scope, identity=custom_id)
    custom_key = getattr(custom, QUEUE_COLUMNS[scope])
    hot = await request(store, scope, identity=entity(store, scope, below=custom_key, used=used), weight=8)
    await activate(store, hot)
    await score(store, custom, scope, 1)
    await activate(store, custom)
    assert await score(store, hot, scope) == 0
    served = []
    for _ in range(12):
        arrival = await request(store, scope, identity=entity(store, scope, below=custom_key, used=used), weight=8)
        await activate(store, arrival)
        async with store.factory.begin() as session:
            claim = await copy_work.claim(session, owner_id=uuid4())
            assert claim is not None
            served.append(claim.task_id)
            task = await session.get(models.CopyWorkTask, claim.task_id)
            task.state = 'COMPLETED'
            task.lease_owner_id = task.lease_expires_at = None
            (await session.get(models.CopyWorkflow, task.workflow_id)).state = 'COMPLETED'
    assert custom.id in served, f'{scope} starved behind newly arriving zero-credit queues'


@pytest.mark.parametrize('previous_finish', [0, 100])
async def test_reactivated_inactive_policy_aligns_up_without_lowering_ahead_clock(store, previous_finish):
    old = await request(store, weight=8)
    await activate(store, old)
    async with store.factory.begin() as session:
        (await session.get(models.CopyWorkTask, old.id)).state = 'COMPLETED'
        (await session.get(models.CopyWorkflow, old.workflow_id)).state = 'COMPLETED'
    await score(store, old, 'POLICY', previous_finish)
    custom = await request(store)
    await activate(store, custom)
    await score(store, custom, 'POLICY', 50)
    reactivated = await request(store, identity=old.sla_policy_id, weight=8)
    await activate(store, reactivated)
    assert await score(store, reactivated, 'POLICY') == max(previous_finish, 50.125)
    async with store.factory.begin() as session:
        assert (await copy_work.claim(session, owner_id=uuid4())).task_id == custom.id


@pytest.mark.parametrize('state', ['READY', 'RUNNING'])
async def test_already_backlogged_deferred_policy_is_not_reset(store, state):
    active = await request(store, weight=8)
    await activate(store, active)
    await score(store, active, 'POLICY', 0)
    async with store.factory.begin() as session:
        task = await session.get(models.CopyWorkTask, active.id)
        task.state = state
        task.ready_at = task.lease_expires_at = datetime.now(timezone.utc) + timedelta(hours=1)
    sibling = await request(store)
    await activate(store, sibling)
    await score(store, sibling, 'POLICY', 100)
    arrival = await request(store, identity=active.sla_policy_id, weight=8)
    await activate(store, arrival)
    assert await score(store, arrival, 'POLICY') == 0


@pytest.mark.parametrize('backlog_state', ['READY', 'RUNNING'])
async def test_alignment_ignores_draining_and_stale_attempt_siblings_but_counts_deferred_backlog(store, backlog_state):
    backlog = await request(store, weight=8)
    await activate(store, backlog)
    await score(store, backlog, 'POLICY', 100)
    async with store.factory.begin() as session:
        task = await session.get(models.CopyWorkTask, backlog.id)
        task.state = backlog_state
        task.ready_at = task.lease_expires_at = datetime.now(timezone.utc) + timedelta(hours=1)
    draining = await request(store)
    await score(store, draining, 'POLICY', -100)
    stale = await request(store)
    await activate(store, stale)
    await score(store, stale, 'POLICY', -200)
    async with store.factory.begin() as session:
        (await session.get(models.CopyWorkflow, stale.workflow_id)).capture_attempt += 1
    other_lane = await request(store, lane='micro')
    await activate(store, other_lane)
    await score(store, other_lane, 'POLICY', -300)
    other_parent = await request(store, 'TENANT')
    await activate(store, other_parent)
    await score(store, other_parent, 'POLICY', -400)
    arrival = await request(store, weight=8)
    await activate(store, arrival)
    assert await score(store, arrival, 'POLICY') == 100.125


async def test_stale_attempt_does_not_keep_an_inactive_policy_backlogged(store):
    old = await request(store, weight=8)
    await activate(store, old)
    async with store.factory.begin() as session:
        (await session.get(models.CopyWorkflow, old.workflow_id)).capture_attempt += 1
    custom = await request(store)
    await activate(store, custom)
    await score(store, custom, 'POLICY', 50)
    arrival = await request(store, identity=old.sla_policy_id, weight=8)
    await activate(store, arrival)
    assert await score(store, arrival, 'POLICY') == 50.125
