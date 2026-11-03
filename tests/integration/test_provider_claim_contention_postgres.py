"""Provider admission contention, quotas and service clocks on real PostgreSQL."""
import asyncio
from collections import Counter
from datetime import datetime, timezone
from uuid import uuid4

import pytest
from sqlalchemy import event, select, update

from shared import models, provider_work as work
from test_chat_catalogue_postgres import store
from test_provider_work_postgres import claim, enqueue

pytestmark = pytest.mark.integration


async def test_claim_can_admit_sibling_while_checkpoint_holds_job_read_lock(store):
    first = await enqueue(store)
    second_scope = await store.new_scope()
    async with store.factory.begin() as db:
        sibling_resource = models.Resource(id=uuid4(), tenant_id=store.tenant,
            type=models.ResourceType.USER_MAIL, external_id='sibling-mail', display_name='Sibling mail')
        db.add(sibling_resource)
        await db.flush()
        await db.execute(update(models.Snapshot).where(models.Snapshot.id == second_scope.snapshot_id)
            .values(job_id=first.job_id, resource_id=sibling_resource.id))
        second = await work.ensure_work(db, snapshot_id=second_scope.snapshot_id,
            snapshot_token=0, job_token=0)
    owner = await claim(store)
    assert owner.work_id == first.work_id
    async with store.factory.begin() as checkpoint:
        await work.record_unit_checkpoint(checkpoint, owner, unit_key='page', kind='mail')
        sibling = await asyncio.wait_for(claim(store), 2)
        assert sibling is not None, 'A sibling checkpoint must not prevent provider admission'
        assert sibling.work_id == second.work_id


@pytest.mark.parametrize('blocked_parent', ['job', 'snapshot'])
async def test_skipped_parent_does_not_lock_its_children(store, blocked_parent):
    scope = await store.new_scope()
    partition_id = uuid4()
    async with store.factory.begin() as db:
        snapshot = await db.get(models.Snapshot, scope.snapshot_id)
        db.add(models.SnapshotPartition(id=partition_id, snapshot_id=snapshot.id,
            job_id=snapshot.job_id, resource_id=store.resource, tenant_id=store.tenant,
            partition_index=0, status='IN_PROGRESS'))
        await db.flush()
        receipt = await work.ensure_work(db, snapshot_id=snapshot.id, snapshot_token=0,
            job_token=0, partition_id=partition_id, partition_token=0)
    parent_model, parent_id = ((models.Job, receipt.job_id) if blocked_parent == 'job'
        else (models.Snapshot, receipt.snapshot_id))
    child_model, child_id = ((models.Snapshot, receipt.snapshot_id) if blocked_parent == 'job'
        else (models.SnapshotPartition, partition_id))
    async with store.factory.begin() as blocker:
        await blocker.scalar(select(parent_model).where(parent_model.id == parent_id).with_for_update())
        async with store.factory.begin() as admission:
            assert await work.claim_due_work(admission, owner_id=uuid4()) is None
            async with store.factory.begin() as probe:
                assert await probe.scalar(select(child_model).where(child_model.id == child_id)
                    .with_for_update(nowait=True)) is not None


async def test_blocked_candidates_do_not_repeat_capacity_scans(store):
    pending = [await enqueue(store) for _ in range(12)]
    statements = []

    def record(conn, cursor, statement, parameters, context, executemany):
        statements.append(statement.lower())

    async with store.factory.begin() as blocker:
        await blocker.scalars(select(models.Job).where(models.Job.id.in_([r.job_id for r in pending]))
            .with_for_update())
        event.listen(store.engine.sync_engine, 'before_cursor_execute', record)
        try:
            assert await claim(store) is None
        finally:
            event.remove(store.engine.sync_engine, 'before_cursor_execute', record)
    capacity_scans = [sql for sql in statements if sql.startswith('select count(')]
    assert len(capacity_scans) <= 1, 'Admission must not recount capacity for each blocked leaf'


async def test_claim_adds_fair_charge_to_concurrently_advanced_queue_floor(store):
    receipt = await enqueue(store, weight=4)
    async with store.factory() as db:
        row = await db.get(models.ProviderWork, receipt.work_id)
        keys = (row.tenant_queue_key, row.policy_queue_key, row.resource_queue_key)
    updating = asyncio.Event()

    def record(conn, cursor, statement, parameters, context, executemany):
        sql = statement.lower()
        if (sql.startswith('update provider_work_queues') or
                ('from provider_work_queues' in sql and 'for update' in sql)):
            updating.set()

    task = None
    try:
        async with store.factory.begin() as floor:
            await floor.execute(update(models.ProviderWorkQueue).where(models.ProviderWorkQueue.key.in_(keys))
                .values(virtual_finish=100))
            event.listen(store.engine.sync_engine, 'before_cursor_execute', record)
            task = asyncio.create_task(claim(store))
            await asyncio.wait_for(updating.wait(), 2)
        assert (await asyncio.wait_for(task, 2)).work_id == receipt.work_id
    finally:
        event.remove(store.engine.sync_engine, 'before_cursor_execute', record)
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    async with store.factory() as db:
        finishes = {row.key: row.virtual_finish for row in (await db.scalars(
            select(models.ProviderWorkQueue).where(models.ProviderWorkQueue.key.in_(keys)))).all()}
    assert [finishes[key] for key in keys] == [101, 100.25, 101]


async def test_twenty_claimers_obey_total_and_tenant_capacity(store):
    second_tenant, second_resource = uuid4(), uuid4()
    async with store.factory.begin() as db:
        tenant = await db.get(models.Tenant, store.tenant)
        db.add(models.Tenant(id=second_tenant, org_id=tenant.org_id, display_name='Other tenant'))
        await db.flush()
        db.add(models.Resource(id=second_resource, tenant_id=second_tenant,
            type=models.ResourceType.USER_MAIL, external_id='other-mail', display_name='Other mail'))
    for index in range(12):
        scope = await store.new_scope()
        async with store.factory.begin() as db:
            snapshot = await db.get(models.Snapshot, scope.snapshot_id)
            if index % 2:
                snapshot.resource_id = second_resource
                job = await db.get(models.Job, snapshot.job_id)
                job.tenant_id, job.resource_id = second_tenant, second_resource
            await work.ensure_work(db, snapshot_id=snapshot.id, snapshot_token=0, job_token=0)
    async with store.factory.begin() as db:
        control = await db.get(models.ProviderWorkControl, 'global')
        control.total_capacity, control.tenant_capacity = 5, 3

    async def contender():
        for _ in range(30):
            owner = await claim(store)
            if owner is not None:
                return owner
            await asyncio.sleep(.002)

    owners = [owner for owner in await asyncio.gather(*(contender() for _ in range(20))) if owner]
    assert len({owner.work_id for owner in owners}) == len(owners) == 5
    async with store.factory() as db:
        tenants = list((await db.scalars(select(models.ProviderWork.tenant_id)
            .where(models.ProviderWork.state == 'RESUMING'))).all())
    assert sorted(Counter(tenants).values()) == [2, 3]


async def test_continuous_backlog_preserves_four_positive_policy_weights(store):
    receipts = {weight: await enqueue(store, weight=weight) for weight in (8, 4, 2, 1)}
    served = []
    for _ in range(150):
        owner = await claim(store)
        served.append(owner.work_id)
        async with store.factory.begin() as db:
            await work.suspend_current_work(db, owner, next_eligible_at=datetime.now(timezone.utc),
                reason='checkpoint')
    counts = Counter(served)
    assert [counts[receipts[weight].work_id] for weight in (8, 4, 2, 1)] == [80, 40, 20, 10]
    assert all(receipts[1].work_id in served[start:start + 16] for start in range(135))
