"""Durable copy ownership and all-policy fairness on real PostgreSQL."""
import importlib
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from sqlalchemy import select, update

from test_chat_catalogue_postgres import store
from shared import models

pytestmark = pytest.mark.integration


def api():
    return importlib.import_module('shared.copy_work')


async def request(store, *, policy=None, weight=1, key='chat-alpha'):
    scope = await store.new_scope()
    async with store.factory.begin() as db:
        return await api().enqueue(db, api().CopyRequest(
            kind='CHAT_CARRY', lane='giant', target_snapshot_id=scope.snapshot_id,
            params={'tenant_id': str(store.tenant), 'resource_id': str(store.resource),
                    'target_snapshot_id': str(scope.snapshot_id), 'test_key': key},
            sla_policy_id=policy, weight=weight))


async def test_enqueue_is_idempotent_and_waiting_work_is_not_claimed_before_seal(store):
    first = await request(store)
    async with store.factory.begin() as db:
        again = await api().enqueue(db, api().CopyRequest(kind=first.kind, lane=first.lane,
            target_snapshot_id=first.params['target_snapshot_id'], params=first.params))
        assert again.id == first.id
        assert await api().claim(db, owner_id=uuid4()) is None
        workflow = await db.get(models.CopyWorkflow, first.workflow_id)
        assert workflow.state == 'DRAINING'
        workflow.state = 'WAITING'
    async with store.factory.begin() as db:
        claimed = await api().claim(db, owner_id=uuid4())
        assert claimed.task_id == first.id and claimed.generation == 1


async def test_claim_generation_prevents_duplicate_ownership_and_reclaims_only_expired_work(store):
    task = await request(store)
    async with store.factory.begin() as db:
        await db.execute(update(models.CopyWorkflow).where(models.CopyWorkflow.id == task.workflow_id).values(state='WAITING'))
    async with store.factory.begin() as db:
        first = await api().claim(db, owner_id=uuid4())
    async with store.factory.begin() as db:
        assert await api().claim(db, owner_id=uuid4()) is None
        await db.execute(update(models.CopyWorkTask).where(models.CopyWorkTask.id == task.id)
            .values(lease_expires_at=datetime.now(timezone.utc) - timedelta(seconds=1)))
    async with store.factory.begin() as db:
        second = await api().claim(db, owner_id=uuid4())
        assert second.task_id == first.task_id and second.generation == first.generation + 1


async def test_every_policy_id_including_bronze_and_custom_gets_shared_fair_turns(store):
    policies = [uuid4() for _ in range(5)]
    async with store.factory.begin() as db:
        for pid, name in zip(policies, ['Gold','Silver','Bronze','Customer policy','Renamed custom']):
            db.add(models.SlaPolicy(id=pid,tenant_id=store.tenant,name=name))
    tasks=[]
    for pid, weight in zip(policies,[4,2,1,3,1]):
        tasks.append(await request(store,policy=pid,weight=weight,key=str(pid)))
    async with store.factory.begin() as db:
        await db.execute(update(models.CopyWorkflow).values(state='WAITING'))
    observed={pid:0 for pid in policies}
    for _ in range(44):
        async with store.factory.begin() as db:
            claimed=await api().claim(db,owner_id=uuid4())
            assert claimed is not None
            task=await db.get(models.CopyWorkTask,claimed.task_id)
            observed[task.sla_policy_id]+=1
            task.state='READY';task.lease_owner_id=None;task.lease_expires_at=None
    assert all(value>0 for value in observed.values()), observed
    assert observed[policies[0]]>observed[policies[2]]


async def test_cancelled_parent_cannot_supply_new_copy_work(store):
    scope=await store.new_scope()
    async with store.factory.begin() as db:
        job=await db.get(models.Job,scope.job_id) if hasattr(scope,'job_id') else await db.get(models.Job,(await db.get(models.Snapshot,scope.snapshot_id)).job_id)
        job.status=models.JobStatus.CANCELLED
    async with store.factory.begin() as db:
        with pytest.raises(api().CopyWorkConflict):
            await api().enqueue(db,api().CopyRequest(kind='CHAT_CARRY',lane='giant',
                target_snapshot_id=scope.snapshot_id,params={}))


@pytest.mark.parametrize('weight',[0,-1,9,True])
async def test_invalid_scheduling_weights_are_refused_before_persistence(store,weight):
    with pytest.raises(ValueError):
        await request(store,weight=weight)


async def test_additive_migration_creates_real_queue_tables_and_refuses_active_downgrade(store):
    from pathlib import Path
    from alembic.migration import MigrationContext
    from alembic.operations import Operations
    spec=importlib.util.spec_from_file_location('copy_work_migration',Path(__file__).resolve().parents[2]/'alembic/versions/0012_copy_work.py')
    migration=importlib.util.module_from_spec(spec);spec.loader.exec_module(migration)
    names=['copy_work_items','copy_work_tasks','copy_work_queues','copy_workflows','copy_work_control']
    async with store.engine.begin() as conn:
        from sqlalchemy import text
        for name in names: await conn.execute(text('DROP TABLE '+name+' CASCADE'))
        def upgrade(sync):
            with Operations.context(MigrationContext.configure(sync)): migration.upgrade()
        await conn.run_sync(upgrade)
    task=await request(store)
    async with store.engine.begin() as conn:
        def downgrade(sync):
            with Operations.context(MigrationContext.configure(sync)): migration.downgrade()
        with pytest.raises(RuntimeError,match='active copy'):
            await conn.run_sync(downgrade)
