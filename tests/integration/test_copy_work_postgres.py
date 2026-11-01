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
    names=['copy_work_inputs','copy_work_items','copy_work_tasks','copy_work_queues','copy_workflows','copy_work_control']
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


async def test_explicit_empty_predecessor_is_pinned_before_manifest_preparation(store):
    from sqlalchemy import delete
    from sqlalchemy.exc import IntegrityError
    prior=await store.new_scope()
    async with store.factory.begin() as db:
        await db.execute(update(models.Snapshot).where(models.Snapshot.id==prior.snapshot_id).values(status=models.SnapshotStatus.COMPLETED))
    target=await store.new_scope()
    async with store.factory.begin() as db:
        await api().enqueue(db,api().CopyRequest(kind='FILE_VERSIONS',lane='micro',target_snapshot_id=target.snapshot_id,
            params={'prior_snap_id':str(prior.snapshot_id),'file_ids':[]}))
    with pytest.raises(IntegrityError):
        async with store.factory.begin() as db:
            await db.execute(delete(models.Snapshot).where(models.Snapshot.id==prior.snapshot_id))


async def test_rename_waits_until_later_collected_insert_tasks_settle(store):
    scope=await store.new_scope()
    async with store.factory.begin() as db:
        rename=await api().enqueue(db,api().CopyRequest(kind='CHAT_RENAME',lane='giant',target_snapshot_id=scope.snapshot_id,params={'names':['A'],'chat_ids':['chat']}))
        carry=await api().enqueue(db,api().CopyRequest(kind='CHAT_CARRY',lane='giant',target_snapshot_id=scope.snapshot_id,params={}))
        await db.execute(update(models.CopyWorkTask).where(models.CopyWorkTask.id==rename.id)
            .values(created_at=datetime.now(timezone.utc)-timedelta(minutes=1)))
        await db.execute(update(models.CopyWorkflow).where(models.CopyWorkflow.id==carry.workflow_id).values(state='WAITING'))
    async with store.factory.begin() as db:
        first=await api().claim(db,owner_id=uuid4());assert first.task_id==carry.id
        await db.execute(update(models.CopyWorkTask).where(models.CopyWorkTask.id==carry.id).values(state='COMPLETED'))
    async with store.factory.begin() as db:
        second=await api().claim(db,owner_id=uuid4());assert second.task_id==rename.id


async def test_terminal_workflow_history_does_not_block_snapshot_retention(store):
    from sqlalchemy import delete
    task=await request(store)
    async with store.factory.begin() as db:
        await db.execute(update(models.CopyWorkTask).where(models.CopyWorkTask.id==task.id).values(state='COMPLETED'))
        await db.execute(update(models.CopyWorkflow).where(models.CopyWorkflow.id==task.workflow_id).values(state='COMPLETED'))
    async with store.factory.begin() as db:
        deleted=await db.execute(delete(models.Snapshot).where(models.Snapshot.id==task.params['target_snapshot_id']))
        assert deleted.rowcount==1


async def test_large_copy_selections_are_durable_rows_not_unbounded_task_json(store):
    from shared.copy_executor import prepare_manifest,copy_manifest_batch
    prior=await store.new_scope()
    ids=['file-'+str(i) for i in range(257)]
    async with store.factory.begin() as db:
        await db.execute(update(models.Snapshot).where(models.Snapshot.id==prior.snapshot_id).values(status=models.SnapshotStatus.COMPLETED))
        db.add(models.SnapshotItem(snapshot_id=prior.snapshot_id,tenant_id=store.tenant,backend_id=store.backend,
            item_type='FILE_VERSION',external_id='v1',name='v1',parent_external_id=ids[-1],content_size=3))
    target=await store.new_scope()
    async with store.factory.begin() as db:
        task=await api().enqueue(db,api().CopyRequest(kind='FILE_VERSIONS',lane='micro',target_snapshot_id=target.snapshot_id,
            params={'prior_snap_id':str(prior.snapshot_id),'file_ids':ids}))
        assert task.params['file_ids']==[]
        assert len(list((await db.scalars(select(models.CopyWorkInput).where(models.CopyWorkInput.task_id==task.id))).all()))==257
    async with store.factory.begin() as db:
        task=await db.get(models.CopyWorkTask,task.id)
        await prepare_manifest(db,task)
        result=await copy_manifest_batch(db,task,limit=10)
        assert result['rows']==1


async def test_new_leaf_generation_reopens_abandoned_capture_without_old_task_replay(store):
    task=await request(store)
    async with store.factory.begin() as db:
        workflow=await db.get(models.CopyWorkflow,task.workflow_id)
        workflow.state='FAILED'
        taskrow=await db.get(models.CopyWorkTask,task.id);taskrow.state='CANCELLED'
        snapshot=await db.get(models.Snapshot,workflow.target_snapshot_id);snapshot.lease_token+=1
    async with store.factory.begin() as db:
        retry=await api().enqueue(db,api().CopyRequest(kind=task.kind,lane=task.lane,
            target_snapshot_id=task.params['target_snapshot_id'],params=task.params))
        assert retry.id!=task.id
        assert retry.capture_attempt==1
        workflow=await db.get(models.CopyWorkflow,retry.workflow_id);workflow.state='WAITING'
    async with store.factory.begin() as db:
        chosen=await api().claim(db,owner_id=uuid4())
        assert chosen.task_id==retry.id
