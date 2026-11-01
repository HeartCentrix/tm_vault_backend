"""Copy admission uses server-held guards and transactional checkpoints."""
import asyncio
from datetime import datetime, timedelta, timezone
import importlib
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import func, select, text, update

from test_chat_catalogue_postgres import store
from shared import copy_executor, copy_work, models

pytestmark = pytest.mark.integration


@pytest_asyncio.fixture(autouse=True)
async def installed_copy_migration(store):
    from pathlib import Path
    from alembic.migration import MigrationContext
    from alembic.operations import Operations
    spec=importlib.util.spec_from_file_location('copy_admission_migration',Path(__file__).resolve().parents[2]/'alembic/versions/0012_copy_work.py')
    migration=importlib.util.module_from_spec(spec);spec.loader.exec_module(migration)
    async with store.engine.begin() as connection:
        def upgrade(sync):
            with Operations.context(MigrationContext.configure(sync)): migration.upgrade()
        await connection.run_sync(upgrade)


def api():
    try:
        return importlib.import_module('shared.copy_admission')
    except ModuleNotFoundError:
        pytest.fail('transactional copy admission is not implemented')


async def queued(store, *, rows=2, lane='giant', capacity=2, partition=False, resource_id=None):
    resource_id = resource_id or store.resource
    source = await store.new_scope()
    async with store.factory.begin() as db:
        snap = await db.get(models.Snapshot, source.snapshot_id)
        snap.resource_id = resource_id
        (await db.get(models.Job, snap.job_id)).resource_id = resource_id
        snap.status = models.SnapshotStatus.COMPLETED
        snap.created_at = datetime.utcnow() - timedelta(days=1)
        for index in range(rows):
            db.add(models.SnapshotItem(snapshot_id=snap.id, tenant_id=store.tenant,
                item_type='TEAMS_CHAT_MESSAGE', external_id=f'm{index}', name='message',
                content_size=7, backend_id=store.backend, extra_data={}))
    target = await store.new_scope()
    async with store.factory.begin() as db:
        target_snapshot = await db.get(models.Snapshot, target.snapshot_id)
        target_snapshot.resource_id = resource_id
        (await db.get(models.Job, target_snapshot.job_id)).resource_id = resource_id
        control = await db.get(models.CopyWorkControl, 'global')
        if control is None:
            control = models.CopyWorkControl(id='global')
            db.add(control)
        control.enabled = True
        control.micro_capacity = control.giant_capacity = capacity
        control.total_capacity = control.tenant_capacity = capacity
        pid = None
        if partition:
            pid = uuid4()
            db.add(models.SnapshotPartition(id=pid, snapshot_id=target.snapshot_id,
                tenant_id=store.tenant, resource_id=resource_id,
                job_id=(await db.get(models.Snapshot, target.snapshot_id)).job_id,
                partition_type='CHATS', partition_index=0, status='IN_PROGRESS',
                lease_token=0, payload={}))
            await db.flush()
        task = await copy_work.enqueue(db, copy_work.CopyRequest(kind='CHAT_CARRY', lane=lane,
            target_snapshot_id=target.snapshot_id, partition_id=pid))
        workflow = await db.get(models.CopyWorkflow, task.workflow_id)
        workflow.state = 'WAITING'
        workflow.generation = 1
        leaf = await db.get(models.SnapshotPartition if pid else models.Snapshot, pid or target.snapshot_id)
        leaf.lease_token += 1
        leaf.lease_owner_id = leaf.lease_expires_at = None
        workflow.parent_lease_token = leaf.lease_token
        if not pid:
            workflow.snapshot_lease_token = leaf.lease_token
    async with store.factory.begin() as db:
        claim = await copy_work.claim(db, owner_id=uuid4())
        assert claim is not None
    return target, task.id, claim


async def test_batch_and_checkpoint_commit_together_then_reenter_queue(store):
    target, task_id, claim = await queued(store)
    first = await api().run_batch(store.factory, claim, batch_size=1)
    assert (first.status, first.rows, first.bytes) == ('READY', 1, 7)
    async with store.factory.begin() as db:
        task = await db.get(models.CopyWorkTask, task_id)
        assert (task.checkpoint, task.row_count, task.bytes_count) == (1, 1, 7)
        assert task.lease_owner_id is None
        assert (await db.get(models.Snapshot, target.snapshot_id)).status == models.SnapshotStatus.IN_PROGRESS
        next_claim = await copy_work.claim(db, owner_id=uuid4())
    second = await api().run_batch(store.factory, next_claim, batch_size=1)
    assert (second.status, second.rows, second.bytes) == ('COMPLETED', 1, 7)
    async with store.factory() as db:
        task = await db.get(models.CopyWorkTask, task_id)
        assert (task.checkpoint, task.row_count, task.bytes_count) == (2, 2, 14)
        assert await db.scalar(select(func.count()).select_from(models.CopyWorkItem).where(models.CopyWorkItem.task_id == task_id,models.CopyWorkItem.ordinal>0)) == 2
        assert (await db.get(models.CopyWorkflow, task.workflow_id)).state == 'WAITING'


@pytest.mark.parametrize(('lane', 'first_slot'), [('micro', 0), ('giant', 1000000)])
async def test_held_first_lane_permit_uses_idle_alternative(store, lane, first_slot):
    _, _, claim = await queued(store, lane=lane)
    async with store.factory.begin() as holder:
        assert await holder.scalar(text('SELECT pg_try_advisory_xact_lock(420018,:slot)'), {'slot': first_slot})
        result = await api().run_batch(store.factory, claim)
        assert result.status == 'COMPLETED'


async def test_capacity_miss_releases_all_guards_and_does_not_spend_retry_budget(store):
    _, task_id, claim = await queued(store, capacity=1)
    async with store.factory.begin() as db:
        task = await db.get(models.CopyWorkTask, task_id)
        created = task.created_at
        task.cumulative_wait_s = 37
    async with store.factory.begin() as holder:
        assert await holder.scalar(text('SELECT pg_try_advisory_xact_lock(420018,1000000)'))
        result = await api().run_batch(store.factory, claim)
        assert result.status == 'DEFERRED'
        # Only this independent holder's lane lock remains. Partial target,
        # resource, tenant and aggregate guards must have been rolled back.
        locks = (await holder.execute(text("SELECT classid::bigint,count(*) FROM pg_locks "
            "WHERE locktype='advisory' AND classid BETWEEN 420018 AND 420022 GROUP BY classid"))).all()
        assert locks == [(420018, 1)]
    async with store.factory() as db:
        task = await db.get(models.CopyWorkTask, task_id)
        assert task.state == 'READY' and task.attempts == 0
        assert task.created_at == created and task.cumulative_wait_s >= 37
        assert task.lease_owner_id is task.lease_expires_at is None
        assert not task.prepared


async def test_disabled_protocol_never_executes_copy(store):
    target, task_id, claim = await queued(store)
    async with store.factory.begin() as db:
        (await db.get(models.CopyWorkControl, 'global')).enabled = False
    result = await api().run_batch(store.factory, claim)
    assert result.status == 'DEFERRED'
    async with store.factory() as db:
        assert not (await db.get(models.CopyWorkTask, task_id)).prepared
        assert await db.scalar(select(func.count()).select_from(models.SnapshotItem)
                               .where(models.SnapshotItem.snapshot_id == target.snapshot_id)) == 0


async def test_replaced_leaf_generation_cannot_write_or_clear_new_owner(store):
    _, task_id, claim = await queued(store, partition=True)
    async with store.factory.begin() as db:
        task = await db.get(models.CopyWorkTask, task_id)
        wf = await db.get(models.CopyWorkflow, task.workflow_id)
        leaf = await db.get(models.SnapshotPartition, wf.partition_id)
        leaf.lease_token += 1
    assert (await api().run_batch(store.factory, claim)).status == 'STALE'
    async with store.factory() as db:
        task = await db.get(models.CopyWorkTask, task_id)
        assert not task.prepared and task.lease_owner_id == claim.owner_id


async def test_parent_cancel_settles_only_matching_claim_without_copy(store):
    target, task_id, claim = await queued(store)
    async with store.factory.begin() as db:
        job = await db.get(models.Job, (await db.get(models.Snapshot, target.snapshot_id)).job_id)
        job.status = models.JobStatus.CANCELLED
    assert (await api().run_batch(store.factory, claim)).status == 'CANCELLED'
    async with store.factory() as db:
        task = await db.get(models.CopyWorkTask, task_id)
        assert task.state == 'CANCELLED' and not task.prepared
        assert task.error == {'code': 'copy_parent_cancelled'}


async def test_statement_timeout_rolls_back_manifests_and_has_bounded_typed_failure(store, monkeypatch):
    target, task_id, claim = await queued(store)
    calls = 0
    async def timeout(db, task, **kwargs):
        nonlocal calls
        calls += 1
        await db.execute(text('SELECT pg_sleep(0.2)'))
        raise AssertionError('statement timeout did not execute')
    monkeypatch.setattr(copy_executor, 'copy_manifest_batch', timeout)
    result = await api().run_batch(store.factory, claim, statement_timeout_ms=20)
    assert result.status == 'FAILED' and calls == 3
    async with store.factory() as db:
        task = await db.get(models.CopyWorkTask, task_id)
        assert task.checkpoint == task.row_count == 0 and not task.prepared
        assert task.error == {'code': 'copy_statement_timeout', 'sqlstate': '57014', 'attempts': 3}
        assert await db.scalar(select(func.count()).select_from(models.SnapshotItem)
            .where(models.SnapshotItem.snapshot_id == target.snapshot_id)) == 0


async def test_server_transaction_outlives_task_lease_without_reclaim_or_extra_capacity(store, monkeypatch):
    _, task_id, claim = await queued(store, capacity=1)
    async with store.factory.begin() as db:
        (await db.get(models.CopyWorkTask, task_id)).lease_expires_at = datetime.now(timezone.utc) + timedelta(milliseconds=100)
    started = asyncio.Event()
    original = copy_executor.copy_manifest_batch
    async def slow(db, task, **kwargs):
        started.set()
        await db.execute(text('SELECT pg_sleep(0.35)'))
        return await original(db, task, **kwargs)
    monkeypatch.setattr(copy_executor, 'copy_manifest_batch', slow)
    execution = asyncio.create_task(api().run_batch(store.factory, claim))
    try:
        await asyncio.wait_for(started.wait(), timeout=2)
        async with store.factory.begin() as db:
            await db.execute(text('SELECT pg_sleep(0.15)'))
            assert await copy_work.claim(db, owner_id=uuid4()) is None
            assert not await db.scalar(text('SELECT pg_try_advisory_xact_lock(420018,1000000)'))
            assert not await db.scalar(text('SELECT pg_try_advisory_xact_lock(420020,0)'))
        assert (await execution).status == 'COMPLETED'
    finally:
        if not execution.done():
            execution.cancel()
        await asyncio.gather(execution, return_exceptions=True)


async def test_activation_rejects_live_legacy_sql_then_persists_epoch(store):
    async with store.factory.begin() as holder:
        assert await holder.scalar(text('SELECT pg_try_advisory_xact_lock(420018,1000000)'))
        with pytest.raises(RuntimeError, match='legacy'):
            await api().enable_copy_protocol(store.factory, micro_capacity=1, giant_capacity=1,
                                              total_capacity=2, tenant_capacity=1)
    epoch = await api().enable_copy_protocol(store.factory, micro_capacity=1, giant_capacity=1,
                                            total_capacity=2, tenant_capacity=1)
    assert epoch == 1
    async with store.factory() as db:
        control = await db.get(models.CopyWorkControl, 'global')
        assert control.enabled and control.epoch == 1
        assert (control.micro_capacity, control.giant_capacity, control.total_capacity, control.tenant_capacity) == (1, 1, 2, 1)


async def test_activation_refuses_pending_durable_work_or_oversized_legacy_capacity(store):
    await queued(store)
    with pytest.raises(RuntimeError, match='active'):
        await api().enable_copy_protocol(store.factory, micro_capacity=1, giant_capacity=1,
                                          total_capacity=1, tenant_capacity=1)
    with pytest.raises(ValueError, match='capacity'):
        await api().enable_copy_protocol(store.factory, micro_capacity=1000001, giant_capacity=1,
                                          total_capacity=1, tenant_capacity=1)


async def test_aggregate_budget_applies_across_independent_lane_permits(store):
    _, task_id, claim = await queued(store)
    async with store.factory.begin() as db:
        (await db.get(models.CopyWorkControl, 'global')).total_capacity = 1
    async with store.factory.begin() as holder:
        assert await holder.scalar(text('SELECT pg_try_advisory_xact_lock(420020,0)'))
        assert (await api().run_batch(store.factory, claim)).status == 'DEFERRED'
    async with store.factory() as db:
        assert (await db.get(models.CopyWorkTask, task_id)).error == {'code': 'copy_aggregate_busy'}


@pytest.mark.parametrize('same_resource', [True, False])
async def test_resource_exclusion_and_per_tenant_capacity_are_server_held(store, monkeypatch, same_resource):
    _, _, first_claim = await queued(store)
    resource = store.resource if same_resource else uuid4()
    if not same_resource:
        async with store.factory.begin() as db:
            db.add(models.Resource(id=resource, tenant_id=store.tenant, type=models.ResourceType.USER_CHATS,
                external_id=str(resource), display_name='Another target'))
    _, second_id, second_claim = await queued(store, resource_id=resource)
    async with store.factory.begin() as db:
        (await db.get(models.CopyWorkControl, 'global')).tenant_capacity = 1
    started = asyncio.Event()
    original = copy_executor.copy_manifest_batch
    async def slow(db, task, **kwargs):
        started.set()
        await db.execute(text('SELECT pg_sleep(0.35)'))
        return await original(db, task, **kwargs)
    monkeypatch.setattr(copy_executor, 'copy_manifest_batch', slow)
    execution = asyncio.create_task(api().run_batch(store.factory, first_claim))
    try:
        await asyncio.wait_for(started.wait(), timeout=2)
        assert (await api().run_batch(store.factory, second_claim)).status == 'DEFERRED'
        async with store.factory() as db:
            assert (await db.get(models.CopyWorkTask, second_id)).error == {
                'code': 'copy_resource_busy' if same_resource else 'copy_tenant_busy'}
        assert (await execution).status == 'COMPLETED'
    finally:
        if not execution.done():
            execution.cancel()
        await asyncio.gather(execution, return_exceptions=True)


async def test_deadlock_retry_rolls_back_copied_rows_before_fresh_transaction(store, monkeypatch):
    target, task_id, claim = await queued(store)
    original, calls = copy_executor.copy_manifest_batch, 0
    async def transient(db, task, **kwargs):
        nonlocal calls
        calls += 1
        result = await original(db, task, **kwargs)
        if calls < 3:
            await db.execute(text("DO $$ BEGIN RAISE EXCEPTION USING ERRCODE='40P01'; END $$"))
        return result
    monkeypatch.setattr(copy_executor, 'copy_manifest_batch', transient)
    result = await api().run_batch(store.factory, claim)
    assert result.status == 'COMPLETED' and result.rows == 2 and calls == 3
    async with store.factory() as db:
        assert (await db.get(models.CopyWorkTask, task_id)).row_count == 2
        assert await db.scalar(select(func.count()).select_from(models.SnapshotItem)
            .where(models.SnapshotItem.snapshot_id == target.snapshot_id)) == 2


async def test_unknown_error_stores_typed_code_without_provider_urls(store, monkeypatch):
    _, task_id, claim = await queued(store)
    async def failure(db, task, **kwargs):
        raise RuntimeError('https://provider.invalid/path?signed_token=do-not-persist')
    monkeypatch.setattr(copy_executor, 'copy_manifest_batch', failure)
    assert (await api().run_batch(store.factory, claim)).status == 'FAILED'
    async with store.factory() as db:
        assert (await db.get(models.CopyWorkTask, task_id)).error == {'code': 'copy_execution_failed'}


async def test_old_claim_cannot_clear_replacement_owner(store):
    _, task_id, claim = await queued(store)
    replacement = uuid4()
    async with store.factory.begin() as db:
        task = await db.get(models.CopyWorkTask, task_id)
        task.lease_owner_id, task.generation = replacement, claim.generation + 1
    assert (await api().run_batch(store.factory, claim)).status == 'STALE'
    async with store.factory() as db:
        task = await db.get(models.CopyWorkTask, task_id)
        assert task.lease_owner_id == replacement and not task.prepared


async def test_sql_execution_time_is_durable_and_separate_from_capacity_wait(store,monkeypatch):
    from shared.copy_observability import copy_work_summary
    import shared.copy_workflow as workflow_module
    monkeypatch.setattr(workflow_module,'_COPY_GUARDS_ACTIVE',True)
    target,task_id,claim=await queued(store)
    async with store.factory.begin() as db:
        task=await db.get(models.CopyWorkTask,task_id);task.cumulative_wait_s=37
    result=await api().run_batch(store.factory,claim)
    assert result.status=='COMPLETED'
    async with store.factory() as db:
        task=await db.get(models.CopyWorkTask,task_id)
        assert task.execution_s>0
        summary=await copy_work_summary(db,job_id=(await db.get(models.Snapshot,target.snapshot_id)).job_id)
        assert summary['admission_wait_s']>=37
        assert 0<summary['sql_runtime_s']<37


async def test_protocol_activation_refuses_models_only_schema_without_delete_guard(store):
    async with store.engine.begin() as conn:
        await conn.execute(text('DROP TRIGGER IF EXISTS copy_workflow_delete_guard ON copy_workflows'))
    with pytest.raises(RuntimeError,match='migration|schema|guard'):
        await api().enable_copy_protocol(store.factory,micro_capacity=1,giant_capacity=1,total_capacity=1,tenant_capacity=1)


async def test_repeated_sql_lock_failures_are_bounded_without_spending_capacity_wait_budget(store,monkeypatch):
    _,task_id,current=await queued(store)
    async def locked(session,task):
        await session.execute(text("DO $$ BEGIN RAISE EXCEPTION 'busy' USING ERRCODE='55P03'; END $$"))
    monkeypatch.setattr(copy_executor,'prepare_manifest',locked)
    states=[]
    for attempt in range(3):
        result=await api().run_batch(store.factory,current);states.append(result.status)
        if attempt<2:
            async with store.factory.begin() as db:
                task=await db.get(models.CopyWorkTask,task_id);task.ready_at=await db.scalar(select(func.clock_timestamp()))
            async with store.factory.begin() as db:
                current=await copy_work.claim(db,owner_id=uuid4())
    assert states==['DEFERRED','DEFERRED','FAILED']
