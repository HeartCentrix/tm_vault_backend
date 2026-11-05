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
    gate = task_id.int % (2 ** 31)
    async with store.engine.connect() as blocker:
        await blocker.execute(text('SELECT pg_advisory_xact_lock(433010,:gate)'), {'gate':gate})
        async def slow(db, task, **kwargs):
            started.set()
            # Hold an actual server statement until the observer releases it.
            # A fixed sleep races a busy test process and can finish too early.
            await db.execute(text('SELECT pg_advisory_xact_lock(433010,:gate)'), {'gate':gate})
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
            await blocker.rollback()
            assert (await execution).status == 'COMPLETED'
        finally:
            await blocker.rollback()
            if not execution.done(): execution.cancel()
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


@pytest.mark.parametrize(('sqlstate', 'code'), [
    ('40P01', 'copy_deadlock'), ('40001', 'copy_serialization_failure'),
])
async def test_transaction_abort_requeues_once_and_persists_budget_across_restart_and_capacity(
        store, monkeypatch, sqlstate, code):
    from sqlalchemy.ext.asyncio import async_sessionmaker
    target, task_id, current = await queued(store, capacity=1)
    original, calls, transactions = copy_executor.copy_manifest_batch, 0, []
    async def abort_after_copy(db, task, **kwargs):
        nonlocal calls
        calls += 1
        transactions.append(await db.scalar(text('SELECT txid_current()')))
        await original(db, task, **kwargs)
        await db.execute(text(f"DO $$ BEGIN RAISE EXCEPTION USING ERRCODE='{sqlstate}'; END $$"))
    monkeypatch.setattr(copy_executor, 'copy_manifest_batch', abort_after_copy)

    async def reclaim():
        async with store.factory.begin() as db:
            task = await db.get(models.CopyWorkTask, task_id)
            task.ready_at = await db.scalar(select(func.clock_timestamp()))
        async with store.factory.begin() as db:
            result = await copy_work.claim(db, owner_id=uuid4())
            assert result is not None
            return result

    for failures in range(1, 4):
        # A recreated factory has no in-memory retry history, as after restart.
        restarted = async_sessionmaker(store.engine, expire_on_commit=False)
        result = await api().run_batch(restarted, current)
        assert calls == failures
        assert result.status == ('FAILED' if failures == 3 else 'DEFERRED')
        async with store.factory.begin() as db:
            task = await db.get(models.CopyWorkTask, task_id)
            assert task.error == {'code': code, 'sqlstate': sqlstate, 'execution_failures': failures}
            assert task.state == ('FAILED' if failures == 3 else 'READY')
            assert task.checkpoint == task.row_count == 0 and not task.prepared
            assert task.lease_owner_id is task.lease_expires_at is None
            assert await db.scalar(select(func.count()).select_from(models.SnapshotItem)
                .where(models.SnapshotItem.snapshot_id == target.snapshot_id)) == 0
            # Both physical permits are free while ready_at still delays retry.
            assert await db.scalar(text('SELECT pg_try_advisory_xact_lock(420018,1000000)'))
            assert await db.scalar(text('SELECT pg_try_advisory_xact_lock(420020,0)'))
            if failures < 3:
                assert task.ready_at > await db.scalar(select(func.clock_timestamp()))
        if failures == 1:
            current = await reclaim()
            async with store.factory.begin() as holder:
                assert await holder.scalar(text('SELECT pg_try_advisory_xact_lock(420018,1000000)'))
                assert (await api().run_batch(restarted, current)).status == 'DEFERRED'
            async with store.factory() as db:
                task = await db.get(models.CopyWorkTask, task_id)
                assert task.error == {'code': 'copy_lane_busy', 'execution_failures': 1}
                assert task.attempts == 1
            assert calls == 1
        if failures < 3:
            current = await reclaim()
    assert len(set(transactions)) == 3


async def test_successful_checkpoint_clears_execution_failure_budget(store, monkeypatch):
    _, task_id, current = await queued(store, rows=3)
    original, fail_next = copy_executor.copy_manifest_batch, True
    async def sometimes_abort(db, task, **kwargs):
        nonlocal fail_next
        result = await original(db, task, **kwargs)
        if fail_next:
            fail_next = False
            await db.execute(text("DO $$ BEGIN RAISE EXCEPTION USING ERRCODE='40001'; END $$"))
        return result
    monkeypatch.setattr(copy_executor, 'copy_manifest_batch', sometimes_abort)
    assert (await api().run_batch(store.factory, current, batch_size=1)).status == 'DEFERRED'
    async with store.factory.begin() as db:
        task = await db.get(models.CopyWorkTask, task_id)
        task.ready_at = await db.scalar(select(func.clock_timestamp()))
    async with store.factory.begin() as db:
        current = await copy_work.claim(db, owner_id=uuid4())
    assert (await api().run_batch(store.factory, current, batch_size=1)).status == 'READY'
    async with store.factory.begin() as db:
        task = await db.get(models.CopyWorkTask, task_id)
        assert task.error is None and task.checkpoint == task.row_count == 1
        current = await copy_work.claim(db, owner_id=uuid4())
    fail_next = True
    assert (await api().run_batch(store.factory, current, batch_size=1)).status == 'DEFERRED'
    async with store.factory() as db:
        task = await db.get(models.CopyWorkTask, task_id)
        assert task.error['execution_failures'] == 1
        assert task.checkpoint == task.row_count == 1


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


async def test_settlement_contention_retries_without_replaying_copy_and_survives_restart(store, monkeypatch):
    from sqlalchemy.ext.asyncio import async_sessionmaker
    module = api()
    _, task_id, current = await queued(store, capacity=1)
    original_settle, original_sleep = module._settle, asyncio.sleep
    copy_calls, settlement_waits = 0, []
    holder_session = None
    async def abort(db, task, **kwargs):
        nonlocal copy_calls
        copy_calls += 1
        await db.execute(text("DO $$ BEGIN RAISE EXCEPTION USING ERRCODE='40001'; END $$"))
    async def contend(factory, claim, **kwargs):
        nonlocal holder_session
        async with factory.begin() as holder:
            holder_session = holder
            await holder.scalar(select(models.CopyWorkTask).where(
                models.CopyWorkTask.id == task_id).with_for_update())
            return await original_settle(factory, claim, **kwargs)
    async def release_after_failed_settlement(delay):
        settlement_waits.append(delay)
        await holder_session.rollback()
        async with store.factory.begin() as observer:
            assert await observer.scalar(text('SELECT pg_try_advisory_xact_lock(420018,1000000)'))
            assert await observer.scalar(text('SELECT pg_try_advisory_xact_lock(420020,0)'))
        await original_sleep(0)
    monkeypatch.setattr(copy_executor, 'copy_manifest_batch', abort)
    monkeypatch.setattr(module, '_settle', contend)
    monkeypatch.setattr(asyncio, 'sleep', release_after_failed_settlement)
    states = []
    for failures in range(1, 4):
        restarted = async_sessionmaker(store.engine, expire_on_commit=False)
        result = await module.run_batch(restarted, current)
        states.append(result.status)
        assert copy_calls == failures
        async with restarted.begin() as db:
            task = await db.get(models.CopyWorkTask, task_id)
            assert task.error == {'code': 'copy_serialization_failure', 'sqlstate': '40001',
                                  'execution_failures': failures}
            assert task.state == ('FAILED' if failures == 3 else 'READY')
            assert task.lease_owner_id is task.lease_expires_at is None
            if failures < 3:
                task.ready_at = await db.scalar(select(func.clock_timestamp()))
        if failures < 3:
            async with restarted.begin() as db:
                current = await copy_work.claim(db, owner_id=uuid4())
                assert current is not None
    assert states == ['DEFERRED', 'DEFERRED', 'FAILED']
    assert len(settlement_waits) == 3


@pytest.mark.parametrize('sqlstate', ['55P03', '57014', '40P01', '40001'])
async def test_settlement_retry_exhaustion_is_reported_without_false_deferral(store, monkeypatch, sqlstate):
    from sqlalchemy.exc import DBAPIError
    module = api()
    _, task_id, claim = await queued(store)
    original_settings = module._settings
    copy_calls, settlement_calls = 0, []
    async def abort(db, task, **kwargs):
        nonlocal copy_calls
        copy_calls += 1
        await db.execute(text("DO $$ BEGIN RAISE EXCEPTION USING ERRCODE='40001'; END $$"))
    async def reject_settlement(db, claimed, statement_timeout_ms, lock_timeout_ms):
        await original_settings(db, claimed, statement_timeout_ms, lock_timeout_ms)
        if statement_timeout_ms == module.SETTLE_STATEMENT_MS:
            settlement_calls.append(await db.scalar(text('SELECT txid_current()')))
            await db.execute(text(f"DO $$ BEGIN RAISE EXCEPTION USING ERRCODE='{sqlstate}'; END $$"))
    async def no_wait(delay):
        pass
    monkeypatch.setattr(copy_executor, 'copy_manifest_batch', abort)
    monkeypatch.setattr(module, '_settings', reject_settlement)
    monkeypatch.setattr(asyncio, 'sleep', no_wait)
    with pytest.raises(DBAPIError) as failed:
        await module.run_batch(store.factory, claim)
    assert failed.value.orig.sqlstate == sqlstate
    assert copy_calls == 1
    assert len(set(settlement_calls)) == 3
    async with store.factory() as db:
        task = await db.get(models.CopyWorkTask, task_id)
        assert task.state == 'RUNNING' and task.error is None


@pytest.mark.parametrize('recovery', ['expiry', 'shutdown'])
async def test_unsettled_aborts_keep_conservative_budget_across_reclaim_and_restart(store, monkeypatch, recovery):
    from sqlalchemy.exc import DBAPIError
    from sqlalchemy.ext.asyncio import async_sessionmaker
    module = api()
    _, task_id, current = await queued(store)
    original_settings = module._settings
    blocked, copy_calls = True, 0
    async def abort(db, task, **kwargs):
        nonlocal copy_calls
        copy_calls += 1
        await db.execute(text("DO $$ BEGIN RAISE EXCEPTION USING ERRCODE='40001'; END $$"))
    async def settlement_unavailable(db, claim, statement_timeout_ms, lock_timeout_ms):
        await original_settings(db, claim, statement_timeout_ms, lock_timeout_ms)
        if blocked and statement_timeout_ms == module.SETTLE_STATEMENT_MS:
            await db.execute(text("DO $$ BEGIN RAISE EXCEPTION USING ERRCODE='55P03'; END $$"))
    async def no_wait(delay):
        pass
    monkeypatch.setattr(copy_executor, 'copy_manifest_batch', abort)
    monkeypatch.setattr(module, '_settings', settlement_unavailable)
    monkeypatch.setattr(asyncio, 'sleep', no_wait)
    for executions in range(1, 4):
        restarted = async_sessionmaker(store.engine, expire_on_commit=False)
        with pytest.raises(DBAPIError):
            await module.run_batch(restarted, current)
        assert copy_calls == executions
        async with restarted.begin() as db:
            task = await db.get(models.CopyWorkTask, task_id)
            assert task.state == 'RUNNING' and task.error is None
            assert task.attempts == executions
            task.lease_expires_at = (await db.scalar(select(func.clock_timestamp()))) - timedelta(seconds=1)
        if recovery == 'shutdown':
            from shared.copy_runtime import CopyWorkRunner
            runner = CopyWorkRunner(restarted, None)
            await runner._release_cancelled_claim(current)
        async with restarted.begin() as db:
            current = await copy_work.claim(db, owner_id=uuid4())
            assert current is not None
    blocked = False
    assert (await module.run_batch(restarted, current)).status == 'FAILED'
    assert copy_calls == 3
    async with restarted() as db:
        task = await db.get(models.CopyWorkTask, task_id)
        # These expired claims could not record their outcomes. Do not claim
        # the recovery cap diagnosed a proven deadlock or capacity miss.
        assert task.error == {'code': 'copy_execution_unsettled', 'execution_failures': 3}
        assert task.state == 'FAILED' and task.lease_owner_id is task.lease_expires_at is None


async def test_normal_capacity_waits_never_spend_per_checkpoint_execution_reservation(store):
    _, task_id, current = await queued(store, capacity=1, rows=3)
    for _ in range(4):
        async with store.factory.begin() as holder:
            assert await holder.scalar(text('SELECT pg_try_advisory_xact_lock(420018,1000000)'))
            assert (await api().run_batch(store.factory, current)).status == 'DEFERRED'
        async with store.factory.begin() as db:
            task = await db.get(models.CopyWorkTask, task_id)
            assert task.attempts == 0 and 'execution_failures' not in task.error
            task.ready_at = await db.scalar(select(func.clock_timestamp()))
        async with store.factory.begin() as db:
            current = await copy_work.claim(db, owner_id=uuid4())
            assert current is not None
    for expected in ['READY', 'READY', 'COMPLETED']:
        assert (await api().run_batch(store.factory, current, batch_size=1)).status == expected
        async with store.factory.begin() as db:
            task = await db.get(models.CopyWorkTask, task_id)
            assert task.attempts == 0 and task.error is None
            if expected == 'READY':
                current = await copy_work.claim(db, owner_id=uuid4())
                assert current is not None


@pytest.mark.parametrize('known_failures', [0, 2])
async def test_legacy_cumulative_claims_are_normalized_without_losing_known_failure_budget(
        store, monkeypatch, known_failures):
    _, task_id, _ = await queued(store)
    async with store.factory.begin() as db:
        task = await db.get(models.CopyWorkTask, task_id)
        task.state, task.lease_owner_id, task.lease_expires_at = 'READY', None, None
        task.attempts = 100
        task.params = {key: value for key, value in task.params.items()
                       if key != '_copy_execution_budget_v1'}
        task.error = {'code': 'copy_lock_timeout', 'execution_failures': 2} if known_failures else None
        task.ready_at = await db.scalar(select(func.clock_timestamp()))
    async with store.factory.begin() as db:
        current = await copy_work.claim(db, owner_id=uuid4())
        task = await db.get(models.CopyWorkTask, task_id)
        assert task.attempts == known_failures + 1
        assert task.params['_copy_execution_budget_v1'] is True
    if known_failures:
        async def abort(db, task, **kwargs):
            await db.execute(text("DO $$ BEGIN RAISE EXCEPTION USING ERRCODE='40001'; END $$"))
        monkeypatch.setattr(copy_executor, 'copy_manifest_batch', abort)
    assert (await api().run_batch(store.factory, current)).status == ('FAILED' if known_failures else 'COMPLETED')
    async with store.factory() as db:
        task = await db.get(models.CopyWorkTask, task_id)
        assert task.params['_copy_execution_budget_v1'] is True
        if known_failures:
            assert task.error['execution_failures'] == 3
        else:
            assert task.attempts == 0


async def test_shutdown_release_keeps_legacy_unreserved_claim_behavior(store):
    from shared.copy_runtime import CopyWorkRunner
    _, task_id, current = await queued(store)
    async with store.factory.begin() as db:
        task = await db.get(models.CopyWorkTask, task_id)
        task.params = {key: value for key, value in task.params.items()
                       if key != '_copy_execution_budget_v1'}
        task.attempts = 10
    await CopyWorkRunner(store.factory, None)._release_cancelled_claim(current)
    async with store.factory() as db:
        task = await db.get(models.CopyWorkTask, task_id)
        assert task.state == 'READY' and task.attempts == 9
        assert task.lease_owner_id is task.lease_expires_at is None


@pytest.mark.parametrize('parent_kind', ['job', 'snapshot', 'partition'])
async def test_busy_parent_defers_without_settlement_relocking_or_spending_execution(store, parent_kind):
    target, task_id, current = await queued(store, partition=parent_kind == 'partition')
    for _ in range(4):
        async with store.factory.begin() as holder:
            snap = await holder.get(models.Snapshot, target.snapshot_id)
            if parent_kind == 'job':
                await holder.get(models.Job, snap.job_id, with_for_update=True)
            elif parent_kind == 'snapshot':
                await holder.get(models.Snapshot, snap.id, with_for_update=True, populate_existing=True)
            else:
                workflow = await holder.get(models.CopyWorkflow,
                    (await holder.get(models.CopyWorkTask, task_id)).workflow_id)
                await holder.get(models.SnapshotPartition, workflow.partition_id, with_for_update=True)
            result = await asyncio.wait_for(api().run_batch(store.factory, current), timeout=3)
            assert result.status == 'DEFERRED' and result.query_seconds == 0
            async with store.factory() as db:
                task = await db.get(models.CopyWorkTask, task_id)
                assert task.state == 'READY' and task.attempts == 0 and not task.prepared
                assert task.error == {'code': 'copy_lock_timeout', 'sqlstate': '55P03', 'phase': 'ownership'}
                assert task.lease_owner_id is task.lease_expires_at is None
        async with store.factory.begin() as db:
            (await db.get(models.CopyWorkTask, task_id)).ready_at = await db.scalar(select(func.clock_timestamp()))
        async with store.factory.begin() as db:
            current = await copy_work.claim(db, owner_id=uuid4())
            assert current is not None
    assert (await api().run_batch(store.factory, current)).status == 'COMPLETED'


@pytest.mark.parametrize('sqlstate', ['57014', '40P01', '40001'])
async def test_transient_abort_before_copy_does_not_consume_execution_failures(store, monkeypatch, sqlstate):
    module = api()
    _, task_id, current = await queued(store)
    async def abort_admission(db, *args):
        await db.execute(text(f"DO $$ BEGIN RAISE EXCEPTION USING ERRCODE='{sqlstate}'; END $$"))
    monkeypatch.setattr(module, '_physical_guards', abort_admission)
    for _ in range(4):
        result = await module.run_batch(store.factory, current)
        assert result.status == 'DEFERRED' and result.query_seconds == 0
        async with store.factory.begin() as db:
            task = await db.get(models.CopyWorkTask, task_id)
            assert task.attempts == 0 and 'execution_failures' not in task.error
            assert task.error['sqlstate'] == sqlstate and task.error['phase'] == 'admission'
            task.ready_at = await db.scalar(select(func.clock_timestamp()))
        async with store.factory.begin() as db:
            current = await copy_work.claim(db, owner_id=uuid4())
