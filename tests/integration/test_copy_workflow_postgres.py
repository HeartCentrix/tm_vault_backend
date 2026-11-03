"""Durable leaf handoff and required-copy barriers."""
import importlib
from uuid import uuid4

import pytest
from sqlalchemy import select, update
from test_chat_catalogue_postgres import store
from shared import models

pytestmark=pytest.mark.integration


def api():
    return importlib.import_module('shared.copy_workflow')


async def test_flagoff_collection_is_noop_without_database(monkeypatch):
    monkeypatch.delenv('COPY_WORK_ENABLED',raising=False)
    with api().copy_collection(uuid4()) as active:
        assert active is None
        assert await api().defer_current_copy('CHAT_CARRY',{}) is None
    assert api().pending_copy_sql(snapshot_expr='s.id') == 'TRUE'


async def test_seal_hands_off_leaf_then_resume_waits_for_all_required_tasks(store,monkeypatch):
    module=api();monkeypatch.setattr(module,'_COPY_GUARDS_ACTIVE',False);monkeypatch.setenv('COPY_WORK_ENABLED','true')
    monkeypatch.setattr(module,'async_session_factory',store.factory)
    scope=await store.new_scope()
    async with store.factory() as db:
        before=(await db.get(models.Snapshot,scope.snapshot_id)).lease_token
    with module.copy_collection(scope.snapshot_id) as active:
        assert active is not None
        delegated=await module.defer_current_copy('CHAT_CARRY',{'target_snapshot_id':str(scope.snapshot_id)})
        result=await module.seal_current_copy({'resume_kind':'user_content','result':{'item_count':0}})
    assert result['copy_waiting']
    async with store.factory() as db:
        snap=await db.get(models.Snapshot,scope.snapshot_id)
        assert snap.lease_token==before+1 and snap.lease_owner_id is None
        assert await module.has_pending_copy(db,snapshot_id=snap.id)
    owner=uuid4()
    assert await module.begin_resume(store.factory,delegated.workflow_id,owner) is None
    async with store.factory.begin() as db:
        await db.execute(update(models.CopyWorkTask).where(models.CopyWorkTask.id==delegated.task_id).values(state='COMPLETED'))
    workflow=await module.begin_resume(store.factory,delegated.workflow_id,owner)
    assert workflow.state=='RESUMING'
    with module.resuming_copy_workflow(workflow.id):
        async with store.factory() as db:
            assert not await module.has_pending_copy(db,snapshot_id=scope.snapshot_id)
    async with store.factory.begin() as db:
        await db.execute(update(models.Snapshot).where(models.Snapshot.id==scope.snapshot_id).values(status=models.SnapshotStatus.COMPLETED))
    await module.finish_resume(store.factory,workflow.id,owner)
    async with store.factory() as db:
        assert await module.has_pending_copy(db,snapshot_id=scope.snapshot_id)
    assert not await module.finish_parent_settlement(store.factory,workflow.id)
    async with store.factory.begin() as db:
        snap=await db.get(models.Snapshot,scope.snapshot_id)
        (await db.get(models.Job,snap.job_id)).status=models.JobStatus.COMPLETED
    assert await module.finish_parent_settlement(store.factory,workflow.id)
    async with store.factory() as db:
        assert not await module.has_pending_copy(db,snapshot_id=scope.snapshot_id)


async def test_swallowed_enqueue_failure_cannot_seal_successful_workflow(store,monkeypatch):
    module=api();monkeypatch.setattr(module,'_COPY_GUARDS_ACTIVE',False);monkeypatch.setenv('COPY_WORK_ENABLED','true');monkeypatch.setattr(module,'async_session_factory',store.factory)
    scope=await store.new_scope()
    with module.copy_collection(scope.snapshot_id):
        try: await module.defer_current_copy('NOT_A_COPY',{'target_snapshot_id':str(scope.snapshot_id)})
        except ValueError: pass
        with pytest.raises(Exception,match='copy.*persist|copy.*failed'):
            await module.seal_current_copy({'resume_kind':'user_content'})


async def test_old_resume_context_is_fenced_after_expired_callback_is_replaced(store,monkeypatch):
    from datetime import datetime,timedelta,timezone
    module=api();monkeypatch.setattr(module,'_COPY_GUARDS_ACTIVE',False);monkeypatch.setenv('COPY_WORK_ENABLED','true');monkeypatch.setattr(module,'async_session_factory',store.factory)
    scope=await store.new_scope()
    with module.copy_collection(scope.snapshot_id):
        delegated=await module.defer_current_copy('CHAT_CARRY',{})
        await module.seal_current_copy({'resume_kind':'user_content'})
    async with store.factory.begin() as db:
        await db.execute(update(models.CopyWorkTask).where(models.CopyWorkTask.id==delegated.task_id).values(state='COMPLETED'))
    old=await module.begin_resume(store.factory,delegated.workflow_id,uuid4())
    with module.resuming_copy_workflow(old.id):
        async with store.factory.begin() as db:
            await db.execute(update(models.CopyWorkflow).where(models.CopyWorkflow.id==old.id)
                .values(lease_expires_at=datetime.now(timezone.utc)-timedelta(seconds=1)))
        replacement=await module.begin_resume(store.factory,old.id,uuid4())
        assert replacement.generation>old.generation
        async with store.factory.begin() as db:
            assert await module.has_pending_copy(db,snapshot_id=scope.snapshot_id)
            assert not await module.verify_copy_resume(db)


async def test_cancelled_parent_recovery_releases_quiescent_source_pins(store,monkeypatch):
    module=api();monkeypatch.setattr(module,'_COPY_GUARDS_ACTIVE',False);monkeypatch.setenv('COPY_WORK_ENABLED','true');monkeypatch.setattr(module,'async_session_factory',store.factory)
    prior=await store.new_scope()
    async with store.factory.begin() as db:
        await db.execute(update(models.Snapshot).where(models.Snapshot.id==prior.snapshot_id).values(status=models.SnapshotStatus.COMPLETED))
    target=await store.new_scope()
    with module.copy_collection(target.snapshot_id):
        delegated=await module.defer_current_copy('FILE_VERSIONS',{'prior_snap_id':str(prior.snapshot_id),'file_ids':[]},lane='micro')
        await module.seal_current_copy({'resume_kind':'user_content'})
    async with store.factory.begin() as db:
        task=await db.get(models.CopyWorkTask,delegated.task_id);task.state='COMPLETED'
        workflow=await db.get(models.CopyWorkflow,delegated.workflow_id)
        job=await db.get(models.Job,workflow.job_id);job.status=models.JobStatus.CANCELLED
    assert await module.begin_resume(store.factory,delegated.workflow_id,uuid4()) is None
    async with store.factory() as db:
        assert not list((await db.scalars(select(models.CopyWorkItem).where(models.CopyWorkItem.task_id==delegated.task_id))).all())


async def test_source_drain_time_does_not_consume_database_admission_wait_budget(store,monkeypatch):
    from datetime import datetime,timedelta,timezone
    from shared.copy_work import claim
    module=api();monkeypatch.setattr(module,'_COPY_GUARDS_ACTIVE',False);monkeypatch.setenv('COPY_WORK_ENABLED','true');monkeypatch.setattr(module,'async_session_factory',store.factory)
    scope=await store.new_scope()
    with module.copy_collection(scope.snapshot_id):
        delegated=await module.defer_current_copy('CHAT_CARRY',{})
        async with store.factory.begin() as db:
            task=await db.get(models.CopyWorkTask,delegated.task_id)
            task.queue_entered_at=datetime.now(timezone.utc)-timedelta(hours=6)
            task.wait_max_s=10
        await module.seal_current_copy({'resume_kind':'user_content'})
    async with store.factory.begin() as db:
        admitted=await claim(db,owner_id=uuid4())
        assert admitted is not None
        task=await db.get(models.CopyWorkTask,admitted.task_id)
        assert task.cumulative_wait_s<10


async def test_producer_flag_rollback_keeps_database_protocol_authoritative(store,monkeypatch):
    module=api();monkeypatch.setattr(module,'_COPY_GUARDS_ACTIVE',False);monkeypatch.setenv('COPY_WORK_ENABLED','false');monkeypatch.setattr(module,'async_session_factory',store.factory)
    async with store.factory.begin() as db:
        db.add(models.CopyWorkControl(id='global',enabled=True))
    await module.initialize_copy_guards(store.factory)
    target=await store.new_scope()
    with module.copy_collection(target.snapshot_id) as active:
        assert active is not None
        delegated=await module.defer_current_copy('CHAT_CARRY',{})
        assert delegated is not None


async def test_stale_source_capture_cannot_adopt_replacement_before_first_intent(store,monkeypatch):
    module=api();monkeypatch.setattr(module,'_COPY_GUARDS_ACTIVE',False);monkeypatch.setenv('COPY_WORK_ENABLED','true');monkeypatch.setattr(module,'async_session_factory',store.factory)
    target=await store.new_scope()
    with module.copy_collection(target.snapshot_id,expected_snapshot_token=0):
        async with store.factory.begin() as db:
            await db.execute(update(models.Snapshot).where(models.Snapshot.id==target.snapshot_id).values(lease_token=1,lease_owner_id=uuid4()))
        with pytest.raises(module.CopyWorkConflict,match='generation'):
            await module.defer_current_copy('CHAT_CARRY',{})
    async with store.factory() as db:
        assert not list((await db.scalars(select(models.CopyWorkflow).where(models.CopyWorkflow.target_snapshot_id==target.snapshot_id))).all())


async def test_copy_parent_settlement_ignores_completed_provider_receipts_but_not_active_work(store, monkeypatch):
    from shared import provider_work
    from sqlalchemy import text
    module = api()
    monkeypatch.setenv('COPY_WORK_ENABLED', 'true')
    monkeypatch.setenv('PROVIDER_WORK_ENABLED', 'true')
    monkeypatch.setattr(provider_work, 'guards_enabled', lambda: True)
    scope = await store.new_scope()
    async with store.factory.begin() as db:
        receipt = await provider_work.ensure_work(db, snapshot_id=scope.snapshot_id,
            snapshot_token=0, job_token=0, continuation={})
        row = await db.get(models.ProviderWork, receipt.work_id)
        row.state = 'COMPLETED'
        row.parent_settle_pending = True
    async with store.factory() as db:
        assert await module.has_pending_copy(db, snapshot_id=scope.snapshot_id)
        with module.settling_copy_parent():
            assert not await module.has_pending_copy(db, snapshot_id=scope.snapshot_id)
            predicate = module.pending_copy_sql(snapshot_expr="'" + str(scope.snapshot_id) + "'::uuid")
            assert await db.scalar(text('SELECT ' + predicate))
        assert await module.has_pending_copy(db, snapshot_id=scope.snapshot_id)
    async with store.factory.begin() as db:
        (await db.get(models.ProviderWork, receipt.work_id)).state = 'WAITING'
    async with store.factory() as db:
        with module.settling_copy_parent():
            assert await module.has_pending_copy(db, snapshot_id=scope.snapshot_id)
            predicate = module.pending_copy_sql(snapshot_expr="'" + str(scope.snapshot_id) + "'::uuid")
            assert not await db.scalar(text('SELECT ' + predicate))


async def test_partition_cannot_finish_between_mail_carry_claim_and_copy_registration(store):
    import ast
    from pathlib import Path
    from sqlalchemy import text
    tree = ast.parse((Path(__file__).resolve().parents[2] / 'workers/backup-worker/main.py').read_text())
    method = next(n for n in ast.walk(tree) if isinstance(n, ast.AsyncFunctionDef)
                  and n.name == '_finalize_partitioned_snapshot')
    statement = next(n for n in ast.walk(method) if isinstance(n, ast.JoinedStr)
                     and any(isinstance(v, ast.Constant) and 'WITH expected AS' in str(v.value) for v in n.values))
    sql = eval(compile(ast.Expression(statement), '<partition-finalizer-sql>', 'eval'), {'copy_guard': 'TRUE'})
    scope = await store.new_scope()
    async with store.factory.begin() as db:
        snapshot = await db.get(models.Snapshot, scope.snapshot_id)
        snapshot.extra_data = {'mail_union_carry_claimed': True, 'mail_carry_forward_pending': True}
        db.add(models.SnapshotPartition(snapshot_id=snapshot.id,
            tenant_id=store.tenant, resource_id=store.resource, job_id=snapshot.job_id,
            drive_id='', file_ids=[], partition_index=0, total_files=0, total_bytes_est=0,
            partition_type='MAIL_FOLDERS', status='COMPLETED', lease_token=0))
    async with store.factory.begin() as db:
        assert (await db.execute(text(sql), {'sid': str(scope.snapshot_id)})).first() is None
        snapshot = await db.get(models.Snapshot, scope.snapshot_id)
        assert snapshot.status == models.SnapshotStatus.IN_PROGRESS
        snapshot.extra_data = {**snapshot.extra_data, 'mail_carry_forward_pending': False}
    async with store.factory.begin() as db:
        row = (await db.execute(text(sql), {'sid': str(scope.snapshot_id)})).first()
        assert row.new_status == 'COMPLETED'
