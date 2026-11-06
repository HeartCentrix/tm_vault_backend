"""Parent finalization remains durable after concurrent child continuations."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import select, update

from test_chat_catalogue_postgres import store
from test_copy_worker_handoff_postgres import bw, prepare
from shared import models, copy_workflow
from shared.copy_runtime import CopyWorkRunner

pytestmark = pytest.mark.integration


def worker_for(bw):
    worker = bw.BackupWorker.__new__(bw.BackupWorker)
    worker.worker_id = 'copy-parent-validation'
    worker._heartbeat = SimpleNamespace(worker_uuid=uuid4())
    worker._check_snapshot_anomaly = AsyncMock()
    worker._stamp_resource_from_snapshot_row = AsyncMock()
    worker._stamp_resource_backup_state = AsyncMock()
    worker.flush_version_rows = AsyncMock(return_value=0)
    worker.audit_logger = SimpleNamespace(log=AsyncMock())
    worker._emit_partition_audit = AsyncMock()
    worker.get_graph_client = AsyncMock(side_effect=AssertionError('no source enumeration on resume'))
    return worker


async def partition_work(store, monkeypatch, bw, count=1):
    scope, first = await prepare(store, monkeypatch, bw, partition=True)
    partitions = [first] + [uuid4() for _ in range(count-1)]
    async with store.factory.begin() as db:
        head = await db.get(models.Snapshot, scope.snapshot_id)
        db.add(models.SnapshotItem(snapshot_id=head.id, tenant_id=store.tenant,
            external_id='captured', item_type='TEAMS_CHAT_MESSAGE', name='captured',
            content_size=7, backend_id=store.backend))
        for i, pid in enumerate(partitions[1:], 1):
            db.add(models.SnapshotPartition(id=pid, snapshot_id=head.id, tenant_id=store.tenant,
                resource_id=store.resource, job_id=head.job_id, drive_id='', file_ids=[],
                partition_index=i, total_files=0, total_bytes_est=0,
                partition_type='CHATS', status='IN_PROGRESS', lease_token=1))
    workflows = []
    for pid in partitions:
        with copy_workflow.copy_collection(scope.snapshot_id, pid):
            task = await copy_workflow.defer_current_copy('CHAT_CARRY', {})
            await copy_workflow.seal_current_copy({'resume_kind':'partition', 'partition_id':str(pid),
                                                  'result':{'item_count':1, 'bytes_added':7}})
            workflows.append(task.workflow_id)
    async with store.factory.begin() as db:
        await db.execute(update(models.CopyWorkTask).where(models.CopyWorkTask.workflow_id.in_(workflows))
                         .values(state='COMPLETED'))
    return scope, workflows


@pytest.mark.parametrize('source_loss',[False,True])
async def test_concurrent_partition_continuations_complete_parent_after_barrier(store, monkeypatch, bw, source_loss):
    scope, workflows = await partition_work(store, monkeypatch, bw, 2)
    if source_loss:
        async with store.factory.begin() as db:
            wf=await db.get(models.CopyWorkflow,workflows[0])
            wf.continuation={**wf.continuation,'result':{'item_count':1,'files_failed':2,'enumeration_incomplete':True}}
    worker = worker_for(bw)
    original = worker._finalize_partitioned_snapshot
    arrived, gate = [], asyncio.Event()
    async def both_children(*args, **kwargs):
        arrived.append(1)
        if len(arrived) >= 2: gate.set()
        await asyncio.wait_for(gate.wait(), 2)
        return await original(*args, **kwargs)
    worker._finalize_partitioned_snapshot = both_children
    await asyncio.gather(*(worker.resume_copy_workflow(wid) for wid in workflows))
    async with store.factory() as db:
        snapshot = await db.get(models.Snapshot, scope.snapshot_id)
        assert snapshot.status == (models.SnapshotStatus.PARTIAL if source_loss else models.SnapshotStatus.COMPLETED)
        assert (await db.get(models.Job, snapshot.job_id)).status == (
            models.JobStatus.PARTIAL if source_loss else models.JobStatus.COMPLETED)
        assert not any(w.parent_settle_pending for w in
            (await db.scalars(select(models.CopyWorkflow).where(models.CopyWorkflow.id.in_(workflows)))).all())


async def test_crash_after_leaf_completion_recovers_parent_without_broker(store, monkeypatch, bw):
    scope, workflows = await partition_work(store, monkeypatch, bw)
    worker = worker_for(bw)
    original = getattr(worker, '_settle_copy_parent', None)
    worker._settle_copy_parent = AsyncMock(side_effect=RuntimeError('simulated process exit after leaf commit'))
    with pytest.raises(RuntimeError, match='simulated process exit'):
        await worker.resume_copy_workflow(workflows[0])
    async with store.factory.begin() as db:
        wf = await db.get(models.CopyWorkflow, workflows[0])
        assert wf.parent_settle_pending and wf.state == 'COMPLETED'
        control=await db.get(models.CopyWorkControl,'global')
        if control is None: db.add(models.CopyWorkControl(id='global', enabled=True))
        else: control.enabled=True
    worker._settle_copy_parent = original
    runner = CopyWorkRunner(store.factory, worker.resume_copy_workflow)
    try:
        assert await runner.recover_once() == 1
    finally:
        await runner.close()
    async with store.factory() as db:
        head = await db.get(models.Snapshot, scope.snapshot_id)
        assert head.status == models.SnapshotStatus.COMPLETED
        assert (await db.get(models.Job, head.job_id)).status == models.JobStatus.COMPLETED
        assert not (await db.get(models.CopyWorkflow, workflows[0])).parent_settle_pending


async def test_concurrent_bulk_resource_continuations_finish_shared_job(store, monkeypatch, bw):
    scope, _ = await prepare(store, monkeypatch, bw)
    rid, second = uuid4(), uuid4()
    async with store.factory.begin() as db:
        first = await db.get(models.Snapshot, scope.snapshot_id)
        job = await db.get(models.Job, first.job_id)
        job.batch_resource_ids = [str(store.resource), str(rid)]
        db.add(models.Resource(id=rid, tenant_id=store.tenant, type=models.ResourceType.USER_CHATS,
                              external_id='second-validation-resource', display_name='Second validation', extra_data={}))
        await db.flush()
        db.add(models.Snapshot(id=second, resource_id=rid, job_id=job.id, backend_id=store.backend, extra_data={}))
        await db.flush()
        for snapshot_id in (first.id, second):
            db.add(models.SnapshotItem(snapshot_id=snapshot_id, tenant_id=store.tenant,
                item_type='TEAMS_CHAT_MESSAGE', external_id='captured', name='captured',
                content_size=7, backend_id=store.backend))
        job_id = job.id
    workflows = []
    for snapshot_id in (scope.snapshot_id, second):
        with copy_workflow.copy_collection(snapshot_id):
            task = await copy_workflow.defer_current_copy('CHAT_CARRY', {})
            await copy_workflow.seal_current_copy({'resume_kind':'user_content', 'result':{}})
            workflows.append(task.workflow_id)
    async with store.factory.begin() as db:
        await db.execute(update(models.CopyWorkTask).where(models.CopyWorkTask.workflow_id.in_(workflows))
                         .values(state='COMPLETED'))
    worker = worker_for(bw)
    original, arrived, gate = worker._finalize_bulk_parent_if_complete, [], asyncio.Event()
    async def both_resources(*args, **kwargs):
        arrived.append(1)
        if len(arrived) >= 2: gate.set()
        await asyncio.wait_for(gate.wait(), 2)
        return await original(*args, **kwargs)
    worker._finalize_bulk_parent_if_complete = both_resources
    await asyncio.gather(*(worker.resume_copy_workflow(wid) for wid in workflows))
    async with store.factory() as db:
        assert (await db.get(models.Job, job_id)).status == models.JobStatus.COMPLETED
        assert not any(w.parent_settle_pending for w in
            (await db.scalars(select(models.CopyWorkflow).where(models.CopyWorkflow.id.in_(workflows)))).all())


async def test_expired_callback_after_partition_commit_recovers_parent_without_abandoning_copy(store,monkeypatch,bw):
    from datetime import datetime,timedelta,timezone
    scope,workflows=await partition_work(store,monkeypatch,bw)
    worker=worker_for(bw)
    finish=copy_workflow.finish_resume
    monkeypatch.setattr(copy_workflow,'finish_resume',AsyncMock(side_effect=RuntimeError('crash before workflow commit')))
    with pytest.raises(RuntimeError,match='crash before workflow commit'):
        await worker.resume_copy_workflow(workflows[0])
    monkeypatch.setattr(copy_workflow,'finish_resume',finish)
    async with store.factory.begin() as db:
        (await db.get(models.CopyWorkControl,'global')).enabled=True
    runner=CopyWorkRunner(store.factory,worker.resume_copy_workflow)
    try:
        assert await runner.recover_once()==0
        async with store.factory.begin() as db:
            wf=await db.get(models.CopyWorkflow,workflows[0])
            assert wf.state=='RESUMING'
            wf.lease_expires_at=datetime.now(timezone.utc)-timedelta(seconds=1)
        assert await runner.recover_once()==1
    finally:await runner.close()
    async with store.factory() as db:
        head=await db.get(models.Snapshot,scope.snapshot_id)
        assert head.status==models.SnapshotStatus.COMPLETED
        assert (await db.get(models.Job,head.job_id)).status==models.JobStatus.COMPLETED
        wf=await db.get(models.CopyWorkflow,workflows[0])
        assert wf.state=='COMPLETED' and not wf.parent_settle_pending
