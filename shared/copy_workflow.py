"""Required-copy collection, fenced leaf suspension and durable continuation."""
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import timedelta
import json
import os
from uuid import UUID

from sqlalchemy import delete, exists, func, select, update
from sqlalchemy.exc import SQLAlchemyError

from shared.database import async_session_factory
from shared.models import (CopyWorkflow, CopyWorkTask, CopyWorkItem, CopyWorkInput, CopyWorkControl,
    Job, Snapshot, SnapshotPartition)
from shared.copy_work import ACTIVE_WORKFLOWS, TERMINAL_TASKS, CopyRequest, CopyWorkConflict, enqueue

_COPY_GUARDS_ACTIVE = False
_COLLECTION = ContextVar('required_copy_collection', default=None)
_RESUMING = ContextVar('resuming_copy_workflow', default=None)
_RESUME_CLAIM = ContextVar('copy_resume_claim', default=None)
_PROTOCOL_MODE = ContextVar('copy_database_protocol', default=None)


@dataclass
class CopyCollection:
    snapshot_id: UUID
    partition_id: UUID | None = None
    error: BaseException | None = None
    expected_snapshot_token: int | None = None
    expected_partition_token: int | None = None
    expected_job_token: int | None = None
    sealed_result: dict | None = None


@dataclass(frozen=True)
class DeferredCopy:
    task_id: UUID
    workflow_id: UUID


@dataclass(frozen=True)
class ResumeClaim:
    workflow_id: UUID
    owner_id: UUID
    generation: int
    parent_lease_token: int


def _uuid(value):
    return UUID(str(value)) if value is not None else None


def copy_work_enabled():
    return os.getenv('COPY_WORK_ENABLED', 'false').strip().lower() in ('1','true','yes','on')


def copy_work_guards_enabled():
    return copy_work_enabled() or _COPY_GUARDS_ACTIVE or _RESUMING.get() is not None


async def initialize_copy_guards(factory):
    """Latch persisted work even when a deployment turns producers back off."""
    global _COPY_GUARDS_ACTIVE
    try:
        async with factory() as session:
            control = await session.get(CopyWorkControl, 'global')
            pending = await session.scalar(select(exists(select(CopyWorkflow.id).where(CopyWorkflow.state.in_(ACTIVE_WORKFLOWS)))))
            # Epoch mode is inherited by this worker's handler tasks. A local
            # flag rollback cannot silently mix legacy writers with the active
            # database protocol. Disabling the epoch requires drain/restart.
            _PROTOCOL_MODE.set(bool(pending or (control and control.enabled)))
            _COPY_GUARDS_ACTIVE = _COPY_GUARDS_ACTIVE or copy_work_enabled() or bool(pending or (control and control.enabled))
    except SQLAlchemyError as exc:
        from shared.db_retry import _chain
        missing = any(getattr(e, 'sqlstate', None) == '42P01' for e in _chain(exc))
        if not missing or copy_work_enabled():
            _COPY_GUARDS_ACTIVE = True
            raise
    return copy_work_guards_enabled()


@contextmanager
def copy_collection(snapshot_id, partition_id=None, *, expected_snapshot_token=None,
                    expected_partition_token=None, expected_job_token=None):
    mode=_PROTOCOL_MODE.get()
    active=copy_work_enabled() if mode is None else mode
    if not active:
        yield None
        return
    snapshot_id,partition_id=_uuid(snapshot_id),_uuid(partition_id)
    existing=_COLLECTION.get()
    if active and existing and (existing.snapshot_id,existing.partition_id)==(snapshot_id,partition_id):
        # Nested wrappers own the same source observation/error latch. They
        # cannot replace the captured generation with a newly read one.
        yield existing
        return
    capture = CopyCollection(snapshot_id,partition_id,
        expected_snapshot_token=expected_snapshot_token,expected_partition_token=expected_partition_token,
        expected_job_token=expected_job_token) if active else None
    token = _COLLECTION.set(capture)
    try:
        yield capture
    finally:
        _COLLECTION.reset(token)


@contextmanager
def resuming_copy_workflow(workflow_id):
    claim = _RESUME_CLAIM.get()
    if claim is None or claim.workflow_id != _uuid(workflow_id):
        raise CopyWorkConflict('copy resume has no claimed generation')
    token = _RESUMING.set(claim)
    try:
        yield
    finally:
        _RESUMING.reset(token)


def _conditions(snapshot_expr=None, partition_expr=None, job_expr=None):
    values=[]
    if snapshot_expr is not None: values.append('cw.target_snapshot_id='+snapshot_expr)
    if partition_expr is not None: values.append('cw.partition_id='+partition_expr)
    if job_expr is not None: values.append('cw.job_id='+job_expr)
    if not values: raise ValueError('copy guard needs a parent expression')
    return '('+' AND '.join(values)+')'


def pending_copy_sql(snapshot_expr=None, partition_expr=None, job_expr=None):
    if not copy_work_guards_enabled(): return 'TRUE'
    own = _RESUMING.get()
    exclusion = " AND cw.id <> '"+str(own.workflow_id)+"'::uuid" if own else ''
    predicate = "NOT EXISTS (SELECT 1 FROM copy_workflows cw WHERE cw.state IN ('DRAINING','WAITING','RESUMING') AND "+_conditions(snapshot_expr,partition_expr,job_expr)+exclusion+')'
    if own:
        predicate += " AND EXISTS (SELECT 1 FROM copy_workflows rw WHERE rw.id='"+str(own.workflow_id)+"'::uuid AND rw.state='RESUMING' AND rw.lease_owner_id='"+str(own.owner_id)+"'::uuid AND rw.generation="+str(own.generation)+" AND rw.parent_lease_token="+str(own.parent_lease_token)+")"
    return '('+predicate+')'


def copy_source_pinned_sql(snapshot_expr):
    if not copy_work_guards_enabled(): return 'TRUE'
    value='CAST(('+snapshot_expr+') AS text)'
    return """NOT EXISTS (SELECT 1 FROM copy_workflows cw
        WHERE cw.state IN ('DRAINING','WAITING','RESUMING') AND (
          cw.target_snapshot_id=("""+snapshot_expr+""") OR EXISTS (
            SELECT 1 FROM copy_work_tasks ct LEFT JOIN copy_work_items ci ON ci.task_id=ct.id
             WHERE ct.workflow_id=cw.id AND (ci.source_snapshot_id=("""+snapshot_expr+""")
                OR ct.params->>'prior_snap_id'="""+value+"""
                OR ct.params->>'prior_snapshot_id'="""+value+"""))))"""


def copy_blob_pinned_sql(blob_expr):
    if not copy_work_guards_enabled(): return 'TRUE'
    return """NOT EXISTS (SELECT 1 FROM copy_work_items ci JOIN copy_work_tasks ct ON ct.id=ci.task_id
        JOIN copy_workflows cw ON cw.id=ct.workflow_id
        WHERE cw.state IN ('DRAINING','WAITING','RESUMING') AND ci.payload->>'blob_path'=("""+blob_expr+"""))
      AND NOT EXISTS (SELECT 1 FROM snapshot_items csi WHERE csi.blob_path=("""+blob_expr+""")
        AND NOT ("""+copy_source_pinned_sql('csi.snapshot_id')+"""))"""


async def has_pending_copy(session, *, snapshot_id=None, partition_id=None, job_id=None):
    if not copy_work_guards_enabled(): return False
    if _RESUMING.get() is not None and not await verify_copy_resume(session, lock=False): return True
    filters=[CopyWorkflow.state.in_(ACTIVE_WORKFLOWS)]
    for column,value in ((CopyWorkflow.target_snapshot_id,snapshot_id),(CopyWorkflow.partition_id,partition_id),(CopyWorkflow.job_id,job_id)):
        if value is not None: filters.append(column==_uuid(value))
    if _RESUMING.get(): filters.append(CopyWorkflow.id!=_RESUMING.get().workflow_id)
    value=await session.scalar(select(exists(select(CopyWorkflow.id).where(*filters))))
    return value is True


async def defer_current_copy(kind, params, lane='giant'):
    global _COPY_GUARDS_ACTIVE
    capture=_COLLECTION.get()
    if capture is None: return None
    try:
        target=_uuid(params.get('target_snapshot_id') or capture.snapshot_id)
        if target!=capture.snapshot_id: raise CopyWorkConflict('copy target differs from current leaf')
        async with async_session_factory.begin() as session:
            pointer=await session.get(Snapshot,target)
            if pointer is None: raise CopyWorkConflict('copy source capture parent disappeared')
            job=await session.get(Job,pointer.job_id,with_for_update={'read':True},populate_existing=True)
            snapshot=await session.get(Snapshot,target,with_for_update={'read':True},populate_existing=True)
            partition=await session.get(SnapshotPartition,capture.partition_id,
                with_for_update={'read':True},populate_existing=True) if capture.partition_id else None
            for expected,actual in ((capture.expected_job_token,job.lease_token if job else None),
                    (capture.expected_snapshot_token,snapshot.lease_token if snapshot else None),
                    (capture.expected_partition_token,partition.lease_token if partition else None)):
                if expected is not None and expected!=actual:
                    raise CopyWorkConflict('copy source capture generation has moved')
            if capture.expected_snapshot_token is None: capture.expected_snapshot_token=snapshot.lease_token
            if capture.expected_job_token is None: capture.expected_job_token=job.lease_token
            if partition is not None and capture.expected_partition_token is None:
                capture.expected_partition_token=partition.lease_token
            task=await enqueue(session,CopyRequest(kind=kind,lane=lane,target_snapshot_id=target,
                partition_id=capture.partition_id,params=params))
            delegated=DeferredCopy(task.id,task.workflow_id)
        _COPY_GUARDS_ACTIVE=True
        return delegated
    except BaseException as exc:
        capture.error=exc
        raise


async def _lock_parents(session, workflow, *, shared=False):
    # Same order as API cancellation: job -> snapshot -> partition -> workflow.
    job=await session.scalar(select(Job).where(Job.id==workflow.job_id).with_for_update(read=shared))
    snapshot=await session.scalar(select(Snapshot).where(Snapshot.id==workflow.target_snapshot_id).with_for_update(read=shared))
    partition=None
    if workflow.partition_id:
        partition=await session.scalar(select(SnapshotPartition).where(SnapshotPartition.id==workflow.partition_id).with_for_update(read=shared))
    return job,snapshot,partition


async def verify_copy_resume(session, *, lock=True):
    """Fence real settlement writes, holding parent/claim locks until commit."""
    claim=_RESUMING.get()
    if claim is None: return True
    workflow=await session.get(CopyWorkflow,claim.workflow_id)
    if workflow is None: return False
    if lock:
        job,snapshot,partition=await _lock_parents(session,workflow)
        workflow=await session.scalar(select(CopyWorkflow).where(CopyWorkflow.id==claim.workflow_id)
            .with_for_update().execution_options(populate_existing=True))
    else:
        job=await session.get(Job,workflow.job_id,populate_existing=True)
        snapshot=await session.get(Snapshot,workflow.target_snapshot_id,populate_existing=True)
        partition=await session.get(SnapshotPartition,workflow.partition_id,populate_existing=True) if workflow.partition_id else None
        workflow=await session.get(CopyWorkflow,claim.workflow_id,populate_existing=True)
    if (workflow is None or workflow.state!='RESUMING' or workflow.generation!=claim.generation
        or workflow.lease_owner_id!=claim.owner_id or workflow.parent_lease_token!=claim.parent_lease_token
        or job is None or snapshot is None or job.status.value in ('CANCELLED','CANCELLING')
        or (snapshot.extra_data or {}).get('cancelled_at')): return False
    leaf=partition or snapshot
    if leaf is None or leaf.lease_token!=claim.parent_lease_token: return False
    # An expired lease with unchanged generation can safely renew only while
    # locked; a replacement generation is never adopted by the old callback.
    if lock:
        now=await session.scalar(select(func.clock_timestamp()))
        workflow.lease_expires_at=now+timedelta(seconds=90)
    return True


async def seal_current_copy(continuation):
    capture=_COLLECTION.get()
    if capture is None: return None
    if capture.error is not None: raise CopyWorkConflict('required copy intent persistence failed') from capture.error
    if capture.sealed_result is not None: return capture.sealed_result
    encoded=json.dumps(continuation,default=str)
    if len(encoded.encode())>65536: raise ValueError('copy continuation must contain bounded scalars and durable references')
    async with async_session_factory.begin() as session:
        leaf_key=('partition:'+str(capture.partition_id)) if capture.partition_id else ('snapshot:'+str(capture.snapshot_id))
        workflow=await session.scalar(select(CopyWorkflow).where(CopyWorkflow.leaf_key==leaf_key))
        if workflow is None: return None
        job,snapshot,partition=await _lock_parents(session,workflow)
        workflow=await session.scalar(select(CopyWorkflow).where(CopyWorkflow.id==workflow.id).with_for_update())
        if workflow.state!='DRAINING' or job.status.value not in ('RUNNING','QUEUED','PENDING','RETRYING') or snapshot.status.value!='IN_PROGRESS':
            raise CopyWorkConflict('copy parent no longer permits suspension')
        leaf=partition or snapshot
        if partition is not None and partition.status!='IN_PROGRESS': raise CopyWorkConflict('copy partition is terminal')
        if leaf.lease_token!=workflow.parent_lease_token: raise CopyWorkConflict('copy leaf ownership moved during capture')
        leaf.lease_token+=1
        leaf.lease_owner_id=None;leaf.lease_expires_at=None
        workflow.parent_lease_token=leaf.lease_token
        workflow.snapshot_lease_token=snapshot.lease_token
        workflow.job_lease_token=job.lease_token
        workflow.continuation=json.loads(encoded)
        workflow.state='WAITING';workflow.generation+=1
        workflow.lease_owner_id=None;workflow.lease_expires_at=None
        now=await session.scalar(select(func.clock_timestamp()))
        # Source enumeration was not capacity waiting. Start the admission
        # clock only when this drain has actually made its intents runnable.
        await session.execute(update(CopyWorkTask).where(CopyWorkTask.workflow_id==workflow.id,
            CopyWorkTask.capture_attempt==workflow.capture_attempt,CopyWorkTask.state=='READY')
            .values(ready_at=now,queue_entered_at=now,cumulative_wait_s=0))
        snapshot.extra_data={**(snapshot.extra_data or {}),'copy_workflow_id':str(workflow.id),'copy_phase':'QUEUED'}
        capture.sealed_result={'copy_waiting':True,'copy_workflow_id':str(workflow.id),'snapshot_id':str(snapshot.id)}
        return capture.sealed_result


async def begin_resume(factory, workflow_id, owner_id):
    async with factory.begin() as session:
        workflow=await session.get(CopyWorkflow,_uuid(workflow_id))
        if workflow is None: return None
        job,snapshot,partition=await _lock_parents(session,workflow)
        workflow=await session.scalar(select(CopyWorkflow).where(CopyWorkflow.id==workflow.id).with_for_update())
        now=await session.scalar(select(func.clock_timestamp()))
        if workflow.state not in ('WAITING','RESUMING'): return None
        if workflow.state=='RESUMING' and workflow.lease_expires_at and workflow.lease_expires_at>now: return None
        pending=await session.scalar(select(exists(select(CopyWorkTask.id).where(
            CopyWorkTask.workflow_id==workflow.id,CopyWorkTask.capture_attempt==workflow.capture_attempt,CopyWorkTask.state.notin_(TERMINAL_TASKS)))))
        if pending: return None
        if job is None or snapshot is None: raise CopyWorkConflict('copy resume parent missing')
        leaf=partition or snapshot
        terminal=(partition.status in ('COMPLETED','FAILED') if partition else snapshot.status.value in ('COMPLETED','PARTIAL','FAILED'))
        if terminal:
            workflow.state='COMPLETED' if (partition.status=='COMPLETED' if partition else snapshot.status.value=='COMPLETED') else 'PARTIAL'
            workflow.completed_at=now
            await session.execute(delete(CopyWorkItem).where(CopyWorkItem.task_id.in_(select(CopyWorkTask.id).where(CopyWorkTask.workflow_id==workflow.id))))
            await session.execute(delete(CopyWorkInput).where(CopyWorkInput.task_id.in_(select(CopyWorkTask.id).where(CopyWorkTask.workflow_id==workflow.id))))
            return None
        if job.status.value not in ('RUNNING','QUEUED','PENDING','RETRYING'):
            workflow.state='CANCELLED';workflow.completed_at=now
            await session.execute(delete(CopyWorkItem).where(CopyWorkItem.task_id.in_(select(CopyWorkTask.id).where(CopyWorkTask.workflow_id==workflow.id))))
            await session.execute(delete(CopyWorkInput).where(CopyWorkInput.task_id.in_(select(CopyWorkTask.id).where(CopyWorkTask.workflow_id==workflow.id))))
            return None
        if leaf.lease_token!=workflow.parent_lease_token: raise CopyWorkConflict('copy resume leaf generation moved')
        leaf.lease_token+=1;leaf.lease_owner_id=_uuid(owner_id);leaf.lease_expires_at=now+timedelta(seconds=90)
        workflow.parent_lease_token=leaf.lease_token
        workflow.snapshot_lease_token=snapshot.lease_token
        workflow.state='RESUMING';workflow.generation+=1
        workflow.lease_owner_id=_uuid(owner_id);workflow.lease_expires_at=now+timedelta(seconds=90)
        failed=await session.scalar(select(exists(select(CopyWorkTask.id).where(CopyWorkTask.workflow_id==workflow.id,CopyWorkTask.capture_attempt==workflow.capture_attempt,CopyWorkTask.state!='COMPLETED'))))
        if failed:
            snapshot.extra_data={**(snapshot.extra_data or {}),'copy_required_failed':True,'copy_phase':'FAILED',
                'degraded_reason':'required database copy did not complete'}
        else:
            data={**(snapshot.extra_data or {}),'copy_phase':'COMPLETE'}
            data.pop('copy_required_failed',None)
            if data.get('degraded_reason')=='required database copy did not complete': data.pop('degraded_reason',None)
            snapshot.extra_data=data
        await session.flush()
        _RESUME_CLAIM.set(ResumeClaim(workflow.id,workflow.lease_owner_id,workflow.generation,workflow.parent_lease_token))
        return workflow


async def finish_resume(factory, workflow_id, owner_id, failed=False):
    async with factory.begin() as session:
        workflow=await session.get(CopyWorkflow,_uuid(workflow_id))
        if workflow is None: return False
        job,snapshot,partition=await _lock_parents(session,workflow)
        workflow=await session.scalar(select(CopyWorkflow).where(CopyWorkflow.id==workflow.id).with_for_update())
        if workflow.state!='RESUMING' or workflow.lease_owner_id!=_uuid(owner_id): return False
        leaf=partition or snapshot
        terminal=partition.status in ('COMPLETED','FAILED') if partition else snapshot.status.value in ('COMPLETED','PARTIAL','FAILED')
        if not terminal: raise CopyWorkConflict('copy continuation did not settle its leaf')
        gaps=await session.scalar(select(exists(select(CopyWorkTask.id).where(CopyWorkTask.workflow_id==workflow.id,CopyWorkTask.capture_attempt==workflow.capture_attempt,CopyWorkTask.state!='COMPLETED'))))
        workflow.state='PARTIAL' if failed or gaps else 'COMPLETED'
        workflow.completed_at=await session.scalar(select(func.clock_timestamp()))
        workflow.lease_owner_id=None;workflow.lease_expires_at=None
        await session.execute(delete(CopyWorkItem).where(CopyWorkItem.task_id.in_(select(CopyWorkTask.id).where(CopyWorkTask.workflow_id==workflow.id))))
        await session.execute(delete(CopyWorkInput).where(CopyWorkInput.task_id.in_(select(CopyWorkTask.id).where(CopyWorkTask.workflow_id==workflow.id))))
        return True


async def cancel_copy_work(session, *, snapshot_id=None, job_id=None, workflow_id=None, reason='cancelled', terminal_state='CANCELLED'):
    if not copy_work_guards_enabled(): return 0
    filters=[CopyWorkflow.state.in_(ACTIVE_WORKFLOWS)]
    if snapshot_id is not None: filters.append(CopyWorkflow.target_snapshot_id==_uuid(snapshot_id))
    if job_id is not None: filters.append(CopyWorkflow.job_id==_uuid(job_id))
    if workflow_id is not None: filters.append(CopyWorkflow.id==_uuid(workflow_id))
    if snapshot_id is None and job_id is None and workflow_id is None: raise ValueError('copy cancellation needs parent scope')
    if terminal_state not in ('CANCELLED','FAILED'): raise ValueError('invalid copy abort state')
    workflows=list((await session.scalars(select(CopyWorkflow).where(*filters).order_by(CopyWorkflow.job_id,CopyWorkflow.target_snapshot_id,CopyWorkflow.id))).all())
    for initial in workflows:
        job,snapshot,partition=await _lock_parents(session,initial)
        workflow=await session.scalar(select(CopyWorkflow).where(CopyWorkflow.id==initial.id).with_for_update())
        tasks=list((await session.scalars(select(CopyWorkTask).where(CopyWorkTask.workflow_id==workflow.id).order_by(CopyWorkTask.id).with_for_update())).all())
        for task in tasks:
            if task.state not in TERMINAL_TASKS:
                task.state='CANCELLED';task.error={'code':'copy_cancelled','reason':str(reason)[:80]}
                task.generation+=1;task.lease_owner_id=None;task.lease_expires_at=None
        workflow.state=terminal_state;workflow.generation+=1;workflow.completed_at=await session.scalar(select(func.clock_timestamp()))
        leaf=partition or snapshot
        if (terminal_state=='FAILED' and snapshot is not None and leaf is not None
            and leaf.lease_token==workflow.parent_lease_token and snapshot.status.value=='IN_PROGRESS'):
            snapshot.extra_data={**(snapshot.extra_data or {}),'copy_required_failed':True,
                'degraded_reason':'required database copy did not complete','copy_phase':'FAILED'}
        if job is not None:
            job.result={**(job.result or {}),'copy_work_last_failure':{
                'workflow_id':str(workflow.id),'code':'copy_abandoned' if terminal_state=='FAILED' else 'copy_cancelled',
                'task_count':len(tasks)}}
        await session.execute(delete(CopyWorkItem).where(CopyWorkItem.task_id.in_([task.id for task in tasks])))
        await session.execute(delete(CopyWorkInput).where(CopyWorkInput.task_id.in_([task.id for task in tasks])))
    return len(workflows)
