"""Bounded SQL batches with transactional physical capacity and owner fences."""
import asyncio
from dataclasses import dataclass
from datetime import timedelta
import hashlib
import logging
import random
from time import monotonic

from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert

from shared import copy_executor
from shared.copy_work import ACTIVE_WORKFLOWS, CopyClaim
from shared.database import CARRY_FORWARD_BUCKETS, GIANT_CARRY_FORWARD_BUCKETS
from shared.db_retry import first_db_sqlstate
from shared.models import CopyWorkControl, CopyWorkflow, CopyWorkTask, Job, Snapshot, SnapshotPartition


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BatchResult:
    status: str
    task_id: object
    rows: int = 0
    bytes: int = 0
    query_seconds: float = 0.0


class _Stale(Exception):
    pass


class _Cancelled(Exception):
    pass


class _ParentEnded(Exception):
    pass


class _ExecutionUnsettled(Exception):
    pass


class _Deferred(Exception):
    def __init__(self, code):
        self.code = code


def _status(row):
    return getattr(row.status, 'value', row.status)


def _lock_key(*parts):
    return int.from_bytes(hashlib.blake2s(':'.join(map(str, parts)).encode(), digest_size=4).digest(),
                          byteorder='big', signed=True)


async def _settings(session, claim, statement_timeout_ms, lock_timeout_ms):
    await session.execute(text("""SELECT set_config('statement_timeout',:statement,true),
        set_config('lock_timeout',:lock,true),set_config('application_name',:name,true)"""),
        {'statement': f'{statement_timeout_ms}ms', 'lock': f'{lock_timeout_ms}ms',
         'name': f'tm-copy:{str(claim.task_id)[:8]}:{claim.generation}'[:63]})


async def _owned(session, claim, *, lock_parents=True):
    # Parent-first locks match cancellation. SHARE permits independent batches
    # under the same job while preventing its cancellation from committing
    # between the ownership check and the atomic copy/checkpoint commit.
    pointers = (await session.execute(select(CopyWorkflow.job_id, CopyWorkflow.target_snapshot_id,
        CopyWorkflow.partition_id, CopyWorkflow.id).join(CopyWorkTask,
            CopyWorkTask.workflow_id == CopyWorkflow.id).where(CopyWorkTask.id == claim.task_id))).first()
    if pointers is None:
        raise _Stale
    parent_lock = {'read': True, 'key_share': True, 'nowait': True} if lock_parents else None
    job = await session.get(Job, pointers.job_id,
        with_for_update=parent_lock, populate_existing=True)
    snapshot = await session.get(Snapshot, pointers.target_snapshot_id,
        with_for_update=parent_lock, populate_existing=True)
    partition = await session.get(SnapshotPartition, pointers.partition_id,
        with_for_update=parent_lock, populate_existing=True) if pointers.partition_id else None
    workflow = await session.get(CopyWorkflow, pointers.id,
        with_for_update={'nowait': True}, populate_existing=True)
    task = await session.get(CopyWorkTask, claim.task_id,
        with_for_update={'nowait': True}, populate_existing=True)
    if (task is None or workflow is None or job is None or snapshot is None
        or task.workflow_id != workflow.id or workflow.state != 'WAITING'
        or task.state != 'RUNNING' or task.generation != claim.generation
        or task.capture_attempt != workflow.capture_attempt
        or task.lease_owner_id != claim.owner_id):
        raise _Stale
    if (workflow.job_id != job.id or workflow.target_snapshot_id != snapshot.id
        or snapshot.job_id != job.id or snapshot.resource_id != workflow.resource_id
        or job.tenant_id != workflow.tenant_id or task.tenant_id != workflow.tenant_id
        or task.resource_id != workflow.resource_id):
        raise _Stale
    if _status(job) == 'CANCELLED' or (snapshot.extra_data or {}).get('cancelled_at'):
        raise _Cancelled
    if _status(job) not in {'RUNNING', 'QUEUED', 'PENDING', 'RETRYING'} or _status(snapshot) != 'IN_PROGRESS':
        raise _ParentEnded
    leaf = partition if workflow.partition_id else snapshot
    if (leaf is None or (partition is not None and (
            partition.snapshot_id != snapshot.id or partition.job_id != job.id
            or partition.resource_id != workflow.resource_id or partition.tenant_id != workflow.tenant_id))
        or _status(leaf) != 'IN_PROGRESS' or leaf.lease_owner_id is not None
        or leaf.lease_expires_at is not None or leaf.lease_token != workflow.parent_lease_token
        or snapshot.lease_token != workflow.snapshot_lease_token
        or job.lease_token != workflow.job_lease_token):
        raise _Stale
    return task, workflow


async def _try_lock(session, namespace, key):
    return bool(await session.scalar(text('SELECT pg_try_advisory_xact_lock(:namespace,:key)'),
                                     {'namespace': namespace, 'key': key}))


async def _any_permit(session, namespace, keys):
    for key in keys:
        if await _try_lock(session, namespace, key):
            return True
    return False


async def _physical_guards(session, task, workflow, control):
    # Stable order: resource, target, tenant, lane, aggregate. A miss aborts
    # this entire transaction before a separate fenced READY transition.
    if not await _try_lock(session, 420021, _lock_key(workflow.tenant_id, workflow.resource_id)):
        raise _Deferred('copy_resource_busy')
    if not await _try_lock(session, 420019, _lock_key(workflow.tenant_id, workflow.target_snapshot_id)):
        raise _Deferred('copy_target_busy')
    if not await _any_permit(session, 420022,
            (_lock_key(workflow.tenant_id, slot) for slot in range(control.tenant_capacity))):
        raise _Deferred('copy_tenant_busy')
    base = 1000000 if task.lane == 'giant' else 0
    capacity = control.giant_capacity if task.lane == 'giant' else control.micro_capacity
    if not await _any_permit(session, 420018, range(base, base + capacity)):
        raise _Deferred('copy_lane_busy')
    if not await _any_permit(session, 420020, range(control.total_capacity)):
        raise _Deferred('copy_aggregate_busy')


# Same budget as the runtime's control statements: a forced checkpoint stalls every query
# 4-9 s (Railway 2026-11-05 16:45:59: settlement timed out at the former 5 s).
SETTLE_STATEMENT_MS = 30000


async def _settle_once(factory, claim, *, status, error, queued_seconds=0.0, spend_attempt=True, execution_s=0.0):
    """Fresh transaction after rollback; never release a replacement's claim."""
    try:
        async with factory.begin() as session:
            await _settings(session, claim, SETTLE_STATEMENT_MS, 1000)
            try:
                # A proven nonexecution release changes only the owned task. Reading
                # parents is sufficient: the next execution still locks/fences them.
                task, _ = await _owned(session, claim, lock_parents=spend_attempt)
            except _Cancelled:
                # _owned locks and validates the task before reporting cancel.
                task = await session.get(CopyWorkTask, claim.task_id)
                status, error = 'CANCELLED', {'code': 'copy_parent_cancelled'}
            except _ParentEnded:
                task = await session.get(CopyWorkTask, claim.task_id)
                status, error = 'FAILED', {'code': 'copy_parent_terminal'}
            now = await session.scalar(select(func.clock_timestamp()))
            if status == 'DEFERRED':
                previous=(task.error or {}).get('execution_failures',0)
                failures=previous if type(previous) is int and 0<=previous<=3 else 0
                if spend_attempt:
                    if (task.params or {}).get('_copy_execution_budget_v1') is True:
                        # A prior expired claim may have executed but failed to
                        # settle. Its committed reservation survives rollback.
                        failures = max(failures, task.attempts - 1)
                    failures += 1
                if failures:
                    error={**error,'execution_failures':failures}
                    if failures>=3: status='FAILED'
                task.cumulative_wait_s += max(0.0, queued_seconds)
                task.queue_entered_at = now
                task.ready_at = now + timedelta(seconds=1)
                if not spend_attempt:
                    task.attempts = max(0, task.attempts - 1)
                if task.wait_max_s and task.cumulative_wait_s >= task.wait_max_s:
                    status, error = 'FAILED', {'code': 'copy_admission_timeout'}
            task.state = 'READY' if status == 'DEFERRED' else status
            task.execution_s = float(task.execution_s or 0)+max(0.0,execution_s)
            task.error = error
            task.lease_owner_id = task.lease_expires_at = None
        return status
    except _Stale:
        return 'STALE'


async def _settle(factory, claim, **kwargs):
    """Retry only settlement; DEFERRED always means its READY update committed."""
    for attempt in range(3):
        try:
            return await _settle_once(factory, claim, **kwargs)
        except Exception as exc:
            logger.warning('copy_settlement_failed task_id=%s generation=%s sqlstate=%s phase=settlement error_type=%s',
                claim.task_id, claim.generation, first_db_sqlstate(exc), type(exc).__name__)
            if attempt == 2 or first_db_sqlstate(exc) not in {'55P03', '57014', '40P01', '40001'}:
                raise
            await asyncio.sleep(random.uniform(0.05, min(0.5, 0.05 * 2 ** (attempt + 1))))


async def run_batch(factory, claim: CopyClaim, *, batch_size=512,
                    statement_timeout_ms=60000, lock_timeout_ms=5000) -> BatchResult:
    """Run one bounded transaction. SQL locks survive lease expiry or lost workers."""
    if any(type(value) is not int or value < 1 for value in
           (batch_size, statement_timeout_ms, lock_timeout_ms)):
        raise ValueError('batch size and statement/lock timeouts must be positive integers')
    query_seconds = 0.0
    for attempt in range(1, 4):
        admission_started, query_started = monotonic(), None
        phase = 'settings'
        try:
            async with factory.begin() as session:
                await _settings(session, claim, statement_timeout_ms, lock_timeout_ms)
                phase = 'ownership'
                task, workflow = await _owned(session, claim)
                control = await session.get(CopyWorkControl, 'global')
                if control is None or not control.enabled:
                    raise _Deferred('copy_protocol_disabled')
                if task.wait_max_s and task.cumulative_wait_s >= task.wait_max_s:
                    raise _Deferred('copy_admission_timeout')
                phase = 'admission'
                await _physical_guards(session, task, workflow, control)
                if (task.params or {}).get('_copy_execution_budget_v1') is True and task.attempts > 3:
                    # Unsettled expired claims cannot prove nonexecution. Stop
                    # before a fourth execution, without diagnosing a deadlock.
                    raise _ExecutionUnsettled
                phase = 'execution'
                query_started = monotonic()
                if not task.prepared:
                    await copy_executor.prepare_manifest(session, task)
                result = await copy_executor.copy_manifest_batch(session, task, limit=batch_size)
                query_seconds += monotonic() - query_started
                query_started = None
                task.checkpoint = result['cursor']
                task.row_count += result['rows']
                task.bytes_count += result['bytes']
                task.execution_s = float(task.execution_s or 0)+query_seconds
                task.state = 'COMPLETED' if result['done'] else 'READY'
                task.lease_owner_id = task.lease_expires_at = None
                task.error = None
                task.attempts = 0
                if not result['done']:
                    task.ready_at = task.queue_entered_at = await session.scalar(select(func.clock_timestamp()))
                status = task.state
            return BatchResult(status, claim.task_id, result['rows'], result['bytes'], query_seconds)
        except _Stale:
            return BatchResult('STALE', claim.task_id, query_seconds=query_seconds)
        except _ExecutionUnsettled:
            status = await _settle(factory, claim, status='FAILED',
                error={'code': 'copy_execution_unsettled', 'execution_failures': 3},
                execution_s=query_seconds)
            return BatchResult(status, claim.task_id, query_seconds=query_seconds)
        except (_Cancelled, _ParentEnded) as exc:
            status = await _settle(factory, claim, status='CANCELLED' if isinstance(exc, _Cancelled) else 'FAILED',
                error={'code': 'copy_parent_cancelled' if isinstance(exc, _Cancelled) else 'copy_parent_terminal'},execution_s=query_seconds)
            return BatchResult(status, claim.task_id, query_seconds=query_seconds)
        except _Deferred as exc:
            status = await _settle(factory, claim, status='DEFERRED', error={'code': exc.code},
                queued_seconds=monotonic() - admission_started, spend_attempt=False,execution_s=query_seconds)
            return BatchResult(status, claim.task_id, query_seconds=query_seconds)
        except Exception as exc:
            if query_started is not None:
                query_seconds += monotonic() - query_started
            state = first_db_sqlstate(exc)
            logger.warning('copy_batch_failed task_id=%s generation=%s sqlstate=%s phase=%s error_type=%s',
                claim.task_id, claim.generation, state, phase, type(exc).__name__)
            if phase != 'execution' and state in {'55P03', '57014', '40P01', '40001'}:
                code = {'55P03': 'copy_lock_timeout', '57014': 'copy_statement_timeout',
                        '40P01': 'copy_deadlock', '40001': 'copy_serialization_failure'}[state]
                status = await _settle(factory, claim, status='DEFERRED',
                    error={'code': code, 'sqlstate': state, 'phase': phase}, spend_attempt=False,
                    queued_seconds=monotonic() - admission_started, execution_s=query_seconds)
                return BatchResult(status, claim.task_id, query_seconds=query_seconds)
            if state == '57014' and attempt < 3:
                continue
            if state in {'40P01', '40001'}:
                code = 'copy_deadlock' if state == '40P01' else 'copy_serialization_failure'
                status = await _settle(factory, claim, status='DEFERRED',
                    error={'code': code, 'sqlstate': state}, spend_attempt=phase == 'execution', execution_s=query_seconds)
            elif state == '55P03':
                status = await _settle(factory, claim, status='DEFERRED',
                    error={'code': 'copy_lock_timeout', 'sqlstate': state, 'phase': phase},
                    queued_seconds=monotonic() - admission_started if phase != 'execution' else 0,
                    spend_attempt=phase == 'execution', execution_s=query_seconds)
            else:
                code = 'copy_statement_timeout' if state == '57014' else 'copy_execution_failed'
                error = {'code': code}
                if state:
                    error.update(sqlstate=state, attempts=attempt)
                status = await _settle(factory, claim, status='FAILED', error=error,execution_s=query_seconds)
            return BatchResult(status, claim.task_id, query_seconds=query_seconds)


async def enable_copy_protocol(factory, *, micro_capacity, giant_capacity, total_capacity, tenant_capacity):
    """Activate after an administrative pause/drain of every legacy writer.

    Database checks prove no old SQL is running now; they cannot prove that an
    old process will never submit more work after this transaction commits.
    """
    capacities = (micro_capacity, giant_capacity, total_capacity, tenant_capacity)
    if (any(type(value) is not int or value < 1 for value in capacities)
        or micro_capacity > max(1, CARRY_FORWARD_BUCKETS)
        or giant_capacity > max(1, GIANT_CARRY_FORWARD_BUCKETS)
        or total_capacity > micro_capacity + giant_capacity or tenant_capacity > total_capacity):
        raise ValueError('copy capacity exceeds the verified legacy limits or aggregate budget')
    async with factory.begin() as session:
        schema_ready=await session.scalar(text("""SELECT EXISTS(SELECT 1 FROM pg_trigger
            WHERE tgrelid='copy_workflows'::regclass AND tgname='copy_workflow_delete_guard'
              AND NOT tgisinternal AND tgenabled <> 'D')"""))
        if not schema_ready:
            raise RuntimeError('copy-work migration delete guard is not installed/enabled')
        await session.execute(insert(CopyWorkControl).values(id='global').on_conflict_do_nothing())
        control = await session.scalar(select(CopyWorkControl).where(CopyWorkControl.id == 'global').with_for_update(nowait=True))
        active = await session.scalar(select(func.count()).select_from(CopyWorkTask)
            .where(CopyWorkTask.state.in_(['READY', 'RUNNING'])))
        active += await session.scalar(select(func.count()).select_from(CopyWorkflow)
            .where(CopyWorkflow.state.in_(ACTIVE_WORKFLOWS) | CopyWorkflow.parent_settle_pending))
        if active:
            raise RuntimeError('active copy work must drain before protocol activation')
        legacy = await session.scalar(text("""SELECT EXISTS(SELECT 1 FROM pg_locks
            WHERE locktype='advisory' AND classid=420018 AND objsubid=2 AND granted
              AND database=(SELECT oid FROM pg_database WHERE datname=current_database()))"""))
        if legacy:
            raise RuntimeError('legacy copy SQL still holds physical capacity')
        for key in list(range(max(1, CARRY_FORWARD_BUCKETS))) + list(range(1000000, 1000000 + max(1, GIANT_CARRY_FORWARD_BUCKETS))):
            if not await _try_lock(session, 420018, key):
                raise RuntimeError('legacy copy SQL raced protocol activation')
        control.micro_capacity, control.giant_capacity, control.total_capacity, control.tenant_capacity = capacities
        control.epoch += 1
        control.enabled = True
        epoch = control.epoch
    return epoch
