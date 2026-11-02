"""Database-only provider continuation and fencing; no external I/O here."""
from __future__ import annotations
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import timedelta
import hashlib
import json
import math
import os
from uuid import UUID, uuid4
from sqlalchemy import exists, func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import aliased
from shared.models import (Job, Snapshot, SnapshotPartition, Resource, SlaPolicy,
    ProviderWork, ProviderWorkUnit, ProviderWorkQueue, ProviderWorkControl)

ACTIVE = ('DRAINING', 'WAITING', 'RESUMING')
TERMINAL = ('COMPLETED', 'PARTIAL', 'FAILED', 'CANCELLED')
_CURRENT = ContextVar('provider_work_claim', default=None)
_SETTLING = ContextVar('provider_parent_settlement', default=False)
_GUARDS = False

class ProviderWorkConflict(RuntimeError):
    pass

@dataclass(frozen=True)
class ProviderClaim:
    work_id: UUID
    snapshot_id: UUID
    partition_id: UUID | None
    job_id: UUID
    owner_id: UUID | None
    generation: int
    parent_token: int
    snapshot_token: int
    job_token: int


def enabled():
    return os.getenv('PROVIDER_WORK_ENABLED', 'false').lower() in ('true', '1', 'yes', 'on')


def guards_enabled():
    return _GUARDS or enabled() or _CURRENT.get() is not None


async def initialize_guards(factory):
    global _GUARDS
    from sqlalchemy.exc import SQLAlchemyError
    from shared.db_retry import _chain
    try:
        async with factory() as session:
            pending = await session.scalar(select(exists(select(ProviderWork.id).where(
                ProviderWork.state.in_(ACTIVE) | ProviderWork.parent_settle_pending))))
            control = await session.get(ProviderWorkControl, 'global')
            _GUARDS = _GUARDS or enabled() or bool(pending or (control and control.enabled))
    except SQLAlchemyError as exc:
        if enabled() or not any(getattr(e, 'sqlstate', None) == '42P01' for e in _chain(exc)):
            raise
    return guards_enabled()


@contextmanager
def work_scope(claim):
    token = _CURRENT.set(claim)
    try:
        yield claim
    finally:
        _CURRENT.reset(token)


@contextmanager
def settling_parent():
    token = _SETTLING.set(True)
    try:
        yield
    finally:
        _SETTLING.reset(token)


def current_claim():
    return _CURRENT.get()


def _uuid(value):
    return UUID(str(value)) if value is not None else None


def _key(*values):
    return hashlib.sha256(json.dumps(values, default=str, separators=(',', ':')).encode()).hexdigest()


def _bounded(value):
    encoded = json.dumps(value, allow_nan=False, separators=(',', ':'))
    if len(encoded.encode()) > 8192:
        raise ValueError('provider continuation must be bounded; use unit rows')
    return value


def _receipt(row):
    return ProviderClaim(row.id, row.snapshot_id, row.partition_id, row.job_id,
        row.lease_owner_id, row.generation, row.parent_lease_token,
        row.snapshot_lease_token, row.job_lease_token)


async def _parents(session, snapshot_id, partition_id=None, *, skip_locked=False):
    hint = await session.get(Snapshot, _uuid(snapshot_id))
    if hint is None:
        raise ProviderWorkConflict('provider snapshot removed')
    job = await session.scalar(select(Job).where(Job.id == hint.job_id).with_for_update(skip_locked=skip_locked)
        .execution_options(populate_existing=True))
    snapshot = await session.scalar(select(Snapshot).where(Snapshot.id == hint.id).with_for_update(skip_locked=skip_locked)
        .execution_options(populate_existing=True))
    partition = await session.scalar(select(SnapshotPartition).where(SnapshotPartition.id == _uuid(partition_id))
        .with_for_update(skip_locked=skip_locked).execution_options(populate_existing=True)) if partition_id else None
    if job is None or snapshot is None or (partition_id and partition is None):
        raise ProviderWorkConflict('provider parent locked or removed')
    return job, snapshot, partition


def _active(job, snapshot, partition):
    return (job.status.value in ('RUNNING', 'QUEUED', 'PENDING', 'RETRYING') and
        snapshot.status.value == 'IN_PROGRESS' and (partition is None or partition.status in ('IN_PROGRESS', 'QUEUED')))


async def ensure_work(session, *, snapshot_id, snapshot_token, job_token, partition_id=None,
                      partition_token=None, weight=None, policy_id=None, continuation=None):
    job, snapshot, partition = await _parents(session, snapshot_id, partition_id)
    resource = await session.get(Resource, snapshot.resource_id)
    leaf = partition or snapshot
    if (not _active(job, snapshot, partition) or resource is None or resource.tenant_id != job.tenant_id
        or (partition and (partition.snapshot_id != snapshot.id or partition.tenant_id != job.tenant_id))
        or (partition is not None and partition_token != partition.lease_token)
        or (partition is None and (snapshot_token != snapshot.lease_token or job_token != job.lease_token))):
        raise ProviderWorkConflict('provider parent ownership changed')
    leaf_key = ('partition:' + str(partition.id)) if partition else ('snapshot:' + str(snapshot.id))
    existing = await session.scalar(select(ProviderWork).where(ProviderWork.leaf_key == leaf_key).with_for_update())
    if existing is not None:
        return _receipt(existing)
    weight = max(1, min(8, 6 - int(job.priority or 5))) if weight is None else weight
    if type(weight) is not int or weight <= 0:
        raise ValueError('provider policy needs a positive weight')
    policy_id = _uuid(policy_id or resource.sla_policy_id)
    if policy_id:
        policy = await session.get(SlaPolicy, policy_id)
        if policy is None or policy.tenant_id != resource.tenant_id:
            raise ProviderWorkConflict('provider SLA tenant mismatch')
    tenant_key = _key('tenant', resource.tenant_id)
    # Effective weight is immutable, including the no-policy priority fallback.
    policy_key = _key('policy', tenant_key, policy_id, weight)
    resource_key = _key('resource', policy_key, resource.id, job.id)
    for key, scope, parent, share in ((tenant_key, 'TENANT', None, 1),
            (policy_key, 'POLICY', tenant_key, weight), (resource_key, 'RESOURCE', policy_key, 1)):
        floor = await session.scalar(select(func.coalesce(func.max(ProviderWorkQueue.virtual_finish), 0))
            .where(ProviderWorkQueue.parent_key.is_not_distinct_from(parent)))
        await session.execute(insert(ProviderWorkQueue).values(key=key, scope=scope, parent_key=parent,
            tenant_id=resource.tenant_id, weight=share, virtual_finish=float(floor)).on_conflict_do_nothing())
    await session.execute(insert(ProviderWorkControl).values(id='global', enabled=True).on_conflict_do_nothing())
    row = ProviderWork(id=uuid4(), leaf_key=leaf_key, tenant_id=resource.tenant_id,
        resource_id=resource.id, job_id=job.id, snapshot_id=snapshot.id,
        partition_id=partition.id if partition else None, sla_policy_id=policy_id, weight=weight,
        tenant_queue_key=tenant_key, policy_queue_key=policy_key, resource_queue_key=resource_key,
        parent_lease_token=leaf.lease_token, snapshot_lease_token=snapshot.lease_token,
        job_lease_token=job.lease_token, continuation=_bounded(continuation or {}), state='WAITING')
    session.add(row)
    # The durable row is the outbox. No broker receipt is required for recovery.
    leaf.lease_owner_id = None
    leaf.lease_expires_at = None
    await session.flush()
    return _receipt(row)


async def _owned(session, claim, *, allow_terminal=False):
    job, snapshot, partition = await _parents(session, claim.snapshot_id, claim.partition_id)
    row = await session.scalar(select(ProviderWork).where(ProviderWork.id == claim.work_id).with_for_update()
        .execution_options(populate_existing=True))
    leaf = partition or snapshot
    now = await session.scalar(select(func.clock_timestamp()))
    if (row is None or row.state not in ('DRAINING', 'RESUMING') or row.generation != claim.generation
        or row.lease_owner_id != claim.owner_id or row.lease_expires_at is None or row.lease_expires_at <= now
        or (not _active(job, snapshot, partition) and not (allow_terminal and job.status.value != 'CANCELLED'
            and (leaf.status.value if hasattr(leaf.status, 'value') else leaf.status) in ('COMPLETED', 'PARTIAL', 'FAILED')))
        or leaf.lease_token != claim.parent_token
        or leaf.lease_owner_id != claim.owner_id
        or (partition is None and (snapshot.lease_token != claim.snapshot_token or job.lease_token != claim.job_token))):
        raise ProviderWorkConflict('provider work ownership changed')
    return row, job, snapshot, partition


async def claim_due_work(session, *, owner_id, lease_seconds=120):
    if lease_seconds <= 0:
        raise ValueError('provider lease must be positive')
    await session.execute(insert(ProviderWorkControl).values(id='global', enabled=enabled()).on_conflict_do_nothing())
    control = await session.scalar(select(ProviderWorkControl).where(ProviderWorkControl.id == 'global').with_for_update())
    if not control.enabled:
        return None
    now = await session.scalar(select(func.clock_timestamp()))
    running = (ProviderWork.state.in_(('DRAINING', 'RESUMING')) & (ProviderWork.lease_expires_at > now))
    if (await session.scalar(select(func.count()).select_from(ProviderWork).where(running))) >= control.total_capacity:
        return None
    tq, pq, rq = (aliased(ProviderWorkQueue) for _ in range(3))
    due = (((ProviderWork.state == 'WAITING') & (ProviderWork.next_eligible_at <= now)) |
        (ProviderWork.state.in_(('DRAINING', 'RESUMING')) & (ProviderWork.lease_expires_at <= now)))
    candidates = (await session.scalars(select(ProviderWork).join(tq, tq.key == ProviderWork.tenant_queue_key)
        .join(pq, pq.key == ProviderWork.policy_queue_key).join(rq, rq.key == ProviderWork.resource_queue_key)
        .where(due, ProviderWork.protocol_version == control.epoch, ProviderWork.tenant_id.notin_(
            select(ProviderWork.tenant_id).where(running).group_by(ProviderWork.tenant_id)
                .having(func.count() >= control.tenant_capacity)))
        .order_by(tq.virtual_finish, pq.virtual_finish, rq.virtual_finish, ProviderWork.queue_entered_at, ProviderWork.id)
        .limit(64))).all()
    for hint in candidates:
        if (await session.scalar(select(func.count()).select_from(ProviderWork).where(running, ProviderWork.tenant_id == hint.tenant_id))) >= control.tenant_capacity:
            continue
        try:
            job, snapshot, partition = await _parents(session, hint.snapshot_id, hint.partition_id, skip_locked=True)
        except ProviderWorkConflict:
            continue
        row = await session.scalar(select(ProviderWork).where(ProviderWork.id == hint.id).with_for_update(skip_locked=True)
            .execution_options(populate_existing=True))
        if row is None:
            continue
        if not _active(job, snapshot, partition):
            terminal = (partition.status if partition else snapshot.status.value) in ('COMPLETED', 'PARTIAL', 'FAILED')
            row.state = 'COMPLETED' if terminal and job.status.value != 'CANCELLED' else 'CANCELLED'
            row.parent_settle_pending = row.state == 'COMPLETED'
            row.generation += 1
            continue
        if row.state not in ACTIVE or (row.lease_expires_at and row.lease_expires_at > now and row.state != 'WAITING'):
            continue
        leaf = partition or snapshot
        # A copy continuation owns its distinct drain until its receipts settle.
        from shared.models import CopyWorkflow
        copy = await session.scalar(select(CopyWorkflow).where(CopyWorkflow.target_snapshot_id == snapshot.id,
            CopyWorkflow.partition_id.is_not_distinct_from(row.partition_id), CopyWorkflow.state.in_(ACTIVE)).with_for_update())
        if copy is not None and copy.state != 'DRAINING':
            continue
        leaf.lease_token += 1
        leaf.lease_owner_id, leaf.lease_expires_at = _uuid(owner_id), now + timedelta(seconds=lease_seconds)
        if partition:
            partition.status = 'IN_PROGRESS'
        snapshot.extra_data = {**(snapshot.extra_data or {}), 'provider_phase': 'RUNNING', 'provider_next_eligible_at': None}
        row.state, row.lease_owner_id, row.lease_expires_at = 'RESUMING', leaf.lease_owner_id, leaf.lease_expires_at
        row.generation += 1
        row.parent_lease_token, row.snapshot_lease_token, row.job_lease_token = leaf.lease_token, snapshot.lease_token, job.lease_token
        if copy is not None:
            copy.parent_lease_token, copy.snapshot_lease_token, copy.job_lease_token = row.parent_lease_token, row.snapshot_lease_token, row.job_lease_token
        for key in (row.tenant_queue_key, row.policy_queue_key, row.resource_queue_key):
            queue = await session.get(ProviderWorkQueue, key)
            queue.virtual_finish += 1 / queue.weight
        await session.flush()
        return _receipt(row)
    return None


async def renew_work(session, claim, *, lease_seconds=120):
    row, _, snapshot, partition = await _owned(session, claim)
    expiry = await session.scalar(select(func.clock_timestamp())) + timedelta(seconds=lease_seconds)
    row.lease_expires_at = (partition or snapshot).lease_expires_at = expiry
    return claim


async def record_unit_checkpoint(session, claim, *, unit_key, kind, state='PENDING', checkpoint=None,
        receipt_id=None, source_version=None, attempts=0, waited_s=0, reconciliations=0, deadline_at=None):
    await _owned(session, claim, allow_terminal=state in ('COMPLETED', 'FAILED'))
    if (not unit_key or len(unit_key) > 128 or state not in ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED')
        or type(attempts) is not int or attempts < 0 or not math.isfinite(waited_s) or waited_s < 0
        or type(reconciliations) is not int or reconciliations < 0):
        raise ValueError('invalid provider unit checkpoint')
    now = await session.scalar(select(func.clock_timestamp()))
    row = await session.scalar(select(ProviderWorkUnit).where(ProviderWorkUnit.work_id == claim.work_id,
        ProviderWorkUnit.unit_key == unit_key).with_for_update())
    if row is None:
        row = ProviderWorkUnit(id=uuid4(), work_id=claim.work_id, unit_key=unit_key, kind=kind,
            source_version=source_version, attempts=0, waited_s=0, reconciliations=0,
            deadline_at=deadline_at or now + timedelta(seconds=float(os.getenv('PROVIDER_UNIT_MAX_AGE_S', '604800'))))
        session.add(row)
    elif row.kind != kind or row.source_version != source_version:
        raise ProviderWorkConflict('provider unit identity changed')
    if row.state == 'COMPLETED':
        if state == 'COMPLETED' and receipt_id is not None and row.receipt_id != receipt_id:
            raise ProviderWorkConflict('provider receipt changed')
        return row
    row.state = state
    row.attempts += attempts
    row.waited_s += waited_s
    row.reconciliations += reconciliations
    if checkpoint is not None:
        row.checkpoint = _bounded(checkpoint)
    if receipt_id is not None:
        row.receipt_id = receipt_id
    await session.flush()
    return row


async def suspend_current_work(session, claim, *, next_eligible_at, reason, continuation=None):
    # Lost commit acknowledgement re-reads the deterministic suspension receipt.
    hint = await session.get(ProviderWork, claim.work_id)
    if hint and hint.state == 'WAITING' and hint.suspension_generation == claim.generation:
        return _receipt(hint)
    row, job, snapshot, partition = await _owned(session, claim)
    leaf = partition or snapshot
    job.result = {**(job.result or {}), 'provider_phase': 'WAITING', 'provider_next_eligible_at': next_eligible_at.isoformat()}
    snapshot.extra_data = {**(snapshot.extra_data or {}), 'provider_phase': 'WAITING', 'provider_next_eligible_at': next_eligible_at.isoformat()}
    row.state, row.next_eligible_at = 'WAITING', next_eligible_at
    row.suspension_generation = claim.generation
    row.last_reason = str(reason)[:80]
    row.generation += 1
    if continuation is not None:
        row.continuation = _bounded(continuation)
    leaf.lease_token += 1
    row.parent_lease_token = leaf.lease_token
    row.snapshot_lease_token = snapshot.lease_token
    row.lease_owner_id = leaf.lease_owner_id = None
    row.lease_expires_at = leaf.lease_expires_at = None
    await session.flush()
    return _receipt(row)


async def finish_work(session, claim, *, state='COMPLETED'):
    row, _, _, _ = await _owned(session, claim, allow_terminal=True)
    if state not in ('COMPLETED', 'PARTIAL', 'FAILED'):
        raise ValueError('invalid provider completion')
    pending = await session.scalar(select(exists(select(ProviderWorkUnit.id).where(
        ProviderWorkUnit.work_id == row.id, ProviderWorkUnit.state.in_(('PENDING', 'RUNNING'))))))
    if pending:
        raise ProviderWorkConflict('provider work still has pending units')
    row.state, row.parent_settle_pending = state, True
    row.completed_at = await session.scalar(select(func.clock_timestamp()))
    row.lease_owner_id, row.lease_expires_at = None, None
    await session.flush()


async def finish_parent_settlement(session, work_id):
    hint = await session.get(ProviderWork, _uuid(work_id))
    if hint is None:
        return False
    await _parents(session, hint.snapshot_id, hint.partition_id)
    row = await session.scalar(select(ProviderWork).where(ProviderWork.id == hint.id).with_for_update())
    if row.state not in TERMINAL:
        raise ProviderWorkConflict('provider work has not finished')
    row.parent_settle_pending = False
    return True


def _guard_claim():
    from shared.provider_runtime import current_execution
    execution = current_execution()
    return None if execution and execution.deferred else _CURRENT.get()


def pending_provider_sql(snapshot_expr=None, partition_expr=None, job_expr=None):
    if not guards_enabled():
        return 'TRUE'
    conditions = []
    for column, value in (('snapshot_id', snapshot_expr), ('partition_id', partition_expr), ('job_id', job_expr)):
        if value is not None:
            conditions.append('pw.' + column + '=' + value)
    if not conditions:
        raise ValueError('provider guard needs parent scope')
    active = "pw.state IN ('DRAINING','WAITING','RESUMING')"
    if not _SETTLING.get():
        active = '(' + active + ' OR pw.parent_settle_pending)'
    own = _guard_claim()
    exclusion = " AND pw.id <> '" + str(own.work_id) + "'::uuid" if own else ''
    predicate = 'NOT EXISTS (SELECT 1 FROM provider_work pw WHERE ' + active + ' AND ' + ' AND '.join(conditions) + exclusion + ')'
    if own:
        predicate += " AND EXISTS (SELECT 1 FROM provider_work ow WHERE ow.id='" + str(own.work_id) + "'::uuid AND ow.generation=" + str(own.generation) + " AND ow.state='RESUMING' AND ow.lease_owner_id='" + str(own.owner_id) + "'::uuid AND ow.lease_expires_at > NOW())"
    return '(' + predicate + ')'


async def has_pending_provider(session, *, snapshot_id=None, partition_id=None, job_id=None):
    # Explicit calls are useful before initialization; SQL integration is gated.
    conditions = []
    for column, value in ((ProviderWork.snapshot_id, snapshot_id), (ProviderWork.partition_id, partition_id), (ProviderWork.job_id, job_id)):
        if value is not None:
            conditions.append(column == _uuid(value))
    if not conditions:
        raise ValueError('provider guard needs parent scope')
    own = _guard_claim()
    if own:
        valid = await session.scalar(select(ProviderWork.id).where(ProviderWork.id == own.work_id,
            ProviderWork.generation == own.generation, ProviderWork.state == 'RESUMING',
            ProviderWork.lease_owner_id == own.owner_id, ProviderWork.lease_expires_at > func.clock_timestamp()))
        if valid is None:
            return True
        conditions.append(ProviderWork.id != own.work_id)
    active = ProviderWork.state.in_(ACTIVE)
    if not _SETTLING.get():
        active = active | ProviderWork.parent_settle_pending
    return bool(await session.scalar(select(exists(select(ProviderWork.id).where(active, *conditions)))))


async def cancel_provider_work(session, *, snapshot_id=None, partition_id=None, job_id=None, reason='cancelled'):
    conditions = []
    for column, value in ((ProviderWork.snapshot_id, snapshot_id), (ProviderWork.partition_id, partition_id), (ProviderWork.job_id, job_id)):
        if value is not None:
            conditions.append(column == _uuid(value))
    if not conditions:
        raise ValueError('provider cancellation needs parent scope')
    rows = (await session.scalars(select(ProviderWork).where(ProviderWork.state.in_(ACTIVE) | ProviderWork.parent_settle_pending,
        *conditions).order_by(ProviderWork.job_id, ProviderWork.snapshot_id, ProviderWork.id))).all()
    for hint in rows:
        _, snapshot, partition = await _parents(session, hint.snapshot_id, hint.partition_id)
        row = await session.scalar(select(ProviderWork).where(ProviderWork.id == hint.id).with_for_update())
        row.state, row.parent_settle_pending, row.last_reason = 'CANCELLED', False, str(reason)[:80]
        row.generation += 1
        leaf = partition or snapshot
        if leaf.lease_token == row.parent_lease_token:
            leaf.lease_token += 1
            leaf.lease_owner_id = leaf.lease_expires_at = None
        row.lease_owner_id = row.lease_expires_at = None
        await session.execute(update(ProviderWorkUnit).where(ProviderWorkUnit.work_id == row.id,
            ProviderWorkUnit.state.in_(('PENDING', 'RUNNING'))).values(state='CANCELLED'))
    return len(rows)
