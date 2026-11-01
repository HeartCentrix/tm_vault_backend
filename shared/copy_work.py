"""Authoritative copy work, idempotent enqueue and durable policy fairness."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
import hashlib
import json
from uuid import UUID, uuid4

from sqlalchemy import exists, func, select
from sqlalchemy.dialects.postgresql import insert

from shared.models import (CopyWorkflow, CopyWorkTask, CopyWorkQueue, CopyWorkControl,
                           Job, Resource, Snapshot, SnapshotPartition, SlaPolicy)

ACTIVE_WORKFLOWS = ('DRAINING', 'WAITING', 'RESUMING')
TERMINAL_TASKS = ('COMPLETED', 'FAILED', 'CANCELLED')
KINDS = {'CHAT_CARRY', 'MAIL_CARRY', 'FILE_VERSIONS', 'LEGACY_CHAT_ATTACHMENTS', 'CHAT_RENAME', 'ARCHIVE_CARRY'}


class CopyWorkConflict(RuntimeError):
    """Copy ownership or parent state no longer permits this operation."""


@dataclass(frozen=True)
class CopyRequest:
    kind: str
    lane: str
    target_snapshot_id: UUID | str
    params: dict = field(default_factory=dict)
    partition_id: UUID | None = None
    sla_policy_id: UUID | None = None
    weight: int = 1


@dataclass(frozen=True)
class CopyClaim:
    task_id: UUID
    owner_id: UUID
    generation: int


def _uuid(value):
    return UUID(str(value)) if value is not None else None


def _key(*parts):
    return hashlib.sha256(json.dumps(parts, sort_keys=True, default=str, separators=(',', ':')).encode()).hexdigest()


async def _parent(session, snapshot_id):
    row = (await session.execute(select(Snapshot, Resource, Job).join(Resource, Resource.id == Snapshot.resource_id)
        .join(Job, Job.id == Snapshot.job_id).where(Snapshot.id == _uuid(snapshot_id)))).one_or_none()
    if row is None:
        raise CopyWorkConflict('copy target has no owned parent')
    snapshot, resource, job = row
    if str(job.status.value) not in ('RUNNING', 'QUEUED', 'PENDING', 'RETRYING') or snapshot.status.value != 'IN_PROGRESS':
        raise CopyWorkConflict('copy parent is no longer active')
    if job.tenant_id != resource.tenant_id:
        raise CopyWorkConflict('copy parent tenant mismatch')
    return snapshot, resource, job


async def enqueue(session, request: CopyRequest) -> CopyWorkTask:
    if request.kind not in KINDS or request.lane not in ('micro', 'giant'):
        raise ValueError('unknown copy kind/lane')
    if type(request.weight) is not int or not 1 <= request.weight <= 8:
        raise ValueError('copy scheduling weight must be between 1 and 8')
    snapshot, resource, job = await _parent(session, request.target_snapshot_id)
    policy_id = _uuid(request.sla_policy_id or resource.sla_policy_id)
    if policy_id is not None:
        policy = await session.get(SlaPolicy, policy_id)
        if policy is None or policy.tenant_id != resource.tenant_id:
            raise CopyWorkConflict('copy SLA policy tenant mismatch')
    partition_id = _uuid(request.partition_id)
    if partition_id:
        partition = await session.get(SnapshotPartition, partition_id)
        if partition is None or partition.snapshot_id != snapshot.id or partition.status != 'IN_PROGRESS':
            raise CopyWorkConflict('copy partition is no longer active')
    leaf_key = 'partition:' + str(partition_id) if partition_id else 'snapshot:' + str(snapshot.id)
    await session.execute(insert(CopyWorkflow).values(id=uuid4(), leaf_key=leaf_key, tenant_id=resource.tenant_id,
        resource_id=resource.id, job_id=job.id, target_snapshot_id=snapshot.id, partition_id=partition_id,
        state='DRAINING', generation=0, parent_lease_token=partition.lease_token if partition_id else snapshot.lease_token,
        snapshot_lease_token=snapshot.lease_token, job_lease_token=job.lease_token, continuation={})
        .on_conflict_do_nothing(index_elements=['leaf_key']))
    workflow = await session.scalar(select(CopyWorkflow).where(CopyWorkflow.leaf_key == leaf_key))
    if workflow.state != 'DRAINING':
        raise CopyWorkConflict('copy workflow already sealed')
    from shared.database import CARRY_FORWARD_WAIT_MAX_S
    params = {**request.params, 'target_snapshot_id': str(snapshot.id)}
    digest = _key(workflow.id, request.kind, params)
    tenant_key = _key('TENANT', resource.tenant_id, request.lane)
    policy_key = _key('POLICY', resource.tenant_id, policy_id, request.lane)
    resource_key = _key('RESOURCE', resource.tenant_id, policy_id, resource.id, request.lane)
    for key, scope, parent, weight, rid in ((tenant_key, 'TENANT', None, 1, None),
            (policy_key, 'POLICY', tenant_key, request.weight, None),
            (resource_key, 'RESOURCE', policy_key, 1, resource.id)):
        await session.execute(insert(CopyWorkQueue).values(key=key, scope=scope, parent_key=parent,
            tenant_id=resource.tenant_id, sla_policy_id=policy_id, resource_id=rid,
            lane=request.lane, weight=weight, virtual_finish=0).on_conflict_do_nothing(index_elements=['key']))
    await session.execute(insert(CopyWorkTask).values(id=uuid4(), workflow_id=workflow.id,
        tenant_id=resource.tenant_id, resource_id=resource.id, sla_policy_id=policy_id,
        queue_key=policy_key, resource_queue_key=resource_key, tenant_queue_key=tenant_key,
        idempotency_key=digest, kind=request.kind, lane=request.lane, params=params, state='READY',
        wait_max_s=max(0, CARRY_FORWARD_WAIT_MAX_S), prepared=False, checkpoint=0,
        row_count=0, bytes_count=0, generation=0, cumulative_wait_s=0, attempts=0)
        .on_conflict_do_nothing(index_elements=['idempotency_key']))
    return await session.scalar(select(CopyWorkTask).where(CopyWorkTask.idempotency_key == digest))


async def claim(session, *, owner_id, lease_seconds=90) -> CopyClaim | None:
    # Serializes only short scheduling decisions, never execution or waiting.
    await session.execute(insert(CopyWorkControl).values(id='global').on_conflict_do_nothing())
    control = await session.scalar(select(CopyWorkControl).where(CopyWorkControl.id == 'global').with_for_update())
    now = await session.scalar(select(func.clock_timestamp()))
    candidates = select(CopyWorkTask).join(CopyWorkflow, CopyWorkflow.id == CopyWorkTask.workflow_id).where(
        CopyWorkflow.state == 'WAITING',
        ((CopyWorkTask.state == 'READY') & (CopyWorkTask.ready_at <= now)) |
        ((CopyWorkTask.state == 'RUNNING') & (CopyWorkTask.lease_expires_at < now)))
    active = candidates.subquery()
    chosen = []
    parent = None
    for scope, column in (('TENANT', active.c.tenant_queue_key), ('POLICY', active.c.queue_key),
                          ('RESOURCE', active.c.resource_queue_key)):
        stmt = select(CopyWorkQueue).where(CopyWorkQueue.scope == scope,
            CopyWorkQueue.key.in_(select(column))).order_by(CopyWorkQueue.virtual_finish, CopyWorkQueue.key).limit(1)
        if parent is not None:
            stmt = stmt.where(CopyWorkQueue.parent_key == parent)
        queue = await session.scalar(stmt)
        if queue is None:
            return None
        chosen.append(queue)
        parent = queue.key
    task = await session.scalar(candidates.where(CopyWorkTask.resource_queue_key == parent)
        .order_by(CopyWorkTask.created_at, CopyWorkTask.id).limit(1).with_for_update(of=CopyWorkTask, skip_locked=True))
    if task is None:
        return None
    workflow = await session.get(CopyWorkflow, task.workflow_id)
    await _parent(session, workflow.target_snapshot_id)
    if task.state == 'READY':
        task.cumulative_wait_s += max(0, (now - task.queue_entered_at).total_seconds())
    if task.wait_max_s and task.cumulative_wait_s >= task.wait_max_s:
        task.state, task.error = 'FAILED', {'code': 'copy_admission_timeout'}
        return None
    for queue in chosen:
        queue.virtual_finish += 1 / queue.weight
    task.state = 'RUNNING'
    task.generation += 1
    task.lease_owner_id = _uuid(owner_id)
    task.lease_expires_at = now + timedelta(seconds=lease_seconds)
    task.attempts += 1
    await session.flush()
    return CopyClaim(task.id, task.lease_owner_id, task.generation)
