"""Authoritative copy work, idempotent enqueue and durable policy fairness."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
import hashlib
import json
from uuid import UUID, uuid4

from sqlalchemy import exists, func, select
from sqlalchemy.orm import aliased
from sqlalchemy.dialects.postgresql import insert

from shared.models import (CopyWorkflow, CopyWorkTask, CopyWorkInput, CopyWorkQueue, CopyWorkControl,
                           Job, Resource, Snapshot, SnapshotPartition, SlaPolicy)

ACTIVE_WORKFLOWS = ('DRAINING', 'WAITING', 'RESUMING')
TERMINAL_TASKS = ('COMPLETED', 'FAILED', 'CANCELLED')
KINDS = {'CHAT_CARRY', 'MAIL_CARRY', 'FILE_VERSIONS', 'LEGACY_CHAT_ATTACHMENTS', 'CHAT_RENAME', 'CHAT_LABELS', 'CHAT_PRESENTATION', 'ARCHIVE_CARRY'}


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
    weight: int | None = None


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
    if request.weight is not None and (type(request.weight) is not int or not 1 <= request.weight <= 8):
        raise ValueError('copy scheduling weight must be between 1 and 8')
    snapshot, resource, job = await _parent(session, request.target_snapshot_id)
    if request.kind in {'CHAT_LABELS', 'CHAT_PRESENTATION'}:
        from shared.chat_presentation_work import validate_labels
        from shared.models import ResourceType
        if (snapshot.extra_data or {}).get('chat_inventory_format') == 1:
            raise CopyWorkConflict('catalogue snapshots already own their captured chat labels')
        if request.kind == 'CHAT_LABELS':
            validate_labels(request.params.get('labels'))
        elif request.partition_id is not None:
            raise CopyWorkConflict('chat presentation publication belongs to the snapshot')
        if resource.type != ResourceType.USER_CHATS:
            raise CopyWorkConflict('chat labels require a chat resource')
    # Existing job priority is numeric (1 is urgent); policy names never
    # determine eligibility. Every automatic fallback retains a positive share.
    scheduling_weight = request.weight if request.weight is not None else max(1, min(8, 6-int(job.priority or 5)))
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
    # Serialize source choice without a parent/cancellation lock-order cycle.
    # NO KEY UPDATE on the workflow remains compatible with child FK checks.
    job = await session.scalar(select(Job).where(Job.id == job.id).with_for_update(read=True)
                               .execution_options(populate_existing=True))
    snapshot = await session.scalar(select(Snapshot).where(Snapshot.id == snapshot.id).with_for_update(read=True)
                                    .execution_options(populate_existing=True))
    if partition_id:
        partition = await session.scalar(select(SnapshotPartition).where(SnapshotPartition.id == partition_id)
            .with_for_update(read=True).execution_options(populate_existing=True))
    if (job.status.value not in ('RUNNING', 'QUEUED', 'PENDING', 'RETRYING')
        or snapshot.status.value != 'IN_PROGRESS' or (partition_id and partition.status != 'IN_PROGRESS')):
        raise CopyWorkConflict('copy parent no longer permits source registration')
    leaf_key = 'partition:' + str(partition_id) if partition_id else 'snapshot:' + str(snapshot.id)
    await session.execute(insert(CopyWorkflow).values(id=uuid4(), leaf_key=leaf_key, tenant_id=resource.tenant_id,
        resource_id=resource.id, job_id=job.id, target_snapshot_id=snapshot.id, partition_id=partition_id,
        state='DRAINING', generation=0, parent_lease_token=partition.lease_token if partition_id else snapshot.lease_token,
        snapshot_lease_token=snapshot.lease_token, job_lease_token=job.lease_token, continuation={})
        .on_conflict_do_nothing(index_elements=['leaf_key']))
    workflow = await session.scalar(select(CopyWorkflow).where(CopyWorkflow.leaf_key == leaf_key)
        .with_for_update(key_share=True).execution_options(populate_existing=True))
    leaf = partition if partition_id else snapshot
    if workflow.state in ('FAILED','CANCELLED') and leaf.lease_token > workflow.parent_lease_token:
        # Retain old task diagnostics, but never replay an abandoned drain's
        # intents as part of its new owner's source observation.
        workflow.capture_attempt += 1
        workflow.generation += 1
        workflow.state='DRAINING'
        workflow.parent_lease_token=leaf.lease_token
        workflow.snapshot_lease_token=snapshot.lease_token
        workflow.job_lease_token=job.lease_token
        workflow.continuation={}
        workflow.completed_at=None
    if workflow.state != 'DRAINING' or leaf.lease_token != workflow.parent_lease_token:
        raise CopyWorkConflict('copy workflow already sealed')
    if request.kind in {'CHAT_LABELS', 'CHAT_PRESENTATION'}:
        from shared.models import ChatPresentationPlan
        if partition_id and not {label['chat_id'] for label in request.params['labels']}.issubset(
                set((partition.payload or {}).get('chat_ids', []))):
            raise CopyWorkConflict('chat label is outside the source partition')
        await session.execute(insert(ChatPresentationPlan).values(snapshot_id=snapshot.id,
            state='PENDING', published_count=0).on_conflict_do_nothing())
        plan = await session.get(ChatPresentationPlan, snapshot.id)
        if plan.state == 'PUBLISHED':
            raise CopyWorkConflict('captured chat presentation is already sealed')
        if request.kind == 'CHAT_LABELS' and plan.state == 'ABORTED':
            plan.state, plan.task_id = 'PENDING', None
            plan.expected_count, plan.published_count = 0, 0
    from shared.database import CARRY_FORWARD_WAIT_MAX_S
    params = {**request.params, 'target_snapshot_id': str(snapshot.id)}
    params.pop('_copy_execution_budget_v1', None)
    # Derived source evidence is not part of the producer's idempotency key.
    # Re-enqueueing a returned task's params must reuse its original intent.
    source_fixed = params.pop('_copy_source_fixed', False)
    source_implicit = params.pop('_copy_source_implicit', False)
    if source_fixed and (source_implicit or params.get('prior_snap_id')):
        params.pop('prior_snapshot_id', None)
    params.pop('_copy_p8_sources_fixed', None)
    params.pop('_copy_p8_source_count', None)
    digest = _key(workflow.id, workflow.capture_attempt, request.kind, params)
    large_inputs={key:value for key,value in params.items() if key in (
        'file_ids','external_ids','excluded_paths','tombstoned_ids','keep_folder_ids','chat_ids','names','labels')
        and isinstance(value,list) and len(value)>256}
    if request.kind=='CHAT_RENAME':
        ids,names=params.get('chat_ids',[]),params.get('names',[])
        if len(ids)!=len(names) or len(set(ids))!=len(ids) or any(not v for v in ids+names):
            raise ValueError('rename requires distinct ids and matching nonempty names')
    if large_inputs:
        params.update({key:[] for key in large_inputs})
        params['_copy_input_keys']=sorted(large_inputs)
    tenant_key = _key('TENANT', resource.tenant_id, request.lane)
    policy_key = _key('POLICY', resource.tenant_id, policy_id, request.lane)
    resource_key = _key('RESOURCE', resource.tenant_id, policy_id, resource.id, request.lane)
    for key, scope, parent, weight, rid in ((tenant_key, 'TENANT', None, 1, None),
            (policy_key, 'POLICY', tenant_key, scheduling_weight, None),
            (resource_key, 'RESOURCE', policy_key, 1, resource.id)):
        await session.execute(insert(CopyWorkQueue).values(key=key, scope=scope, parent_key=parent,
            tenant_id=resource.tenant_id, sla_policy_id=policy_id, resource_id=rid,
            lane=request.lane, weight=weight, virtual_finish=0).on_conflict_do_nothing(index_elements=['key']))
    await session.execute(insert(CopyWorkTask).values(id=uuid4(), workflow_id=workflow.id,
        tenant_id=resource.tenant_id, resource_id=resource.id, sla_policy_id=policy_id,
        queue_key=policy_key, resource_queue_key=resource_key, tenant_queue_key=tenant_key,
        idempotency_key=digest, kind=request.kind, lane=request.lane, params=params, state='READY',
        capture_attempt=workflow.capture_attempt,
        wait_max_s=max(0, CARRY_FORWARD_WAIT_MAX_S), prepared=False, checkpoint=0,
        row_count=0, bytes_count=0, generation=0, cumulative_wait_s=0, attempts=0)
        .on_conflict_do_nothing(index_elements=['idempotency_key']))
    task = await session.scalar(select(CopyWorkTask).where(CopyWorkTask.idempotency_key == digest))
    for key,values in large_inputs.items():
        for start in range(0,len(values),128):
            await session.execute(insert(CopyWorkInput).values([
                {'task_id':task.id,'input_key':key,'ordinal':i,'payload':{'value':values[i]}}
                for i in range(start,min(start+128,len(values)))])
                .on_conflict_do_nothing(index_elements=['task_id','input_key','ordinal']))
    from shared.copy_executor import pin_intent_sources
    await pin_intent_sources(session, task)
    return task


async def align_ready_workflow(session, workflow_id) -> None:
    """An inactive queue enters behind the backlogged siblings' next fair turn."""
    workflow = await session.get(CopyWorkflow, _uuid(workflow_id))
    if workflow is None or workflow.state != 'DRAINING':
        return
    # Share claim's short scheduling mutex. Claim never locks a workflow and
    # skips locked task rows, so seal's held DRAINING workflow cannot block it.
    await session.execute(insert(CopyWorkControl).values(id='global').on_conflict_do_nothing())
    await session.scalar(select(CopyWorkControl.id).where(CopyWorkControl.id == 'global').with_for_update())
    backlog = select(CopyWorkTask.id).join(CopyWorkflow, CopyWorkflow.id == CopyWorkTask.workflow_id).where(
        CopyWorkflow.state == 'WAITING', CopyWorkflow.id != workflow.id,
        CopyWorkTask.capture_attempt == CopyWorkflow.capture_attempt,
        CopyWorkTask.state.in_(('READY', 'RUNNING')))
    for scope, column in (('TENANT', CopyWorkTask.tenant_queue_key),
                          ('POLICY', CopyWorkTask.queue_key), ('RESOURCE', CopyWorkTask.resource_queue_key)):
        siblings = aliased(CopyWorkQueue)
        minimum = select(func.min(siblings.virtual_finish)).where(
            siblings.scope == scope, siblings.lane == CopyWorkQueue.lane,
            siblings.parent_key.is_not_distinct_from(CopyWorkQueue.parent_key),
            siblings.key != CopyWorkQueue.key,
            exists(backlog.where(column == siblings.key)).correlate(siblings),
        ).correlate(CopyWorkQueue).scalar_subquery()
        keys = select(column).where(CopyWorkTask.workflow_id == workflow.id,
            CopyWorkTask.capture_attempt == workflow.capture_attempt, CopyWorkTask.state.in_(('READY', 'RUNNING')))
        rows = await session.execute(select(CopyWorkQueue,
            exists(backlog.where(column == CopyWorkQueue.key)).correlate(CopyWorkQueue), minimum,
        ).where(CopyWorkQueue.scope == scope, CopyWorkQueue.key.in_(keys)))
        for queue, already_active, sibling_min in rows:
            if not already_active and sibling_min is not None:
                # A full positive quantum also prevents smaller-key new queues
                # from repeatedly winning equal-clock or epsilon-only ties.
                queue.virtual_finish = max(queue.virtual_finish, sibling_min + 1 / queue.weight)
    await session.flush()


async def claim(session, *, owner_id, lease_seconds=90) -> CopyClaim | None:
    # Serializes only short scheduling decisions, never execution or waiting.
    await session.execute(insert(CopyWorkControl).values(id='global').on_conflict_do_nothing())
    control = await session.scalar(select(CopyWorkControl).where(CopyWorkControl.id == 'global').with_for_update())
    now = await session.scalar(select(func.clock_timestamp()))
    candidates = select(CopyWorkTask).join(CopyWorkflow, CopyWorkflow.id == CopyWorkTask.workflow_id).where(
        CopyWorkflow.state == 'WAITING',
        CopyWorkTask.capture_attempt == CopyWorkflow.capture_attempt,
        ((CopyWorkTask.state == 'READY') & (CopyWorkTask.ready_at <= now)) |
        ((CopyWorkTask.state == 'RUNNING') & (CopyWorkTask.lease_expires_at < now)))
    inserts = aliased(CopyWorkTask)
    candidates = candidates.where((CopyWorkTask.kind != 'CHAT_RENAME') | ~exists(select(inserts.id).where(
        inserts.workflow_id == CopyWorkTask.workflow_id, inserts.kind != 'CHAT_RENAME',
        inserts.capture_attempt == CopyWorkTask.capture_attempt,
        inserts.state.notin_(TERMINAL_TASKS))))
    candidates = candidates.where((CopyWorkTask.kind != 'CHAT_PRESENTATION') | ~exists(select(inserts.id).where(
        inserts.workflow_id == CopyWorkTask.workflow_id, inserts.kind != 'CHAT_PRESENTATION',
        inserts.capture_attempt == CopyWorkTask.capture_attempt,
        inserts.state.notin_(TERMINAL_TASKS))))
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
    # A still-running old SQL transaction can lock an expired lease row.
    # Advance the fair turn even on SKIP LOCKED so it cannot stall other queues.
    for queue in chosen:
        queue.virtual_finish += 1 / queue.weight
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
    if (task.params or {}).get('_copy_execution_budget_v1') is not True:
        # Earlier attempts were lifetime claim counts. Start per-checkpoint
        # reservations from known failures, preserving old prepared manifests.
        previous = (task.error or {}).get('execution_failures', 0)
        task.attempts = previous if type(previous) is int and 0 <= previous <= 3 else 0
        task.params = {**(task.params or {}), '_copy_execution_budget_v1': True}
    task.state = 'RUNNING'
    task.generation += 1
    task.lease_owner_id = _uuid(owner_id)
    task.lease_expires_at = now + timedelta(seconds=lease_seconds)
    task.attempts += 1
    await session.flush()
    return CopyClaim(task.id, task.lease_owner_id, task.generation)
