"""Small captured-label manifests in the existing fenced copy-work protocol."""
from sqlalchemy import case, delete, exists, func, literal, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import aliased

from shared.chat_presentation import MESSAGE_TYPES, ChatSelectionError, message_chat_id
from shared.models import (ChatPresentationDraft, ChatPresentationPlan, CopyWorkflow,
    CopyWorkInput, CopyWorkItem, CopyWorkTask, Snapshot, SnapshotChatPresentation, SnapshotItem)


def validate_labels(labels):
    if not isinstance(labels, list):
        raise ValueError("chat labels must be a list")
    seen = set()
    for value in labels:
        if (not isinstance(value, dict) or not isinstance(value.get('chat_id'), str)
            or not value['chat_id'] or value['chat_id'] in seen
            or not isinstance(value.get('folder_path'), str) or not value['folder_path']
            or type(value.get('quality')) is not int or not 0 <= value['quality'] <= 3
            or not isinstance(value.get('provenance'), str) or not 0 < len(value['provenance']) <= 64):
            raise ValueError("chat labels require distinct identities, nonempty paths and typed evidence")
        seen.add(value['chat_id'])


async def _source(session, task):
    workflow = await session.get(CopyWorkflow, task.workflow_id, populate_existing=True)
    if (workflow is None or task.capture_attempt != workflow.capture_attempt
        or workflow.state not in ('DRAINING', 'WAITING')):
        raise ValueError("chat label source capture is no longer authoritative")
    return workflow


async def prepare_presentation_manifest(session, task, params):
    workflow = await _source(session, task)
    if task.kind == 'CHAT_LABELS':
        if 'labels' in params.get('_copy_input_keys', []):
            stmt = select(literal(task.id), CopyWorkInput.ordinal + 1,
                CopyWorkInput.payload['value'], literal(0)).where(
                CopyWorkInput.task_id == task.id, CopyWorkInput.input_key == 'labels')
            await session.execute(insert(CopyWorkItem).from_select(
                ['task_id', 'ordinal', 'payload', 'phase'], stmt))
        else:
            labels = params.get('labels')
            validate_labels(labels)
            if labels:
                await session.execute(insert(CopyWorkItem), [dict(task_id=task.id,
                    ordinal=i + 1, payload=value, phase=0) for i, value in enumerate(labels)])
    else:
        pending = await session.scalar(select(exists(select(CopyWorkTask.id).where(
            CopyWorkTask.workflow_id == workflow.id,
            CopyWorkTask.capture_attempt == workflow.capture_attempt,
            CopyWorkTask.id != task.id, CopyWorkTask.state.notin_(('COMPLETED', 'FAILED', 'CANCELLED'))))))
        siblings = await session.scalar(select(exists(select(CopyWorkflow.id).where(
            CopyWorkflow.target_snapshot_id == workflow.target_snapshot_id,
            CopyWorkflow.id != workflow.id, CopyWorkflow.state.in_(('DRAINING', 'WAITING', 'RESUMING'))))))
        if pending or siblings:
            raise ValueError("chat presentation source work has not settled")
        plan = await session.get(ChatPresentationPlan, workflow.target_snapshot_id, with_for_update=True)
        if plan.state == 'PUBLISHED':
            raise ValueError("chat presentation is already immutable")
        if plan.task_id != task.id:
            await session.execute(delete(SnapshotChatPresentation).where(
                SnapshotChatPresentation.snapshot_id == workflow.target_snapshot_id))
        identity = message_chat_id()
        unknown = await session.scalar(select(Snapshot.id).join(SnapshotItem,
            SnapshotItem.snapshot_id == Snapshot.id).where(Snapshot.id == workflow.target_snapshot_id,
            SnapshotItem.item_type.in_(MESSAGE_TYPES), identity.is_(None)).limit(1))
        if unknown:
            raise ChatSelectionError('CHAT_IDENTITY_UNAVAILABLE')
        identities = select(identity.label('chat_id'),
            func.max(func.nullif(SnapshotItem.folder_path, '')).label('stored_path')).where(
            SnapshotItem.snapshot_id == workflow.target_snapshot_id,
            SnapshotItem.item_type.in_(MESSAGE_TYPES)).group_by(identity).subquery()
        draft = ChatPresentationDraft
        winners = select(draft.chat_id, draft.folder_path, draft.quality, draft.provenance, draft.conflicted).join(
            CopyWorkflow, CopyWorkflow.id == draft.workflow_id).where(
            draft.snapshot_id == workflow.target_snapshot_id,
            CopyWorkflow.target_snapshot_id == workflow.target_snapshot_id,
            draft.capture_attempt == CopyWorkflow.capture_attempt, CopyWorkflow.state != 'CANCELLED')
        winners = winners.distinct(draft.chat_id).order_by(draft.chat_id, draft.quality.desc(),
            draft.accepted_at, draft.workflow_id).subquery()
        source_ids = select(CopyWorkItem.source_snapshot_id).join(CopyWorkTask,
            CopyWorkTask.id == CopyWorkItem.task_id).join(CopyWorkflow,
            CopyWorkflow.id == CopyWorkTask.workflow_id).where(
            CopyWorkflow.target_snapshot_id == workflow.target_snapshot_id,
            CopyWorkTask.capture_attempt == CopyWorkflow.capture_attempt,
            CopyWorkItem.source_snapshot_id.is_not(None))
        previous = aliased(Snapshot)
        saved = SnapshotChatPresentation
        prior = select(saved.chat_id, saved.folder_path, saved.quality).join(previous,
            previous.id == saved.snapshot_id).where(previous.id.in_(source_ids),
            previous.resource_id == workflow.resource_id,
            previous.extra_data['chat_presentation_format'].as_integer() == 1)
        prior = prior.distinct(saved.chat_id).order_by(saved.chat_id, previous.created_at.desc(), previous.id.desc()).subquery()
        retain = (prior.c.quality > func.coalesce(winners.c.quality, -1)) & (
            (func.coalesce(winners.c.provenance, '') != 'current_topic_cleared') | (prior.c.quality < 3))
        path = case((retain, prior.c.folder_path), else_=winners.c.folder_path)
        path = func.coalesce(path, identities.c.stored_path, literal('chats/') + identities.c.chat_id)
        other, producer = aliased(ChatPresentationDraft), aliased(CopyWorkflow)
        conflict = exists(select(other.chat_id).join(producer, producer.id == other.workflow_id).where(
            other.snapshot_id == workflow.target_snapshot_id, other.chat_id == identities.c.chat_id,
            producer.target_snapshot_id == workflow.target_snapshot_id, producer.state != 'CANCELLED',
            other.capture_attempt == producer.capture_attempt, other.quality == winners.c.quality,
            other.folder_path != winners.c.folder_path)).correlate(identities, winners)
        payload = func.json_build_object('chat_id', identities.c.chat_id, 'folder_path', path,
            'quality', func.coalesce(case((retain, prior.c.quality), else_=winners.c.quality), 0),
            'provenance', case((retain, 'retained_history'), else_=func.coalesce(winners.c.provenance, 'legacy_stored')),
            'conflicted', func.coalesce(winners.c.conflicted, False) | conflict)
        selected = select(literal(task.id), func.row_number().over(order_by=identities.c.chat_id),
            payload, literal(0)).select_from(identities.outerjoin(winners,
                winners.c.chat_id == identities.c.chat_id).outerjoin(prior, prior.c.chat_id == identities.c.chat_id))
        await session.execute(insert(CopyWorkItem).from_select(['task_id', 'ordinal', 'payload', 'phase'], selected))
        plan.state, plan.task_id = 'PUBLISHING', task.id
        plan.expected_count = await session.scalar(select(func.count()).select_from(CopyWorkItem).where(CopyWorkItem.task_id == task.id))
        plan.published_count = 0
    task.prepared = True
    await session.flush()


async def apply_label_batch(session, task, *, limit):
    workflow = await _source(session, task)
    rows = list((await session.scalars(select(CopyWorkItem).where(
        CopyWorkItem.task_id == task.id, CopyWorkItem.ordinal > int(task.checkpoint or 0))
        .order_by(CopyWorkItem.ordinal).limit(limit))).all())
    if rows:
        values = [dict(snapshot_id=workflow.target_snapshot_id,
            chat_id=row.payload['chat_id'], folder_path=row.payload['folder_path'],
            quality=row.payload['quality'], provenance=row.payload['provenance'],
            conflicted=bool(row.payload.get('conflicted'))) for row in rows]
        if task.kind == 'CHAT_LABELS':
            for value in values:
                value.update(workflow_id=workflow.id, capture_attempt=workflow.capture_attempt)
            stmt = insert(ChatPresentationDraft).values(values)
            old, new = ChatPresentationDraft, stmt.excluded
            await session.execute(stmt.on_conflict_do_update(
                index_elements=['snapshot_id', 'workflow_id', 'capture_attempt', 'chat_id'], set_={
                    'folder_path': case((new.quality > old.quality, new.folder_path), else_=old.folder_path),
                    'provenance': case((new.quality > old.quality, new.provenance), else_=old.provenance),
                    'accepted_at': case((new.quality > old.quality, func.now()), else_=old.accepted_at),
                    'quality': func.greatest(old.quality, new.quality),
                    'conflicted': old.conflicted | ((new.quality == old.quality) & (new.folder_path != old.folder_path))}))
        else:
            await session.execute(insert(SnapshotChatPresentation).values(values).on_conflict_do_nothing())
    cursor = int(rows[-1].ordinal) if rows else int(task.checkpoint or 0)
    done = not await session.scalar(select(exists(select(CopyWorkItem.task_id).where(
        CopyWorkItem.task_id == task.id, CopyWorkItem.ordinal > cursor))))
    if task.kind == 'CHAT_PRESENTATION':
        plan = await session.get(ChatPresentationPlan, workflow.target_snapshot_id, with_for_update=True)
        if plan.task_id != task.id:
            raise ValueError('chat presentation publication capture changed')
        plan.published_count = await session.scalar(select(func.count()).select_from(SnapshotChatPresentation).where(
            SnapshotChatPresentation.snapshot_id == workflow.target_snapshot_id))
        if done:
            if plan.published_count != plan.expected_count:
                raise ValueError('chat presentation publication coverage gap')
            snapshot = await session.get(Snapshot, workflow.target_snapshot_id)
            snapshot.extra_data = {**(snapshot.extra_data or {}), 'chat_presentation_format': 1}
            plan.state = 'PUBLISHED'
            await session.execute(delete(ChatPresentationDraft).where(
                ChatPresentationDraft.snapshot_id == workflow.target_snapshot_id))
    return {'done': bool(done), 'cursor': cursor, 'rows': len(rows), 'bytes': 0}


