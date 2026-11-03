"""Verified legacy naming companion; legacy DR still requires its database backup."""
import hashlib
import hmac
import json
from uuid import UUID

from azure.core.exceptions import ResourceExistsError
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from shared.chat_presentation import MESSAGE_TYPES, message_chat_id
from shared.chat_presentation_work import validate_labels
from shared.models import (ChatPresentationPlan, Resource, ResourceType, Snapshot,
    SnapshotChatPresentation, SnapshotItem, SnapshotStatus)

FORMAT = 'tmvault.chat-presentation.v1'
_FIELDS = ('chat_id', 'folder_path', 'quality', 'provenance', 'conflicted', 'policy_version')


async def _owned_snapshot(session, snapshot_id, tenant_id, *, write=False):
    snapshot = await session.scalar(select(Snapshot).where(Snapshot.id == UUID(str(snapshot_id)))
        .with_for_update(read=not write).execution_options(populate_existing=True))
    resource = await session.get(Resource, snapshot.resource_id) if snapshot else None
    if (snapshot is None or resource is None or resource.tenant_id != UUID(str(tenant_id))
        or resource.type != ResourceType.USER_CHATS
        or snapshot.status not in (SnapshotStatus.COMPLETED, SnapshotStatus.PARTIAL)
        or (snapshot.extra_data or {}).get('chat_inventory_format') == 1):
        raise ValueError('chat presentation DR scope is not an owned terminal legacy snapshot')
    return snapshot, resource


async def export_presentation_metadata(session, *, snapshot_id, tenant_id):
    snapshot, resource = await _owned_snapshot(session, snapshot_id, tenant_id)
    plan = await session.get(ChatPresentationPlan, snapshot.id)
    if ((snapshot.extra_data or {}).get('chat_presentation_format') != 1 or plan is None
        or plan.state != 'PUBLISHED' or plan.expected_count != plan.published_count):
        raise ValueError('chat presentation DR publication receipt is incomplete')
    rows = list((await session.scalars(select(SnapshotChatPresentation).where(
        SnapshotChatPresentation.snapshot_id == snapshot.id).order_by(SnapshotChatPresentation.chat_id))).all())
    if len(rows) != plan.expected_count:
        raise ValueError('chat presentation DR coverage gap')
    payload = {'format': FORMAT, 'snapshot_id': str(snapshot.id), 'tenant_id': str(resource.tenant_id),
        'resource_id': str(resource.id), 'labels': [{field: getattr(row, field) for field in _FIELDS} for row in rows]}
    return json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(',', ':')).encode()


async def replicate_presentation_metadata(session, *, snapshot_id, tenant_id, client):
    data = await export_presentation_metadata(session, snapshot_id=snapshot_id, tenant_id=tenant_id)
    digest = hashlib.sha256(data).hexdigest()
    try:
        await client.upload_blob(data, overwrite=False, metadata={'sha256': digest,
            'source_snapshot_id': str(snapshot_id), 'content_kind': 'chat-presentation-v1'})
    except ResourceExistsError:
        pass  # An immutable retry must verify the existing object, not overwrite it.
    stream = await client.download_blob()
    replica = await stream.readall()
    if len(replica) != len(data) or not hmac.compare_digest(hashlib.sha256(replica).hexdigest(), digest):
        raise ValueError('chat presentation DR replica checksum differs')
    return {'format': FORMAT, 'sha256': digest, 'size': len(data)}


async def restore_presentation_metadata(session, *, snapshot_id, tenant_id, data, expected_sha256):
    """Restore names after the database inventory and original IDs are recovered."""
    if (not isinstance(data, bytes) or not isinstance(expected_sha256, str)
        or not hmac.compare_digest(hashlib.sha256(data).hexdigest(), expected_sha256)):
        raise ValueError('chat presentation DR checksum differs')
    payload = json.loads(data)
    snapshot, resource = await _owned_snapshot(session, snapshot_id, tenant_id, write=True)
    if (not isinstance(payload, dict) or payload.get('format') != FORMAT
        or payload.get('snapshot_id') != str(snapshot.id) or payload.get('tenant_id') != str(resource.tenant_id)
        or payload.get('resource_id') != str(resource.id)):
        raise ValueError('chat presentation DR scope differs')
    labels = payload.get('labels')
    validate_labels(labels)
    if any(label.get('policy_version') != 1 or type(label.get('conflicted')) is not bool for label in labels):
        raise ValueError('unsupported chat presentation DR policy')
    membership = set((await session.scalars(select(message_chat_id()).where(
        SnapshotItem.snapshot_id == snapshot.id, SnapshotItem.item_type.in_(MESSAGE_TYPES)).distinct())).all())
    wanted = {label['chat_id']: {key: label[key] for key in _FIELDS} for label in labels}
    if membership != set(wanted):
        raise ValueError('chat presentation DR scope does not cover the recovered inventory')
    existing = list((await session.scalars(select(SnapshotChatPresentation).where(
        SnapshotChatPresentation.snapshot_id == snapshot.id))).all())
    if any(row.chat_id not in wanted or {key: getattr(row, key) for key in _FIELDS} != wanted[row.chat_id] for row in existing):
        raise ValueError('chat presentation DR conflicts with immutable captured labels')
    plan = await session.get(ChatPresentationPlan, snapshot.id)
    if plan is not None and (plan.state != 'PUBLISHED' or plan.expected_count != len(labels)):
        raise ValueError('chat presentation DR conflicts with existing publication')
    if labels:
        for start in range(0, len(labels), 256):
            await session.execute(insert(SnapshotChatPresentation).values([
                dict(snapshot_id=snapshot.id, **wanted[label['chat_id']]) for label in labels[start:start + 256]])
                .on_conflict_do_nothing())
    await session.execute(insert(ChatPresentationPlan).values(snapshot_id=snapshot.id, state='PUBLISHED',
        expected_count=len(labels), published_count=len(labels)).on_conflict_do_nothing())
    snapshot.extra_data = {**(snapshot.extra_data or {}), 'chat_presentation_format': 1}
    await session.flush()
