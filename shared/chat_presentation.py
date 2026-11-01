"""Snapshot-owned chat names and exact conversation selection.

Labels are presentation, never conversation identity. Callers authorize the
requested snapshots before using these database-only helpers.
"""
import hashlib
import os
from uuid import UUID

from sqlalchemy import case, func, select, or_
from sqlalchemy.orm import aliased

from shared.models import (ChatCaptureAttempt, ChatPresentationPlan, Resource, Snapshot,
    SnapshotChatInventory, SnapshotChatPresentation, SnapshotItem, SnapshotStatus)

MESSAGE_TYPES = ("TEAMS_CHAT_MESSAGE", "TEAMS_MESSAGE", "TEAMS_MESSAGE_REPLY")


class ChatSelectionError(ValueError):
    """A chat-specific selector cannot prove one captured conversation."""


def chat_presentation_enabled():
    return os.getenv('CHAT_PRESENTATION_ENABLED', 'false').strip().lower() in ('1', 'true', 'yes', 'on')


def label_observations(chat_meta, *, owned_chat_ids, current_chats=()):
    """Resolve only owned labels from evidence already fetched by this user."""
    current = {row.get('id'): row for row in current_chats}
    result = []
    for chat_id in sorted(set(owned_chat_ids) & set(chat_meta)):
        info, observed = chat_meta[chat_id] or {}, current.get(chat_id, {})
        topic = observed.get('topic')
        known_topic = 'topic' in observed and (topic is None or isinstance(topic, str))
        cached_topic = info.get('topic')
        name = info.get('displayName') or ''
        if isinstance(topic, str) and topic.strip():
            name, quality, source = topic.strip(), 3, 'current_topic'
        elif not known_topic and isinstance(cached_topic, str) and cached_topic.strip():
            name, quality, source = cached_topic.strip(), 3, 'cached_topic'
        else:
            cleared = known_topic and not (topic or '').strip() and str(observed.get('chatType') or info.get('chatType') or '').lower() != 'oneonone'
            if cleared and cached_topic and name == cached_topic:
                name = ''
            fallback = not name or name.startswith(('Untitled chat #', 'Untitled group #',
                '1-on-1 Chat (19:', 'Group Chat (19:', 'Chat '))
            if not name:
                name = 'Untitled chat #' + hashlib.sha1(chat_id.encode()).hexdigest()[:8]
            quality = 0 if fallback else 1 if info.get('nameSource') == 'message_author' else 2
            source = 'current_topic_cleared' if cleared else 'fallback' if fallback else (
                'author' if quality == 1 else 'participants')
        result.append({'chat_id': chat_id, 'folder_path': 'chats/' + name,
            'quality': quality, 'provenance': source})
    return result


def message_chat_id(item=SnapshotItem):
    snake = func.nullif(item.extra_data["chat_id"].as_string(), "")
    camel = func.nullif(item.extra_data["chatId"].as_string(), "")
    # A reply's parent can be another message; only top-level pointers use
    # their parent as chat identity. Explicit provenance wins for replies.
    parent = case((item.item_type.in_(MESSAGE_TYPES[:2]), func.nullif(item.parent_external_id, "")))
    return case((snake.is_not(None) & camel.is_not(None) & (snake != camel), None),
        else_=func.coalesce(snake, camel, parent))


def _ids(values):
    return list(dict.fromkeys(UUID(str(value)) for value in values))


async def chat_message_statement(session, *, snapshot_ids, chat_id=None,
                                 thread_path=None, item_ids=(), resource_ids=None):
    """Return a scoped statement; ID+path validates an alias, then selects ID.

    Path-only requests retain the exact old path subset. Ambiguous paths fail
    rather than combining conversations that happen to share a title.
    """
    snapshots, items = _ids(snapshot_ids), _ids(item_ids)
    if (items and (chat_id or thread_path is not None)) or not (chat_id or thread_path is not None or items):
        raise ChatSelectionError("INVALID_SELECTION" if items else "SCOPE_EMPTY")
    if not snapshots:
        raise ChatSelectionError("SCOPE_EMPTY")
    owned = set((await session.scalars(select(Snapshot.id).where(Snapshot.id.in_(snapshots),
        Snapshot.status != SnapshotStatus.PENDING_DELETION))).all())
    if owned != set(snapshots):
        raise ChatSelectionError("SNAPSHOT_UNAVAILABLE")
    if resource_ids is not None:
        matching = set((await session.scalars(select(Snapshot.id).where(
            Snapshot.id.in_(snapshots), Snapshot.resource_id.in_(_ids(resource_ids))))).all())
        if matching != set(snapshots):
            raise ChatSelectionError('SNAPSHOT_RESOURCE_MISMATCH')
    identity = message_chat_id()
    stmt = select(SnapshotItem).where(SnapshotItem.snapshot_id.in_(snapshots),
        SnapshotItem.item_type.in_(MESSAGE_TYPES))
    if chat_id:
        present = set((await session.scalars(select(SnapshotItem.snapshot_id).where(
            SnapshotItem.snapshot_id.in_(snapshots), SnapshotItem.item_type.in_(MESSAGE_TYPES),
            identity == chat_id).distinct())).all())
        present.update((await session.scalars(select(SnapshotChatInventory.snapshot_id).join(Snapshot,
            Snapshot.id == SnapshotChatInventory.snapshot_id).where(
            SnapshotChatInventory.snapshot_id.in_(snapshots), SnapshotChatInventory.chat_id == chat_id,
            Snapshot.extra_data['chat_inventory_format'].as_integer() == 1))).all())
        if present != set(snapshots):
            raise ChatSelectionError("CHAT_NOT_IN_SNAPSHOT")
        if thread_path is not None:
            aliases = set((await session.scalars(select(SnapshotItem.snapshot_id).where(
                SnapshotItem.snapshot_id.in_(snapshots), SnapshotItem.item_type.in_(MESSAGE_TYPES),
                identity == chat_id, SnapshotItem.folder_path == thread_path).distinct())).all())
            captured_aliases = set((await session.scalars(select(SnapshotChatPresentation.snapshot_id)
                .join(Snapshot, Snapshot.id == SnapshotChatPresentation.snapshot_id)
                .join(ChatPresentationPlan, ChatPresentationPlan.snapshot_id == Snapshot.id)
                .where(SnapshotChatPresentation.snapshot_id.in_(snapshots),
                    SnapshotChatPresentation.chat_id == chat_id,
                    SnapshotChatPresentation.folder_path == thread_path,
                    Snapshot.extra_data['chat_presentation_format'].as_integer() == 1,
                    ChatPresentationPlan.state == 'PUBLISHED'))).all())
            captured_aliases.update((await session.scalars(select(SnapshotChatInventory.snapshot_id)
                .join(ChatCaptureAttempt, ChatCaptureAttempt.id == SnapshotChatInventory.capture_attempt_id)
                .join(Snapshot, Snapshot.id == SnapshotChatInventory.snapshot_id).where(
                    SnapshotChatInventory.snapshot_id.in_(snapshots), SnapshotChatInventory.chat_id == chat_id,
                    ChatCaptureAttempt.folder_path == thread_path,
                    Snapshot.extra_data['chat_inventory_format'].as_integer() == 1))).all())
            if aliases | captured_aliases != set(snapshots):
                raise ChatSelectionError("CHAT_ALIAS_MISMATCH")
        return stmt.where(identity == chat_id)
    stmt = stmt.where(SnapshotItem.id.in_(items)) if items else stmt.where(SnapshotItem.folder_path == thread_path)
    chats = list((await session.scalars(select(identity).where(
        SnapshotItem.id.in_(stmt.with_only_columns(SnapshotItem.id))).distinct().limit(2))).all())
    if None in chats:
        raise ChatSelectionError("CHAT_IDENTITY_UNAVAILABLE")
    if len(chats) > 1:
        raise ChatSelectionError("AMBIGUOUS_CHAT_PATH" if not items else "MULTI_THREAD_NOT_SUPPORTED_YET")
    return stmt


def item_chat_id(item):
    metadata = item.extra_data if isinstance(item.extra_data, dict) else {}
    snake, camel = metadata.get('chat_id'), metadata.get('chatId')
    if snake and camel and snake != camel:
        raise ChatSelectionError('CHAT_IDENTITY_UNAVAILABLE')
    identity = snake or camel
    if not identity and item.item_type in MESSAGE_TYPES[:2]:
        identity = item.parent_external_id
    if not isinstance(identity, str) or not identity:
        raise ChatSelectionError('CHAT_IDENTITY_UNAVAILABLE')
    return identity


def child_chat_id(item=SnapshotItem):
    """An attachment parent is a message, qualified by its own snapshot."""
    snake = func.nullif(item.extra_data['chat_id'].as_string(), '')
    camel = func.nullif(item.extra_data['chatId'].as_string(), '')
    parent = aliased(SnapshotItem)
    identity = message_chat_id(parent)
    proven = select(case((func.count(func.distinct(identity)) == 1, func.min(identity)))).where(
        parent.snapshot_id == item.snapshot_id, parent.external_id == item.parent_external_id,
        parent.item_type.in_(MESSAGE_TYPES)).correlate(item).scalar_subquery()
    return case((snake.is_not(None) & camel.is_not(None) & (snake != camel), None),
        else_=func.coalesce(snake, camel, proven))


async def filter_legacy_children(session, rows, chat_id):
    """Qualify before deduplication, including old children without metadata."""
    identities, missing = {}, []
    for row in rows:
        metadata = row.extra_data if isinstance(row.extra_data, dict) else {}
        snake, camel = metadata.get('chat_id'), metadata.get('chatId')
        if snake and camel and snake != camel:
            raise ChatSelectionError('CHAT_IDENTITY_UNAVAILABLE')
        if snake or camel:
            identities[row.id] = snake or camel
        else:
            missing.append(row.id)
    for start in range(0, len(missing), 500):
        found = (await session.execute(select(SnapshotItem.id, child_chat_id().label('chat_id')).where(
            SnapshotItem.id.in_(missing[start:start + 500])))).all()
        identities.update(found)
    if any(not identities.get(row.id) for row in rows):
        raise ChatSelectionError('CHAT_CHILD_IDENTITY_UNAVAILABLE')
    return [row for row in rows if identities[row.id] == chat_id]


async def captured_chat_path(session, snapshot_id, chat_id):
    snapshot = await session.get(Snapshot, UUID(str(snapshot_id)))
    if snapshot is None:
        raise ChatSelectionError('SNAPSHOT_UNAVAILABLE')
    metadata = snapshot.extra_data or {}
    if metadata.get('chat_inventory_format') == 1:
        path = await session.scalar(select(ChatCaptureAttempt.folder_path).join(SnapshotChatInventory,
            SnapshotChatInventory.capture_attempt_id == ChatCaptureAttempt.id).where(
            SnapshotChatInventory.snapshot_id == snapshot.id, SnapshotChatInventory.chat_id == chat_id))
    elif metadata.get('chat_presentation_format') == 1:
        if not await presentation_ready(session, snapshot.id):
            raise ChatSelectionError('CHAT_PRESENTATION_INTEGRITY_GAP')
        path = await session.scalar(select(SnapshotChatPresentation.folder_path).where(
            SnapshotChatPresentation.snapshot_id == snapshot.id, SnapshotChatPresentation.chat_id == chat_id))
    else:
        path = await session.scalar(select(func.max(func.nullif(SnapshotItem.folder_path, ''))).where(
            SnapshotItem.snapshot_id == snapshot.id, SnapshotItem.item_type.in_(MESSAGE_TYPES),
            message_chat_id() == chat_id))
    return path or f'chats/{chat_id}'


async def chat_restore_items(session, *, snapshot_ids, chat_id, folder_paths=(), item_ids=(), excluded_item_ids=()):
    """Generic restore/export shares the chat selector, never a folder OR."""
    if item_ids or len(folder_paths) > 1:
        raise ChatSelectionError('INVALID_SELECTION')
    stmt = await chat_message_statement(session, snapshot_ids=snapshot_ids, chat_id=chat_id,
        thread_path=next(iter(folder_paths), None))
    messages = list((await session.scalars(stmt)).all())
    snapshots = list((await session.scalars(select(Snapshot).where(Snapshot.id.in_(_ids(snapshot_ids))))).all())
    tenants = set((await session.scalars(select(Resource.tenant_id).where(
        Resource.id.in_({snapshot.resource_id for snapshot in snapshots})))).all())
    if len(tenants) != 1:
        raise ChatSelectionError('CHAT_TENANT_SCOPE_MISMATCH')
    catalogue_ids = {s.id for s in snapshots if (s.extra_data or {}).get('chat_inventory_format') == 1}
    if catalogue_ids:
        if catalogue_ids != {s.id for s in snapshots}:
            raise ChatSelectionError('AMBIGUOUS_CAPTURE_VERSIONS')
        inventories = set((await session.scalars(select(SnapshotChatInventory.inventory_id).where(
            SnapshotChatInventory.snapshot_id.in_(catalogue_ids), SnapshotChatInventory.chat_id == chat_id))).all())
        if len(inventories) > 1:
            raise ChatSelectionError('AMBIGUOUS_CAPTURE_VERSIONS')
        rows = messages  # The catalogue materializer loads exact captured children.
    else:
        children = []
        external_ids = [message.external_id for message in messages]
        for start in range(0, len(external_ids), 500):
            query = select(SnapshotItem).join(Snapshot, Snapshot.id == SnapshotItem.snapshot_id).join(Resource,
                Resource.id == Snapshot.resource_id).where(Resource.tenant_id.in_(tenants),
                or_(SnapshotItem.tenant_id.is_(None), SnapshotItem.tenant_id.in_(tenants)),
                SnapshotItem.item_type.in_(('CHAT_ATTACHMENT', 'CHAT_HOSTED_CONTENT')),
                SnapshotItem.parent_external_id.in_(external_ids[start:start + 500]))
            if all((snapshot.extra_data or {}).get('chat_presentation_format') == 1 for snapshot in snapshots):
                query = query.where(SnapshotItem.snapshot_id.in_(_ids(snapshot_ids)))
            candidates = list((await session.scalars(query.order_by(SnapshotItem.created_at.desc()))).all())
            children.extend(await filter_legacy_children(session, candidates, chat_id))
        best = {}
        for child in children:
            key = (child.item_type, child.parent_external_id, child.external_id)
            if key not in best or (not best[key].blob_path and child.blob_path):
                best[key] = child
        rows = messages + list(best.values())
    excluded = set(_ids(excluded_item_ids))
    return [row for row in rows if row.id not in excluded]


async def list_chat_presentations(session, snapshot_id, *, offset=0, limit=None):
    """Legacy compatibility reads only this snapshot's stored labels.

    MAX preserves the existing representative rule; aliases disclose mixed
    historical presentation instead of inventing a verified original topic.
    """
    identity = message_chat_id()
    statement = (select(identity.label("chat_id"),
        func.count().label("count"), func.max(func.nullif(SnapshotItem.folder_path, "")).label("path"),
        func.array_agg(func.distinct(SnapshotItem.folder_path)).label("aliases"))
        .join(Snapshot, Snapshot.id == SnapshotItem.snapshot_id)
        .join(Resource, Resource.id == Snapshot.resource_id)
        .where(SnapshotItem.snapshot_id == UUID(str(snapshot_id)),
            SnapshotItem.item_type.in_(MESSAGE_TYPES), identity.is_not(None),
            (SnapshotItem.tenant_id.is_(None) | (SnapshotItem.tenant_id == Resource.tenant_id)))
        .group_by(identity).order_by(identity))
    if limit is not None:
        statement = statement.offset(offset).limit(limit)
    rows = (await session.execute(statement)).all()
    snapshot = await session.get(Snapshot, UUID(str(snapshot_id)))
    captured = {}
    if (snapshot.extra_data or {}).get('chat_presentation_format') == 1:
        if not await presentation_ready(session, snapshot_id):
            raise ChatSelectionError('CHAT_PRESENTATION_INTEGRITY_GAP')
        captured = {label.chat_id: label for label in (await session.scalars(
            select(SnapshotChatPresentation).where(SnapshotChatPresentation.snapshot_id == snapshot.id,
                SnapshotChatPresentation.chat_id.in_([row.chat_id for row in rows])))).all()}
        if set(captured) != {row.chat_id for row in rows}:
            raise ChatSelectionError('CHAT_PRESENTATION_INTEGRITY_GAP')
    result = []
    for row in rows:
        aliases = sorted(path for path in row.aliases if path)
        path = row.path or f"chats/{row.chat_id}"
        label = captured.get(row.chat_id)
        if label is not None:
            path = label.folder_path
            aliases = sorted(set(aliases) | {path})
        result.append({"chatId": row.chat_id, "displayName": path.removeprefix("chats/"),
            "folderPath": path, "folderAliases": aliases, "count": int(row.count),
            "lastMessageAt": None, "presentationSource": 'captured' if label else (
                "legacy_mixed" if len(aliases) > 1 else "legacy_stored")})
    return result


async def presentation_ready(session, snapshot_id):
    """A persisted receipt remains required after a producer-flag rollback."""
    plan = await session.get(ChatPresentationPlan, UUID(str(snapshot_id)))
    if plan is None:
        return True
    metadata = await session.scalar(select(Snapshot.extra_data).where(Snapshot.id == plan.snapshot_id))
    if plan.state == 'ABORTED':
        return bool((metadata or {}).get('copy_required_failed'))
    if plan.state != 'PUBLISHED' or plan.published_count != plan.expected_count:
        return False
    return (metadata or {}).get('chat_presentation_format') == 1
