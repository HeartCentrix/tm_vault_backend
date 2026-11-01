"""Snapshot-owned chat names and exact conversation selection.

Labels are presentation, never conversation identity. Callers authorize the
requested snapshots before using these database-only helpers.
"""
from uuid import UUID

from sqlalchemy import case, func, select

from shared.models import (ChatPresentationPlan, Resource, Snapshot,
    SnapshotChatPresentation, SnapshotItem, SnapshotStatus)

MESSAGE_TYPES = ("TEAMS_CHAT_MESSAGE", "TEAMS_MESSAGE", "TEAMS_MESSAGE_REPLY")


class ChatSelectionError(ValueError):
    """A chat-specific selector cannot prove one captured conversation."""


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
                                 thread_path=None, item_ids=()):
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
    identity = message_chat_id()
    stmt = select(SnapshotItem).where(SnapshotItem.snapshot_id.in_(snapshots),
        SnapshotItem.item_type.in_(MESSAGE_TYPES))
    if chat_id:
        present = set((await session.scalars(select(SnapshotItem.snapshot_id).where(
            SnapshotItem.snapshot_id.in_(snapshots), SnapshotItem.item_type.in_(MESSAGE_TYPES),
            identity == chat_id).distinct())).all())
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


async def list_chat_presentations(session, snapshot_id):
    """Legacy compatibility reads only this snapshot's stored labels.

    MAX preserves the existing representative rule; aliases disclose mixed
    historical presentation instead of inventing a verified original topic.
    """
    identity = message_chat_id()
    rows = (await session.execute(select(identity.label("chat_id"),
        func.count().label("count"), func.max(func.nullif(SnapshotItem.folder_path, "")).label("path"),
        func.array_agg(func.distinct(SnapshotItem.folder_path)).label("aliases"))
        .join(Snapshot, Snapshot.id == SnapshotItem.snapshot_id)
        .join(Resource, Resource.id == Snapshot.resource_id)
        .where(SnapshotItem.snapshot_id == UUID(str(snapshot_id)),
            SnapshotItem.item_type.in_(MESSAGE_TYPES), identity.is_not(None),
            (SnapshotItem.tenant_id.is_(None) | (SnapshotItem.tenant_id == Resource.tenant_id)))
        .group_by(identity).order_by(identity))).all()
    snapshot = await session.get(Snapshot, UUID(str(snapshot_id)))
    captured = {}
    if (snapshot.extra_data or {}).get('chat_presentation_format') == 1:
        if not await presentation_ready(session, snapshot_id):
            raise ChatSelectionError('CHAT_PRESENTATION_INTEGRITY_GAP')
        captured = {label.chat_id: label for label in (await session.scalars(
            select(SnapshotChatPresentation).where(SnapshotChatPresentation.snapshot_id == snapshot.id))).all()}
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
    if plan.state != 'PUBLISHED' or plan.published_count != plan.expected_count:
        return False
    snapshot = await session.get(Snapshot, plan.snapshot_id)
    return snapshot is not None and (snapshot.extra_data or {}).get('chat_presentation_format') == 1
