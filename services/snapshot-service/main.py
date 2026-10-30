import asyncio
import hashlib as _hashlib
import os
import re as _re
import base64 as _b64
import io as _io
import time as _time
"""Snapshot Service - Manages snapshots and snapshot items"""
from contextlib import asynccontextmanager
from typing import Optional, List, Dict, Any, Tuple
from uuid import UUID, uuid4
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, Depends, HTTPException, Query, Body
from fastapi.responses import StreamingResponse
from sqlalchemy import select, func, distinct, and_, or_, text, String, cast, DateTime
from sqlalchemy.orm import aliased

from shared.database import get_db, init_db, close_db, AsyncSession, async_session_factory
from shared.models import Snapshot, SnapshotItem, Resource, SnapshotType, SnapshotStatus, ResourceType, ResourceStatus, Job, ChatItemVersion, SnapshotChatInventory, ChatCaptureAttempt
from shared.chat_catalogue import materialize_chat_items, ChatCatalogueIntegrityError
from shared.snapshot_reuse import resolve_snapshot_items_target, SnapshotNotFound
from shared.audit import internal_audit_headers
from shared.config import settings
# Producer-only; on-demand archive enrichment nudge. Defensive so this read API
# still boots if the broker deps are ever absent — the trigger just no-ops.
try:
    from shared.message_bus import message_bus
except Exception:  # noqa: BLE001
    message_bus = None
from shared.power_bi_snapshot import assemble_power_bi_items
from shared.azure_storage import azure_storage_manager, workload_candidates_for_resource_type
from shared.schemas import (
    SnapshotListResponse, SnapshotResponse, SnapshotItemListResponse, SnapshotItemResponse, SnapshotDiff
)
import logging

logger = logging.getLogger(__name__)


async def _backfill_folder_paths():
    """One-shot backfill: derive `folder_path` from `extra_data.raw` for any
    snapshot item still missing it. Lets pre-fix snapshots show real folder
    grouping in the Recovery left panel without re-running their backups.

    OneDrive / SharePoint: takes raw.parentReference.path (Microsoft Graph
    driveItem schema — see https://learn.microsoft.com/graph/api/resources/itemreference)
    and strips the "/drive/root:" prefix. Mail / contacts: falls back to
    "folder:<8-char id prefix>" because folder NAMES require a separate
    Graph round-trip we can't make at startup. Re-running the backup with
    the new handler populates names properly going forward."""
    from sqlalchemy import text as _text
    statements = [
        # OneDrive / SharePoint files — name is in parentReference.path.
        # Coerce empty path (root items) to "/" so the tree has a sensible
        # bucket instead of a nameless one.
        """
        UPDATE snapshot_items
        SET folder_path = COALESCE(
            NULLIF(
              CASE
                WHEN metadata->'raw'->'parentReference'->>'path' LIKE '%:%'
                THEN split_part(metadata->'raw'->'parentReference'->>'path', ':', 2)
                ELSE metadata->'raw'->'parentReference'->>'path'
              END,
              ''
            ),
            '/'
        )
        WHERE (folder_path IS NULL OR folder_path = '')
          AND item_type IN ('FILE', 'ONEDRIVE_FILE', 'SHAREPOINT_FILE')
          AND metadata->'raw'->'parentReference'->>'path' IS NOT NULL
        """,
        # Mail — opaque folder ID groups together at least.
        """
        UPDATE snapshot_items
        SET folder_path = 'folder:' || substr(metadata->'raw'->>'parentFolderId', 1, 12)
        WHERE (folder_path IS NULL OR folder_path = '')
          AND item_type = 'EMAIL'
          AND metadata->'raw'->>'parentFolderId' IS NOT NULL
        """,
        # Contacts — same fallback.
        """
        UPDATE snapshot_items
        SET folder_path = 'folder:' || substr(metadata->'raw'->>'parentFolderId', 1, 12)
        WHERE (folder_path IS NULL OR folder_path = '')
          AND item_type IN ('USER_CONTACT', 'CONTACT')
          AND metadata->'raw'->>'parentFolderId' IS NOT NULL
        """,
    ]
    from shared.database import async_session_factory as _factory
    async with _factory() as session:
        for stmt in statements:
            try:
                await session.execute(_text(stmt))
            except Exception as e:
                print(f"[SNAPSHOT] folder_path backfill skipped: {e}")
        await session.commit()


async def _backfill_chat_topic_fallbacks():
    """One-shot: null out chat_threads.chat_topic rows that were poisoned with
    synthetic fallback display names ("Group Chat (19:abcd1)", "1-on-1 Chat
    (19:abcd1)", "Chat 19:abcd1"). The write path no longer persists those —
    it now stores NULL when Graph returns no real topic — and the read path
    composes the displayName from member_names_json + chat_type at request
    time. Leaving the poisoned values in place would make those rows skip
    the new composition, so we erase them here. Idempotent: re-running
    against already-cleaned rows is a no-op.
    """
    from sqlalchemy import text as _text
    sql = (
        "UPDATE chat_threads "
        "   SET chat_topic = NULL "
        " WHERE chat_topic IS NOT NULL "
        "   AND ( chat_topic LIKE 'Group Chat (19:%' "
        "      OR chat_topic LIKE '1-on-1 Chat (19:%' "
        "      OR chat_topic LIKE 'Chat 19:%' ) "
    )
    from shared.database import async_session_factory as _factory
    async with _factory() as session:
        try:
            result = await session.execute(_text(sql))
            await session.commit()
            n = getattr(result, "rowcount", None)
            if n:
                print(f"[SNAPSHOT] backfilled {n} chat_threads.chat_topic → NULL (synthetic fallback)")
        except Exception as e:
            print(f"[SNAPSHOT] chat_topic backfill skipped: {e}")


_CHAT_NAME_MAX_LEN = 200


def _compose_chat_name(
    chat_id: str,
    topic: Optional[str],
    chat_type: Optional[str],
    member_names: Optional[list],
    self_display_name: Optional[str] = None,
    self_user_id: Optional[str] = None,
    member_user_ids: Optional[list] = None,
) -> str:
    """Read-time chat display name. Never persisted.

    MUST stay aligned with the write-side helper in backup-worker
    (`_compose_chat_name_local`) so /folders and /chats/groups agree
    on the same name.

    Resolution order:
      1. Topic — matches Teams' own rename UX.
      2. Others (filter self out by AAD object id when available, else by
         displayName match against `self_display_name`).
      3. 1:1 → the other person's display name.
         Group / meeting → comma-joined names of others, smart-truncated
         past 200 chars with a trailing "+N more".
      4. Fallback: "Untitled chat/group #<sha1[:8]>" — unique per chat_id
         so unnamed chats never collapse into a shared folder.
    """
    if isinstance(topic, str) and topic.strip():
        return topic.strip()
    raw_names: List[str] = []
    if isinstance(member_names, list):
        raw_names = [
            str(n).strip()
            for n in member_names
            if isinstance(n, str) and n.strip()
        ]
    raw_uids: List[str] = []
    if isinstance(member_user_ids, list):
        raw_uids = [
            str(u).strip()
            for u in member_user_ids
            if u
        ]
    self_uid_norm = (self_user_id or "").strip().lower()
    self_dn_norm = (self_display_name or "").strip().lower()
    others: List[str] = []
    if raw_uids and raw_names and len(raw_uids) == len(raw_names):
        for _u, _n in zip(raw_uids, raw_names):
            if _u and _u.strip().lower() == self_uid_norm:
                continue
            others.append(_n)
    else:
        for _n in raw_names:
            if self_dn_norm and _n.lower() == self_dn_norm:
                continue
            others.append(_n)
    ct = (chat_type or "").lower()
    if ct == "oneonone":
        if others:
            return others[0]
    elif others:
        joined = ", ".join(others)
        if len(joined) <= _CHAT_NAME_MAX_LEN:
            return joined
        picked: List[str] = []
        used = 0
        for _n in others:
            sep_len = 2 if picked else 0
            tail_len = 10  # room for ' +N more'
            if used + sep_len + len(_n) + tail_len > _CHAT_NAME_MAX_LEN:
                break
            picked.append(_n)
            used += sep_len + len(_n)
        remaining = len(others) - len(picked)
        if remaining > 0:
            return f"{', '.join(picked)} +{remaining} more"
        return ", ".join(picked)
    suffix = _hashlib.sha1(chat_id.encode("utf-8")).hexdigest()[:8]
    if ct == "oneonone":
        return f"Untitled chat #{suffix}"
    return f"Untitled group #{suffix}"


@asynccontextmanager
async def lifespan(app: FastAPI):
    from shared.storage.startup import startup_router, shutdown_router
    from shared import core_metrics
    core_metrics.init()
    await init_db()
    # At-rest encryption hook so item downloads/previews can decrypt encrypted
    # blobs on read (no-op unless BACKUP_ENCRYPTION_* env is configured).
    try:
        from shared.crypto import bootstrap as _enc_bootstrap
        from shared.database import async_session_factory as _asf
        if await _enc_bootstrap.install(_asf):
            print("[SNAPSHOT] at-rest encryption hook installed")
    except Exception as e:
        print(f"[SNAPSHOT] encryption hook install failed (non-fatal): {e}")
    try:
        await _backfill_folder_paths()
    except Exception as e:
        print(f"[SNAPSHOT] folder backfill failed (non-fatal): {e}")
    try:
        await _backfill_chat_topic_fallbacks()
    except Exception as e:
        print(f"[SNAPSHOT] chat_topic backfill failed (non-fatal): {e}")
    await startup_router()
    # Producer-only RabbitMQ connect so browse can nudge on-demand archive
    # preview-enrichment (enrich.priority). Best-effort — a read API must still
    # start if the broker is down; publish() no-ops when the exchange is unset.
    if settings.RABBITMQ_ENABLED and message_bus is not None:
        try:
            await message_bus.connect()
        except Exception as e:
            print(f"[SNAPSHOT] message_bus connect failed (non-fatal): {e}")
    try:
        yield
    finally:
        await shutdown_router()
        await close_db()


async def _maybe_trigger_archive_enrichment(db: AsyncSession, items: List[SnapshotItem]) -> None:
    """On-demand preview enrichment: if this browsed page includes archive items
    that still need enriching, nudge the enrichment-worker (enrich.priority).
    Delegates to the SHARED nudge so browse and export stay in lock-step; it's
    best-effort + idempotent and never raises."""
    if message_bus is None:
        return
    from shared import archive_enrichment as _arch_eng
    await _arch_eng.nudge_enrichment(db, items, message_bus.publish)


from shared.audit_middleware import AuditMiddleware  # Ops/Data Check 3: mutation-audit backstop
app = FastAPI(title="Snapshot Service", version="1.0.0", lifespan=lifespan)
app.add_middleware(AuditMiddleware, service_name="snapshot-service")
from shared.auth_enforcement import AuthMiddleware, require_admin, confine_guest_path, is_admin  # Ops/Data Check 17
import re as _authz_re
from starlette.requests import Request as _AuthzRequest

_SNAP_ID_RE = _authz_re.compile(r"/resources/snapshots/([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})")
_ITEM_ID_RE = _authz_re.compile(r"/(?:snapshot-items|items)/([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})")
_HOSTED_TID_RE = _authz_re.compile(r"/teams/hosted-content/([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})")


async def _snapshot_tenant_guard(request: _AuthzRequest):
    """Ops/Data Check 17 — close the authenticated cross-tenant IDOR on the
    snapshot content routes. AuthMiddleware has already verified a principal;
    this app-level dependency resolves the snapshot / item / hosted-content id
    in the path to its owning tenant and 403s if the caller is not entitled to
    that tenant. Admin and internal principals are exempt. Runs one bounded DB
    lookup; routes that name no id (e.g. /health, folder listings) pass through.
    """
    user = request.scope.get("tmvault_user")
    if not user or is_admin(user) or user.get("_principal") == "internal":
        return
    entitled = {str(t) for t in (user.get("tenant_ids") or [])}
    path = request.url.path
    tid = None
    try:
        async with async_session_factory() as _db:
            m3 = _HOSTED_TID_RE.search(path)
            m1 = _SNAP_ID_RE.search(path)
            m2 = _ITEM_ID_RE.search(path)
            if m3:
                tid = m3.group(1)
            elif m1:
                snap = await _db.get(Snapshot, UUID(m1.group(1)))
                if snap:
                    res = await _db.get(Resource, snap.resource_id)
                    tid = str(res.tenant_id) if res and res.tenant_id else None
            elif m2:
                it = await _db.get(SnapshotItem, UUID(m2.group(1)))
                if it:
                    snap = await _db.get(Snapshot, it.snapshot_id)
                    if snap:
                        res = await _db.get(Resource, snap.resource_id)
                        tid = str(res.tenant_id) if res and res.tenant_id else None
    except Exception:
        # A malformed id or a transient DB error must not fail OPEN — but it
        # also must not mask a real 404 the handler would give. Only enforce
        # when we positively resolved a tenant; otherwise let the handler run
        # (it still requires a valid principal via the middleware).
        return
    if tid is not None and tid not in entitled:
        raise HTTPException(status_code=403, detail="Not entitled to the requested resource's tenant")
from shared.csv_safe import SafeCsvWriter  # Ops/Data Check 17: CSV formula-injection
app.add_middleware(AuthMiddleware, service_name="snapshot-service", enforce_tenant_scope=True)
# Ops/Data Check 17: per-request tenant-entitlement on every route that names a
# snapshot/item/hosted-content id (closes the authenticated cross-tenant IDOR).
app.router.dependencies.append(Depends(_snapshot_tenant_guard))


def _item_to_response(item: SnapshotItem) -> SnapshotItemResponse:
    return SnapshotItemResponse(
        id=str(item.id),
        snapshotId=str(item.snapshot_id),
        externalId=item.external_id,
        itemType=item.item_type,
        name=item.name,
        folderPath=item.folder_path,
        contentSize=item.content_size or 0,
        metadata=item.extra_data or {},
        isDeleted=item.is_deleted or False,
        createdAt=item.created_at.isoformat() if item.created_at else "",
        blobPath=item.blob_path,
    )


async def _catalogue_snapshot(db, snapshot_id, *, resolve_chat_sibling=False):
    """Select one captured restore point; a catalogue never unions history."""
    source = await db.get(Snapshot, UUID(str(snapshot_id)))
    if source is None:
        raise HTTPException(404, detail="Snapshot not found")
    if (source.extra_data or {}).get("chat_inventory_format") == 1:
        if source.status == SnapshotStatus.PENDING_DELETION:
            raise HTTPException(404, detail="Snapshot is being deleted")
        return source
    if not resolve_chat_sibling:
        return None
    resource = await db.get(Resource, source.resource_id)
    if resource is not None and resource.type == ResourceType.USER_CHATS:
        return source
    if resource is None:
        return None
    parent_id = resource.id if resource.type == ResourceType.ENTRA_USER else resource.parent_resource_id
    if parent_id is None:
        return None
    sibling_id = await db.scalar(select(Resource.id).where(
        Resource.parent_resource_id == parent_id, Resource.type == ResourceType.USER_CHATS,
        Resource.tenant_id == resource.tenant_id))
    if sibling_id is None:
        return None
    candidates = select(Snapshot).where(Snapshot.resource_id == sibling_id,
        Snapshot.status.in_([SnapshotStatus.COMPLETED, SnapshotStatus.PARTIAL]))
    selected = await db.scalar(candidates.where(Snapshot.job_id == source.job_id)
        .order_by(Snapshot.created_at.desc(), Snapshot.id.desc()).limit(1))
    source_job = await db.get(Job, source.job_id) if source.job_id else None
    batch_id = (source_job.spec or {}).get("batch_id") if source_job else None
    if selected is None and batch_id:
        selected = await db.scalar(candidates.join(Job, Job.id == Snapshot.job_id)
            .where(Job.spec["batch_id"].as_string() == str(batch_id))
            .order_by(Snapshot.created_at.desc(), Snapshot.id.desc()).limit(1))
    if selected is None:
        selected = await db.scalar(candidates.where(Snapshot.created_at <= source.created_at)
            .order_by(Snapshot.created_at.desc(), Snapshot.id.desc()).limit(1))
    if selected is None:
        raise HTTPException(404, detail="No chat restore point exists for the selected backup")
    return selected


async def _assert_catalogue_links(db, snapshot):
    bad = await db.scalar(select(SnapshotItem.id).outerjoin(ChatItemVersion,
        ChatItemVersion.id == SnapshotItem.chat_version_id).where(SnapshotItem.snapshot_id == snapshot.id,
        or_(ChatItemVersion.id.is_(None), ChatItemVersion.tenant_id != SnapshotItem.tenant_id,
            ChatItemVersion.logical_key != SnapshotItem.external_id,
            ChatItemVersion.item_type != SnapshotItem.item_type,
            SnapshotItem.extra_data["chat_id"].as_string().is_(None),
            ChatItemVersion.chat_id != SnapshotItem.extra_data["chat_id"].as_string())).limit(1))
    if bad is not None:
        raise ChatCatalogueIntegrityError("new-format snapshot link has no owned captured version")


async def _catalogue_page(db, snapshot, *, page, size, item_type=None, chat_id=None,
                          folder=None, query=None, messages_only=False):
    # A nullable broken link must not vanish inside the version JOIN/filter.
    await _assert_catalogue_links(db, snapshot)
    stmt = select(SnapshotItem).join(ChatItemVersion,
        ChatItemVersion.id == SnapshotItem.chat_version_id).where(SnapshotItem.snapshot_id == snapshot.id)
    if messages_only:
        stmt = stmt.where(SnapshotItem.item_type.in_(["TEAMS_CHAT_MESSAGE", "TEAMS_MESSAGE", "TEAMS_MESSAGE_REPLY"]))
    if item_type:
        stmt = stmt.where(SnapshotItem.item_type == item_type)
    if chat_id:
        stmt = stmt.where(ChatItemVersion.chat_id == chat_id)
    if folder is not None:
        stmt = stmt.where(SnapshotItem.folder_path == folder)
    if query:
        stmt = stmt.where(or_(SnapshotItem.name.ilike(f"%{query}%"),
            ChatItemVersion.graph_id.ilike(f"%{query}%"),
            ChatItemVersion.payload["body"]["content"].as_string().ilike(f"%{query}%")))
    total = await db.scalar(select(func.count()).select_from(stmt.order_by(None).subquery())) or 0
    # Source-created timestamps are immutable; the item UUID breaks ties.
    stmt = stmt.order_by(cast(ChatItemVersion.payload["createdDateTime"].as_string(), DateTime(timezone=True)).desc().nullslast(),
        SnapshotItem.created_at.desc(), SnapshotItem.id.desc()).offset((page - 1) * size).limit(size)
    links = list((await db.scalars(stmt)).all())
    return await materialize_chat_items(db, links), total


async def _catalogue_item_response(db, snapshot, *, page, size, item_type=None, folder=None, query=None):
    items, total = await _catalogue_page(db, snapshot, page=page, size=size,
        item_type=item_type, folder=folder, query=query)
    return SnapshotItemListResponse(content=[_item_to_response(i) for i in items],
        totalElements=total, totalPages=max(1, (total + size - 1) // size), size=size, number=page)


async def _catalogue_groups(db, snapshot):
    await _assert_catalogue_links(db, snapshot)
    messages = ["TEAMS_CHAT_MESSAGE", "TEAMS_MESSAGE", "TEAMS_MESSAGE_REPLY"]
    rows = (await db.execute(select(SnapshotChatInventory.chat_id,
        ChatCaptureAttempt.folder_path, func.count(SnapshotItem.id).label("count"),
        func.max(cast(ChatItemVersion.payload["createdDateTime"].as_string(), DateTime(timezone=True))).label("last_message_at"))
        .join(ChatCaptureAttempt, ChatCaptureAttempt.id == SnapshotChatInventory.capture_attempt_id)
        .outerjoin(SnapshotItem, and_(SnapshotItem.snapshot_id == SnapshotChatInventory.snapshot_id,
            SnapshotItem.extra_data["chat_id"].as_string() == SnapshotChatInventory.chat_id,
            SnapshotItem.item_type.in_(messages)))
        .outerjoin(ChatItemVersion, ChatItemVersion.id == SnapshotItem.chat_version_id)
        .where(SnapshotChatInventory.snapshot_id == snapshot.id)
        .group_by(SnapshotChatInventory.chat_id, ChatCaptureAttempt.folder_path)
        .order_by(func.max(cast(ChatItemVersion.payload["createdDateTime"].as_string(), DateTime(timezone=True))).desc().nullslast(),
            SnapshotChatInventory.chat_id))).all()
    return [{"chatId": r.chat_id, "displayName": (r.folder_path or f"chats/{r.chat_id}").removeprefix("chats/"),
        "folderPath": r.folder_path or f"chats/{r.chat_id}", "count": int(r.count),
        "lastMessageAt": r.last_message_at.isoformat() if r.last_message_at else None} for r in rows]


async def _catalogue_folders(db, snapshot, *, page, size, item_type=None):
    await _assert_catalogue_links(db, snapshot)
    stmt = select(SnapshotItem.folder_path.label("path"), func.count().label("count")).where(
        SnapshotItem.snapshot_id == snapshot.id)
    stmt = stmt.where(SnapshotItem.item_type == item_type) if item_type else stmt.where(
        SnapshotItem.item_type.in_(["TEAMS_CHAT_MESSAGE", "TEAMS_MESSAGE", "TEAMS_MESSAGE_REPLY"]))
    stmt = stmt.group_by(SnapshotItem.folder_path)
    total = await db.scalar(select(func.count()).select_from(stmt.subquery())) or 0
    rows = (await db.execute(stmt.order_by(SnapshotItem.folder_path).offset((page - 1) * size).limit(size))).all()
    return {"content": [{"path": r.path or "", "count": int(r.count)} for r in rows],
        "total": total, "page": page, "size": size, "hasMore": page * size < total}


async def _catalogue_blob_response(db, item, *, download):
    """Stream exact captured bytes while a durable object pin protects them."""
    import hashlib
    import mimetypes
    from urllib.parse import quote
    from starlette.background import BackgroundTask
    from shared.chat_catalogue_gc import catalogue_read_pin
    from shared.storage.router import router
    meta = item.extra_data or {}
    if not all(meta.get(key) for key in ("object_id", "container", "sha256")):
        raise ChatCatalogueIntegrityError("captured content storage descriptor is incomplete")
    manager = catalogue_read_pin(async_session_factory, snapshot_ids=[item.snapshot_id],
        object_ids=[UUID(str(meta["object_id"]))])
    lease = await manager.__aenter__()
    closed = False
    async def close_pin():
        nonlocal closed
        if not closed:
            closed = True
            await manager.__aexit__(None, None, None)
    try:
        store = router.get_store_by_id(str(item.backend_id))
        properties = await store.get_properties(meta["container"], item.blob_path)
        if properties is None:
            raise HTTPException(404, detail="Captured content is missing from storage")
        snapshot = await db.get(Snapshot, item.snapshot_id)
        lease.assert_valid()
    except BaseException:
        await close_pin()
        raise
    declared = meta.get("content_type") or ""
    media = declared if "/" in declared else mimetypes.guess_type(item.name or "")[0]
    media = media or "application/octet-stream"
    safe_image = media.lower().split(";", 1)[0] in {"image/png", "image/jpeg", "image/gif", "image/webp", "image/bmp"}
    headers = {"X-Content-Type-Options": "nosniff", "Content-Length": str(item.content_size or 0),
        "ETag": f'"{meta["sha256"]}"', "Cache-Control": "private, max-age=3600, immutable"}
    if not safe_image:
        media = "application/octet-stream"
        headers["Content-Disposition"] = "attachment"
    if download:
        filename = item.name or f"item-{item.id}"
        ascii_name = filename.encode("ascii", "replace").decode().replace('"', "'").replace("\r", "").replace("\n", "")
        headers["Content-Disposition"] = f"attachment; filename=\"{ascii_name}\"; filename*=UTF-8''{quote(filename)}"
    async def stream():
        digest, size, pending = hashlib.sha256(), 0, None
        try:
            async for chunk in lease.guard(store.download_stream(meta["container"], item.blob_path)):
                if not chunk:
                    continue
                digest.update(chunk)
                size += len(chunk)
                if size > item.content_size:
                    raise ChatCatalogueIntegrityError("captured content length differs during download")
                if pending is not None:
                    yield pending
                pending = chunk
            if size != item.content_size or digest.hexdigest() != meta["sha256"]:
                raise ChatCatalogueIntegrityError("captured content checksum differs during download")
            lease.assert_valid()
            # Withhold the final chunk until integrity is known; failed streams
            # cannot satisfy Content-Length with corrupt captured bytes.
            if pending is not None:
                yield pending
            if download:
                await _emit_download_audit(action="FILE_DOWNLOADED", outcome="SUCCESS",
                    tenant_id=str(item.tenant_id), resource_id=str(snapshot.resource_id) if snapshot else None,
                    resource_type=item.item_type, snapshot_id=str(item.snapshot_id),
                    details={"itemId": str(item.id), "filename": item.name, "byteSize": size,
                        "sha256": meta["sha256"]})
        finally:
            await close_pin()
    return StreamingResponse(stream(), media_type=media, headers=headers, background=BackgroundTask(close_pin))


async def _catalogue_message_response(db, snapshot, *, page, size, chat_id=None, folder=None, search=None):
    items, total = await _catalogue_page(db, snapshot, page=page, size=size,
        chat_id=chat_id, folder=folder, query=search, messages_only=True)
    # Page-sized child load; links are joined by qualified physical message key.
    ids = [i.id for i in items]
    parent = aliased(SnapshotItem)
    children = list((await db.scalars(select(SnapshotItem).join(parent,
        and_(parent.snapshot_id == SnapshotItem.snapshot_id,
            parent.external_id == SnapshotItem.parent_external_id)).where(parent.id.in_(ids),
        SnapshotItem.item_type == "CHAT_HOSTED_CONTENT"))).all()) if ids else []
    child_items = await materialize_chat_items(db, children)
    hosted = {}
    for child in child_items:
        meta = child.extra_data or {}
        hosted[(meta.get("chat_id"), child.parent_external_id, child.external_id)] = str(child.id)
    result = []
    from urllib.parse import unquote
    for item in items:
        raw = (item.extra_data or {}).get("raw") or {}
        raw = dict(raw)
        body = dict(raw.get("body") or {})
        cid = item.extra_data.get("chatId") or item.parent_external_id
        def replace_hosted(match):
            child_id = hosted.get((cid, item.external_id, unquote(match.group(1))))
            if child_id is None:
                raise ChatCatalogueIntegrityError("captured message references missing hosted content")
            return f"/api/v1/resources/snapshots/{snapshot.id}/items/{child_id}/content"
        body["content"] = _HOSTED_CONTENT_RE.sub(replace_hosted, body.get("content") or "")
        raw["body"] = body
        sender = (raw.get("from") or {}).get("user") or {}
        result.append({"id": str(item.id), "snapshotId": str(item.snapshot_id),
            "externalId": item.external_id, "itemType": item.item_type,
            "sender": sender.get("displayName") or "Unknown", "senderEmail": sender.get("userPrincipalName") or sender.get("id") or "",
            "body": body.get("content") or "", "bodyContentType": body.get("contentType") or "text",
            "date": raw.get("createdDateTime"), "chatTopic": (item.folder_path or f"chats/{cid}").removeprefix("chats/"),
            "channelName": "", "folderPath": item.folder_path, "attachments": raw.get("attachments") or [],
            "mentions": raw.get("mentions") or [], "isDeleted": bool(raw.get("deletedDateTime")) or bool(item.is_deleted),
            "isReply": bool(raw.get("replyToId")), "contentSize": item.content_size or 0,
            "createdAt": item.created_at.isoformat() if item.created_at else "", "name": sender.get("displayName") or "Unknown",
            "metadata": {"raw": raw}})
    return {"content": result, "totalElements": total, "totalPages": max(1, (total + size - 1) // size),
        "size": size, "number": page}


async def _get_power_bi_assembled_items(db: AsyncSession, snapshot_id: str) -> Optional[List[SnapshotItem]]:
    snapshot = await db.get(Snapshot, UUID(snapshot_id))
    if not snapshot:
        return None

    resource = await db.get(Resource, snapshot.resource_id)
    if not resource:
        return None

    resource_type = resource.type.value if hasattr(resource.type, "value") else str(resource.type)
    if resource_type != "POWER_BI":
        return None

    snapshots_result = await db.execute(
        select(Snapshot).where(
            Snapshot.resource_id == snapshot.resource_id,
            Snapshot.status.in_([SnapshotStatus.COMPLETED, SnapshotStatus.PARTIAL]),
            Snapshot.created_at <= snapshot.created_at,
        ).order_by(Snapshot.created_at.asc())
    )
    snapshots = snapshots_result.scalars().all()
    if not snapshots:
        return []

    snapshot_ids = [s.id for s in snapshots]
    items_result = await db.execute(
        select(SnapshotItem).where(SnapshotItem.snapshot_id.in_(snapshot_ids)).order_by(SnapshotItem.created_at.asc())
    )
    return assemble_power_bi_items(snapshots, items_result.scalars().all(), up_to_snapshot_id=snapshot_id)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "snapshot"}


@app.get("/api/v1/resources/{resource_id}/snapshots")
async def list_snapshots(
    resource_id: str,
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1, le=1000),
    include_children: bool = Query(False, description="Also include snapshots of child resources (Tier 2 USER_MAIL/ONEDRIVE/etc. under an ENTRA_USER parent). Needed so the Recovery sparkline can plot the actual content bytes instead of the parent's tiny metadata row."),
    include_failed: bool = Query(False, description="Include FAILED / RUNNING snapshots. Default off — the version dropdown only wants successful, recoverable points in time."),
    include_empty: bool = Query(False, description="Include INCREMENTAL snapshots that captured 0 items (no delta since the prior snapshot). Default off — they would otherwise pollute the version dropdown with duplicates that present the same data as the previous entry."),
    db: AsyncSession = Depends(get_db),
):
    # Build the set of resource IDs whose snapshots we want. By default
    # just the requested resource; with include_children=true we also
    # pull the children linked via parent_resource_id (the Tier 2 fan-out
    # from ENTRA_USER).
    target_ids = [UUID(resource_id)]
    if include_children:
        child_rows = (await db.execute(
            select(Resource.id).where(Resource.parent_resource_id == UUID(resource_id))
        )).all()
        target_ids.extend(r[0] for r in child_rows)

    filters = [Snapshot.resource_id.in_(target_ids)]
    # By default the version dropdown should show only RECOVERABLE
    # points in time. Hide failed + empty-delta snapshots which both
    # surface as confusing "duplicate" versions in the UI dropdown
    # (failed = nothing to restore, empty incremental = same data as
    # the prior entry). Admin / debug callers can opt-in via query.
    if not include_failed:
        # PARTIAL is a RECOVERABLE point in time — some content was missed,
        # but everything that landed is restorable — so it belongs in the
        # dropdown. This used to be a bare `== COMPLETED`, which swept
        # PARTIAL up with FAILED/RUNNING under a flag named `include_failed`
        # and hid EVERY degraded restore point from Recovery. Live on
        # 2026-08-11: SHAREPOINT_SITE "IT OPS NOTES" had PARTIAL snapshots
        # for its three most recent runs and the version dropdown offered
        # none of them, so the newest browsable backup was 2 days stale.
        # Expect more PARTIAL rows here from 9a861f5 onward, which stops the
        # reconciler overwriting a handler-declared degraded_reason — that
        # is honesty, and honest rows must stay reachable.
        filters.append(
            Snapshot.status.in_([SnapshotStatus.COMPLETED, SnapshotStatus.PARTIAL])
        )
    if not include_empty:
        filters.append(Snapshot.item_count > 0)

    # Per-bulk-run dedup. One "Backup now" click for a user fans out to
    # THREE Jobs sharing the same `spec.batch_id`: Tier-1 ENTRA_USER bulk,
    # Tier-2 mail/chats/cal/contacts bulk, and Tier-2 OneDrive bulk in
    # the heavy lane. Each Job creates its own per-resource snapshots —
    # and partitioned workloads (USER_CHATS, USER_MAIL, SharePoint) ALSO
    # produce a coordinator snapshot plus N partition-shard snapshots
    # for the same (job_id, resource_id) pair. From the Recovery UI's
    # POV all of these are a single backup attempt and should collapse
    # to one entry per (batch_id, resource_id). The previous (job_id,
    # resource_id) partition was the right level of dedup BEFORE the
    # batch_id propagation work landed — now it over-counts: Narendra
    # with 2 clicks showed 5 "versions" because batch_1's tier-1, mail/
    # chats/cal/contacts tier-2, OneDrive tier-2, plus batch_2's tier-1
    # and tier-2 were 5 distinct job_ids.
    #
    # Partition key COALESCE order:
    #   1. spec.batch_id  — one click, even across multiple Jobs
    #   2. snapshot.job_id — pre-batch-id legacy snapshots
    #   3. snapshot.id    — manual / ad-hoc snapshots (no job)
    # NULL job_id snapshots stay singletons via the snapshot.id fallback.
    #
    # The resource_id half of the partition is non-negotiable: without
    # it, calls without a resource_id filter (admin / search) would
    # collapse N different users' snapshots from one bulk into a single
    # row.
    from sqlalchemy import func as _func
    from sqlalchemy import case as _case
    # NOTE: Job.spec is a plain JSON column, not JSONB — `.astext` is
    # JSONB-only and raises AttributeError on Comparator. Use
    # json_extract_path_text which works for both JSON and JSONB.
    # `batch_id_raw` is the unmunged batch_id (may be NULL); `dedup_group`
    # is the COALESCE used for ranking. Surfacing batch_id_raw on the
    # response lets the Recovery UI's version-dropdown collapse cross-
    # resource children of one bulk click into a single entry — the
    # 3-Job fan-out (Tier-1 + Tier-2-urgent + Tier-2-heavy) shares
    # batch_id but has 3 distinct job_ids, so jobId alone over-counts.
    snap_with_batch = (
        select(
            Snapshot,
            _func.json_extract_path_text(Job.spec, "batch_id").label("batch_id_raw"),
            _func.coalesce(
                _func.json_extract_path_text(Job.spec, "batch_id"),
                _func.cast(Snapshot.job_id, String),
                _func.cast(Snapshot.id, String),
            ).label("dedup_group"),
        )
        .select_from(Snapshot)
        .outerjoin(Job, Job.id == Snapshot.job_id)
        .where(*filters)
        .subquery()
    )
    row_num = _func.row_number().over(
        partition_by=(
            snap_with_batch.c.dedup_group,
            snap_with_batch.c.resource_id,
        ),
        order_by=(
            snap_with_batch.c.item_count.desc().nullslast(),
            # Tiebreak on status before recency: now that PARTIAL rows are
            # candidates (see the include_failed filter above), a group can
            # hold a COMPLETED and a PARTIAL row with the SAME item_count,
            # and which one won would otherwise depend on created_at alone.
            # A clean restore point must never lose to an equally-full
            # degraded one. Ordering item_count first is still deliberate:
            # within one bulk click the shard/coordinator rows differ in
            # completeness, and the fullest row is the best restore point
            # even when it is the PARTIAL one.
            _case(
                (snap_with_batch.c.status == SnapshotStatus.COMPLETED, 0),
                else_=1,
            ).asc(),
            snap_with_batch.c.created_at.desc(),
        ),
    ).label("dedup_rank")
    ranked = (
        select(snap_with_batch, row_num)
        .subquery()
    )
    snap_alias = aliased(Snapshot, ranked)
    deduped_filter = ranked.c.dedup_rank == 1

    total = (await db.execute(
        select(_func.count())
        .select_from(ranked)
        .where(deduped_filter)
    )).scalar() or 0
    stmt = (
        select(
            snap_alias,
            ranked.c.batch_id_raw,
        )
        .where(deduped_filter)
        # id tiebreak: one bulk click commits every shard's snapshot row in
        # the same transaction, so created_at ties are the norm here and a
        # non-unique sort key lets LIMIT/OFFSET slice the tie group
        # differently per request.
        .order_by(snap_alias.created_at.desc(), snap_alias.id.desc())
        .offset((page - 1) * size)
        .limit(size)
    )
    result = await db.execute(stmt)
    rows = result.all()

    return SnapshotListResponse(
        content=[
            SnapshotResponse(
                id=str(s.id), resourceId=str(s.resource_id),
                createdAt=s.created_at.isoformat() if s.created_at else "",
                size=s.bytes_total or 0,
                status=s.status.value if hasattr(s.status, 'value') else str(s.status),
                type=s.type.value if hasattr(s.type, 'value') else str(s.type),
                itemCount=s.item_count or 0,
                jobId=str(s.job_id) if s.job_id else None,
                batchId=batch_id_raw or None,
            )
            for (s, batch_id_raw) in rows
        ],
        totalPages=max(1, (total + size - 1) // size),
        totalElements=total,
        size=size, number=page,
    )


@app.get("/api/v1/resources/{resource_id}/storage-summary")
async def storage_summary(
    resource_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Authoritative "actually-used storage" rollup for the protection tab.

    Why this exists: the frontend was computing the headline "Backup size"
    + 1w/1m/1y deltas + per-day sparkline by summing snapshot.bytes_total
    on the client. That number is wrong twice over:
      1. bytes_total is *cumulative content size* of a snapshot's view —
         re-summing it across snapshots double-counts unchanged items
         that the next incremental snapshot still references.
      2. ENTRA_USER backups fan out into sibling per-content-type
         snapshots (USER_MAIL/CALENDAR/CONTACTS/ONEDRIVE/CHATS) that
         all live under the same parent. Without subtree awareness the
         parent looks empty (~8 KB metadata) while the actual user data
         lives on the children.

    Correct metric = sum of bytes_added across non-failed snapshots in
    the parent + child subtree. bytes_added is the bytes the worker
    actually wrote to object storage on that backup run, so summing
    across all retained snapshots gives the true on-disk footprint.

    Returns headline `totalBytes` + 1w/1m/1y window deltas + a 7-day
    daily series (today-3 .. today+3). Single round-trip, all sums
    derived from one query.
    """
    from datetime import timedelta

    rid = UUID(resource_id)

    # Resource subtree = self + direct children (Tier-2 USER_* fan-out
    # under ENTRA_USER, content-type leaves under SHAREPOINT_SITE, etc.)
    child_rows = (await db.execute(
        select(Resource.id).where(Resource.parent_resource_id == rid)
    )).all()
    target_ids = [rid] + [r[0] for r in child_rows]

    # Pull (started_at, bytes_added) for every snapshot whose data is
    # still on disk. IN_PROGRESS rows might still be writing — exclude
    # so a half-done backup doesn't inflate the total. FAILED rows had
    # their bytes cleaned up by backup_cleanup. PENDING_DELETION is
    # awaiting GC — its bytes still exist on disk so we count them.
    rows = (await db.execute(
        select(Snapshot.started_at, Snapshot.bytes_added).where(
            Snapshot.resource_id.in_(target_ids),
            Snapshot.status.in_([
                SnapshotStatus.COMPLETED,
                SnapshotStatus.PARTIAL,
                SnapshotStatus.PENDING_DELETION,
            ]),
        )
    )).all()

    pairs = [(r[0], int(r[1] or 0)) for r in rows if r[0] is not None]

    # Headline = PROTECTED-DATA size: newest-version content_size per
    # live item across the subtree. Summing bytes_added undercounts
    # twice — content-hash dedup means a re-baseline uploads almost
    # nothing (jrue's 124GB force-full added 16GB), and retention
    # pruning drops the original baseline's bytes from the sum while
    # its blobs stay referenced by newer snapshots' items. Observed
    # 2026-07-14: headline read 32.9GB against a 124GB OneDrive.
    # bytes_added remains correct for the deltas/sparkline below —
    # "new data written this period" is exactly what it measures.
    total_bytes = 0
    for target_id in target_ids:
        snap_rows = (await db.execute(
            select(Snapshot.id).where(
                Snapshot.resource_id == target_id,
                Snapshot.status.in_([
                    SnapshotStatus.COMPLETED,
                    SnapshotStatus.PARTIAL,
                    SnapshotStatus.PENDING_DELETION,
                ]),
            ).order_by(Snapshot.created_at.desc())
        )).all()
        if not snap_rows:
            continue
        sib_ids = [r[0] for r in snap_rows]
        # sib_ids[0] = latest snapshot -> cache key; entries self-
        # invalidate when a newer snapshot completes.
        _, child_bytes = await _rollup_latest_items(
            db, target_id, sib_ids[0], sib_ids, None,
        )
        total_bytes += child_bytes

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    week_cut = now - timedelta(days=7)
    month_cut = now - timedelta(days=30)
    year_cut = now - timedelta(days=365)

    deltas = {
        "week": sum(b for ts, b in pairs if ts >= week_cut),
        "month": sum(b for ts, b in pairs if ts >= month_cut),
        "year": sum(b for ts, b in pairs if ts >= year_cut),
    }

    # 7-day daily series anchored on today (today-3 .. today+3) so the
    # sparkline lines up with the calendar in the user's timezone-naive
    # representation. Future days render as null on the FE so we emit
    # null, not 0, for them.
    today_midnight = datetime(now.year, now.month, now.day)
    daily_series = []
    for offset in range(-3, 4):
        day_start = today_midnight + timedelta(days=offset)
        day_end = day_start + timedelta(days=1)
        added = sum(b for ts, b in pairs if day_start <= ts < day_end)
        is_future = day_start > today_midnight
        daily_series.append({
            "date": day_start.date().isoformat(),
            "bytesAdded": added if (added or not is_future) else None,
            "isFuture": is_future,
        })

    return {
        "resourceId": str(rid),
        "subtreeSize": len(target_ids),
        "totalBytes": total_bytes,
        "deltas": deltas,
        "dailySeries": daily_series,
    }


# Cross-sibling auto-resolve for /snapshots/folders: when the caller
# scopes by item type but passed a different resource's snapshot id
# (typically the ENTRA_USER parent's), swap to the Tier-2 child that
# actually owns rows of that type. ONEDRIVE_FILE/FILE were missing
# until 2026-07-13 — the Recovery OneDrive tab (which sends the parent
# snapshot id) got the parent's junk folder rows back, so My Drive
# rendered files but zero folders. Pinned by
# tests/services/test_snapshot_folder_type_resolve.py.
FOLDER_ITEM_TYPE_TO_CHILD: Dict[str, "ResourceType"] = {
    "EMAIL": ResourceType.USER_MAIL,
    "TEAMS_CHAT_MESSAGE": ResourceType.USER_CHATS,
    "TEAMS_MESSAGE": ResourceType.USER_CHATS,
    "TEAMS_MESSAGE_REPLY": ResourceType.USER_CHATS,
    "CALENDAR_EVENT": ResourceType.USER_CALENDAR,
    "USER_CONTACT": ResourceType.USER_CONTACTS,
    "CONTACT": ResourceType.USER_CONTACTS,
    "ONEDRIVE_FILE": ResourceType.USER_ONEDRIVE,
    "FILE": ResourceType.USER_ONEDRIVE,
    "ARCHIVE_ITEM": ResourceType.USER_ARCHIVE,
}


@app.get("/api/v1/resources/snapshots/folders")
async def get_snapshot_folders(
    snapshot_id: str = Query(...),
    item_type: Optional[str] = Query(None),
    category: Optional[str] = Query(None, description="With item_type=ARCHIVE_ITEM: mail|contacts|calendar — count only that kind"),
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    """Get distinct folder paths for items in a snapshot, optionally filtered by item type.

    Resolution order per item:
      1. `folder_path` column if populated (newer Tier 2 backups + legacy mailbox/onedrive handlers).
      2. `extra_data.raw.parentReference.path` for OneDrive / SharePoint files (cleaned of the "/drive/root:" prefix).
         Reference: Microsoft Graph driveItem `parentReference.path` —
         https://learn.microsoft.com/graph/api/resources/itemreference
      3. `extra_data.raw.parentFolderId` for mail items — opaque ID until we
         can resolve folder names; better than one giant bucket.

    Falling back inline like this avoids a one-time backfill migration and
    keeps the endpoint useful for snapshots taken before the folder_path
    plumbing landed."""
    catalogue = await _catalogue_snapshot(db, snapshot_id,
        resolve_chat_sibling=item_type in {"TEAMS_CHAT_MESSAGE", "TEAMS_MESSAGE", "TEAMS_MESSAGE_REPLY"})
    if catalogue is not None:
        if (catalogue.extra_data or {}).get("chat_inventory_format") == 1:
            return await _catalogue_folders(db, catalogue, page=page, size=size, item_type=item_type)
        snapshot_id = str(catalogue.id)

    # Archive folders are shared across kinds (one physical folder can hold
    # mail + contacts + events), so a kind-agnostic count would show the mail
    # total under the Contacts tab. Count per-category using the SAME MAPI-kind
    # match as the item view so the folder count matches what opening it shows.
    if (item_type or "").upper() == "ARCHIVE_ITEM" and category:
        return await _serve_archive_folders(db, snapshot_id, category)
    assembled_items = await _get_power_bi_assembled_items(db, snapshot_id)
    if assembled_items is not None:
        filtered = [item for item in assembled_items if not item_type or item.item_type == item_type]
        counts: dict[str, int] = {}
        for item in filtered:
            path = item.folder_path or ""
            counts[path] = counts.get(path, 0) + 1
        all_sorted = [{"path": path, "count": count} for path, count in sorted(counts.items())]
        total = len(all_sorted)
        off = (page - 1) * size
        return {"content": all_sorted[off: off + size], "total": total, "page": page, "size": size, "hasMore": off + size < total}

    # Aggregate across every sibling snapshot of this resource so the
    # left-panel folder list reflects the full running state (mail,
    # chats, calendar all use delta backups — the latest snapshot alone
    # may hold zero or near-zero items after a successful delta).
    # Cross-sibling auto-resolve by item_type: when the caller wants a
    # folder list scoped to EMAIL / TEAMS_CHAT_MESSAGE / CALENDAR_EVENT /
    # USER_CONTACT but passed the ENTRA_USER parent's snapshot id (or
    # the sibling Contacts snapshot id, etc.), swap to the Tier 2 child
    # of the matching type. Without this, the mail folders tab was
    # empty even when the USER_MAIL snapshot had 39 folders + 967 items.
    target_child = FOLDER_ITEM_TYPE_TO_CHILD.get(item_type or "")
    sibling_ids = await _resolve_sibling_snapshot_ids(
        db, snapshot_id, target_child_type=target_child,
    )
    if not sibling_ids:
        sibling_ids = [UUID(snapshot_id)]
    filters = [SnapshotItem.snapshot_id.in_(sibling_ids)]
    if item_type:
        filters.append(SnapshotItem.item_type == item_type)
    else:
        # Exclude child/attachment rows from folder counts. They share
        # their parent's folder_path (so the mailbox message and its
        # attachment both live under "/Inbox", and a chat message and
        # its attachment both live under "chats/<chat name>"), but the
        # middle-panel list endpoints only show primary items. If we
        # include attachments in the folder count, the user sees "62" in
        # the left panel and "50 of 50" in the middle and thinks the
        # middle is wrong — it's the folder count that was over-counting.
        filters.append(SnapshotItem.item_type.notin_(
            ["EMAIL_ATTACHMENT", "CHAT_ATTACHMENT"]
        ))

    # PERF: push the whole aggregation into Postgres. For a 12k-item
    # resource, the previous Python-side dedup + count added ~900 ms on
    # top of the 100 ms query. SQL-side DISTINCT ON + COUNT runs in a
    # single pass and returns ~90 rows instead of 12,500.
    #
    # DISTINCT ON (external_id) with ORDER BY external_id, created_at
    # DESC picks the newest snapshot's version of each item, then we
    # GROUP BY its folder_path to get per-bucket counts.
    from sqlalchemy import text as _text
    _sibling_uuids = list(sibling_ids)
    if not _sibling_uuids:
        all_sorted: list = []
    else:
        # _sibling_uuids are uuid.UUID objects. Psycopg binds them fine
        # through the `snapshots` param when we use ANY(:snaps).
        _CHAT_ITEM_TYPES = ("TEAMS_CHAT_MESSAGE", "TEAMS_MESSAGE", "TEAMS_MESSAGE_REPLY")
        if item_type in _CHAT_ITEM_TYPES:
            # Chat folders ARE chats, and the per-chat count must match the
            # message view (the durable union in list_snapshot_messages). A
            # shared 1:1/group chat is ONE durable store + ONE cursor but lives
            # in every member's chat list; whichever member's drain ran first
            # captured the messages into THAT member's snapshot, so counting
            # only this snapshot's pointers showed e.g. 2993 for one member vs
            # 3010 for the other on the SAME chat (the data is complete in the
            # shared store — only this per-snapshot COUNT was short). Count
            # distinct message ids over the snapshot pointers (A) UNION the
            # shared durable chat_thread_messages (B); attribute BOTH to the
            # chat's representative folder_path (one row per chat via `scope`)
            # and scope B to the chats this snapshot covers so nothing leaks.
            sql = _text("""
                WITH scope AS (
                  SELECT si.tenant_id, si.parent_external_id AS chat_id,
                         MAX(COALESCE(si.folder_path, '')) AS path
                  FROM snapshot_items si
                  WHERE si.snapshot_id = ANY(:snaps)
                    AND si.item_type = :itype
                    AND si.parent_external_id IS NOT NULL
                  GROUP BY si.tenant_id, si.parent_external_id
                ),
                msg_ids AS (
                  SELECT sc.path, si.external_id AS ext
                  FROM snapshot_items si
                  JOIN scope sc
                    ON sc.tenant_id = si.tenant_id
                   AND sc.chat_id = si.parent_external_id
                  WHERE si.snapshot_id = ANY(:snaps)
                    AND si.item_type = :itype
                    AND si.parent_external_id IS NOT NULL
                  UNION
                  SELECT sc.path, ctm.message_external_id AS ext
                  FROM chat_thread_messages ctm
                  JOIN chat_threads ct
                    ON ct.id = ctm.chat_thread_id AND ct.archived_at IS NULL
                  JOIN scope sc
                    ON sc.tenant_id = ct.tenant_id AND sc.chat_id = ct.chat_id
                  WHERE ctm.archived_at IS NULL
                )
                SELECT path, COUNT(DISTINCT ext) AS cnt
                FROM msg_ids
                GROUP BY path
                ORDER BY path
            """)
            rows = (await db.execute(
                sql, {"snaps": _sibling_uuids, "itype": item_type},
            )).all()
            all_sorted = [{"path": r[0], "count": int(r[1])} for r in rows]
        else:
            # Same sibling-set-version cache as _paged_items_cache: this
            # aggregation walks the full union (3.1s on a 1.22M-row mailbox)
            # and its result only changes when a sibling's rows change —
            # which flips a sibling status or adds/removes an id, both of
            # which change the fingerprint key. Non-settled sibling sets
            # (streaming backup / pending deletion) are never cached.
            _state_fp, _cacheable = await _sibling_state_fingerprint(db, _sibling_uuids)
            _fkey = (_state_fp, item_type)
            _now = _time.monotonic()
            _fhit = _folder_agg_cache.get(_fkey) if _cacheable else None
            if _fhit is not None and (_now - _fhit[0]) < _LIST_CACHE_TTL_S:
                all_sorted = _fhit[1]
            else:
                # _sibling_uuids are uuid.UUID objects. Psycopg binds them fine
                # through the `snapshots` param when we use ANY(:snaps).
                exclude_types = [] if item_type else ["EMAIL_ATTACHMENT", "CHAT_ATTACHMENT"]
                sql = _text("""
                    WITH latest AS (
                      SELECT DISTINCT ON (external_id)
                             external_id, COALESCE(folder_path, '') AS path
                      FROM snapshot_items
                      WHERE snapshot_id = ANY(:snaps)
                        AND COALESCE(is_deleted, false) = false
                        {type_filter}
                      ORDER BY external_id, created_at DESC
                    )
                    SELECT path, COUNT(*) AS cnt
                    FROM latest
                    GROUP BY path
                    ORDER BY path
                """.format(
                    type_filter=(
                        "AND item_type = :itype" if item_type
                        else "AND item_type <> ALL(:excluded)"
                    )
                ))
                params: Dict[str, Any] = {"snaps": _sibling_uuids}
                if item_type:
                    params["itype"] = item_type
                else:
                    params["excluded"] = exclude_types
                await _set_local_work_mem(db)
                rows = (await db.execute(sql, params)).all()
                all_sorted = [{"path": r[0], "count": int(r[1])} for r in rows]
                if _cacheable:
                    _cache_put(_folder_agg_cache, _FOLDER_AGG_CACHE_MAX, _fkey, (_now, all_sorted))
    total = len(all_sorted)
    off = (page - 1) * size
    return {
        "content": all_sorted[off: off + size],
        "total": total,
        "page": page,
        "size": size,
        "hasMore": off + size < total,
    }


@app.get("/api/v1/resources/snapshots/{snapshot_id}", response_model=SnapshotResponse)
async def get_snapshot(snapshot_id: str, db: AsyncSession = Depends(get_db)):
    stmt = select(Snapshot).where(Snapshot.id == UUID(snapshot_id))
    result = await db.execute(stmt)
    snapshot = result.scalar_one_or_none()
    if not snapshot:
        raise HTTPException(status_code=404, detail="Snapshot not found")

    # Pull batch_id from the Job's spec for parity with the list
    # endpoint. Same plain-JSON path used in dedup_group above.
    batch_id_raw: Optional[str] = None
    if snapshot.job_id:
        try:
            from sqlalchemy import func as _func
            batch_id_raw = (await db.execute(
                select(_func.json_extract_path_text(Job.spec, "batch_id"))
                .where(Job.id == snapshot.job_id)
            )).scalar_one_or_none()
        except Exception:
            batch_id_raw = None

    # Partition rollup — populated only when this snapshot was sharded
    # by the cross-replica partition path. Non-partitioned snapshots
    # return `partitions: null` (backward compatible). Single join +
    # GROUP BY covers all partition_types (ONEDRIVE_FILES / CHATS /
    # MAIL_FOLDERS / SHAREPOINT_DRIVES) — the discriminator is opaque
    # to the rollup; ops uses it to filter, the UI doesn't read it.
    partitions_block = None
    try:
        rows = (await db.execute(
            text(
                """
                SELECT
                  COUNT(*)                                    AS total,
                  COUNT(*) FILTER (WHERE status='QUEUED')     AS queued,
                  COUNT(*) FILTER (WHERE status='IN_PROGRESS')AS in_progress,
                  COUNT(*) FILTER (WHERE status='COMPLETED')  AS completed,
                  COUNT(*) FILTER (WHERE status='FAILED')     AS failed,
                  COALESCE(SUM(files_uploaded), 0)            AS files_uploaded,
                  COALESCE(SUM(bytes_uploaded), 0)            AS bytes_uploaded
                FROM snapshot_partitions
                WHERE snapshot_id = :sid
                """
            ),
            {"sid": str(snapshot.id)},
        )).first()
        if rows and (rows[0] or 0) > 0:
            by_region_rows = (await db.execute(
                text(
                    """
                    SELECT COALESCE(worker_region, 'unassigned') AS region,
                           COUNT(*) AS n
                      FROM snapshot_partitions
                     WHERE snapshot_id = :sid
                     GROUP BY 1
                    """
                ),
                {"sid": str(snapshot.id)},
            )).fetchall()
            partitions_block = {
                "total":          int(rows[0] or 0),
                "queued":         int(rows[1] or 0),
                "in_progress":    int(rows[2] or 0),
                "completed":      int(rows[3] or 0),
                "failed":         int(rows[4] or 0),
                "files_uploaded": int(rows[5] or 0),
                "bytes_uploaded": int(rows[6] or 0),
                "by_region": {
                    str(r[0]): int(r[1] or 0) for r in by_region_rows
                },
            }
    except Exception as _part_exc:
        # Table may not exist on older deployments; fall back to null.
        partitions_block = None

    return SnapshotResponse(
        id=str(snapshot.id), resourceId=str(snapshot.resource_id),
        createdAt=snapshot.created_at.isoformat(),
        size=snapshot.bytes_total or 0,
        status=snapshot.status.value if hasattr(snapshot.status, 'value') else str(snapshot.status),
        type=snapshot.type.value if hasattr(snapshot.type, 'value') else str(snapshot.type),
        itemCount=snapshot.item_count or 0,
        jobId=str(snapshot.job_id) if snapshot.job_id else None,
        batchId=batch_id_raw or None,
        partitions=partitions_block,
    )


@app.get("/api/v1/resources/snapshots/{snapshot_id}/items")
@app.get("/api/v1/resources/{resource_id}/snapshots/{snapshot_id}/items")
async def list_snapshot_items(
    snapshot_id: str,
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1, le=1000),
    itemType: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    catalogue = await _catalogue_snapshot(db, snapshot_id)
    if catalogue is not None:
        return await _catalogue_item_response(db, catalogue, page=page, size=size, item_type=itemType)

    assembled_items = await _get_power_bi_assembled_items(db, snapshot_id)
    if assembled_items is not None:
        filtered = [item for item in assembled_items if not itemType or item.item_type == itemType]
        total = len(filtered)
        page_items = filtered[(page - 1) * size : page * size]
        return SnapshotItemListResponse(
            content=[_item_to_response(item) for item in page_items],
            totalPages=max(1, (total + size - 1) // size),
            totalElements=total,
            size=size,
            number=page,
        )

    # Resolve through the reuse chain so a reuse snapshot returns
    # the chain root's items. Pre-deploy snapshots and full
    # (non-reuse) snapshots have NULL reuse_chain_root_id and the
    # resolver returns the input id unchanged.
    try:
        _resolved = await resolve_snapshot_items_target(db, snapshot_id)
        _target = _resolved.target_id
    except SnapshotNotFound:
        raise HTTPException(status_code=404, detail="Snapshot not found")
    filters = [SnapshotItem.snapshot_id == _target]
    if itemType:
        filters.append(SnapshotItem.item_type == itemType)

    total = (await db.execute(select(func.count(SnapshotItem.id)).where(*filters))).scalar() or 0
    # ORDER BY is not optional on a LIMIT/OFFSET query. This had none at all,
    # so Postgres returned whatever order the plan produced and page 2 could
    # repeat or skip rows from page 1 (TMV-006's defect class, measured on
    # /api/v1/resources at 33/50 id stability between two identical
    # requests). Capture order, primary key as the unique tiebreak.
    stmt = (
        select(SnapshotItem).where(*filters)
        .order_by(SnapshotItem.created_at.desc(), SnapshotItem.id.desc())
        .offset((page-1)*size).limit(size)
    )
    result = await db.execute(stmt)
    items = result.scalars().all()

    await _maybe_trigger_archive_enrichment(db, items)
    return SnapshotItemListResponse(
        content=[_item_to_response(item) for item in items],
        totalPages=max(1, (total + size - 1) // size),
        totalElements=total,
        size=size, number=page,
    )


@app.get("/api/v1/resources/snapshots/{snapshot_id}/diff", response_model=SnapshotDiff)
async def diff_snapshots(snapshot_id: str, snapshot2: str = Query(...)):
    return SnapshotDiff(added=[], removed=[], modified=[])


@app.get("/api/v1/resources/{resource_id}/snapshots/{snapshot_id}/items")
async def browse_snapshot_items(
    resource_id: str,
    snapshot_id: str,
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1, le=1000),
    itemType: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    catalogue = await _catalogue_snapshot(db, snapshot_id)
    if catalogue is not None:
        if catalogue.resource_id != UUID(resource_id):
            owner = await db.get(Resource, catalogue.resource_id)
            if owner is None or owner.parent_resource_id != UUID(resource_id):
                raise HTTPException(404, detail="Snapshot not found for resource")
        return await _catalogue_item_response(db, catalogue, page=page, size=size, item_type=itemType)

    assembled_items = await _get_power_bi_assembled_items(db, snapshot_id)
    if assembled_items is not None:
        filtered = [item for item in assembled_items if not itemType or item.item_type == itemType]
        total = len(filtered)
        page_items = filtered[(page - 1) * size : page * size]
        return SnapshotItemListResponse(
            content=[_item_to_response(item) for item in page_items],
            totalPages=max(1, (total + size - 1) // size),
            totalElements=total,
            size=size,
            number=page,
        )

    # Resolve through the reuse chain (see sibling list_snapshot_items
    # comment). 404 a missing snapshot rather than returning an
    # empty list — an empty result for a missing id would be silently
    # confusing in the Recovery UI.
    try:
        _resolved = await resolve_snapshot_items_target(db, snapshot_id)
        _target = _resolved.target_id
    except SnapshotNotFound:
        raise HTTPException(status_code=404, detail="Snapshot not found")
    filters = [SnapshotItem.snapshot_id == _target]
    if itemType:
        filters.append(SnapshotItem.item_type == itemType)

    total = (await db.execute(select(func.count(SnapshotItem.id)).where(*filters))).scalar() or 0
    # Same unordered-pagination defect as its sibling list_snapshot_items
    # above — see that comment.
    stmt = (
        select(SnapshotItem).where(*filters)
        .order_by(SnapshotItem.created_at.desc(), SnapshotItem.id.desc())
        .offset((page-1)*size).limit(size)
    )
    result = await db.execute(stmt)
    items = result.scalars().all()

    await _maybe_trigger_archive_enrichment(db, items)
    return SnapshotItemListResponse(
        content=[_item_to_response(item) for item in items],
        totalPages=max(1, (total + size - 1) // size),
        totalElements=total,
        size=size, number=page,
    )


# ============ Recovery: List resources with backups ============

@app.get("/api/v1/resources/with-backups")
async def list_resources_with_backups(
    tenantId: str = Query(...),
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
):
    """List all resources for a tenant that have at least one real recovery point.

    "Real recovery point" is _real_restore_point() (defined below): COMPLETED
    or PARTIAL, excluding skipped_concurrent placeholders. Counting COMPLETED
    only had two failure modes here — a resource whose snapshots are ALL
    PARTIAL was dropped from Recovery entirely (it looked like it had never
    been backed up, even though a PARTIAL run holds every item it captured),
    and a resource whose only "backup" is a 0-item concurrency placeholder was
    listed as protected with nothing behind it.

    Tier 2 children (parent_resource_id IS NOT NULL) are hidden from the
    Recovery list — their snapshot counts and item totals roll up under the
    parent resource so the user sees one row per backed-up identity rather
    than one row per content category.

    TEAMS_CHANNEL + Tier 2 USER_* resource types mirror the resource-
    service's UI_HIDDEN_TYPES exclusion so Recovery doesn't show a second
    card for the same team / user content that M365_GROUP (or the user's
    ENTRA_USER parent) already covers."""
    # Keep in sync with resource-service/main.py::UI_HIDDEN_TYPES.
    # Filter on the enum's string values to avoid pulling ResourceType
    # into this module — SQLAlchemy compares the Postgres enum column
    # against string values cleanly via .in_().
    _HIDDEN_RECOVERY_TYPES = (
        "TEAMS_CHAT_EXPORT",
        "USER_MAIL", "USER_ONEDRIVE", "USER_CONTACTS", "USER_CALENDAR", "USER_CHATS",
        "USER_ARCHIVE", "TEAMS_CHANNEL",
    )
    resources_stmt = (
        select(Resource)
        .where(Resource.tenant_id == UUID(tenantId))
        # Archived rows (e.g. the 8,270 phantom TEAMS_CHANNEL resources
        # minted while _get() dropped the teams provisioning filter,
        # archived 2026-07-14) must not resurface in Recovery.
        .where(Resource.status != ResourceStatus.ARCHIVED)
        # display_name is not unique (rooms and shared mailboxes repeat
        # names across divisions) — id keeps the Recovery list order stable
        # between refreshes.
        .order_by(Resource.display_name, Resource.id)
    )
    resources_result = await db.execute(resources_stmt)
    all_resources = resources_result.scalars().all()

    if not all_resources:
        return {
            "item_number": 0,
            "page_number": page,
            "next_page_token": None,
            "items": [],
        }

    resource_ids = [r.id for r in all_resources]

    stats_stmt = (
        select(
            Snapshot.resource_id,
            func.count(Snapshot.id).label("snapshot_count"),
            func.sum(Snapshot.item_count).label("total_items"),
        )
        # Recovery-point semantics, not run-outcome statistics: this feeds
        # "does this resource have restorable data / how much". PARTIAL
        # counts; the 0-item skipped_concurrent placeholder does not.
        .where(_real_restore_point())
        .where(Snapshot.resource_id.in_(resource_ids))
        .group_by(Snapshot.resource_id)
    )
    stats_result = await db.execute(stats_stmt)
    raw_stats: Dict[str, Dict[str, int]] = {}
    for s in stats_result.fetchall():
        raw_stats[str(s[0])] = {"snapshot_count": int(s[1] or 0), "total_items": int(s[2] or 0)}

    def _type_name(resource: Resource) -> str:
        return resource.type.value if hasattr(resource.type, 'value') else str(resource.type)

    visible_resource_ids = {
        str(r.id) for r in all_resources
        if _type_name(r) not in _HIDDEN_RECOVERY_TYPES
    }

    # Roll children up under their parent. Stats keyed by the parent's ID.
    stats: Dict[str, Dict[str, int]] = {}
    for r in all_resources:
        # Tier 2 children never get their own row — they accumulate onto the
        # parent's stats below.
        if r.parent_resource_id is not None:
            continue
        if str(r.id) not in visible_resource_ids:
            continue
        own = raw_stats.get(str(r.id), {"snapshot_count": 0, "total_items": 0})
        stats[str(r.id)] = dict(own)

    for r in all_resources:
        if r.parent_resource_id is None:
            continue
        parent_key = str(r.parent_resource_id)
        if parent_key not in stats:
            # Parent is missing or hidden, so the child remains hidden too.
            continue
        child_stats = raw_stats.get(str(r.id))
        if not child_stats:
            continue
        stats[parent_key]["snapshot_count"] += child_stats["snapshot_count"]
        stats[parent_key]["total_items"] += child_stats["total_items"]

    # Drop parents with no rolled-up snapshots (no own + no child snapshots).
    stats = {k: v for k, v in stats.items() if v["snapshot_count"] > 0}

    total = len(stats)
    paginated_ids = list(stats.keys())[(page - 1) * size : page * size]

    def map_kind(t):
        m = {"MAILBOX": "office_user", "SHARED_MAILBOX": "shared_mailbox", "ROOM_MAILBOX": "room_mailbox",
             "ONEDRIVE": "onedrive", "SHAREPOINT_SITE": "sharepoint_site", "TEAMS_CHANNEL": "teams_channel",
             "TEAMS_CHAT": "teams_chat", "ENTRA_USER": "entra_user", "ENTRA_GROUP": "entra_group",
             "ENTRA_APP": "entra_app", "ENTRA_DEVICE": "entra_device", "AZURE_VM": "azure_vm",
             "AZURE_SQL_DB": "azure_sql", "AZURE_POSTGRESQL": "azure_postgresql", "POWER_BI": "power_bi"}
        return m.get(t, t.lower() if t else "unknown")

    resources_map = {str(r.id): r for r in all_resources}

    items = []
    for rid in paginated_ids:
        r = resources_map.get(rid)
        if not r:
            continue
        s = stats[rid]
        type_name = _type_name(r)
        items.append({
            "id": str(r.id),
            "tenant_id": str(r.tenant_id),
            "kind": map_kind(type_name),
            "provider": "azure" if "AZURE" in (type_name or "") else "o365",
            "external_id": r.external_id,
            "name": r.display_name,
            "email": r.email,
            # Merge top-level Azure columns into the data dict so the
            # Recover modal can read subscription/RG/region without a
            # second fetch. Backfills `location` for SQL resources
            # (discovery only stores it on PostgreSQL metadata).
            "data": {
                **(r.extra_data or {}),
                **({"azure_region": r.azure_region} if r.azure_region else {}),
                **({"azure_subscription_id": r.azure_subscription_id} if r.azure_subscription_id else {}),
                **({"azure_resource_group": r.azure_resource_group} if r.azure_resource_group else {}),
            },
            "storage_bytes": r.storage_bytes or 0,
            "last_backup_at": r.last_backup_at.isoformat() if r.last_backup_at else None,
            "last_backup_status": r.last_backup_status if r.last_backup_status else None,
            "snapshot_count": s["snapshot_count"],
            "total_items": s["total_items"],
        })

    has_next = (page * size) < total
    return {
        "item_number": total,
        "page_number": page,
        "next_page_token": str(page + 1) if has_next else None,
        "items": items,
    }


# Maps each Recovery content tab to the Tier 2 child resource type that
# holds its snapshots. The parent ENTRA_USER's own snapshots stay separate
# (they hold identity items: profile, manager, group memberships) and are
# not surfaced via these tabs.
CONTENT_TAB_TO_TYPE: Dict[str, str] = {
    "mail": "USER_MAIL",
    "onedrive": "USER_ONEDRIVE",
    "contacts": "USER_CONTACTS",
    "calendar": "USER_CALENDAR",
    "chats": "USER_CHATS",
    # Exchange Online Archive — its own Tier-2 child. The tab only renders
    # when this child has a completed snapshot (byContent.archive != null), so
    # users without an archive never see it.
    "archive": "USER_ARCHIVE",
}


# Per-tab newest-wins rollup, fully database-side. The previous
# implementation selected (external_id, content_size, created_at) for
# EVERY sibling-snapshot row and deduped in a Python loop — O(total
# rows) of process memory. The chats backup re-captures most of the
# thread history on every run, so one heavy user reached 11M pointer
# rows and a single Recovery resource-select OOMKilled the 512Mi pod
# (2026-07-14). GROUP BY hash-aggregates those rows inside Postgres
# and returns exactly two scalars.
#
# Semantics vs the old loop: newest-wins by created_at per external_id
# is unchanged; rows with NULL external_id now each count once (keyed
# by pk) instead of collapsing on shared created_at; deleted-tombstone
# rows are excluded to match the middle-panel lists and folder badges
# (commit baa0a69).
_CONTENT_ROLLUP_SQL = """
    SELECT COUNT(*) AS cnt, COALESCE(SUM(size), 0) AS bytes
    FROM (
      SELECT (array_agg(si.content_size ORDER BY si.created_at DESC))[1] AS size
      FROM snapshot_items si
      WHERE si.snapshot_id = ANY(:snaps)
        {type_filter}
        AND COALESCE(si.is_deleted, false) = false
      GROUP BY COALESCE(si.external_id, si.id::text)
    ) latest
"""

# Rollup cache keyed by (child resource, its latest COMPLETED snapshot,
# item-type set). A new completed snapshot changes the key, so entries
# invalidate themselves; per-process (one cache per uvicorn worker) and
# rebuilt on restart. Bounded by wholesale clear — Recovery-opened
# resources number in the dozens, the bound is a runaway guard.
_content_rollup_cache: Dict[tuple, tuple] = {}
_CONTENT_ROLLUP_CACHE_MAX = 10000


async def _rollup_latest_items(
    db: AsyncSession,
    child_resource_id,
    latest_snapshot_id,
    sibling_ids: list,
    count_types: Optional[list] = None,
) -> tuple:
    """count_types=None rolls up EVERY item type — used by
    /storage-summary for the protected-data headline."""
    types_key = tuple(sorted(count_types)) if count_types else ("*",)
    key = (str(child_resource_id), str(latest_snapshot_id), types_key)
    hit = _content_rollup_cache.get(key)
    if hit is not None:
        return hit
    sql = _CONTENT_ROLLUP_SQL.format(
        type_filter="AND si.item_type = ANY(:types)" if count_types else ""
    )
    params: Dict[str, Any] = {"snaps": list(sibling_ids)}
    if count_types:
        params["types"] = list(count_types)
    # Same rationale as _set_local_work_mem's own comment: this GROUP BY
    # hash-aggregates ~1M rows for heavy mailboxes; the bump keeps it off
    # temp disk. (Helper is defined later in the module — resolved at
    # call time, and every call happens post-import.)
    await _set_local_work_mem(db)
    row = (await db.execute(text(sql), params)).first()
    out = (int(row[0] or 0), int(row[1] or 0)) if row else (0, 0)
    if len(_content_rollup_cache) >= _CONTENT_ROLLUP_CACHE_MAX:
        _content_rollup_cache.clear()
    _content_rollup_cache[key] = out
    return out


# A snapshot is a usable recovery point iff it terminated holding data we
# can read AND it isn't one of the empty concurrency placeholders.
#
# PARTIAL belongs here. A run that lost some shards still holds every item
# it did capture — jrue's 1,114,848-item archive baseline is PARTIAL, and
# the lossy-shard derivation makes PARTIAL the normal outcome for any
# SharePoint/OneDrive run with files_failed > 0. Anchoring only on
# COMPLETED hid such runs entirely; worse, because the sibling roll-up
# below counts items with created_at <= the anchor, anchoring to an OLDER
# COMPLETED snapshot silently truncated everything the newest run added.
#
# skipped_concurrent placeholders are excluded. backup-worker's cross-job
# concurrency guard creates a snapshot row purely so the skipped job's
# FANOUT/FINALIZE counters balance, then immediately marks it COMPLETED
# with 0 items (~19ms end to end). Those must never win the anchor or
# count as a version: jrue's newest COMPLETED archive snapshot is one, so
# the archive would report a 0-item recovery point over a 1.1M-item one.
_READABLE_SNAPSHOT_STATES = (SnapshotStatus.COMPLETED, SnapshotStatus.PARTIAL)


def _real_restore_point():
    """SQL predicate: terminal-with-data, and not a concurrency placeholder."""
    return and_(
        Snapshot.status.in_(_READABLE_SNAPSHOT_STATES),
        # json_extract_path_text is NULL-safe for absent keys and works on
        # both json and jsonb (extra_data is json); COALESCE keeps rows
        # that predate the flag. Compared as text so a non-boolean value
        # can't raise the way an ->>::boolean cast would.
        func.coalesce(
            func.json_extract_path_text(Snapshot.extra_data, "skipped_concurrent"),
            "false",
        ).notin_(("true", "t", "1")),
    )


@app.get("/api/v1/resources/{resource_id}/content-snapshots")
async def get_content_snapshots(
    resource_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Return the latest completed snapshot per content tab for a resource.

    Resolution: each tab maps to the child resource type that backs it
    (mail → USER_MAIL, onedrive → USER_ONEDRIVE, …). For each child the
    most recent *real recovery point* wins — see _real_restore_point():
    COMPLETED or PARTIAL, excluding skipped_concurrent placeholders. Tabs
    with no snapshot yet return null so the UI can render an empty state
    without a second round-trip.

    Recovery uses this on resource select so it can hide the snapshot
    dropdown — clicking a tab now jumps straight to the right snapshot."""
    parent = await db.get(Resource, UUID(resource_id))
    if not parent:
        raise HTTPException(status_code=404, detail="Resource not found")

    # Pull every Tier 2 child of this resource in one shot.
    children = (await db.execute(
        select(Resource).where(Resource.parent_resource_id == parent.id)
    )).scalars().all()
    children_by_type: Dict[str, Resource] = {c.type.value: c for c in children}

    # Resource IDs we'll roll into snapshot count (parent + every child).
    counted_ids = [parent.id] + [c.id for c in children]

    snapshot_count = (await db.execute(
        select(func.count(Snapshot.id))
        .where(Snapshot.resource_id.in_(counted_ids))
        .where(_real_restore_point())
    )).scalar() or 0

    # versionCount = how many distinct backup *attempts* (batch_id) have
    # completed for this identity. One "Backup now" click for a user
    # fans out to ~5 surface snapshots across THREE Jobs (the Tier-1
    # ENTRA_USER bulk + Tier-2 fan-out into mail/chats/cal/contacts + a
    # separate heavy-lane OneDrive Tier-2 Job) — but is a single
    # recovery point from the user's POV. AFI calls these "versions."
    # All three Jobs share the same `spec.batch_id` (set by job-service
    # at trigger time, propagated to Tier-2 via the discovery handoff),
    # so counting DISTINCT batch_id collapses one click into one row.
    # Counting DISTINCT job_id would over-report by 2-3x. NULL job_ids
    # (manual / ad-hoc) fall back to grouping by snapshot.id so each
    # singleton stays its own version.
    version_count = (await db.execute(
        select(
            func.count(
                distinct(
                    func.coalesce(
                        func.json_extract_path_text(Job.spec, "batch_id"),
                        func.cast(Snapshot.job_id, String),
                        func.cast(Snapshot.id, String),
                    )
                )
            )
        )
        .select_from(Snapshot)
        .outerjoin(Job, Job.id == Snapshot.job_id)
        .where(Snapshot.resource_id.in_(counted_ids))
        .where(_real_restore_point())
    )).scalar() or 0

    # Per-content-type item_type to count against (for the aggregated
    # roll-up below). Mail & chats are delta-backed and their per-snapshot
    # item_count reflects only the delta captured in that run — we need
    # to count distinct external_ids across every sibling snapshot.
    COUNT_TYPE_PER_TAB: Dict[str, list] = {
        "mail": ["EMAIL"],
        "onedrive": ["ONEDRIVE_FILE"],
        "contacts": ["USER_CONTACT", "CONTACT"],
        "calendar": ["CALENDAR_EVENT"],
        "chats": ["TEAMS_CHAT_MESSAGE", "TEAMS_MESSAGE", "TEAMS_MESSAGE_REPLY"],
        "archive": ["ARCHIVE_ITEM"],
    }

    by_content: Dict[str, Optional[Dict[str, Any]]] = {}
    for tab, child_type in CONTENT_TAB_TO_TYPE.items():
        child = children_by_type.get(child_type)
        if not child:
            by_content[tab] = None
            continue
        snap = (await db.execute(
            select(Snapshot)
            .where(Snapshot.resource_id == child.id)
            .where(_real_restore_point())
            .order_by(Snapshot.created_at.desc())
            .limit(1)
        )).scalar_one_or_none()
        if not snap:
            by_content[tab] = None
            continue

        # Aggregate count + bytes across every sibling snapshot of this
        # child (newest-wins dedup by external_id). Mirrors the approach
        # used by the /mail, /chats, /calendar endpoints so the tab badge
        # matches the count rendered in the middle panel.
        agg_count = int(snap.item_count or 0)
        agg_bytes = int(snap.bytes_total or 0)
        count_types = COUNT_TYPE_PER_TAB.get(tab) or []
        if count_types:
            sib_ids = [
                row[0] for row in (await db.execute(
                    select(Snapshot.id).where(
                        Snapshot.resource_id == child.id,
                        Snapshot.created_at <= snap.created_at,
                        # Mirror _resolve_sibling_snapshot_ids: FAILED
                        # runs' leftover metadata rows must not count
                        # toward tab badges, and PENDING_DELETION rows
                        # are mid-teardown (sweep claim) — their items/
                        # blobs are being deleted right now.
                        Snapshot.status.notin_([
                            SnapshotStatus.FAILED,
                            SnapshotStatus.PENDING_DELETION,
                        ]),
                    )
                )).all()
            ]
            if sib_ids:
                agg_count, agg_bytes = await _rollup_latest_items(
                    db, child.id, snap.id, sib_ids, count_types,
                )

        # status / snapshotItemCount (TMV-010, 2026-08-11). The QA sweep read
        # the Chats tab badge (719,826) against `snapshots.item_count`
        # (598,807) and called the 121k gap an inconsistency. It is not: the
        # two are different metrics and both are right. `itemCount` here is
        # the cross-sibling newest-wins rollup — the number of distinct chat
        # messages recoverable AS OF this snapshot, across all 18 siblings —
        # while `snapshots.item_count` is what THIS one delta run captured.
        # Verified on prod for the test resource: rollup over the anchor's 18
        # siblings = 719,826 distinct external_ids, anchor item_count =
        # 598,807.
        #
        # The real defect the finding names is the other half: that anchor
        # snapshot's status is PARTIAL and the payload had no way to say so,
        # so Recovery rendered a silently-incomplete backup as complete.
        # `_real_restore_point()` deliberately admits PARTIAL (a lossy-shard
        # run still holds everything it captured), which makes surfacing the
        # status mandatory rather than optional. Both fields are additive —
        # existing consumers keep reading itemCount unchanged.
        _snap_status = (
            snap.status.value if hasattr(snap.status, "value") else str(snap.status)
        )
        by_content[tab] = {
            "snapshotId": str(snap.id),
            "childResourceId": str(child.id),
            "itemCount": agg_count,
            "bytesTotal": agg_bytes,
            "createdAt": snap.created_at.isoformat() if snap.created_at else None,
            # Status of the anchor snapshot this tab points at.
            "status": _snap_status,
            # True for anything that is not a clean COMPLETED — the UI should
            # badge the tab and banner the panel when this is set.
            "isPartial": _snap_status != SnapshotStatus.COMPLETED.value,
            # This snapshot's OWN captured count, next to the cross-sibling
            # rollup, so the two numbers are never mistaken for each other
            # again.
            "snapshotItemCount": int(snap.item_count or 0),
        }

    return {
        "resourceId": str(parent.id),
        "snapshotCount": int(snapshot_count),
        "versionCount": int(version_count),
        "byContent": by_content,
    }


# ============ Recovery: Search items across snapshots ============

@app.get("/api/v1/resources/{resource_id}/snapshots/search")
async def search_snapshot_items(
    resource_id: str,
    query: Optional[str] = Query(None),
    item_type: Optional[str] = Query(None),
    folder_path: Optional[str] = Query(None),
    snapshot_id: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
):
    """Search snapshot items for a resource with optional filters."""
    catalogue = await _catalogue_snapshot(db, snapshot_id) if snapshot_id else None
    if catalogue is not None:
        if catalogue.resource_id != UUID(resource_id):
            raise HTTPException(404, detail="Snapshot not found for resource")
        return await _catalogue_item_response(db, catalogue, page=page, size=size,
            item_type=item_type, folder=folder_path, query=query)
    resource = await db.get(Resource, UUID(resource_id))
    resource_type = resource.type.value if resource and hasattr(resource.type, "value") else (str(resource.type) if resource else "")
    if not snapshot_id and resource_type == "USER_CHATS":
        latest = await db.scalar(select(Snapshot).where(Snapshot.resource_id == resource.id,
            Snapshot.status.in_([SnapshotStatus.COMPLETED, SnapshotStatus.PARTIAL]))
            .order_by(Snapshot.created_at.desc(), Snapshot.id.desc()).limit(1))
        if latest is not None and (latest.extra_data or {}).get("chat_inventory_format") == 1:
            return await _catalogue_item_response(db, latest, page=page, size=size,
                item_type=item_type, folder=folder_path, query=query)
    if resource and resource_type == "POWER_BI":
        target_snapshot_id = snapshot_id
        if not target_snapshot_id:
            latest_snapshot_result = await db.execute(
                select(Snapshot).where(
                    Snapshot.resource_id == UUID(resource_id),
                    Snapshot.status.in_([SnapshotStatus.COMPLETED, SnapshotStatus.PARTIAL]),
                ).order_by(Snapshot.created_at.desc()).limit(1)
            )
            latest_snapshot = latest_snapshot_result.scalar_one_or_none()
            target_snapshot_id = str(latest_snapshot.id) if latest_snapshot else None

        if not target_snapshot_id:
            return SnapshotItemListResponse(
                content=[], totalPages=0, totalElements=0, size=size, number=page,
            )

        assembled_items = await _get_power_bi_assembled_items(db, target_snapshot_id)
        filtered = assembled_items or []
        if item_type:
            filtered = [item for item in filtered if item.item_type == item_type]
        if folder_path:
            filtered = [item for item in filtered if item.folder_path == folder_path]
        if query:
            lowered = query.lower()
            filtered = [
                item for item in filtered
                if lowered in (item.name or "").lower() or lowered in (item.external_id or "").lower()
            ]
        total = len(filtered)
        page_items = filtered[(page - 1) * size : page * size]
        return SnapshotItemListResponse(
            content=[_item_to_response(item) for item in page_items],
            totalPages=max(1, (total + size - 1) // size),
            totalElements=total,
            size=size,
            number=page,
        )

    snapshot_ids_stmt = select(Snapshot.id).where(
        Snapshot.resource_id == UUID(resource_id),
        # PARTIAL rows are real restore points and must be searchable: a
        # counter-driven honesty gate can flip an otherwise-good snapshot to
        # PARTIAL, and for some resources EVERY recent snapshot is PARTIAL
        # (e.g. SHAREPOINT_SITE "IT OPS NOTES", 2026-08-10 → 08-11). Filtering
        # to COMPLETED alone hid the newest backup from Recovery entirely.
        # Mirrors the POWER_BI branch above, which already accepts both.
        Snapshot.status.in_([SnapshotStatus.COMPLETED, SnapshotStatus.PARTIAL]),
    )
    if snapshot_id:
        snapshot_ids_stmt = snapshot_ids_stmt.where(Snapshot.id == UUID(snapshot_id))
    snapshot_ids_result = await db.execute(snapshot_ids_stmt)
    # ``select(Snapshot.id)`` + ``.scalars()`` yields bare UUIDs, NOT ORM rows.
    # The old ``s.id`` therefore raised
    #   AttributeError: 'asyncpg.pgproto.pgproto.UUID' object has no attribute 'id'
    # and 500'd EVERY non-POWER_BI recovery search — including the folder
    # drill-down, which routes through this same handler with folder_path set.
    # The empty-result case masked the bug for years: the comprehension never
    # dereferenced, so a resource with no matching snapshot returned a
    # plausible-looking 200/total=0 instead of the 500. Pinned by
    # tests/services/test_recovery_search_snapshot_ids.py.
    snapshot_ids = list(snapshot_ids_result.scalars().all())

    if not snapshot_ids:
        return SnapshotItemListResponse(
            content=[], totalPages=0, totalElements=0, size=size, number=page,
        )

    filters = [SnapshotItem.snapshot_id.in_(snapshot_ids)]
    if item_type:
        filters.append(SnapshotItem.item_type == item_type)
    if folder_path:
        filters.append(SnapshotItem.folder_path == folder_path)
    if query:
        filters.append(
            or_(
                SnapshotItem.name.ilike(f"%{query}%"),
                SnapshotItem.external_id.ilike(f"%{query}%"),
            )
        )

    total = (await db.execute(select(func.count(SnapshotItem.id)).where(*filters))).scalar() or 0
    stmt = (
        select(SnapshotItem)
        .where(*filters)
        # id tiebreak — snapshot_items are bulk-inserted, so created_at ties
        # span thousands of rows and page 2 would otherwise re-slice them.
        .order_by(SnapshotItem.created_at.desc(), SnapshotItem.id.desc())
        .offset((page - 1) * size)
        .limit(size)
    )
    result = await db.execute(stmt)
    items = result.scalars().all()

    return SnapshotItemListResponse(
        content=[_item_to_response(item) for item in items],
        totalPages=max(1, (total + size - 1) // size),
        totalElements=total,
        size=size, number=page,
    )


@app.get("/api/v1/resources/snapshots/{snapshot_id}/items/{item_id}", response_model=SnapshotItemResponse)
async def get_snapshot_item_detail(snapshot_id: str, item_id: str, db: AsyncSession = Depends(get_db)):
    assembled_items = await _get_power_bi_assembled_items(db, snapshot_id)
    if assembled_items is not None:
        item = next((row for row in assembled_items if str(row.id) == item_id), None)
        if not item:
            raise HTTPException(status_code=404, detail="Snapshot item not found")
        return _item_to_response(item)

    stmt = select(SnapshotItem).where(
        SnapshotItem.snapshot_id == UUID(snapshot_id),
        SnapshotItem.id == UUID(item_id),
    )
    result = await db.execute(stmt)
    item = result.scalar_one_or_none()
    if not item:
        raise HTTPException(status_code=404, detail="Snapshot item not found")
    if item.chat_version_id is not None or (await _catalogue_snapshot(db, snapshot_id)) is not None:
        item = (await materialize_chat_items(db, [item]))[0]
    return _item_to_response(item)


# ==================== Content-Type-Specific Endpoints ====================


async def _load_blob_context(db: AsyncSession, snapshot_id: str):
    """Load the snapshot + resource + prepared shard/workload hints needed to read blobs
    for every item in this snapshot. Returns (shard, tenant_id, candidates) or (None, None, ()).

    Shard is keyed by resource_id (matching the worker's write-side call).
    Candidates is an ordered tuple of workload suffixes for get_container_name."""
    snapshot = await db.get(Snapshot, UUID(snapshot_id))
    if not snapshot:
        return None, None, ()
    resource = await db.get(Resource, snapshot.resource_id)
    if not resource or not azure_storage_manager.shards:
        return None, str(resource.tenant_id) if resource else None, ()
    resource_type = resource.type.value if hasattr(resource.type, "value") else str(resource.type)
    shard = azure_storage_manager.get_shard_for_resource(
        str(resource.id), str(resource.tenant_id)
    )
    candidates = workload_candidates_for_resource_type(resource_type)
    return shard, str(resource.tenant_id), candidates


async def _download_item_blob(shard, tenant_id: str, candidates: tuple, blob_path: str) -> Optional[bytes]:
    """Try each candidate container in order, returning the first blob found.
    Returns None if the blob is absent from every candidate container or if no
    shard/tenant/candidates are available. Logs a warning if nothing was found."""
    if not shard or not tenant_id or not blob_path or not candidates:
        return None
    tried: List[str] = []
    for workload in candidates:
        container = azure_storage_manager.get_container_name(tenant_id, workload)
        tried.append(container)
        try:
            data = await shard.download_blob(container, blob_path)
        except Exception as exc:
            logger.warning("download_blob error on %s/%s: %s", container, blob_path, exc)
            continue
        if data is not None:
            return data
    logger.warning("blob not found in any candidate container %s for %s", tried, blob_path)
    return None


async def _read_blob_json(shard, tenant_id: str, candidates: tuple, blob_path: str) -> dict:
    """Thin JSON wrapper around _download_item_blob. Returns {} on miss or decode failure."""
    import json as _j
    data = await _download_item_blob(shard, tenant_id, candidates, blob_path)
    if not data:
        return {}
    try:
        return _j.loads(data.decode("utf-8"))
    except Exception as exc:
        logger.warning("blob at %s is not UTF-8 JSON: %s", blob_path, exc)
        return {}


async def _resolve_sibling_snapshot_ids(
    db: AsyncSession,
    snapshot_id: str,
    target_child_type: Optional[ResourceType] = None,
) -> list:
    """For a given snapshot, return every snapshot ID for the same resource
    up to and including this one (ordered by created_at).

    Delta-based backups split content across snapshots: the initial
    full-pull captures the whole state, and each subsequent snapshot
    holds only the delta since the prior run. To show a consistent
    "state as of snapshot X" view in Recovery — without requiring every
    endpoint to fan out across snapshots — this helper returns the list
    of sibling snapshot ids the caller should UNION over. Callers then
    dedupe items by external_id with newest-wins semantics.

    When ``target_child_type`` is passed (e.g. ResourceType.USER_CHATS),
    the helper also performs cross-resource auto-resolve: if the
    incoming snapshot's resource isn't of the target type, it walks up
    to the ENTRA_USER parent and down to the sibling of the target type
    and returns ITS snapshots instead. This fixes the UX bug where the
    Recovery UI could pick any of a user's Tier 2 snapshot ids (Contacts,
    Mail, Calendar, Chats) and navigate to `/chats`, `/mail` etc. —
    without this, the endpoint would return 0 silently because the
    queried snapshot held no rows of the requested type.

    Returns [snapshot_uuid] alone if the snapshot can't be resolved
    (keeps the endpoint behavior unchanged for malformed inputs).
    """
    try:
        snap_uuid = UUID(snapshot_id)
    except Exception:
        return []
    row = (await db.execute(select(Snapshot).where(Snapshot.id == snap_uuid))).scalar_one_or_none()
    if not row:
        return [snap_uuid]

    resource_id_for_siblings = row.resource_id
    cross_resource_swap = False

    # Cross-resource auto-resolve — if caller requested a specific
    # child type and this snapshot's resource doesn't match, swap to
    # the sibling resource of the requested type under the same
    # parent user. Best-effort: falls back to the passed snapshot's
    # own resource on any lookup failure.
    if target_child_type is not None:
        try:
            src_res = (await db.execute(
                select(Resource).where(Resource.id == row.resource_id)
            )).scalar_one_or_none()
            if src_res is not None and src_res.type != target_child_type:
                # Find the user root: either src itself (if ENTRA_USER)
                # or src.parent_resource_id (if one of the Tier-2 kids).
                parent_id = (
                    src_res.id if src_res.type == ResourceType.ENTRA_USER
                    else src_res.parent_resource_id
                )
                if parent_id is not None:
                    sibling = (await db.execute(
                        select(Resource).where(
                            Resource.parent_resource_id == parent_id,
                            Resource.type == target_child_type,
                        )
                    )).scalar_one_or_none()
                    if sibling is not None:
                        resource_id_for_siblings = sibling.id
                        cross_resource_swap = True
        except Exception:
            # Auto-resolve is advisory; fall back to the literal resource.
            pass

    # For same-resource sibling aggregation (delta runs of the same
    # resource): cap at source.created_at so "state as of snapshot X"
    # is well-defined.
    # For cross-resource swaps (different sibling resource entirely):
    # DON'T cap — the target resource's snapshots have independent
    # timing. Capping by source.created_at excludes the Chats snapshot
    # created 9 seconds after the Calendar snapshot in the same batch,
    # leaving the Recovery chats tab empty when navigated from any
    # non-chat sibling. Instead return every snapshot of the target.
    where_clauses = [
        Snapshot.resource_id == resource_id_for_siblings,
        # FAILED runs can leave blobless coordinator metadata rows behind
        # (a reaped 348GB force-full persisted ~199k of them, 2026-07-14).
        # Including those snapshots in the union makes dead rows render
        # as recoverable "metadata only" items and inflates every tab
        # badge. Failed runs contribute no recoverable state — drop them.
        # PENDING_DELETION rows are mid-teardown (cancelled-sweep claim):
        # their blobs are being deleted this minute — letting them win
        # the newest-wins dedup would surface items whose restore 404s
        # and shadow the live versions underneath.
        Snapshot.status.notin_([
            SnapshotStatus.FAILED,
            SnapshotStatus.PENDING_DELETION,
        ]),
    ]
    if not cross_resource_swap:
        where_clauses.append(Snapshot.created_at <= row.created_at)
    rows = (await db.execute(
        select(Snapshot.id).where(*where_clauses)
        .order_by(Snapshot.created_at.desc())
    )).all()
    sibling_ids = [r[0] for r in rows]
    # Keep the explicitly-requested snapshot viewable even when it is
    # itself FAILED (deep links / post-mortem inspection of a failed
    # run) — but only when no cross-resource swap redirected us to a
    # different resource entirely. PENDING_DELETION is the exception:
    # unlike an inert FAILED row, a sweep-claimed snapshot's blobs are
    # being deleted at this moment and its rows vanish minutes later —
    # re-admitting it would let its (typically newest) items win the
    # union's newest-wins dedup and shadow live versions with
    # restore-404s, the exact defect the exclusion above prevents.
    if (
        not cross_resource_swap
        and snap_uuid not in sibling_ids
        and row.status != SnapshotStatus.PENDING_DELETION
    ):
        sibling_ids.append(snap_uuid)
    return sibling_ids or [snap_uuid]


def _raw(item) -> dict:
    meta = item.extra_data or {}
    r = meta.get("raw") or meta.get("structured") or meta
    return r if isinstance(r, dict) else {}


# The sibling-union queries sort ~1M rows for heavy mailboxes; at the
# default 4MB work_mem those sorts spill hundreds of MB to disk
# (measured 405MB/232MB on a 1.22M-row mailbox, 2026-08-03). A per-
# transaction bump keeps them in memory without touching the shared
# cluster default. SET LOCAL scopes it to the current transaction.
_QUERY_WORK_MEM = os.getenv("SNAPSHOT_QUERY_WORK_MEM", "128MB")
_WORK_MEM_UNIT_KB = {"kB": 1, "MB": 1024, "GB": 1024 * 1024}


def _work_mem_kb(value: str) -> Optional[int]:
    """Parse a work_mem value to whole kB, or None if unusable.

    SET cannot take bind parameters, so the interpolated value must be
    provably an integer; and Postgres range-checks at execution
    (64kB..2147483647kB) — an out-of-range value would ERROR, and any
    errored statement aborts the open transaction, failing every later
    query in the request. Validating the range here keeps the knob
    genuinely advisory.
    """
    m = _re.match(r"^(\d{1,9})(kB|MB|GB)$", value or "")
    if not m:
        return None
    kb = int(m.group(1)) * _WORK_MEM_UNIT_KB[m.group(2)]
    if not (64 <= kb <= 2_147_483_647):
        return None
    return kb


async def _set_local_work_mem(db: AsyncSession) -> None:
    kb = _work_mem_kb(_QUERY_WORK_MEM)
    if kb is None:
        return
    try:
        # SAVEPOINT wrapper: if the SET still fails for any reason, the
        # savepoint rolls back and the enclosing transaction stays
        # usable — a bare failed statement would abort it and every
        # later query in the request would raise InFailedSQLTransaction.
        # (GUC changes survive RELEASE SAVEPOINT, so a successful SET
        # LOCAL still applies to the rest of the transaction.)
        async with db.begin_nested():
            await db.execute(text(f"SET LOCAL work_mem = '{kb}kB'"))
    except Exception:
        # Advisory only — a failed SET must never fail the request.
        pass


# Result cache for the union-dedup list/count queries. For a mailbox
# with 15 near-identical full-capture snapshots, one page-1 request
# costs ~8s of Postgres time; users click back and forth between
# accounts, re-paying that on every click. Entries are keyed by the
# sibling snapshot set *plus each sibling's status*, so:
#   - a new snapshot (backup completed, retention pruned) changes the
#     id set → new key;
#   - an IN_PROGRESS sibling flipping to COMPLETED changes the status
#     fingerprint → new key (rows stream into an in-progress snapshot
#     under an unchanged id, so the id set alone is not enough).
# The residual staleness window — rows streaming into a still-running
# snapshot — is bounded by the TTL below. Values hold only page ids
# (≤500 UUID strings); ORM rows are re-hydrated per request so no
# session-bound state is ever cached. Per-process (one cache per
# uvicorn worker), wholesale-cleared at the bound as a runaway guard.
_paged_items_cache: Dict[tuple, Tuple[float, tuple]] = {}
_PAGED_ITEMS_CACHE_MAX = 4096
# The dedup COUNT is cached SEPARATELY from the page ids, because it
# depends on strictly less: the sibling set and the row filters, but
# not the display sort, the page number or the page size. Folding it
# into the page key (as it was until 2026-08-11) meant every 1→2→3
# page click, every size change and every sort flip re-ran the
# full-union COUNT — 209,325 rows scanned / 1.7 s of Postgres time for
# Riley Stillman's calendar, and 8,545,195 rows / 140 s for Jason
# Rue's chats (measured on prod 2026-08-11). Split, the count is paid
# once per (sibling set, filters) and every later page is page-query
# only. Pinned by test_count_cache_survives_page_and_sort_changes.
_dedup_count_cache: Dict[tuple, Tuple[float, int]] = {}
_DEDUP_COUNT_CACHE_MAX = 4096
_folder_agg_cache: Dict[tuple, Tuple[float, list]] = {}
_FOLDER_AGG_CACHE_MAX = 4096
_LIST_CACHE_TTL_S = int(os.getenv("SNAPSHOT_LIST_CACHE_TTL_S", "900"))

# Statuses whose snapshot_items rows are settled. IN_PROGRESS rows are
# still streaming in and PENDING_DELETION rows are being removed — a
# fingerprint containing either must not be cached (the id+status pair
# wouldn't change while the rows do). FAILED appears only for an
# explicitly-requested failed snapshot (post-mortem view) — inert rows.
_CACHEABLE_SNAPSHOT_STATES = frozenset({"COMPLETED", "FAILED", "PARTIAL"})

# Session factory for the concurrent COUNT in _paged_latest_items —
# module-level indirection so unit tests can substitute a fake without
# touching the real engine.
_count_session_factory = async_session_factory

# At most this many COUNTs may run concurrently on second sessions; the
# rest fall back to running sequentially on the request's own session.
# Bounds the extra pooled-connection demand: a cold-cache burst of N
# list requests otherwise wants 2N connections and can starve the pool
# right when Recovery is busiest (each heavy query holds its connection
# for seconds).
_COUNT_CONCURRENCY = asyncio.Semaphore(int(os.getenv("SNAPSHOT_COUNT_CONCURRENCY", "8")))


def _cache_put(cache: Dict[tuple, tuple], bound: int, key: tuple, value: tuple) -> None:
    """Insert with expired-entry sweep before the wholesale-clear guard."""
    if len(cache) >= bound:
        now = _time.monotonic()
        for k in [k for k, v in cache.items() if (now - v[0]) >= _LIST_CACHE_TTL_S]:
            cache.pop(k, None)
        if len(cache) >= bound:
            cache.clear()
    cache[key] = value


async def _sibling_state_fingerprint(db: AsyncSession, sibling_ids) -> Tuple[tuple, bool]:
    """((id, status) per sibling, sorted) + whether the set is cacheable.

    The fingerprint is the cache-version token: any snapshot add/remove
    or status flip changes it. `cacheable` is False while any sibling
    is in a state whose rows still change under an unchanged
    fingerprint (streaming backup, pending deletion) — those requests
    run live, exactly the pre-cache behavior.
    """
    if not sibling_ids:
        return (), False
    rows = (await db.execute(
        select(Snapshot.id, Snapshot.status)
        .where(Snapshot.id.in_(list(sibling_ids)))
    )).all()
    states = [getattr(r[1], "value", str(r[1])) for r in rows]
    fp = tuple(sorted((str(r[0]), s) for r, s in zip(rows, states)))
    cacheable = all(s in _CACHEABLE_SNAPSHOT_STATES for s in states)
    return fp, cacheable


_SINCE_UNITS = {"d": 86400, "h": 3600, "m": 60}


def _parse_since(value: Optional[str]) -> Optional[str]:
    """Normalise a recency window to a Graph-style Zulu timestamp string.

    Accepts an absolute ISO-8601 instant ("2026-08-01T00:00:00Z") or a
    relative window ("30d", "12h", "90m"). Returns None for None/blank so
    callers can treat "no window" as "no predicate".

    Why a string and not a datetime: the values it is compared against live
    in `metadata->'raw'` as Graph's fixed-width Zulu strings, and the sort
    keys on the same expression. Lexicographic order over that format is
    chronological order, so filter and sort agree by construction — casting
    to timestamptz instead would detoast and parse every raw payload in the
    scan, which is precisely the cost _paged_latest_items exists to avoid.
    """
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    unit = raw[-1].lower()
    if unit in _SINCE_UNITS and raw[:-1].isdigit():
        cutoff = datetime.utcnow() - timedelta(seconds=int(raw[:-1]) * _SINCE_UNITS[unit])
        return cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        raise HTTPException(
            status_code=400,
            detail=(
                f"Invalid recency window {value!r}: expected ISO-8601 "
                "(2026-08-01T00:00:00Z) or a relative window (30d / 12h / 90m)"
            ),
        )
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed.strftime("%Y-%m-%dT%H:%M:%SZ")


async def _paged_latest_items(
    db: AsyncSession,
    sibling_ids,
    item_types,
    *,
    folder: Optional[str] = None,
    search: Optional[str] = None,
    exclude_deleted: bool = False,
    sort_sql: Optional[str] = None,
    sort_dir: str = "DESC",
    extra_where: Optional[str] = None,
    extra_params: Optional[Dict[str, Any]] = None,
    page: int = 1,
    size: int = 500,
):
    """Newest-wins dedup + display sort + pagination, all database-side.

    The Tier-2 list endpoints aggregate across every sibling snapshot
    (delta backups split content over many snapshots), which for heavy
    resources means hundreds of thousands of rows carrying full raw
    Graph payloads — jrue's USER_MAIL alone is 353k rows / 1.4 GB of
    metadata JSON. Materializing that per request OOMKilled the pod
    (2026-07-20 at 1.5 Gi, 2026-07-21 at 3 Gi), so only three bounded
    queries ever leave Postgres:

      1. COUNT over a GROUP BY dedup_key subquery for totalElements
         (hash-aggregates; COUNT(DISTINCT) can only sort-aggregate and
         spilled 405MB to disk on a 1.22M-row mailbox) — run
         concurrently with (2) on its own session;
      2. DISTINCT ON dedup (newest snapshot row wins per external_id)
         in a subquery, display-sorted + LIMIT/OFFSET in the outer
         query, returning one page of row ids;
      3. hydrate exactly those ids into ORM rows for the response
         formatters, re-ordered to the SQL page order.

    (1) and (2) are cached per sibling-set-version (see
    _paged_items_cache above); (3) always runs so no ORM state outlives
    its session.

    `sort_sql` is a SQL expression over `si.*` producing the display
    sort key (callers COALESCE it non-null so `sort_dir` needs no
    NULLS handling); default is capture order (`si.created_at`).
    """
    dedup_key = "COALESCE(si.external_id, si.id::text)"
    where_parts = [
        "si.snapshot_id = ANY(:sids)",
        "si.item_type = ANY(:types)",
    ]
    params: Dict[str, Any] = {
        "sids": [str(s) for s in sibling_ids],
        "types": list(item_types),
    }
    if folder is not None:
        where_parts.append("si.folder_path = :folder")
        params["folder"] = folder
    if search:
        where_parts.append("si.name ILIKE :search")
        params["search"] = f"%{search}%"
    if exclude_deleted:
        where_parts.append("si.is_deleted IS NOT TRUE")
    # Caller-supplied row predicate (currently the OneDrive recency window —
    # see list_snapshot_onedrive). Applied INSIDE the dedup subquery so the
    # COUNT and the page agree, and folded into both cache keys below so a
    # different window can never read another window's cached total.
    if extra_where:
        where_parts.append(extra_where)
        params.update(extra_params or {})
    where_sql = " AND ".join(where_parts)

    sort_dir = "ASC" if sort_dir.upper() == "ASC" else "DESC"
    sort_expr = sort_sql or "si.created_at"

    # ── Cache lookup ────────────────────────────────────────────────
    # Two caches, deliberately keyed differently (see _dedup_count_cache
    # above): the COUNT depends only on the sibling set + row filters,
    # the page ids additionally on sort/page/size. A user paging or
    # re-sorting therefore re-pays the page query only.
    state_fp, cacheable = await _sibling_state_fingerprint(db, sibling_ids)
    filter_key = (
        state_fp, tuple(item_types), folder, search, exclude_deleted,
        extra_where, tuple(sorted((extra_params or {}).items())),
    )
    page_key = filter_key + (sort_expr, sort_dir, page, size)
    now = _time.monotonic()

    total: Optional[int] = None
    page_ids: Optional[list] = None
    if cacheable:
        c_hit = _dedup_count_cache.get(filter_key)
        if c_hit is not None and (now - c_hit[0]) < _LIST_CACHE_TTL_S:
            total = c_hit[1]
        p_hit = _paged_items_cache.get(page_key)
        if p_hit is not None and (now - p_hit[0]) < _LIST_CACHE_TTL_S:
            page_ids = list(p_hit[1])

    if total is None or page_ids is None:
        # COUNT via GROUP BY subquery instead of COUNT(DISTINCT ...):
        # Postgres can only sort-aggregate COUNT(DISTINCT) (2.8s + a
        # 405MB disk spill on a 1.22M-row mailbox) while the GROUP BY
        # form hash-aggregates (1.6s, no spill). When the concurrency
        # budget allows, it runs CONCURRENTLY with the page query on
        # its own session — they don't depend on each other and each
        # costs seconds cold, so overlapping them nearly halves cold
        # latency. Past the budget it runs sequentially on the request
        # session instead (no extra pooled connection).
        count_sql = (
            f"SELECT COUNT(*) AS n FROM ("
            f"  SELECT 1 FROM snapshot_items si WHERE {where_sql}"
            f"  GROUP BY {dedup_key}"
            f") dedup"
        )

        async def _run_count_on(count_db) -> int:
            await _set_local_work_mem(count_db)
            row = (await count_db.execute(text(count_sql), params)).first()
            return int(row.n or 0) if row else 0

        async def _run_count_concurrent() -> int:
            async with _count_session_factory() as count_db:
                return await _run_count_on(count_db)

        # The permit is released at the await sites below (both the
        # success and the page-query-failure path), NOT inside the task:
        # a task cancelled before its first tick never enters its body,
        # so a release-in-finally there could leak the permit.
        count_task: Optional[asyncio.Task] = None
        if total is None and not _COUNT_CONCURRENCY.locked():
            # Non-blocking acquire: never WAIT on the budget — waiting
            # while holding the request connection is the hold-and-wait
            # shape that starves the pool under cold-cache bursts.
            await _COUNT_CONCURRENCY.acquire()
            count_task = asyncio.create_task(_run_count_concurrent())

        if page_ids is None:
            params_page = dict(params)
            params_page["limit"] = size
            params_page["offset"] = (page - 1) * size
            # DISTINCT ON requires its expression to lead the inner ORDER
            # BY, so the display sort can't live there — it goes on the
            # outer query. HOW it gets there decides the plan:
            #
            #  * A JSON sort key (mail's metadata->'raw'->>'received…')
            #    must NOT be evaluated inside the dedup — that detoasts
            #    the full raw Graph payload for every *input* row
            #    (16.4 s vs 2.9 s on jrue's 353k-row mailbox), so it is
            #    computed for the deduped winners only, via a self-join
            #    back to snapshot_items on the winning id.
            #  * A plain-column sort key (calendar's si.created_at,
            #    contacts' and OneDrive's LOWER(si.name)) is already in
            #    the row the dedup scanned. Routing it through the
            #    self-join anyway re-read every deduped winner through
            #    the pkey index to fetch a column already in hand.
            #    Measured on prod 2026-08-11 against Riley Stillman's
            #    calendar (18 sibling snapshots, 209,325 union rows,
            #    38,223 distinct events): 5.6 s / 235k shared buffers
            #    with the self-join vs 1.9 s / 44k buffers with the key
            #    selected inline — byte-identical page ids. That
            #    nested loop was 3.5 s of the 5.6 s.
            #
            # si.id / latest.id tiebreaks equal sort keys either way so
            # pages never overlap. Pinned by
            # test_plain_sort_key_skips_self_join.
            if "metadata" in sort_expr.lower():
                page_sql = (
                    f"SELECT si.id FROM ("
                    f"  SELECT DISTINCT ON ({dedup_key}) si.id"
                    f"  FROM snapshot_items si WHERE {where_sql}"
                    f"  ORDER BY {dedup_key}, si.created_at DESC"
                    f") latest JOIN snapshot_items si ON si.id = latest.id "
                    f"ORDER BY ({sort_expr}) {sort_dir}, si.id "
                    f"LIMIT :limit OFFSET :offset"
                )
            else:
                page_sql = (
                    f"SELECT latest.id FROM ("
                    f"  SELECT DISTINCT ON ({dedup_key}) si.id, ({sort_expr}) AS _sk"
                    f"  FROM snapshot_items si WHERE {where_sql}"
                    f"  ORDER BY {dedup_key}, si.created_at DESC"
                    f") latest "
                    f"ORDER BY latest._sk {sort_dir}, latest.id "
                    f"LIMIT :limit OFFSET :offset"
                )
            try:
                await _set_local_work_mem(db)
                page_rows = (await db.execute(text(page_sql), params_page)).all()
            except BaseException:
                if count_task is not None:
                    count_task.cancel()
                    try:
                        await count_task
                    except BaseException:
                        pass
                    finally:
                        _COUNT_CONCURRENCY.release()
                raise
            page_ids = [r.id for r in page_rows]
            if cacheable:
                _cache_put(
                    _paged_items_cache, _PAGED_ITEMS_CACHE_MAX,
                    page_key, (now, tuple(page_ids)),
                )

        if count_task is not None:
            try:
                total = await count_task
            finally:
                _COUNT_CONCURRENCY.release()
            if cacheable:
                _cache_put(
                    _dedup_count_cache, _DEDUP_COUNT_CACHE_MAX,
                    filter_key, (now, total),
                )
        elif total is None:
            total = await _run_count_on(db)
            if cacheable:
                _cache_put(
                    _dedup_count_cache, _DEDUP_COUNT_CACHE_MAX,
                    filter_key, (now, total),
                )

    if not page_ids:
        return [], total

    orm_rows = (await db.execute(
        select(SnapshotItem).where(SnapshotItem.id.in_(page_ids))
    )).scalars().all()
    by_id = {r.id: r for r in orm_rows}
    return [by_id[i] for i in page_ids if i in by_id], total


@app.get("/api/v1/resources/snapshots/{snapshot_id}/emails")
@app.get("/api/v1/resources/snapshots/{snapshot_id}/mail")
async def list_snapshot_emails(
    snapshot_id: str,
    page: int = Query(1, ge=1),
    size: int = Query(500, ge=1, le=500),
    folder: Optional[str] = Query(None, description="Filter to a specific mail folder path"),
    search: Optional[str] = Query(None, description="Case-insensitive substring match on name/subject"),
    source: Optional[str] = Query(None, description="'archive' → serve Online Archive items via this (gateway-allowed) path"),
    category: Optional[str] = Query(None, description="With source=archive: mail | contacts | calendar"),
    bodies: Optional[str] = Query(
        None,
        description=(
            "'none' omits body/attachments payloads from each row "
            "(~10x smaller pages; the Recovery UI fetches the selected "
            "email's body via the item-detail endpoint instead). "
            "Default returns full bodies for back-compat."
        ),
    ),
    db: AsyncSession = Depends(get_db),
):
    """Return emails with from/to/cc/subject/body/date extracted from metadata.

    ``source=archive`` serves the Online Archive here instead: the api-gateway
    allow-lists the per-content paths (/mail, /contacts, …) but not a new
    /archive path, so the archive folder-in-tab UI reaches its items through
    this endpoint. ``category`` then splits archive items by kind."""
    if (source or "").lower() == "archive":
        return await _serve_archive_items(db, snapshot_id, page, size, folder, search, category)
    # Delta-based mail backups split content across multiple snapshots.
    # Aggregate every EMAIL row across all sibling snapshots (newest-wins)
    # so the view shows "inbox state as of this snapshot" instead of just
    # the messages captured in the most recent delta.
    # Auto-resolve to USER_MAIL sibling if caller passed a non-mail
    # snapshot id — otherwise the endpoint returned 0 silently.
    sibling_ids = await _resolve_sibling_snapshot_ids(
        db, snapshot_id, target_child_type=ResourceType.USER_MAIL,
    )
    if not sibling_ids:
        sibling_ids = [UUID(snapshot_id)]
    # Dedup + sort + paginate in SQL (see _paged_latest_items). Page 1 =
    # newest emails: receivedDateTime is the inbox-arrival timestamp
    # (what Outlook / Gmail order on), sentDateTime covers Sent Items,
    # created_at covers malformed rows — ISO-8601 strings sort
    # lexicographically by time so the whole key stays text.
    # Graph delta deletion markers (@removed / deleted facet) are not
    # restorable emails — same rule /onedrive already applies below. They
    # carry no subject and no receivedDateTime/sentDateTime, so the sort
    # below falls through to created_at (the backup run's own timestamp)
    # and ranks every one of them above all real mail: 13,249 tombstones
    # buried Jason Rue's 435k dated messages behind pages of
    # "(no subject)" (2026-08-07). The walk no longer persists them;
    # exclude_deleted also hides the rows written before that fix.
    items, total = await _paged_latest_items(
        db, sibling_ids, ["EMAIL"],
        folder=folder, search=search, exclude_deleted=True,
        sort_sql=(
            "COALESCE(si.metadata->'raw'->>'receivedDateTime', "
            "si.metadata->'raw'->>'sentDateTime', "
            "to_char(si.created_at, 'YYYY-MM-DD\"T\"HH24:MI:SS'), '')"
        ),
        sort_dir="DESC", page=page, size=size,
    )

    # Heavy per-message payloads dropped when bodies == "none". The list
    # panel renders only subject/from/date/bodyPreview; shipping every
    # message's full HTML body (typ. 471KB per 50-row page vs ~45KB slim)
    # made tab-in latency payload-bound. The preview pane lazily fetches
    # the selected email via GET .../items/{item_id}, whose metadata.raw
    # still carries the full body.
    _HEAVY_RAW_KEYS = ("body", "uniqueBody", "attachments", "internetMessageHeaders")
    slim = bodies == "none"

    def fmt(i):
        raw = _raw(i)
        if slim:
            raw = {k: v for k, v in raw.items() if k not in _HEAVY_RAW_KEYS}
        from_addr = raw.get("from", {}).get("emailAddress", {})
        to_list = [r.get("emailAddress", {}) for r in raw.get("toRecipients", [])]
        cc_list = [r.get("emailAddress", {}) for r in raw.get("ccRecipients", [])]
        return {
            "id": str(i.id),
            "snapshotId": str(i.snapshot_id),
            "externalId": i.external_id,
            "itemType": i.item_type,
            "subject": raw.get("subject") or i.name,
            "from": f"{from_addr.get('name', '')} <{from_addr.get('address', '')}>".strip(" <>") or None,
            "to": "; ".join(f"{r.get('name','')} <{r.get('address','')}>".strip(" <>") for r in to_list),
            "cc": "; ".join(f"{r.get('name','')} <{r.get('address','')}>".strip(" <>") for r in cc_list),
            "date": raw.get("sentDateTime") or raw.get("receivedDateTime") or (i.created_at.isoformat() if i.created_at else None),
            "bodyPreview": raw.get("bodyPreview") or "",
            "body": raw.get("body", {}).get("content") or "",
            "bodyContentType": raw.get("body", {}).get("contentType") or "text",
            "hasAttachments": raw.get("hasAttachments", False),
            "attachments": raw.get("attachments", []),
            "folderPath": i.folder_path,
            "contentSize": i.content_size or 0,
            "isDeleted": i.is_deleted or False,
            "createdAt": i.created_at.isoformat() if i.created_at else "",
            "name": raw.get("subject") or i.name or "",
            "metadata": {"raw": raw},
        }

    return {"content": [fmt(i) for i in items], "totalElements": total, "totalPages": max(1, (total+size-1)//size), "size": size, "number": page}


_ARCHIVE_CATEGORY_MATCH = {
    # Which archive items belong under each content tab's "Online Archive"
    # folder, by MAPI class (extra_data.mapi_type) / friendly kind.
    "mail": lambda mt, k: mt.startswith("IPM.Note") or k == "Email" or (not mt and not k),
    "contacts": lambda mt, k: mt.startswith("IPM.Contact") or k == "Contact",
    "calendar": lambda mt, k: mt.startswith("IPM.Appointment") or mt.startswith("IPM.Schedule") or "Calendar" in (k or ""),
}


async def _serve_archive_folders(db: AsyncSession, snapshot_id: str, category: str):
    """Per-category archive folder list + counts for the left panel. Mirrors
    `_serve_archive_items`' sibling-resolution, first-seen dedup, and MAPI-kind
    match so each folder's count equals exactly what opening it (same folder +
    category) would show — no kind-agnostic over-count leaking mail totals onto
    the Contacts/Calendar tabs. Shape matches `get_snapshot_folders`."""
    sibling_ids = await _resolve_sibling_snapshot_ids(
        db, snapshot_id, target_child_type=ResourceType.USER_ARCHIVE,
    )
    if not sibling_ids:
        sibling_ids = [UUID(snapshot_id)]
    rows = (await db.execute(select(SnapshotItem).where(
        SnapshotItem.snapshot_id.in_(sibling_ids),
        SnapshotItem.item_type == "ARCHIVE_ITEM",
    ))).scalars().all()
    _match = _ARCHIVE_CATEGORY_MATCH.get((category or "").lower())
    seen: set = set()
    counts: Dict[str, int] = {}
    for it in rows:
        key = it.external_id or str(it.id)
        if key in seen:
            continue
        seen.add(key)
        ed = it.extra_data or {}
        if _match and not _match((ed.get("mapi_type") or ""), (ed.get("kind") or "")):
            continue
        p = it.folder_path or ""
        counts[p] = counts.get(p, 0) + 1
    content = [{"path": p, "count": c} for p, c in sorted(counts.items())]
    return {"content": content, "total": len(content), "page": 1, "size": len(content), "hasMore": False}


async def _serve_archive_items(db: AsyncSession, snapshot_id: str, page: int = 1,
                               size: int = 500, folder: Optional[str] = None,
                               search: Optional[str] = None, category: Optional[str] = None):
    """Exchange Online Archive items for the browse tab. Archive items are
    backed up as OPAQUE MAPI blobs (unparseable), so subject/body/from only
    exist once the ENRICHMENT service has staged them (extra_data.raw +
    enrich_status='enriched'). Un-enriched rows return their friendly kind
    ("Email"/"Contact"/…) + enrich_status so the UI shows a "processing" pill;
    enriched rows render through the same EmailPreview as primary mail.

    Browsing the tab also NUDGES on-demand enrichment (enrich.priority) for any
    rows still pending — so opening your archive processes it first."""
    sibling_ids = await _resolve_sibling_snapshot_ids(
        db, snapshot_id, target_child_type=ResourceType.USER_ARCHIVE,
    )
    if not sibling_ids:
        sibling_ids = [UUID(snapshot_id)]
    filters = [
        SnapshotItem.snapshot_id.in_(sibling_ids),
        SnapshotItem.item_type == "ARCHIVE_ITEM",
    ]
    if folder is not None:
        filters.append(SnapshotItem.folder_path == folder)
    if search:
        filters.append(SnapshotItem.name.ilike(f"%{search}%"))
    all_rows = (await db.execute(select(SnapshotItem).where(*filters))).scalars().all()
    seen: set = set()
    deduped = []
    for it in all_rows:
        key = it.external_id or str(it.id)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(it)

    # Split by content kind so each tab's "Online Archive" folder shows only its
    # own items (mail under Mail, contacts under Contacts, events under Calendar).
    _match = _ARCHIVE_CATEGORY_MATCH.get((category or "").lower())
    if _match:
        def _keep(r):
            ed = r.extra_data or {}
            return _match((ed.get("mapi_type") or ""), (ed.get("kind") or ""))
        deduped = [r for r in deduped if _keep(r)]

    def _arch_sort_key(r):
        raw = (r.extra_data or {}).get("raw") or {}
        ts = raw.get("receivedDateTime") or raw.get("sentDateTime") or (r.extra_data or {}).get("received")
        return ts or (r.created_at.isoformat() if r.created_at else "")
    deduped.sort(key=_arch_sort_key, reverse=True)
    total = len(deduped)
    offset = (page - 1) * size
    items = deduped[offset: offset + size]

    # Fire on-demand enrichment for any un-enriched rows in scope (idempotent).
    await _maybe_trigger_archive_enrichment(db, items)

    def fmt(i):
        ed = i.extra_data or {}
        raw = ed.get("raw") or {}
        enriched = ed.get("enrich_status") == "enriched"
        from_addr = raw.get("from", {}).get("emailAddress", {})
        to_list = [r.get("emailAddress", {}) for r in raw.get("toRecipients", [])]
        cc_list = [r.get("emailAddress", {}) for r in raw.get("ccRecipients", [])]
        # Enriched → real subject; else the friendly kind so the row isn't blank.
        display = raw.get("subject") if enriched else None
        display = display or i.name or ed.get("kind") or "Archived item"
        return {
            "id": str(i.id),
            "snapshotId": str(i.snapshot_id),
            "externalId": i.external_id,
            "itemType": i.item_type,  # ARCHIVE_ITEM
            "subject": raw.get("subject") or i.name,
            "from": f"{from_addr.get('name', '')} <{from_addr.get('address', '')}>".strip(" <>") or None,
            "to": "; ".join(f"{r.get('name','')} <{r.get('address','')}>".strip(" <>") for r in to_list),
            "cc": "; ".join(f"{r.get('name','')} <{r.get('address','')}>".strip(" <>") for r in cc_list),
            "date": raw.get("sentDateTime") or raw.get("receivedDateTime") or ed.get("received") or (i.created_at.isoformat() if i.created_at else None),
            "bodyPreview": raw.get("bodyPreview") or "",
            "body": raw.get("body", {}).get("content") or "",
            "bodyContentType": raw.get("body", {}).get("contentType") or "text",
            "hasAttachments": raw.get("hasAttachments", ed.get("has_attachments", False)),
            "attachments": raw.get("attachments", []),
            "folderPath": i.folder_path,
            "contentSize": i.content_size or 0,
            "isDeleted": i.is_deleted or False,
            "createdAt": i.created_at.isoformat() if i.created_at else "",
            "name": display,
            # enrich_status drives the UI's "processing" pill; raw drives preview.
            "metadata": {"raw": raw, "enrich_status": ed.get("enrich_status"),
                         "kind": ed.get("kind"), "mapi_type": ed.get("mapi_type")},
        }

    return {"content": [fmt(i) for i in items], "totalElements": total, "totalPages": max(1, (total+size-1)//size), "size": size, "number": page}


@app.get("/api/v1/resources/snapshots/{snapshot_id}/archive")
async def list_snapshot_archive(
    snapshot_id: str,
    page: int = Query(1, ge=1),
    size: int = Query(500, ge=1),
    folder: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """Direct Online Archive path. Kept for when the api-gateway allow-lists it;
    today the frontend reaches archive items via /mail?source=archive (which the
    gateway already forwards)."""
    return await _serve_archive_items(db, snapshot_id, page, size, folder, search, category)


# ============================================================================
# Teams hosted-content proxy (Recovery UI inline images)
#
# Teams chat message bodies contain `<img src="https://graph.microsoft.com/.../
# hostedContents/{id}/$value">` URLs. Those URLs require an OAuth bearer token,
# which the browser doesn't have — every inline image renders broken. The
# bytes are already in our storage (chat_url_cache or CHAT_HOSTED_CONTENT
# items). This proxy serves them.
#
# Flow:
#  1. backup-worker fetches /chats/{cid}/messages with $expand=hostedContents,
#     gets each message's hostedContents[] list (id + contentType).
#  2. _userchats_backup_hosted_contents downloads each hostedContent's bytes
#     via /chats/{cid}/messages/{mid}/hostedContents/{hcid}/$value and stores
#     them as CHAT_HOSTED_CONTENT snapshot_items (inline_b64 for small ones,
#     SeaweedFS blob_path for large).
#  3. snapshot-service /chats endpoint runs _rewrite_hosted_content_urls() on
#     each body before returning it to the UI, replacing the Graph URL with
#     a self-hosted proxy URL.
#  4. The browser hits this proxy, which serves the bytes we already have.
# ============================================================================
import hashlib as _hashlib

_HOSTED_CONTENT_RE = _re.compile(
    r'https?://graph\.microsoft\.com/[^"\'<>]*?/hostedContents/([^/"\']+)/\$value',
    _re.IGNORECASE,
)


def _rewrite_hosted_content_urls(body: str, tenant_id: str) -> str:
    """Replace inline hostedContents image URLs with our proxy URLs.

    SHA matches what backup-worker computes via hashlib.sha256(url) at the
    time the cache row / hosted-content row is written. Same URL → same SHA
    → same proxy path.
    """
    if not body:
        return body

    def _sub(m: "_re.Match[str]") -> str:
        original_url = m.group(0)
        u_sha = _hashlib.sha256(original_url.encode("utf-8")).hexdigest()
        # API gateway / direct UI both call snapshot-service on the same
        # origin; a relative path keeps the request inside whichever host
        # the user is on (avoids CORS).
        return f"/api/v1/teams/hosted-content/{tenant_id}/{u_sha}"

    return _HOSTED_CONTENT_RE.sub(_sub, body)


@app.get("/api/v1/teams/hosted-content/{tenant_id}/{url_sha}")
async def serve_hosted_content(
    tenant_id: str, url_sha: str,
    db: AsyncSession = Depends(get_db),
):
    """Serve an inline-image / hostedContent blob from our storage.

    Resolution order:
      1. chat_url_cache row by (tenant_id, url_sha256) — covers any URL
         we hashed at backup time. inline_b64 fast path, blob_path
         streamed from SeaweedFS.
      2. CHAT_HOSTED_CONTENT snapshot_items fallback — the dedicated
         hosted-content path stores rows here instead of chat_url_cache.
      3. 404 — the bytes weren't captured (older backup before the
         $expand=hostedContents fix, or PERMISSION error at fetch).
    """
    # Try chat_url_cache first.
    row = (await db.execute(text(
        "SELECT inline_b64, blob_path, content_size "
        "  FROM chat_url_cache "
        " WHERE tenant_id = :t AND url_sha256 = :u "
        " LIMIT 1"
    ), {"t": tenant_id, "u": url_sha})).first()

    if row is not None:
        if row.inline_b64:
            try:
                data = _b64.b64decode(row.inline_b64)
            except Exception:
                raise HTTPException(status_code=500, detail="cache decode error")
            return StreamingResponse(
                _io.BytesIO(data),
                media_type="image/*",
                headers={"Cache-Control": "private, max-age=3600"},
            )
        if row.blob_path:
            # Look up a shard via any chat_thread in this tenant; all chat
            # blobs go to the same workload container.
            res = (await db.execute(text(
                "SELECT s.id AS sid, s.resource_id "
                "  FROM snapshots s "
                "  JOIN resources r ON r.id = s.resource_id "
                " WHERE r.tenant_id = :t "
                "   AND r.type = 'USER_CHATS' "
                " ORDER BY s.created_at DESC "
                " LIMIT 1"
            ), {"t": tenant_id})).first()
            if res is not None:
                shard, _tid, candidates = await _load_blob_context(
                    db, str(res.sid),
                )
                if shard is not None:
                    data = await _download_item_blob(
                        shard, tenant_id, candidates, row.blob_path,
                    )
                    if data is not None:
                        return StreamingResponse(
                            _io.BytesIO(data),
                            media_type="image/*",
                            headers={"Cache-Control": "private, max-age=3600"},
                        )

    # Fallback: CHAT_HOSTED_CONTENT snapshot_items. Inline images stored
    # via _userchats_backup_hosted_contents land here, not in chat_url_cache.
    # Match on extra_data->'source_message_id' isn't possible from URL
    # alone, so probe by any hc that maps to this sha via parent_external_id.
    # Simpler: scan recent hosted-content rows whose blob_path or inline_b64
    # corresponds to the URL we'd reconstruct. Skipped for now — chat_url_cache
    # is the canonical path for hostedContents going forward.

    raise HTTPException(status_code=404, detail="hosted content not in vault")


@app.get("/api/v1/resources/snapshots/{snapshot_id}/messages")
@app.get("/api/v1/resources/snapshots/{snapshot_id}/chats")
async def list_snapshot_messages(
    snapshot_id: str,
    page: int = Query(1, ge=1),
    size: int = Query(500, ge=1, le=500),
    chatId: Optional[str] = Query(None),
    folder: Optional[str] = Query(None, description="Filter to one chat by folder_path"),
    search: Optional[str] = Query(None, description="Case-insensitive substring match on body"),
    db: AsyncSession = Depends(get_db),
):
    """Return Teams chat messages joined to the tenant-singleton
    ``chat_thread_messages`` store.

    Since the 2026-05-13 Level 2 refactor, ``snapshot_items`` for chat
    messages holds only thin pointer rows (per-snapshot inventory).
    The actual body / sender / attachments live in
    ``chat_thread_messages``, keyed by (chat_thread_id, message_external_id).
    A row resolves via:
      snapshot_items (parent_external_id, tenant_id, external_id)
        → chat_threads (tenant_id, chat_id)
        → chat_thread_messages (chat_thread_id, message_external_id)

    Auto-resolves to the USER_CHATS sibling when the caller passes the
    ENTRA_USER parent or a non-chat sibling's snapshot id.
    """
    catalogue = await _catalogue_snapshot(db, snapshot_id, resolve_chat_sibling=True)
    if catalogue is not None:
        if (catalogue.extra_data or {}).get("chat_inventory_format") == 1:
            return await _catalogue_message_response(db, catalogue, page=page, size=size,
                chat_id=chatId, folder=folder, search=search)
        snapshot_id = str(catalogue.id)
    CHAT_TYPES = ("TEAMS_CHAT_MESSAGE", "TEAMS_MESSAGE", "TEAMS_MESSAGE_REPLY")
    sibling_ids = await _resolve_sibling_snapshot_ids(
        db, snapshot_id, target_child_type=ResourceType.USER_CHATS,
    )
    if not sibling_ids:
        sibling_ids = [UUID(snapshot_id)]

    # Two predicate lists: `si_where_parts` is the subset that only
    # touches snapshot_items (so it can be evaluated without either
    # LEFT JOIN — see the COUNT below), `where_parts` is the full set.
    si_where_parts = [
        "si.snapshot_id = ANY(:sids)",
        "si.item_type = ANY(:types)",
    ]
    params: Dict[str, Any] = {
        "sids": [str(s) for s in sibling_ids],
        "types": list(CHAT_TYPES),
    }
    if folder is not None:
        si_where_parts.append("si.folder_path = :folder")
        params["folder"] = folder
    if chatId:
        si_where_parts.append("si.parent_external_id = :cid")
        params["cid"] = chatId
    where_parts = list(si_where_parts)
    if search:
        where_parts.append("ctm.body_content ILIKE :search")
        params["search"] = f"%{search}%"

    # The JOIN to chat_threads is keyed by (tenant_id, chat_id) — both
    # come from the snapshot_items pointer row, no need to load tenant
    # from the resource. LEFT JOIN tolerates pointer rows whose backing
    # chat_thread_messages was deleted (e.g. mid-cascade).
    # P2: filter soft-archived singleton rows so reads never see a chat
    # that ops has scheduled for purge.
    # ── A: per-snapshot pointer rows (existing behavior; also the source for
    #       export-path chats whose bodies live in si.metadata, ctm = NULL).
    base_sql_a = (
        " FROM snapshot_items si "
        " LEFT JOIN chat_threads ct "
        "        ON ct.tenant_id = si.tenant_id "
        "       AND ct.chat_id = si.parent_external_id "
        "       AND ct.archived_at IS NULL "
        " LEFT JOIN chat_thread_messages ctm "
        "        ON ctm.chat_thread_id = ct.id "
        "       AND ctm.message_external_id = si.external_id "
        "       AND ctm.archived_at IS NULL "
        f"WHERE {' AND '.join(where_parts)} "
    )

    # ── B: durable-store messages for the chats THIS snapshot covers that have
    # NO pointer row here. Cross-member completeness fix: a shared 1:1/group
    # chat has ONE durable store + ONE drain cursor but lives in EVERY member's
    # chat list. Whichever member's drain runs first captures the messages into
    # THAT member's snapshot (advancing the shared cursor), so other members'
    # snapshots miss them even though the data is safely in chat_thread_messages
    # (verified live: a member's snapshot had 0 pointers for a chat the store
    # holds 3010 messages of). Scope is restricted to the chats this snapshot
    # references (via si), so we never surface chats the user isn't a member of.
    _chat_scope_sql = (
        " SELECT DISTINCT ct2.id FROM snapshot_items si2 "
        "   JOIN chat_threads ct2 ON ct2.tenant_id = si2.tenant_id "
        "    AND ct2.chat_id = si2.parent_external_id AND ct2.archived_at IS NULL "
        "  WHERE si2.snapshot_id = ANY(:sids) AND si2.item_type = ANY(:types) "
    )
    if chatId:
        _chat_scope_sql += " AND si2.parent_external_id = :cid "
    if folder is not None:
        _chat_scope_sql += " AND si2.folder_path = :folder "
    _b_where = [
        "ct.id IN (" + _chat_scope_sql + ")",
        "ctm.archived_at IS NULL",
        "NOT EXISTS (SELECT 1 FROM snapshot_items si3 "
        "  WHERE si3.snapshot_id = ANY(:sids) AND si3.item_type = ANY(:types) "
        "    AND si3.parent_external_id = ct.chat_id "
        "    AND si3.external_id = ctm.message_external_id) ",
    ]
    if search:
        _b_where.append("ctm.body_content ILIKE :search")
    base_sql_b = (
        " FROM chat_thread_messages ctm "
        " JOIN chat_threads ct ON ct.id = ctm.chat_thread_id AND ct.archived_at IS NULL "
        f"WHERE {' AND '.join(_b_where)} "
    )

    # Unified projection so A and B align for UNION ALL. item_type cast to text
    # so the enum (A) and the literal (B) match; si_created_at cast to plain
    # timestamp so A's `timestamp` and B's `timestamptz` source align.
    _cols_a = (
        "    si.id, si.snapshot_id, si.external_id, si.parent_external_id, "
        "    si.tenant_id AS si_tenant_id, si.item_type::text AS item_type, "
        "    si.folder_path, si.content_size, si.is_deleted, "
        "    si.created_at::timestamp AS si_created_at, si.metadata AS si_metadata, "
        "    ct.chat_topic, ct.chat_type, ct.member_names_json, "
        "    ctm.body_content, ctm.body_content_type, "
        "    ctm.from_user_id, ctm.from_display_name, "
        "    ctm.created_date_time, ctm.last_modified_date_time, "
        "    ctm.deleted_date_time, ctm.metadata_raw "
    )
    _cols_b = (
        "    NULL::uuid AS id, NULL::uuid AS snapshot_id, "
        "    ctm.message_external_id AS external_id, ct.chat_id AS parent_external_id, "
        "    ct.tenant_id AS si_tenant_id, 'TEAMS_CHAT_MESSAGE'::text AS item_type, "
        "    NULL::varchar AS folder_path, ctm.content_size, "
        "    (ctm.deleted_date_time IS NOT NULL) AS is_deleted, "
        "    ctm.created_date_time::timestamp AS si_created_at, NULL::json AS si_metadata, "
        "    ct.chat_topic, ct.chat_type, ct.member_names_json, "
        "    ctm.body_content, ctm.body_content_type, "
        "    ctm.from_user_id, ctm.from_display_name, "
        "    ctm.created_date_time, ctm.last_modified_date_time, "
        "    ctm.deleted_date_time, ctm.metadata_raw "
    )
    _union_sql = (
        "(SELECT " + _cols_a + base_sql_a +
        " UNION ALL SELECT " + _cols_b + base_sql_b + ") u "
    )

    # Newest-first by message send time. NULLs (pointer rows that lost
    # their backing message somehow) sort last.
    order_clause = (
        " ORDER BY ctm.created_date_time DESC NULLS LAST, "
        "          si.created_at DESC "
    )

    # Dedup by external_id across sibling snapshots — multiple snapshots
    # carry pointer rows for the same Graph msg id (the chat_thread_messages
    # row is single, so the body is consistent). Use DISTINCT ON to keep
    # one pointer per external_id (the newest-by-snapshot one).
    # Count the same durable union as the page, with no body joins on
    # the pointer leg unless searching. GROUP BY permits hash aggregation.
    pointer_count_from = base_sql_a if search else (
        " FROM snapshot_items si "
        f" WHERE {' AND '.join(si_where_parts)} "
    )
    sql_count = (
        "SELECT COUNT(*) AS n FROM ("
        " SELECT si.external_id " + pointer_count_from +
        " GROUP BY si.external_id "
        " UNION SELECT ctm.message_external_id AS external_id " + base_sql_b +
        ") dedup"
    )

    state_fp, cacheable = await _sibling_state_fingerprint(db, sibling_ids)
    durable_state = (await db.execute(text("""
        WITH tenant_scope AS (
            SELECT DISTINCT tenant_id FROM snapshots WHERE id = ANY(:sids)
        )
        SELECT MAX(ct.updated_at) AS updated_at, COUNT(ct.id) AS thread_count,
               EXISTS (
                   SELECT 1 FROM snapshots s
                   JOIN resources r ON r.id = s.resource_id
                   WHERE s.tenant_id IN (SELECT tenant_id FROM tenant_scope)
                     AND r.type = 'USER_CHATS'
                     AND s.status IN ('IN_PROGRESS', 'PENDING_DELETION')
               ) AS changing
        FROM chat_threads ct
        WHERE ct.tenant_id IN (SELECT tenant_id FROM tenant_scope)
    """), {"sids": list(sibling_ids)})).first()
    # A thread's updated_at changes on claim and final drain. Do not cache
    # during any member's active backup: messages commit between those stamps.
    cacheable = cacheable and durable_state is not None and not durable_state.changing
    durable_fp = (
        durable_state.updated_at, durable_state.thread_count,
    ) if durable_state is not None else ()
    count_key = ("chats", state_fp, durable_fp, folder, chatId, search)
    now = _time.monotonic()
    total: Optional[int] = None
    if cacheable:
        c_hit = _dedup_count_cache.get(count_key)
        if c_hit is not None and (now - c_hit[0]) < _LIST_CACHE_TTL_S:
            total = c_hit[1]

    async def _run_chat_count_on(count_db) -> int:
        await _set_local_work_mem(count_db)
        row = (await count_db.execute(text(sql_count), params)).first()
        return int(row.n or 0) if row else 0

    async def _run_chat_count_concurrent() -> int:
        async with _count_session_factory() as count_db:
            return await _run_chat_count_on(count_db)

    # Permit released at the await sites below, never inside the task —
    # a task cancelled before its first tick never enters its body, so
    # a release-in-finally there could leak the permit.
    count_task: Optional[asyncio.Task] = None
    if total is None and not _COUNT_CONCURRENCY.locked():
        await _COUNT_CONCURRENCY.acquire()
        count_task = asyncio.create_task(_run_chat_count_concurrent())

    offset = (page - 1) * size
    params_page = dict(params)
    params_page["limit"] = size
    params_page["offset"] = offset

    # Pagination must slice by message DATE, not by external_id. PG's
    # DISTINCT ON requires its leading sort column to be the dedup key
    # (external_id) — which means a single-statement query paginated with
    # LIMIT/OFFSET on that statement slices rows in external_id order, not
    # chronological. The result: page 1 is "50 messages whose external_ids
    # happen to sort first lexicographically", not "50 newest messages".
    # Symptom: November messages appearing before December (or any random
    # date scatter) as the user paginates.
    #
    # Fix: dedup in a subquery, then sort + paginate the deduped set by
    # message timestamp DESC. The in-process re-sort below is now redundant
    # but kept as a safety net for NULL timestamps.
    # Dedup by external_id over the A∪B union, newest-by-message-time wins,
    # then paginate by message date. (si.metadata still hydrates body/sender
    # for export-path chats where ctm is NULL — those rows come from the A leg;
    # B rows always carry ctm, so they need no si.metadata fallback.)
    sql_page = (
        "SELECT * FROM ("
        "  SELECT DISTINCT ON (external_id) * FROM " + _union_sql +
        "  ORDER BY external_id, created_date_time DESC NULLS LAST, si_created_at DESC "
        ") sub "
        " ORDER BY sub.created_date_time DESC NULLS LAST, "
        "          sub.si_created_at DESC "
        " LIMIT :limit OFFSET :offset"
    )
    # PG requires the outer ORDER BY for pagination to match the DISTINCT ON
    # leading column. Re-sort the page in-process by message timestamp.
    # The union sort spills to temp for heavy chats (1.2 GB written on
    # Jason Rue's 8.5M-row union at the 4 MB default) — same SET LOCAL
    # bump the /mail /calendar /onedrive /contacts helper already does.
    try:
        await _set_local_work_mem(db)
        rows = (await db.execute(text(sql_page), params_page)).all()
    except BaseException:
        if count_task is not None:
            count_task.cancel()
            try:
                await count_task
            except BaseException:
                pass
            finally:
                _COUNT_CONCURRENCY.release()
        raise
    if count_task is not None:
        try:
            total = await count_task
        finally:
            _COUNT_CONCURRENCY.release()
        if cacheable:
            _cache_put(
                _dedup_count_cache, _DEDUP_COUNT_CACHE_MAX,
                count_key, (now, total),
            )
    elif total is None:
        # Past the concurrency budget — run it on the request session.
        total = await _run_chat_count_on(db)
        if cacheable:
            _cache_put(
                _dedup_count_cache, _DEDUP_COUNT_CACHE_MAX,
                count_key, (now, total),
            )
    rows = sorted(
        rows,
        key=lambda r: (r.created_date_time or r.si_created_at, ""),
        reverse=True,
    )

    # Build a tenant-wide aadUserId → displayName lookup so we can fill in
    # eventDetail.initiator.user.displayName + members[].displayName for
    # Teams system events (joined/left/added/call started/etc.). Graph
    # returns those events with displayName=null, so the UI used to render
    # "Someone started a call" instead of the real user name. We get the
    # mapping from two sources: any chat message with a populated
    # from.user.{id,displayName} pair, and the ENTRA_USER resource rows.
    _user_name_map: Dict[str, str] = {}
    _tenant_ids_seen = {
        str(r.si_tenant_id) for r in rows
        if getattr(r, "si_tenant_id", None)
    }
    if _tenant_ids_seen:
        try:
            # PREFER the resources.display_name (it's the canonical name
            # Graph hands back from /users/{id}, e.g. "Vinay Chauhan").
            # The chat_thread_messages source can have multiple variants
            # for the same user — Graph returns the bare UPN local-part
            # ("vinay.chauhan") for some message types (typically system
            # events and bot-relayed ones), so picking the first DISTINCT
            # row produced a lowercase mail-like name in the UI.
            r_rows = (await db.execute(text(
                "SELECT external_id, display_name FROM resources "
                " WHERE tenant_id::text = ANY(:tids) "
                "   AND type = 'ENTRA_USER' "
                "   AND external_id IS NOT NULL "
                "   AND display_name IS NOT NULL"
            ), {"tids": list(_tenant_ids_seen)})).all()
            for rr in r_rows:
                if rr.external_id:
                    _user_name_map[str(rr.external_id)] = rr.display_name or ""
            # Backfill from chat_thread_messages for users without a
            # resource row. Choose the LONGEST display_name per user id
            # as a proxy for the most complete variant (e.g. "Vinay
            # Chauhan" wins over "Vinay" wins over "vinay.chauhan").
            id_rows = (await db.execute(text(
                "SELECT from_user_id AS uid, "
                "       (ARRAY_AGG(from_display_name ORDER BY LENGTH(from_display_name) DESC))[1] AS name "
                "  FROM chat_thread_messages ctm "
                "  JOIN chat_threads ct ON ct.id = ctm.chat_thread_id "
                " WHERE ct.tenant_id::text = ANY(:tids) "
                "   AND ctm.from_user_id IS NOT NULL "
                "   AND ctm.from_display_name IS NOT NULL "
                "   AND ctm.from_display_name <> '' "
                " GROUP BY from_user_id"
            ), {"tids": list(_tenant_ids_seen)})).all()
            for ur in id_rows:
                if ur.uid and ur.name and ur.uid not in _user_name_map:
                    _user_name_map[str(ur.uid)] = ur.name
        except Exception:
            # Lookup is best-effort; system events just fall back to
            # the generic "Someone" / "a member" labels if it fails.
            pass

    def _fill_user_names(ed: Any) -> Any:
        """Recursively walk eventDetail and fill displayName for any user
        whose id we recognise. Handles two Graph shapes:
          1. nested:  {"user": {"id":..., "displayName": null}}
                      (initiator.user, mentioned.user)
          2. flat:    {"id":..., "displayName": null, "userIdentityType":"aadUser"}
                      (eventDetail.members[])
        Returns the same dict (mutated). Safe on non-dict/non-list inputs."""
        if isinstance(ed, dict):
            # Nested shape (initiator/mentioned).
            u = ed.get("user")
            if isinstance(u, dict):
                uid = u.get("id")
                if uid and not u.get("displayName"):
                    name = _user_name_map.get(str(uid))
                    if name:
                        u["displayName"] = name
            # Flat shape (members[] entries) — detected by the presence
            # of both id AND a null displayName + a userIdentityType,
            # which is the Graph signature for an aadUser identity.
            if (
                ed.get("id")
                and ed.get("displayName") in (None, "")
                and "userIdentityType" in ed
            ):
                uid = ed.get("id")
                name = _user_name_map.get(str(uid))
                if name:
                    ed["displayName"] = name
            for v in ed.values():
                _fill_user_names(v)
        elif isinstance(ed, list):
            for v in ed:
                _fill_user_names(v)
        return ed

    # P6: read-time integrity-gap detection. The LEFT JOIN to
    # chat_thread_messages returns body_content=NULL when a pointer row
    # has lost its backing singleton (bit-rot, accidental delete, or a
    # half-committed drain that pre-dates the P5 transaction wrapper).
    # If >20% of a single chat's pointer rows on this page miss, fire one
    # INTEGRITY_GAP audit event per affected chat. The nightly P4 verifier
    # is the canonical repair path — this is the safety net that alerts
    # ops if a gap slips through.
    try:
        from collections import defaultdict as _dd
        _by_chat: Dict[str, Dict[str, Any]] = _dd(lambda: {
            "total": 0, "missing": 0, "tenant_id": None,
        })
        for _r in rows:
            cid = _r.parent_external_id or ""
            slot = _by_chat[cid]
            slot["total"] += 1
            if not _r.body_content:
                slot["missing"] += 1
            if slot["tenant_id"] is None and getattr(_r, "si_tenant_id", None):
                slot["tenant_id"] = _r.si_tenant_id
        for _cid, _s in _by_chat.items():
            if _s["total"] < 5:
                continue  # tiny pages — noise
            miss_pct = (_s["missing"] / float(_s["total"])) * 100.0
            if miss_pct < 20.0:
                continue
            try:
                async with _httpx.AsyncClient(timeout=2.0) as _c:
                    await _c.post(
                        f"{settings.AUDIT_SERVICE_URL}/api/v1/audit/log",
                        headers=internal_audit_headers(),
                        json={
                            "action": "INTEGRITY_GAP",
                            "actor_type": "SYSTEM",
                            "tenant_id": (
                                str(_s["tenant_id"]) if _s["tenant_id"] else None
                            ),
                            "resource_type": "CHAT_THREAD",
                            "resource_id": _cid,
                            "outcome": "DETECTED",
                            "details": {
                                "chat_id": _cid,
                                "page_total": _s["total"],
                                "missing_count": _s["missing"],
                                "miss_pct": round(miss_pct, 2),
                                "source": "snapshot-service:/chats",
                            },
                        },
                    )
            except Exception:
                pass  # best-effort; never block the read
    except Exception:
        pass

    def _fmt(r) -> Dict[str, Any]:
        # Two-tier hydration:
        #   tier 1 — chat_thread_messages (ctm) row (the canonical
        #            singleton store, populated by chat-export-worker)
        #   tier 2 — snapshot_items.metadata.raw (the per-row Graph
        #            payload stamped by backup-worker's
        #            TEAMS_CHAT_EXPORT path)
        # The new fallback recovers chat data on snapshots where the
        # chat-export-worker pipeline didn't run / failed but the
        # backup-worker still captured the full message into
        # snapshot_items.metadata. All ~19k messages on snapshot
        # f9e39faa-* are in this state.
        raw = r.metadata_raw if isinstance(r.metadata_raw, dict) else {}
        si_meta = getattr(r, "si_metadata", None)
        # JSON column can come back as dict (driver) or str (older PG
        # driver setting). Parse defensively; never raise on bad JSON.
        if isinstance(si_meta, str):
            try:
                import json as _json
                si_meta = _json.loads(si_meta)
            except Exception:
                si_meta = None
        if not isinstance(si_meta, dict):
            si_meta = {}
        si_raw = si_meta.get("raw") if isinstance(si_meta.get("raw"), dict) else {}
        if not raw and si_raw:
            raw = si_raw

        # Fill in displayName for any user id we know in eventDetail (and
        # in from.user for safety). Mutates a shallow copy so we don't
        # poison the SQLAlchemy ORM cache.
        if raw:
            try:
                import copy as _copy
                raw = _copy.deepcopy(raw)
                if raw.get("eventDetail"):
                    _fill_user_names(raw["eventDetail"])
                if raw.get("from"):
                    _fill_user_names(raw["from"])
            except Exception:
                pass

        # body — prefer ctm.body_content; fall back to raw.body.content
        body_text = r.body_content or ""
        if not body_text and isinstance(raw.get("body"), dict):
            body_text = raw["body"].get("content") or ""

        # Rewrite inline-image URLs from Graph's auth-only hostedContents
        # endpoint to our snapshot-service proxy, which serves the bytes
        # we captured at backup time. Without this rewrite the browser
        # gets a 401 + CORS error and shows a broken-image icon.
        tenant_id_for_body = (
            str(r.si_tenant_id) if getattr(r, "si_tenant_id", None) else ""
        )
        if tenant_id_for_body and body_text and "hostedContents" in body_text:
            body_text = _rewrite_hosted_content_urls(body_text, tenant_id_for_body)

        # sender / senderEmail — prefer ctm columns; fall back to raw.from
        sender = r.from_display_name or ""
        if not sender and isinstance(raw.get("from"), dict):
            _from_user = raw["from"].get("user") or {}
            sender = _from_user.get("displayName") or ""
        sender_email = ""
        if isinstance(raw.get("from"), dict):
            sender_email = (raw["from"].get("user") or {}).get(
                "userPrincipalName"
            ) or ""

        # body content type — prefer ctm column; fall back to raw.body.contentType
        body_ct = r.body_content_type or ""
        if not body_ct and isinstance(raw.get("body"), dict):
            body_ct = raw["body"].get("contentType") or ""
        body_ct = body_ct or "html"

        # date — ctm.created_date_time, else raw.createdDateTime, else si.created_at
        if r.created_date_time:
            date_iso = r.created_date_time.isoformat()
        elif raw.get("createdDateTime"):
            date_iso = str(raw.get("createdDateTime"))
        elif r.si_created_at:
            date_iso = r.si_created_at.isoformat()
        else:
            date_iso = None

        # chatTopic — _compose_chat_name when ct row matched; else use
        # si.metadata.chatTopic which TEAMS_CHAT_EXPORT stamps alongside raw
        if r.parent_external_id:
            chat_topic_out = _compose_chat_name(
                str(r.parent_external_id or ""),
                r.chat_topic,
                r.chat_type,
                r.member_names_json,
            )
        else:
            chat_topic_out = (
                r.chat_topic
                or (si_meta.get("chatTopic") if isinstance(si_meta, dict) else None)
                or ""
            )

        return {
            "id": str(r.id),
            "snapshotId": str(r.snapshot_id),
            "externalId": r.external_id,
            "itemType": r.item_type,
            "sender": sender,
            "senderEmail": sender_email,
            "body": body_text,
            "bodyContentType": body_ct,
            "date": date_iso,
            "chatTopic": chat_topic_out,
            "channelName": "",
            "folderPath": r.folder_path,
            "attachments": raw.get("attachments", []) if isinstance(raw, dict) else [],
            "mentions": raw.get("mentions", []) if isinstance(raw, dict) else [],
            "isDeleted": bool(r.deleted_date_time) or bool(r.is_deleted),
            "isReply": r.item_type == "TEAMS_MESSAGE_REPLY",
            "contentSize": r.content_size or 0,
            "createdAt": r.si_created_at.isoformat() if r.si_created_at else "",
            "name": sender,
            "metadata": {"raw": raw},
        }

    return {
        "content": [_fmt(r) for r in rows],
        "totalElements": total,
        "totalPages": max(1, (total + size - 1) // size),
        "size": size,
        "number": page,
    }


WELL_KNOWN_CONTACT_FOLDERS = [
    "Contacts", "Recipient Cache", "Deleted Items", "Recoverable Items",
]


@app.get("/api/v1/resources/snapshots/{snapshot_id}/contact-folders")
async def list_snapshot_contact_folders(
    snapshot_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Distinct folder_path values for USER_CONTACT items in a snapshot.
    Powers the folder-grain checkbox subgroup in the Download modal.

    Sort order: well-known folders (Contacts, Recipient Cache, Deleted
    Items, Recoverable Items) first in canonical order, then any custom
    folders alphabetically."""
    try:
        sid = UUID(snapshot_id)
    except ValueError:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail="invalid snapshot_id")

    stmt = (
        select(distinct(SnapshotItem.folder_path))
        .where(SnapshotItem.snapshot_id == sid)
        .where(SnapshotItem.item_type == "USER_CONTACT")
        .where(SnapshotItem.folder_path.isnot(None))
    )
    rows = (await db.execute(stmt)).scalars().all()
    seen = {f for f in rows if f}
    well_known = [f for f in WELL_KNOWN_CONTACT_FOLDERS if f in seen]
    others = sorted(seen - set(WELL_KNOWN_CONTACT_FOLDERS))
    return {"folders": well_known + others}


@app.get("/api/v1/resources/snapshots/{snapshot_id}/chats/groups")
async def list_snapshot_chat_groups(
    snapshot_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Distinct chats inside a USER_CHATS snapshot, with display name + count.

    Aggregates pointer rows from this snapshot (+ siblings) by
    parent_external_id = chat_id, then joins chat_threads for the
    canonical chat_topic and chat_thread_messages for the newest message
    timestamp. Powers the chats tab's left panel — clicking a row sets
    ?chatId=<id> on the messages query.

    Auto-resolves to the USER_CHATS sibling if the caller passed the
    ENTRA_USER parent's snapshot id or a non-chat sibling's. Aggregating
    across all prior USER_CHATS snapshots via the sibling helper means a
    delta-only later run does not drop threads captured by an earlier
    full run.
    """
    catalogue = await _catalogue_snapshot(db, snapshot_id, resolve_chat_sibling=True)
    if catalogue is not None:
        if (catalogue.extra_data or {}).get("chat_inventory_format") == 1:
            return await _catalogue_groups(db, catalogue)
        snapshot_id = str(catalogue.id)
    sibling_ids = await _resolve_sibling_snapshot_ids(
        db, snapshot_id, target_child_type=ResourceType.USER_CHATS,
    )
    if not sibling_ids:
        sibling_ids = [UUID(snapshot_id)]

    # The aggregate query:
    #   1. Pull pointer rows for these sibling snapshots, group by
    #      chat_id (parent_external_id), counting distinct external_ids.
    #   2. JOIN chat_threads on (tenant_id, chat_id) → chat_topic.
    #   3. LEFT JOIN to chat_thread_messages to pick the newest
    #      createdDateTime as the lastMessageAt label.
    rows = (await db.execute(
        text(
            # Per-chat count must match the message VIEW (the A∪B union in
            # list_snapshot_messages): a shared 1:1/group chat's count is the
            # COMPLETE conversation for EVERY member, not just the messages this
            # member's drain captured first under the shared cursor. Counting
            # only this snapshot's pointers (the old COUNT(DISTINCT
            # si.external_id)) made the panel-left show e.g. 2993 for one member
            # vs 3010 for the other on the SAME chat. Count distinct message ids
            # from the snapshot pointers (A) UNION the shared durable store (B),
            # B scoped to the chats this snapshot covers so nothing leaks.
            "WITH scope AS ( "
            "  SELECT DISTINCT si.tenant_id, si.parent_external_id AS chat_id "
            "    FROM snapshot_items si "
            "   WHERE si.snapshot_id = ANY(:sids) "
            "     AND si.item_type IN ('TEAMS_CHAT_MESSAGE','TEAMS_MESSAGE','TEAMS_MESSAGE_REPLY') "
            "     AND si.parent_external_id IS NOT NULL "
            "), "
            "msg_ids AS ( "
            "  SELECT si.tenant_id, si.parent_external_id AS chat_id, si.external_id AS ext "
            "    FROM snapshot_items si "
            "   WHERE si.snapshot_id = ANY(:sids) "
            "     AND si.item_type IN ('TEAMS_CHAT_MESSAGE','TEAMS_MESSAGE','TEAMS_MESSAGE_REPLY') "
            "     AND si.parent_external_id IS NOT NULL "
            "  UNION "
            "  SELECT ct.tenant_id, ct.chat_id, ctm.message_external_id AS ext "
            "    FROM chat_thread_messages ctm "
            "    JOIN chat_threads ct ON ct.id = ctm.chat_thread_id AND ct.archived_at IS NULL "
            "    JOIN scope sc ON sc.tenant_id = ct.tenant_id AND sc.chat_id = ct.chat_id "
            "   WHERE ctm.archived_at IS NULL "
            "), "
            "thread_msgs AS ( "
            "  SELECT tenant_id, chat_id, COUNT(DISTINCT ext) AS msg_count "
            "    FROM msg_ids GROUP BY tenant_id, chat_id "
            ") "
            "SELECT tm.chat_id, tm.msg_count, ct.chat_topic, ct.chat_type, "
            "       ct.member_names_json, "
            "       ( SELECT MAX(ctm.created_date_time) "
            "           FROM chat_thread_messages ctm "
            "          WHERE ctm.chat_thread_id = ct.id "
            "            AND ctm.archived_at IS NULL ) AS last_message_at "
            "  FROM thread_msgs tm "
            "  LEFT JOIN chat_threads ct "
            "         ON ct.tenant_id = tm.tenant_id "
            "        AND ct.chat_id = tm.chat_id "
            "        AND ct.archived_at IS NULL "
            " ORDER BY last_message_at DESC NULLS LAST"
        ),
        {"sids": [str(s) for s in sibling_ids]},
    )).all()

    return [
        {
            "chatId": r.chat_id,
            "displayName": _compose_chat_name(
                r.chat_id, r.chat_topic, r.chat_type, r.member_names_json,
            ),
            "count": int(r.msg_count or 0),
            "lastMessageAt": (
                r.last_message_at.isoformat() if r.last_message_at else None
            ),
        }
        for r in rows
    ]


@app.get("/api/v1/resources/snapshots/{snapshot_id}/azure-db/export")
async def azure_db_export(
    snapshot_id: str,
    items: Optional[str] = Query(None, description="Comma-separated SnapshotItem ids to export"),
    item_type: Optional[str] = Query(None, description="Fallback — export every item of this type in the snapshot"),
    db: AsyncSession = Depends(get_db),
):
    """Azure DB-aware download.

    Builds a ZIP (or single file when only one item is requested) tailored
    to each Azure DB item type:

      * AZURE_DB_CONFIG      → `<db>-config.json` — raw JSON blob.
      * AZURE_DB_TABLE       → `<db>/<schema>/<table>.csv` — rows JSON
        converted to CSV, headers from the payload's `columns` list.
      * AZURE_DB_SCHEMA_FILE → `<db>/<folder_path>/<name>` — raw blob
        bytes, original extension preserved in the name.

    Matches the Recovery UI's download semantics: picking one table
    yields a plain .csv, picking a schema/database or mixed set yields
    a .zip with the folder structure intact.
    """
    import csv as _csv
    import io as _io
    import json as _json
    import zipfile as _zip

    try:
        snap_uuid = UUID(snapshot_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid snapshot_id")

    item_ids: list = []
    if items:
        for tok in items.split(","):
            tok = tok.strip()
            if not tok:
                continue
            try:
                item_ids.append(UUID(tok))
            except Exception:
                raise HTTPException(status_code=400, detail=f"Invalid item id: {tok}")

    # Resolve the rows — either by explicit IDs, or fall back to "every
    # item of a given type in this snapshot" when the caller only
    # passed item_type. The fallback powers the Download button on the
    # Azure DB Configuration / Database / Schema tabs when nothing is
    # ticked (implicit "download all on this tab").
    if item_ids:
        rows_q = await db.execute(
            select(SnapshotItem).where(
                SnapshotItem.snapshot_id == snap_uuid,
                SnapshotItem.id.in_(item_ids),
            )
        )
    elif item_type:
        rows_q = await db.execute(
            select(SnapshotItem).where(
                SnapshotItem.snapshot_id == snap_uuid,
                SnapshotItem.item_type == item_type,
            )
        )
    else:
        raise HTTPException(status_code=400, detail="Provide `items` or `item_type`")

    snap_items = rows_q.scalars().all()
    if not snap_items:
        raise HTTPException(status_code=404, detail="No items found for export")

    shard, tenant_id, candidates = await _load_blob_context(db, snapshot_id)

    # Pull resource_type for audit scoping so the Activity feed's
    # service filter can separate Azure SQL vs PostgreSQL downloads.
    _snap = await db.get(Snapshot, UUID(snapshot_id))
    _resource = await db.get(Resource, _snap.resource_id) if _snap else None
    _resource_type = (
        _resource.type.value if _resource and hasattr(_resource.type, "value")
        else (str(_resource.type) if _resource else None)
    )
    await _emit_download_audit(
        action="AZURE_DB_DOWNLOAD",
        outcome="SUCCESS",
        tenant_id=tenant_id,
        resource_id=str(_resource.id) if _resource else None,
        resource_type=_resource_type,
        snapshot_id=snapshot_id,
        item_count=len(snap_items),
        details={"item_ids": [str(it.id) for it in snap_items][:50]},
    )

    async def _fetch_item_bytes(item: SnapshotItem) -> tuple[str, bytes]:
        """Return (zip-path, bytes) for a single SnapshotItem per its
        item_type's download format."""
        ed = item.extra_data or {}
        itype = (item.item_type or "").upper()

        if itype == "AZURE_DB_CONFIG":
            raw = ed.get("raw")
            if raw is not None:
                # Prefer the rich raw dict already stored on the row.
                data = _json.dumps(raw, indent=2, default=str).encode("utf-8")
            else:
                data = await _download_item_blob(shard, tenant_id, candidates, item.blob_path)
                if not data:
                    data = b"{}"
            return item.name or f"{(ed.get('server_name') or 'server')}-config.json", data

        if itype == "AZURE_DB_TABLE":
            rows_blob = ed.get("rows_blob_path") or item.blob_path
            payload = await _read_blob_json(shard, tenant_id, candidates, rows_blob) if rows_blob else {}
            columns = payload.get("columns") or ed.get("columns") or []
            data_rows = payload.get("rows") or []
            buf = _io.StringIO()
            writer = SafeCsvWriter(_csv.writer(buf))
            if columns:
                writer.writerow(columns)
            for r in data_rows:
                if isinstance(r, dict):
                    writer.writerow([r.get(c) for c in columns])
                else:
                    writer.writerow(r)
            schema = ed.get("schema") or "public"
            dbname = ed.get("database_name") or "db"
            zip_name = f"{dbname}/{schema}/{item.name}.csv"
            return zip_name, buf.getvalue().encode("utf-8")

        if itype == "AZURE_DB_SCHEMA_FILE":
            data = await _download_item_blob(shard, tenant_id, candidates, item.blob_path)
            if not data:
                data = b""
            folder = (item.folder_path or "").strip("/")
            zip_name = f"{folder}/{item.name}" if folder else (item.name or "schema")
            return zip_name, data

        # Unknown Azure DB type — fall back to raw bytes.
        data = await _download_item_blob(shard, tenant_id, candidates, item.blob_path) if item.blob_path else b""
        return item.name or str(item.id), data

    # Single-item fast path — skip the ZIP wrapper. This makes "download
    # one config" / "download one table" behave like the original file.
    if len(snap_items) == 1:
        name, data = await _fetch_item_bytes(snap_items[0])
        basename = name.rsplit("/", 1)[-1]
        # Content-type hinted from extension; Content-Disposition uses
        # the item's original name so the browser keeps the extension.
        if basename.endswith(".json"):
            media = "application/json"
        elif basename.endswith(".csv"):
            media = "text/csv"
        elif basename.endswith(".sql") or basename.endswith(".dump"):
            media = "text/plain"
        else:
            media = "application/octet-stream"
        return StreamingResponse(
            _io.BytesIO(data),
            media_type=media,
            headers={"Content-Disposition": f'attachment; filename="{basename}"'},
        )

    # Multi-item → ZIP with the folder structure intact.
    zbuf = _io.BytesIO()
    with _zip.ZipFile(zbuf, "w", _zip.ZIP_DEFLATED) as zf:
        for it in snap_items:
            try:
                path, data = await _fetch_item_bytes(it)
                zf.writestr(path, data)
            except Exception as fe:
                logger.warning("azure_db_export skipping %s: %s", it.id, fe)
    zbuf.seek(0)
    return StreamingResponse(
        zbuf,
        media_type="application/zip",
        headers={"Content-Disposition": 'attachment; filename="azure-db-export.zip"'},
    )


@app.get("/api/v1/resources/snapshots/{snapshot_id}/azure-db/table")
async def azure_db_table(
    snapshot_id: str,
    item_id: str = Query(..., description="SnapshotItem id of the AZURE_DB_TABLE row"),
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1, le=500),
    search_op: Optional[str] = Query(None, description="One of =, <, >, <=, >=, [] (between)"),
    search_val: Optional[str] = Query(None, description="Value to compare against the first column"),
    search_val2: Optional[str] = Query(None, description="Upper bound — only used when search_op is '[]'"),
    db: AsyncSession = Depends(get_db),
):
    """Return paginated rows of a single backed-up Azure DB table.

    The backup handler writes each table as a JSON blob alongside the
    .sql dump. This endpoint reads that blob, applies a search filter
    on the first column (if provided), then returns one page.

    Response shape:
      { columns: [...], rows: [[...], ...], total, page, size, hasMore }
    """
    try:
        snap_uuid = UUID(snapshot_id)
        it_uuid = UUID(item_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid snapshot_id / item_id")

    item = (await db.execute(select(SnapshotItem).where(SnapshotItem.id == it_uuid))).scalar_one_or_none()
    if not item or item.snapshot_id != snap_uuid:
        raise HTTPException(status_code=404, detail="Table row not found")
    if (item.item_type or "") != "AZURE_DB_TABLE":
        raise HTTPException(status_code=400, detail="Item is not an AZURE_DB_TABLE row")

    ed = item.extra_data or {}
    rows_blob = ed.get("rows_blob_path") or item.blob_path
    if not rows_blob:
        return {"columns": ed.get("columns", []), "rows": [], "total": 0, "page": page, "size": size, "hasMore": False}

    shard, tenant_id, candidates = await _load_blob_context(db, snapshot_id)
    payload = await _read_blob_json(shard, tenant_id, candidates, rows_blob)
    if not isinstance(payload, dict):
        payload = {}

    columns = payload.get("columns") or ed.get("columns") or []
    all_rows = payload.get("rows") or []

    # Filter on the first column using the chosen operator. "[]" is a
    # set match: the value is split on commas and whitespace, each
    # token is coerced, and the row matches if the first-column value
    # equals any token. Other operators are scalar comparisons.
    if search_op and columns and search_val is not None and search_val != "":
        col = columns[0]
        def _coerce(x):
            try:
                return float(x)
            except Exception:
                return str(x)

        target_set: list = []
        target = None
        if search_op == "[]":
            import re as _re
            for tok in _re.split(r"[\s,]+", search_val.strip()):
                if tok:
                    target_set.append(_coerce(tok))
        else:
            target = _coerce(search_val)

        def _match(val) -> bool:
            v = _coerce(val)
            try:
                if search_op == "=":  return v == target
                if search_op == ">":  return v > target
                if search_op == "<":  return v < target
                if search_op == ">=": return v >= target
                if search_op == "<=": return v <= target
                if search_op == "[]":
                    # Coerce the row value once; check containment.
                    return v in target_set or str(val) in {str(t) for t in target_set}
            except TypeError:
                return str(val) == str(search_val)
            return False

        all_rows = [r for r in all_rows if _match(r.get(col) if isinstance(r, dict) else None)]

    total = len(all_rows)
    offset = (page - 1) * size
    page_rows = all_rows[offset: offset + size]

    return {
        "columns": columns,
        "rows": page_rows,
        "total": total,
        "page": page,
        "size": size,
        "hasMore": offset + size < total,
        "firstColumn": columns[0] if columns else None,
    }


@app.get("/api/v1/resources/snapshots/{snapshot_id}/calendar")
async def list_snapshot_calendar(
    snapshot_id: str,
    page: int = Query(1, ge=1),
    # le=1000, not 500, because the Recovery Calendar view asks for exactly
    # 1000: tm_vault/src/pages/Recovery.tsx:1633
    # `SnapshotService.listCalendarEvents(snapshotId, 1, 1000)`. Against
    # le=500 FastAPI rejected that with 422 BEFORE the handler ran, on every
    # single request — so calendar recovery was not slow or flaky, it was
    # categorically impossible (QA-01..54 sweep 2026-08-11, QA-03, reported
    # as a 503; measured as a deterministic 422 in 0.0s).
    # Raising the ceiling costs nothing: this endpoint's cost is dominated by
    # the cross-snapshot union it must assemble to rebuild running state, NOT
    # by the page size. Measured live on snapshot 70acfd9f (18 sibling
    # snapshots, 209,325 union rows, 38,223 distinct events): size=10 and
    # size=500 both landed in the same 1-2s band after the query fix, and
    # before it size=10 was if anything SLOWER than size=500 under load.
    # The other four content endpoints keep le=500 — no client asks them for
    # more, and a ceiling that matches its caller is the honest contract.
    size: int = Query(500, ge=1, le=1000),
    search: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """Return calendar events with start/end/location/attendees extracted from metadata.

    Backups are incremental (delta), so each snapshot only carries
    *changes* (newly created / modified / cancelled occurrences). To
    mirror AFI's behavior — "show the full running state of the
    calendar as of this snapshot" — we UNION every CALENDAR_EVENT row
    across every snapshot of the same resource taken on or before the
    requested snapshot, then dedupe by external_id keeping the newest
    version. Without this, the calendar view flickers in and out of
    existence between full-pulls and delta-resumes (particularly for
    cancelled events that only appeared in the initial full snapshot).
    Auto-resolves to the USER_CALENDAR sibling when the passed
    snapshot id is for a non-calendar resource.
    """
    sibling_ids = await _resolve_sibling_snapshot_ids(
        db, snapshot_id, target_child_type=ResourceType.USER_CALENDAR,
    )
    if not sibling_ids:
        return {"content": [], "total": 0, "page": page, "size": size, "pages": 0}

    # Dedup + paginate in SQL (see _paged_latest_items) — "10K events
    # per calendar is fine in Python" stopped being true at jrue's
    # 25,855 delta-duplicated rows. Display order stays capture order
    # (created_at DESC), matching the pre-SQL behavior.
    #
    # exclude_deleted (TMV-009, 2026-08-11): the tab badge and this list must
    # agree and they did not — badge 5,416 vs sidebar 5,418 on the test
    # resource. `_CONTENT_ROLLUP_SQL`, which computes the badge, has always
    # carried `AND COALESCE(si.is_deleted, false) = false`; this list did
    # not, so the two Graph @removed tombstones under that calendar (both
    # literally named "(deleted event)", 232 B of metadata, no restorable
    # body) were listed as recoverable events and inflated the sidebar by
    # exactly 2. Measured against the anchor snapshot's 23 siblings:
    # dedup incl. deleted = 5,418, excl. deleted = 5,416, deleted rows = 2.
    # The badge is the correct number; /onedrive already excludes tombstones
    # for the same reason, so this makes calendar consistent with both.
    items, total = await _paged_latest_items(
        db, sibling_ids, ["CALENDAR_EVENT"],
        search=search, sort_sql="si.created_at",
        sort_dir="DESC", page=page, size=size,
        exclude_deleted=True,
    )

    shard, tenant_id, candidates = await _load_blob_context(db, snapshot_id)
    sem = asyncio.Semaphore(20)

    async def fmt(i):
        raw = _raw(i)
        # Read from blob concurrently if metadata is empty
        if (not raw or len(raw) <= 2) and i.blob_path:
            async with sem:
                raw = await _read_blob_json(shard, tenant_id, candidates, i.blob_path)
        if not isinstance(raw, dict):
            raw = {}

        organizer_obj = (raw.get("organizer") or {}).get("emailAddress") or {}
        start_obj = raw.get("start") or {}
        end_obj = raw.get("end") or {}
        # Prefer real titles: raw.subject, then raw.name, then the stored item
        # name (recurring occurrences inherit their series-master subject into
        # SnapshotItem.name during backup). Only fabricate a placeholder when
        # NONE of those exist — otherwise the inherited title was being thrown
        # away in favour of "Event · <time>".
        _item_name = i.name if i.name and i.name != "(no subject)" else ""
        subject = raw.get("subject") or raw.get("name") or _item_name
        graph_type = raw.get("type") or ""  # singleInstance, occurrence, seriesMaster, exception

        # Build display name from available fields
        if not subject:
            start_dt = start_obj.get("dateTime") or start_obj.get("date") or ""
            if start_dt:
                try:
                    from datetime import datetime as _dt
                    parsed = _dt.fromisoformat(start_dt.replace("Z", "+00:00").split(".")[0])
                    time_str = parsed.strftime("%-I:%M %p") if not start_obj.get("date") else parsed.strftime("%b %-d")
                    subject = f"Event · {time_str}"
                except Exception:
                    subject = "Calendar Event"
            else:
                subject = "Calendar Event"

        # Derive a human-readable eventType label for filter sidebar
        is_cancelled = raw.get("isCancelled", False) or subject.lower().startswith("canceled") or subject.lower().startswith("cancelled")
        is_all_day = raw.get("isAllDay", False) or (not start_obj.get("dateTime") and bool(start_obj.get("date")))
        is_online = raw.get("isOnlineMeeting", False)
        has_recurrence = bool(raw.get("recurrence")) or graph_type in ("seriesMaster", "occurrence")
        has_attendees = bool(raw.get("attendees"))
        if is_cancelled:
            event_type = "Cancelled"
        elif graph_type == "seriesMaster":
            event_type = "Recurring Series"
        elif graph_type == "occurrence":
            event_type = "Recurring"
        elif graph_type == "exception":
            event_type = "Exception"
        elif is_all_day:
            event_type = "All Day"
        elif is_online:
            event_type = "Online Meeting"
        elif has_recurrence:
            event_type = "Recurring"
        elif has_attendees:
            event_type = "Meeting"
        else:
            event_type = "Appointment"

        # Graph returns `start.dateTime` without a timezone suffix (e.g.
        # "2024-01-15T09:00:00.0000000") and the zone in the sibling
        # `timeZone` field. When timeZone is "UTC", we append Z so the
        # frontend's `new Date(s).toLocaleString()` converts to the
        # viewer's local timezone instead of treating the string as
        # local (which would hide the timezone shift).
        def _iso_with_tz(dt_str: Optional[str], tz_name: Optional[str]) -> Optional[str]:
            if not dt_str:
                return dt_str
            # Already has a tz marker (Z or ±HH:MM) — leave alone.
            if dt_str.endswith("Z") or "+" in dt_str[10:] or "-" in dt_str[10:]:
                return dt_str
            if (tz_name or "").upper() == "UTC":
                return dt_str + "Z"
            # Non-UTC named tz (e.g. "India Standard Time"): we don't have
            # a Windows→IANA map here, so pass through as local-style ISO
            # and let the frontend interpret. Better than fabricating the
            # wrong offset.
            return dt_str
        _tz = start_obj.get("timeZone") or "UTC"
        return {
            "id": str(i.id),
            "snapshotId": str(i.snapshot_id),
            "externalId": i.external_id,
            "itemType": i.item_type,
            "subject": subject or i.name or "",
            "start": _iso_with_tz(start_obj.get("dateTime") or start_obj.get("date"), _tz),
            "end": _iso_with_tz(end_obj.get("dateTime") or end_obj.get("date"), start_obj.get("timeZone") or _tz),
            "timeZone": _tz,
            "isAllDay": is_all_day,
            "isCancelled": is_cancelled,
            "location": (raw.get("location") or {}).get("displayName") or "",
            "organizer": organizer_obj.get("name") or organizer_obj.get("address") or None,
            "organizerEmail": organizer_obj.get("address") or "",
            "attendees": raw.get("attendees") or [],
            "body": (raw.get("body") or {}).get("content") or "",
            "bodyContentType": (raw.get("body") or {}).get("contentType") or "text",
            "isOnlineMeeting": is_online,
            "recurrence": raw.get("recurrence"),
            "recurrenceType": (raw.get("recurrence") or {}).get("pattern", {}).get("type") or None,
            "graphType": graph_type,
            "showAs": raw.get("showAs") or "",
            "importance": raw.get("importance") or "normal",
            "sensitivity": raw.get("sensitivity") or "normal",
            "categories": raw.get("categories") or [],
            "eventType": event_type,
            "folderPath": i.folder_path or "Calendar",
            "contentSize": i.content_size or 0,
            "isDeleted": i.is_deleted or False,
            "createdAt": i.created_at.isoformat() if i.created_at else "",
            "name": subject or i.name or "",
            "date": start_obj.get("dateTime") or start_obj.get("date") or "",
            "metadata": {"raw": raw},
        }

    results = await asyncio.gather(*[fmt(i) for i in items])
    return {"content": list(results), "totalElements": total, "totalPages": max(1, (total+size-1)//size), "size": size, "number": page}


# ── Per-content-type endpoints — Tier 2 backup browsers ─────────────────────
# These five fixed endpoints (mail/onedrive/contacts/calendar/chats) replace
# the previous dynamic /content-types lookup. The Recovery UI now hardcodes
# its tabs and queries these directly — no more "what's in this snapshot?"
# round-trip before render.

ONEDRIVE_ITEM_TYPES = ("FILE", "ONEDRIVE_FILE", "FILE_VERSION")
CONTACT_ITEM_TYPES = ("USER_CONTACT", "CONTACT")


@app.get("/api/v1/resources/snapshots/{snapshot_id}/onedrive")
async def list_snapshot_onedrive(
    snapshot_id: str,
    page: int = Query(1, ge=1),
    size: int = Query(500, ge=1, le=500),
    folder: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    sort: Optional[str] = Query(None, description="name_asc|created_desc|modified_desc"),
    createdSince: Optional[str] = Query(
        None,
        description=(
            "Keep only files whose Graph createdDateTime is at or after this "
            "point. ISO-8601 (2026-08-01T00:00:00Z) or a relative window "
            "(30d / 12h). Bounds the Recovery 'Recent' view."
        ),
    ),
    modifiedSince: Optional[str] = Query(
        None,
        description="Same as createdSince but against lastModifiedDateTime.",
    ),
    db: AsyncSession = Depends(get_db),
):
    """Return OneDrive files in a snapshot.

    `sort`:
      - `created_desc`  → newest file first (Graph createdDateTime)
      - `modified_desc` → most-recently-edited first (Graph lastModifiedDateTime)
      - default / anything else → name A→Z (case-insensitive)

    `createdSince` / `modifiedSince` bound the result to a recency window.

    TMV-016 (QA sweep 2026-08-11): the Recovery "Recent" view was reported
    as "lists the entire drive (8,295 items), unsorted". The ORDERING half
    was already here and works — measured live against snapshot a9ce8054,
    `?sort=created_desc` returns "Screenshot 2026-08-11 090118.png"
    (createdDateTime 2026-08-11T16:01:24Z) first in 0.21 s, versus
    "+Connect to New Data Source.odc" with no sort — and Recovery.tsx:6368
    already sends `sort=created_desc` for that view. What the backend had
    NO way to express was the WINDOW: `totalElements` stayed 8,295, so
    "Recent" was the whole drive in a different order and scrolling walked
    every file. These two params are that missing half; the count and the
    page are filtered together, so `totalElements` now reports the window.

    Frontend wiring for the Recent view (the other half of TMV-016):
        GET …/onedrive?sort=created_desc&createdSince=30d&size=50
    Any window works — `30d`, `12h`, or an absolute ISO-8601 instant.
    Comparison is lexicographic over the Graph Zulu strings, which is the
    same key the sort already uses, so a file that sorts into the window
    always passes the filter.

    Sibling-snapshot aggregation: OneDrive uses delta backups, so an
    initial full-pull captures every file and every subsequent run
    only re-emits changed files. Without sibling aggregation, opening
    Recovery on the latest delta snapshot (item_count=0) returned an
    empty file list even though the resource has thousands of files
    via the initial snapshot — symptom in the 2026-05-15 case was
    Amit's My Drive root showing folder rows (from /folders, which
    already aggregates) but zero files. Mirror the pattern used by
    /mail, /chats, /calendar and /contacts.
    """
    sibling_ids = await _resolve_sibling_snapshot_ids(
        db, snapshot_id, target_child_type=ResourceType.USER_ONEDRIVE,
    )
    if not sibling_ids:
        sibling_ids = [UUID(snapshot_id)]
    # Dedup + sort + paginate in SQL (see _paged_latest_items) — the
    # JSON-path sorts compose fine as ->> expressions on the outer
    # query. Deletion tombstones (Graph @removed/deleted-facet rows
    # that the pre-2026-07-13 walk persisted, backfilled to
    # is_deleted=true) are not restorable files — hidden via
    # exclude_deleted.
    if sort == "created_desc":
        sort_sql, sort_dir = "COALESCE(si.metadata->'raw'->>'createdDateTime', '')", "DESC"
    elif sort == "modified_desc":
        sort_sql, sort_dir = "COALESCE(si.metadata->'raw'->>'lastModifiedDateTime', '')", "DESC"
    else:
        sort_sql, sort_dir = "LOWER(COALESCE(si.name, ''))", "ASC"

    recency_clauses = []
    recency_params: Dict[str, Any] = {}
    for _param, _field, _bind in (
        (createdSince, "createdDateTime", "rec_created"),
        (modifiedSince, "lastModifiedDateTime", "rec_modified"),
    ):
        cutoff = _parse_since(_param)
        if cutoff is None:
            continue
        # MISSING TIMESTAMP MUST NOT MEAN "EXCLUDE". ONEDRIVE_ITEM_TYPES spans
        # FILE / ONEDRIVE_FILE / FILE_VERSION, and not every producer writes a
        # `metadata.raw` envelope: all 124,428 FILE rows on prod (SHAREPOINT_SITE
        # resources) store `metadata.structured` with NO `raw` key, so both
        # timestamps read NULL there. With a bare COALESCE(..,'') those rows
        # compare '' >= cutoff = false and 100% of them vanish — and because
        # this predicate is applied INSIDE the dedup subquery, totalElements
        # would report the reduced number as the truth. That is a file browser
        # silently hiding a backup's contents, which is the one thing this
        # product must never do. A row we cannot date is kept and left for the
        # sort to order; "recent" is a convenience filter, not an authority on
        # what exists. Pinned by tests/services/test_onedrive_recency_window.py.
        recency_clauses.append(
            f"(si.metadata->'raw'->>'{_field}' IS NULL"
            f" OR si.metadata->'raw'->>'{_field}' = ''"
            f" OR si.metadata->'raw'->>'{_field}' >= :{_bind})"
        )
        recency_params[_bind] = cutoff

    items, total = await _paged_latest_items(
        db, sibling_ids, list(ONEDRIVE_ITEM_TYPES),
        folder=folder, search=search, exclude_deleted=True,
        sort_sql=sort_sql, sort_dir=sort_dir,
        extra_where=(" AND ".join(recency_clauses) or None),
        extra_params=(recency_params or None),
        page=page, size=size,
    )
    return {
        "content": [_item_to_response(i) for i in items],
        "totalElements": total,
        "totalPages": max(1, (total + size - 1) // size),
        "size": size,
        "number": page,
    }


@app.get("/api/v1/resources/snapshots/{snapshot_id}/onedrive/ids")
async def list_onedrive_ids_by_prefix(
    snapshot_id: str,
    folder_prefix: str = Query(..., description="Folder path prefix; '/' means all files"),
    db: AsyncSession = Depends(get_db),
):
    """Return the ids of every ONEDRIVE_FILE in the snapshot whose
    folder_path starts with `folder_prefix`. Used by the Recovery UI's
    folder-row checkbox to bulk-select every file under a folder — works
    recursively (selecting `/Documents` also picks up files in
    `/Documents/Sub/` etc.).

    Kept as a separate lightweight endpoint so the bulk-select doesn't
    have to page through the main /onedrive response and re-assemble it
    on the client.

    Sibling-snapshot aggregation: matches /onedrive so a bulk-select
    against the latest empty delta snapshot doesn't return 0 ids —
    falls through to the prior snapshots that actually hold the files.
    Dedup is by external_id, newest-wins (returns the ID from the
    latest snapshot that still holds that file)."""
    sibling_ids = await _resolve_sibling_snapshot_ids(
        db, snapshot_id, target_child_type=ResourceType.USER_ONEDRIVE,
    )
    if not sibling_ids:
        sibling_ids = [UUID(snapshot_id)]
    prefix = folder_prefix
    if prefix == "/" or prefix == "":
        # Root select → every file in the drive.
        filters = [
            SnapshotItem.snapshot_id.in_(sibling_ids),
            SnapshotItem.item_type.in_(ONEDRIVE_ITEM_TYPES),
        ]
    else:
        # Match the folder itself AND any nested child (LIKE 'prefix%').
        # Escape SQL LIKE wildcards in the prefix so a folder named with
        # a literal `_` doesn't accidentally match sibling paths.
        escaped = prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        filters = [
            SnapshotItem.snapshot_id.in_(sibling_ids),
            SnapshotItem.item_type.in_(ONEDRIVE_ITEM_TYPES),
            (SnapshotItem.folder_path == prefix) | (SnapshotItem.folder_path.like(f"{escaped}/%", escape="\\")),
        ]
    rows = (await db.execute(
        select(SnapshotItem.id, SnapshotItem.external_id, SnapshotItem.created_at)
        .where(*filters)
        .order_by(SnapshotItem.external_id, SnapshotItem.created_at.desc())
    )).all()
    # Newest-wins dedup by external_id. Rows without external_id are
    # passed through (e.g. legacy ones); they can't collide with each
    # other meaningfully since `id` is unique anyway.
    seen: set = set()
    deduped_ids: list = []
    for row_id, ext_id, _created in rows:
        key = ext_id if ext_id else str(row_id)
        if ext_id and key in seen:
            continue
        if ext_id:
            seen.add(key)
        deduped_ids.append(str(row_id))
    return {"ids": deduped_ids, "count": len(deduped_ids)}


@app.get("/api/v1/resources/snapshots/{snapshot_id}/files")
async def list_snapshot_files(
    snapshot_id: str,
    page: int = Query(1, ge=1),
    size: int = Query(200, ge=1),
    search: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """Return EVERY item in a snapshot as a uniform 'file' row.

    Used by the Recovery page for resource kinds that don't fit the five
    fixed tabs (Power BI workspaces, SharePoint sites, Azure resources,
    etc.) — one flat list of items with name / size / captured-at /
    blob-path so the frontend can render a simple files table.

    No item_type filter — whatever is in the snapshot shows up. Mail /
    OneDrive / Contacts / Calendar / Chats have their own dedicated
    endpoints that filter by type; this one is intentionally broad."""
    filters = [SnapshotItem.snapshot_id == UUID(snapshot_id)]
    if search:
        filters.append(SnapshotItem.name.ilike(f"%{search}%"))
    total = (await db.execute(select(func.count(SnapshotItem.id)).where(*filters))).scalar() or 0
    items = (await db.execute(
        select(SnapshotItem).where(*filters)
        # Names are far from unique in a snapshot (every "Untitled.docx",
        # every same-named file in a different folder), so LOWER(name) alone
        # is not a total order and paging re-slices the ties.
        .order_by(func.lower(SnapshotItem.name).asc(), SnapshotItem.id.asc())
        .offset((page - 1) * size).limit(size)
    )).scalars().all()
    return {
        "content": [_item_to_response(i) for i in items],
        "totalElements": total,
        "totalPages": max(1, (total + size - 1) // size),
        "size": size,
        "number": page,
    }


@app.get("/api/v1/resources/snapshots/{snapshot_id}/contacts")
async def list_snapshot_contacts(
    snapshot_id: str,
    page: int = Query(1, ge=1),
    size: int = Query(500, ge=1, le=500),
    folder: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """Return contacts in a snapshot. Auto-resolves to the USER_CONTACTS
    sibling when the caller passed a non-contacts snapshot id, so the
    Recovery UI doesn't silently show 0 when a real contacts snapshot
    exists under the same user."""
    sibling_ids = await _resolve_sibling_snapshot_ids(
        db, snapshot_id, target_child_type=ResourceType.USER_CONTACTS,
    )
    if not sibling_ids:
        sibling_ids = [UUID(snapshot_id)]
    # Dedup + paginate in SQL (see _paged_latest_items). The old path
    # was already bounded (LIMIT/OFFSET) but paged duplicate sibling
    # rows with no dedup and no ORDER BY — the same contact appeared
    # once per delta snapshot, in nondeterministic order. Name A→Z now.
    items, total = await _paged_latest_items(
        db, sibling_ids, list(CONTACT_ITEM_TYPES),
        folder=folder, search=search,
        sort_sql="LOWER(COALESCE(si.name, ''))", sort_dir="ASC",
        page=page, size=size,
    )

    def fmt(i):
        raw = _raw(i)
        emails = raw.get("emailAddresses") or []
        phones = raw.get("businessPhones") or []
        return {
            "id": str(i.id),
            "snapshotId": str(i.snapshot_id),
            "externalId": i.external_id,
            "itemType": i.item_type,
            "displayName": raw.get("displayName") or i.name,
            "givenName": raw.get("givenName") or "",
            "surname": raw.get("surname") or "",
            "companyName": raw.get("companyName") or "",
            "jobTitle": raw.get("jobTitle") or "",
            "emails": [e.get("address") for e in emails if isinstance(e, dict)],
            "primaryEmail": (emails[0].get("address") if emails and isinstance(emails[0], dict) else None),
            "phones": phones,
            "folderPath": i.folder_path or "Contacts",
            "contentSize": i.content_size or 0,
            "isDeleted": i.is_deleted or False,
            "createdAt": i.created_at.isoformat() if i.created_at else "",
            "name": raw.get("displayName") or i.name or "",
            "metadata": {"raw": raw},
        }

    return {
        "content": [fmt(i) for i in items],
        "totalElements": total,
        "totalPages": max(1, (total+size-1)//size),
        "size": size,
        "number": page,
    }


@app.get("/api/v1/resources/snapshots/{snapshot_id}/items/{item_id}/content")
async def get_item_content(
    snapshot_id: str,
    item_id: str,
    download: bool = Query(False, description="If true, stream as a file attachment with Content-Disposition"),
    db: AsyncSession = Depends(get_db),
):
    """Return the raw content of a snapshot item, reading from blob when present.

    Behavior:
      - Blob-backed items (e.g. EMAIL_ATTACHMENT) stream the bytes. When the
        payload parses as JSON we wrap it in `{"source":"blob","content":…}`
        for frontend convenience; otherwise we stream octets with a proper
        Content-Type (pulled from extra_data) and — if ?download=1 — a
        Content-Disposition: attachment header carrying the original
        filename so the browser saves it with the right name/extension.
      - Inline items (extra_data.raw) return JSON as before.
      - Missing items return an empty shape so the caller doesn't 404."""
    from fastapi.responses import Response

    item = await db.get(SnapshotItem, UUID(item_id))
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")

    if item.snapshot_id != UUID(snapshot_id):
        raise HTTPException(status_code=404, detail="Item not found")
    catalogue = await _catalogue_snapshot(db, snapshot_id)
    if catalogue is not None:
        item = (await materialize_chat_items(db, [item]))[0]
        if item.blob_path:
            return await _catalogue_blob_response(db, item, download=download)
        if item.item_type in {"CHAT_ATTACHMENT", "CHAT_HOSTED_CONTENT"} and not item.extra_data.get("inline_b64"):
            return {"source": "metadata", "content": item.extra_data}

    # Inline path: items with `extra_data.inline_b64` set carry their
    # bytes directly in the JSON column instead of in Azure Blob. This
    # covers EMAIL_ATTACHMENT / CHAT_ATTACHMENT / CHAT_HOSTED_CONTENT
    # rows ≤256 KB. Check this before the blob path because inline rows
    # have blob_path=None by construction.
    _ed_for_inline = item.extra_data or {}
    _inline_b64 = _ed_for_inline.get("inline_b64")
    data: Optional[bytes] = None
    source: Optional[str] = None
    if _inline_b64:
        import base64 as _b64
        try:
            data = _b64.b64decode(_inline_b64)
            source = "inline"
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Inline decode failed: {e}")

    if data is None and item.blob_path and azure_storage_manager.shards:
        shard, tenant_id, candidates = await _load_blob_context(db, snapshot_id)
        try:
            data = await _download_item_blob(shard, tenant_id, candidates, item.blob_path)
            source = "blob"
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Blob read failed: {e}")
    if data is not None:
            import json as _json
            meta = item.extra_data or {}
            # Only try JSON parsing when the caller didn't ask for a raw
            # download. EMAIL_ATTACHMENTS of type application/json round-trip
            # through the browser fine as raw bytes too.
            if not download:
                try:
                    return {"source": source or "blob", "content": _json.loads(data.decode("utf-8"))}
                except Exception:
                    pass

            # Content-Type priority:
            #   1. `extra_data.content_type` — Graph's declared MIME (most
            #      accurate for fileAttachments: "application/pdf" etc.).
            #      But skip it if it's a non-MIME string like "reference"
            #      that Graph hands back for chat attachments.
            #   2. Infer from the filename extension via mimetypes —
            #      covers chat referenceAttachments (we have the filename
            #      but Graph gave us "reference", not the actual MIME).
            #   3. Type-based default (EMAIL/CALENDAR_EVENT/TEAMS_CHAT_MESSAGE
            #      are JSON) or application/octet-stream.
            import mimetypes as _mt
            declared_ct = (meta.get("content_type") or "").strip()
            looks_like_mime = "/" in declared_ct  # filters out bare words like "reference"
            guessed_ct, _ = _mt.guess_type(item.name or "")
            content_type = (
                declared_ct if looks_like_mime else
                guessed_ct or (
                    "application/json" if item.item_type in ("EMAIL", "CALENDAR_EVENT", "TEAMS_CHAT_MESSAGE")
                    else "application/octet-stream"
                )
            )
            # Ops/Data Check 17: the content bytes AND their declared MIME are
            # attacker-authored (a sender names the attachment and its type).
            # Never let the browser render them inline same-origin as HTML/SVG.
            # Only a safe raster image type may render inline; everything else
            # is forced to an octet-stream attachment. nosniff on every response.
            _INLINE_SAFE = {"image/png", "image/jpeg", "image/gif", "image/webp", "image/bmp"}
            headers = {"X-Content-Type-Options": "nosniff"}
            _ct_base = content_type.split(";", 1)[0].strip().lower()
            if _ct_base not in _INLINE_SAFE:
                content_type = "application/octet-stream"
                if not download:
                    # force a download rather than inline-render an untrusted type
                    headers["Content-Disposition"] = "attachment"
            if download:
                # RFC 5987 filename* for non-ASCII safety; plain filename
                # kept as a fallback for older browsers. The filename
                # retains the extension from item.name (we set it at
                # backup time from Graph's `name` field, which already
                # includes the extension).
                import urllib.parse as _urlp
                fname = (item.name or f"item-{item.id}").strip()
                safe = _urlp.quote(fname)
                # HTTP headers are latin-1 only. Graph-sourced filenames often
                # carry chars like U+202F (narrow no-break space) that break
                # Response() header encoding. Strip to ASCII for the plain
                # filename fallback; filename* keeps the UTF-8 original for
                # RFC 5987-aware clients.
                ascii_fname = fname.encode("ascii", "replace").decode("ascii").replace('"', "'")
                headers["Content-Disposition"] = f"attachment; filename=\"{ascii_fname}\"; filename*=UTF-8''{safe}"

                # Audit: FILE_DOWNLOADED — one row per single-file download
                # (distinct from bulk EXPORT_DOWNLOADED which bundles many
                # items into a zip). Details carry enough to reconstruct
                # what was pulled: item name, size, content-type, blob path,
                # source folder. Fire-and-forget so a slow audit-service
                # doesn't delay the actual download bytes.
                try:
                    import httpx as _httpx2
                    # tenant_id lives on Resource, not Snapshot — snapshots
                    # only link to their Resource. Look both up so the
                    # audit row has proper scoping (tenant_id is what
                    # /audit/events filters on).
                    snap = await db.get(Snapshot, UUID(snapshot_id))
                    resource = await db.get(Resource, snap.resource_id) if snap and snap.resource_id else None
                    payload = {
                        "action": "FILE_DOWNLOADED",
                        "tenant_id": str(resource.tenant_id) if resource and resource.tenant_id else None,
                        "actor_type": "USER",
                        "resource_id": str(snap.resource_id) if snap and snap.resource_id else None,
                        "resource_type": item.item_type,
                        "resource_name": item.name,
                        "outcome": "SUCCESS",
                        "snapshot_id": snapshot_id,
                        "details": {
                            "itemId": str(item.id),
                            "externalId": item.external_id,
                            "itemType": item.item_type,
                            "filename": fname,
                            "folderPath": item.folder_path,
                            "contentType": content_type,
                            "byteSize": len(data),
                            "blobPath": item.blob_path,
                            "parentResource": resource.display_name if resource else None,
                        },
                    }
                    async with _httpx2.AsyncClient(timeout=3.0) as _c:
                        r = await _c.post(f"{settings.AUDIT_SERVICE_URL}/api/v1/audit/log", json=payload, headers=internal_audit_headers())
                        if r.status_code >= 300:
                            print(f"[FILE_DOWNLOADED audit] HTTP {r.status_code}: {r.text[:200]}", flush=True)
                except Exception as _e:
                    print(f"[FILE_DOWNLOADED audit] failed: {type(_e).__name__}: {_e}", flush=True)
            return Response(content=data, media_type=content_type, headers=headers)

    # Fall back to inline metadata (e.g. email bodies stored directly in extra_data)
    meta = item.extra_data or {}
    raw = meta.get("raw") or meta.get("structured")
    if raw:
        return {"source": "metadata", "content": raw}

    return {"source": "none", "content": {}}


def _safe_uuid(value):
    """Parse a path/query param into a UUID, or return None if it isn't one.

    The frontend sometimes serializes a null id into the URL as the literal
    string 'None'/'null'/'undefined' — e.g. a chat message reconstructed from
    the shared durable store (the f0481cb union) carries no snapshot_item id,
    so its attachment fetch hits .../snapshots/None/items/None/attachments.
    UUID('None') raises ValueError, which surfaced as an HTTP 500. Callers use
    the None return to degrade gracefully (empty result / 404).
    """
    if not value or value in ("None", "null", "undefined"):
        return None
    try:
        return UUID(value)
    except (ValueError, AttributeError, TypeError):
        return None


def _attachment_resolved(item_type: str, meta: dict, blob_path) -> bool:
    """Whether the UI should link the download chip to our /content endpoint
    (bytes are local) vs. fall back to an external sourceUrl.

    ARCHIVE_ATTACHMENT rows are always our own captured fileAttachment blobs —
    enrichment only creates the sibling row after it decoded + uploaded the
    bytes (plan_archive_attachments skips item/referenceAttachments), so they
    carry NO `resolved` meta flag; blob (or inline_b64) presence alone resolves
    them. EMAIL/CHAT rows keep the explicit flag because a referenceAttachment
    gets a metadata-only row with no local bytes."""
    has_bytes = bool(blob_path) or bool((meta or {}).get("inline_b64"))
    if item_type == "ARCHIVE_ATTACHMENT":
        return has_bytes
    return bool((meta or {}).get("resolved")) and has_bytes


@app.get("/api/v1/resources/snapshots/{snapshot_id}/items/{item_id}/attachments")
async def get_item_attachments(
    snapshot_id: str,
    item_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Return the EMAIL_ATTACHMENT rows parented to a given email item.

    The Tier 2 USER_MAIL backup stores each attachment as its own
    SnapshotItem with `extra_data.parent_item_id` = the email's Graph id.
    This endpoint lets the frontend's EmailPreview render downloadable
    attachment chips for the selected email without scanning every item
    in the snapshot.

    Returns minimal fields needed to build a download link:
      { id, name, size, kind, contentType, isInline, resolved }
    `resolved` is true when the bytes are actually in blob storage (so the
    chip links to the content endpoint); false for referenceAttachments
    whose sharing URL couldn't be resolved (metadata-only row)."""
    # Frontend can serialize a null id into the path as the literal string
    # "None" (e.g. a chat message reconstructed from the durable store has no
    # snapshot_item id). UUID("None") used to ValueError -> HTTP 500; degrade
    # to "no attachments" so the message view never errors on such a row.
    _sid = _safe_uuid(snapshot_id)
    _iid = _safe_uuid(item_id)
    if _sid is None or _iid is None:
        return []
    parent = await db.get(SnapshotItem, _iid)
    if not parent:
        raise HTTPException(status_code=404, detail="Item not found")
    # Defense in depth — the URL gives us both snapshot_id and item_id.
    # The auth layer enforces tenant ownership of the snapshot. Here we
    # also require the item belong to that snapshot so callers can't
    # enumerate other snapshots' items by guessing a stray item_id.
    if parent.snapshot_id != _sid:
        raise HTTPException(status_code=404, detail="Item not found")

    catalogue = await _catalogue_snapshot(db, snapshot_id)
    if catalogue is not None:
        if parent.chat_version_id is None:
            raise ChatCatalogueIntegrityError("new-format message has no captured version")
        children = list((await db.scalars(select(SnapshotItem).where(
            SnapshotItem.snapshot_id == _sid,
            SnapshotItem.parent_external_id == parent.external_id,
            SnapshotItem.item_type == "CHAT_ATTACHMENT").order_by(SnapshotItem.name, SnapshotItem.id))).all())
        items = await materialize_chat_items(db, children)
        return [{"id": str(item.id), "name": item.name, "size": item.content_size or 0,
            "kind": (item.extra_data or {}).get("attachment_kind"),
            "contentType": (item.extra_data or {}).get("content_type"),
            "isInline": bool((item.extra_data or {}).get("is_inline")),
            "contentId": (item.extra_data or {}).get("content_id"),
            "resolved": bool((item.extra_data or {}).get("resolved")),
            "sourceUrl": (item.extra_data or {}).get("content_url")} for item in items]

    # Email and chat message external_id IS the Graph id — both attachment
    # kinds store that as parent_item_id. One endpoint serves both so the
    # UI can look up attachments without caring which content tab it's on.
    parent_msg_id = parent.external_id
    # Snapshot scoping differs by item type:
    #   EMAIL_ATTACHMENT — private to the user's mailbox snapshot. Scope
    #     strictly by snapshot_id so a user can never enumerate another
    #     mailbox's attachments by guessing a message id.
    #   CHAT_ATTACHMENT  — since the 2026-05-13 Level 2 refactor, chat
    #     bodies + attachments live tenant-wide (chat_thread_messages +
    #     CHAT_ATTACHMENT rows persisted in the FIRST user's snapshot
    #     only — claim-skipped users don't re-persist the attachment row
    #     even though they DO get the pointer row for the message). If we
    #     filtered by snapshot_id here, claim-skipped users would never
    #     find the attachment row → chip stuck on "(capturing…)" and the
    #     download 404s. Scope by tenant_id instead — safe because we
    #     already proved this caller can see the parent message in their
    #     own snapshot above (parent.snapshot_id == :snapshot_id), and
    #     tenant_id on the parent gates cross-tenant lookup.
    #   ARCHIVE_ATTACHMENT — sibling of an enriched ARCHIVE_ITEM, captured into
    #     the user's own archive snapshot. Private like EMAIL_ATTACHMENT, so
    #     scope strictly by snapshot_id (the parent archive item lives in the
    #     same snapshot).
    filters = [
        SnapshotItem.item_type.in_(["EMAIL_ATTACHMENT", "CHAT_ATTACHMENT", "ARCHIVE_ATTACHMENT"]),
        func.json_extract_path_text(SnapshotItem.extra_data, "parent_item_id") == parent_msg_id,
        or_(
            and_(
                SnapshotItem.item_type.in_(["EMAIL_ATTACHMENT", "ARCHIVE_ATTACHMENT"]),
                SnapshotItem.snapshot_id == _sid,
            ),
            and_(
                SnapshotItem.item_type == "CHAT_ATTACHMENT",
                SnapshotItem.tenant_id == parent.tenant_id,
            ),
        ),
    ]
    rows = (await db.execute(
        select(SnapshotItem).where(*filters).order_by(SnapshotItem.name)
    )).scalars().all()
    # Tenant-wide CHAT_ATTACHMENT lookup can return the same attachment
    # row N times (one per snapshot that holds it). Dedup by external_id,
    # keeping the first row encountered.
    _seen_ext: set = set()
    _deduped = []
    for r in rows:
        if r.item_type == "CHAT_ATTACHMENT":
            if r.external_id in _seen_ext:
                continue
            _seen_ext.add(r.external_id)
        _deduped.append(r)
    rows = _deduped

    def fmt(r):
        meta = r.extra_data or {}
        # Email attachments store the original URL as `source_url`;
        # chat attachments as `content_url`. Surface one normalized field
        # so the UI doesn't need to know the difference.
        # contentId is the MIME Content-ID the email body references
        # via <img src="cid:..."> for inline images (logos, embedded
        # signatures). We expose it so EmailPreview can rewrite those
        # cid: URLs into real content-endpoint URLs before rendering —
        # without it, all inline images show broken.
        return {
            "id": str(r.id),
            "name": r.name,
            "size": r.content_size or 0,
            "kind": meta.get("attachment_kind"),
            "contentType": meta.get("content_type"),
            "isInline": bool(meta.get("is_inline")),
            "contentId": meta.get("content_id"),
            # "resolved" tells the UI we have the bytes locally and the
            # chip should download from our /content endpoint instead of
            # opening the SharePoint sourceUrl in a new tab.
            # Bytes live in EITHER r.blob_path (SeaweedFS) OR metadata
            # inline_b64 (small payloads inlined into the row). Without
            # the inline_b64 branch, every attachment under the 256 KB
            # inline threshold renders as an external SharePoint link
            # that 401s for any user other than the original uploader.
            "resolved": _attachment_resolved(r.item_type, meta, r.blob_path),
            "sourceUrl": meta.get("source_url") or meta.get("content_url"),
        }

    return [fmt(r) for r in rows]


# ──────────────────────────────────────────────────────────────────────
# Azure VM live detail — enrich a SnapshotItem with its current ARM
# state so the Recovery detail panels render the same fields the
# Azure Portal shows (IOPS, disk tier, security type, etc.) instead
# of just the shallow subset captured at backup time.
# ──────────────────────────────────────────────────────────────────────

import httpx as _httpx
import time as _t


@app.post("/api/v1/snapshot-items/{item_id}/download-audit")
async def azure_item_download_audit(
    item_id: str,
    payload: dict = Body(...),
    db: AsyncSession = Depends(get_db),
):
    """Client-called audit emitter for downloads that assemble their
    bytes entirely in the browser (VM config JSON / Disk / NIC /
    Public IP JSON). The frontend POSTs here after saving the file
    so the Audit + Activity feeds still carry a DOWNLOAD row even
    for those bypass-the-server flows.

    Body: { action?, kind?, notes? }"""
    item = await db.get(SnapshotItem, UUID(item_id))
    if not item:
        raise HTTPException(status_code=404, detail="Snapshot item not found")
    snapshot = await db.get(Snapshot, item.snapshot_id)
    resource = await db.get(Resource, snapshot.resource_id) if snapshot else None
    rtype = (
        resource.type.value if resource and hasattr(resource.type, "value")
        else (str(resource.type) if resource else None)
    )
    await _emit_download_audit(
        action=str(payload.get("action") or "AZURE_VM_DOWNLOAD"),
        outcome="SUCCESS",
        tenant_id=str(resource.tenant_id) if resource else None,
        resource_id=str(resource.id) if resource else None,
        resource_type=rtype,
        snapshot_id=str(snapshot.id) if snapshot else None,
        item_count=1,
        details={
            "kind": payload.get("kind") or item.item_type,
            "item_id": item_id,
            **({"notes": payload.get("notes")} if payload.get("notes") else {}),
        },
    )
    return {"ok": True}


async def _emit_download_audit(
    *, action: str, outcome: str,
    tenant_id: Optional[str], resource_id: Optional[str], resource_type: Optional[str],
    snapshot_id: Optional[str] = None, item_count: int = 1,
    details: Optional[dict] = None,
) -> None:
    """Best-effort audit emission for user-triggered downloads.
    Never raises — audit-service downtime shouldn't block the download.
    Reused by every Azure-flavored download endpoint (Azure DB export,
    VM volume file / ZIP, VM config JSON) so the Activity + Audit feeds
    show a consistent row per download."""
    try:
        payload = {
            "action": action,
            "tenant_id": tenant_id,
            "actor_type": "USER",
            "resource_id": resource_id,
            "resource_type": resource_type,
            "outcome": outcome,
            "details": {
                **(details or {}),
                **({"snapshot_id": snapshot_id} if snapshot_id else {}),
                "item_count": item_count,
            },
        }
        async with _httpx.AsyncClient(timeout=5.0) as c:
            await c.post(f"{settings.AUDIT_SERVICE_URL}/api/v1/audit/log", json=payload, headers=internal_audit_headers())
    except Exception:
        pass

_VM_DETAIL_CACHE: dict[str, tuple[dict, float]] = {}
_VM_DETAIL_TTL = 60

async def _arm_token_for_tenant(tenant_external_id: str) -> Optional[str]:
    cid = settings.EFFECTIVE_ARM_CLIENT_ID or settings.MICROSOFT_CLIENT_ID
    csec = settings.EFFECTIVE_ARM_CLIENT_SECRET or settings.MICROSOFT_CLIENT_SECRET
    if not (cid and csec and tenant_external_id):
        return None
    try:
        async with _httpx.AsyncClient(timeout=10) as c:
            r = await c.post(
                f"https://login.microsoftonline.com/{tenant_external_id}/oauth2/v2.0/token",
                data={"client_id": cid, "client_secret": csec,
                      "scope": "https://management.azure.com/.default",
                      "grant_type": "client_credentials"},
            )
            if r.status_code != 200:
                return None
            return r.json().get("access_token")
    except Exception:
        return None


async def _arm_get_json(token: str, url: str) -> Optional[dict]:
    try:
        async with _httpx.AsyncClient(timeout=15) as c:
            r = await c.get(url, headers={"Authorization": f"Bearer {token}"})
            if r.status_code != 200:
                return None
            return r.json()
    except Exception:
        return None


@app.get("/api/v1/snapshot-items/{item_id}/azure-vm-detail")
async def azure_vm_item_live_detail(item_id: str, db: AsyncSession = Depends(get_db)):
    """Return the current Azure ARM state for a VM-flavored SnapshotItem.
    item_type dictates which ARM resource we look up:
      AZURE_VM_CONFIG  → /virtualMachines/{vm}?$expand=instanceView
      AZURE_VM_DISK    → /disks/{disk}
      AZURE_VM_NIC     → /networkInterfaces/{nic}
      AZURE_VM_PUBLIC_IP → /publicIPAddresses/{pip}
      AZURE_VM_VOLUME  → mirrored from the parent disk (volume isn't a
                         real Azure resource).
    The response is the raw ARM payload — the UI renders the fields
    it knows about and ignores the rest."""
    from shared.models import Tenant

    cached = _VM_DETAIL_CACHE.get(item_id)
    import time as _t
    if cached and cached[1] > _t.time():
        return cached[0]

    item = await db.get(SnapshotItem, UUID(item_id))
    if not item:
        raise HTTPException(status_code=404, detail="Snapshot item not found")
    snapshot = await db.get(Snapshot, item.snapshot_id)
    if not snapshot:
        raise HTTPException(status_code=404, detail="Parent snapshot not found")
    resource = await db.get(Resource, snapshot.resource_id)
    if not resource:
        raise HTTPException(status_code=404, detail="Source resource not found")
    tenant = await db.get(Tenant, resource.tenant_id)
    if not tenant or not tenant.external_tenant_id:
        raise HTTPException(status_code=400, detail="Tenant missing external_tenant_id")

    sub = resource.azure_subscription_id
    rg = resource.azure_resource_group
    vm_name = resource.external_id
    if not (sub and rg and vm_name):
        raise HTTPException(status_code=400, detail="Resource missing subscription/RG/name")

    token = await _arm_token_for_tenant(tenant.external_tenant_id)
    if not token:
        raise HTTPException(status_code=502, detail="Failed to acquire ARM token")

    base = f"https://management.azure.com/subscriptions/{sub}/resourceGroups/{rg}"
    item_type = (item.item_type or "").upper()
    meta = item.extra_data or {}

    # Prefer the full ARM id stashed in metadata over a name-based
    # lookup — the name we persist (e.g. "vm-demo-os-disk") is a
    # synthetic display label, not the real Azure resource name.
    def _arm_url_from_id(arm_id: str, api: str) -> str:
        return f"https://management.azure.com{arm_id}?api-version={api}"

    url: Optional[str] = None
    if item_type == "AZURE_VM_CONFIG":
        url = f"{base}/providers/Microsoft.Compute/virtualMachines/{vm_name}?$expand=instanceView&api-version=2024-03-01"
    elif item_type == "AZURE_VM_DISK":
        arm_id = meta.get("disk_arm_id") or meta.get("id") or ""
        if arm_id:
            url = _arm_url_from_id(arm_id, "2023-10-02")
        else:
            # Legacy rows store a synthetic name like "vm-demo-os-disk"
            # that doesn't match the real Azure resource. Fall back to
            # listing the RG's disks and matching on `managedBy` + lun.
            list_url = f"{base}/providers/Microsoft.Compute/disks?api-version=2023-10-02"
            listed = await _arm_get_json(token, list_url)
            match = None
            for d in (listed or {}).get("value", []) or []:
                mb = (d.get("managedBy") or "").lower()
                if mb.endswith(f"/virtualmachines/{vm_name.lower()}"):
                    if (meta.get("os_type") or "").lower() in ("windows", "linux"):
                        if d.get("properties", {}).get("osType"):
                            match = d; break
                    else:
                        if not d.get("properties", {}).get("osType"):
                            match = d; break
            if not match:
                disk_name = meta.get("disk_name") or item.name
                url = f"{base}/providers/Microsoft.Compute/disks/{disk_name}?api-version=2023-10-02"
            else:
                url = _arm_url_from_id(match["id"], "2023-10-02")
    elif item_type == "AZURE_VM_NIC":
        arm_id = meta.get("id") or ""
        if arm_id:
            url = _arm_url_from_id(arm_id, "2023-11-01")
        else:
            nic_name = meta.get("nic_name") or item.name
            url = f"{base}/providers/Microsoft.Network/networkInterfaces/{nic_name}?api-version=2023-11-01"
    elif item_type == "AZURE_VM_PUBLIC_IP":
        arm_id = meta.get("id") or ""
        if arm_id:
            url = _arm_url_from_id(arm_id, "2023-11-01")
        else:
            pip_name = meta.get("public_ip_name") or item.name
            url = f"{base}/providers/Microsoft.Network/publicIPAddresses/{pip_name}?api-version=2023-11-01"
    elif item_type == "AZURE_VM_VOLUME":
        # Volumes mirror their underlying disk — reuse the disk fetch.
        arm_id = meta.get("disk_arm_id") or ""
        if arm_id:
            url = _arm_url_from_id(arm_id, "2023-10-02")
        else:
            disk_name = meta.get("disk_name") or (item.name or "").replace(" (volume)", "")
            url = f"{base}/providers/Microsoft.Compute/disks/{disk_name}?api-version=2023-10-02"
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported item_type: {item_type}")

    payload = await _arm_get_json(token, url)
    if payload is None:
        raise HTTPException(status_code=502, detail="Azure ARM lookup failed")

    # The ARM payload nests real fields under `properties`. Flatten
    # those into the top level so the UI can read them without a
    # double-hop, while keeping the raw payload around for fields we
    # didn't explicitly surface.
    result = {
        "id": payload.get("id"),
        "name": payload.get("name"),
        "type": payload.get("type"),
        "location": payload.get("location"),
        "zones": payload.get("zones") or [],
        "tags": payload.get("tags") or {},
        "sku": payload.get("sku") or {},
        "managed_by": payload.get("managedBy"),
        "properties": payload.get("properties") or {},
        "raw": payload,
    }
    _VM_DETAIL_CACHE[item_id] = (result, _t.time() + _VM_DETAIL_TTL)
    return result


# ──────────────────────────────────────────────────────────────────────
# VM volume file browsing — executes Get-ChildItem / ls on the source
# VM via Azure Run Command and parses the output. Requires the VM to
# be running and the Microsoft RunCommand agent to be installed (it
# is by default on Azure-provisioned VMs). Used by the Volumes tab's
# right-hand file browser; "Raw block device" rows always return an
# empty list so the UI can render the "not detected" placeholder.
# ──────────────────────────────────────────────────────────────────────

_VM_FILES_CACHE: dict[str, tuple[dict, float]] = {}
_VM_FILES_TTL = 30

import re as _re
import json as _json


async def _run_command_on_vm(
    token: str, sub: str, rg: str, vm_name: str, script_lines: list[str],
    is_windows: bool,
) -> Optional[str]:
    """POST to Azure Run Command, poll until completed, return stdout."""
    command_id = "RunPowerShellScript" if is_windows else "RunShellScript"
    url = (
        f"https://management.azure.com/subscriptions/{sub}/resourceGroups/{rg}"
        f"/providers/Microsoft.Compute/virtualMachines/{vm_name}/runCommand"
        f"?api-version=2024-03-01"
    )
    body = {"commandId": command_id, "script": script_lines}
    try:
        async with _httpx.AsyncClient(timeout=90) as c:
            r = await c.post(url, json=body, headers={"Authorization": f"Bearer {token}"})
            if r.status_code not in (200, 201, 202):
                return None
            # Run Command is async — Azure returns 202 + Azure-AsyncOperation header.
            status_url = r.headers.get("azure-asyncoperation") or r.headers.get("location")
            if not status_url:
                # 200 with inline body (rare) — try to read it
                try:
                    data = r.json()
                    return (data.get("value") or [{}])[0].get("message", "")
                except Exception:
                    return None
            for _ in range(45):   # ≈90s
                await asyncio.sleep(2)
                poll = await c.get(status_url, headers={"Authorization": f"Bearer {token}"})
                if poll.status_code != 200:
                    continue
                body = poll.json()
                status = (body.get("status") or "").lower()
                if status in ("succeeded", "failed", "canceled"):
                    val = body.get("properties", {}).get("output", {}).get("value") or body.get("value") or []
                    if val and isinstance(val, list):
                        return val[0].get("message", "")
                    return None
            return None
    except Exception:
        return None


@app.get("/api/v1/snapshot-items/{item_id}/vm-volume-files")
async def azure_vm_volume_files(
    item_id: str,
    path: str = Query("", description="Guest-OS path to list (defaults to C:\\ or /)"),
    db: AsyncSession = Depends(get_db),
    _admin: dict = Depends(require_admin),  # Check 17: live-VM ops are admin-only
):
    """List files/folders inside a volume at `path`. Returns empty when
    the volume is a Raw block device (no filesystem detected)."""
    from shared.models import Tenant

    item = await db.get(SnapshotItem, UUID(item_id))
    if not item or (item.item_type or "").upper() != "AZURE_VM_VOLUME":
        raise HTTPException(status_code=404, detail="Volume item not found")

    meta = item.extra_data or {}
    os_type = (meta.get("os_type") or "").lower()
    # Legacy rows don't carry `volume_kind`. Treat OS disks as filesystem
    # volumes so the user can still browse files on old backups.
    is_fs = meta.get("volume_kind") == "filesystem" or os_type in ("windows", "linux")
    if not is_fs:
        return {"detected": False, "items": [], "path": path}

    snapshot = await db.get(Snapshot, item.snapshot_id)
    resource = await db.get(Resource, snapshot.resource_id)
    tenant = await db.get(Tenant, resource.tenant_id)
    if not tenant or not tenant.external_tenant_id:
        raise HTTPException(status_code=400, detail="Tenant missing external_tenant_id")

    sub = resource.azure_subscription_id
    rg = resource.azure_resource_group
    vm_name = resource.external_id
    if not (sub and rg and vm_name):
        raise HTTPException(status_code=400, detail="Resource missing subscription/RG/name")

    fs_hint = (meta.get("file_system") or "").upper()
    is_windows = fs_hint == "NTFS" or (not fs_hint and os_type == "windows")
    default_mount = meta.get("mount_point") or ("C:\\" if is_windows else "/")
    # Check 17: confine the client path under the volume mount point.
    target = confine_guest_path(default_mount, path, is_windows)

    cache_key = f"{item_id}|{target}"
    cached = _VM_FILES_CACHE.get(cache_key)
    if cached and cached[1] > _t.time():
        return cached[0]

    token = await _arm_token_for_tenant(tenant.external_tenant_id)
    if not token:
        raise HTTPException(status_code=502, detail="Failed to acquire ARM token")

    if is_windows:
        # Emit a JSON array so we don't have to scrape formatted output.
        # -Force to include hidden items; errors suppressed so a locked
        # folder still returns what it can.
        safe = target.replace("'", "''")
        script = [
            f"$items = Get-ChildItem -Force -LiteralPath '{safe}' -ErrorAction SilentlyContinue | "
            "Select-Object Name, @{n='IsDirectory';e={$_.PSIsContainer}}, Length, "
            "@{n='LastWriteTime';e={$_.LastWriteTime.ToString('o')}}; "
            "$items | ConvertTo-Json -Compress"
        ]
        stdout = await _run_command_on_vm(token, sub, rg, vm_name, script, is_windows=True)
    else:
        safe = target.replace("'", "'\\''")
        script = [
            f"ls -la --time-style=full-iso '{safe}' 2>/dev/null | awk '"
            "NR>1 {type=substr($1,1,1); size=$5; t=$6\" \"$7; "
            "name=\"\"; for(i=9;i<=NF;i++) name=name\" \"$i; sub(/^ /,\"\",name); "
            "printf \"%s\\t%s\\t%s\\t%s\\n\", (type==\"d\"?\"D\":\"F\"), size, t, name}'"
        ]
        stdout = await _run_command_on_vm(token, sub, rg, vm_name, script, is_windows=False)

    if stdout is None:
        return {"detected": True, "items": [], "path": target, "error": "VM not reachable (may be stopped)"}

    items: list[dict] = []
    if is_windows:
        # Strip the PSHostRunCommand header lines; JSON is the last chunk.
        m = _re.search(r"(\[[\s\S]*\]|\{[\s\S]*\})", stdout)
        if m:
            try:
                parsed = _json.loads(m.group(1))
                if isinstance(parsed, dict):  # single-entry directories return a dict
                    parsed = [parsed]
                for e in parsed:
                    items.append({
                        "name": e.get("Name", ""),
                        "isDirectory": bool(e.get("IsDirectory")),
                        "size": int(e.get("Length") or 0),
                        "modified": e.get("LastWriteTime", ""),
                    })
            except Exception:
                pass
    else:
        for line in stdout.splitlines():
            parts = line.split("\t")
            if len(parts) >= 4 and parts[3].strip() not in (".", ".."):
                items.append({
                    "name": parts[3].strip(),
                    "isDirectory": parts[0] == "D",
                    "size": int(parts[1] or 0),
                    "modified": parts[2],
                })

    result = {"detected": True, "items": items, "path": target}
    _VM_FILES_CACHE[cache_key] = (result, _t.time() + _VM_FILES_TTL)
    return result


# ──────────────────────────────────────────────────────────────────────
# VM volume file download — for a given volume SnapshotItem, fetch one
# or more files / folders from the running VM via Azure Run Command
# and either stream the single file back or build a ZIP preserving
# the original folder structure. Used by the Volumes tab's Download
# button. Run Command has a ~4 KB stdout cap so we read file bytes in
# chunked base64 blocks — each chunk is its own Run Command call.
# ──────────────────────────────────────────────────────────────────────

import base64 as _b64
import io as _io
import zipfile as _zipfile
import posixpath as _pp


async def _run_script(
    token: str, sub: str, rg: str, vm_name: str, script: list[str], is_windows: bool,
) -> Optional[str]:
    return await _run_command_on_vm(token, sub, rg, vm_name, script, is_windows=is_windows)


def _windows_path_join(parent: str, name: str) -> str:
    p = parent if parent.endswith("\\") else (parent + "\\")
    return p + name


def _posix_path_join(parent: str, name: str) -> str:
    return _pp.join(parent, name)


async def _vm_list_dir(token: str, sub: str, rg: str, vm_name: str,
                      path: str, is_windows: bool) -> list[dict]:
    """Return `{name,isDirectory,size}[]` for every entry in `path`.
    Recursive enumeration happens in the caller (one RunCommand per
    directory) so we stay under the output limit even for deep trees."""
    safe = path.replace("'", "''" if is_windows else "'\\''")
    if is_windows:
        script = [
            f"Get-ChildItem -Force -LiteralPath '{safe}' -ErrorAction SilentlyContinue | "
            "Select-Object Name, @{n='IsDirectory';e={$_.PSIsContainer}}, Length | "
            "ConvertTo-Json -Compress"
        ]
    else:
        script = [
            f"ls -la --time-style=full-iso '{safe}' 2>/dev/null | awk '"
            "NR>1 {type=substr($1,1,1); size=$5; "
            "name=\"\"; for(i=9;i<=NF;i++) name=name\" \"$i; sub(/^ /,\"\",name); "
            "printf \"%s\\t%s\\t%s\\n\", (type==\"d\"?\"D\":\"F\"), size, name}'"
        ]
    stdout = await _run_script(token, sub, rg, vm_name, script, is_windows=is_windows)
    if stdout is None:
        return []
    out: list[dict] = []
    if is_windows:
        m = _re.search(r"(\[[\s\S]*\]|\{[\s\S]*\})", stdout)
        if m:
            try:
                parsed = _json.loads(m.group(1))
                if isinstance(parsed, dict):
                    parsed = [parsed]
                for e in parsed:
                    nm = e.get("Name")
                    if nm and nm not in (".", ".."):
                        out.append({
                            "name": nm,
                            "isDirectory": bool(e.get("IsDirectory")),
                            "size": int(e.get("Length") or 0),
                        })
            except Exception:
                pass
    else:
        for line in stdout.splitlines():
            parts = line.split("\t")
            if len(parts) >= 3 and parts[2].strip() not in (".", ".."):
                out.append({
                    "name": parts[2].strip(),
                    "isDirectory": parts[0] == "D",
                    "size": int(parts[1] or 0),
                })
    return out


async def _vm_read_file_bytes(
    token: str, sub: str, rg: str, vm_name: str, path: str, is_windows: bool,
) -> Optional[bytes]:
    """Read a single file off the VM as bytes. Azure Run Command caps
    output at ~4096 bytes so we stream the file in 3000-byte base64
    chunks — each chunk is its own Run Command invocation reading a
    different byte range of the source file."""
    chunk_bytes = 3000  # chunk size in RAW bytes; base64 expands ~1.33x → ~4000 chars
    if is_windows:
        safe = path.replace("'", "''")
        # First get the file length so we know how many chunks to read.
        len_script = [f"(Get-Item -LiteralPath '{safe}' -ErrorAction Stop).Length"]
        len_out = await _run_script(token, sub, rg, vm_name, len_script, is_windows=True)
        if len_out is None:
            return None
        try:
            total = int(_re.search(r"\d+", len_out or "").group(0))
        except Exception:
            return None
        buf = bytearray()
        offset = 0
        while offset < total:
            take = min(chunk_bytes, total - offset)
            # Read a specific byte range from the file and emit it
            # base64-encoded. Run Command returns stdout as text; we
            # strip the transcript header with a regex on the client.
            script = [
                f"$fs = [System.IO.File]::OpenRead('{safe}'); "
                f"$fs.Seek({offset}, 'Begin') | Out-Null; "
                f"$buf = New-Object byte[] {take}; "
                f"$n = $fs.Read($buf, 0, {take}); "
                f"$fs.Close(); "
                f"[Convert]::ToBase64String($buf, 0, $n)"
            ]
            out = await _run_script(token, sub, rg, vm_name, script, is_windows=True)
            if out is None:
                return None
            # Extract the base64 payload (only printable A-Z,a-z,0-9,+,/,= run).
            m = _re.search(r"[A-Za-z0-9+/=]{4,}\s*$", out.strip())
            if not m:
                return None
            try:
                buf.extend(_b64.b64decode(m.group(0)))
            except Exception:
                return None
            offset += take
        return bytes(buf)
    else:
        safe = path.replace("'", "'\\''")
        len_script = [f"stat -c%s '{safe}' 2>/dev/null"]
        len_out = await _run_script(token, sub, rg, vm_name, len_script, is_windows=False)
        if len_out is None:
            return None
        try:
            total = int(_re.search(r"\d+", len_out or "").group(0))
        except Exception:
            return None
        buf = bytearray()
        offset = 0
        while offset < total:
            take = min(chunk_bytes, total - offset)
            script = [
                f"dd if='{safe}' bs=1 skip={offset} count={take} 2>/dev/null | base64 -w0"
            ]
            out = await _run_script(token, sub, rg, vm_name, script, is_windows=False)
            if out is None:
                return None
            m = _re.search(r"[A-Za-z0-9+/=]{4,}\s*$", out.strip())
            if not m:
                return None
            try:
                buf.extend(_b64.b64decode(m.group(0)))
            except Exception:
                return None
            offset += take
        return bytes(buf)


@app.post("/api/v1/snapshot-items/{item_id}/vm-volume-download")
async def azure_vm_volume_download(
    item_id: str,
    payload: dict = Body(...),
    db: AsyncSession = Depends(get_db),
    _admin: dict = Depends(require_admin),  # Check 17: live-VM ops are admin-only
):
    """Download one or more files / folders from a VM volume.

    Body: `{ paths: ["C:\\Users\\admin\\notes.txt", "C:\\Temp"] }`
    Returns either the raw file (when paths is exactly one file) or
    a ZIP archive with the paths rooted at their basename so the
    original folder structure is preserved inside the archive."""
    from shared.models import Tenant
    paths = [p for p in (payload.get("paths") or []) if isinstance(p, str) and p]
    if not paths:
        raise HTTPException(status_code=400, detail="paths[] required")

    item = await db.get(SnapshotItem, UUID(item_id))
    if not item or (item.item_type or "").upper() != "AZURE_VM_VOLUME":
        raise HTTPException(status_code=404, detail="Volume item not found")

    snapshot = await db.get(Snapshot, item.snapshot_id)
    resource = await db.get(Resource, snapshot.resource_id)
    tenant = await db.get(Tenant, resource.tenant_id)
    sub = resource.azure_subscription_id
    rg = resource.azure_resource_group
    vm_name = resource.external_id
    if not (tenant and tenant.external_tenant_id and sub and rg and vm_name):
        raise HTTPException(status_code=400, detail="Missing tenant/sub/RG/vm")

    meta = item.extra_data or {}
    os_type = (meta.get("os_type") or "").lower()
    fs_hint = (meta.get("file_system") or "").upper()
    is_windows = fs_hint == "NTFS" or (not fs_hint and os_type == "windows")

    # Check 17: confine each client-supplied path under the volume mount point
    # so a caller cannot read arbitrary guest files (ntds.dit, /etc/shadow).
    _root = meta.get("mount_point") or ("C:\\" if is_windows else "/")
    paths = [confine_guest_path(_root, p, is_windows) for p in paths]

    token = await _arm_token_for_tenant(tenant.external_tenant_id)
    if not token:
        raise HTTPException(status_code=502, detail="Failed to acquire ARM token")

    # Figure out whether each path is a file or a directory up front
    # so we can decide between single-file stream vs ZIP.
    async def _stat(path: str) -> Optional[dict]:
        """Is it a file or a folder? `Test-Path` tells us existence even
        when `Get-Item` returns null for locked files (pagefile.sys,
        DumpStack.log.tmp, swapfile, etc) — access denied shouldn't
        look the same as "doesn't exist" to the caller."""
        safe = path.replace("'", "''" if is_windows else "'\\''")
        if is_windows:
            script = [
                f"$p = '{safe}'; "
                "if (Test-Path -LiteralPath $p -PathType Container) { "
                "'{\"IsDirectory\":true,\"Length\":0}' "
                "} elseif (Test-Path -LiteralPath $p) { "
                # File exists. Try to read its length; if the OS has
                # the handle open exclusively, fall back to 0 (the
                # reader will fail later with a clear error rather
                # than us claiming the file doesn't exist here).
                "$len = 0; "
                "try { $len = (Get-Item -Force -LiteralPath $p -ErrorAction Stop).Length } catch {} "
                "'{\"IsDirectory\":false,\"Length\":' + $len + '}' "
                "} else { 'NOT_FOUND' }"
            ]
        else:
            script = [f"[ -d '{safe}' ] && echo D || ([ -f '{safe}' ] && echo F || echo X); stat -c%s '{safe}' 2>/dev/null || echo 0"]
        out = await _run_script(token, sub, rg, vm_name, script, is_windows=is_windows)
        if out is None:
            return None
        if is_windows:
            if "NOT_FOUND" in out:
                return None
            m = _re.search(r"\{[\s\S]*?\}", out)
            if not m:
                return None
            try:
                return _json.loads(m.group(0))
            except Exception:
                return None
        lines = out.strip().splitlines()
        if len(lines) < 2:
            return None
        typ, sz = lines[0].strip(), lines[1].strip()
        if typ == "X":
            return None
        return {"IsDirectory": typ == "D", "Length": int(sz or 0)}

    # Best-effort stat. Windows system files (pagefile.sys, swapfile,
    # DumpStack.log.tmp, etc.) are locked by the kernel in ways that
    # confuse both Get-Item and Test-Path depending on the SP's ACLs.
    # Rather than claim the file doesn't exist we mark it "unknown
    # file" and let the reader surface the real error if it fails.
    stats = {}
    for p in paths:
        stats[p] = await _stat(p) or {"IsDirectory": False, "Length": 0, "statFailed": True}

    # Single file → stream directly. Only take this shortcut when the
    # request was for exactly one path and that path is a regular file.
    if len(paths) == 1 and not stats[paths[0]]["IsDirectory"]:
        path = paths[0]
        data = await _vm_read_file_bytes(token, sub, rg, vm_name, path, is_windows=is_windows)
        if data is None:
            raise HTTPException(status_code=502, detail="Failed to read file from VM")
        filename = path.rsplit("\\" if is_windows else "/", 1)[-1] or "download"
        await _emit_download_audit(
            action="AZURE_VM_DOWNLOAD",
            outcome="SUCCESS",
            tenant_id=str(tenant.id),
            resource_id=str(resource.id),
            resource_type="AZURE_VM",
            snapshot_id=str(snapshot.id),
            item_count=1,
            details={"mode": "single_file", "path": path, "volume_item_id": item_id},
        )
        return StreamingResponse(
            iter([data]),
            media_type="application/octet-stream",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    # Otherwise build a ZIP in memory. Walk any folders recursively
    # so the caller can select a folder and get everything inside.
    sep = "\\" if is_windows else "/"

    async def _walk(root_path: str, arcname_prefix: str):
        """Yield (archive_name, bytes) for every file under root_path."""
        stack: list[tuple[str, str]] = [(root_path, arcname_prefix)]
        while stack:
            cur, arc = stack.pop()
            entries = await _vm_list_dir(token, sub, rg, vm_name, cur, is_windows=is_windows)
            for e in entries:
                nm = e["name"]
                child = (_windows_path_join(cur, nm) if is_windows
                         else _posix_path_join(cur, nm))
                arc_child = f"{arc}/{nm}" if arc else nm
                if e["isDirectory"]:
                    stack.append((child, arc_child))
                else:
                    data = await _vm_read_file_bytes(token, sub, rg, vm_name, child, is_windows=is_windows)
                    if data is not None:
                        yield (arc_child, data)

    zip_buf = _io.BytesIO()
    with _zipfile.ZipFile(zip_buf, "w", _zipfile.ZIP_DEFLATED) as zf:
        for p in paths:
            base = p.rstrip(sep).rsplit(sep, 1)[-1] or p
            if stats[p]["IsDirectory"]:
                async for arc_name, data in _walk(p, base):
                    zf.writestr(arc_name, data)
            else:
                data = await _vm_read_file_bytes(token, sub, rg, vm_name, p, is_windows=is_windows)
                if data is not None:
                    zf.writestr(base, data)

    zip_buf.seek(0)
    await _emit_download_audit(
        action="AZURE_VM_DOWNLOAD",
        outcome="SUCCESS",
        tenant_id=str(tenant.id),
        resource_id=str(resource.id),
        resource_type="AZURE_VM",
        snapshot_id=str(snapshot.id),
        item_count=len(paths),
        details={
            "mode": "zip",
            "paths": paths[:10],
            "volume_item_id": item_id,
        },
    )
    return StreamingResponse(
        zip_buf,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{vm_name}-volume-files.zip"'},
    )
