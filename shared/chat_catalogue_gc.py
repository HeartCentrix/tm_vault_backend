"""Fenced read leases and exclusive catalogue object ownership."""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from datetime import timedelta
import logging
import time
from uuid import UUID, uuid4

from sqlalchemy import delete, exists, func, or_, select, text

from shared.chat_catalogue import ChatCatalogueIntegrityError
from shared.models import (ChatBlobObject, ChatCataloguePin, ChatCaptureAttempt, ChatInventory,
                           ChatInventoryItem, ChatItemVersion, Snapshot,
                           SnapshotChatInventory, SnapshotItem)

log = logging.getLogger(__name__)


def _arguments(owner_id, expires_seconds):
    if not isinstance(owner_id, UUID):
        raise ValueError("pin owner must be a UUID")
    if type(expires_seconds) is not int or expires_seconds <= 0:
        raise ValueError("pin lifetime must be a positive integer")


async def acquire_catalogue_read_pins(session, *, snapshot_ids, owner_kind, owner_id,
                                      expires_seconds=300, object_ids=None):
    """Protect only catalogue content referenced by authorized snapshot IDs."""
    _arguments(owner_id, expires_seconds)
    if owner_kind not in {"read", "export", "dr"}:
        raise ValueError("unsupported catalogue pin owner")
    snapshot_ids = set(snapshot_ids)
    snapshots = list((await session.scalars(select(Snapshot).where(Snapshot.id.in_(snapshot_ids)).order_by(Snapshot.id).with_for_update(read=True))).all())
    if not snapshot_ids or {s.id for s in snapshots} != snapshot_ids:
        raise ChatCatalogueIntegrityError("read pin snapshot scope no longer exists")
    new_ids = {s.id for s in snapshots if (s.extra_data or {}).get("chat_inventory_format") == 1}
    referenced = set((await session.scalars(select(ChatItemVersion.object_id).join(
        SnapshotItem, SnapshotItem.chat_version_id == ChatItemVersion.id).where(
        SnapshotItem.snapshot_id.in_(new_ids), ChatItemVersion.object_id.is_not(None)))).all())
    inventories = []
    covered = set()
    if object_ids is not None:
        selected = set(object_ids)
        if not selected <= referenced:
            raise ChatCatalogueIntegrityError("object is not referenced by the owned snapshot scope")
    else:
        inventories = list((await session.scalars(select(ChatInventory).join(
            SnapshotChatInventory, SnapshotChatInventory.inventory_id == ChatInventory.id).where(
            SnapshotChatInventory.snapshot_id.in_(new_ids)).order_by(ChatInventory.id)
            .with_for_update(of=ChatInventory).execution_options(populate_existing=True))).unique().all())
        if any(i.state not in {"COMPLETE", "PARTIAL"} for i in inventories):
            raise ChatCatalogueIntegrityError("snapshot inventory is unavailable for reading")
        covered = set((await session.scalars(select(ChatItemVersion.object_id).join(
            ChatInventoryItem, ChatInventoryItem.version_id == ChatItemVersion.id).where(
            ChatInventoryItem.inventory_id.in_({i.id for i in inventories}),
            ChatItemVersion.object_id.is_not(None)))).all())
        selected = referenced - covered
    required = selected | covered
    checked = list((await session.scalars(select(ChatBlobObject).where(ChatBlobObject.id.in_(required))
        .order_by(ChatBlobObject.id).with_for_update().execution_options(populate_existing=True))).all()) if required else []
    if len(checked) != len(required) or any(o.state != "LIVE" for o in checked):
        raise ChatCatalogueIntegrityError("required object is not LIVE or is retiring")
    objects = [obj for obj in checked if obj.id in selected]
    now = await session.scalar(select(func.clock_timestamp()))
    pins = []
    for field, rows in (("inventory_id", inventories), ("object_id", objects)):
        for row in rows:
            identifier = uuid4()
            session.add(ChatCataloguePin(id=identifier, **{field: row.id}, owner_kind=owner_kind,
                owner_id=owner_id, generation=1, expires_at=now + timedelta(seconds=expires_seconds)))
            pins.append(identifier)
    await session.flush()
    return pins


async def renew_catalogue_read_pins(session, *, pin_ids, owner_id, expires_seconds=300):
    _arguments(owner_id, expires_seconds)
    ids = set(pin_ids)
    if not ids:
        return
    rows = list((await session.scalars(select(ChatCataloguePin).where(ChatCataloguePin.id.in_(ids))
        .order_by(ChatCataloguePin.id).with_for_update().execution_options(populate_existing=True))).all())
    now = await session.scalar(select(func.clock_timestamp()))
    if len(rows) != len(ids) or any(p.owner_id != owner_id or p.generation != 1 or
            p.expires_at is None or p.expires_at <= now for p in rows):
        raise ChatCatalogueIntegrityError("read pin ownership is missing or expired")
    for pin in rows:
        pin.expires_at = now + timedelta(seconds=expires_seconds)
    await session.flush()


async def release_catalogue_read_pins(session, *, pin_ids, owner_id):
    await session.execute(delete(ChatCataloguePin).where(ChatCataloguePin.id.in_(set(pin_ids)),
        ChatCataloguePin.owner_id == owner_id, ChatCataloguePin.generation == 1))


class _CatalogueReadLease:
    def __init__(self, factory, pins, owner, seconds, deadline):
        self.factory, self.pins, self.owner, self.seconds = factory, pins, owner, seconds
        self.deadline, self.failure = deadline, None

    def assert_valid(self):
        if self.failure is not None or time.monotonic() >= self.deadline:
            raise ChatCatalogueIntegrityError("catalogue read pin lease renewal was lost") from self.failure

    async def guard(self, stream):
        try:
            self.assert_valid()
            async for chunk in stream:
                self.assert_valid()
                yield chunk
            self.assert_valid()
        finally:
            close = getattr(stream, "aclose", None)
            if close is not None:
                await close()

    async def keep_alive(self):
        try:
            while True:
                await asyncio.sleep(self.seconds / 3)
                self.assert_valid()
                started = time.monotonic()
                async with self.factory.begin() as session:
                    try:
                        await renew_catalogue_read_pins(session, pin_ids=self.pins,
                            owner_id=self.owner, expires_seconds=self.seconds)
                    except Exception as exc:
                        # Fence readers before rollback/network cleanup can yield.
                        self.failure = exc
                        raise
                # Conservative deadline: transaction/network time cannot extend a DB lease.
                self.deadline = started + self.seconds
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.failure = exc


@asynccontextmanager
async def catalogue_read_pin(session_factory, *, snapshot_ids, owner_kind="read", owner_id=None,
                              expires_seconds=300, object_ids=None):
    owner = owner_id or uuid4()
    started = time.monotonic()
    async with session_factory.begin() as session:
        ids = await acquire_catalogue_read_pins(session, snapshot_ids=snapshot_ids,
            owner_kind=owner_kind, owner_id=owner, expires_seconds=expires_seconds, object_ids=object_ids)
    lease = _CatalogueReadLease(session_factory, ids, owner, expires_seconds, started + expires_seconds)
    renewer = asyncio.create_task(lease.keep_alive())
    try:
        lease.assert_valid()
        yield lease
        lease.assert_valid()
    finally:
        renewer.cancel()
        with suppress(asyncio.CancelledError):
            await renewer
        try:
            async with session_factory.begin() as session:
                await release_catalogue_read_pins(session, pin_ids=ids, owner_id=owner)
        except Exception:
            log.warning("catalogue pin cleanup failed; retained leases will expire")


@dataclass(frozen=True)
class CatalogueGCResult:
    inventories_deleted: int = 0
    versions_deleted: int = 0
    objects_deleted: int = 0
    deletion_failures: int = 0


_ACTIVE_CAPTURE = {"CREATED", "ENUMERATING", "CONTENT_PENDING", "VERIFYING", "READY"}


def _pin_live(now):
    return or_(ChatCataloguePin.expires_at.is_(None), ChatCataloguePin.expires_at > now)


async def _retire_unused_inventories(session, limit):
    now = await session.scalar(select(func.clock_timestamp()))
    expired = list((await session.scalars(select(ChatCataloguePin.id).where(
        ChatCataloguePin.expires_at <= now).order_by(ChatCataloguePin.id).limit(limit)
        .with_for_update(skip_locked=True))).all())
    if expired:
        await session.execute(delete(ChatCataloguePin).where(ChatCataloguePin.id.in_(expired)))
    candidates = list((await session.scalars(select(ChatInventory.id).where(
        ~exists().where(SnapshotChatInventory.inventory_id == ChatInventory.id),
        ~exists().where(ChatCataloguePin.inventory_id == ChatInventory.id),
        ~exists().where(ChatCaptureAttempt.staged_inventory_id == ChatInventory.id,
            ChatCaptureAttempt.state.in_(_ACTIVE_CAPTURE), ChatCaptureAttempt.expires_at > now))
        .order_by(ChatInventory.created_at, ChatInventory.id).limit(limit))).all())
    count = 0
    for inventory_id in candidates:
        # A terminal/expired attempt cannot publish. Physical snapshot links
        # independently retain any readable unpublished partial versions.
        attempts = list((await session.scalars(select(ChatCaptureAttempt).where(
            ChatCaptureAttempt.staged_inventory_id == inventory_id).order_by(ChatCaptureAttempt.id)
            .with_for_update(skip_locked=True).execution_options(populate_existing=True))).all())
        total = await session.scalar(select(func.count()).select_from(ChatCaptureAttempt).where(
            ChatCaptureAttempt.staged_inventory_id == inventory_id))
        if len(attempts) != total or any(
                a.state in _ACTIVE_CAPTURE and a.expires_at > now for a in attempts):
            continue
        inventory = await session.scalar(select(ChatInventory).where(ChatInventory.id == inventory_id)
            .with_for_update(skip_locked=True).execution_options(populate_existing=True))
        if inventory is None:
            continue
        if await session.scalar(select(exists().where(SnapshotChatInventory.inventory_id == inventory_id))):
            continue
        if await session.scalar(select(exists().where(ChatCataloguePin.inventory_id == inventory_id))):
            continue
        # Every referencing attempt must be removed in this transaction: its
        # inventory FK is deferred, while the reverse FK is immediate.
        ids = {a.id for a in attempts}
        if inventory.capture_attempt_id not in ids:
            continue
        if await session.scalar(select(exists().where(SnapshotChatInventory.capture_attempt_id.in_(ids)))):
            continue
        await session.execute(delete(ChatInventory).where(ChatInventory.id == inventory_id))
        await session.execute(delete(ChatCaptureAttempt).where(ChatCaptureAttempt.id.in_(ids)))
        count += 1
    return count


def _version_unreferenced():
    return (~exists().where(ChatInventoryItem.version_id == ChatItemVersion.id),
            ~exists().where(SnapshotItem.chat_version_id == ChatItemVersion.id))


async def _claim_object(session, object_id):
    obj = await session.scalar(select(ChatBlobObject).where(ChatBlobObject.id == object_id)
        .with_for_update(skip_locked=True).execution_options(populate_existing=True))
    if obj is None or not obj.path.startswith("chat-catalogue/v1/"):
        return None, 0
    now = await session.scalar(select(func.clock_timestamp()))
    if obj.state == "DELETED" and (obj.deleted_at is None or obj.deleted_at > now - timedelta(days=1)):
        return None, 0
    if await session.scalar(select(exists().where(ChatCataloguePin.object_id == obj.id, _pin_live(now)))):
        return None, 0
    owner = await session.get(ChatCaptureAttempt, obj.capture_attempt_id) if obj.capture_attempt_id else None
    if owner is not None and owner.state in _ACTIVE_CAPTURE and owner.expires_at > now:
        return None, 0
    if obj.state == "STAGING" and (obj.upload_deadline is None or obj.quiescent_after is None or
            obj.upload_deadline > now or obj.quiescent_after > now):
        return None, 0
    # Stage/publish also locks objects before creating version references. Retire
    # only after removing versions with no inventory OR physical snapshot link.
    removed = list((await session.scalars(delete(ChatItemVersion).where(
        ChatItemVersion.object_id == obj.id, *_version_unreferenced()).returning(ChatItemVersion.id))).all())
    if await session.scalar(select(exists().where(ChatItemVersion.object_id == obj.id))):
        return None, len(removed)
    obj.state = "DELETING"
    obj.deletion_task_id = obj.deletion_task_id or uuid4()
    obj.deletion_requested_at = now
    obj.deletion_attempts += 1
    await session.flush()
    return (obj.id, obj.deletion_task_id, obj.backend_id, obj.container, obj.path), len(removed)


async def collect_chat_catalogue(*, session_factory, storage, limit=100):
    """Bounded exclusive GC; durable retirement precedes physical deletion.

    Only catalogue-owned generations are touched. Snapshot associations and
    active pins preserve inventory members; unknown upload quiescence retains
    staging objects. Failed deletes keep the same durable task for replay.
    """
    if type(limit) is not int or not 1 <= limit <= 1000:
        raise ValueError("catalogue GC limit must be between 1 and 1000")
    async with session_factory.begin() as session:
        # Never stack collectors from several scheduler replicas.
        acquired = await session.scalar(text("SELECT pg_try_advisory_xact_lock(807631199, 1)"))
        if not acquired:
            return CatalogueGCResult()
        inventories = await _retire_unused_inventories(session, limit)
        inline_ids = list((await session.scalars(select(ChatItemVersion.id).where(
            ChatItemVersion.object_id.is_(None), *_version_unreferenced())
            .order_by(ChatItemVersion.id).limit(limit).with_for_update(skip_locked=True))).all())
        if inline_ids:
            await session.execute(delete(ChatItemVersion).where(ChatItemVersion.id.in_(inline_ids), *_version_unreferenced()))
        # Oldest first, so a bounded pass makes progress through an orphan backlog.
        candidates = list((await session.scalars(select(ChatBlobObject.id).where(
            or_(ChatBlobObject.state != "DELETED", ChatBlobObject.deleted_at <= func.clock_timestamp() - timedelta(days=1)),
            ~exists().where(ChatCataloguePin.object_id == ChatBlobObject.id, _pin_live(func.clock_timestamp())),
            ~exists().where(ChatItemVersion.object_id == ChatBlobObject.id,
                or_(exists().where(ChatInventoryItem.version_id == ChatItemVersion.id),
                    exists().where(SnapshotItem.chat_version_id == ChatItemVersion.id))),
            ~exists().where(ChatCaptureAttempt.id == ChatBlobObject.capture_attempt_id,
                ChatCaptureAttempt.state.in_(_ACTIVE_CAPTURE), ChatCaptureAttempt.expires_at > func.clock_timestamp()),
            or_(ChatBlobObject.state != "STAGING", (ChatBlobObject.upload_deadline <= func.clock_timestamp()) &
                (ChatBlobObject.quiescent_after <= func.clock_timestamp()))
            ).order_by(ChatBlobObject.created_at, ChatBlobObject.id)
            .limit(limit))).all())
    deleted_count = failures = 0
    versions = len(inline_ids)
    for object_id in candidates:
        async with session_factory.begin() as session:
            task, removed = await _claim_object(session, object_id)
            versions += removed
        if task is None:
            continue
        identifier, task_id, backend_id, container, path = task
        try:
            store = storage.get_store_by_id(str(backend_id))
            if store is None:
                raise RuntimeError("catalogue backend unavailable")
            await store.delete(container, path)
        except Exception:
            failures += 1
            async with session_factory.begin() as session:
                await session.execute(ChatBlobObject.__table__.update().where(
                    ChatBlobObject.id == identifier, ChatBlobObject.deletion_task_id == task_id,
                    ChatBlobObject.state == "DELETING").values(deletion_error="storage deletion failed"))
            continue
        async with session_factory.begin() as session:
            await session.execute(ChatBlobObject.__table__.update().where(
                ChatBlobObject.id == identifier, ChatBlobObject.deletion_task_id == task_id,
                ChatBlobObject.state == "DELETING").values(state="DELETED", deleted_at=func.clock_timestamp(),
                                                         deletion_error=None))
        deleted_count += 1
    return CatalogueGCResult(inventories, versions, deleted_count, failures)
