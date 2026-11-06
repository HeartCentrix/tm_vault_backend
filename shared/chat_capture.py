"""Bounded full chat reconciliation into immutable, fenced catalogue captures."""
from __future__ import annotations

import asyncio
import copy
from collections import OrderedDict
from contextlib import AsyncExitStack, asynccontextmanager, suppress
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
import logging
import os
import re
import sqlite3
import tempfile
import time
from uuid import UUID
from weakref import WeakKeyDictionary

from sqlalchemy import and_, delete, exists, func, or_, select, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import SQLAlchemyError

from shared import chat_catalogue as catalogue
from shared.chat_catalogue_identity import (
    SOURCE_UNAVAILABLE_FIELD,
    CaptureEvidence, CaptureResult, CapturedChatItem, ChatContentDescriptor,
    _digest, _payload, authentication_context_fingerprint, canonical_version_fingerprint, metadata_only_attachment,
    qualified_chat_item_key, observation_semantic, source_inventory_fingerprint, validate_observation_hash_version,
)
from shared.chat_capture_graph import EXTERNAL_TENANT_FILE, ChatGraphError, StrictChatGraphSource, effective_capture_scope_hash, pinned_chat_source
from shared.chat_capture_storage import upload_capture_content, verify_catalogue_content
from shared.chat_content_stream import spool_chat_content
from shared.db_retry import run_catalogue_transaction
from shared.lease import run_bounded
from shared.provider_read import _identifier
from shared.models import ChatBlobObject, ChatCaptureAttempt, ChatInventory, ChatInventoryItem, ChatItemVersion, SnapshotChatInventory, SnapshotItem

logger = logging.getLogger(__name__)


class ChatSourceMovement(RuntimeError):
    """The same source membership/version could not be reconciled."""


_HOSTED_CONTENT_REFERENCE = re.compile(r"/messages/[^/\"'<>\s]+/hostedContents/[^/\"'<>\s]+/\$value")


def message_references_hosted_content(message) -> bool:
    """Graph addresses hosted content only by its ``$value`` URL (inline image
    ``src``, card/code-snippet ``contentUrl``); a message without that URL has
    nothing to list, so the per-message ``/hostedContents`` request is skipped."""
    return bool(_HOSTED_CONTENT_REFERENCE.search(json.dumps(message, default=str)))


class ChatObservationLedger:
    """Disk-backed hash ordering; retain no whole-chat payload list in memory."""

    def __init__(self, scope, observation_hash_version=1):
        self.observation_hash_version = validate_observation_hash_version(observation_hash_version)
        self.scope = scope
        handle = tempfile.NamedTemporaryFile(prefix="chat-observation-", suffix=".sqlite", delete=False)
        self.path = handle.name
        handle.close()
        self.db = sqlite3.connect(self.path)
        self.db.execute("CREATE TABLE observed (pass INTEGER, key TEXT, semantic TEXT, binary INTEGER, item_type TEXT, "
                        "graph_id TEXT, parent_graph_id TEXT, file_payload TEXT, PRIMARY KEY(pass,key))")
        self.db.execute("CREATE TABLE expected (pass INTEGER, key TEXT, required INTEGER, PRIMARY KEY(pass,key))")
        self.db.execute("CREATE TABLE failures (pass INTEGER, phase TEXT, error_type TEXT, status INTEGER, graph_id TEXT, parent_graph_id TEXT, provider_code TEXT, provider_inner_code TEXT, provider_request_id TEXT)")
        self.db.execute("CREATE TABLE source_messages (pass INTEGER, message_id TEXT, source_sha256 TEXT NOT NULL, PRIMARY KEY(pass,message_id))")

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.db.close()
        os.unlink(self.path)

    def expect(self, observation, item_type, graph_id, parent_graph_id, *, required):
        key = qualified_chat_item_key(self.scope.chat_id, item_type, graph_id, parent_graph_id)
        self.db.execute("INSERT INTO expected VALUES (?,?,?) ON CONFLICT(pass,key) DO UPDATE SET required=MAX(required,excluded.required)",
                        (observation, key, int(required)))

    def observe_message(self, observation, message):
        # Overlapping pages need one observation per message, independently in each pass.
        identifier = message.get("id")
        if not isinstance(identifier, str) or not identifier:
            raise ValueError("source message identity is missing")
        source = hashlib.sha256(json.dumps(message, sort_keys=True, separators=(",", ":"),
            ensure_ascii=False, allow_nan=False).encode()).hexdigest()
        previous = self.db.execute("SELECT source_sha256 FROM source_messages WHERE pass=? AND message_id=?",
            (observation, identifier)).fetchone()
        if previous is not None and previous[0] == source:
            return False
        # A refreshed signed URL needs normal verification, not a forced reconciliation.
        self.db.execute("INSERT OR REPLACE INTO source_messages VALUES (?,?,?)", (observation, identifier, source))
        return True

    def record(self, observation, item):
        key = qualified_chat_item_key(self.scope.chat_id, item.item_type, item.graph_id, item.parent_graph_id)
        content = item.content
        semantic = observation_semantic(item, self.observation_hash_version)
        old = self.db.execute("SELECT semantic FROM observed WHERE pass=? AND key=?", (observation, key)).fetchone()
        if old:
            if old[0] != semantic:
                raise ChatSourceMovement("conflicting source versions during enumeration")
            return False
        payload = (json.dumps(_payload(item.payload), sort_keys=True, separators=(",", ":"))
                   if item.item_type == "CHAT_ATTACHMENT" else None)
        self.db.execute("INSERT INTO observed VALUES (?,?,?,?,?,?,?,?)", (observation, key, semantic, int(content is not None),
                        item.item_type, item.graph_id, item.parent_graph_id, payload))
        return True

    def fingerprint(self, observation):
        digest = hashlib.sha256()
        prefix = json.dumps([f"chat-source-inventory-v{self.observation_hash_version}", authentication_context_fingerprint(self.scope)], separators=(",", ":"))
        digest.update((prefix[:-1] + ",[").encode())
        separator = b""
        for key, semantic in self.db.execute("SELECT key,semantic FROM observed WHERE pass=? ORDER BY key", (observation,)):
            digest.update(separator)
            digest.update(json.dumps([key, semantic], separators=(",", ":")).encode())
            separator = b","
        digest.update(b"]]")
        return digest.hexdigest()

    def mirror(self, source, target):
        """Copy one pass's observations to another (single-pass reuse of a verified state)."""
        for table in ("observed", "expected"):
            self.db.execute(f"DELETE FROM {table} WHERE pass=?", (target,))
        self.db.execute("INSERT INTO observed SELECT ?, key, semantic, binary, item_type, graph_id, parent_graph_id, file_payload "
                        "FROM observed WHERE pass=?", (target, source))
        self.db.execute("INSERT INTO expected SELECT ?, key, required FROM expected WHERE pass=?", (target, source))

    def changed_files(self, limit=20):
        """The two passes saw the same items and differ only in file attachments (a file edited while
        the chat was read): [(key, graph_id, parent_graph_id, payload, second-pass semantic)], else None."""
        differs = self.db.execute("""SELECT a.key, b.item_type, b.graph_id, b.parent_graph_id, b.file_payload, b.semantic
            FROM observed a JOIN observed b ON b.pass=1 AND b.key=a.key WHERE a.pass=0 AND a.semantic<>b.semantic""").fetchall()
        missing = sum(self.db.execute(query).fetchone()[0] for query in (
            "SELECT COUNT(*) FROM observed a LEFT JOIN observed b ON b.pass=1 AND b.key=a.key WHERE a.pass=0 AND b.key IS NULL",
            "SELECT COUNT(*) FROM observed b LEFT JOIN observed a ON a.pass=0 AND a.key=b.key WHERE b.pass=1 AND a.key IS NULL"))
        if (missing or not differs or len(differs) > limit
                or any(row[1] != "CHAT_ATTACHMENT" or not row[4] for row in differs)):
            return None
        return [(key, graph_id, parent, json.loads(payload), semantic) for key, _, graph_id, parent, payload, semantic in differs]

    def adopt(self, key, semantic):
        """Pass 1's entry takes the version both pass 2 and a fresh re-fetch observed."""
        self.db.execute("UPDATE observed SET semantic=? WHERE pass=0 AND key=?", (semantic, key))

    def item_count(self, observation):
        return self.db.execute("SELECT COUNT(*) FROM observed WHERE pass=?", (observation,)).fetchone()[0]

    def required_count(self, observation):
        return self.db.execute("SELECT COUNT(*) FROM expected WHERE pass=? AND required=1", (observation,)).fetchone()[0]

    def durable_count(self, observation):
        return self.db.execute("SELECT COUNT(*) FROM observed WHERE pass=? AND binary=1", (observation,)).fetchone()[0]

    def failure(self, observation, phase, error, graph_id=None, parent_id=None):
        # Exception text can contain signed URLs. Retain only typed diagnostics.
        status = getattr(error, "status_code", None)
        if type(status) is not int:
            status = None
        self.db.execute("INSERT INTO failures VALUES (?,?,?,?,?,?,?,?,?)",
            (observation, phase, type(error).__name__, status, graph_id, parent_id,
             _identifier(getattr(error, "code", None)), _identifier(getattr(error, "inner_code", None)),
             _identifier(getattr(error, "request_id", None))))
        # ChatGraphError messages are fixed adapter strings (no URLs); other exception text is not logged.
        reason = str(error)[:80] if isinstance(error, ChatGraphError) and status is None else None
        logger.warning("chat_capture_failure observation=%s phase=%s error=%s status=%s code=%s request_id=%s reason=%s",
                       observation, phase, type(error).__name__, status,
                       _identifier(getattr(error, "code", None)), _identifier(getattr(error, "request_id", None)), reason)

    def unavailable(self, observation, item_type, graph_id, parent_graph_id):
        """Microsoft could not serve this listed child's bytes: not a required binary."""
        key = qualified_chat_item_key(self.scope.chat_id, item_type, graph_id, parent_graph_id)
        self.db.execute("UPDATE expected SET required=0 WHERE pass=? AND key=?", (observation, key))

    def unavailable_in_first_pass(self, item_type, graph_id, parent_graph_id):
        key = qualified_chat_item_key(self.scope.chat_id, item_type, graph_id, parent_graph_id)
        return self.db.execute("SELECT 1 FROM expected WHERE pass=0 AND key=? AND required=0", (key,)).fetchone() is not None

    def failure_details(self):
        names = ("observation", "phase", "error_type", "status_code", "graph_id", "parent_graph_id",
                 "provider_code", "provider_inner_code", "provider_request_id")
        return [dict(zip(names, row)) for row in self.db.execute("SELECT * FROM failures LIMIT 50")]

    def difference_details(self):
        queries = {
            "only_first": "SELECT a.key FROM observed a LEFT JOIN observed b ON b.pass=1 AND b.key=a.key WHERE a.pass=0 AND b.key IS NULL",
            "only_second": "SELECT b.key FROM observed b LEFT JOIN observed a ON a.pass=0 AND a.key=b.key WHERE b.pass=1 AND a.key IS NULL",
            "changed": "SELECT a.key FROM observed a JOIN observed b ON b.pass=1 AND b.key=a.key WHERE a.pass=0 AND a.semantic<>b.semantic",
        }
        result = {"observation_hash_version": self.observation_hash_version, "sample_keys": []}
        for kind, query in queries.items():
            result[kind] = self.db.execute("SELECT COUNT(*) FROM (" + query + ")").fetchone()[0]
            remaining = 20 - len(result["sample_keys"])
            result["sample_keys"].extend({"kind": kind, "logical_key": row[0]} for row in
                self.db.execute(query + " LIMIT ?", (remaining,)))
        return result


@dataclass
class _Pass:
    complete: bool = False
    unresolved: int = 0
    conflicts: int = 0
    pages: int = 0
    messages: int = 0
    hosted_lists: int = 0
    duplicate_messages: int = 0
    verified_binary: int = 0
    stage_s: float = 0.0
    hosted_reused: int = 0


class _CaptureLeaseKeeper:
    def __init__(self, factory, lease):
        self.factory, self.lease, self.failure = factory, lease, None

    def check(self):
        if self.failure is not None:
            if isinstance(self.failure, catalogue.ChatCatalogueWorkLeaseMoved):
                raise self.failure
            raise catalogue.ChatCatalogueConflict("capture lease renewal failed") from self.failure

    async def run(self):
        try:
            while True:
                await asyncio.sleep(30)
                async with self.factory.begin() as session:
                    self.lease = await catalogue.renew_capture(session, lease=self.lease)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.failure = exc

    async def guard(self, chunks):
        try:
            self.check()
            async for chunk in chunks:
                self.check()
                yield chunk
            self.check()
        finally:
            close = getattr(chunks, "aclose", None)
            if close:
                await close()


def stable_attachment_identity(attachment):
    """Graph mints a new ``id`` for card/reference attachments on every read, which
    made two identical observations look like source movement. Their identity is
    their content; file attachments keep the Graph id their download needs."""
    if not metadata_only_attachment(attachment):
        return attachment["id"]
    body = {k: v for k, v in attachment.items() if k != "id"}
    return "card:" + hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()[:40]


_ATTACHMENT_REFERENCE = re.compile(r"""(<attachment\s+id\s*=\s*["'])([^"']+)(["'])""", re.IGNORECASE)


def stable_attachment_references(message):
    """Give attachments their content-derived identity and follow it in the body.

    Identities are computed per attachment: Graph can hand two references
    (messageReference + forwardedMessageReference) the same rotating id with
    different content. The body token cannot tell those apart, so it maps to the
    smallest canonical id deterministically, independent of attachment order."""
    native = message.get("attachments")
    if not isinstance(native, list) or not native:
        return message
    if any(not isinstance(a, dict) or not isinstance(a.get("id"), str) or not a["id"] for a in native):
        return message
    stable = [stable_attachment_identity(a) for a in native]
    candidates = {}
    for attachment, new in zip(native, stable):
        if new != attachment["id"]:
            candidates.setdefault(attachment["id"], []).append(new)
    if not candidates:
        return message
    out = {**message, "attachments": [{**a, "id": new} for a, new in zip(native, stable)]}
    mapping = {old: min(news) for old, news in candidates.items()}
    body = message.get("body")
    content = body.get("content") if isinstance(body, dict) else None
    if isinstance(content, str) and content:
        out["body"] = {**body, "content": _ATTACHMENT_REFERENCE.sub(
            lambda m: m.group(1) + mapping.get(m.group(2), m.group(2)) + m.group(3), content)}
    return out


def _file_source(item):
    parent = item.get("parentReference")
    drive, identifier = (parent.get("driveId") if isinstance(parent, dict) else None), item.get("id")
    if not isinstance(drive, str) or not drive or not isinstance(identifier, str) or not identifier:
        raise RuntimeError("required file identity unavailable")
    tags = []
    for field in ("eTag", "cTag"):
        value = item.get(field)
        if value is not None and (not isinstance(value, str) or not value):
            raise RuntimeError("required file version unavailable")
        tags.append(value)
    return dict(source_drive_id=drive, source_item_id=identifier, source_etag=tags[0], source_ctag=tags[1])


async def _stage(factory, keeper, items):
    keeper.check()
    for start in range(0, len(items), 200):
        async def operation(session):
            await catalogue.stage_versions(session, lease=keeper.lease, items=items[start:start + 200])
        await run_catalogue_transaction(factory, operation)


async def _remove_staged_item(factory, keeper, scope, key):
    async def operation(session):
        await catalogue.renew_capture(session, lease=keeper.lease)
        version = await session.scalar(select(ChatInventoryItem.version_id).where(
            ChatInventoryItem.inventory_id == keeper.lease.staged_inventory_id, ChatInventoryItem.logical_key == key))
        await session.execute(delete(SnapshotItem).where(SnapshotItem.snapshot_id == scope.snapshot_id,
            SnapshotItem.external_id == key, SnapshotItem.chat_version_id == version))
        await session.execute(delete(ChatInventoryItem).where(ChatInventoryItem.inventory_id == keeper.lease.staged_inventory_id,
            ChatInventoryItem.logical_key == key))
    await run_catalogue_transaction(factory, operation)


async def _retained(*, scope, keeper, factory, storage, item_type, graph_id, parent_id,
                    payload, source, checksum=None, stage=False):
    key = qualified_chat_item_key(scope.chat_id, item_type, graph_id, parent_id)
    if checksum is None and not (source.get("source_etag") or source.get("source_ctag")):
        return None
    async def find(session):
        # Published PARTIAL inventories hold LIVE, verified objects too; without them a
        # chat with one unavailable file re-uploaded every healthy binary each run.
        complete_candidate = ChatInventory.state.in_(("COMPLETE", "PARTIAL"))
        if checksum is None:
            # Fast tag-only reuse stays in the same observed visibility scope.
            # Cross-context reuse requires this capture's actual source bytes.
            complete_candidate = and_(complete_candidate,
                ChatInventory.authentication_context_sha256 == authentication_context_fingerprint(scope))
        # Earlier attempts of this same snapshot/chat (reconciliation restarts) staged
        # verified LIVE objects; re-uploading them doubled restart cost.
        earlier_attempt = and_(ChatInventory.capture_attempt_id.in_(select(ChatCaptureAttempt.id).where(
                ChatCaptureAttempt.snapshot_id == scope.snapshot_id, ChatCaptureAttempt.chat_id == scope.chat_id)),
            ChatInventory.authentication_context_sha256 == authentication_context_fingerprint(scope))
        retained_membership = exists(select(ChatInventoryItem.logical_key).join(
            ChatInventory, ChatInventory.id == ChatInventoryItem.inventory_id).where(
                ChatInventoryItem.version_id == ChatItemVersion.id,
                or_(ChatInventory.id == keeper.lease.staged_inventory_id, complete_candidate, earlier_attempt)))
        query = select(ChatItemVersion).where(ChatItemVersion.tenant_id == scope.tenant_id,
            ChatItemVersion.chat_id == scope.chat_id, ChatItemVersion.logical_key == key, retained_membership)
        for field, value in source.items():
            # A byte checksum can validate content despite a changed metadata tag.
            if checksum is not None and field in {"source_etag", "source_ctag"}:
                continue
            expression = ChatItemVersion.content[field].as_string()
            query = query.where(expression.is_(None) if value is None else expression == value)
        if checksum is not None:
            query = query.where(ChatItemVersion.content["sha256"].as_string() == checksum)
        version = await session.scalar(query.order_by(ChatItemVersion.created_at.desc()).limit(1))
        if version is None:
            return None
        old = catalogue._captured(version)
        if old.content is None or canonical_version_fingerprint(tenant_id=scope.tenant_id, chat_id=scope.chat_id, item=old) != version.version_sha256:
            raise catalogue.ChatCatalogueIntegrityError("retained captured content identity differs")
        content = replace(old.content, **source)
        if content.backend_id is not None and content.backend_id != scope.backend_id:
            return None
        if content.object_id is not None:
            # GC claims objects FOR UPDATE; a retiring object is not reusable.
            state = await session.scalar(select(ChatBlobObject.state).where(
                ChatBlobObject.id == content.object_id).with_for_update())
            if state != "LIVE":
                return None
        item_payload = dict(payload)
        if not item_payload.get("captured_content_type") and old.payload.get("captured_content_type"):
            item_payload["captured_content_type"] = old.payload["captured_content_type"]
        item = CapturedChatItem(item_type, graph_id, parent_id, item_payload, content)
        if stage:
            # This transaction acquires ownership before external storage probes.
            await catalogue.stage_versions(session, lease=keeper.lease, items=[item])
        return item
    item = await run_catalogue_transaction(factory, find)
    if item is None:
        return None
    try:
        await verify_catalogue_content(storage, item.content)
    except Exception:
        if stage:
            await _remove_staged_item(factory, keeper, scope, key)
        raise
    return item


def _hosted_marker(message):
    """Version tag for hosted content: Graph gives hostedContents no etag, but they only
    change with their message, whose etag/lastModifiedDateTime move on every edit."""
    etag, modified = message.get("etag"), message.get("lastModifiedDateTime")
    if not (isinstance(etag, str) and etag) and not (isinstance(modified, str) and modified):
        return None
    return "chat-message:" + hashlib.sha256(json.dumps([message.get("id"), etag, modified],
                                                        separators=(",", ":")).encode()).hexdigest()


async def _binary(*, scope, keeper, factory, storage, source_client, item_type,
                  graph_id, parent_id, payload, stage, file_versions=None, hosted_marker=None, file_inflight=None,
                  deferred=None):
    file = item_type == "CHAT_ATTACHMENT"
    raw = await source_client.resolve_file(payload.get("contentUrl")) if file else None
    payload = dict(payload)
    if file and isinstance(raw.get("file"), dict) and isinstance(raw["file"].get("mimeType"), str):
        payload["captured_content_type"] = raw["file"]["mimeType"]
    source = _file_source(raw) if file else dict(source_drive_id=None, source_item_id=None, source_etag=hosted_marker, source_ctag=None)
    cache_key = tuple(source.items()) if file and (source['source_etag'] or source['source_ctag']) else None
    # The same exact file version attached to several messages downloads once, also when
    # those messages are captured in parallel: later ones wait for the first, then reuse it.
    owned = None
    if file_inflight is not None and cache_key is not None:
        while cache_key in file_inflight:
            await file_inflight[cache_key].wait()
        if file_versions is None or cache_key not in file_versions:
            owned = file_inflight[cache_key] = asyncio.Event()
    try:
        return await _binary_capture(scope=scope, keeper=keeper, factory=factory, storage=storage,
            source_client=source_client, item_type=item_type, graph_id=graph_id, parent_id=parent_id,
            payload=payload, stage=stage, file_versions=file_versions, file=file, raw=raw, source=source,
            cache_key=cache_key, deferred=deferred)
    finally:
        if owned is not None:
            file_inflight.pop(cache_key, None)
            owned.set()


async def _binary_capture(*, scope, keeper, factory, storage, source_client, item_type, graph_id, parent_id,
                          payload, stage, file_versions, file, raw, source, cache_key, deferred=None):
    # Reused children go to the caller's batch (``deferred``): each staging locks the capture's
    # inventory row, so one transaction per child queued 8 parallel children on it (Railway
    # 2026-11-05: 4.8 s median per child, 5.8 min for one edit in a 1,369-message chat).
    # stage_versions re-checks every object under lock, so a batch never links a retired one.
    defer = stage and deferred is not None
    if file_versions is not None and cache_key in file_versions:
        content, content_type = file_versions[cache_key]
        file_versions.move_to_end(cache_key)
        keeper.check()
        await verify_catalogue_content(storage, content)
        if content_type:
            payload.setdefault('captured_content_type', content_type)
        captured = CapturedChatItem(item_type, graph_id, parent_id, payload, content)
        if defer:
            deferred.append(captured)
        elif stage:
            await _stage(factory, keeper, [captured])
        return captured

    def remember(captured):
        # Only pinned, verified exact file versions; hosted content has no version tag.
        if (file_versions is not None and cache_key is not None and captured.content is not None
                and captured.content.object_id not in (None, UUID(int=0))):
            file_versions[cache_key] = (captured.content, captured.payload.get('captured_content_type'))
            file_versions.move_to_end(cache_key)
            if len(file_versions) > 128:
                file_versions.popitem(last=False)
        return captured

    marks, started = {}, time.monotonic()
    last = [started]
    def mark(name):
        now = time.monotonic(); marks[name] = round((now - last[0]) * 1000); last[0] = now
    def report(item):
        total = round((time.monotonic() - started) * 1000)
        if total >= 1000:
            logger.info("chat_binary_timing attempt_id=%s type=%s size=%s total_ms=%s %s", keeper.lease.attempt_id, item_type,
                        item.content.size if item.content else None, total, " ".join(f"{k}_ms={v}" for k, v in marks.items()))
        return remember(item)
    retained = await _retained(scope=scope, keeper=keeper, factory=factory, storage=storage,
        item_type=item_type, graph_id=graph_id, parent_id=parent_id, payload=payload, source=source,
        stage=stage and not defer)
    mark("retained_lookup")
    if retained is not None:
        if defer:
            deferred.append(retained)
        return report(retained)
    opened = source_client.open_file(raw) if file else source_client.open_hosted(scope.chat_id, parent_id, graph_id)
    async with AsyncExitStack() as spools:
        async with opened as (chunks, expected_size, content_type):
            if isinstance(content_type, str) and content_type:
                payload.setdefault("captured_content_type", content_type)
            spooled = await spools.enter_async_context(
                spool_chat_content(keeper.guard(chunks), expected_size=expected_size))
        mark("download")
        # Verified EOF closes the provider response before refresh/upload work.
        if file:
            refreshed = await source_client.refresh_file(source["source_drive_id"], source["source_item_id"])
            if _file_source(refreshed) != source:
                raise ChatSourceMovement("required file changed during download")
            mark("refresh")
        retained = await _retained(scope=scope, keeper=keeper, factory=factory, storage=storage,
            item_type=item_type, graph_id=graph_id, parent_id=parent_id, payload=payload,
            source=source, checksum=spooled.sha256, stage=stage and not defer)
        mark("retained_checksum")
        if retained is not None:
            if defer:
                deferred.append(retained)
            return report(retained)
        if stage:
            content = await upload_capture_content(scope=scope, lease=keeper.lease,
                spooled=spooled, storage=storage, session_factory=factory, **source)
            mark("upload")
        else:
            # Reconciliation hashes semantic bytes; this location is never
            # persisted, uploaded or used for reads.
            content = ChatContentDescriptor(UUID(int=0), scope.backend_id, "observation", "observation",
                spooled.sha256, spooled.size, None, 1, None, **source)
        item = CapturedChatItem(item_type, graph_id, parent_id, payload, content)
        if stage:
            await _stage(factory, keeper, [item])
            mark("stage")
        return report(item)

def _source_unavailable(exc) -> bool:
    """Microsoft named the item but cannot serve its bytes: 404/410 gone, 423 the owning site
    is locked by a tenant admin ("Access to this site has been blocked"), or a 500 that
    survived the request retries (Teams /views/video clips answer 500 every time). 401/403
    (our permissions), 429 and 502-504 (throttling/outage), transport and storage failures
    stay unresolved, so they keep the chat PARTIAL. A file held by another tenant is that
    tenant's content (like an external-tenant chat), not a gap in our permissions."""
    return isinstance(exc, ChatGraphError) and (getattr(exc, "status_code", None) in (404, 410, 423, 500)
                                                or getattr(exc, "code", None) == EXTERNAL_TENANT_FILE)


def _without_home_tenant(message: dict, home_tenant) -> dict:
    """Graph reports a same-tenant reacting/mentioned user's tenantId as the home tenant on one
    read and omits it on the next, and swaps a reaction's displayName label between wordings.
    Store neither, so those flips are not new message versions. Any other tenant's id is real
    cross-tenant identity and is kept; exports render reactions from reactionType."""
    home = home_tenant.lower() if isinstance(home_tenant, str) and home_tenant else None
    for field, path in (("reactions", ("user", "user")), ("mentions", ("mentioned", "user"))):
        entries = message.get(field)
        if not isinstance(entries, list):
            continue
        entries = copy.deepcopy(entries)
        for entry in entries:
            if field == "reactions" and isinstance(entry, dict):
                entry.pop("displayName", None)
            identity = entry
            for key in path:
                identity = identity.get(key) if isinstance(identity, dict) else None
            if isinstance(identity, dict) and "tenantId" in identity:
                tenant = identity["tenantId"]
                if tenant is None or (home is not None and str(tenant).lower() == home):
                    del identity["tenantId"]
        message[field] = entries
    return message


_STOPS_CAPTURE = (asyncio.CancelledError, ChatSourceMovement, catalogue.ChatCatalogueConflict,
                  catalogue.ChatCatalogueIntegrityError, SQLAlchemyError)


class _BinaryPool:
    """A chat's files and hosted contents captured at most `limit` at a time (Railway 2026-11-04:
    one after another, 101 binaries took ~1,091 s of a 1,374 s capture). Bytes spool to disk,
    so memory stays bounded by the limit. An error that stops a capture cancels the rest and
    reaches the pass unchanged; staging still serialises on the attempt row lock."""

    def __init__(self, limit):
        self.slots, self.tasks = asyncio.Semaphore(max(1, limit)), set()

    def _raise_finished(self):
        for task in [t for t in self.tasks if t.done()]:
            self.tasks.discard(task)
            if task.cancelled():
                raise asyncio.CancelledError()
            if task.exception() is not None:
                raise task.exception()

    async def submit(self, coro):
        try:
            self._raise_finished()
            await self.slots.acquire()
            self._raise_finished()
        except BaseException:
            coro.close()
            raise

        async def run():
            try:
                await coro
            finally:
                self.slots.release()
        self.tasks.add(asyncio.create_task(run()))

    async def drain(self):
        while self.tasks:
            await asyncio.wait(self.tasks, return_when=asyncio.FIRST_COMPLETED)
            self._raise_finished()

    async def cancel(self):
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks.clear()


async def _observe(*, scope, keeper, factory, storage, source, ledger, observation,
                   exclude_message=None, folder_path=None):
    stats = _Pass()
    file_versions, file_inflight = OrderedDict(), {}
    home_tenant = getattr(source, "home_tenant_id", None)
    # ponytail: per-chat cap only; a worker can hold CHAT_CAPTURE_CONCURRENCY x this many files in flight
    # (8 x 8 = 64 on Railway, streamed via disk). Add a process-wide cap if disk or memory gets tight.
    pool = _BinaryPool(int(os.getenv("CHAT_BINARY_CONCURRENCY", "8")))
    pending = []

    async def flush(at_least=1):
        if len(pending) >= at_least:
            batch = pending[:]
            del pending[:]
            await _stage(factory, keeper, batch)

    async def capture_binary(kind, child, identifier, marker):
        try:
            captured = await _binary(scope=scope, keeper=keeper, factory=factory, storage=storage,
                source_client=source, item_type=kind, graph_id=child["id"], parent_id=identifier,
                payload=child, stage=observation == 0, file_versions=file_versions,
                hosted_marker=marker if kind == "CHAT_HOSTED_CONTENT" else None, file_inflight=file_inflight,
                deferred=pending)
            if ledger.record(observation, captured):
                stats.verified_binary += 1
        except _STOPS_CAPTURE:
            raise
        except Exception as exc:
            ledger.failure(observation, kind, exc, child["id"], identifier)
            if _source_unavailable(exc):
                ledger.unavailable(observation, kind, child["id"], identifier)
                captured = CapturedChatItem(kind, child["id"], identifier,
                                            {**child, SOURCE_UNAVAILABLE_FIELD: True}, None)
                if ledger.record(observation, captured) and observation == 0:
                    pending.append(captured)
            else:
                stats.unresolved += 1

    try:
        await _observe_pages(scope=scope, keeper=keeper, factory=factory, source=source, ledger=ledger,
            observation=observation, exclude_message=exclude_message, folder_path=folder_path,
            stats=stats, home_tenant=home_tenant, pool=pool, pending=pending, flush=flush,
            capture_binary=capture_binary)
    finally:
        await pool.cancel()
    return stats


async def _prefetched(pages):
    """One page of lookahead: the next Graph page is fetched while this one is staged and verified
    (pass 1 of a 3,955-message chat waited on staging before asking for each next page)."""
    iterator = pages.__aiter__()
    following = asyncio.ensure_future(iterator.__anext__())
    try:
        while True:
            try:
                page = await following
            except StopAsyncIteration:
                return
            following = asyncio.ensure_future(iterator.__anext__())
            yield page
    finally:
        if not following.done():
            following.cancel()
            await asyncio.wait([following])
        if not following.cancelled():
            following.exception()   # a lookahead failure after the consumer stopped is not raised
        close = getattr(iterator, "aclose", None)
        if close is not None:
            with suppress(Exception):
                await close()


async def _known_hosted(factory, scope, messages):
    """Stored hosted-content lists of published message versions identical to these Graph messages (same
    id, etag and lastModifiedDateTime, children complete). Hosted images only change with their message,
    so these need no hostedContents request (a 3,955-message chat listed 77 per pass at 1 request/s)."""
    wanted = {m["id"]: (m.get("etag"), m.get("lastModifiedDateTime")) for m in messages
              if isinstance(m.get("id"), str) and m.get("etag") and m.get("lastModifiedDateTime")}
    if not wanted:
        return {}

    async def find(session):
        return (await session.execute(select(ChatItemVersion.graph_id, ChatItemVersion.payload).where(
            ChatItemVersion.tenant_id == scope.tenant_id, ChatItemVersion.chat_id == scope.chat_id,
            ChatItemVersion.item_type == "TEAMS_CHAT_MESSAGE",
            func.md5(ChatItemVersion.graph_id).in_([hashlib.md5(i.encode()).hexdigest() for i in wanted]),
            ChatItemVersion.graph_id.in_(list(wanted)),
            # Only lists an earlier published backup verified: pass 2 must still observe what pass 1 staged.
            exists(select(ChatInventoryItem.version_id).join(ChatInventory, ChatInventory.id == ChatInventoryItem.inventory_id)
                   .where(ChatInventoryItem.version_id == ChatItemVersion.id, ChatInventory.state.in_(("COMPLETE", "PARTIAL")))))
            .order_by(ChatItemVersion.created_at.desc()))).all()
    found = {}
    for graph_id, payload in await run_catalogue_transaction(factory, find):
        if graph_id in found or not isinstance(payload, dict):
            continue
        hosted = payload.get("hostedContents")
        complete = (payload.get("chat_catalogue_expected_children") or {}).get("complete") is True
        if complete and isinstance(hosted, list) and (payload.get("etag"), payload.get("lastModifiedDateTime")) == wanted[graph_id]:
            found[graph_id] = hosted
    return found


async def _known_unavailable(factory, scope) -> set:
    """(graph id, parent id) of this chat's hosted contents already recorded unavailable (byte-less).
    That is final: a re-read records them unavailable again without a request (Railway 2026-11-06:
    every re-read of a 1:1 chat fetched a call recording's /views/video, which Microsoft never serves;
    the recording itself is backed up from the organizer's OneDrive). A new hosted id is fetched."""
    async def find(session):
        return set((await session.execute(select(ChatItemVersion.graph_id, ChatItemVersion.parent_graph_id).where(
            ChatItemVersion.tenant_id == scope.tenant_id, ChatItemVersion.chat_id == scope.chat_id,
            ChatItemVersion.item_type == "CHAT_HOSTED_CONTENT", ChatItemVersion.object_id.is_(None),
            func.cast(ChatItemVersion.payload, JSONB)[SOURCE_UNAVAILABLE_FIELD].astext == "true"))).all())
    return await run_catalogue_transaction(factory, find)


async def _observe_pages(*, scope, keeper, factory, source, ledger, observation, exclude_message, folder_path,
                         stats, home_tenant, pool, pending, flush, capture_binary):
    gone = None   # read once, at the chat's first hosted content
    try:
        async for page in _prefetched(source.iter_message_pages(scope.chat_id)):
            keeper.check()
            prepared, parents = [], []
            known = await _known_hosted(factory, scope, [raw for raw in page if isinstance(raw, dict)
                                                           and message_references_hosted_content(raw)])
            try:
                for raw in page:
                    parent = _without_home_tenant(stable_attachment_references(dict(raw)), home_tenant)
                    if exclude_message is not None and exclude_message({**parent, "_chat_folder_path": folder_path}):
                        continue
                    if not ledger.observe_message(observation, raw):
                        stats.duplicate_messages += 1
                        continue
                    identifier = parent.get("id")
                    native = parent.get("attachments") or []
                    valid = isinstance(native, list) and all(isinstance(a, dict) and isinstance(a.get("id"), str) and a["id"] for a in native)
                    attachments = native if valid else []
                    if not valid:
                        stats.unresolved += 1
                    hosted, hosted_complete = [], True
                    if identifier in known:
                        hosted = copy.deepcopy(known[identifier])
                        stats.hosted_reused += 1
                    elif message_references_hosted_content(raw):
                        try:
                            stats.hosted_lists += 1
                            async for child in source.iter_hosted_contents(scope.chat_id, identifier):
                                hosted.append(child)
                        except asyncio.CancelledError:
                            raise
                        except Exception as exc:
                            ledger.failure(observation, "hosted_list", exc, identifier)
                            hosted_complete, stats.unresolved = False, stats.unresolved + 1
                    parent["hostedContents"] = hosted
                    manifest = []
                    for kind, values in (("CHAT_ATTACHMENT", attachments), ("CHAT_HOSTED_CONTENT", hosted)):
                        for child in values:
                            required = kind == "CHAT_HOSTED_CONTENT" or not metadata_only_attachment(child)
                            manifest.append({"item_type": kind, "graph_id": child["id"], "requires_content": required})
                            ledger.expect(observation, kind, child["id"], identifier, required=required)
                    parent["chat_catalogue_expected_children"] = {"complete": hosted_complete and valid, "items": manifest}
                    parent_item = CapturedChatItem("TEAMS_CHAT_MESSAGE", identifier, None, parent, None)
                    if ledger.record(observation, parent_item) and observation == 0:
                        parents.append(parent_item)
                    prepared.append((identifier, attachments, hosted, _hosted_marker(parent)))
                    stats.messages += 1
            finally:
                # Parent rows must already exist before any child upload/link;
                # interruption then leaves a readable partial message inventory.
                # Earlier pages' reused children ride along: one staging per page.
                batch = parents + pending[:]
                del pending[:]
                if batch:
                    began = time.monotonic()
                    await _stage(factory, keeper, batch)
                    stats.stage_s += time.monotonic() - began
            for identifier, attachments, hosted, marker in prepared:
                for kind, values in (("CHAT_ATTACHMENT", attachments), ("CHAT_HOSTED_CONTENT", hosted)):
                    for child in values:
                        if kind == "CHAT_HOSTED_CONTENT" and observation == 0 and gone is None:
                            gone = await _known_unavailable(factory, scope)
                        if kind == "CHAT_ATTACHMENT" and metadata_only_attachment(child):
                            captured = CapturedChatItem(kind, child["id"], identifier, child, None)
                            if ledger.record(observation, captured) and observation == 0:
                                pending.append(captured)
                        elif ((observation and ledger.unavailable_in_first_pass(kind, child["id"], identifier))
                              or (kind == "CHAT_HOSTED_CONTENT" and (child["id"], identifier) in (gone or ()))):
                            # Pass 2 re-observes membership; bytes Microsoft could not serve in
                            # pass 1 (or in an earlier capture, for hosted content) are not fetched again.
                            ledger.unavailable(observation, kind, child["id"], identifier)
                            captured = CapturedChatItem(kind, child["id"], identifier,
                                                        {**child, SOURCE_UNAVAILABLE_FIELD: True}, None)
                            if ledger.record(observation, captured) and observation == 0:
                                pending.append(captured)
                        else:
                            await pool.submit(capture_binary(kind, child, identifier, marker))
                        await flush(200)
            stats.pages += 1
            logger.info(
                "chat_capture_progress attempt_id=%s observation=%s pages=%s messages_observed=%s "
                "hosted_enumerations=%s duplicate_messages_skipped=%s verified_binary=%s unresolved=%s stage_s=%.1f hosted_reused=%s",
                keeper.lease.attempt_id, observation + 1, stats.pages, stats.messages,
                stats.hosted_lists, stats.duplicate_messages, stats.verified_binary, stats.unresolved, stats.stage_s, stats.hosted_reused)
        await pool.drain()
        await flush()
        stats.complete = True
    except _STOPS_CAPTURE:
        raise
    except Exception as exc:
        ledger.failure(observation, "message_enumeration", exc)
        stats.unresolved += 1
        # Children already started still land (as before, when they ran inline).
        await pool.drain()
        await flush()


def _single_pass_enabled():
    return os.getenv("CHAT_SINGLE_PASS_UNCHANGED", "true").strip().lower() not in ("0", "false", "no", "off")


async def _verified_state(factory, scope, fingerprint, observation_hash_version):
    async def find(session):
        return await session.scalar(select(ChatInventory.id).where(
            ChatInventory.tenant_id == scope.tenant_id, ChatInventory.chat_id == scope.chat_id,
            ChatInventory.authentication_context_sha256 == authentication_context_fingerprint(scope),
            ChatInventory.observation_hash_version == observation_hash_version,
            ChatInventory.membership_sha256 == fingerprint, ChatInventory.state == "COMPLETE").limit(1))
    return await run_catalogue_transaction(factory, find) is not None


async def _discard_attempt(session, lease, scope, reason):
    """Drop an unpublished attempt's snapshot links so a fresh attempt can stage the chat."""
    if await session.get(SnapshotChatInventory, (scope.snapshot_id, scope.chat_id)) is not None:
        raise catalogue.ChatCatalogueConflict("terminal inventory cannot restart")
    matching = exists(select(ChatInventoryItem.logical_key).where(
        ChatInventoryItem.inventory_id == lease.staged_inventory_id,
        ChatInventoryItem.logical_key == SnapshotItem.external_id,
        ChatInventoryItem.version_id == SnapshotItem.chat_version_id))
    await session.execute(delete(SnapshotItem).where(SnapshotItem.snapshot_id == scope.snapshot_id, matching))
    attempt = await session.get(ChatCaptureAttempt, lease.attempt_id)
    attempt.state, attempt.failure_reason = "FAILED", reason


async def _restart(factory, keeper, scope):
    async def operation(session):
        await catalogue.renew_capture(session, lease=keeper.lease)
        await _discard_attempt(session, keeper.lease, scope, "source changed during bounded reconciliation")
    await run_catalogue_transaction(factory, operation)


SHARED_CAPTURE_BUSY = object()


def _recorded_filter(attempt):
    for entry in attempt.evidence_history or []:
        if isinstance(entry, dict) and "source_filter_sha256" in entry:
            return entry["source_filter_sha256"]
    return None


def _when(value):
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _observed_times(attempt):
    """(when the attempt's data was read from Microsoft, when the last full capture of it ran).
    A copy keeps the original reading time: changes after it must still be seen."""
    for entry in attempt.evidence_history or []:
        if isinstance(entry, dict) and entry.get("source_observed_at"):
            observed = _when(entry["source_observed_at"])
            return observed, _when(entry.get("full_observed_at")) or observed
    started = _when((attempt.evidence or {}).get("started_at")) if attempt.evidence else None
    return started, started


def _inventory_items(session, inventory_id):
    return session.scalars(select(ChatItemVersion).join(ChatInventoryItem,
        ChatInventoryItem.version_id == ChatItemVersion.id).where(ChatInventoryItem.inventory_id == inventory_id))


async def _publish_copy(*, scope, session_factory, items, folder_path, exclude_message, scope_filter_hash, provenance, label,
                        source=None):
    """Publish this member's inventory from already-captured items (another member's capture or
    its own unchanged one), re-applying its visibility. The normal staging locks every object and
    rejects retired ones; the catalogue's own validation is checked before publishing. Any
    mismatch discards the attempt and returns None so the caller captures from Graph. ``source``
    (inventory id, snapshot id) of a publication holding these items lets the database copy them
    instead of re-staging every row; staging remains the fallback."""
    from shared.config import settings
    version = settings.CHAT_OBSERVATION_HASH_VERSION
    kept, selected = set(), []
    for item in items:
        if item.item_type in catalogue._MESSAGE_TYPES:
            if exclude_message is not None and exclude_message({**item.payload, "_chat_folder_path": folder_path}):
                continue
            kept.add(item.graph_id)
            selected.append(item)
    # Parents before children: a child link needs its parent's staged row.
    selected += [item for item in items if item.item_type not in catalogue._MESSAGE_TYPES
                 and (item.parent_graph_id is None or item.parent_graph_id in kept)]
    started = datetime.now(timezone.utc)

    async def begin(session):
        lease = await catalogue.begin_capture(session, scope=scope, folder_path=folder_path,
                                              observation_hash_version=version)
        attempt = await session.get(ChatCaptureAttempt, lease.attempt_id)
        attempt.evidence_history = [*([{"source_filter_sha256": scope_filter_hash}] if scope_filter_hash is not None else []),
                                    provenance]
        return lease

    lease = await run_catalogue_transaction(session_factory, begin)
    try:
        copied = False
        if source is not None:
            async def bulk(session):
                nonlocal lease
                lease = await catalogue.renew_capture(session, lease=lease)
                return await catalogue.copy_versions(session, lease=lease, inventory_id=source[0], source_snapshot_id=source[1],
                    keys=[qualified_chat_item_key(scope.chat_id, item.item_type, item.graph_id, item.parent_graph_id) for item in selected])
            copied = await run_catalogue_transaction(session_factory, bulk)
        for start in range(0, 0 if copied else len(selected), 200):
            async def stage(session, batch=selected[start:start + 200]):
                nonlocal lease
                lease = await catalogue.renew_capture(session, lease=lease)
                await catalogue.stage_versions(session, lease=lease, items=batch)
            await run_catalogue_transaction(session_factory, stage)
    except catalogue.ChatCatalogueIntegrityError:
        return await _reject(scope, session_factory, lease, label, provenance)
    return await _publish_staged(scope=scope, session_factory=session_factory, lease=lease, items=selected,
                                 started=started, label=label, provenance=provenance)


async def _reject(scope, session_factory, lease, label, provenance):
    async def discard(session):
        await _discard_attempt(session, lease, scope, f"{label} rejected")
    await run_catalogue_transaction(session_factory, discard)
    logger.warning("%s_rejected chat=%s provenance=%s", label, scope.chat_id, provenance)
    return None


async def _publish_staged(*, scope, session_factory, lease, items, started, label, provenance):
    """Publish a capture whose ``items`` are staged, when the catalogue's own validation of the staged
    rows (fingerprint, child coverage, required/durable content) agrees; else discard it (None)."""
    version = lease.observation_hash_version
    required = sum(1 for item in items if item.item_type in ("CHAT_ATTACHMENT", "CHAT_HOSTED_CONTENT")
                   and (item.item_type == "CHAT_HOSTED_CONTENT" or not metadata_only_attachment(item.payload))
                   and item.payload.get(SOURCE_UNAVAILABLE_FIELD) is not True)
    durable = sum(1 for item in items if item.content is not None)
    fingerprint = source_inventory_fingerprint(scope=scope, items=items, observation_hash_version=version)
    evidence = CaptureEvidence(True, True, fingerprint, fingerprint, 0, required, durable, 0,
                               started, datetime.now(timezone.utc), observation_hash_version=version)

    async def finish(session):
        attempt, _ = await catalogue._attempt(session, lease)
        staged = await catalogue._staged(session, await session.get(ChatInventory, attempt.staged_inventory_id), scope)
        if staged.gaps or staged.source_hash != fingerprint or (staged.required, staged.durable) != (required, durable):
            return None
        await catalogue.record_capture_evidence(session, lease=lease, evidence=evidence)
        return await catalogue.publish_inventory(session, lease=lease)

    try:
        result = await run_catalogue_transaction(session_factory, finish)
    except catalogue.ChatCatalogueIntegrityError:
        result = None
    if result is None:
        return await _reject(scope, session_factory, lease, label, provenance)
    logger.info("%s attempt_id=%s chat=%s items=%s", label, lease.attempt_id, scope.chat_id, len(items))
    return result


async def derive_shared_chat(*, scope, session_factory, covers, folder_path=None,
                             exclude_message=None, scope_filter_hash=None):
    """Build this member's inventory from another member's COMPLETE capture of the chat whose
    data was read after this member's backup run started (a copy counts by its original read
    time), so a run of any length shares its captures and never borrows an earlier run's.

    Messages, hosted contents and files are read through app-level routes, so every member's
    capture of a chat sees the same source; only the visible-history boundary differs, and it
    is re-applied here. ``covers(principal_id, recorded_filter_hash)`` says whether that
    member's capture holds everything this member sees. Returns SHARED_CAPTURE_BUSY while a
    covering member is capturing the chat, None when there is no usable donor (the caller
    captures from Graph), else the published CaptureResult.
    """
    from shared.config import settings
    from shared.models import Job, Snapshot
    version = settings.CHAT_OBSERVATION_HASH_VERSION

    async def find(session):
        snapshot = await session.get(Snapshot, scope.snapshot_id)
        job = await session.get(Job, snapshot.job_id) if snapshot.job_id else None
        started = _when((job.created_at if job else None) or snapshot.started_at)
        now = await session.scalar(select(func.clock_timestamp()))
        rows = (await session.execute(select(ChatCaptureAttempt, ChatInventory).outerjoin(
            ChatInventory, ChatInventory.id == ChatCaptureAttempt.staged_inventory_id).where(
            ChatCaptureAttempt.tenant_id == scope.tenant_id, ChatCaptureAttempt.chat_id == scope.chat_id,
            ChatCaptureAttempt.principal_id != scope.principal_id,
            ChatCaptureAttempt.created_at >= started)
            .order_by(ChatCaptureAttempt.created_at.desc()))).all()
        busy = False
        for attempt, inventory in rows:
            if not covers(attempt.principal_id, _recorded_filter(attempt)):
                continue
            observed, full = _observed_times(attempt)
            if (attempt.state == "PUBLISHED" and inventory is not None and inventory.state == "COMPLETE"
                    and inventory.observation_hash_version == version and observed and observed >= started):
                return attempt, observed, full, [catalogue._captured(v) for v in (await _inventory_items(session, inventory.id)).all()]
            busy = busy or (attempt.state in catalogue._ACTIVE and attempt.expires_at > now)
        return SHARED_CAPTURE_BUSY if busy else None

    found = await run_catalogue_transaction(session_factory, find)
    if found is None or found is SHARED_CAPTURE_BUSY:
        return found
    donor, observed, full, items = found
    return await _publish_copy(scope=scope, session_factory=session_factory, items=items, folder_path=folder_path,
        exclude_message=exclude_message, scope_filter_hash=scope_filter_hash, label="chat_shared_capture_derived",
        source=(donor.staged_inventory_id, donor.snapshot_id), provenance={"derived_from_attempt": str(donor.id), "derived_from_principal": donor.principal_id,
                    "source_observed_at": observed.isoformat() if observed else None,
                    "full_observed_at": full.isoformat() if full else None})


async def _changes_already_read(session_factory, candidates) -> set:
    """``candidates``: chat id -> (inventory id, newest message change). The chats whose inventory already
    holds a message version modified exactly then. Callers ask only for changes made before that read
    started, inside the safety margin: nothing is newer, so that read saw the chat as it is now.
    Railway 2026-11-05 #8: an edit 4 minutes before a read re-read the 3,955-message chat next run."""
    if not candidates:
        return set()
    chats = list(candidates)

    async def find(session):
        return set((await session.scalars(text("""
            SELECT c.chat FROM unnest(CAST(:chats AS text[]), CAST(:inventories AS uuid[]), CAST(:changes AS timestamptz[]))
                AS c(chat, inventory, changed)
            WHERE EXISTS (SELECT 1 FROM chat_inventory_items ii JOIN chat_item_versions v ON v.id = ii.version_id
                WHERE ii.inventory_id = c.inventory AND v.item_type = ANY(CAST(:types AS text[]))
                  AND CASE WHEN v.payload::jsonb->>'lastModifiedDateTime' ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T'
                           THEN (v.payload::jsonb->>'lastModifiedDateTime')::timestamptz END = c.changed)"""),
            {"chats": chats, "inventories": [str(candidates[c][0]) for c in chats],
             "changes": [candidates[c][1] for c in chats], "types": list(catalogue._MESSAGE_TYPES)})).all())
    return await run_catalogue_transaction(session_factory, find)


async def reuse_unchanged_chat(*, scope, graph_client, session_factory, folder_path=None,
                               exclude_message=None, scope_filter_hash=None):
    """Incremental: a chat with no message modified since this member's last COMPLETE capture of
    the same scope reuses that capture instead of re-reading every page (one Graph request).

    Kill switch CHAT_INCREMENTAL_REUSE=false (on by default). A full capture still runs when the
    chat changed, when the scope (principal, permissions, visibility filter) differs, or when the
    last full capture is older than CHAT_INCREMENTAL_RECONCILE_HOURS (default 168: a daily SLA still reuses; weekly full read), which also
    covers changes a modification time may not reflect (e.g. linked-file edits). Returns None
    whenever the caller should capture from Graph.
    """
    if os.getenv("CHAT_INCREMENTAL_REUSE", "true").strip().lower() not in ("1", "true", "yes", "on"):
        return None
    from shared.config import settings
    version = settings.CHAT_OBSERVATION_HASH_VERSION
    reconcile = timedelta(hours=float(os.getenv("CHAT_INCREMENTAL_RECONCILE_HOURS", "168")))
    overlap = timedelta(minutes=float(os.getenv("CHAT_INCREMENTAL_OVERLAP_MIN", "15")))

    async def find(session):
        row = (await session.execute(select(ChatCaptureAttempt, ChatInventory).join(
            ChatInventory, ChatInventory.id == ChatCaptureAttempt.staged_inventory_id).where(
            ChatCaptureAttempt.tenant_id == scope.tenant_id, ChatCaptureAttempt.chat_id == scope.chat_id,
            ChatCaptureAttempt.principal_id == scope.principal_id, ChatCaptureAttempt.state == "PUBLISHED",
            ChatCaptureAttempt.snapshot_id != scope.snapshot_id, ChatInventory.state == "COMPLETE",
            ChatInventory.authentication_context_sha256 == authentication_context_fingerprint(scope),
            ChatInventory.observation_hash_version == version)
            .order_by(ChatCaptureAttempt.created_at.desc()).limit(1))).first()
        if row is None:
            return None
        attempt, inventory = row
        observed, full = _observed_times(attempt)
        return attempt.id, inventory.id, observed, full, attempt.snapshot_id, attempt.evidence

    found = await run_catalogue_transaction(session_factory, find)
    if found is None:
        return None
    attempt_id, inventory_id, observed, full, source_snapshot_id, source_evidence = found
    if observed is None or full is None or datetime.now(timezone.utc) - full >= reconcile:
        return None
    try:
        async with _source(graph_client, scope.principal_id) as source:
            latest = await source.latest_message_change(scope.chat_id)
    except ChatGraphError as exc:
        logger.warning("chat_incremental_check_failed chat=%s status=%s", scope.chat_id, getattr(exc, "status_code", None))
        return None
    if latest is not None and latest > observed - overlap and not (
            latest <= observed and await _changes_already_read(session_factory, {scope.chat_id: (inventory_id, latest)})):
        return None
    provenance = {"reused_unchanged_from_attempt": str(attempt_id), "source_observed_at": observed.isoformat(),
                  "full_observed_at": full.isoformat(), "latest_message_change": latest.isoformat() if latest else None}
    linked = await _link_unchanged(scope=scope, session_factory=session_factory, folder_path=folder_path,
        inventory_id=inventory_id, source_snapshot_id=source_snapshot_id, source_evidence=source_evidence,
        history=[*([{"source_filter_sha256": scope_filter_hash}] if scope_filter_hash is not None else []), provenance])
    if linked is not None:
        return linked

    async def load(session):
        return [catalogue._captured(v) for v in (await _inventory_items(session, inventory_id)).all()]

    items = await run_catalogue_transaction(session_factory, load)
    return await _publish_copy(scope=scope, session_factory=session_factory, items=items, folder_path=folder_path,
        exclude_message=exclude_message, scope_filter_hash=scope_filter_hash, label="chat_incremental_reused",
        provenance=provenance)


def _changed_only_enabled():
    return os.getenv("CHAT_CHANGED_ONLY_READ", "true").strip().lower() not in ("0", "false", "no", "off")


class _FeedMessages:
    """The source, except that the chat's messages are the delta feed's records (pages of 50):
    hosted contents and files of those messages go through the normal per-message capture."""

    def __init__(self, source, messages):
        self._source, self._messages = source, messages

    def __getattr__(self, name):
        return getattr(self._source, name)

    async def iter_message_pages(self, chat_id):
        for start in range(0, len(self._messages), 50):
            yield copy.deepcopy(self._messages[start:start + 50])


def _feed_messages(chat_id, records):
    """{message id: newest record} when every record is a plain message of this chat that a full read
    would list the same way; else (None, reason). Replies, system events (member changes...) and
    @removed tombstones cannot be placed: a full read decides what they mean."""
    from shared.chat_catalogue_entry import _graph_time
    newest = {}
    for record in records or ():
        if not isinstance(record, dict) or "@removed" in record:
            return None, "removed_record"
        identifier, modified = record.get("id"), _graph_time(record.get("lastModifiedDateTime"))
        if record.get("chatId") != chat_id or not isinstance(identifier, str) or not identifier or modified is None:
            return None, "unplaced_record"
        if record.get("replyToId") or record.get("messageType", "message") != "message":
            return None, "reply_or_system_event"
        if identifier not in newest or modified >= newest[identifier][0]:
            newest[identifier] = (modified, record)
    if not newest:
        return None, "no_feed_records"
    return newest, None


async def capture_changed_only(*, scope, source, records, since_snapshot_id, session_factory, storage,
                               folder_path=None, exclude_message=None, scope_filter_hash=None):
    """A chat the delta feed reports changed, rebuilt from this member's COMPLETE capture published in
    ``since_snapshot_id`` (the backup whose run started the feed's cursor) with the feed's records
    (full chatMessage payloads) in place of the messages they name: only those messages' hosted
    contents and files are read, every other message keeps its captured version (a 1,369-message
    chat re-read ~28 pages for one edit). Kill switch CHAT_CHANGED_ONLY_READ=false.

    None (the caller reads the whole chat) on any doubt: records it cannot place, no prior capture
    there with the same scope/principal/authentication/observation version, the weekly
    CHAT_INCREMENTAL_RECONCILE_HOURS full read due, the one-request check showing a change newer than
    the newest record (the feed alone once missed an edit, Railway 2026-11-05), or a capture that
    does not verify complete."""
    def fallback(reason):
        logger.info("chat_changed_only_fallback chat=%s reason=%s", scope.chat_id, reason)
        return None
    if not _changed_only_enabled():
        return None
    if records is None:
        return fallback("feed_records_unavailable")
    newest, reason = _feed_messages(scope.chat_id, records)
    if newest is None:
        return fallback(reason)
    from shared.config import settings
    from shared.snapshot_reuse import items_snapshot_id
    version = settings.CHAT_OBSERVATION_HASH_VERSION
    reconcile = timedelta(hours=float(os.getenv("CHAT_INCREMENTAL_RECONCILE_HOURS", "168")))

    async def find(session):
        snapshot = await items_snapshot_id(session, since_snapshot_id)
        row = (await session.execute(select(ChatCaptureAttempt, ChatInventory)
            .join(ChatInventory, ChatInventory.id == ChatCaptureAttempt.staged_inventory_id)
            .join(SnapshotChatInventory, and_(SnapshotChatInventory.snapshot_id == ChatCaptureAttempt.snapshot_id,
                                              SnapshotChatInventory.capture_attempt_id == ChatCaptureAttempt.id))
            .where(ChatCaptureAttempt.snapshot_id == snapshot, ChatCaptureAttempt.chat_id == scope.chat_id,
                   ChatCaptureAttempt.state == "PUBLISHED", ChatInventory.state == "COMPLETE",
                   SnapshotChatInventory.coverage_status == "COMPLETE"))).first()
        if row is None:
            return None
        attempt, inventory = row
        return attempt, inventory, [catalogue._captured(v) for v in (await _inventory_items(session, inventory.id)).all()]

    found = await run_catalogue_transaction(session_factory, find) if since_snapshot_id else None
    if found is None:
        return fallback("no_prior_complete_capture")
    attempt, inventory, prior = found
    observed, full = _observed_times(attempt)
    if (attempt.principal_id != scope.principal_id or inventory.observation_hash_version != version
            or inventory.authentication_context_sha256 != authentication_context_fingerprint(scope)
            or _recorded_filter(attempt) != scope_filter_hash):
        return fallback("scope_changed")
    if observed is None or full is None or datetime.now(timezone.utc) - full >= reconcile:
        return fallback("reconcile_due")
    checked_at = datetime.now(timezone.utc)
    try:
        latest = await source.latest_message_change(scope.chat_id)
    except ChatGraphError as exc:
        return fallback(f"check_failed_{getattr(exc, 'status_code', None)}")
    if latest is None or latest > max(modified for modified, _ in newest.values()):
        return fallback("check_newer_than_feed")

    changed = set(newest)
    reused = [item for item in prior if (item.graph_id if item.item_type in catalogue._MESSAGE_TYPES
                                         else item.parent_graph_id) not in changed]
    provenance = {"changed_only_from_attempt": str(attempt.id), "changed_messages": len(changed),
                  "source_observed_at": checked_at.isoformat(), "full_observed_at": full.isoformat()}
    started = datetime.now(timezone.utc)

    async def begin(session):
        lease = await catalogue.begin_capture(session, scope=scope, folder_path=folder_path, observation_hash_version=version)
        record = await session.get(ChatCaptureAttempt, lease.attempt_id)
        record.evidence_history = [*([{"source_filter_sha256": scope_filter_hash}] if scope_filter_hash is not None else []),
                                   provenance]
        return lease
    keeper = _CaptureLeaseKeeper(session_factory, await run_catalogue_transaction(session_factory, begin))
    renewer = asyncio.create_task(keeper.run())
    label = "chat_changed_only_capture"
    try:
        with ChatObservationLedger(scope, observation_hash_version=version) as ledger:
            stats = await _observe(scope=scope, keeper=keeper, factory=session_factory, storage=storage,
                source=_FeedMessages(source, [record for _, record in newest.values()]), ledger=ledger,
                observation=0, exclude_message=exclude_message, folder_path=folder_path)
        if not stats.complete or stats.unresolved:
            await _reject(scope, session_factory, keeper.lease, label, provenance)
            return fallback("changed_messages_incomplete")
        keys = [qualified_chat_item_key(scope.chat_id, i.item_type, i.graph_id, i.parent_graph_id) for i in reused]

        async def bulk(session):
            keeper.lease = await catalogue.renew_capture(session, lease=keeper.lease)
            return await catalogue.copy_versions(session, lease=keeper.lease, inventory_id=inventory.id,
                                                 source_snapshot_id=attempt.snapshot_id, keys=keys)
        if keys and not await run_catalogue_transaction(session_factory, bulk):
            await _stage(session_factory, keeper, reused)

        async def load(session):
            return [catalogue._captured(v) for v in (await _inventory_items(session, keeper.lease.staged_inventory_id)).all()]
        items = await run_catalogue_transaction(session_factory, load)
        keeper.check()
        result = await _publish_staged(scope=scope, session_factory=session_factory, lease=keeper.lease, items=items,
                                       started=started, label=label, provenance=provenance)
    except (asyncio.CancelledError, catalogue.ChatCatalogueConflict):
        raise
    except Exception as exc:
        # Source movement, a staging integrity refusal, a Graph error: the full read decides.
        with suppress(Exception):
            await _reject(scope, session_factory, keeper.lease, label, provenance)
        return fallback(f"capture_failed_{type(exc).__name__}")
    finally:
        renewer.cancel()
        with suppress(asyncio.CancelledError):
            await renewer
    if result is None:
        return fallback("verification_rejected")
    deleted = sum(1 for _, record in newest.values() if record.get("deletedDateTime"))
    logger.info("chat_changed_only_capture chat=%s changed=%s deleted=%s reused=%s",
                scope.chat_id, len(changed) - deleted, deleted, len(reused))
    return result


_PROBE_TTL_S = 6 * 3600   # one key per chat per run; outlives any run


def _probe_key(run_key: str, tenant_id: str, chat_id: str) -> str:
    return "chat:probe:" + hashlib.sha256(f"{run_key}|{tenant_id}|{chat_id}".encode()).hexdigest()[:32]


async def _shared_latest_changes(run_key, tenant_id, chat_ids, probe) -> dict:
    """Newest message change per chat (None: no messages, False: the check failed), checked once per
    chat per run for every member and worker through the shared Redis. Teams allows 1 request/s per
    chat, so a group chat with hundreds of members must not be checked once per member. A new run
    checks again; a failed check is never shared; without Redis each member checks."""
    from shared.graph_rate_limiter import graph_rate_limiter
    keys = {cid: _probe_key(str(run_key), str(tenant_id), cid) for cid in chat_ids}
    redis, cached = None, {}
    try:
        redis = await graph_rate_limiter.redis()
        if redis is not None and keys:
            cached = {cid: v for cid, v in zip(keys, await redis.mget(list(keys.values()))) if v is not None}
    except Exception as exc:
        logger.warning("chat_probe_share_unavailable error=%s", type(exc).__name__)
        redis, cached = None, {}
    found = {cid: None if v == "none" else datetime.fromisoformat(v) for cid, v in cached.items()}
    missing = [cid for cid in chat_ids if cid not in found]
    slots = asyncio.Semaphore(8)

    async def check(chat_id):
        async with slots:
            try:
                return await probe(chat_id)
            except ChatGraphError:
                return False
    found.update(zip(missing, await asyncio.gather(*(check(cid) for cid in missing))))
    fresh = {keys[cid]: "none" if found[cid] is None else found[cid].isoformat()
             for cid in missing if found[cid] is not False}
    if redis is not None and fresh:
        try:
            async with redis.pipeline(transaction=False) as pipe:
                for key, value in fresh.items():
                    pipe.set(key, value, ex=_PROBE_TTL_S, nx=True)
                await pipe.execute()
        except Exception as exc:
            logger.warning("chat_probe_share_unavailable error=%s", type(exc).__name__)
    logger.info("chat_probe_shared checked=%s reused=%s", len(missing), len(cached))
    return found


async def link_unchanged_chats(*, scopes, folders, filter_hashes, session_factory, source_snapshot_id,
                               latest_change, whole_snapshot=False, external_chats=None) -> set:
    """Publish, in one step, every chat of ``scopes`` that the principal's delta feed showed unchanged
    since ``source_snapshot_id`` (whose run started the feed's cursor) from its COMPLETE publication
    there. Same scope, observation version, weekly full-read deadline and one-request change check
    (``latest_change``) as reuse_unchanged_chat: the feed alone missed an edit of a 13-hour-old group
    message (Railway 2026-11-05). Chats that do not qualify are left for the per-chat path. Returns
    the chat ids published. The change check of each chat is shared by every member of the run
    (_shared_latest_changes). ``whole_snapshot``: ``scopes`` are every chat of this backup; when all of
    them link and match the source backup exactly, the backup points at it instead (_point_at_source)."""
    if not scopes:
        return set()
    from shared.snapshot_reuse import items_snapshot_id
    source_snapshot_id = await run_catalogue_transaction(session_factory,
        lambda session: items_snapshot_id(session, source_snapshot_id))
    from shared.config import settings
    version = settings.CHAT_OBSERVATION_HASH_VERSION
    reconcile = timedelta(hours=float(os.getenv("CHAT_INCREMENTAL_RECONCILE_HOURS", "168")))
    overlap = timedelta(minutes=float(os.getenv("CHAT_INCREMENTAL_OVERLAP_MIN", "15")))

    async def find(session):
        return (await session.execute(select(ChatCaptureAttempt, ChatInventory)
            .join(ChatInventory, ChatInventory.id == ChatCaptureAttempt.staged_inventory_id)
            .join(SnapshotChatInventory, and_(SnapshotChatInventory.snapshot_id == ChatCaptureAttempt.snapshot_id,
                                              SnapshotChatInventory.capture_attempt_id == ChatCaptureAttempt.id))
            .where(ChatCaptureAttempt.snapshot_id == source_snapshot_id, ChatCaptureAttempt.chat_id.in_(list(scopes)),
                   ChatCaptureAttempt.state == "PUBLISHED", ChatInventory.state == "COMPLETE",
                   ChatInventory.observation_hash_version == version,
                   SnapshotChatInventory.coverage_status == "COMPLETE"))).all()
    from shared.models import Snapshot
    any_scope = next(iter(scopes.values()))

    async def run_of(session):
        return await session.scalar(select(Snapshot.job_id).where(Snapshot.id == any_scope.snapshot_id))
    now = datetime.now(timezone.utc)
    eligible = []
    for attempt, inventory in await run_catalogue_transaction(session_factory, find):
        scope = scopes[attempt.chat_id]
        observed, full = _observed_times(attempt)
        if (attempt.principal_id != scope.principal_id or not attempt.evidence or observed is None or full is None
                or now - full >= reconcile
                or inventory.authentication_context_sha256 != authentication_context_fingerprint(scope)):
            continue
        eligible.append((attempt, inventory, observed, full))

    run_key = await run_catalogue_transaction(session_factory, run_of) or any_scope.snapshot_id
    latest = await _shared_latest_changes(run_key, any_scope.tenant_id,
                                          [attempt.chat_id for attempt, *_ in eligible], latest_change)
    already_read = await _changes_already_read(session_factory, {
        attempt.chat_id: (inventory.id, latest[attempt.chat_id]) for attempt, inventory, observed, _ in eligible
        if latest[attempt.chat_id] and observed - overlap < latest[attempt.chat_id] <= observed})
    links = {}
    for attempt, inventory, observed, full in eligible:
        changed_at = latest[attempt.chat_id]
        if changed_at is False or (changed_at is not None and changed_at > observed - overlap
                                   and attempt.chat_id not in already_read):
            continue
        provenance = {"reused_unchanged_from_attempt": str(attempt.id), "source_observed_at": observed.isoformat(),
                      "full_observed_at": full.isoformat(), "delta_unchanged_since_snapshot": str(source_snapshot_id),
                      "latest_message_change": changed_at.isoformat() if changed_at else None}
        recorded = filter_hashes.get(attempt.chat_id)
        links[attempt.chat_id] = (inventory.id, {**attempt.evidence, "started_at": now.isoformat(), "finished_at": now.isoformat()},
                                  [*([{"source_filter_sha256": recorded}] if recorded is not None else []), provenance])

    if whole_snapshot and links and len(links) == len(scopes) and os.getenv(
            "CHAT_UNCHANGED_REUSE_ENABLED", "true").strip().lower() not in ("0", "false", "no", "off"):
        async def point(session):
            return await _point_at_source(session, scopes=scopes, links=links, folders=folders,
                                          source_snapshot_id=source_snapshot_id, external_chats=external_chats or set())
        if await run_catalogue_transaction(session_factory, point):
            logger.info("chat_unchanged_backup_reused snapshot=%s source=%s chats=%s",
                        next(iter(scopes.values())).snapshot_id, source_snapshot_id, len(scopes))
            return set(scopes)

    async def publish(session):
        return await catalogue.publish_unchanged_inventories(session, scopes=scopes, links=links, folders=folders,
                                                             source_snapshot_id=source_snapshot_id)
    try:
        linked = await run_catalogue_transaction(session_factory, publish) if links else set()
    except (catalogue.ChatCatalogueIntegrityError, catalogue.ChatCatalogueConflict) as exc:
        logger.warning("chat_delta_bulk_link_rejected chats=%s reason=%s", len(links), exc)
        linked = set()
    logger.info("chat_delta_bulk_linked chats=%s eligible=%s checked_changed=%s candidates=%s",
                len(linked), len(links), len(eligible) - len(links), len(scopes))
    return linked


async def _point_at_source(session, *, scopes, links, folders, source_snapshot_id, external_chats):
    """Point this backup at ``source_snapshot_id`` when it would be an exact copy: the source is a
    COMPLETED chat backup holding exactly these chats, each with the inventory being linked and the same
    folder, the same chats hosted in other tenants, and this backup holds nothing yet. Every unchanged
    incremental copied each chat's rows (203,780 rows for 10 users per run)."""
    from shared.models import Snapshot, SnapshotStatus
    from shared.snapshot_reuse import point_at
    first = next(iter(scopes.values()))
    parents = await catalogue._lock_scope(session, first)
    await catalogue._eligible(session, first, parents)
    source = await session.get(Snapshot, source_snapshot_id)
    record = ((source.extra_data or {}).get("chat_catalogue_scopes") or {}).get("resource") or {} if source else {}
    if (source is None or source.status != SnapshotStatus.COMPLETED or source.resource_id != first.resource_id
            or (source.extra_data or {}).get("chat_inventory_format") != 1 or (source.extra_data or {}).get("cancelled_at")
            or not record.get("complete") or set(record.get("external_tenant_chats") or ()) != set(external_chats)):
        return False
    held = {row.chat_id: (row.inventory_id, row.folder_path) for row in (await session.execute(
        select(SnapshotChatInventory.chat_id, SnapshotChatInventory.inventory_id, ChatCaptureAttempt.folder_path)
        .join(ChatCaptureAttempt, ChatCaptureAttempt.id == SnapshotChatInventory.capture_attempt_id)
        .where(SnapshotChatInventory.snapshot_id == source_snapshot_id))).all()}
    if held != {cid: (links[cid][0], folders.get(cid)) for cid in scopes}:
        return False
    if await session.scalar(select(func.count()).select_from(SnapshotChatInventory)
                            .where(SnapshotChatInventory.snapshot_id == first.snapshot_id)):
        return False
    return await point_at(session, target_snapshot_id=first.snapshot_id, prior_snapshot_id=source_snapshot_id,
                          marker="chat_reused_prior")


async def _link_unchanged(*, scope, session_factory, folder_path, inventory_id, source_snapshot_id, source_evidence, history):
    """Publish an unchanged chat by linking its COMPLETE inventory (catalogue.publish_unchanged_inventory).
    None when that is not possible; the caller then re-stages the items."""
    from shared.config import settings
    if not source_evidence or source_snapshot_id is None:
        return None
    version = settings.CHAT_OBSERVATION_HASH_VERSION

    async def begin(session):
        return await catalogue.begin_capture(session, scope=scope, folder_path=folder_path,
                                             observation_hash_version=version)
    lease = await run_catalogue_transaction(session_factory, begin)
    now = datetime.now(timezone.utc).isoformat()
    evidence = {**source_evidence, "started_at": now, "finished_at": now}

    async def link(session):
        return await catalogue.publish_unchanged_inventory(session, lease=lease, inventory_id=inventory_id,
            source_snapshot_id=source_snapshot_id, evidence=evidence, history=history)
    try:
        result = await run_catalogue_transaction(session_factory, link)
    except catalogue.ChatCatalogueIntegrityError:
        result = None
    if result is None:
        async def discard(session):
            await _discard_attempt(session, lease, scope, "chat_incremental_linked rejected")
        await run_catalogue_transaction(session_factory, discard)
        return None
    logger.info("chat_incremental_linked attempt_id=%s chat=%s items=%s", lease.attempt_id, scope.chat_id, result.item_count)
    return result


@asynccontextmanager
async def _source(client, principal_id):
    if isinstance(client, StrictChatGraphSource):
        if client.principal_id != principal_id:
            raise ValueError("source principal differs from capture scope")
        yield client
    else:
        async with pinned_chat_source(client, principal_id=principal_id) as opened:
            yield opened


_CAPTURE_SLOTS = WeakKeyDictionary()


async def _refetch_changed_files(*, scope, keeper, factory, storage, source, ledger, observation_hash_version) -> bool:
    """Only file attachments differ between the passes (a file edited while the chat was read): fetch
    those files again instead of the whole chat (Railway 2026-11-05: one .xlsx edit re-read a
    3,955-message chat, 12.5 min). True only when every re-fetch matches what pass 2 saw."""
    changed = ledger.changed_files()
    if not changed or any(metadata_only_attachment(payload) for _, _, _, payload, _ in changed):
        return False
    try:
        for key, graph_id, parent_id, payload, seen in changed:
            await _remove_staged_item(factory, keeper, scope, key)
            captured = await _binary(scope=scope, keeper=keeper, factory=factory, storage=storage, source_client=source,
                item_type="CHAT_ATTACHMENT", graph_id=graph_id, parent_id=parent_id, payload=payload, stage=True)
            semantic = observation_semantic(captured, observation_hash_version)
            if semantic != seen:
                return False
            ledger.adopt(key, semantic)
    except _STOPS_CAPTURE:
        raise
    except Exception as exc:
        logger.warning("chat_file_refetch_failed attempt_id=%s chat=%s error=%s", keeper.lease.attempt_id, scope.chat_id, type(exc).__name__)
        return False
    logger.info("chat_file_movement_refetched attempt_id=%s chat=%s files=%s", keeper.lease.attempt_id, scope.chat_id, len(changed))
    return ledger.fingerprint(0) == ledger.fingerprint(1)


async def capture_chat_inventory(*, scope, graph_client, storage, session_factory,
                                 folder_path=None, exclude_message=None, scope_filter_hash=None) -> CaptureResult:
    """Bound open chat ledgers and preserve their iterators across local waits."""
    from shared.provider_runtime import provider_live_wait_scope, current_unit, ProviderBudgetExhausted
    loop = asyncio.get_running_loop()
    if loop not in _CAPTURE_SLOTS:
        capacity = int(os.getenv('CHAT_CAPTURE_CONCURRENCY', '8'))
        if not 1 <= capacity <= 64:
            raise ValueError('chat capture concurrency must be between 1 and 64')
        _CAPTURE_SLOTS[loop] = asyncio.Semaphore(capacity)
    unit = current_unit()
    remaining = max(0, (unit.deadline_at - datetime.now(timezone.utc)).total_seconds()) if unit else 1800
    deadline = asyncio.timeout(remaining)
    try:
        async with deadline, provider_live_wait_scope(), _CAPTURE_SLOTS[loop]:
            return await _capture_chat_inventory(scope=scope, graph_client=graph_client, storage=storage,
                session_factory=session_factory, folder_path=folder_path,
                exclude_message=exclude_message, scope_filter_hash=scope_filter_hash)
    except TimeoutError:
        if not deadline.expired():
            raise
        raise ProviderBudgetExhausted('chat capture original deadline exhausted') from None


async def _capture_chat_inventory(*, scope, graph_client, storage, session_factory,
                                 folder_path=None, exclude_message=None, scope_filter_hash=None) -> CaptureResult:
    """Capture one target's observed chat coverage, with two bounded reconciliations."""
    from shared.config import settings
    observation_version = settings.CHAT_OBSERVATION_HASH_VERSION
    from shared.provider_runtime import current_unit, current_execution, ProviderTurnDeferred, checkpoint_unit
    unit = current_unit()
    started = unit.created_at if unit else datetime.now(timezone.utc)
    if exclude_message is not None and not scope_filter_hash:
        raise ValueError("source exclusions require a stable configured filter hash")
    async with _source(graph_client, scope.principal_id) as source:
        if effective_capture_scope_hash(source.permission_scope_hash, scope_filter_hash) != scope.permission_scope_hash:
            raise ValueError("capture scope differs from pinned source configuration")
        for retry in range(unit.reconciliations if unit else 0, 3):
            async def begin(session):
                lease = await catalogue.begin_capture(session, scope=scope, folder_path=folder_path,
                    observation_hash_version=observation_version)
                if scope_filter_hash is not None:
                    attempt = await session.get(ChatCaptureAttempt, lease.attempt_id)
                    attempt.evidence_history = [{"source_filter_sha256": scope_filter_hash}]
                return lease
            lease = await run_catalogue_transaction(session_factory, begin)
            keeper = _CaptureLeaseKeeper(session_factory, lease)
            renewer = asyncio.create_task(keeper.run())
            try:
                with ChatObservationLedger(scope, observation_hash_version=lease.observation_hash_version) as ledger:
                    first, second = _Pass(), _Pass()
                    movement, movement_reason = False, "observation fingerprints differ"
                    try:
                        first = await _observe(scope=scope, keeper=keeper, factory=session_factory, storage=storage,
                            source=source, ledger=ledger, observation=0, exclude_message=exclude_message, folder_path=folder_path)
                        if (first.complete and not first.unresolved and _single_pass_enabled() and
                                await _verified_state(session_factory, scope, ledger.fingerprint(0), lease.observation_hash_version)):
                            # Pass 1 reproduced a state two passes already verified COMPLETE:
                            # that is a consistent observation, so the second listing is skipped.
                            ledger.mirror(0, 1)
                            second = _Pass(complete=True, pages=first.pages, messages=first.messages)
                            logger.info("chat_single_pass_reuse attempt_id=%s chat=%s messages=%s",
                                        lease.attempt_id, scope.chat_id, first.messages)
                        else:
                            second = await _observe(scope=scope, keeper=keeper, factory=session_factory, storage=storage,
                                source=source, ledger=ledger, observation=1, exclude_message=exclude_message, folder_path=folder_path)
                        movement = (first.complete and second.complete and not first.unresolved and not second.unresolved and
                                    ledger.fingerprint(0) != ledger.fingerprint(1))
                        if movement:
                            movement = not await _refetch_changed_files(scope=scope, keeper=keeper, factory=session_factory,
                                storage=storage, source=source, ledger=ledger, observation_hash_version=lease.observation_hash_version)
                    except ChatSourceMovement as exc:
                        movement, first.conflicts, movement_reason = True, 1, str(exc)
                    if movement:
                        mismatch = {**ledger.difference_details(), "movement_reason": movement_reason}
                        logger.warning("chat_source_movement attempt_id=%s chat=%s retry=%s reason=%s only_first=%s only_second=%s changed=%s",
                                       lease.attempt_id, scope.chat_id, retry, movement_reason,
                                       mismatch.get("only_first"), mismatch.get("only_second"), mismatch.get("changed"))
                        async def save_mismatch(session, mismatch=mismatch):
                            attempt, _ = await catalogue._attempt(session, keeper.lease)
                            attempt.evidence_history = [*(attempt.evidence_history or []), {"observation_mismatch": mismatch}]
                        await run_catalogue_transaction(session_factory, save_mismatch)
                    if movement and retry < 2:
                        await _restart(session_factory, keeper, scope)
                        await checkpoint_unit(state="PENDING", reconciliations=1)
                        continue
                    evidence = CaptureEvidence(first.complete, second.complete,
                        ledger.fingerprint(0), ledger.fingerprint(1), first.conflicts + int(movement),
                        ledger.required_count(0), ledger.durable_count(0),
                        first.unresolved + second.unresolved, started, datetime.now(timezone.utc),
                        observation_hash_version=lease.observation_hash_version)
                    keeper.check()
                    failure_details = ledger.failure_details()
                    async def finish(session):
                        await catalogue.record_capture_evidence(session, lease=keeper.lease, evidence=evidence)
                        if failure_details:
                            attempt = await session.get(ChatCaptureAttempt, keeper.lease.attempt_id)
                            attempt.evidence_history = [*(attempt.evidence_history or []), {"capture_errors": failure_details}]
                        return await catalogue.publish_inventory(session, lease=keeper.lease)
                    return await run_catalogue_transaction(session_factory, finish)
            except ProviderTurnDeferred as deferred:
                execution = current_execution()
                async def suspend(session):
                    await catalogue.suspend_capture(session, lease=keeper.lease, claim=execution.claim,
                        unit_key=unit.key, next_eligible_at=deferred.next_eligible_at)
                await run_catalogue_transaction(session_factory, suspend)
                raise
            except asyncio.CancelledError:
                execution = current_execution()
                if execution is not None and unit is not None:
                    async def suspend_cancelled(session):
                        due = execution.deferred.next_eligible_at if execution.deferred else datetime.now(timezone.utc)
                        await catalogue.suspend_capture(session, lease=keeper.lease, claim=execution.claim,
                            unit_key=unit.key, next_eligible_at=due)
                    # Bound DB cleanup and preserve the first cancellation on repeated shutdown signals.
                    with suppress(Exception, asyncio.CancelledError):
                        await run_bounded(run_catalogue_transaction(session_factory, suspend_cancelled), timeout_s=10)
                    raise
                async def abort(session):
                    await catalogue.abort_capture(session, lease=keeper.lease, reason="capture cancelled before verification")
                with suppress(Exception, asyncio.CancelledError):
                    await run_bounded(run_catalogue_transaction(session_factory, abort), timeout_s=10)
                raise
            finally:
                renewer.cancel()
                with suppress(asyncio.CancelledError):
                    await renewer
    raise RuntimeError("bounded chat reconciliation exhausted")
