"""Bounded full chat reconciliation into immutable, fenced catalogue captures."""
from __future__ import annotations

import asyncio
import copy
from collections import OrderedDict
from contextlib import AsyncExitStack, asynccontextmanager, suppress
from dataclasses import dataclass, replace
from datetime import datetime, timezone
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

from sqlalchemy import and_, delete, exists, or_, select
from sqlalchemy.exc import SQLAlchemyError

from shared import chat_catalogue as catalogue
from shared.chat_catalogue_identity import (
    SOURCE_UNAVAILABLE_FIELD,
    CaptureEvidence, CaptureResult, CapturedChatItem, ChatContentDescriptor,
    _digest, _payload, authentication_context_fingerprint, canonical_version_fingerprint,
    qualified_chat_item_key, observation_semantic, validate_observation_hash_version,
)
from shared.chat_capture_graph import ChatGraphError, StrictChatGraphSource, effective_capture_scope_hash, pinned_chat_source
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
        self.db.execute("CREATE TABLE observed (pass INTEGER, key TEXT, semantic TEXT, binary INTEGER, PRIMARY KEY(pass,key))")
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
        self.db.execute("INSERT INTO observed VALUES (?,?,?,?)", (observation, key, semantic, int(content is not None)))
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
        self.db.execute("INSERT INTO observed SELECT ?, key, semantic, binary FROM observed WHERE pass=?", (target, source))
        self.db.execute("INSERT INTO expected SELECT ?, key, required FROM expected WHERE pass=?", (target, source))

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


def _metadata_only(attachment):
    content_type = attachment.get("contentType", "")
    return isinstance(content_type, str) and (content_type.startswith("application/vnd.microsoft.card") or
        content_type in {"messageReference", "forwardedMessageReference", "meetingReference"})


def stable_attachment_identity(attachment):
    """Graph mints a new ``id`` for card/reference attachments on every read, which
    made two identical observations look like source movement. Their identity is
    their content; file attachments keep the Graph id their download needs."""
    if not _metadata_only(attachment):
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
                  graph_id, parent_id, payload, stage, file_versions=None, hosted_marker=None):
    file = item_type == "CHAT_ATTACHMENT"
    raw = await source_client.resolve_file(payload.get("contentUrl")) if file else None
    payload = dict(payload)
    if file and isinstance(raw.get("file"), dict) and isinstance(raw["file"].get("mimeType"), str):
        payload["captured_content_type"] = raw["file"]["mimeType"]
    source = _file_source(raw) if file else dict(source_drive_id=None, source_item_id=None, source_etag=hosted_marker, source_ctag=None)
    cache_key = tuple(source.items()) if file and (source['source_etag'] or source['source_ctag']) else None
    if file_versions is not None and cache_key in file_versions:
        content, content_type = file_versions[cache_key]
        file_versions.move_to_end(cache_key)
        keeper.check()
        await verify_catalogue_content(storage, content)
        if content_type:
            payload.setdefault('captured_content_type', content_type)
        captured = CapturedChatItem(item_type, graph_id, parent_id, payload, content)
        if stage:
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
        item_type=item_type, graph_id=graph_id, parent_id=parent_id, payload=payload, source=source, stage=stage)
    mark("retained_lookup")
    if retained is not None:
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
            source=source, checksum=spooled.sha256, stage=stage)
        mark("retained_checksum")
        if retained is not None:
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
    stay unresolved, so they keep the chat PARTIAL."""
    return isinstance(exc, ChatGraphError) and getattr(exc, "status_code", None) in (404, 410, 423, 500)


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


async def _observe(*, scope, keeper, factory, storage, source, ledger, observation,
                   exclude_message=None, folder_path=None):
    stats = _Pass()
    file_versions = OrderedDict()
    home_tenant = getattr(source, "home_tenant_id", None)
    try:
        async for page in source.iter_message_pages(scope.chat_id):
            keeper.check()
            prepared, parents = [], []
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
                    if message_references_hosted_content(raw):
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
                            required = kind == "CHAT_HOSTED_CONTENT" or not _metadata_only(child)
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
                if parents:
                    await _stage(factory, keeper, parents)
            pending = []
            for identifier, attachments, hosted, marker in prepared:
                for kind, values in (("CHAT_ATTACHMENT", attachments), ("CHAT_HOSTED_CONTENT", hosted)):
                    for child in values:
                        try:
                            if kind == "CHAT_ATTACHMENT" and _metadata_only(child):
                                captured = CapturedChatItem(kind, child["id"], identifier, child, None)
                                if ledger.record(observation, captured) and observation == 0:
                                    pending.append(captured)
                            else:
                                captured = await _binary(scope=scope, keeper=keeper, factory=factory, storage=storage,
                                    source_client=source, item_type=kind, graph_id=child["id"], parent_id=identifier,
                                    payload=child, stage=observation == 0, file_versions=file_versions,
                                    hosted_marker=marker if kind == "CHAT_HOSTED_CONTENT" else None)
                                if ledger.record(observation, captured):
                                    stats.verified_binary += 1
                        except (asyncio.CancelledError, ChatSourceMovement, catalogue.ChatCatalogueConflict, catalogue.ChatCatalogueIntegrityError, SQLAlchemyError):
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
                        if len(pending) >= 200:
                            await _stage(factory, keeper, pending)
                            pending = []
            if pending:
                await _stage(factory, keeper, pending)
            stats.pages += 1
            logger.info(
                "chat_capture_progress attempt_id=%s observation=%s pages=%s messages_observed=%s "
                "hosted_enumerations=%s duplicate_messages_skipped=%s verified_binary=%s unresolved=%s",
                keeper.lease.attempt_id, observation + 1, stats.pages, stats.messages,
                stats.hosted_lists, stats.duplicate_messages, stats.verified_binary, stats.unresolved)
        stats.complete = True
    except (asyncio.CancelledError, ChatSourceMovement, catalogue.ChatCatalogueConflict, catalogue.ChatCatalogueIntegrityError, SQLAlchemyError):
        raise
    except Exception as exc:
        ledger.failure(observation, "message_enumeration", exc)
        stats.unresolved += 1
    return stats


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


async def _restart(factory, keeper, scope):
    async def operation(session):
        await catalogue.renew_capture(session, lease=keeper.lease)
        if await session.get(SnapshotChatInventory, (scope.snapshot_id, scope.chat_id)) is not None:
            raise catalogue.ChatCatalogueConflict("terminal inventory cannot restart")
        matching = exists(select(ChatInventoryItem.logical_key).where(
            ChatInventoryItem.inventory_id == keeper.lease.staged_inventory_id,
            ChatInventoryItem.logical_key == SnapshotItem.external_id,
            ChatInventoryItem.version_id == SnapshotItem.chat_version_id))
        await session.execute(delete(SnapshotItem).where(SnapshotItem.snapshot_id == scope.snapshot_id, matching))
        attempt = await session.get(ChatCaptureAttempt, keeper.lease.attempt_id)
        attempt.state, attempt.failure_reason = "FAILED", "source changed during bounded reconciliation"
    await run_catalogue_transaction(factory, operation)


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


async def capture_chat_inventory(*, scope, graph_client, storage, session_factory,
                                 folder_path=None, exclude_message=None, scope_filter_hash=None) -> CaptureResult:
    """Bound open chat ledgers and preserve their iterators across local waits."""
    from shared.provider_runtime import provider_live_wait_scope, current_unit, ProviderBudgetExhausted
    loop = asyncio.get_running_loop()
    if loop not in _CAPTURE_SLOTS:
        capacity = int(os.getenv('CHAT_CAPTURE_CONCURRENCY', '2'))
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
