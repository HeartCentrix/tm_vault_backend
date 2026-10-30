"""Bounded full chat reconciliation into immutable, fenced catalogue captures."""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, replace
from datetime import datetime, timezone
import hashlib
import json
import os
import sqlite3
import tempfile
from uuid import UUID

from sqlalchemy import and_, delete, exists, or_, select
from sqlalchemy.exc import SQLAlchemyError

from shared import chat_catalogue as catalogue
from shared.chat_catalogue_identity import (
    CaptureEvidence, CaptureResult, CapturedChatItem, ChatContentDescriptor,
    _digest, _payload, authentication_context_fingerprint, canonical_version_fingerprint,
    qualified_chat_item_key,
)
from shared.chat_capture_graph import StrictChatGraphSource, effective_capture_scope_hash, pinned_chat_source
from shared.chat_capture_storage import upload_capture_content, verify_catalogue_content
from shared.chat_content_stream import spool_chat_content
from shared.db_retry import run_catalogue_transaction
from shared.models import ChatCaptureAttempt, ChatInventory, ChatInventoryItem, ChatItemVersion, SnapshotChatInventory, SnapshotItem


class ChatSourceMovement(RuntimeError):
    """The same source membership/version could not be reconciled."""


class ChatObservationLedger:
    """Disk-backed hash ordering; retain no whole-chat payload list in memory."""

    def __init__(self, scope):
        self.scope = scope
        handle = tempfile.NamedTemporaryFile(prefix="chat-observation-", suffix=".sqlite", delete=False)
        self.path = handle.name
        handle.close()
        self.db = sqlite3.connect(self.path)
        self.db.execute("CREATE TABLE observed (pass INTEGER, key TEXT, semantic TEXT, binary INTEGER, PRIMARY KEY(pass,key))")
        self.db.execute("CREATE TABLE expected (pass INTEGER, key TEXT, required INTEGER, PRIMARY KEY(pass,key))")
        self.db.execute("CREATE TABLE failures (pass INTEGER, phase TEXT, error_type TEXT, status INTEGER, graph_id TEXT, parent_graph_id TEXT)")

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.db.close()
        os.unlink(self.path)

    def expect(self, observation, item_type, graph_id, parent_graph_id, *, required):
        key = qualified_chat_item_key(self.scope.chat_id, item_type, graph_id, parent_graph_id)
        self.db.execute("INSERT INTO expected VALUES (?,?,?) ON CONFLICT(pass,key) DO UPDATE SET required=MAX(required,excluded.required)",
                        (observation, key, int(required)))

    def record(self, observation, item):
        key = qualified_chat_item_key(self.scope.chat_id, item.item_type, item.graph_id, item.parent_graph_id)
        content = item.content
        source = ([content.source_drive_id, content.source_item_id, content.source_etag,
                   content.source_ctag, content.sha256, content.size] if content else None)
        semantic = _digest([_payload(item.payload), source])
        old = self.db.execute("SELECT semantic FROM observed WHERE pass=? AND key=?", (observation, key)).fetchone()
        if old:
            if old[0] != semantic:
                raise ChatSourceMovement("conflicting source versions during enumeration")
            return False
        self.db.execute("INSERT INTO observed VALUES (?,?,?,?)", (observation, key, semantic, int(content is not None)))
        return True

    def fingerprint(self, observation):
        digest = hashlib.sha256()
        prefix = json.dumps(["chat-source-inventory-v1", authentication_context_fingerprint(self.scope)], separators=(",", ":"))
        digest.update((prefix[:-1] + ",[").encode())
        separator = b""
        for key, semantic in self.db.execute("SELECT key,semantic FROM observed WHERE pass=? ORDER BY key", (observation,)):
            digest.update(separator)
            digest.update(json.dumps([key, semantic], separators=(",", ":")).encode())
            separator = b","
        digest.update(b"]]")
        return digest.hexdigest()

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
        self.db.execute("INSERT INTO failures VALUES (?,?,?,?,?,?)",
            (observation, phase, type(error).__name__, status, graph_id, parent_id))

    def failure_details(self):
        names = ("observation", "phase", "error_type", "status_code", "graph_id", "parent_graph_id")
        return [dict(zip(names, row)) for row in self.db.execute("SELECT * FROM failures LIMIT 50")]


@dataclass
class _Pass:
    complete: bool = False
    unresolved: int = 0
    conflicts: int = 0


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
        complete_candidate = ChatInventory.state == "COMPLETE"
        if checksum is None:
            # Fast tag-only reuse stays in the same observed visibility scope.
            # Cross-context reuse requires this capture's actual source bytes.
            complete_candidate = and_(complete_candidate,
                ChatInventory.authentication_context_sha256 == authentication_context_fingerprint(scope))
        retained_membership = exists(select(ChatInventoryItem.logical_key).join(
            ChatInventory, ChatInventory.id == ChatInventoryItem.inventory_id).where(
                ChatInventoryItem.version_id == ChatItemVersion.id,
                or_(ChatInventory.id == keeper.lease.staged_inventory_id, complete_candidate)))
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


async def _binary(*, scope, keeper, factory, storage, source_client, item_type,
                  graph_id, parent_id, payload, stage):
    file = item_type == "CHAT_ATTACHMENT"
    raw = await source_client.resolve_file(payload.get("contentUrl")) if file else None
    payload = dict(payload)
    if file and isinstance(raw.get("file"), dict) and isinstance(raw["file"].get("mimeType"), str):
        payload["captured_content_type"] = raw["file"]["mimeType"]
    source = _file_source(raw) if file else dict(source_drive_id=None, source_item_id=None, source_etag=None, source_ctag=None)
    retained = await _retained(scope=scope, keeper=keeper, factory=factory, storage=storage,
        item_type=item_type, graph_id=graph_id, parent_id=parent_id, payload=payload, source=source, stage=stage)
    if retained is not None:
        return retained
    opened = source_client.open_file(raw) if file else source_client.open_hosted(scope.chat_id, parent_id, graph_id)
    async with opened as (chunks, expected_size, content_type):
        if isinstance(content_type, str) and content_type:
            payload.setdefault("captured_content_type", content_type)
        async with spool_chat_content(keeper.guard(chunks), expected_size=expected_size) as spooled:
            if file:
                refreshed = await source_client.refresh_file(source["source_drive_id"], source["source_item_id"])
                if _file_source(refreshed) != source:
                    raise ChatSourceMovement("required file changed during download")
            retained = await _retained(scope=scope, keeper=keeper, factory=factory, storage=storage,
                item_type=item_type, graph_id=graph_id, parent_id=parent_id, payload=payload,
                source=source, checksum=spooled.sha256, stage=stage)
            if retained is not None:
                return retained
            if stage:
                content = await upload_capture_content(scope=scope, lease=keeper.lease,
                    spooled=spooled, storage=storage, session_factory=factory, **source)
            else:
                # Reconciliation hashes semantic bytes; this location is never
                # persisted, uploaded or used for reads.
                content = ChatContentDescriptor(UUID(int=0), scope.backend_id, "observation", "observation",
                    spooled.sha256, spooled.size, None, 1, None, **source)
            item = CapturedChatItem(item_type, graph_id, parent_id, payload, content)
            if stage:
                await _stage(factory, keeper, [item])
            return item


async def _observe(*, scope, keeper, factory, storage, source, ledger, observation,
                   exclude_message=None, folder_path=None):
    stats = _Pass()
    try:
        async for page in source.iter_message_pages(scope.chat_id):
            keeper.check()
            prepared, parents = [], []
            try:
                for raw in page:
                    parent = dict(raw)
                    if exclude_message is not None and exclude_message({**parent, "_chat_folder_path": folder_path}):
                        continue
                    identifier = parent.get("id")
                    native = parent.get("attachments") or []
                    valid = isinstance(native, list) and all(isinstance(a, dict) and isinstance(a.get("id"), str) and a["id"] for a in native)
                    attachments = native if valid else []
                    if not valid:
                        stats.unresolved += 1
                    hosted, hosted_complete = [], True
                    try:
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
                    prepared.append((identifier, attachments, hosted))
            finally:
                # Parent rows must already exist before any child upload/link;
                # interruption then leaves a readable partial message inventory.
                if parents:
                    await _stage(factory, keeper, parents)
            pending = []
            for identifier, attachments, hosted in prepared:
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
                                    payload=child, stage=observation == 0)
                                ledger.record(observation, captured)
                        except (asyncio.CancelledError, ChatSourceMovement, catalogue.ChatCatalogueConflict, catalogue.ChatCatalogueIntegrityError, SQLAlchemyError):
                            raise
                        except Exception as exc:
                            ledger.failure(observation, kind, exc, child["id"], identifier)
                            stats.unresolved += 1
                        if len(pending) >= 200:
                            await _stage(factory, keeper, pending)
                            pending = []
            if pending:
                await _stage(factory, keeper, pending)
        stats.complete = True
    except (asyncio.CancelledError, ChatSourceMovement, catalogue.ChatCatalogueConflict, catalogue.ChatCatalogueIntegrityError, SQLAlchemyError):
        raise
    except Exception as exc:
        ledger.failure(observation, "message_enumeration", exc)
        stats.unresolved += 1
    return stats


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


async def capture_chat_inventory(*, scope, graph_client, storage, session_factory,
                                 folder_path=None, exclude_message=None, scope_filter_hash=None) -> CaptureResult:
    """Capture one target's observed chat coverage, with two bounded reconciliations."""
    started = datetime.now(timezone.utc)
    if exclude_message is not None and not scope_filter_hash:
        raise ValueError("source exclusions require a stable configured filter hash")
    async with _source(graph_client, scope.principal_id) as source:
        if effective_capture_scope_hash(source.permission_scope_hash, scope_filter_hash) != scope.permission_scope_hash:
            raise ValueError("capture scope differs from pinned source configuration")
        for retry in range(3):
            async def begin(session):
                lease = await catalogue.begin_capture(session, scope=scope, folder_path=folder_path)
                if scope_filter_hash is not None:
                    attempt = await session.get(ChatCaptureAttempt, lease.attempt_id)
                    attempt.evidence_history = [{"source_filter_sha256": scope_filter_hash}]
                return lease
            lease = await run_catalogue_transaction(session_factory, begin)
            keeper = _CaptureLeaseKeeper(session_factory, lease)
            renewer = asyncio.create_task(keeper.run())
            try:
                with ChatObservationLedger(scope) as ledger:
                    first, second = _Pass(), _Pass()
                    movement = False
                    try:
                        first = await _observe(scope=scope, keeper=keeper, factory=session_factory, storage=storage,
                            source=source, ledger=ledger, observation=0, exclude_message=exclude_message, folder_path=folder_path)
                        second = await _observe(scope=scope, keeper=keeper, factory=session_factory, storage=storage,
                            source=source, ledger=ledger, observation=1, exclude_message=exclude_message, folder_path=folder_path)
                        movement = (first.complete and second.complete and not first.unresolved and not second.unresolved and
                                    ledger.fingerprint(0) != ledger.fingerprint(1))
                    except ChatSourceMovement:
                        movement, first.conflicts = True, 1
                    if movement and retry < 2:
                        await _restart(session_factory, keeper, scope)
                        continue
                    evidence = CaptureEvidence(first.complete, second.complete,
                        ledger.fingerprint(0), ledger.fingerprint(1), first.conflicts + int(movement),
                        ledger.required_count(0), ledger.durable_count(0),
                        first.unresolved + second.unresolved, started, datetime.now(timezone.utc))
                    keeper.check()
                    failure_details = ledger.failure_details()
                    async def finish(session):
                        await catalogue.record_capture_evidence(session, lease=keeper.lease, evidence=evidence)
                        if failure_details:
                            attempt = await session.get(ChatCaptureAttempt, keeper.lease.attempt_id)
                            attempt.evidence_history = [*(attempt.evidence_history or []), {"capture_errors": failure_details}]
                        return await catalogue.publish_inventory(session, lease=keeper.lease)
                    return await run_catalogue_transaction(session_factory, finish)
            except asyncio.CancelledError:
                async def abort(session):
                    await catalogue.abort_capture(session, lease=keeper.lease, reason="capture cancelled before verification")
                with suppress(Exception):
                    await asyncio.shield(run_catalogue_transaction(session_factory, abort))
                raise
            finally:
                renewer.cancel()
                with suppress(asyncio.CancelledError):
                    await renewer
    raise RuntimeError("bounded chat reconciliation exhausted")
