"""Fenced chat catalogue transactions; callers own sessions and commits.

Lock order is shared job/snapshot/resource eligibility, exclusive attempt and
inventory, then object UUID. Different chats can stage under the same job.
These functions perform database work only. Source enumeration, upload receipts,
read pins and physical deletion belong to their respective adapters/lifecycles.

Before staging a parent, capture persists ``chat_catalogue_expected_children``
in its payload: ``{"complete": true, "items": [{"item_type": "CHAT_ATTACHMENT"
| "CHAT_HOSTED_CONTENT", "graph_id": "raw-id", "requires_content": true}]}``.
The complete manifest includes explicit empty enumeration; native attachments
and hostedContents metadata are also checked. This internal field is not public
Graph data and is removed from detached DTOs.
"""
from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
import json
from types import SimpleNamespace
from uuid import UUID, uuid4

from sqlalchemy import and_, delete, func, or_, select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError

from shared.chat_catalogue_identity import (
    CaptureEvidence, CaptureLease, CaptureResult, CapturedChatItem,
    ChatCaptureScope, ChatContentDescriptor, StageResult,
    _digest, _payload, authentication_context_fingerprint,
    canonical_payload_sha256, canonical_version_fingerprint,
    qualified_chat_item_key,
)
from shared.models import (
    ChatBlobObject, ChatCaptureAttempt, ChatInventory, ChatInventoryItem,
    ChatItemVersion, Job, JobStatus, Resource, ResourceType, Snapshot, SnapshotChatInventory,
    SnapshotItem, SnapshotPartition, SnapshotStatus,
)


class ChatCatalogueConflict(RuntimeError):
    """Capture no longer owns an eligible snapshot/chat fence."""


class ChatCatalogueIntegrityError(RuntimeError):
    """Persisted catalogue, source evidence or physical links disagree."""


_ACTIVE = {"CREATED", "ENUMERATING", "CONTENT_PENDING", "VERIFYING", "READY"}
_TERMINAL = {"PUBLISHED", "PARTIAL"}
_MESSAGE_TYPES = {"TEAMS_CHAT_MESSAGE", "TEAMS_MESSAGE", "TEAMS_MESSAGE_REPLY"}
_OBJECT_FIELDS = ("backend_id", "container", "path", "sha256", "size", "encryption_key_id", "backup_version")


def _scope(attempt):
    if attempt.snapshot_id is None:
        raise ChatCatalogueConflict("capture snapshot was deleted")
    return ChatCaptureScope(attempt.snapshot_id, attempt.resource_id, attempt.tenant_id,
        attempt.chat_id, attempt.principal_id, attempt.permission_scope_hash,
        attempt.backend_id, attempt.partition_id)


async def _locked(session, model, *conditions, shared=False):
    return await session.scalar(select(model).where(*conditions).with_for_update(read=shared)
                                .execution_options(populate_existing=True))


async def _lock_scope(session, scope):
    job_id = await session.scalar(select(Snapshot.job_id).where(Snapshot.id == scope.snapshot_id))
    job = await _locked(session, Job, Job.id == job_id, shared=True) if job_id else None
    snapshot = await _locked(session, Snapshot, Snapshot.id == scope.snapshot_id, shared=True)
    resource = await _locked(session, Resource, Resource.id == scope.resource_id, shared=True)
    return job, snapshot, resource


async def _eligible(session, scope, parents):
    job, snapshot, resource = parents
    metadata = (resource.extra_data or {}) if resource is not None else {}
    # USER_CHATS resources normally carry Graph user_id; older records use the
    # resource external_id directly. Neither target can come from caller scope.
    principal = metadata.get("user_id") or (resource.external_id if resource is not None else None)
    if (snapshot is None or resource is None or job is None or
        snapshot.resource_id != scope.resource_id or resource.tenant_id != scope.tenant_id or
        resource.type != ResourceType.USER_CHATS or principal != scope.principal_id or
        metadata.get("chat_inventory_format") != 1 or snapshot.job_id != job.id or
        snapshot.backend_id != scope.backend_id or job.tenant_id != scope.tenant_id or
        job.status not in {JobStatus.RUNNING, JobStatus.RETRYING} or
        snapshot.status != SnapshotStatus.IN_PROGRESS or
        (snapshot.extra_data or {}).get("chat_inventory_format") != 1 or
        resource.archived_at is not None or resource.deletion_queued_at is not None or
        str(getattr(resource.status, "value", resource.status)) == "PENDING_DELETION"):
        raise ChatCatalogueConflict("snapshot, job or capture scope is no longer eligible")
    if scope.partition_id is not None:
        partition = await _locked(session, SnapshotPartition, SnapshotPartition.id == scope.partition_id, shared=True)
        if (partition is None or partition.snapshot_id != scope.snapshot_id or
            partition.tenant_id != scope.tenant_id or partition.resource_id != scope.resource_id or
            partition.status != "IN_PROGRESS" or scope.chat_id not in (partition.payload or {}).get("chat_ids", [])):
            raise ChatCatalogueConflict("capture partition is no longer eligible")


async def _attempt(session, lease, *, terminal_ok=False):
    hint = await session.get(ChatCaptureAttempt, lease.attempt_id)
    if hint is None:
        raise ChatCatalogueConflict("capture attempt is missing")
    scope = _scope(hint)
    parents = await _lock_scope(session, scope)
    attempt = await _locked(session, ChatCaptureAttempt, ChatCaptureAttempt.id == lease.attempt_id)
    if attempt is None or attempt.generation != lease.generation:
        raise ChatCatalogueConflict("capture fence was superseded")
    if terminal_ok and attempt.state in _TERMINAL:
        return attempt, scope
    now = await session.scalar(select(func.clock_timestamp()))
    if (attempt.state not in _ACTIVE or attempt.staged_inventory_id != lease.staged_inventory_id or
        attempt.expires_at <= now):
        raise ChatCatalogueConflict("capture fence is expired or terminal")
    await _eligible(session, scope, parents)
    if attempt.authentication_context_sha256 != authentication_context_fingerprint(scope):
        raise ChatCatalogueIntegrityError("capture authentication context changed")
    return attempt, scope


async def begin_capture(session, *, scope: ChatCaptureScope, folder_path: str | None = None) -> CaptureLease:
    if folder_path is None:
        folder_path = f"chats/{scope.chat_id}"
    if not isinstance(folder_path, str) or not folder_path.strip():
        raise ValueError("folder_path must be a nonempty string")
    # Serialize only competing creators for this exact snapshot/chat fence.
    lock_key = int(_digest([str(scope.snapshot_id), scope.chat_id])[:16], 16)
    if lock_key >= 2 ** 63:
        lock_key -= 2 ** 64
    await session.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": lock_key})
    parents = await _lock_scope(session, scope)
    await _eligible(session, scope, parents)
    if await session.get(SnapshotChatInventory, (scope.snapshot_id, scope.chat_id)):
        raise ChatCatalogueConflict("snapshot chat inventory is already terminal")
    previous = await session.scalar(select(ChatCaptureAttempt).where(
        ChatCaptureAttempt.snapshot_id == scope.snapshot_id, ChatCaptureAttempt.chat_id == scope.chat_id)
        .order_by(ChatCaptureAttempt.generation.desc()).limit(1).with_for_update())
    now = await session.scalar(select(func.clock_timestamp()))
    if previous and previous.state in _ACTIVE:
        if previous.expires_at > now:
            raise ChatCatalogueConflict("snapshot chat already has an active capture")
        previous.state = "SUPERSEDED"
    attempt_id, inventory_id = uuid4(), uuid4()
    generation = previous.generation + 1 if previous else 1
    expires = now + timedelta(minutes=5)
    auth = authentication_context_fingerprint(scope)
    session.add(ChatCaptureAttempt(id=attempt_id, snapshot_id=scope.snapshot_id,
        resource_id=scope.resource_id, tenant_id=scope.tenant_id, chat_id=scope.chat_id, folder_path=folder_path,
        principal_id=scope.principal_id, permission_scope_hash=scope.permission_scope_hash,
        authentication_context_sha256=auth, backend_id=scope.backend_id, partition_id=scope.partition_id,
        generation=generation, expires_at=expires, staged_inventory_id=inventory_id))
    await session.flush()
    session.add(ChatInventory(id=inventory_id, capture_attempt_id=attempt_id, tenant_id=scope.tenant_id,
                              chat_id=scope.chat_id, authentication_context_sha256=auth))
    await session.flush()
    return CaptureLease(attempt_id, generation, expires, inventory_id)


async def renew_capture(session, *, lease: CaptureLease, lease_seconds: int = 300) -> CaptureLease:
    """Extend a live fence in a short DB transaction between external I/O calls."""
    if type(lease_seconds) is not int or lease_seconds <= 0:
        raise ValueError("lease_seconds must be a positive integer")
    attempt, _ = await _attempt(session, lease)
    now = await session.scalar(select(func.clock_timestamp()))
    attempt.expires_at = now + timedelta(seconds=lease_seconds)
    await session.flush()
    return CaptureLease(attempt.id, attempt.generation, attempt.expires_at, attempt.staged_inventory_id)


async def _check_expiry(session, attempt):
    if attempt.expires_at <= await session.scalar(select(func.clock_timestamp())):
        raise ChatCatalogueConflict("capture lease expired while waiting for database work")


def _descriptor_json(content):
    if content is None:
        return None
    values = asdict(content)
    for name in ("object_id", "backend_id"):
        if values[name] is not None:
            values[name] = str(values[name])
    return values


def _captured(version):
    content = dict(version.content) if version.content is not None else None
    if content is not None:
        for name in ("object_id", "backend_id"):
            if content[name] is not None:
                content[name] = UUID(content[name])
        content = ChatContentDescriptor(**content)
    return CapturedChatItem(version.item_type, version.graph_id, version.parent_graph_id, version.payload, content)


def _object_matches(obj, content, scope):
    return (obj.tenant_id == scope.tenant_id and obj.backend_id == scope.backend_id and
            all(getattr(obj, name) == getattr(content, name) for name in _OBJECT_FIELDS))


def _logical_size(item):
    if item.content is not None:
        return item.content.size
    return len(json.dumps(_payload(item.payload), sort_keys=True, separators=(",", ":"),
                          ensure_ascii=False, allow_nan=False).encode("utf-8"))


async def _objects(session, items, scope):
    descriptors = {}
    for item in items:
        content = item.content
        if content and content.object_id:
            if content.object_id in descriptors and any(getattr(descriptors[content.object_id], name) != getattr(content, name)
                                                       for name in _OBJECT_FIELDS):
                raise ChatCatalogueIntegrityError("one object has conflicting content descriptors")
            descriptors[content.object_id] = content
    if not descriptors:
        return {}
    rows = list((await session.scalars(select(ChatBlobObject).where(ChatBlobObject.id.in_(descriptors))
        .order_by(ChatBlobObject.id).with_for_update().execution_options(populate_existing=True))).all())
    objects = {obj.id: obj for obj in rows}
    if set(objects) != set(descriptors):
        raise ChatCatalogueIntegrityError("required catalogue object is missing")
    for object_id, content in descriptors.items():
        obj = objects[object_id]
        if not _object_matches(obj, content, scope) or obj.state in {"DELETING", "DELETED"}:
            raise ChatCatalogueIntegrityError("required catalogue object is retired or its descriptor differs")
    return objects


def _parent_key(scope, item, parents, *, allow_missing=False):
    if item.parent_graph_id is None:
        return scope.chat_id
    keys = parents.get(item.parent_graph_id, set())
    if not keys and allow_missing:
        return qualified_chat_item_key(scope.chat_id, "TEAMS_CHAT_MESSAGE", item.parent_graph_id)
    if len(keys) != 1:
        raise ChatCatalogueIntegrityError("child has a missing or ambiguous captured parent message")
    return next(iter(keys))


def _parents(scope, items):
    result = {}
    for item in items:
        if item.item_type in _MESSAGE_TYPES:
            result.setdefault(item.graph_id, set()).add(qualified_chat_item_key(
                scope.chat_id, item.item_type, item.graph_id, item.parent_graph_id))
    return result


async def stage_versions(session, *, lease: CaptureLease, items: list[CapturedChatItem]) -> StageResult:
    if len(items) > 200:
        raise ValueError("stage_versions accepts at most 200 descriptors")
    attempt, scope = await _attempt(session, lease)
    inventory = await _locked(session, ChatInventory, ChatInventory.id == lease.staged_inventory_id)
    if inventory is None or inventory.state != "STAGING":
        raise ChatCatalogueConflict("capture inventory is no longer staging")
    keyed = {}
    for item in items:
        key = qualified_chat_item_key(scope.chat_id, item.item_type, item.graph_id, item.parent_graph_id)
        version_hash = canonical_version_fingerprint(tenant_id=scope.tenant_id, chat_id=scope.chat_id, item=item)
        if key in keyed and keyed[key][0] != version_hash:
            raise ChatCatalogueIntegrityError("conflicting versions in one staging batch")
        keyed[key] = (version_hash, item)
    await _objects(session, items, scope)
    await _check_expiry(session, attempt)
    if not keyed:
        return StageResult(0, 0)
    # Hash lookup precedes payload construction/transmission for unchanged versions.
    version_keys = select(ChatItemVersion.id, ChatItemVersion.version_sha256).where(
        ChatItemVersion.tenant_id == scope.tenant_id, ChatItemVersion.chat_id == scope.chat_id,
        ChatItemVersion.version_sha256.in_([v[0] for v in keyed.values()]))
    versions = (await session.execute(version_keys)).all()
    by_hash = {v.version_sha256: v for v in versions}
    missing = []
    for key, (version_hash, item) in sorted(keyed.items()):
        if version_hash not in by_hash:
            missing.append(dict(id=uuid4(), tenant_id=scope.tenant_id, chat_id=scope.chat_id,
                logical_key=key, item_type=item.item_type, graph_id=item.graph_id, parent_graph_id=item.parent_graph_id,
                payload_sha256=canonical_payload_sha256(item.payload), version_sha256=version_hash,
                payload=_payload(item.payload), content=_descriptor_json(item.content),
                object_id=item.content.object_id if item.content else None))
    new_versions = 0
    if missing:
        inserted = (await session.execute(insert(ChatItemVersion).values(missing)
            .on_conflict_do_nothing(constraint="uq_chat_item_version").returning(ChatItemVersion.id))).all()
        new_versions = len(inserted)
        versions = (await session.execute(version_keys)).all()
        by_hash = {v.version_sha256: v for v in versions}
    existing_entries = dict((await session.execute(select(ChatInventoryItem.logical_key, ChatInventoryItem.version_id)
        .where(ChatInventoryItem.inventory_id == inventory.id, ChatInventoryItem.logical_key.in_(keyed)))).all())
    existing_links = {r.external_id: r for r in (await session.scalars(select(SnapshotItem).where(
        SnapshotItem.snapshot_id == scope.snapshot_id, SnapshotItem.external_id.in_(keyed)))).all()}
    prior_parents = (await session.execute(select(ChatItemVersion.graph_id, ChatItemVersion.item_type,
        ChatItemVersion.parent_graph_id).join(ChatInventoryItem,
        ChatInventoryItem.version_id == ChatItemVersion.id).where(ChatInventoryItem.inventory_id == inventory.id,
        ChatItemVersion.item_type.in_(_MESSAGE_TYPES)))).all()
    parents = _parents(scope, [*items, *prior_parents])
    entries, links = [], []
    for key, (version_hash, item) in sorted(keyed.items()):
        version = by_hash[version_hash]
        if key in existing_entries and existing_entries[key] != version.id:
            raise ChatCatalogueIntegrityError("staged logical item already has a different version")
        old_link = existing_links.get(key)
        if old_link is not None and (old_link.chat_version_id != version.id or old_link.item_type != item.item_type):
            raise ChatCatalogueIntegrityError("snapshot link already has a different version")
        if (key in existing_entries) != (old_link is not None):
            raise ChatCatalogueIntegrityError("staged entry and snapshot link disagree")
        if key in existing_entries:
            old_link.parent_external_id = _parent_key(scope, item, parents, allow_missing=True)
            continue
        entries.append(dict(inventory_id=inventory.id, logical_key=key, version_id=version.id))
        content = item.content
        if item.item_type in _MESSAGE_TYPES:
            body = item.payload.get("body")
            text_content = body.get("content") if isinstance(body, Mapping) else None
            name = text_content[:120] if isinstance(text_content, str) and text_content else item.graph_id
        else:
            name = item.payload.get("name") or item.graph_id
        links.append(dict(id=uuid4(), snapshot_id=scope.snapshot_id, tenant_id=scope.tenant_id,
            external_id=key, parent_external_id=_parent_key(scope, item, parents, allow_missing=True),
            item_type=item.item_type, name=str(name)[:255], folder_path=attempt.folder_path,
            content_hash=content.sha256 if content else None, content_checksum=content.sha256 if content else None,
            content_size=_logical_size(item), blob_path=content.path if content else None,
            encryption_key_id=content.encryption_key_id if content else None,
            backup_version=content.backup_version if content else 1,
            backend_id=content.backend_id if content and content.backend_id else scope.backend_id,
            chat_version_id=version.id, metadata={"chat_id": scope.chat_id}))
    if entries:
        await session.execute(insert(ChatInventoryItem).values(entries))
        await session.execute(insert(SnapshotItem.__table__).values(links))
    attempt.state = "ENUMERATING"
    await _check_expiry(session, attempt)
    return StageResult(len(entries), new_versions)


def _evidence_json(evidence):
    values = asdict(evidence)
    for name in ("started_at", "finished_at"):
        values[name] = values[name].isoformat()
    return values


def _evidence(value):
    if value is None:
        return None
    values = dict(value)
    for name in ("started_at", "finished_at"):
        values[name] = datetime.fromisoformat(values[name])
    return CaptureEvidence(**values)


async def record_capture_evidence(session, *, lease: CaptureLease, evidence: CaptureEvidence) -> None:
    attempt, _ = await _attempt(session, lease)
    attempt.evidence = _evidence_json(evidence)
    attempt.evidence_history = [*(attempt.evidence_history or []), attempt.evidence]
    attempt.state = "VERIFYING"
    await session.flush()


async def _staged(session, inventory, scope):
    # Lock narrow object records in one deterministic order. Heavy source JSON
    # is validated in bounded batches rather than retained for the whole chat.
    object_ids = select(ChatItemVersion.object_id).join(ChatInventoryItem,
        ChatInventoryItem.version_id == ChatItemVersion.id).where(
        ChatInventoryItem.inventory_id == inventory.id, ChatItemVersion.object_id.is_not(None))
    objects = {obj.id: obj for obj in (await session.scalars(select(ChatBlobObject).where(
        ChatBlobObject.id.in_(object_ids)).order_by(ChatBlobObject.id).with_for_update()
        .execution_options(populate_existing=True))).all()}
    statement = select(ChatInventoryItem.logical_key, ChatItemVersion,
        SnapshotItem.id.label("link_id"), SnapshotItem.chat_version_id.label("link_version"),
        SnapshotItem.tenant_id.label("link_tenant"), SnapshotItem.item_type.label("link_type"),
        SnapshotItem.parent_external_id.label("link_parent"), SnapshotItem.is_deleted.label("link_deleted"),
        SnapshotItem.content_size.label("link_size"), SnapshotItem.extra_data.label("link_metadata")) \
        .join(ChatItemVersion, ChatItemVersion.id == ChatInventoryItem.version_id) \
        .outerjoin(SnapshotItem, and_(SnapshotItem.snapshot_id == scope.snapshot_id,
            SnapshotItem.external_id == ChatInventoryItem.logical_key)) \
        .where(ChatInventoryItem.inventory_id == inventory.id).order_by(ChatInventoryItem.logical_key)
    rows = await session.stream(statement.execution_options(yield_per=25))
    expected, observations, representations, coverage, parents_to_check = set(), {}, [], [], []
    logical_bytes = durable = 0
    async for row in rows:
        key, version = row[0], row[1]
        item = _captured(version)
        if (version.tenant_id != scope.tenant_id or version.chat_id != scope.chat_id or
            key != version.logical_key or key != qualified_chat_item_key(scope.chat_id, item.item_type, item.graph_id, item.parent_graph_id) or
            version.version_sha256 != canonical_version_fingerprint(tenant_id=scope.tenant_id, chat_id=scope.chat_id, item=item) or
            version.payload_sha256 != canonical_payload_sha256(item.payload) or
            version.object_id != (item.content.object_id if item.content else None)):
            raise ChatCatalogueIntegrityError("inventory version identity or canonical hash differs")
        if key in observations or row.link_id is None or row.link_version != version.id or row.link_tenant != scope.tenant_id:
            raise ChatCatalogueIntegrityError("snapshot/chat links do not exactly match staged logical keys and versions")
        size = _logical_size(item)
        if (row.link_type != item.item_type or row.link_deleted or row.link_size != size or
                (row.link_metadata or {}).get("chat_id") != scope.chat_id):
            raise ChatCatalogueIntegrityError("snapshot link parent, type, visibility or logical size differs")
        content = item.content
        if content and content.object_id:
            obj = objects.get(content.object_id)
            if obj is None or not _object_matches(obj, content, scope) or obj.state in {"DELETING", "DELETED"}:
                raise ChatCatalogueIntegrityError("required catalogue object is retired or its descriptor differs")
        durable += int(content is not None and (content.object_id is None or objects[content.object_id].state == "LIVE"))
        source = ([content.source_drive_id, content.source_item_id, content.source_etag,
                   content.source_ctag, content.sha256, content.size] if content else None)
        observations[key] = _digest([_payload(item.payload), source])
        expected.add((key, version.id))
        representations.append((key, version.version_sha256))
        logical_bytes += size
        # Coverage needs child IDs and classifications, never body/card bytes.
        payload = {}
        if item.item_type in _MESSAGE_TYPES:
            manifest = item.payload.get("chat_catalogue_expected_children")
            payload["chat_catalogue_expected_children"] = manifest
            for field in ("attachments", "hostedContents"):
                children = item.payload.get(field, ())
                payload[field] = ([{"id": child.get("id"), "contentType": child.get("contentType", "")}
                    if isinstance(child, Mapping) else None for child in children]
                    if isinstance(children, (tuple, list)) else None)
        narrow = SimpleNamespace(item_type=item.item_type, graph_id=item.graph_id,
            parent_graph_id=item.parent_graph_id, payload=payload, content=True if content is not None else None)
        coverage.append(narrow)
        parents_to_check.append((row.link_parent, narrow))
    await rows.close()
    count = await session.scalar(select(func.count()).select_from(SnapshotItem).outerjoin(ChatItemVersion,
        ChatItemVersion.id == SnapshotItem.chat_version_id).where(SnapshotItem.snapshot_id == scope.snapshot_id,
        or_(SnapshotItem.extra_data["chat_id"].as_string() == scope.chat_id, ChatItemVersion.chat_id == scope.chat_id)))
    if count != len(expected):
        raise ChatCatalogueIntegrityError("snapshot/chat links do not exactly match staged logical keys and versions")
    parents = _parents(scope, coverage)
    for link_parent, item in parents_to_check:
        if link_parent != _parent_key(scope, item, parents):
            raise ChatCatalogueIntegrityError("snapshot link parent, type, visibility or logical size differs")
    required, gaps = _coverage(scope, coverage)
    return SimpleNamespace(expected=expected, item_count=len(expected), logical_bytes=logical_bytes,
        source_hash=_digest(["chat-source-inventory-v1", authentication_context_fingerprint(scope), sorted(observations.items())]),
        representation_hash=_digest(representations), durable=durable, required=required, gaps=gaps, objects=objects)


def _result(attempt, association, inventory, uploaded_bytes=None):
    if uploaded_bytes is None:
        uploaded_bytes = association.uploaded_bytes
    return CaptureResult(association.coverage_status.lower(), inventory.id, attempt.reused_inventory,
        inventory.item_count, inventory.logical_bytes, uploaded_bytes,
        association.started_at, association.finished_at, association.failure_reason)


def _coverage(scope, items):
    actual = {qualified_chat_item_key(scope.chat_id, item.item_type, item.graph_id, item.parent_graph_id): item
              for item in items if item.item_type in {"CHAT_ATTACHMENT", "CHAT_HOSTED_CONTENT"}}
    expected, gaps = {}, []

    def expect(parent, kind, graph_id, required):
        if kind not in {"CHAT_ATTACHMENT", "CHAT_HOSTED_CONTENT"} or not isinstance(graph_id, str) or not graph_id.strip() or type(required) is not bool:
            gaps.append({"kind": "invalid_child_manifest", "parent_graph_id": parent.graph_id})
            return
        key = qualified_chat_item_key(scope.chat_id, kind, graph_id, parent.graph_id)
        if key in expected and expected[key] != required:
            gaps.append({"kind": "conflicting_child_manifest", "logical_key": key})
        expected[key] = expected.get(key, False) or required

    for parent in items:
        if parent.item_type not in _MESSAGE_TYPES:
            continue
        manifest = parent.payload.get("chat_catalogue_expected_children")
        if not isinstance(manifest, Mapping) or manifest.get("complete") is not True or not isinstance(manifest.get("items"), (tuple, list)):
            gaps.append({"kind": "incomplete_child_manifest", "parent_graph_id": parent.graph_id})
        else:
            for entry in manifest["items"]:
                if not isinstance(entry, Mapping):
                    gaps.append({"kind": "invalid_child_manifest", "parent_graph_id": parent.graph_id})
                    continue
                expect(parent, entry.get("item_type"), entry.get("graph_id"), entry.get("requires_content"))
        for field, kind in (("attachments", "CHAT_ATTACHMENT"), ("hostedContents", "CHAT_HOSTED_CONTENT")):
            children = parent.payload.get(field, ())
            if not isinstance(children, (tuple, list)):
                gaps.append({"kind": "invalid_child_manifest", "parent_graph_id": parent.graph_id})
                continue
            for child in children:
                if not isinstance(child, Mapping):
                    gaps.append({"kind": "invalid_child_manifest", "parent_graph_id": parent.graph_id})
                    continue
                content_type = child.get("contentType", "")
                card = isinstance(content_type, str) and (content_type.startswith("application/vnd.microsoft.card") or
                    content_type in {"messageReference", "forwardedMessageReference", "meetingReference"})
                expect(parent, kind, child.get("id"), kind == "CHAT_HOSTED_CONTENT" or not card)
    gaps.extend({"kind": "missing_child", "logical_key": key} for key in sorted(set(expected) - set(actual)))
    gaps.extend({"kind": "unmanifested_child", "logical_key": key} for key in sorted(set(actual) - set(expected)))
    required_keys = {key for key, required in expected.items() if required}
    required_keys.update(qualified_chat_item_key(scope.chat_id, item.item_type, item.graph_id, item.parent_graph_id)
                         for item in items if item.content is not None)
    return len(required_keys), gaps


async def publish_inventory(session, *, lease: CaptureLease) -> CaptureResult:
    attempt, scope = await _attempt(session, lease, terminal_ok=True)
    association = await session.get(SnapshotChatInventory, (scope.snapshot_id, scope.chat_id))
    if association is not None:
        if association.capture_attempt_id != attempt.id or attempt.state not in _TERMINAL:
            raise ChatCatalogueConflict("snapshot chat already has another terminal capture")
        inventory = await session.get(ChatInventory, association.inventory_id)
        return _result(attempt, association, inventory)
    if attempt.state in _TERMINAL:
        raise ChatCatalogueIntegrityError("terminal capture has no snapshot association")
    inventory = await _locked(session, ChatInventory, ChatInventory.id == attempt.staged_inventory_id)
    if (inventory is None or inventory.state != "STAGING" or inventory.capture_attempt_id != attempt.id or
        inventory.tenant_id != scope.tenant_id or inventory.chat_id != scope.chat_id or
        inventory.authentication_context_sha256 != authentication_context_fingerprint(scope)):
        raise ChatCatalogueIntegrityError("staged inventory scope differs")
    staged = await _staged(session, inventory, scope)
    expected, objects = staged.expected, staged.objects
    await _check_expiry(session, attempt)
    source_hash, representation_hash = staged.source_hash, staged.representation_hash
    evidence = _evidence(attempt.evidence)
    durable, required, gaps = staged.durable, staged.required, staged.gaps
    complete = (not attempt.failure_reason and evidence is not None and evidence.satisfies_completeness_conditions and
                evidence.membership_sha256 == source_hash and evidence.required_content_count == required and
                evidence.durable_content_count == durable and required == durable and not gaps)
    now = await session.scalar(select(func.clock_timestamp()))
    started = evidence.started_at if evidence else attempt.created_at
    finished = evidence.finished_at if evidence else now
    reason = None if complete else attempt.failure_reason or "source coverage, reconciliation or durable content is incomplete"
    if not complete and not staged.item_count:
        attempt.state, attempt.failure_reason = "FAILED", reason
        await session.flush()
        return CaptureResult("failed", None, False, 0, 0, 0, started, finished, reason)
    inventory.membership_sha256 = source_hash
    inventory.representation_sha256 = representation_hash
    inventory.item_count = staged.item_count
    inventory.logical_bytes = staged.logical_bytes
    inventory.evidence = {**(attempt.evidence or {}), "validation_gaps": gaps,
        "persisted_required_content_count": required, "persisted_durable_content_count": durable,
        "persisted_membership_sha256": source_hash}
    inventory.failure_reason = reason
    await session.flush()
    chosen = inventory
    if complete:
        identity = (ChatInventory.tenant_id == scope.tenant_id, ChatInventory.chat_id == scope.chat_id,
            ChatInventory.authentication_context_sha256 == authentication_context_fingerprint(scope),
            ChatInventory.membership_sha256 == source_hash, ChatInventory.representation_sha256 == representation_hash,
            ChatInventory.state == "COMPLETE")
        chosen = await _locked(session, ChatInventory, *identity)
        if chosen is None:
            try:
                async with session.begin_nested():
                    await session.execute(update(ChatInventory).where(ChatInventory.id == inventory.id).values(state="COMPLETE"))
                chosen = inventory
            except IntegrityError as exc:
                if "uq_chat_complete_inventory" not in str(exc.orig):
                    raise
                chosen = await _locked(session, ChatInventory, *identity)
                if chosen is None:
                    raise ChatCatalogueIntegrityError("complete inventory conflict has no winner") from exc
        if chosen.id != inventory.id:
            retained = set((await session.execute(select(ChatInventoryItem.logical_key, ChatInventoryItem.version_id)
                .where(ChatInventoryItem.inventory_id == chosen.id))).all())
            if retained != expected:
                raise ChatCatalogueIntegrityError("reusable inventory membership differs")
            attempt.reused_inventory = True
            attempt.staged_inventory_id = chosen.id
            await session.flush()
            await session.execute(delete(ChatInventory).where(ChatInventory.id == inventory.id))
    else:
        inventory.state = "PARTIAL"
    association = SnapshotChatInventory(snapshot_id=scope.snapshot_id, chat_id=scope.chat_id,
        tenant_id=scope.tenant_id, inventory_id=chosen.id, capture_attempt_id=attempt.id,
        coverage_status="COMPLETE" if complete else "PARTIAL", started_at=started, finished_at=finished, failure_reason=reason)
    session.add(association)
    await _check_expiry(session, attempt)
    attempt.state = "PUBLISHED" if complete else "PARTIAL"
    uploaded = sum(obj.uploaded_bytes for obj in objects.values() if obj.capture_attempt_id == attempt.id and obj.state == "LIVE")
    association.uploaded_bytes = uploaded
    await session.flush()
    return _result(attempt, association, chosen, uploaded)


async def abort_capture(session, *, lease: CaptureLease, reason: str) -> None:
    if not isinstance(reason, str) or not reason.strip():
        raise ValueError("abort reason is required")
    attempt, _ = await _attempt(session, lease)
    attempt.failure_reason = reason
    await session.flush()
    await publish_inventory(session, lease=lease)


async def materialize_chat_items(session, items: list) -> list:
    """Return detached item-shaped objects; never mutate tracked ORM rows."""
    ids = {item.chat_version_id for item in items if item.chat_version_id is not None}
    versions = {v.id: v for v in (await session.scalars(select(ChatItemVersion).where(ChatItemVersion.id.in_(ids)))).all()} if ids else {}
    snapshot_ids = {item.snapshot_id for item in items}
    snapshots = {s.id: s for s in (await session.scalars(select(Snapshot).where(Snapshot.id.in_(snapshot_ids)))).all()} if snapshot_ids else {}
    object_ids = {v.object_id for v in versions.values() if v.object_id is not None}
    objects = {obj.id: obj for obj in (await session.scalars(select(ChatBlobObject).where(ChatBlobObject.id.in_(object_ids)))).all()} if object_ids else {}
    result = []
    for item in items:
        values = {prop.key: getattr(item, prop.key) for prop in SnapshotItem.__mapper__.column_attrs}
        if item.chat_version_id is None:
            if (snapshots[item.snapshot_id].extra_data or {}).get("chat_inventory_format") == 1:
                raise ChatCatalogueIntegrityError("new-format snapshot link has no captured version")
            values["extra_data"] = deepcopy(item.extra_data or {})
        else:
            version = versions.get(item.chat_version_id)
            if version is None or version.tenant_id != item.tenant_id:
                raise ChatCatalogueIntegrityError("snapshot link has no owned captured version")
            captured = _captured(version)
            if canonical_version_fingerprint(tenant_id=version.tenant_id, chat_id=version.chat_id, item=captured) != version.version_sha256:
                raise ChatCatalogueIntegrityError("captured version content differs from its canonical hash")
            if item.external_id != qualified_chat_item_key(version.chat_id, version.item_type, version.graph_id, version.parent_graph_id):
                raise ChatCatalogueIntegrityError("snapshot link identity differs from captured version")
            payload = _payload(captured.payload)
            payload.pop("chat_catalogue_expected_children", None)
            metadata = ({"raw": payload, "chatId": version.chat_id} if version.item_type in _MESSAGE_TYPES
                        else {**payload, "chat_id": version.chat_id, "parent_item_id": version.parent_graph_id,
                            "content_type": payload.get("contentType", payload.get("content_type")),
                            "content_url": payload.get("contentUrl", payload.get("content_url")),
                            "resolved": captured.content is not None})
            values.update(external_id=version.graph_id, parent_external_id=version.parent_graph_id or version.chat_id,
                          extra_data=metadata, content_size=_logical_size(captured))
            if captured.content:
                content = captured.content
                if content.object_id is not None:
                    obj = objects.get(content.object_id)
                    if obj is None or obj.state != "LIVE" or not _object_matches(obj, content,
                        SimpleNamespace(tenant_id=item.tenant_id, backend_id=content.backend_id)):
                        raise ChatCatalogueIntegrityError("captured object is missing, retired or inconsistent")
                values.update(content_hash=content.sha256, content_checksum=content.sha256,
                    content_size=content.size, blob_path=content.path,
                    encryption_key_id=content.encryption_key_id, backup_version=content.backup_version,
                    backend_id=content.backend_id or snapshots[item.snapshot_id].backend_id)
                values["extra_data"].update(_descriptor_json(captured.content))
        result.append(SimpleNamespace(**values))
    return result
