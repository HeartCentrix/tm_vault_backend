"""Pure chat catalogue identities and validated capture records.

Fingerprints describe observations, not Graph visibility or persisted durability.
Publication must independently verify fenced, persisted evidence and object state.
"""
from __future__ import annotations

import base64
import binascii
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
import hashlib
import json
import re
from types import MappingProxyType
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from uuid import UUID


_CHILD_TYPES = {"CHAT_ATTACHMENT", "CHAT_HOSTED_CONTENT"}
# Only worker-owned root fields; underscore-prefixed Graph extensions are data.
_WORKER_FIELDS = {"_chat_folder_path", "_full_folder_path", "_skip_content_fetch",
                  "_capture_attempt_id", "_capture_generation"}
_DOWNLOAD_FIELDS = {"@microsoft.graph.downloadUrl", "downloadUrl", "download_url"}
_URL_FIELDS = {"contentUrl", "content_url", "thumbnailUrl", "thumbnail_url"}
_AUTH_QUERY_FIELDS = {"sig", "signature", "access_token", "authkey", "tempauth",
                      "se", "st", "sp", "sv", "sr", "si", "sip", "spr",
                      "skoid", "sktid", "skt", "ske", "sks", "skv", "sdd"}
_AUTH_QUERY_MARKERS = {"sig", "signature", "access_token", "authkey", "tempauth",
                       "x-amz-signature", "x-goog-signature"}


def _text(value: str, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a nonempty string")


def _uuid(value: UUID, name: str) -> None:
    if not isinstance(value, UUID):
        raise TypeError(f"{name} must be a UUID")


def _count(value: int, name: str) -> None:
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} must be a nonnegative integer")


def _sha256(value: str, name: str) -> None:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256 hex digest")


def _utc(value: datetime, name: str) -> None:
    if not isinstance(value, datetime) or value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be timezone-aware UTC")


def _interval(started_at: datetime, finished_at: datetime) -> None:
    _utc(started_at, "started_at")
    _utc(finished_at, "finished_at")
    if finished_at < started_at:
        raise ValueError("finished_at precedes started_at")


def _without_url_credentials(value: str) -> str:
    parsed = urlsplit(value)
    query = parse_qsl(parsed.query, keep_blank_values=True)

    def credential(name: str) -> bool:
        lowered = name.lower()
        return lowered in _AUTH_QUERY_FIELDS or lowered.startswith(("x-amz-", "x-goog-"))

    if not any(name.lower() in _AUTH_QUERY_MARKERS for name, _ in query):
        return value
    retained = [(name, val) for name, val in query
                if not credential(name) and name.lower() != "expires"]
    return urlunsplit(parsed._replace(query=urlencode(sorted(retained))))


def _json_value(value: Any, *, root: bool = False) -> Any:
    if isinstance(value, Mapping):
        result = {}
        for key, child in value.items():
            if not isinstance(key, str):
                raise TypeError("JSON object keys must be strings")
            if key in _DOWNLOAD_FIELDS or (root and key in _WORKER_FIELDS):
                continue
            if key in _URL_FIELDS and isinstance(child, str):
                child = _without_url_credentials(child)
            result[key] = _json_value(child)
        return result
    if isinstance(value, (list, tuple)):
        return [_json_value(child) for child in value]
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    raise TypeError("payload must contain JSON values")


def _payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise TypeError("payload must be a JSON object")
    value = _json_value(payload, root=True)
    # Reject NaN/Infinity as well as non-JSON objects before capturing them.
    json.dumps(value, allow_nan=False)
    return value


def _freeze_json(value: Any) -> Any:
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze_json(child) for key, child in value.items()})
    if isinstance(value, list):
        return tuple(_freeze_json(child) for child in value)
    return value


def _digest(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"),
                         ensure_ascii=False, allow_nan=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def qualified_chat_item_key(chat_id: str, item_type: str, graph_id: str,
                            parent_graph_id: str | None = None) -> str:
    """Hash raw chat/type/item/parent identity without delimiter ambiguity."""
    for name, value in (("chat_id", chat_id), ("item_type", item_type), ("graph_id", graph_id)):
        _text(value, name)
    if parent_graph_id is not None or item_type in _CHILD_TYPES:
        _text(parent_graph_id, "parent_graph_id")
    return _digest([chat_id, item_type, graph_id, parent_graph_id])


def canonical_payload_sha256(payload: Mapping[str, Any]) -> str:
    """Hash source JSON while retaining metadata and excluding known controls."""
    return _digest(_payload(payload))


@dataclass(frozen=True)
class ChatCaptureScope:
    snapshot_id: UUID
    resource_id: UUID
    tenant_id: UUID
    chat_id: str
    principal_id: str
    permission_scope_hash: str
    backend_id: UUID
    partition_id: UUID | None = None
    snapshot_lease_token: int | None = None
    job_lease_token: int | None = None
    partition_lease_token: int | None = None

    def __post_init__(self) -> None:
        for name in ("snapshot_id", "resource_id", "tenant_id", "backend_id"):
            _uuid(getattr(self, name), name)
        if self.partition_id is not None:
            _uuid(self.partition_id, "partition_id")
        for name in ("snapshot_lease_token", "job_lease_token", "partition_lease_token"):
            if getattr(self, name) is not None:
                _count(getattr(self, name), name)
        for name in ("chat_id", "principal_id", "permission_scope_hash"):
            _text(getattr(self, name), name)


@dataclass(frozen=True)
class CaptureLease:
    attempt_id: UUID
    generation: int
    expires_at: datetime
    staged_inventory_id: UUID

    def __post_init__(self) -> None:
        _uuid(self.attempt_id, "attempt_id")
        _uuid(self.staged_inventory_id, "staged_inventory_id")
        _count(self.generation, "generation")
        _utc(self.expires_at, "expires_at")


@dataclass(frozen=True)
class ChatContentDescriptor:
    object_id: UUID | None
    backend_id: UUID | None
    container: str | None
    path: str | None
    sha256: str
    size: int
    encryption_key_id: str | None
    backup_version: int | None
    inline_b64: str | None
    source_drive_id: str | None
    source_item_id: str | None
    source_etag: str | None
    source_ctag: str | None

    def __post_init__(self) -> None:
        _sha256(self.sha256, "sha256")
        _count(self.size, "size")
        if (self.object_id is None) == (self.inline_b64 is None):
            raise ValueError("content requires exactly one object or inline representation")
        if self.backend_id is not None:
            _uuid(self.backend_id, "backend_id")
        if self.object_id is not None:
            _uuid(self.object_id, "object_id")
            _uuid(self.backend_id, "backend_id")
            _text(self.container, "container")
            _text(self.path, "path")
            if urlsplit(self.path).scheme or "?" in self.path or "?" in self.container:
                raise ValueError("content location must not be a download URL")
        else:
            if self.container is not None or self.path is not None:
                raise ValueError("inline content cannot have a container or path")
            try:
                raw = base64.b64decode(self.inline_b64, validate=True)
            except (binascii.Error, ValueError, TypeError) as exc:
                raise ValueError("inline content must be valid base64") from exc
            if len(raw) != self.size or hashlib.sha256(raw).hexdigest() != self.sha256:
                raise ValueError("inline content does not match its size and checksum")
        if (self.source_drive_id is None) != (self.source_item_id is None):
            raise ValueError("source file identity requires both drive and item IDs")
        for name in ("encryption_key_id", "source_drive_id", "source_item_id", "source_etag", "source_ctag"):
            if getattr(self, name) is not None:
                _text(getattr(self, name), name)
        if self.backup_version is not None:
            _count(self.backup_version, "backup_version")


@dataclass(frozen=True)
class CapturedChatItem:
    item_type: str
    graph_id: str
    parent_graph_id: str | None
    payload: Mapping[str, Any]
    content: ChatContentDescriptor | None

    def __post_init__(self) -> None:
        qualified_chat_item_key("validation", self.item_type, self.graph_id, self.parent_graph_id)
        object.__setattr__(self, "payload", _freeze_json(_payload(self.payload)))
        if self.content is not None and not isinstance(self.content, ChatContentDescriptor):
            raise TypeError("content must be a ChatContentDescriptor")
        content_type = self.payload.get("contentType", self.payload.get("content_type", ""))
        metadata_only = isinstance(content_type, str) and (
            content_type.startswith("application/vnd.microsoft.card") or
            content_type in {"messageReference", "forwardedMessageReference", "meetingReference"})
        requires_binary = self.item_type == "CHAT_HOSTED_CONTENT" or (
            self.item_type == "CHAT_ATTACHMENT" and not metadata_only)
        if requires_binary and self.content is None:
            raise ValueError("required binary content has no content descriptor")


@dataclass(frozen=True)
class CaptureEvidence:
    enumeration_complete: bool
    reconciliation_complete: bool
    membership_sha256: str
    reconciliation_sha256: str
    conflicting_observations: int
    required_content_count: int
    durable_content_count: int
    unresolved_required_count: int
    started_at: datetime
    finished_at: datetime

    def __post_init__(self) -> None:
        for name in ("enumeration_complete", "reconciliation_complete"):
            if type(getattr(self, name)) is not bool:
                raise TypeError(f"{name} must be a boolean")
        for name in ("membership_sha256", "reconciliation_sha256"):
            _sha256(getattr(self, name), name)
        for name in ("conflicting_observations", "required_content_count", "durable_content_count", "unresolved_required_count"):
            _count(getattr(self, name), name)
        _interval(self.started_at, self.finished_at)

    @property
    def satisfies_completeness_conditions(self) -> bool:
        """Necessary reported conditions; never substitutes for repository checks."""
        return (self.enumeration_complete and self.reconciliation_complete and
                self.membership_sha256 == self.reconciliation_sha256 and
                self.conflicting_observations == 0 and self.unresolved_required_count == 0 and
                self.required_content_count == self.durable_content_count)


@dataclass(frozen=True)
class CaptureResult:
    outcome: str
    inventory_id: UUID | None
    reused: bool
    item_count: int
    logical_bytes: int
    uploaded_bytes: int
    started_at: datetime
    finished_at: datetime
    failure_reason: str | None

    def __post_init__(self) -> None:
        if self.outcome not in {"complete", "partial", "failed"}:
            raise ValueError("unknown capture outcome")
        if type(self.reused) is not bool:
            raise TypeError("reused must be a boolean")
        for name in ("item_count", "logical_bytes", "uploaded_bytes"):
            _count(getattr(self, name), name)
        _interval(self.started_at, self.finished_at)
        if self.inventory_id is not None:
            _uuid(self.inventory_id, "inventory_id")
        if self.outcome == "complete":
            if self.inventory_id is None or self.failure_reason is not None:
                raise ValueError("complete capture requires an inventory and no failure reason")
        else:
            _text(self.failure_reason, "failure_reason")
            if self.reused:
                raise ValueError("only a complete capture can reuse an inventory")
            if self.outcome == "partial" and (self.inventory_id is None or self.item_count == 0):
                raise ValueError("partial capture requires an inventory with captured items")
            if self.outcome == "failed" and (self.inventory_id is not None or self.item_count != 0):
                raise ValueError("failed capture cannot claim a captured inventory")


@dataclass(frozen=True)
class StageResult:
    staged_items: int
    new_versions: int

    def __post_init__(self) -> None:
        _count(self.staged_items, "staged_items")
        _count(self.new_versions, "new_versions")
        if self.new_versions > self.staged_items:
            raise ValueError("new_versions exceeds successfully staged_items")


def authentication_context_fingerprint(scope: ChatCaptureScope) -> str:
    """Identify recorded credentials/target context; this proves no visibility."""
    return _digest(["chat-auth-context-v1", str(scope.tenant_id), scope.chat_id,
                    scope.principal_id, scope.permission_scope_hash])


def canonical_version_fingerprint(*, tenant_id: UUID, chat_id: str,
                                  item: CapturedChatItem) -> str:
    """Identify an exact captured version, including its immutable representation."""
    _uuid(tenant_id, "tenant_id")
    descriptor = asdict(item.content) if item.content else None
    if descriptor is not None:
        for name in ("object_id", "backend_id"):
            if descriptor[name] is not None:
                descriptor[name] = str(descriptor[name])
    return _digest(["chat-item-version-v1", str(tenant_id),
                    qualified_chat_item_key(chat_id, item.item_type, item.graph_id, item.parent_graph_id),
                    _payload(item.payload), descriptor])


def source_inventory_fingerprint(*, scope: ChatCaptureScope,
                                 items: Iterable[CapturedChatItem]) -> str:
    """Hash observed source membership and versions, independently of storage.

    Identical observations deduplicate. Conflicts require reconciliation instead
    of selecting an arbitrary observation. A hash alone certifies no completeness.
    """
    observations = {}
    for item in items:
        key = qualified_chat_item_key(scope.chat_id, item.item_type, item.graph_id, item.parent_graph_id)
        content = item.content
        source = ([content.source_drive_id, content.source_item_id, content.source_etag,
                   content.source_ctag, content.sha256, content.size] if content else None)
        semantic = _digest([_payload(item.payload), source])
        if key in observations and observations[key] != semantic:
            raise ValueError("conflicting observations for one logical chat item")
        observations[key] = semantic
    return _digest(["chat-source-inventory-v1", authentication_context_fingerprint(scope),
                    sorted(observations.items())])
