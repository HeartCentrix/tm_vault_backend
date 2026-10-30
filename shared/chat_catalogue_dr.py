"""Verified catalogue DR archives and cache-independent, streaming recovery.

Source records remain exact. Replica records separately map immutable object IDs
to Azure plaintext streams protected by the verified DR server-side policy.
Recovery needs only the DR store and a trusted manifest identity, never a primary
message cache, source storage credential or Seaweed DEK. Automatic application
restore routing is deliberately not advertised by this archive format.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from uuid import UUID, uuid4

from sqlalchemy import select

from shared.chat_catalogue import ChatCatalogueIntegrityError
from shared.chat_catalogue_gc import catalogue_read_pin
from shared.chat_catalogue_identity import (
    CapturedChatItem, ChatCaptureScope, ChatContentDescriptor, _digest, _payload,
    authentication_context_fingerprint, canonical_payload_sha256,
    canonical_version_fingerprint, qualified_chat_item_key,
)
from shared.models import (ChatBlobObject, ChatCaptureAttempt, ChatInventory,
    ChatInventoryItem, ChatItemVersion, Resource, ResourceType, SlaPolicy, Snapshot,
    SnapshotChatInventory, SnapshotItem, SnapshotStatus, StorageBackend)

FORMAT = "chat-catalogue-dr-v1"
CHUNK_BYTES = 1024 * 1024
MAX_RECORD_BYTES = 8 * 1024 * 1024
MAX_MANIFEST_BYTES = 1024 * 1024 * 1024
_MESSAGE_TYPES = {"TEAMS_CHAT_MESSAGE", "TEAMS_MESSAGE", "TEAMS_MESSAGE_REPLY"}
_OBJECT_FIELDS = ("backend_id", "container", "path", "sha256", "size", "encryption_key_id", "backup_version")


@dataclass(frozen=True)
class CatalogueDRArchive:
    container: str
    path: str
    sha256: str
    item_count: int
    object_count: int
    automatic_restore_supported: bool = False


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _plain(value):
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "value"):
        return value.value
    if isinstance(value, dict):
        return {key: _plain(child) for key, child in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(child) for child in value]
    return value


def _row(model):
    return {prop.key: _plain(getattr(model, prop.key)) for prop in model.__mapper__.column_attrs}


def _captured(version):
    descriptor = dict(version["content"]) if version.get("content") is not None else None
    if descriptor is not None:
        for name in ("object_id", "backend_id"):
            if descriptor[name] is not None:
                descriptor[name] = UUID(descriptor[name])
        descriptor = ChatContentDescriptor(**descriptor)
    return CapturedChatItem(version["item_type"], version["graph_id"], version["parent_graph_id"], version["payload"], descriptor)


def _logical_size(item):
    return item.content.size if item.content else len(_json(_payload(item.payload)))


def _policy_matches(policy, encryption):
    if (encryption.get("mode") != "azure_server_side" or
        bool(encryption.get("scope")) != bool(encryption.get("key_uri"))):
        raise ChatCatalogueIntegrityError("DR encryption mapping is unsupported")
    if policy.get("mode") == "CUSTOMER_KEY":
        uri, name = policy.get("key_vault_uri"), policy.get("key_name")
        if not uri or not name or not encryption.get("scope"):
            raise ChatCatalogueIntegrityError("customer key DR scope is not configured")
        prefix = uri.rstrip("/") + "/keys/" + name + "/"
        version = policy.get("key_version_resolved") or policy.get("key_version")
        observed = encryption["key_uri"]
        if not observed.startswith(prefix) or not observed[len(prefix):] or (version and observed != prefix + version):
            raise ChatCatalogueIntegrityError("DR customer key mapping differs from required policy")
    elif policy.get("mode") != "VAULT_MANAGED":
        raise ChatCatalogueIntegrityError("source encryption policy is unknown")


class _Writer:
    def __init__(self, file):
        self.file, self.digest, self.count = file, hashlib.sha256(), 0

    def record(self, kind, data):
        line = _json({"type": kind, "data": data}) + b"\n"
        if len(line) > MAX_RECORD_BYTES or self.file.tell() + len(line) > MAX_MANIFEST_BYTES:
            raise ChatCatalogueIntegrityError("DR manifest exceeds its bounded metadata size")
        self.file.write(line)
        self.digest.update(line)
        self.count += 1

    def seal(self):
        self.file.write(_json({"type": "seal", "record_count": self.count, "sha256": self.digest.hexdigest()}) + b"\n")
        self.file.flush()


async def _source_manifest(session_factory, snapshot_id, tenant_id, writer, target, container, encryption, prefix):
    async with session_factory() as session:
        snapshot = await session.get(Snapshot, snapshot_id)
        resource = await session.get(Resource, snapshot.resource_id) if snapshot else None
        if (snapshot is None or resource is None or resource.tenant_id != tenant_id or
            resource.type != ResourceType.USER_CHATS or snapshot.status not in {SnapshotStatus.COMPLETED, SnapshotStatus.PARTIAL} or
            (snapshot.extra_data or {}).get("chat_inventory_format") != 1):
            raise ChatCatalogueIntegrityError("DR snapshot scope is not a terminal catalogue recovery point")
        policy_row = await session.get(SlaPolicy, resource.sla_policy_id) if resource.sla_policy_id else None
        policy = {"mode": policy_row.encryption_mode if policy_row else "VAULT_MANAGED"}
        if policy_row:
            policy.update({name: getattr(policy_row, name) for name in
                ("key_vault_uri", "key_name", "key_version", "key_version_resolved")})
        _policy_matches(policy, encryption)
        snapshot_data = {key: value for key, value in _row(snapshot).items()
            if not key.startswith("dr_") and key not in {"delta_token", "delta_tokens_json"}}
        writer.record("snapshot", {"format": FORMAT, "tenant_id": str(tenant_id), "snapshot": snapshot_data,
            "target": {"kind": target.kind, "backend_id": target.backend_id, "container": container,
                       "prefix": prefix}, "encryption": encryption, "source_policy": policy})
        inventory_ids = select(SnapshotChatInventory.inventory_id).where(SnapshotChatInventory.snapshot_id == snapshot_id)
        version_ids = select(ChatInventoryItem.version_id).where(ChatInventoryItem.inventory_id.in_(inventory_ids)).union(
            select(SnapshotItem.chat_version_id).where(SnapshotItem.snapshot_id == snapshot_id, SnapshotItem.chat_version_id.is_not(None)))
        object_ids = select(ChatItemVersion.object_id).where(ChatItemVersion.id.in_(version_ids), ChatItemVersion.object_id.is_not(None))
        attempts = select(SnapshotChatInventory.capture_attempt_id).where(SnapshotChatInventory.snapshot_id == snapshot_id).union(
            select(ChatInventory.capture_attempt_id).where(ChatInventory.id.in_(inventory_ids)))
        backend_ids = select(ChatBlobObject.backend_id).where(ChatBlobObject.id.in_(object_ids)).union(
            select(Snapshot.backend_id).where(Snapshot.id == snapshot_id))
        backends = (await session.execute(select(StorageBackend.id, StorageBackend.kind, StorageBackend.name)
            .where(StorageBackend.id.in_(backend_ids)))).all()
        for backend in backends:
            writer.record("backend", {"id": str(backend.id), "kind": backend.kind, "name": backend.name})
        statements = (
            ("attempt", select(ChatCaptureAttempt).where(ChatCaptureAttempt.id.in_(attempts)).order_by(ChatCaptureAttempt.id)),
            ("inventory", select(ChatInventory).where(ChatInventory.id.in_(inventory_ids)).order_by(ChatInventory.id)),
            ("association", select(SnapshotChatInventory).where(SnapshotChatInventory.snapshot_id == snapshot_id).order_by(SnapshotChatInventory.chat_id)),
            ("inventory_item", select(ChatInventoryItem).where(ChatInventoryItem.inventory_id.in_(inventory_ids)).order_by(ChatInventoryItem.inventory_id, ChatInventoryItem.logical_key)),
            ("version", select(ChatItemVersion).where(ChatItemVersion.id.in_(version_ids)).order_by(ChatItemVersion.id)),
            ("link", select(SnapshotItem).where(SnapshotItem.snapshot_id == snapshot_id).order_by(SnapshotItem.id)),
            ("object", select(ChatBlobObject).where(ChatBlobObject.id.in_(object_ids)).order_by(ChatBlobObject.id)),
        )
        for kind, statement in statements:
            rows = await session.stream_scalars(statement.execution_options(yield_per=25))
            try:
                async for model in rows:
                    writer.record(kind, _row(model))
            finally:
                await rows.close()


async def _checked_stream(stream, *, size, sha256, lease=None):
    digest, total = hashlib.sha256(), 0
    try:
        async for chunk in stream:
            if lease:
                lease.assert_valid()
            if not isinstance(chunk, bytes) or len(chunk) > CHUNK_BYTES:
                raise ChatCatalogueIntegrityError("replica stream exceeds bounded chunk size")
            total += len(chunk)
            if total > size:
                raise ChatCatalogueIntegrityError("replica size exceeds captured size")
            digest.update(chunk)
            yield chunk
        if total != size or digest.hexdigest() != sha256:
            raise ChatCatalogueIntegrityError("replica checksum or size differs from captured bytes")
        if lease:
            lease.assert_valid()
    finally:
        close = getattr(stream, "aclose", None)
        if close:
            await close()


async def _verify_target(target, container, path, size, sha256, encryption):
    await target.verify_encryption(container, path, encryption)
    async for _ in _checked_stream(target.download_stream(container, path, chunk_size=CHUNK_BYTES), size=size, sha256=sha256):
        pass


async def _source_encryption(store, container, encryption):
    if store.kind == "azure_blob":
        from shared.azure_storage import read_encryption_scope_state
        state = await read_encryption_scope_state(container, shard=store._default_shard())
        if state.get("error") or (state.get("scope") and (str(state.get("state")).lower() != "enabled" or not state.get("key_uri"))):
            raise ChatCatalogueIntegrityError("source Azure encryption scope cannot be verified")
        if state.get("key_uri") and state["key_uri"] != encryption.get("key_uri"):
            raise ChatCatalogueIntegrityError("source customer key has no matching DR encryption mapping")


async def replicate_catalogue_snapshot(*, session_factory, snapshot_id, tenant_id, source_router,
                                       target, target_container, encryption, owner_id=None) -> CatalogueDRArchive:
    if target.kind != "azure_blob" or not callable(getattr(target, "verify_encryption", None)):
        raise ChatCatalogueIntegrityError("DR target has no supported encryption verification")
    snapshot_id, tenant_id = UUID(str(snapshot_id)), UUID(str(tenant_id))
    prefix = f"chat-catalogue/v1/{tenant_id}/{snapshot_id}/{uuid4()}"
    async with catalogue_read_pin(session_factory, snapshot_ids=[snapshot_id], owner_kind="dr", owner_id=owner_id) as lease:
        with TemporaryDirectory(prefix="chat-catalogue-dr-") as directory:
            manifest = Path(directory) / "manifest.jsonl"
            with manifest.open("wb") as output:
                writer = _Writer(output)
                await _source_manifest(session_factory, snapshot_id, tenant_id, writer, target,
                                       target_container, encryption, prefix)
                output.flush()
                await target.ensure_container(target_container)
                await target.verify_encryption(target_container, None, encryption)
                backends, objects = {}, 0
                with manifest.open("rb") as source_records:
                    for line in source_records:
                        record = json.loads(line)
                        data = record["data"]
                        if record["type"] == "backend":
                            backends[data["id"]] = data["kind"]
                        if record["type"] != "object":
                            continue
                        if data["tenant_id"] != str(tenant_id) or data["state"] != "LIVE":
                            raise ChatCatalogueIntegrityError("captured DR object is not LIVE or owned")
                        store = source_router.get_store_by_id(data["backend_id"])
                        expected_kind = {"azure": "azure_blob"}.get(backends.get(data["backend_id"]), backends.get(data["backend_id"]))
                        if (str(store.backend_id) != data["backend_id"] or
                            store.kind not in {"azure_blob", "seaweedfs"} or store.kind != expected_kind):
                            raise ChatCatalogueIntegrityError("captured source backend routing differs")
                        await _source_encryption(store, data["container"], encryption)
                        if await store.get_properties(data["container"], data["path"]) is None:
                            raise ChatCatalogueIntegrityError("captured source replica object is missing")
                        spool = Path(directory) / "object.bin"
                        with spool.open("wb") as blob:
                            async for chunk in _checked_stream(store.download_stream(data["container"], data["path"],
                                chunk_size=CHUNK_BYTES), size=data["size"], sha256=data["sha256"], lease=lease):
                                blob.write(chunk)
                        path = prefix + "/objects/" + data["id"]
                        await target.upload_from_file(target_container, path, str(spool), data["size"], overwrite=False)
                        lease.assert_valid()
                        await _verify_target(target, target_container, path, data["size"], data["sha256"], encryption)
                        writer.record("replica", {"object_id": data["id"], "backend_id": target.backend_id,
                            "container": target_container, "path": path, "sha256": data["sha256"],
                            "size": data["size"], "representation": "plaintext", "encryption": encryption})
                        output.flush()
                        objects += 1
                writer.seal()
            digest = hashlib.sha256()
            with manifest.open("rb") as source:
                while chunk := source.read(CHUNK_BYTES):
                    digest.update(chunk)
            sha256 = digest.hexdigest()
            path = prefix + "/manifest-" + sha256 + ".jsonl"
            # Validate source/replica sets before publishing any sealed manifest.
            with _Index(manifest, Path(directory) / "verify.sqlite", sha256, tenant_id, snapshot_id,
                        target, target_container) as indexed:
                count = indexed.count("link")
            lease.assert_valid()
            await target.upload_from_file(target_container, path, str(manifest), manifest.stat().st_size, overwrite=False)
            await _verify_target(target, target_container, path, manifest.stat().st_size, sha256, encryption)
            async with load_catalogue_dr_manifest(target=target, container=target_container, path=path,
                expected_sha256=sha256, tenant_id=tenant_id, snapshot_id=snapshot_id) as recovered:
                # The artifact is usable without SQL/cache/source objects, not merely uploaded JSON.
                async for item in recovered.iter_items():
                    if item.item_type not in _MESSAGE_TYPES and item.extra_data.get("resolved"):
                        async for _ in recovered.stream_content(item.id):
                            lease.assert_valid()
            lease.assert_valid()
            return CatalogueDRArchive(target_container, path, sha256, count, objects)


class _Index:
    """Offset-only disk index: captured payloads stay in the local manifest file."""
    def __init__(self, path, database, expected_sha256, tenant_id, snapshot_id, target, container):
        self.path, self.file = path, path.open("rb")
        self.db = sqlite3.connect(database)
        try:
            self.db.execute("CREATE TABLE records(kind text,key text,parent text,logical text,ref text,offset integer,length integer,seq integer,PRIMARY KEY(kind,key))")
            whole, content, count, sealed = hashlib.sha256(), hashlib.sha256(), 0, False
            for sequence in range(10_000_000):
                offset = self.file.tell()
                line = self.file.readline(MAX_RECORD_BYTES + 1)
                if not line:
                    break
                if sealed or not line.endswith(b"\n") or len(line) > MAX_RECORD_BYTES:
                    raise ChatCatalogueIntegrityError("DR manifest is truncated, oversized or follows its seal")
                whole.update(line)
                record = json.loads(line)
                kind = record.get("type")
                if kind == "seal":
                    if record.get("record_count") != count or record.get("sha256") != content.hexdigest():
                        raise ChatCatalogueIntegrityError("DR manifest seal checksum/count differs")
                    sealed = True
                    continue
                data = record["data"]
                if kind == "snapshot":
                    key = data["snapshot"]["id"]
                elif kind == "association":
                    key = data["chat_id"]
                elif kind == "inventory_item":
                    key = data["inventory_id"] + ":" + data["logical_key"]
                elif kind == "replica":
                    key = data["object_id"]
                elif kind in {"attempt", "backend", "inventory", "version", "link", "object"}:
                    key = data["id"]
                else:
                    raise ChatCatalogueIntegrityError("DR manifest record type is unsupported")
                parent = data.get("inventory_id") if kind == "inventory_item" else data.get("chat_id")
                if kind == "link":
                    parent = (data.get("extra_data") or {}).get("chat_id")
                logical = data.get("logical_key") if kind != "link" else data.get("external_id")
                ref = data.get("version_id") if kind == "inventory_item" else data.get("chat_version_id") if kind == "link" else data.get("object_id")
                if kind == "association":
                    ref = data["inventory_id"]
                self.db.execute("INSERT INTO records VALUES(?,?,?,?,?,?,?,?)", (kind, key, parent, logical, ref, offset, len(line), sequence))
                content.update(line)
                count += 1
            if not sealed or whole.hexdigest() != expected_sha256:
                raise ChatCatalogueIntegrityError("DR manifest checksum or terminal seal is missing")
            self.header = self.get("snapshot", str(snapshot_id))
            if (self.count("snapshot") != 1 or self.header["format"] != FORMAT or
                self.header["tenant_id"] != str(tenant_id) or self.header["snapshot"]["id"] != str(snapshot_id) or
                self.header["target"]["container"] != container or self.header["target"]["backend_id"] != target.backend_id or
                self.header["target"]["kind"] != target.kind):
                raise ChatCatalogueIntegrityError("DR manifest ownership or target routing differs")
            _policy_matches(self.header["source_policy"], self.header["encryption"])
            self._validate()
        except Exception:
            self.close()
            raise

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        self.file.close()
        self.db.close()

    def count(self, kind):
        return self.db.execute("SELECT count(*) FROM records WHERE kind=?", (kind,)).fetchone()[0]

    def get(self, kind, key):
        row = self.db.execute("SELECT offset,length FROM records WHERE kind=? AND key=?", (kind, key)).fetchone()
        if row is None:
            raise ChatCatalogueIntegrityError(f"DR manifest {kind} dependency is missing")
        self.file.seek(row[0])
        return json.loads(self.file.read(row[1]))["data"]

    def records(self, kind, *, parent=None):
        sql, params = "SELECT key FROM records WHERE kind=?", [kind]
        if parent is not None:
            sql += " AND parent=?"
            params.append(parent)
        for row in self.db.execute(sql + " ORDER BY logical,key", params):
            yield self.get(kind, row[0])

    def _validate(self):
        tenant, snapshot = self.header["tenant_id"], self.header["snapshot"]
        if not self.count("association"):
            raise ChatCatalogueIntegrityError("DR snapshot has no terminal inventory association")
        for kind, references in (("version", "'inventory_item','link'"), ("object", "'version'"), ("inventory", "'association'")):
            unused = self.db.execute(f"SELECT key FROM records WHERE kind=? EXCEPT SELECT ref FROM records WHERE kind IN ({references})", (kind,)).fetchone()
            if unused is not None:
                raise ChatCatalogueIntegrityError(f"DR manifest has an unreferenced {kind} dependency")
        unknown_inventory = self.db.execute("SELECT parent FROM records WHERE kind='inventory_item' EXCEPT SELECT key FROM records WHERE kind='inventory'").fetchone()
        if unknown_inventory is not None:
            raise ChatCatalogueIntegrityError("DR inventory membership has an unknown dependency")
        for version in self.records("version"):
            captured = _captured(version)
            if (version["tenant_id"] != tenant or version["logical_key"] != qualified_chat_item_key(
                version["chat_id"], captured.item_type, captured.graph_id, captured.parent_graph_id) or
                version["payload_sha256"] != canonical_payload_sha256(captured.payload) or
                version["version_sha256"] != canonical_version_fingerprint(tenant_id=UUID(tenant), chat_id=version["chat_id"], item=captured)):
                raise ChatCatalogueIntegrityError("DR captured version canonical integrity differs")
            content = captured.content
            if version["object_id"] != (str(content.object_id) if content and content.object_id else None):
                raise ChatCatalogueIntegrityError("DR version object identity differs")
            if version["object_id"]:
                obj = self.get("object", version["object_id"])
                if obj["tenant_id"] != tenant or obj["state"] != "LIVE" or any(
                    obj[name] != _plain(getattr(content, name)) for name in _OBJECT_FIELDS):
                    raise ChatCatalogueIntegrityError("DR captured object descriptor differs")
        for obj in self.records("object"):
            replica = self.get("replica", obj["id"])
            target = self.header["target"]
            if (replica["sha256"] != obj["sha256"] or replica["size"] != obj["size"] or
                replica["backend_id"] != target["backend_id"] or replica["container"] != target["container"] or
                replica["path"] != target["prefix"] + "/objects/" + obj["id"] or
                replica["representation"] != "plaintext" or replica["encryption"] != self.header["encryption"]):
                raise ChatCatalogueIntegrityError("DR replica routing/encryption integrity differs")
        if self.count("replica") != self.count("object"):
            raise ChatCatalogueIntegrityError("DR replica object membership differs")
        for association in self.records("association"):
            inventory = self.get("inventory", association["inventory_id"])
            attempt = self.get("attempt", association["capture_attempt_id"])
            owner = self.get("attempt", inventory["capture_attempt_id"])
            chat = association["chat_id"]
            if (association["tenant_id"] != tenant or association["snapshot_id"] != snapshot["id"] or
                inventory["tenant_id"] != tenant or inventory["chat_id"] != chat or
                inventory["state"] not in {"COMPLETE", "PARTIAL"} or association["coverage_status"] != inventory["state"]):
                raise ChatCatalogueIntegrityError("DR inventory association scope differs")
            scope = ChatCaptureScope(UUID(snapshot["id"]), UUID(snapshot["resource_id"]), UUID(tenant), chat,
                attempt["principal_id"], attempt["permission_scope_hash"], UUID(attempt["backend_id"]))
            auth = authentication_context_fingerprint(scope)
            if attempt["tenant_id"] != tenant or attempt["chat_id"] != chat or owner["tenant_id"] != tenant or owner["chat_id"] != chat or attempt["authentication_context_sha256"] != auth or owner["authentication_context_sha256"] != auth:
                raise ChatCatalogueIntegrityError("DR capture authentication scope differs")
            source = hashlib.sha256(b'["chat-source-inventory-v1",' + _json(auth) + b",[")
            representation, first, count, size = hashlib.sha256(b"["), True, 0, 0
            for entry in self.records("inventory_item", parent=inventory["id"]):
                version = self.get("version", entry["version_id"])
                item = _captured(version)
                if version["chat_id"] != chat or entry["logical_key"] != version["logical_key"]:
                    raise ChatCatalogueIntegrityError("DR inventory item scope differs")
                content = item.content
                semantic = ([content.source_drive_id, content.source_item_id, content.source_etag,
                    content.source_ctag, content.sha256, content.size] if content else None)
                separator = b"" if first else b","
                source.update(separator + _json([entry["logical_key"], _digest([_payload(item.payload), semantic])]))
                representation.update(separator + _json([entry["logical_key"], version["version_sha256"]]))
                first, count, size = False, count + 1, size + _logical_size(item)
            source.update(b"]]")
            representation.update(b"]")
            if (inventory["authentication_context_sha256"] != auth or inventory["membership_sha256"] != source.hexdigest() or
                inventory["representation_sha256"] != representation.hexdigest() or inventory["item_count"] != count or inventory["logical_bytes"] != size):
                raise ChatCatalogueIntegrityError("DR inventory source/representation fingerprint or counters differ")
            for left, right in (("inventory_item", "link"), ("link", "inventory_item")):
                parents = (inventory["id"], chat) if left == "inventory_item" else (chat, inventory["id"])
                diff = self.db.execute("SELECT logical,ref FROM records WHERE kind=? AND parent=? EXCEPT SELECT logical,ref FROM records WHERE kind=? AND parent=?", (left, parents[0], right, parents[1])).fetchone()
                if diff is not None:
                    raise ChatCatalogueIntegrityError("DR snapshot links do not exactly match inventory versions")
        for link in self.records("link"):
            version = self.get("version", link["chat_version_id"])
            item = _captured(version)
            if (link["snapshot_id"] != snapshot["id"] or link["tenant_id"] != tenant or link["is_deleted"] or
                link["external_id"] != version["logical_key"] or link["item_type"] != item.item_type or
                link["content_size"] != _logical_size(item)):
                raise ChatCatalogueIntegrityError("DR physical snapshot link differs")
            self.get("association", version["chat_id"])


class CatalogueDRRecovery:
    def __init__(self, index, target):
        self.index, self.target = index, target
        self.snapshot = index.header["snapshot"]

    async def iter_records(self, kind):
        if kind not in {"snapshot", "attempt", "backend", "inventory", "association", "inventory_item", "version", "link", "object", "replica"}:
            raise ValueError("unsupported recovery record type")
        for record in self.index.records(kind):
            yield record

    async def iter_items(self):
        for link in self.index.records("link"):
            version = self.index.get("version", link["chat_version_id"])
            item = _captured(version)
            payload = _payload(item.payload)
            payload.pop("chat_catalogue_expected_children", None)
            metadata = ({"raw": payload, "chatId": version["chat_id"]} if item.item_type in _MESSAGE_TYPES else
                        {**payload, "chat_id": version["chat_id"], "parent_item_id": item.parent_graph_id,
                         "resolved": item.content is not None})
            values = dict(link)
            for name in ("id", "snapshot_id", "tenant_id", "chat_version_id"):
                values[name] = UUID(values[name])
            for name in ("created_at", "indexed_at"):
                if values.get(name):
                    values[name] = datetime.fromisoformat(values[name])
            values.update(external_id=item.graph_id, parent_external_id=item.parent_graph_id or version["chat_id"], extra_data=metadata)
            if item.content:
                content = _plain(version["content"])
                metadata.update(content)
                key = self.index.header["encryption"]["key_uri"]
                metadata.update(source_encryption_key_id=item.content.encryption_key_id,
                    source_backup_version=item.content.backup_version, encryption_key_id=key,
                    backup_version=1, dr_storage_encryption=self.index.header["encryption"])
                values.update(backend_id=self.target.backend_id, encryption_key_id=key, backup_version=1)
                if item.content.object_id:
                    replica = self.index.get("replica", str(item.content.object_id))
                    key = replica["encryption"]["key_uri"]
                    metadata.update(backend_id=replica["backend_id"], container=replica["container"],
                        path=replica["path"], encryption_key_id=key, backup_version=1,
                        dr_storage_encryption=replica["encryption"])
                    values.update(backend_id=replica["backend_id"], blob_path=replica["path"], encryption_key_id=key, backup_version=1)
            yield SimpleNamespace(**values)

    async def stream_content(self, item_id):
        link = self.index.get("link", str(item_id))
        item = _captured(self.index.get("version", link["chat_version_id"]))
        if item.content is None:
            raise ChatCatalogueIntegrityError("recovered item has no binary content")
        if item.content.inline_b64 is not None:
            import base64
            raw = base64.b64decode(item.content.inline_b64, validate=True)
            for offset in range(0, len(raw), CHUNK_BYTES):
                yield raw[offset:offset + CHUNK_BYTES]
            return
        replica = self.index.get("replica", str(item.content.object_id))
        await self.target.verify_encryption(replica["container"], replica["path"], replica["encryption"])
        async for chunk in _checked_stream(self.target.download_stream(replica["container"], replica["path"],
            chunk_size=CHUNK_BYTES), size=item.content.size, sha256=item.content.sha256):
            yield chunk


@asynccontextmanager
async def load_catalogue_dr_manifest(*, target, container, path, expected_sha256, tenant_id, snapshot_id):
    with TemporaryDirectory(prefix="chat-catalogue-recovery-") as directory:
        manifest = Path(directory) / "manifest.jsonl"
        size = 0
        stream = target.download_stream(container, path, chunk_size=CHUNK_BYTES)
        try:
            with manifest.open("wb") as output:
                async for chunk in stream:
                    size += len(chunk)
                    if len(chunk) > CHUNK_BYTES or size > MAX_MANIFEST_BYTES:
                        raise ChatCatalogueIntegrityError("DR manifest exceeds bounded recovery size")
                    output.write(chunk)
        finally:
            close = getattr(stream, "aclose", None)
            if close:
                await close()
        with _Index(manifest, Path(directory) / "index.sqlite", expected_sha256,
                    tenant_id, snapshot_id, target, container) as index:
            await target.verify_encryption(container, path, index.header["encryption"])
            yield CatalogueDRRecovery(index, target)


from shared.storage.azure_blob import AzureBlobStore


class AzureCatalogueDRStore(AzureBlobStore):
    """Azure store with strict container/blob encryption checks for DR commits."""
    async def verify_encryption(self, container, path, expected):
        shard = self._default_shard()
        client = shard.get_async_client()
        if path is None:
            props = await client.get_container_client(container).get_container_properties()
            scope = getattr(getattr(props, "encryption_scope", None), "default_encryption_scope", None)
        else:
            props = await client.get_blob_client(container, path).get_blob_properties()
            if getattr(props, "server_encrypted", None) is not True:
                raise ChatCatalogueIntegrityError("DR replica is not verifiably encrypted at rest")
            scope = getattr(props, "encryption_scope", None)
        if scope == "$account-encryption-key":
            scope = None
        if scope != expected.get("scope"):
            raise ChatCatalogueIntegrityError("DR blob/container encryption scope differs")
        if scope:
            from shared.azure_storage import read_encryption_scope_state
            state = await read_encryption_scope_state(container, shard=shard)
            if state.get("error") or str(state.get("state")).lower() != "enabled" or state.get("key_uri") != expected.get("key_uri"):
                raise ChatCatalogueIntegrityError("DR customer key cannot be independently verified")
