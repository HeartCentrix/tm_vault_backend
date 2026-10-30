"""Durable catalogue DR archives and recovery independent of the primary cache."""
from __future__ import annotations

import base64
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime, timezone
import hashlib
import importlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import sys
from types import SimpleNamespace
from uuid import uuid4

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT / "tests/integration"))
from test_chat_catalogue_postgres import store, message, parent_for

import pytest
from sqlalchemy import select, text, update

from shared.chat_catalogue_identity import CapturedChatItem, ChatContentDescriptor
from shared.models import ChatBlobObject, ChatCataloguePin, Resource, SlaPolicy, Snapshot, SnapshotChatInventory, SnapshotItem, StorageBackend, Tenant


def api():
    return importlib.import_module("shared.chat_catalogue_dr")


class DiskTarget:
    kind = "azure_blob"
    backend_id = "dr-azure:synthetic"

    def __init__(self, directory):
        self.directory = directory
        self.written = []
        self.fail_write = False
        self.delay_reads = False
        self.on_upload = None
        self.max_chunk = 0
        self.encryption = {"mode": "azure_server_side", "scope": None, "key_uri": None}

    def local(self, container, path):
        return self.directory / container / path

    async def ensure_container(self, container):
        (self.directory / container).mkdir(parents=True, exist_ok=True)

    async def upload_from_file(self, container, path, file_path, size, metadata=None, overwrite=True):
        if self.on_upload:
            await self.on_upload()
        if self.fail_write:
            raise RuntimeError("synthetic interrupted replica upload")
        target = self.local(container, path)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(file_path, target)
        self.written.append(path)
        return SimpleNamespace(size=size)

    async def download_stream(self, container, path, chunk_size=1024 * 1024):
        if self.delay_reads:
            return
        with self.local(container, path).open("rb") as source:
            while chunk := source.read(chunk_size):
                self.max_chunk = max(self.max_chunk, len(chunk))
                yield chunk

    async def verify_encryption(self, container, path, expected):
        if self.encryption != expected:
            raise RuntimeError("DR encryption mapping does not match required policy")

    async def close(self):
        pass


def seaweed_source(data, monkeypatch):
    """Exercise the real Seaweed streaming decrypt seam over synthetic S3 bytes."""
    from shared.crypto import blob_cipher, runtime
    from shared.storage.seaweedfs import SeaweedStore
    key, key_id = b"k" * 32, b"i" * 16
    encrypted = blob_cipher.seal(data, key, key_id)
    class Keys:
        async def dek_for_key_id(self, observed):
            assert observed == key_id
            return key
    monkeypatch.setattr(runtime, "_dek_manager", Keys())
    source = object.__new__(SeaweedStore)
    source.backend_id = "synthetic-source"
    source._bucket = lambda container: container
    source._key = lambda container, path: path
    class Body:
        async def iter_chunks(self, chunk_size):
            for offset in range(0, len(encrypted), min(chunk_size, 19)):
                yield encrypted[offset:offset + min(chunk_size, 19)]
    class Client:
        async def head_object(self, **kwargs):
            return {"ContentLength": len(encrypted), "LastModified": datetime.now(timezone.utc)}
        async def get_object(self, **kwargs):
            return {"Body": Body()}
    @asynccontextmanager
    async def client():
        yield Client()
    source._client_ctx = client
    return source


async def capture_binary(store, monkeypatch, *, body="Captured old body", **evidence_changes):
    scope = await store.new_scope()
    lease = await store.begin(scope)
    binary = await store.binary(lease)
    inline_data = b"inline captured bytes"
    inline = CapturedChatItem("CHAT_HOSTED_CONTENT", "inline-alpha", "message-alpha", {},
        ChatContentDescriptor(None, None, None, None, hashlib.sha256(inline_data).hexdigest(), len(inline_data),
            None, 1, base64.b64encode(inline_data).decode(), None, None, None, None))
    items = [parent_for(binary, inline, parent=message(body)), binary, inline]
    await store.stage(lease, items)
    await store.observe(scope, lease, items, **evidence_changes)
    result = await store.publish(lease)
    async with store.factory.begin() as session:
        await session.execute(update(Snapshot).where(Snapshot.id == scope.snapshot_id).values(status="PARTIAL" if result.outcome == "partial" else "COMPLETED"))
        await session.execute(update(StorageBackend).where(StorageBackend.id == scope.backend_id).values(kind="seaweedfs"))
        # These mutable caches must not be a DR dependency.
        await session.execute(text("DROP TABLE chat_threads CASCADE"))
        await session.execute(text("DROP TABLE chat_url_cache CASCADE"))
    source = seaweed_source(b"abc", monkeypatch)
    source.backend_id = str(scope.backend_id)
    calls = []
    class Router:
        def get_store_by_id(self, backend):
            calls.append(str(backend))
            assert str(backend) == str(scope.backend_id)
            return source
    return scope, binary, inline_data, Router(), calls


async def archive(store, scope, router, target):
    return await api().replicate_catalogue_snapshot(session_factory=store.factory,
        snapshot_id=scope.snapshot_id, tenant_id=scope.tenant_id, source_router=router,
        target=target, target_container="synthetic-dr", encryption=target.encryption)


@pytest.mark.asyncio
async def test_dr_roundtrip_preserves_exact_metadata_inline_and_real_decrypted_bytes_without_primary_cache(store, monkeypatch, tmp_path):
    scope, binary, inline_data, router, calls = await capture_binary(store, monkeypatch)
    target = DiskTarget(tmp_path)
    async def pins_are_live():
        async with store.factory() as session:
            pins = list((await session.scalars(select(ChatCataloguePin))).all())
            assert pins and all(pin.owner_kind == "dr" for pin in pins)
    target.on_upload = pins_are_live
    result = await archive(store, scope, router, target)
    assert result.automatic_restore_supported is False
    assert result.object_count == 1 and result.item_count == 3
    assert target.written[-1] == result.path
    assert calls == [str(scope.backend_id)]
    from shared.crypto import runtime
    monkeypatch.setattr(runtime, "_dek_manager", None)
    router.get_store_by_id = lambda *_: pytest.fail("recovery touched primary storage")
    async with api().load_catalogue_dr_manifest(target=target, container=result.container,
        path=result.path, expected_sha256=result.sha256, tenant_id=scope.tenant_id,
        snapshot_id=scope.snapshot_id) as recovered:
        items = [item async for item in recovered.iter_items()]
        parent = next(item for item in items if item.item_type == "TEAMS_CHAT_MESSAGE")
        assert parent.extra_data["raw"]["body"]["content"] == "Captured old body"
        assert "chat_catalogue_expected_children" not in parent.extra_data["raw"]
        assert parent.folder_path == "chats/chat-alpha"
        for item in items:
            if item.item_type == "TEAMS_CHAT_MESSAGE":
                continue
            actual = b"".join([piece async for piece in recovered.stream_content(item.id)])
            assert actual == (inline_data if item.item_type == "CHAT_HOSTED_CONTENT" else b"abc")
            if item.item_type == "CHAT_ATTACHMENT":
                assert item.backend_id == target.backend_id
                assert item.encryption_key_id is None  # Azure SSE mapping, not the old Seaweed DEK.
                assert item.extra_data["source_encryption_key_id"] == "key-version-1"
    async with store.factory() as session:
        assert list((await session.scalars(select(ChatCataloguePin))).all()) == []


@pytest.mark.asyncio
async def test_dr_recovery_preserves_multiple_backend_routes_and_same_raw_ids_across_chats(store, monkeypatch, tmp_path):
    first, binary, inline_data, first_router, _ = await capture_binary(store, monkeypatch)
    backend = uuid4()
    async with store.factory.begin() as session:
        session.add(StorageBackend(id=backend, kind="seaweedfs", name=uuid4().hex,
            endpoint="https://storage.invalid", config={}, secret_ref="synthetic-only"))
    second = replace(await store.new_scope(), backend_id=backend, chat_id="chat-beta")
    async with store.factory.begin() as session:
        await session.execute(update(Snapshot).where(Snapshot.id == second.snapshot_id).values(backend_id=backend))
    lease = await store.begin(second)
    data = b"different captured backend bytes"
    descriptor = replace(binary.content, object_id=uuid4(), backend_id=backend, path="chat-catalogue/v1/" + uuid4().hex,
                         sha256=hashlib.sha256(data).hexdigest(), size=len(data))
    child = replace(binary, content=descriptor)
    async with store.factory.begin() as session:
        session.add(ChatBlobObject(id=descriptor.object_id, tenant_id=second.tenant_id, backend_id=backend,
            container=descriptor.container, path=descriptor.path, sha256=descriptor.sha256, size=len(data),
            encryption_key_id=descriptor.encryption_key_id, backup_version=descriptor.backup_version,
            state="LIVE", capture_attempt_id=lease.attempt_id, uploaded_bytes=len(data)))
    items = [parent_for(child, parent=message("Different captured chat")), child]
    await store.stage(lease, items)
    await store.observe(second, lease, items)
    await store.publish(lease)
    # A synthetic retained restore point owns these exact inventory associations;
    # earlier snapshots/versions are untouched, as in retention heir copying.
    retained = await store.new_scope()
    async with store.factory.begin() as session:
        for model in (SnapshotItem, SnapshotChatInventory):
            source_rows = list((await session.scalars(select(model).where(model.snapshot_id.in_([first.snapshot_id, second.snapshot_id])))).all())
            for row in source_rows:
                values = {prop.key: getattr(row, prop.key) for prop in model.__mapper__.column_attrs}
                values["snapshot_id"] = retained.snapshot_id
                if model is SnapshotItem:
                    values["id"] = uuid4()
                session.add(model(**values))
        await session.execute(update(Snapshot).where(Snapshot.id == retained.snapshot_id).values(status="COMPLETED"))
    stores = {str(first.backend_id): first_router.get_store_by_id(first.backend_id), str(backend): seaweed_source(data, monkeypatch)}
    stores[str(backend)].backend_id = str(backend)
    calls = []
    class Router:
        def get_store_by_id(self, identifier):
            calls.append(identifier)
            return stores[identifier]
    target = DiskTarget(tmp_path)
    result = await archive(store, retained, Router(), target)
    assert result.item_count == 5 and result.object_count == 2
    assert set(calls) == set(stores) and len(calls) == 2
    async with api().load_catalogue_dr_manifest(target=target, container=result.container,
        path=result.path, expected_sha256=result.sha256, tenant_id=retained.tenant_id,
        snapshot_id=retained.snapshot_id) as recovered:
        items = [item async for item in recovered.iter_items()]
        for item in items:
            if item.item_type == "CHAT_ATTACHMENT":
                content = b"".join([chunk async for chunk in recovered.stream_content(item.id)])
                assert content == (data if item.extra_data["chat_id"] == "chat-beta" else b"abc")
        assert len([item for item in items if item.external_id == "message-alpha"]) == 2


@pytest.mark.asyncio
async def test_dr_partial_inventory_keeps_coverage_gaps_during_cache_independent_recovery(store, monkeypatch, tmp_path):
    scope, _, _, router, _ = await capture_binary(store, monkeypatch, enumeration_complete=False)
    target = DiskTarget(tmp_path)
    result = await archive(store, scope, router, target)
    async with api().load_catalogue_dr_manifest(target=target, container=result.container,
        path=result.path, expected_sha256=result.sha256, tenant_id=scope.tenant_id,
        snapshot_id=scope.snapshot_id) as recovered:
        assert recovered.snapshot["status"] == "PARTIAL"
        inventory, = [row async for row in recovered.iter_records("inventory")]
        assert inventory["state"] == "PARTIAL" and inventory["failure_reason"]
        assert inventory["evidence"]["enumeration_complete"] is False
        assert len([item async for item in recovered.iter_items()]) == 3


@pytest.mark.asyncio
async def test_genuinely_empty_complete_inventory_has_a_recoverable_manifest(store, tmp_path):
    scope, _, _ = await store.capture([])
    async with store.factory.begin() as session:
        await session.execute(update(Snapshot).where(Snapshot.id == scope.snapshot_id).values(status="COMPLETED"))
    router = SimpleNamespace(get_store_by_id=lambda *_: pytest.fail("empty DR touched binary storage"))
    target = DiskTarget(tmp_path)
    result = await archive(store, scope, router, target)
    assert result.item_count == result.object_count == 0
    async with api().load_catalogue_dr_manifest(target=target, container=result.container,
        path=result.path, expected_sha256=result.sha256, tenant_id=scope.tenant_id,
        snapshot_id=scope.snapshot_id) as recovered:
        inventory, = [row async for row in recovered.iter_records("inventory")]
        assert inventory["state"] == "COMPLETE" and inventory["item_count"] == 0
        assert [item async for item in recovered.iter_items()] == []


@pytest.mark.asyncio
async def test_source_backend_object_identity_must_match_descriptor_registry_key(store, monkeypatch, tmp_path):
    scope, _, _, router, _ = await capture_binary(store, monkeypatch)
    source = router.get_store_by_id(scope.backend_id)
    source.backend_id = str(uuid4())
    with pytest.raises(Exception, match="backend|routing"):
        await archive(store, scope, router, DiskTarget(tmp_path))


@pytest.mark.asyncio
async def test_large_metadata_archive_and_recovery_do_not_buffer_the_whole_chat(store, tmp_path):
    import tracemalloc
    scope = await store.new_scope()
    lease = await store.begin(scope)
    items = [message("x" * 65536, graph_id=f"message-{n}") for n in range(300)]
    for offset in range(0, len(items), 150):
        await store.stage(lease, items[offset:offset + 150])
    await store.observe(scope, lease, items)
    await store.publish(lease)
    async with store.factory.begin() as session:
        await session.execute(update(Snapshot).where(Snapshot.id == scope.snapshot_id).values(status="COMPLETED"))
    router = SimpleNamespace(get_store_by_id=lambda *_: pytest.fail("text-only DR touched binary storage"))
    target = DiskTarget(tmp_path)
    tracemalloc.start()
    try:
        result = await archive(store, scope, router, target)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert result.item_count == 300
    assert target.local(result.container, result.path).stat().st_size > 18 * 1024 * 1024
    assert peak < 16 * 1024 * 1024, f"DR metadata peak was {peak} bytes"
    assert target.max_chunk <= api().CHUNK_BYTES


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["upload", "replica_delay", "source_checksum"])
async def test_dr_failures_leave_primary_ownership_and_never_publish_a_recovery_manifest(store, monkeypatch, tmp_path, fault):
    scope, binary, _, router, _ = await capture_binary(store, monkeypatch)
    target = DiskTarget(tmp_path)
    target.fail_write = fault == "upload"
    target.delay_reads = fault == "replica_delay"
    if fault == "source_checksum":
        changed = seaweed_source(b"changed bytes", monkeypatch)
        changed.backend_id = str(scope.backend_id)
        router.get_store_by_id = lambda *_: changed
    with pytest.raises(Exception, match="upload|checksum|size|durable|replica|captured"):
        await archive(store, scope, router, target)
    assert not any("manifest-" in path for path in target.written)
    async with store.factory() as session:
        assert (await session.get(ChatBlobObject, binary.content.object_id)).state == "LIVE"
        assert len(list((await session.scalars(select(SnapshotItem))).all())) == 3
        assert list((await session.scalars(select(ChatCataloguePin))).all()) == []


@pytest.mark.asyncio
async def test_dr_loader_rejects_truncated_or_semantically_tampered_manifest(store, monkeypatch, tmp_path):
    scope, _, _, router, _ = await capture_binary(store, monkeypatch)
    target = DiskTarget(tmp_path)
    result = await archive(store, scope, router, target)
    path = target.local(result.container, result.path)
    original = path.read_bytes()
    path.write_bytes(original[:-10])
    with pytest.raises(Exception, match="manifest|checksum|truncat|seal"):
        async with api().load_catalogue_dr_manifest(target=target, container=result.container,
            path=result.path, expected_sha256=result.sha256, tenant_id=scope.tenant_id,
            snapshot_id=scope.snapshot_id):
            pytest.fail("truncated manifest accepted")
    records = [json.loads(line) for line in original.splitlines()]
    record = next(row for row in records if row["type"] == "version" and row["data"]["item_type"] == "TEAMS_CHAT_MESSAGE")
    record["data"]["payload"]["body"]["content"] = "forged body"
    encoded = b"".join(json.dumps(row, sort_keys=True, separators=(",", ":")).encode() + b"\n" for row in records[:-1])
    records[-1]["sha256"] = hashlib.sha256(encoded).hexdigest()
    forged = encoded + json.dumps(records[-1], sort_keys=True, separators=(",", ":")).encode() + b"\n"
    path.write_bytes(forged)
    with pytest.raises(Exception, match="version|canonical|captured|integrity"):
        async with api().load_catalogue_dr_manifest(target=target, container=result.container,
            path=result.path, expected_sha256=hashlib.sha256(forged).hexdigest(), tenant_id=scope.tenant_id,
            snapshot_id=scope.snapshot_id):
            pytest.fail("forged captured version accepted")


@pytest.mark.asyncio
async def test_dr_loader_rejects_unreferenced_future_versions_even_with_valid_transport_seal(store, monkeypatch, tmp_path):
    from copy import deepcopy
    scope, _, _, router, _ = await capture_binary(store, monkeypatch)
    target = DiskTarget(tmp_path)
    result = await archive(store, scope, router, target)
    path = target.local(result.container, result.path)
    records = [json.loads(line) for line in path.read_bytes().splitlines()]
    extra = deepcopy(next(record for record in records if record["type"] == "version"))
    extra["data"]["id"] = str(uuid4())
    records.insert(-1, extra)
    encoded = b"".join(json.dumps(record, sort_keys=True, separators=(",", ":")).encode() + b"\n" for record in records[:-1])
    records[-1].update(record_count=len(records) - 1, sha256=hashlib.sha256(encoded).hexdigest())
    forged = encoded + json.dumps(records[-1], sort_keys=True, separators=(",", ":")).encode() + b"\n"
    path.write_bytes(forged)
    with pytest.raises(Exception, match="version|membership|unreferenced|dependency"):
        async with api().load_catalogue_dr_manifest(target=target, container=result.container,
            path=result.path, expected_sha256=hashlib.sha256(forged).hexdigest(), tenant_id=scope.tenant_id,
            snapshot_id=scope.snapshot_id):
            pytest.fail("unreferenced captured version accepted")


@pytest.mark.asyncio
async def test_customer_key_mapping_must_match_verified_dr_scope(store, monkeypatch, tmp_path):
    scope, _, _, router, _ = await capture_binary(store, monkeypatch)
    target = DiskTarget(tmp_path)
    required = {"mode": "azure_server_side", "scope": "customer-scope", "key_uri": "https://vault.invalid/keys/customer/version"}
    with pytest.raises(Exception, match="encryption|key|scope"):
        await api().replicate_catalogue_snapshot(session_factory=store.factory,
            snapshot_id=scope.snapshot_id, tenant_id=scope.tenant_id, source_router=router,
            target=target, target_container="synthetic-dr", encryption=required)


@pytest.mark.asyncio
async def test_customer_key_recovery_maps_both_inline_and_object_representations_to_dr_key(store, monkeypatch, tmp_path):
    scope, _, _, router, _ = await capture_binary(store, monkeypatch)
    async with store.factory.begin() as session:
        policy = SlaPolicy(tenant_id=scope.tenant_id, name="Synthetic CMK", encryption_mode="CUSTOMER_KEY",
            key_vault_uri="https://vault.invalid", key_name="customer", key_version="version-one")
        session.add(policy)
        await session.flush()
        await session.execute(update(Resource).where(Resource.id == scope.resource_id).values(sla_policy_id=policy.id))
    target = DiskTarget(tmp_path)
    target.encryption = {"mode": "azure_server_side", "scope": "customer-scope", "key_uri": "https://vault.invalid/keys/customer/version-one"}
    result = await archive(store, scope, router, target)
    async with api().load_catalogue_dr_manifest(target=target, container=result.container,
        path=result.path, expected_sha256=result.sha256, tenant_id=scope.tenant_id,
        snapshot_id=scope.snapshot_id) as recovered:
        for item in [item async for item in recovered.iter_items()]:
            if item.item_type != "TEAMS_CHAT_MESSAGE":
                assert item.encryption_key_id == target.encryption["key_uri"]
                assert item.extra_data["dr_storage_encryption"] == target.encryption


@pytest.mark.asyncio
async def test_source_customer_key_cannot_be_silently_replaced_by_a_different_dr_key(store, monkeypatch, tmp_path):
    from shared import azure_storage
    scope, _, _, router, _ = await capture_binary(store, monkeypatch)
    source = router.get_store_by_id(scope.backend_id)
    source.kind = "azure_blob"
    source._default_shard = lambda: SimpleNamespace()
    async with store.factory.begin() as session:
        await session.execute(update(StorageBackend).where(StorageBackend.id == scope.backend_id).values(kind="azure_blob"))
    async def source_policy(*args, **kwargs):
        return {"scope": "source-customer", "key_uri": "https://vault.invalid/keys/source/version", "state": "Enabled"}
    monkeypatch.setattr(azure_storage, "read_encryption_scope_state", source_policy)
    target = DiskTarget(tmp_path)
    target.encryption = {"mode": "azure_server_side", "scope": "other-scope", "key_uri": "https://vault.invalid/keys/other/version"}
    with pytest.raises(Exception, match="customer key|encryption mapping"):
        await archive(store, scope, router, target)
    assert target.written == []


@pytest.mark.asyncio
async def test_azure_target_verifies_real_sdk_container_scope_shape_and_blob_encryption(monkeypatch):
    from azure.storage.blob import ContainerEncryptionScope, ContainerProperties
    from shared import azure_storage
    props = ContainerProperties()
    props.encryption_scope = ContainerEncryptionScope(default_encryption_scope="customer-scope", prevent_encryption_scope_override=True)
    blob = SimpleNamespace(server_encrypted=True, encryption_scope="customer-scope")
    class Client:
        def get_container_client(self, container):
            async def get():
                return props
            return SimpleNamespace(get_container_properties=get)
        def get_blob_client(self, container, path):
            async def get():
                return blob
            return SimpleNamespace(get_blob_properties=get)
    shard = SimpleNamespace(get_async_client=lambda: Client())
    target = object.__new__(api().AzureCatalogueDRStore)
    target._default_shard = lambda: shard
    expected = {"mode": "azure_server_side", "scope": "customer-scope", "key_uri": "https://vault.invalid/keys/key/version"}
    async def encryption_state(*args, **kwargs):
        return {"scope": expected["scope"], "key_uri": expected["key_uri"], "state": "Enabled"}
    monkeypatch.setattr(azure_storage, "read_encryption_scope_state", encryption_state)
    await target.verify_encryption("container", None, expected)
    await target.verify_encryption("container", "object", expected)
    blob.server_encrypted = False
    with pytest.raises(Exception, match="encrypt"):
        await target.verify_encryption("container", "object", expected)


def worker():
    name = "chat_catalogue_dr_worker_test"
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(name, _ROOT / "services/dr-replication-worker/main.py")
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
    return sys.modules[name]


@pytest.mark.asyncio
async def test_new_format_worker_never_marks_drready_without_automatic_restore_routing(store, monkeypatch, tmp_path):
    scope, _, _, router, _ = await capture_binary(store, monkeypatch)
    target = DiskTarget(tmp_path)
    module = worker()
    monkeypatch.setattr(module, "async_session_factory", store.factory)
    async def target_factory(*args, **kwargs):
        return target, target.encryption
    monkeypatch.setattr(module, "_catalogue_dr_target", target_factory, raising=False)
    monkeypatch.setattr(module.azure_storage_manager, "get_default_shard", lambda: pytest.fail("catalogue DR used legacy default shard"))
    from shared.storage.router import router as global_router
    monkeypatch.setattr(global_router, "get_store_by_id", router.get_store_by_id)
    async with store.factory() as session:
        snapshot = await session.get(Snapshot, scope.snapshot_id)
        tenant = await session.get(Tenant, scope.tenant_id)
        tenant.dr_region_enabled = True
        tenant.dr_storage_account_name = "syntheticdr"
        tenant.dr_storage_account_key_encrypted = b"synthetic-only"
        await session.commit()
        await module.replicate_snapshot(snapshot, tenant, session)
    async with store.factory() as session:
        snapshot = await session.get(Snapshot, scope.snapshot_id)
        assert snapshot.dr_replication_status == "failed"
        assert snapshot.dr_blob_path is None and snapshot.dr_replicated_at is None
        assert "automatic" in snapshot.dr_error.lower() and "unsupported" in snapshot.dr_error.lower()
        archived = snapshot.extra_data["chat_catalogue_dr_archive"]
        assert archived["verified"] and archived["sha256"]
        assert snapshot.extra_data["chat_catalogue_dr_nonretryable"] is True
        assert snapshot.dr_replication_attempts == 1
    writes = list(target.written)
    await module.scan_and_replicate()
    async with store.factory() as session:
        snapshot = await session.get(Snapshot, scope.snapshot_id)
        assert snapshot.dr_replication_attempts == 1
        assert target.written == writes


@pytest.mark.asyncio
async def test_worker_cancellation_releases_dr_pins_and_keeps_primary_retryable(store, monkeypatch, tmp_path):
    import asyncio
    scope, binary, _, router, _ = await capture_binary(store, monkeypatch)
    target, entered = DiskTarget(tmp_path), asyncio.Event()
    async def pause():
        entered.set()
        await asyncio.Future()
    target.on_upload = pause
    module = worker()
    monkeypatch.setattr(module, "async_session_factory", store.factory)
    async def target_factory(*args, **kwargs):
        return target, target.encryption
    monkeypatch.setattr(module, "_catalogue_dr_target", target_factory, raising=False)
    from shared.storage.router import router as global_router
    monkeypatch.setattr(global_router, "get_store_by_id", router.get_store_by_id)
    async with store.factory() as session:
        snapshot = await session.get(Snapshot, scope.snapshot_id)
        tenant = await session.get(Tenant, scope.tenant_id)
        tenant.dr_region_enabled = True
        tenant.dr_storage_account_name = "syntheticdr"
        tenant.dr_storage_account_key_encrypted = b"synthetic-only"
        await session.commit()
        task = asyncio.create_task(module.replicate_snapshot(snapshot, tenant, session))
        await asyncio.wait_for(entered.wait(), 3)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    async with store.factory() as session:
        snapshot = await session.get(Snapshot, scope.snapshot_id)
        assert snapshot.dr_replication_status == "failed"
        assert "interrupted" in snapshot.dr_error.lower()
        assert not snapshot.extra_data.get("chat_catalogue_dr_nonretryable")
        assert snapshot.dr_blob_path is None and snapshot.dr_replicated_at is None
        assert (await session.get(ChatBlobObject, binary.content.object_id)).state == "LIVE"
        assert list((await session.scalars(select(ChatCataloguePin))).all()) == []


async def configure_dr_worker(store, scope, router, target, monkeypatch):
    module = worker()
    monkeypatch.setattr(module, "async_session_factory", store.factory)
    async def target_factory(*args, **kwargs):
        return target, target.encryption
    monkeypatch.setattr(module, "_catalogue_dr_target", target_factory, raising=False)
    from shared.storage.router import router as global_router
    monkeypatch.setattr(global_router, "get_store_by_id", router.get_store_by_id)
    async with store.factory.begin() as session:
        tenant = await session.get(Tenant, scope.tenant_id)
        tenant.dr_region_enabled = True
        tenant.dr_storage_account_name = "syntheticdr"
        tenant.dr_storage_account_key_encrypted = b"synthetic-only"
    return module


@pytest.mark.asyncio
@pytest.mark.parametrize("attempts", [1, 10])
async def test_scan_recovers_a_crashed_unpinned_catalogue_attempt_without_unbounded_retries(store, monkeypatch, tmp_path, attempts):
    from datetime import timedelta
    scope, _, _, router, _ = await capture_binary(store, monkeypatch)
    target = DiskTarget(tmp_path)
    module = await configure_dr_worker(store, scope, router, target, monkeypatch)
    async with store.factory.begin() as session:
        snapshot = await session.get(Snapshot, scope.snapshot_id)
        snapshot.dr_replication_status = "in_progress"
        snapshot.dr_replication_attempts = attempts
        snapshot.extra_data = {**snapshot.extra_data,
            "chat_catalogue_dr_attempt_id": str(uuid4()),
            "chat_catalogue_dr_started_at": (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()}
    await module.scan_and_replicate()
    async with store.factory() as session:
        snapshot = await session.get(Snapshot, scope.snapshot_id)
        assert snapshot.dr_replication_status == "failed"
        if attempts == 1:
            assert snapshot.dr_replication_attempts == 2
            assert snapshot.extra_data["chat_catalogue_dr_archive"]["verified"]
            assert target.written
        else:
            assert snapshot.dr_replication_attempts == 10
            assert "attempt" in snapshot.dr_error.lower()
            assert target.written == []


@pytest.mark.asyncio
async def test_scan_does_not_start_a_second_copy_while_original_dr_owner_has_live_pins(store, monkeypatch, tmp_path):
    from datetime import timedelta
    from shared.chat_catalogue_gc import catalogue_read_pin
    scope, _, _, router, _ = await capture_binary(store, monkeypatch)
    target = DiskTarget(tmp_path)
    module = await configure_dr_worker(store, scope, router, target, monkeypatch)
    owner = uuid4()
    async with store.factory.begin() as session:
        snapshot = await session.get(Snapshot, scope.snapshot_id)
        snapshot.dr_replication_status = "in_progress"
        snapshot.dr_replication_attempts = 1
        snapshot.extra_data = {**snapshot.extra_data, "chat_catalogue_dr_attempt_id": str(owner),
            "chat_catalogue_dr_started_at": (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()}
    async with catalogue_read_pin(store.factory, snapshot_ids=[scope.snapshot_id], owner_kind="dr", owner_id=owner):
        await module.scan_and_replicate()
    async with store.factory() as session:
        snapshot = await session.get(Snapshot, scope.snapshot_id)
        assert snapshot.dr_replication_attempts == 1 and snapshot.dr_replication_status == "in_progress"
        assert target.written == []


@pytest.mark.asyncio
async def test_superseded_dr_writer_cannot_attach_its_receipt_or_overwrite_new_owner_state(store, monkeypatch, tmp_path):
    scope, _, _, router, _ = await capture_binary(store, monkeypatch)
    target = DiskTarget(tmp_path)
    module = await configure_dr_worker(store, scope, router, target, monkeypatch)
    newer = str(uuid4())
    async def supersede():
        async with store.factory.begin() as session:
            row = await session.get(Snapshot, scope.snapshot_id)
            row.extra_data = {**row.extra_data, "chat_catalogue_dr_attempt_id": newer}
    target.on_upload = supersede
    async with store.factory() as session:
        await module.replicate_snapshot(await session.get(Snapshot, scope.snapshot_id),
                                        await session.get(Tenant, scope.tenant_id), session)
    async with store.factory() as session:
        row = await session.get(Snapshot, scope.snapshot_id)
        assert row.extra_data["chat_catalogue_dr_attempt_id"] == newer
        assert row.dr_replication_status == "in_progress"
        assert not row.extra_data.get("chat_catalogue_dr_archive")
        assert not row.extra_data.get("chat_catalogue_dr_nonretryable")
