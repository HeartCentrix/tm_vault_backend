"""Real catalogue transactions plus synthetic source/storage capture adapters."""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timezone
import importlib.util
import os
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")

import pytest
from sqlalchemy import func, select, text

from shared.models import ChatBlobObject, ChatCaptureAttempt, ChatItemVersion, SnapshotItem
from shared.storage.base import BlobInfo, BlobProps


# Reuse the established disposable-schema fixture rather than creating another
# production-like setup path. Its schema and all rows are synthetic per test.
_spec = importlib.util.spec_from_file_location("capture_catalogue_test_fixture", Path(__file__).with_name("test_chat_catalogue_postgres.py"))
_fixture = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_fixture)
store = _fixture.store

pytestmark = pytest.mark.integration


def message(body="original", *, attached=False, hosted=False):
    value = {"id": "message", "body": {"content": body}, "attachments": []}
    if attached:
        value["attachments"] = [{"id": "attachment", "name": "file.pdf", "contentType": "reference", "contentUrl": "https://tenant.sharepoint.com/file.pdf"}]
    if hosted:
        value["_hosted"] = [{"id": "image", "contentType": "image/png"}]
    return value


class Source:
    permission_scope_hash = "a" * 64

    def __init__(self, messages):
        self.messages = messages
        self.passes = 0
        self.downloads = 0
        self.data = b"PDF"
        self.etag, self.ctag = "etag-v1", "ctag-v1"
        self.fail_file = self.fail_page = self.cancel_page = False
        self.cancel_file = False
        self.movement = None

    async def iter_message_pages(self, chat_id):
        self.passes += 1
        values = self.movement(self.passes) if self.movement else self.messages
        for start in range(0, len(values), 50):
            yield [dict(deepcopy(m), chatId=chat_id) for m in values[start:start + 50]]
        if self.cancel_page:
            raise asyncio.CancelledError
        if self.fail_page:
            raise RuntimeError("source page failed")
        if not values:
            yield []

    async def iter_hosted_contents(self, chat_id, message_id):
        value = next((m for m in self.messages if m["id"] == message_id), {})
        for child in value.get("_hosted", []):
            yield deepcopy(child)

    async def resolve_file(self, url):
        if self.fail_file:
            raise RuntimeError("source file unavailable")
        return {"id": "file", "parentReference": {"driveId": "drive"}, "eTag": self.etag,
                "cTag": self.ctag, "size": len(self.data)}

    async def refresh_file(self, drive_id, item_id):
        return await self.resolve_file("unused")

    @asynccontextmanager
    async def open_file(self, item):
        if self.cancel_file:
            raise asyncio.CancelledError
        self.downloads += 1
        data = bytes(self.data)

        async def chunks():
            yield data[:1]
            yield data[1:]

        yield chunks(), len(data), "application/pdf"

    @asynccontextmanager
    async def open_hosted(self, chat_id, message_id, hcid):
        async def chunks():
            yield b"PNG"
        yield chunks(), 3, "image/png"


class Storage:
    kind = "azure_blob"

    def __init__(self, backend):
        self.backend_id = str(backend)
        self.objects = {}
        self.uploads = []
        self.fail_uploads = 0
        self.containers = set()

    async def ensure_container(self, container):
        self.containers.add(container)

    def get_store_by_id(self, backend):
        assert str(backend) == self.backend_id
        return self

    def shard_for(self, *args):
        pytest.fail("catalogue capture must use the stored backend's default store")

    async def upload_from_file(self, container, path, file_path, size, metadata=None, overwrite=True):
        assert Path(file_path).exists()
        self.uploads.append((container, path, dict(metadata or {})))
        if self.fail_uploads:
            self.fail_uploads -= 1
            raise RuntimeError("synthetic upload failure")
        data = Path(file_path).read_bytes()
        assert len(data) == size
        self.objects[(container, path)] = data
        return BlobInfo(self.backend_id, container, path, size, "etag", "", None, datetime.now(timezone.utc))

    async def get_properties(self, container, path):
        data = self.objects.get((container, path))
        if data is None:
            return None
        return BlobProps(len(data), "application/octet-stream", datetime.now(timezone.utc), {}, None, None, None, False)

    async def download_stream(self, container, path):
        if (container, path) in self.objects:
            yield self.objects[(container, path)]


@pytest.fixture
def adapters(monkeypatch, store):
    import shared.chat_capture as capture
    source = Source([message(attached=True, hosted=True)])
    storage = Storage(store.backend)

    @asynccontextmanager
    async def pinned(base, *, principal_id):
        assert principal_id == "principal-alpha"
        yield source

    monkeypatch.setattr(capture, "pinned_chat_source", pinned)
    return SimpleNamespace(capture=capture, source=source, storage=storage)


async def run_capture(store, adapters, **changes):
    scope = await store.new_scope()
    scope = replace(scope, **changes)
    result = await adapters.capture.capture_chat_inventory(scope=scope, graph_client=object(),
        storage=adapters.storage, session_factory=store.factory, folder_path="chats/Captured topic")
    return scope, result


async def test_initial_then_unchanged_reuses_immutable_inventory(store, adapters):
    first_scope, first = await run_capture(store, adapters)
    second_scope, second = await run_capture(store, adapters)
    assert first.outcome == second.outcome == "complete"
    assert second.reused and second.inventory_id == first.inventory_id
    assert first.item_count == second.item_count == 3
    assert first.uploaded_bytes == 6 and second.uploaded_bytes == 0
    assert len(adapters.storage.uploads) == 2
    assert adapters.source.downloads == 1
    async with store.factory() as session:
        assert await session.scalar(select(func.count()).select_from(ChatItemVersion)) == 3
        assert await session.scalar(select(func.count()).select_from(SnapshotItem)) == 6


async def test_old_body_edit_preserves_previous_snapshot_payload(store, adapters):
    old_scope, old = await run_capture(store, adapters)
    adapters.source.messages[0]["body"]["content"] = "edited old message"
    new_scope, new = await run_capture(store, adapters)
    assert new.outcome == "complete" and old.inventory_id != new.inventory_id
    from shared.chat_catalogue import materialize_chat_items
    async with store.factory() as session:
        rows = list((await session.scalars(select(SnapshotItem).where(
            SnapshotItem.snapshot_id.in_([old_scope.snapshot_id, new_scope.snapshot_id]),
            SnapshotItem.item_type == "TEAMS_CHAT_MESSAGE"))).all())
        rows = await materialize_chat_items(session, rows)
        bodies = {r.snapshot_id: r.extra_data["raw"]["body"]["content"] for r in rows}
    assert bodies[old_scope.snapshot_id] == "original"
    assert bodies[new_scope.snapshot_id] == "edited old message"


async def test_file_change_without_message_change_creates_new_version(store, adapters):
    _, first = await run_capture(store, adapters)
    adapters.source.data = b"new PDF version"
    adapters.source.etag, adapters.source.ctag = "etag-v2", "ctag-v2"
    _, changed = await run_capture(store, adapters)
    assert changed.outcome == "complete" and changed.inventory_id != first.inventory_id
    assert changed.uploaded_bytes == len(adapters.source.data)


async def test_missing_tags_force_byte_fetch_but_still_reuse_same_content(store, adapters):
    adapters.source.etag = adapters.source.ctag = None
    _, first = await run_capture(store, adapters)
    first_downloads = adapters.source.downloads
    _, repeated = await run_capture(store, adapters)
    assert first.outcome == repeated.outcome == "complete"
    assert repeated.reused and repeated.uploaded_bytes == 0
    assert first_downloads >= 2 and adapters.source.downloads >= first_downloads + 2


async def test_required_file_failure_preserves_readable_partial(store, adapters):
    adapters.source.messages = [message(attached=True)]
    adapters.source.fail_file = True
    scope, result = await run_capture(store, adapters)
    assert result.outcome == "partial" and not result.reused
    assert result.item_count == 1 and result.failure_reason
    async with store.factory() as session:
        assert await session.scalar(select(func.count()).select_from(SnapshotItem).where(SnapshotItem.snapshot_id == scope.snapshot_id)) == 1


async def test_interrupted_pagination_never_publishes_complete(store, adapters):
    adapters.source.messages = [message()]
    adapters.source.fail_page = True
    _, result = await run_capture(store, adapters)
    assert result.outcome == "partial" and not result.reused


async def test_cancelled_capture_propagates_and_cannot_publish_complete(store, adapters):
    adapters.source.messages = [message()]
    adapters.source.cancel_page = True
    with pytest.raises(asyncio.CancelledError):
        await run_capture(store, adapters)
    async with store.factory() as session:
        states = list((await session.scalars(select(ChatCaptureAttempt.state))).all())
    assert states and "PUBLISHED" not in states


async def test_movement_retries_new_generation_and_keeps_older_snapshot(store, adapters):
    adapters.source.messages = [message()]
    old_scope, old = await run_capture(store, adapters)
    adapters.source.passes = 0
    adapters.source.movement = lambda n: [message("first observation" if n == 1 else "stable edited")]
    new_scope, new = await run_capture(store, adapters)
    assert new.outcome == "complete" and adapters.source.passes == 4
    assert new.inventory_id != old.inventory_id
    async with store.factory() as session:
        states = list((await session.scalars(select(ChatCaptureAttempt.state).where(ChatCaptureAttempt.snapshot_id == new_scope.snapshot_id))).all())
        old_rows = list((await session.scalars(select(SnapshotItem).where(SnapshotItem.snapshot_id == old_scope.snapshot_id))).all())
        new_rows = list((await session.scalars(select(SnapshotItem).where(SnapshotItem.snapshot_id == new_scope.snapshot_id))).all())
    assert sorted(states) == ["FAILED", "PUBLISHED"]
    assert len(old_rows) == len(new_rows) == 1


async def test_continuing_source_movement_is_bounded_partial(store, adapters):
    adapters.source.messages = [message()]
    adapters.source.movement = lambda n: [message(str(n))]
    _, result = await run_capture(store, adapters)
    assert result.outcome == "partial" and adapters.source.passes == 6


async def test_upload_retry_uses_new_generation_and_preserves_spool(store, adapters, monkeypatch):
    import shared.chat_capture_storage as storage_module

    async def no_delay(*args):
        pass

    monkeypatch.setattr(storage_module, "_retry_pause", no_delay)
    adapters.source.messages = [message(attached=True)]
    adapters.storage.fail_uploads = 1
    _, result = await run_capture(store, adapters)
    assert result.outcome == "complete" and len(adapters.storage.uploads) == 2
    assert adapters.storage.uploads[0][1] != adapters.storage.uploads[1][1]
    async with store.factory() as session:
        objects = list((await session.scalars(select(ChatBlobObject))).all())
    assert sorted(o.state for o in objects) == ["LIVE", "STAGING"]
    assert all(o.upload_deadline is not None and o.quiescent_after is not None for o in objects)


async def test_mid_page_bad_message_preserves_already_observed_parent_and_content(store, adapters):
    adapters.source.messages = [message(attached=True), {"id": "bad-message", "attachments": {"not": "a list"}}]
    scope, result = await run_capture(store, adapters)
    assert result.outcome == "partial" and result.item_count >= 2
    async with store.factory() as session:
        assert await session.scalar(select(func.count()).select_from(SnapshotItem).where(
            SnapshotItem.snapshot_id == scope.snapshot_id, SnapshotItem.item_type == "TEAMS_CHAT_MESSAGE")) >= 1


async def test_mid_file_cancel_preserves_observed_message_before_abort(store, adapters):
    adapters.source.messages = [message(attached=True)]
    adapters.source.cancel_file = True
    with pytest.raises(asyncio.CancelledError):
        await run_capture(store, adapters)
    async with store.factory() as session:
        assert await session.scalar(select(func.count()).select_from(SnapshotItem)) == 1


async def test_empty_chat_has_complete_explicit_inventory(store, adapters):
    adapters.source.messages = []
    _, result = await run_capture(store, adapters)
    assert result.outcome == "complete" and result.item_count == 0


async def test_capture_prepares_dedicated_storage_container(store, adapters):
    adapters.source.messages = [message(attached=True)]
    _, result = await run_capture(store, adapters)
    assert result.outcome == "complete"
    assert adapters.storage.containers == {"chat-catalogue-" + str(store.tenant).replace("-", "")[:8]}


async def test_source_exclusion_applies_to_both_passes_and_scope(store, adapters):
    from shared.chat_capture_graph import effective_capture_scope_hash
    adapters.source.messages = [message(), {**message("exclude"), "id": "excluded"}]
    selected = await store.new_scope()
    selected = replace(selected, permission_scope_hash=effective_capture_scope_hash("a" * 64, "policy-hash"))
    calls = []

    def excluded(raw):
        calls.append((raw["id"], raw["_chat_folder_path"]))
        return raw["id"] == "excluded"

    result = await adapters.capture.capture_chat_inventory(scope=selected, graph_client=object(),
        storage=adapters.storage, session_factory=store.factory, folder_path="chats/Filtered",
        exclude_message=excluded, scope_filter_hash="policy-hash")
    assert result.outcome == "complete" and result.item_count == 1
    assert calls == [("message", "chats/Filtered"), ("excluded", "chats/Filtered")] * 2


async def test_publication_deadlock_retries_fresh_transaction_without_new_upload(store, adapters, monkeypatch):
    adapters.source.messages = [message(attached=True)]
    original = adapters.capture.catalogue.publish_inventory
    injected = 0

    async def deadlock_once(session, *, lease):
        nonlocal injected
        result = await original(session, lease=lease)
        if not injected:
            injected += 1
            await session.execute(text("DO $$ BEGIN RAISE EXCEPTION 'synthetic catalogue deadlock' USING ERRCODE='40P01'; END $$"))
        return result

    monkeypatch.setattr(adapters.capture.catalogue, "publish_inventory", deadlock_once)
    scope, result = await run_capture(store, adapters)
    assert injected == 1 and result.outcome == "complete"
    assert result.item_count == 2 and result.uploaded_bytes == 3
    assert len(adapters.storage.uploads) == 1 and adapters.source.passes == 2
    async with store.factory() as session:
        assert await session.scalar(select(func.count()).select_from(SnapshotItem).where(SnapshotItem.snapshot_id == scope.snapshot_id)) == 2


class EncryptedSeaweedStorage(Storage):
    kind = "seaweedfs"

    def __init__(self, backend):
        super().__init__(backend)
        self.key_id, self.dek = uuid4(), b"s" * 32
        self.header_bodies_closed = 0

    async def upload_from_file(self, container, path, file_path, size, metadata=None, overwrite=True, *, tenant_id=None):
        from shared.crypto import envelope
        assert tenant_id == metadata["tenant_id"]
        assert container in self.containers
        raw = Path(file_path).read_bytes()
        sealed = envelope.encrypt_blob(raw, self.dek, self.key_id.bytes)
        self.objects[(container, path)] = sealed
        self.uploads.append((container, path, metadata))
        return BlobInfo(self.backend_id, container, path, len(sealed), "etag", "", None, datetime.now(timezone.utc))

    def _bucket(self, container):
        return container

    def _key(self, container, path):
        return path

    @asynccontextmanager
    async def _client_ctx(self):
        owner = self

        async def get_object(*, Bucket, Key, Range):
            assert Range == "bytes=0-36"
            data = self.objects[(Bucket, Key)]

            class Body:
                async def read(self, length):
                    return data[:length]

                def close(self):
                    owner.header_bodies_closed += 1

            return {"Body": Body()}

        yield SimpleNamespace(get_object=get_object)

    async def download_stream(self, container, path):
        from shared.crypto import envelope
        yield envelope.decrypt_blob(self.objects[(container, path)], self.dek)


async def test_seaweed_explicit_tenant_encrypts_and_records_actual_envelope_key(store, adapters, monkeypatch):
    from shared.crypto import runtime
    monkeypatch.setattr(runtime, "is_write_enabled", lambda: True)
    adapters.source.messages = [message(attached=True)]
    adapters.storage = EncryptedSeaweedStorage(store.backend)
    scope, result = await run_capture(store, adapters)
    assert result.outcome == "complete" and result.uploaded_bytes == 3
    async with store.factory() as session:
        obj = await session.scalar(select(ChatBlobObject))
        link = await session.scalar(select(SnapshotItem).where(SnapshotItem.snapshot_id == scope.snapshot_id, SnapshotItem.item_type == "CHAT_ATTACHMENT"))
        assert obj.encryption_key_id == link.encryption_key_id == str(adapters.storage.key_id)
        assert obj.backup_version == link.backup_version == 1
    assert adapters.storage.header_bodies_closed == 1


async def test_null_graph_hosted_type_records_actual_response_type(store, adapters):
    adapters.source.messages = [message(hosted=True)]
    adapters.source.messages[0]["_hosted"][0]["contentType"] = None
    _, result = await run_capture(store, adapters)
    assert result.outcome == "complete"
    async with store.factory() as session:
        version = await session.scalar(select(ChatItemVersion).where(ChatItemVersion.item_type == "CHAT_HOSTED_CONTENT"))
    assert version.payload["contentType"] is None
    assert version.payload["captured_content_type"] == "image/png"


async def test_partial_content_failure_has_redacted_actionable_evidence(store, adapters):
    adapters.source.messages = [message(attached=True)]
    adapters.source.fail_file = True
    scope, result = await run_capture(store, adapters)
    assert result.outcome == "partial"
    async with store.factory() as session:
        attempt = await session.scalar(select(ChatCaptureAttempt).where(ChatCaptureAttempt.snapshot_id == scope.snapshot_id))
    errors = next(event["capture_errors"] for event in attempt.evidence_history if "capture_errors" in event)
    assert errors[0]["phase"] == "CHAT_ATTACHMENT" and errors[0]["graph_id"] == "attachment"
    assert errors[0]["error_type"] == "RuntimeError"
    assert "https://" not in str(errors)


async def test_metadata_only_card_is_complete_without_binary_download(store, adapters):
    adapters.source.messages = [message()]
    adapters.source.messages[0]["attachments"] = [{"id": "card", "contentType": "application/vnd.microsoft.card.adaptive", "content": '{"type":"AdaptiveCard"}'}]
    _, result = await run_capture(store, adapters)
    assert result.outcome == "complete" and result.item_count == 2
    assert result.uploaded_bytes == 0 and adapters.source.downloads == 0
    assert not adapters.storage.uploads


async def test_shared_file_reuses_object_after_each_principals_own_byte_verification(store, adapters, monkeypatch):
    from shared.chat_catalogue_identity import ChatCaptureScope
    from shared.models import (ChatInventory, Job, JobStatus, JobType, Resource,
                               ResourceType, Snapshot)

    adapters.source.messages = [message(attached=True)]
    alice_scope, alice = await run_capture(store, adapters)
    bob_resource, bob_job, bob_snapshot = uuid4(), uuid4(), uuid4()
    async with store.factory.begin() as session:
        session.add(Resource(id=bob_resource, tenant_id=store.tenant, type=ResourceType.USER_CHATS,
            external_id="principal-beta", display_name="Other synthetic principal",
            extra_data={"user_id": "principal-beta", "chat_inventory_format": 1}))
        await session.flush()
        session.add(Job(id=bob_job, type=JobType.BACKUP, tenant_id=store.tenant,
            resource_id=bob_resource, status=JobStatus.RUNNING))
        await session.flush()
        session.add(Snapshot(id=bob_snapshot, resource_id=bob_resource, job_id=bob_job,
            backend_id=store.backend, extra_data={"chat_inventory_format": 1}))
    bob_scope = ChatCaptureScope(bob_snapshot, bob_resource, store.tenant, alice_scope.chat_id,
        "principal-beta", "b" * 64, store.backend)
    adapters.source.permission_scope_hash = "b" * 64

    @asynccontextmanager
    async def pinned(base, *, principal_id):
        assert principal_id == "principal-beta"
        yield adapters.source

    monkeypatch.setattr(adapters.capture, "pinned_chat_source", pinned)
    bob = await adapters.capture.capture_chat_inventory(scope=bob_scope, graph_client=object(),
        storage=adapters.storage, session_factory=store.factory)
    assert alice.outcome == bob.outcome == "complete"
    assert alice.inventory_id != bob.inventory_id
    # Each principal actually downloads its accessible source bytes before
    # cross-context storage reuse; tags alone cannot authorize that reuse.
    assert adapters.source.downloads == 2
    assert len(adapters.storage.uploads) == 1
    assert alice.uploaded_bytes == 3 and bob.uploaded_bytes == 0
    async with store.factory() as session:
        assert await session.scalar(select(func.count()).select_from(ChatBlobObject)) == 1
        assert await session.scalar(select(func.count()).select_from(ChatItemVersion).where(
            ChatItemVersion.item_type == "CHAT_ATTACHMENT")) == 1
        inventories = list((await session.scalars(select(ChatInventory).where(ChatInventory.state == "COMPLETE"))).all())
        assert len({inventory.authentication_context_sha256 for inventory in inventories}) == 2
        links = list((await session.scalars(select(SnapshotItem).where(SnapshotItem.item_type == "CHAT_ATTACHMENT"))).all())
        assert len(links) == 2 and links[0].chat_version_id == links[1].chat_version_id
