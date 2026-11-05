"""Real catalogue transactions plus synthetic source/storage capture adapters."""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import importlib.util
import os
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")

import pytest
from sqlalchemy import func, select, text

from shared.models import ChatBlobObject, ChatCaptureAttempt, ChatInventory, ChatItemVersion, SnapshotItem
from shared.storage.base import BlobInfo, BlobProps


# Reuse the established disposable-schema fixture rather than creating another
# production-like setup path. Its schema and all rows are synthetic per test.
_spec = importlib.util.spec_from_file_location("capture_catalogue_test_fixture", Path(__file__).with_name("test_chat_catalogue_postgres.py"))
_fixture = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_fixture)
store = _fixture.store

from test_graph_admission_redis import redis_client

pytestmark = pytest.mark.integration


def message(body="original", *, attached=False, hosted=False):
    value = {"id": "message", "body": {"content": body}, "attachments": []}
    if attached:
        value["attachments"] = [{"id": "attachment", "name": "file.pdf", "contentType": "reference", "contentUrl": "https://tenant.sharepoint.com/file.pdf"}]
    if hosted:
        value["_hosted"] = [{"id": "image", "contentType": "image/png"}]
        value["body"] = {"contentType": "html", "content": body + '<img src="https://graph.microsoft.com/v1.0/chats/chat/messages/message/hostedContents/image/$value">'}
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
        self.hosted_lists = 0
        self.cancel_file = False
        self.movement = None
        self.fail_urls = set()
        self.hosted_downloads = 0

    latest_change = None
    change_checks = 0

    async def latest_message_change(self, chat_id):
        self.change_checks += 1
        return self.latest_change

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
        self.hosted_lists += 1
        value = next((m for m in self.messages if m["id"] == message_id), {})
        for child in value.get("_hosted", []):
            yield deepcopy(child)

    async def resolve_file(self, url):
        if self.fail_file or url in self.fail_urls:
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

    hosted_error = None

    hosted_delay, hosted_in_flight, hosted_peak = 0, 0, 0

    @asynccontextmanager
    async def open_hosted(self, chat_id, message_id, hcid):
        self.hosted_downloads += 1
        self.hosted_in_flight += 1
        self.hosted_peak = max(self.hosted_peak, self.hosted_in_flight)
        try:
            await asyncio.sleep(self.hosted_delay)
        finally:
            self.hosted_in_flight -= 1
        if self.hosted_error is not None:
            raise self.hosted_error
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


async def test_only_messages_referencing_hosted_content_are_listed(store, adapters):
    adapters.source.messages = [message(), dict(message(hosted=True), id="with-image")]
    _, result = await run_capture(store, adapters)
    assert result.outcome == "complete" and result.item_count == 3  # two messages + the referenced image
    assert adapters.source.hosted_lists == 2  # the referencing message, once per observation pass


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


async def test_home_tenant_enrichment_flip_keeps_one_version(store, adapters):
    """Graph returns the reacting/mentioned user's tenantId as the home tenant on one read and
    omits it on the next, and swaps the reaction's displayName label ("Laugh" vs "Grinning face
    with smiling eyes"). Those flips must not create a new stored message version; another
    tenant's id is real identity and is kept. Exports render reactions from reactionType."""
    adapters.source.home_tenant_id = "home-tenant"

    def with_identities(home_value, label):
        value = message()
        home = {"id": "u1"} if home_value is None else {"id": "u1", "tenantId": home_value}
        value["reactions"] = [
            {"reactionType": "laugh", "displayName": label, "user": {"user": dict(home)}},
            {"reactionType": "heart", "user": {"user": {"id": "u2", "tenantId": "other-tenant"}}},
        ]
        value["mentions"] = [{"id": 0, "mentioned": {"user": dict(home)}}]
        return value

    adapters.source.messages = [with_identities("HOME-TENANT", "Laugh")]
    _, first = await run_capture(store, adapters)
    adapters.source.messages = [with_identities(None, "Grinning face with smiling eyes")]
    _, second = await run_capture(store, adapters)

    assert first.outcome == second.outcome == "complete"
    assert second.reused and second.inventory_id == first.inventory_id
    async with store.factory() as session:
        payloads = (await session.scalars(select(ChatItemVersion.payload)
            .where(ChatItemVersion.item_type == "TEAMS_CHAT_MESSAGE"))).all()
    assert len(payloads) == 1
    reactions = payloads[0]["reactions"]
    assert [r["user"]["user"] for r in reactions] == [{"id": "u1"}, {"id": "u2", "tenantId": "other-tenant"}]
    assert [r["reactionType"] for r in reactions] == ["laugh", "heart"]
    assert all("displayName" not in r for r in reactions)
    assert payloads[0]["mentions"][0]["mentioned"]["user"] == {"id": "u1"}


async def test_unchanged_chat_matching_a_verified_inventory_needs_one_pass(store, adapters):
    """Pass 1 reproducing an inventory that two passes already verified COMPLETE is a
    consistent state; the second listing only doubled incremental Graph cost."""
    _, first = await run_capture(store, adapters)
    assert first.outcome == "complete" and adapters.source.passes == 2
    _, second = await run_capture(store, adapters)
    assert second.outcome == "complete" and second.reused and second.inventory_id == first.inventory_id
    assert adapters.source.passes == 3
    adapters.source.messages[0]["body"]["content"] = "edited"
    _, third = await run_capture(store, adapters)
    assert third.outcome == "complete" and not third.reused and adapters.source.passes == 5


async def test_single_pass_can_be_disabled(store, adapters, monkeypatch):
    monkeypatch.setenv("CHAT_SINGLE_PASS_UNCHANGED", "false")
    await run_capture(store, adapters)
    await run_capture(store, adapters)
    assert adapters.source.passes == 4


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
    assert bodies[old_scope.snapshot_id] == message(hosted=True)["body"]["content"]
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
    # Without tags the bytes are fetched and checksummed on every run; an unchanged run
    # reproducing a verified inventory does so in its single pass.
    assert first_downloads >= 2 and adapters.source.downloads >= first_downloads + 1


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
    scope, result = await run_capture(store, adapters)
    assert result.outcome == "partial" and adapters.source.passes == 6
    async with store.factory() as session:
        histories = list((await session.scalars(select(ChatCaptureAttempt.evidence_history).where(
            ChatCaptureAttempt.snapshot_id == scope.snapshot_id, ChatCaptureAttempt.state == "FAILED"))).all())
    reasons = [entry["observation_mismatch"]["movement_reason"] for history in histories for entry in history
               if "observation_mismatch" in entry]
    assert reasons == ["observation fingerprints differ"] * 2


async def test_in_pass_conflict_records_its_reason(store, adapters):
    """A ChatSourceMovement raised inside pass 1 used to leave only "only_first=N" behind,
    which reads like an empty second pass (Railway + local, 2026-11-04)."""
    adapters.source.messages = [message("one"), {**message("two"), "id": message("one")["id"]}]
    scope, result = await run_capture(store, adapters)
    assert result.outcome == "partial"
    async with store.factory() as session:
        histories = list((await session.scalars(select(ChatCaptureAttempt.evidence_history).where(
            ChatCaptureAttempt.snapshot_id == scope.snapshot_id, ChatCaptureAttempt.state == "FAILED"))).all())
    reasons = {entry["observation_mismatch"]["movement_reason"] for history in histories for entry in history
               if "observation_mismatch" in entry}
    assert reasons == {"conflicting source versions during enumeration"}


async def test_partial_capture_binaries_are_reused_by_the_next_capture(store, adapters):
    """A chat left PARTIAL by one unavailable file re-uploaded every healthy binary on the
    next capture (52 objects / 8.8 MB on a real tenant, 2026-11-04). Published PARTIAL
    inventories hold LIVE, verified objects and are valid reuse candidates."""
    broken = dict(message("broken", attached=True), id="broken")
    broken["attachments"] = [{**broken["attachments"][0], "id": "missing",
                              "contentUrl": "https://tenant.sharepoint.com/missing.pdf"}]
    adapters.source.messages = [message("ok", attached=True), broken]
    adapters.source.fail_urls = {"https://tenant.sharepoint.com/missing.pdf"}
    _, first = await run_capture(store, adapters)
    assert first.outcome == "partial" and len(adapters.storage.uploads) == 1
    _, second = await run_capture(store, adapters)
    assert second.outcome == "partial" and len(adapters.storage.uploads) == 1


async def test_reconciliation_restart_reuses_binaries_from_the_failed_attempt(store, adapters):
    """A restart after a real message change re-uploaded every unchanged binary
    (Bhavya chat: 67 objects / 14.4 MB uploaded twice, 2026-11-04)."""
    adapters.source.messages = [message(attached=True)]
    adapters.source.movement = lambda n: [message("before" if n == 1 else "after", attached=True)]
    _, result = await run_capture(store, adapters)
    assert result.outcome == "complete" and adapters.source.passes == 4
    assert len(adapters.storage.uploads) == 1


async def test_hosted_content_of_an_unchanged_message_is_not_downloaded_again(store, adapters):
    """Hosted images have no source version tag, so every observation pass and every
    incremental re-downloaded them. The parent message version marks them."""
    stamped = dict(message(hosted=True), lastModifiedDateTime="2026-11-01T10:00:00Z", etag="1780000000000")
    adapters.source.messages = [stamped]
    _, first = await run_capture(store, adapters)
    assert first.outcome == "complete" and adapters.source.hosted_downloads == 1
    _, second = await run_capture(store, adapters)
    assert second.outcome == "complete" and adapters.source.hosted_downloads == 1
    edited = dict(stamped, lastModifiedDateTime="2026-11-02T10:00:00Z", etag="1780000000999")
    adapters.source.messages = [edited]
    _, third = await run_capture(store, adapters)
    assert third.outcome == "complete" and adapters.source.hosted_downloads == 2


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


async def test_teams_tab_is_complete_without_download(store, adapters):
    """A Planner/OneNote tab pinned in a chat is a pointer with no file bytes (Railway 2026-11-04:
    "Kanban Task Board" was fetched as a file and kept five users' group chat PARTIAL)."""
    adapters.source.messages = [message()]
    adapters.source.messages[0]["attachments"] = [{"id": "tab", "contentType": "tabReference",
        "name": "Kanban Task Board", "contentUrl": None, "content": None}]
    _, result = await run_capture(store, adapters)
    assert result.outcome == "complete" and result.item_count == 2
    assert result.uploaded_bytes == 0 and adapters.source.downloads == 0


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


class PlainSeaweedStorage(EncryptedSeaweedStorage):
    async def upload_from_file(self, container, path, file_path, size, metadata=None, overwrite=True, *, tenant_id=None):
        return await Storage.upload_from_file(self, container, path, file_path, size, metadata, overwrite)


@pytest.mark.parametrize("config_row", [True, False])
async def test_required_encryption_does_not_upload_when_bootstrap_left_runtime_off(store, adapters, monkeypatch, config_row):
    from shared.crypto import runtime
    from shared.models import EncryptionConfig
    monkeypatch.setattr(runtime, "is_write_enabled", lambda: False)
    monkeypatch.setenv("BACKUP_ENCRYPTION_ENABLED", "true")
    if config_row:
        async with store.factory.begin() as session:
            session.add(EncryptionConfig(id="singleton", provider="env", enabled=True))
    adapters.source.messages = [message(attached=True)]
    adapters.storage = PlainSeaweedStorage(store.backend)
    _, result = await run_capture(store, adapters)
    assert result.outcome == "partial"
    assert adapters.storage.uploads == []
    async with store.factory() as session:
        assert not list((await session.scalars(select(ChatBlobObject).where(ChatBlobObject.state == "LIVE"))).all())


async def test_required_encryption_intent_is_not_lost_if_runtime_changes_during_upload(store, adapters, monkeypatch):
    from shared.crypto import runtime
    from shared.models import EncryptionConfig
    state = {"write_enabled": True}
    monkeypatch.setattr(runtime, "is_write_enabled", lambda: state["write_enabled"])
    async with store.factory.begin() as session:
        session.add(EncryptionConfig(id="singleton", provider="env", enabled=True))
    adapters.source.messages = [message(attached=True)]
    adapters.storage = PlainSeaweedStorage(store.backend)
    upload = adapters.storage.upload_from_file
    async def changed(*args, **kwargs):
        result = await upload(*args, **kwargs)
        state["write_enabled"] = False
        return result
    monkeypatch.setattr(adapters.storage, "upload_from_file", changed)
    _, result = await run_capture(store, adapters)
    assert result.outcome == "partial"
    async with store.factory() as session:
        assert not list((await session.scalars(select(ChatBlobObject).where(ChatBlobObject.state == "LIVE"))).all())


@pytest.mark.parametrize("version,expected_passes,outcome", [(1, 6, "partial"), (2, 2, "complete")])
async def test_hosted_order_noise_does_not_restart_v2_capture(store, adapters, monkeypatch, version, expected_passes, outcome):
    from shared.config import settings
    monkeypatch.setattr(settings, "CHAT_OBSERVATION_HASH_VERSION", version)
    adapters.source.messages = [message(hosted=True)]

    async def hosted(chat_id, message_id):
        children = [{"id": "a", "contentType": "image/png"}, {"id": "b", "contentType": "image/png"}]
        for child in (reversed(children) if adapters.source.passes % 2 == 0 else children):
            yield child

    monkeypatch.setattr(adapters.source, "iter_hosted_contents", hosted)
    _, result = await run_capture(store, adapters)
    assert result.outcome == outcome
    assert adapters.source.passes == expected_passes

async def test_completed_download_context_closes_before_immutable_upload(store, adapters, monkeypatch):
    active = 0
    original_file, original_hosted = adapters.source.open_file, adapters.source.open_hosted
    def tracked(original):
        @asynccontextmanager
        async def opened(*args,**kwargs):
            nonlocal active
            async with original(*args,**kwargs) as result:
                active += 1
                try: yield result
                finally: active -= 1
        return opened
    monkeypatch.setattr(adapters.source,'open_file',tracked(original_file))
    monkeypatch.setattr(adapters.source,'open_hosted',tracked(original_hosted))
    original_upload = adapters.storage.upload_from_file
    async def upload(*args,**kwargs):
        assert active == 0, 'provider HTTP lease retained through storage upload'
        return await original_upload(*args,**kwargs)
    monkeypatch.setattr(adapters.storage,'upload_from_file',upload)
    _, result = await run_capture(store,adapters)
    assert result.outcome == 'complete' and len(adapters.storage.uploads)==2


async def test_local_slot_wait_preserves_staged_chat_and_same_capture(store, adapters, redis_client):
    from shared.provider_admission import ScopedProviderAdmission, AdmissionLimits, ProviderRequestContext
    limiter = ScopedProviderAdmission(redis_client, limits=AdmissionLimits(tenant_concurrency=1), namespace=uuid4().hex)
    context = ProviderRequestContext('tenant', 'app', 'teams', 'chat')
    adapters.source.messages = [message()]
    original = adapters.source.iter_message_pages
    waiting, release, occupied = asyncio.Event(), asyncio.Event(), asyncio.Event()
    async def hold():
        async with limiter.admit(context, wait_budget=0):
            occupied.set()
            await release.wait()
    async def pages(chat_id):
        async for page in original(chat_id):
            yield page
        if adapters.source.passes == 1:
            waiting.set()
            async with limiter.admit(context, wait_budget=0):
                pass
    adapters.source.iter_message_pages = pages
    holder = asyncio.create_task(hold())
    await occupied.wait()
    capture = asyncio.create_task(run_capture(store, adapters))
    try:
        await asyncio.wait_for(waiting.wait(), 3)
        await asyncio.sleep(.05)
        assert not capture.done()
        async with store.factory() as session:
            assert await session.scalar(select(func.count()).select_from(ChatCaptureAttempt)) == 1
            assert await session.scalar(select(func.count()).select_from(SnapshotItem)) == 1
        release.set()
        _, result = await asyncio.wait_for(capture, 5)
        assert result.outcome == 'complete'
        assert adapters.source.passes == 2
        async with store.factory() as session:
            assert await session.scalar(select(func.count()).select_from(ChatCaptureAttempt)) == 1
    finally:
        release.set()
        for task in (holder,capture):
            if not task.done(): task.cancel()
        await asyncio.gather(holder,capture,return_exceptions=True)


async def test_same_exact_file_version_across_messages_downloaded_once(store, adapters):
    adapters.source.messages = [dict(message(attached=True),id=f'message-{i}') for i in range(3)]
    _, result = await run_capture(store, adapters)
    assert result.outcome == 'complete' and result.item_count == 6
    assert adapters.source.downloads == 1
    assert len(adapters.storage.uploads) == 1


async def test_file_reuse_never_assumes_untagged_contents_unchanged(store, adapters):
    adapters.source.messages = [dict(message(attached=True),id=f'message-{i}') for i in range(2)]
    adapters.source.etag = adapters.source.ctag = None
    _, result = await run_capture(store, adapters)
    assert result.outcome == 'complete'
    assert adapters.source.downloads == 4


async def test_cached_file_still_checks_missing_stored_content(store, adapters, monkeypatch):
    # The scenario is ordered by call count (the second message runs after the first is
    # stored), so files are captured one at a time here.
    monkeypatch.setenv("CHAT_BINARY_CONCURRENCY", "1")
    adapters.source.messages = [dict(message(attached=True),id=f'message-{i}') for i in range(2)]
    original = adapters.source.resolve_file
    calls = 0
    async def resolve(url):
        nonlocal calls
        calls += 1
        if calls == 3:  # first download refresh is call2; next reference loses its blob
            adapters.storage.objects.clear()
        return await original(url)
    adapters.source.resolve_file = resolve
    _, result = await run_capture(store, adapters)
    assert result.outcome == 'partial'


async def test_card_attachment_with_volatile_graph_id_is_stable_across_passes(store, adapters):
    """Graph returns a fresh attachment id for Adaptive Card attachments on every read (seen on Divyansh's chats)."""
    def card(identity):
        return {"id": identity, "contentType": "application/vnd.microsoft.card.adaptive",
                "content": '{"type":"AdaptiveCard","body":[{"type":"TextBlock","text":"same card"}]}'}
    adapters.source.movement = lambda passes: [dict(message(), attachments=[card(f"volatile-{passes}")],
        body={"contentType": "html", "content": f'<attachment id="volatile-{passes}"></attachment>'})]
    _, result = await run_capture(store, adapters)
    assert result.outcome == "complete" and result.item_count == 2
    assert adapters.source.passes == 2


async def member_scope(store, chat_id, principal="principal-beta", scope_hash="b" * 64):
    from shared.chat_catalogue_identity import ChatCaptureScope
    from shared.models import Job, JobStatus, JobType, Resource, ResourceType, Snapshot
    resource, job, snapshot = uuid4(), uuid4(), uuid4()
    async with store.factory.begin() as session:
        session.add(Resource(id=resource, tenant_id=store.tenant, type=ResourceType.USER_CHATS,
            external_id=principal, display_name="Other synthetic principal",
            extra_data={"user_id": principal, "chat_inventory_format": 1}))
        await session.flush()
        session.add(Job(id=job, type=JobType.BACKUP, tenant_id=store.tenant, resource_id=resource,
            status=JobStatus.RUNNING))
        await session.flush()
        session.add(Snapshot(id=snapshot, resource_id=resource, job_id=job, backend_id=store.backend,
            extra_data={"chat_inventory_format": 1}))
    return ChatCaptureScope(snapshot, resource, store.tenant, chat_id, principal, scope_hash, store.backend)


def alpha_covers(principal, filter_hash):
    return principal == "principal-alpha" and filter_hash is None


async def derive(adapters, store, scope, covers=alpha_covers, **kwargs):
    return await adapters.capture.derive_shared_chat(scope=scope, session_factory=store.factory,
        folder_path="chats/Member view", covers=covers, **kwargs)


async def test_member_derives_fresh_complete_capture_without_source_or_storage_work(store, adapters):
    """Railway 2026-11-04: 628 of 1,259 chat captures repeated a chat another member had
    just captured (same 180 objects uploaded twice). Reads are app-level, so a member's
    copy derives from a fresh complete capture of the same chat."""
    adapters.source.messages = [message(attached=True), dict(message(hosted=True), id="with-image")]
    beta_scope = await member_scope(store, "chat-alpha")
    alpha_scope, alpha = await run_capture(store, adapters)
    reads = (adapters.source.passes, adapters.source.downloads, adapters.source.hosted_downloads)
    uploads = len(adapters.storage.uploads)
    beta = await derive(adapters, store, beta_scope)
    assert alpha.outcome == beta.outcome == "complete"
    assert beta.item_count == alpha.item_count == 4 and beta.uploaded_bytes == 0
    assert (adapters.source.passes, adapters.source.downloads, adapters.source.hosted_downloads) == reads
    assert len(adapters.storage.uploads) == uploads
    async with store.factory() as session:
        attempt = await session.scalar(select(ChatCaptureAttempt).where(
            ChatCaptureAttempt.snapshot_id == beta_scope.snapshot_id))
        links = await session.scalar(select(func.count()).select_from(SnapshotItem).where(
            SnapshotItem.snapshot_id == beta_scope.snapshot_id))
    assert attempt.state == "PUBLISHED" and links == 4
    assert any(entry.get("derived_from_principal") == "principal-alpha" for entry in attempt.evidence_history)


async def test_derived_copy_keeps_only_the_members_visible_history(store, adapters):
    old = dict(message(attached=True), id="old", createdDateTime="2025-01-01T00:00:00Z")
    new = dict(message(), id="new", createdDateTime="2026-06-01T00:00:00Z")
    adapters.source.messages = [old, new]
    beta_scope = await member_scope(store, "chat-alpha")
    alpha_scope, alpha = await run_capture(store, adapters)
    beta = await derive(adapters, store, beta_scope,
        exclude_message=lambda raw: raw.get("createdDateTime", "") < "2026-01-01", scope_filter_hash="f" * 64)
    assert alpha.item_count == 3 and beta.outcome == "complete" and beta.item_count == 1
    async with store.factory() as session:
        kept = set((await session.scalars(select(SnapshotItem.external_id).where(
            SnapshotItem.snapshot_id == beta_scope.snapshot_id))).all())
    assert len(kept) == 1 and all("old" not in key for key in kept)


async def test_stale_or_non_covering_capture_is_not_a_donor(store, adapters):
    adapters.source.messages = [message()]
    beta_scope = await member_scope(store, "chat-alpha")
    await run_capture(store, adapters)
    assert await derive(adapters, store, beta_scope, covers=lambda principal, filter_hash: False) is None
    async with store.factory.begin() as session:
        await session.execute(text("UPDATE chat_capture_attempts SET created_at = created_at - interval '3 hours'"))
    assert await derive(adapters, store, beta_scope) is None


async def test_capture_read_before_this_run_started_is_not_a_donor(store, adapters):
    """Railway 2026-11-04: in the 13:52 full run, 55 chat copies came from the 11:56 run's
    captures (read up to 25 min before the run began), so those backups missed messages sent
    in between. Only data read after this member's run started is shared."""
    adapters.source.messages = [message()]
    await run_capture(store, adapters)
    beta_scope = await member_scope(store, "chat-alpha")
    assert await derive(adapters, store, beta_scope) is None


async def test_copy_of_data_read_before_this_run_is_not_a_donor(store, adapters):
    """A copy is made later than the data it holds was read; its read time decides."""
    gamma_scope = await member_scope(store, "chat-alpha", principal="principal-gamma", scope_hash="c" * 64)
    adapters.source.messages = [message()]
    await run_capture(store, adapters)
    beta_scope = await member_scope(store, "chat-alpha")
    assert (await derive(adapters, store, gamma_scope)).outcome == "complete"
    assert await derive(adapters, store, beta_scope,
        covers=lambda principal, filter_hash: principal in ("principal-alpha", "principal-gamma")) is None


async def test_long_run_still_shares_a_capture_made_hours_after_it_started(store, adapters):
    beta_scope = await member_scope(store, "chat-alpha")
    async with store.factory.begin() as session:
        await session.execute(text("UPDATE jobs SET created_at = created_at - interval '3 hours'"))
        await session.execute(text("UPDATE snapshots SET started_at = started_at - interval '3 hours'"))
    adapters.source.messages = [message()]
    await run_capture(store, adapters)
    assert (await derive(adapters, store, beta_scope)).outcome == "complete"


async def test_copy_keeps_every_message_and_file_after_the_capturers_backup_is_deleted(store, adapters):
    """Amit 2026-11-05: members who copied another member's capture must keep all their
    messages and files when that member's backup is later deleted."""
    from sqlalchemy import delete
    from shared.chat_catalogue_gc import collect_chat_catalogue
    from shared.models import Snapshot

    class Deletes:
        def __init__(self):
            self.paths = []

        def get_store_by_id(self, backend):
            return self

        async def delete(self, container, path):
            self.paths.append(path)

    async def delete_backup(scope):
        async with store.factory.begin() as session:
            await session.execute(delete(SnapshotItem).where(SnapshotItem.snapshot_id == scope.snapshot_id))
            await session.execute(delete(Snapshot).where(Snapshot.id == scope.snapshot_id))

    adapters.source.messages = [message(attached=True), dict(message(hosted=True), id="with-image")]
    beta_scope = await member_scope(store, "chat-alpha")
    alpha_scope, _ = await run_capture(store, adapters)
    beta = await derive(adapters, store, beta_scope)
    await delete_backup(alpha_scope)
    storage = Deletes()
    await collect_chat_catalogue(session_factory=store.factory, storage=storage, limit=100)
    async with store.factory() as session:
        kept = (await session.scalars(select(SnapshotItem).where(SnapshotItem.snapshot_id == beta_scope.snapshot_id))).all()
        objects = (await session.scalars(select(ChatBlobObject.state))).all()
    assert storage.paths == [] and len(kept) == beta.item_count == 4
    assert objects and set(objects) == {"LIVE"}
    await delete_backup(beta_scope)                      # the collector does delete them once nobody has them
    await collect_chat_catalogue(session_factory=store.factory, storage=storage, limit=100)
    assert len(storage.paths) == len(objects)


async def test_member_waits_instead_of_duplicating_an_active_capture(store, adapters):
    from shared import chat_catalogue as catalogue
    beta_scope = await member_scope(store, "chat-alpha")
    alpha_scope = await store.new_scope()
    async with store.factory.begin() as session:
        await catalogue.begin_capture(session, scope=alpha_scope)
    assert await derive(adapters, store, beta_scope) is adapters.capture.SHARED_CAPTURE_BUSY


async def test_retired_donor_object_falls_back_to_a_normal_capture(store, adapters, monkeypatch):
    adapters.source.messages = [message(attached=True)]
    beta_scope = await member_scope(store, "chat-alpha")
    await run_capture(store, adapters)
    async with store.factory.begin() as session:
        await session.execute(text("UPDATE chat_blob_objects SET state = 'DELETING'"))
    assert await derive(adapters, store, beta_scope) is None
    async with store.factory.begin() as session:
        await session.execute(text("UPDATE chat_blob_objects SET state = 'LIVE'"))
        assert await session.scalar(select(func.count()).select_from(SnapshotItem).where(
            SnapshotItem.snapshot_id == beta_scope.snapshot_id)) == 0
    adapters.source.permission_scope_hash = "b" * 64

    @asynccontextmanager
    async def pinned(base, *, principal_id):
        yield adapters.source

    monkeypatch.setattr(adapters.capture, "pinned_chat_source", pinned)
    beta = await adapters.capture.capture_chat_inventory(scope=beta_scope, graph_client=object(),
        storage=adapters.storage, session_factory=store.factory)
    assert beta.outcome == "complete"


async def reuse(adapters, store, scope, **kwargs):
    return await adapters.capture.reuse_unchanged_chat(scope=scope, graph_client=object(),
        session_factory=store.factory, folder_path="chats/Captured topic", **kwargs)


async def test_unchanged_chat_reuses_its_last_complete_inventory_without_listing(store, adapters, monkeypatch):
    """Codex 2026-11-04: an incremental re-read every message page of every chat. A chat whose
    newest modification predates the last complete capture reuses that capture (one request)."""
    monkeypatch.setenv("CHAT_INCREMENTAL_REUSE", "true")
    adapters.source.messages = [message(attached=True)]
    first_scope, first = await run_capture(store, adapters)
    adapters.source.latest_change = datetime(2020, 1, 1, tzinfo=timezone.utc)
    passes, downloads = adapters.source.passes, adapters.source.downloads
    second_scope = replace(await store.new_scope(), chat_id=first_scope.chat_id)
    second = await reuse(adapters, store, second_scope)
    assert first.outcome == second.outcome == "complete" and second.item_count == first.item_count == 2
    assert (adapters.source.passes, adapters.source.downloads, adapters.source.change_checks) == (passes, downloads, 1)
    async with store.factory() as session:
        attempt = await session.scalar(select(ChatCaptureAttempt).where(
            ChatCaptureAttempt.snapshot_id == second_scope.snapshot_id))
    assert any("reused_unchanged_from_attempt" in entry for entry in attempt.evidence_history)


async def chat_rows(store, snapshot_id):
    async with store.factory() as session:
        return sorted((r.external_id, r.chat_version_id, r.item_type, r.parent_external_id, r.blob_path, r.folder_path)
                      for r in (await session.scalars(select(SnapshotItem).where(SnapshotItem.snapshot_id == snapshot_id))).all())


async def test_unchanged_chat_links_its_complete_inventory_without_restaging(store, adapters, monkeypatch):
    """Railway 2026-11-05 incremental: every unchanged chat re-staged all its items (payloads loaded
    into the worker, re-hashed and written back 200 at a time): median 5.8 s per chat, 1,115 chats,
    most of a 25-minute run. The unchanged inventory is linked and its rows copied in the database."""
    monkeypatch.setenv("CHAT_INCREMENTAL_REUSE", "true")
    adapters.source.messages = [message(attached=True)]
    first_scope, first = await run_capture(store, adapters)
    adapters.source.latest_change = datetime(2020, 1, 1, tzinfo=timezone.utc)

    async def no_staging(*args, **kwargs):
        raise AssertionError("an unchanged chat must not be re-staged")
    monkeypatch.setattr(adapters.capture.catalogue, "stage_versions", no_staging)
    renamed = "chats/Renamed topic"
    previous, previous_result = first_scope, first
    for _ in range(2):  # a linked copy can itself be reused by the next incremental
        scope = replace(await store.new_scope(), chat_id=first_scope.chat_id)
        result = await adapters.capture.reuse_unchanged_chat(scope=scope, graph_client=object(),
            session_factory=store.factory, folder_path=renamed)
        assert result.outcome == "complete" and result.inventory_id == first.inventory_id
        assert result.item_count == previous_result.item_count == 2
        before, after = await chat_rows(store, previous.snapshot_id), await chat_rows(store, scope.snapshot_id)
        assert [row[:5] for row in after] == [row[:5] for row in before] and {row[5] for row in after} == {renamed}
        async with store.factory() as session:
            attempt = await session.scalar(select(ChatCaptureAttempt).where(ChatCaptureAttempt.snapshot_id == scope.snapshot_id))
            assert attempt.state == "PUBLISHED" and attempt.reused_inventory and attempt.staged_inventory_id == first.inventory_id
            assert any("reused_unchanged_from_attempt" in entry for entry in attempt.evidence_history)
            assert await session.scalar(select(func.count()).select_from(ChatInventory).where(
                ChatInventory.chat_id == first_scope.chat_id, ChatInventory.id != first.inventory_id)) == 0
        previous, previous_result = scope, result


async def test_unchanged_chat_whose_earlier_rows_are_gone_is_restaged(store, adapters, monkeypatch):
    monkeypatch.setenv("CHAT_INCREMENTAL_REUSE", "true")
    adapters.source.messages = [message(attached=True)]
    first_scope, first = await run_capture(store, adapters)
    adapters.source.latest_change = datetime(2020, 1, 1, tzinfo=timezone.utc)
    async with store.factory.begin() as session:
        await session.execute(SnapshotItem.__table__.delete().where(SnapshotItem.snapshot_id == first_scope.snapshot_id))
    scope = replace(await store.new_scope(), chat_id=first_scope.chat_id)
    result = await reuse(adapters, store, scope)
    assert result.outcome == "complete" and result.item_count == 2 and len(await chat_rows(store, scope.snapshot_id)) == 2


async def test_changed_stale_or_disabled_chat_is_captured_normally(store, adapters, monkeypatch):
    adapters.source.messages = [message()]
    first_scope, _ = await run_capture(store, adapters)
    scope = replace(await store.new_scope(), chat_id=first_scope.chat_id)
    adapters.source.latest_change = datetime(2020, 1, 1, tzinfo=timezone.utc)
    monkeypatch.setenv("CHAT_INCREMENTAL_REUSE", "false")
    assert await reuse(adapters, store, scope) is None                      # kill switch
    monkeypatch.delenv("CHAT_INCREMENTAL_REUSE")                            # production default: on
    adapters.source.latest_change = datetime.now(timezone.utc)
    assert await reuse(adapters, store, scope) is None                      # a message changed
    adapters.source.latest_change = datetime(2020, 1, 1, tzinfo=timezone.utc)
    monkeypatch.setenv("CHAT_INCREMENTAL_RECONCILE_HOURS", "0")
    checks = adapters.source.change_checks
    assert await reuse(adapters, store, scope) is None                      # due for a full reconciliation
    assert adapters.source.change_checks == checks
    monkeypatch.delenv("CHAT_INCREMENTAL_RECONCILE_HOURS")
    assert (await reuse(adapters, store, scope)).outcome == "complete"      # unchanged: reused by default


async def test_reuse_requires_the_same_capture_scope(store, adapters, monkeypatch):
    monkeypatch.setenv("CHAT_INCREMENTAL_REUSE", "true")
    adapters.source.messages = [message()]
    first_scope, _ = await run_capture(store, adapters)
    adapters.source.latest_change = datetime(2020, 1, 1, tzinfo=timezone.utc)
    other = replace(await store.new_scope(), chat_id=first_scope.chat_id, permission_scope_hash="c" * 64)
    assert await reuse(adapters, store, other) is None


async def test_second_pass_does_not_retry_content_the_first_pass_found_unavailable(store, adapters):
    """Railway 2026-11-04: a meeting chat with ~100 hosted videos answering 500 spent ~25 s of
    retries per item in BOTH observation passes (55+ min for one chat). The second pass
    re-observes membership; content pass 1 already found unavailable is not fetched again."""
    from shared.chat_capture_graph import ChatGraphError
    from shared.chat_catalogue_identity import SOURCE_UNAVAILABLE_FIELD
    adapters.source.messages = [message(hosted=True)]
    adapters.source.hosted_error = ChatGraphError("Graph capture hosted_content failed with HTTP 500", status_code=500)
    _, result = await run_capture(store, adapters)
    assert result.outcome == "complete" and result.item_count == 2
    assert adapters.source.hosted_downloads == 1


async def test_daily_schedule_reuses_an_unchanged_chat_and_reconciles_weekly(store, adapters, monkeypatch):
    """A once-a-day SLA finds the last full read ~24 h old; reuse must still apply. A full
    re-read happens once the last full read is a week old."""
    monkeypatch.delenv("CHAT_INCREMENTAL_RECONCILE_HOURS", raising=False)
    adapters.source.messages = [message()]
    first_scope, _ = await run_capture(store, adapters)
    adapters.source.latest_change = datetime(2020, 1, 1, tzinfo=timezone.utc)

    async def age(days):  # every capture of the chat, including reused copies' provenance
        when = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        async with store.factory.begin() as session:
            for attempt in (await session.scalars(select(ChatCaptureAttempt).where(
                    ChatCaptureAttempt.chat_id == first_scope.chat_id))).all():
                if attempt.evidence:
                    attempt.evidence = {**attempt.evidence, "started_at": when}
                attempt.evidence_history = [
                    {**e, "source_observed_at": when, "full_observed_at": when} if isinstance(e, dict) and "source_observed_at" in e else e
                    for e in attempt.evidence_history or []]

    await age(2)
    daily = replace(await store.new_scope(), chat_id=first_scope.chat_id)
    assert (await reuse(adapters, store, daily)).outcome == "complete"
    await age(8)
    weekly = replace(await store.new_scope(), chat_id=first_scope.chat_id)
    assert await reuse(adapters, store, weekly) is None


def hosted_message(n):
    value = message(hosted=True)
    value["id"] = f"message-{n}"
    value["_hosted"] = [{"id": f"image-{n}", "contentType": "image/png"}]
    value["body"]["content"] = value["body"]["content"].replace("messages/message/", f"messages/message-{n}/").replace("hostedContents/image/", f"hostedContents/image-{n}/")
    return value


async def test_one_chat_downloads_its_images_in_parallel_up_to_the_limit(store, adapters, monkeypatch):
    """Railway 2026-11-04: a chat's files and images were captured one after another
    (101 binaries took ~1,091 of a 1,374 s capture); a bounded pool keeps memory flat."""
    monkeypatch.setenv("CHAT_BINARY_CONCURRENCY", "4")
    adapters.source.messages = [hosted_message(n) for n in range(10)]
    adapters.source.hosted_delay = 0.05
    _, result = await run_capture(store, adapters)
    assert result.outcome == "complete" and result.item_count == 20
    assert adapters.source.hosted_peak == 4


async def test_parallel_capture_publishes_the_same_inventory_as_one_at_a_time(store, adapters, monkeypatch):
    adapters.source.messages = [hosted_message(n) for n in range(10)] + [dict(message(attached=True), id="with-file")]
    monkeypatch.setenv("CHAT_BINARY_CONCURRENCY", "1")
    _, serial = await run_capture(store, adapters)
    monkeypatch.setenv("CHAT_BINARY_CONCURRENCY", "6")
    _, parallel = await run_capture(store, adapters)
    assert serial.outcome == parallel.outcome == "complete"
    async with store.factory() as session:
        hashes = [
            await session.scalar(select(ChatInventory.membership_sha256).where(ChatInventory.id == r.inventory_id))
            for r in (serial, parallel)]
    assert serial.item_count == parallel.item_count and hashes[0] == hashes[1]


async def test_reused_children_are_staged_in_batches_not_one_transaction_each(store, adapters, monkeypatch):
    """Railway 2026-11-05: re-reading a 1,369-message chat after one edit took 5.8 min. Each reused
    image/file was staged in its own transaction, and every staging locks the capture's inventory
    row, so 8 parallel children queued on it (median 4.8 s per child)."""
    from shared import chat_catalogue
    messages = []
    for i in range(5):
        stamped = dict(message(hosted=True), id=f"message-{i}", lastModifiedDateTime="2026-11-01T10:00:00Z", etag=f"17800000000{i}")
        stamped["_hosted"] = [{"id": f"image-{i}", "contentType": "image/png"}]
        stamped["body"] = {"contentType": "html", "content": f'<img src="https://graph.microsoft.com/v1.0/chats/chat/messages/message-{i}/hostedContents/image-{i}/$value">'}
        messages.append(stamped)
    adapters.source.messages = messages
    _, first = await run_capture(store, adapters)
    assert first.outcome == "complete" and adapters.source.hosted_downloads == 5
    batches = []
    original = chat_catalogue.stage_versions

    async def counted(session, *, lease, items):
        batches.append([item.item_type for item in items])
        return await original(session, lease=lease, items=items)
    monkeypatch.setattr(chat_catalogue, "stage_versions", counted)
    _, second = await run_capture(store, adapters)
    assert second.outcome == "complete" and adapters.source.hosted_downloads == 5
    single_child = [b for b in batches if b == ["CHAT_HOSTED_CONTENT"]]
    assert single_child == [] and sum(b.count("CHAT_HOSTED_CONTENT") for b in batches) == 5
