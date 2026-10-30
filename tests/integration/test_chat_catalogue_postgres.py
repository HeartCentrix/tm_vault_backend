"""Catalogue transactions against explicitly configured disposable PostgreSQL."""
from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
import importlib
import os
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")

import pytest
import pytest_asyncio
from sqlalchemy import delete, event, select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from shared.database import Base
import shared.models as models
from shared.chat_catalogue_identity import (
    CaptureEvidence, CapturedChatItem, ChatCaptureScope, ChatContentDescriptor,
    qualified_chat_item_key, source_inventory_fingerprint,
)

pytestmark = pytest.mark.integration


def api():
    return importlib.import_module("shared.chat_catalogue")


def message(body="original", graph_id="message-alpha"):
    return CapturedChatItem("TEAMS_CHAT_MESSAGE", graph_id, None,
        {"id": graph_id, "body": {"content": body},
         "chat_catalogue_expected_children": {"complete": True, "items": []}}, None)


def parent_for(*children, parent=None):
    parent = parent or message(graph_id=children[0].parent_graph_id)
    return replace(parent, payload=parent.payload | {
        "attachments": [{**child.payload, "id": child.graph_id} for child in children if child.item_type == "CHAT_ATTACHMENT"],
        "hostedContents": [{**child.payload, "id": child.graph_id} for child in children if child.item_type == "CHAT_HOSTED_CONTENT"],
        "chat_catalogue_expected_children": {"complete": True, "items": [
            {"item_type": child.item_type, "graph_id": child.graph_id, "requires_content": child.content is not None}
            for child in children]}})


def apply_catalogue_migration(conn):
    from alembic.migration import MigrationContext
    from alembic.operations import Operations
    spec = importlib.util.spec_from_file_location("chat_catalogue_migration", Path(__file__).resolve().parents[2] / "alembic/versions/0011_chat_catalogue.py")
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    context = MigrationContext.configure(conn)
    with Operations.context(context):
        with context.begin_transaction():
            migration.upgrade()


@pytest_asyncio.fixture
async def store():
    url = os.environ.get("TMVAULT_TEST_DATABASE_URL")
    if not url:
        pytest.skip("requires explicitly configured disposable PostgreSQL")
    schema = "chat_catalogue_test_" + uuid4().hex
    engine = create_async_engine(url, connect_args={
        "server_settings": {"search_path": schema, "application_name": schema}, "statement_cache_size": 0})
    async with engine.begin() as conn:
        await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        await conn.run_sync(Base.metadata.create_all)
        await conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS uq_snapshot_items_snap_ext_type "
                                "ON snapshot_items(snapshot_id, external_id, item_type)"))
    factory = async_sessionmaker(engine, expire_on_commit=False)
    tenant, resource, backend = uuid4(), uuid4(), uuid4()
    async with factory.begin() as session:
        org = models.Organization(name="Synthetic", slug=uuid4().hex)
        session.add(org)
        await session.flush()
        session.add(models.Tenant(id=tenant, org_id=org.id, display_name="Synthetic"))
        session.add(models.StorageBackend(id=backend, kind="azure", name=uuid4().hex,
                    endpoint="https://storage.invalid", config={}, secret_ref="synthetic-reference"))
        await session.flush()
        session.add(models.Resource(id=resource, tenant_id=tenant, type=models.ResourceType.USER_CHATS,
            external_id="principal-alpha", display_name="Synthetic chats",
            extra_data={"user_id": "principal-alpha", "chat_inventory_format": 1}))

    async def new_scope(**changes):
        job_id, snapshot_id = uuid4(), uuid4()
        async with factory.begin() as session:
            session.add(models.Job(id=job_id, type=models.JobType.BACKUP, tenant_id=tenant,
                                   resource_id=resource, status=models.JobStatus.RUNNING))
            await session.flush()
            session.add(models.Snapshot(id=snapshot_id, resource_id=resource, job_id=job_id,
                                        backend_id=backend, extra_data={"chat_inventory_format": 1}))
        return ChatCaptureScope(snapshot_id, resource, tenant, "chat-alpha", "principal-alpha", "a" * 64, backend, **changes)

    async def begin(scope, **kwargs):
        async with factory.begin() as session:
            return await api().begin_capture(session, scope=scope, **kwargs)

    async def stage(lease, items):
        async with factory.begin() as session:
            return await api().stage_versions(session, lease=lease, items=items)

    async def observe(scope, lease, items, **changes):
        now = datetime.now(timezone.utc)
        binary_count = sum(i.content is not None for i in items)
        fingerprint = source_inventory_fingerprint(scope=scope, items=items)
        values = dict(enumeration_complete=True, reconciliation_complete=True,
                      membership_sha256=fingerprint, reconciliation_sha256=fingerprint,
                      conflicting_observations=0, required_content_count=binary_count,
                      durable_content_count=binary_count, unresolved_required_count=0,
                      started_at=now - timedelta(seconds=1), finished_at=now)
        async with factory.begin() as session:
            await api().record_capture_evidence(session, lease=lease, evidence=CaptureEvidence(**(values | changes)))

    async def publish(lease):
        async with factory.begin() as session:
            return await api().publish_inventory(session, lease=lease)

    async def capture(items, scope=None, **evidence_changes):
        scope = scope or await new_scope()
        lease = await begin(scope)
        await stage(lease, items)
        await observe(scope, lease, items, **evidence_changes)
        return scope, lease, await publish(lease)

    async def binary(lease, *, state="LIVE", parent="message-alpha", child="child-alpha"):
        descriptor = ChatContentDescriptor(uuid4(), backend, "catalogue", "chat-catalogue/v1/" + uuid4().hex,
            hashlib.sha256(b"abc").hexdigest(), 3, "key-version-1", 1, None,
            "drive-alpha", "file-alpha", "etag-a", "ctag-a")
        async with factory.begin() as session:
            session.add(models.ChatBlobObject(id=descriptor.object_id, tenant_id=tenant, backend_id=backend,
                container=descriptor.container, path=descriptor.path, sha256=descriptor.sha256,
                size=3, encryption_key_id=descriptor.encryption_key_id, backup_version=1,
                state=state, capture_attempt_id=lease.attempt_id, uploaded_bytes=3))
        return CapturedChatItem("CHAT_ATTACHMENT", child, parent, {"contentType": "reference"}, descriptor)

    try:
        yield SimpleNamespace(**locals())
    finally:
        async with engine.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        await engine.dispose()


async def test_complete_empty_inventory_is_persisted_and_reused(store):
    _, _, first = await store.capture([])
    _, _, second = await store.capture([])
    assert first.outcome == second.outcome == "complete"
    assert first.item_count == second.item_count == 0
    assert second.reused and first.inventory_id == second.inventory_id


async def test_unchanged_version_reuse_avoids_retransmitting_heavy_payload(store):
    _, _, first = await store.capture([message("x" * 10000)])
    scope = await store.new_scope()
    lease = await store.begin(scope)
    statements = []
    def record(conn, cursor, statement, parameters, context, executemany):
        statements.append((statement.lower(), parameters))
    event.listen(store.engine.sync_engine, "before_cursor_execute", record)
    try:
        staged = await store.stage(lease, [message("x" * 10000)])
    finally:
        event.remove(store.engine.sync_engine, "before_cursor_execute", record)
    assert staged.staged_items == 1 and staged.new_versions == 0
    assert not any("insert into chat_item_versions" in sql for sql, _ in statements)
    assert not any("chat_item_versions.payload," in sql or "chat_item_versions.content," in sql for sql, _ in statements)
    await store.observe(scope, lease, [message("x" * 10000)])
    second = await store.publish(lease)
    assert second.reused and second.inventory_id == first.inventory_id


async def test_text_message_sizes_use_canonical_utf8_payload_without_duplicating_metadata(store):
    original = message("café")
    reordered = replace(original, payload={
        "chat_catalogue_expected_children": {"items": [], "complete": True},
        "body": {"content": "café"}, "id": "message-alpha"})
    expected_size = len(b'{"body":{"content":"caf\xc3\xa9"},"chat_catalogue_expected_children":{"complete":true,"items":[]},"id":"message-alpha"}')
    first_scope, first_lease, first = await store.capture([original])
    second_scope, _, second = await store.capture([reordered])
    assert first.logical_bytes == second.logical_bytes == expected_size
    assert first.inventory_id == second.inventory_id and second.reused
    assert (await store.publish(first_lease)).logical_bytes == expected_size
    async with store.factory() as session:
        versions = list((await session.scalars(select(models.ChatItemVersion))).all())
        links = list((await session.scalars(select(models.SnapshotItem).where(
            models.SnapshotItem.snapshot_id.in_([first_scope.snapshot_id, second_scope.snapshot_id])))).all())
        assert len(versions) == 1 and len(links) == 2
        assert {link.chat_version_id for link in links} == {versions[0].id}
        assert all(link.content_size == expected_size for link in links)
        assert all(link.extra_data == {"chat_id": first_scope.chat_id} for link in links)
        assert versions[0].payload["body"] == {"content": "café"}
        hydrated = await api().materialize_chat_items(session, links)
        assert all(item.content_size == expected_size for item in hydrated)


async def test_changed_metadata_and_deleted_membership_create_distinct_inventories(store):
    _, _, first = await store.capture([message(), message(graph_id="message-beta")])
    _, _, edited = await store.capture([message("edited"), message(graph_id="message-beta")])
    _, _, removed = await store.capture([message("edited")])
    assert len({first.inventory_id, edited.inventory_id, removed.inventory_id}) == 3


async def test_folder_rename_reuses_versions_and_inventory_but_keeps_each_snapshot_presentation(store):
    scopes = [await store.new_scope(), await store.new_scope()]
    results = []
    for scope, label in zip(scopes, ("chats/Old topic", "chats/New topic")):
        lease = await store.begin(scope, folder_path=label)
        staged = await store.stage(lease, [message("Captured display text")])
        if results:
            assert staged.new_versions == 0
        await store.observe(scope, lease, [message("Captured display text")])
        results.append(await store.publish(lease))
    assert results[0].inventory_id == results[1].inventory_id and results[1].reused
    async with store.factory() as session:
        first = await session.scalar(select(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == scopes[0].snapshot_id))
        second = await session.scalar(select(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == scopes[1].snapshot_id))
        assert first.chat_version_id == second.chat_version_id
        hydrated = await api().materialize_chat_items(session, [first, second])
        assert [item.folder_path for item in hydrated] == ["chats/Old topic", "chats/New topic"]
        assert [item.name for item in hydrated] == ["Captured display text", "Captured display text"]


async def test_partial_is_readable_but_cannot_block_later_complete_with_same_subset(store):
    first_scope, _, partial = await store.capture([message()], required_content_count=1,
                                                 durable_content_count=0, unresolved_required_count=1)
    assert partial.outcome == "partial" and not partial.reused and partial.failure_reason
    async with store.factory() as session:
        links = list((await session.scalars(select(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == first_scope.snapshot_id))).all())
        hydrated = await api().materialize_chat_items(session, links)
        assert hydrated[0].external_id == "message-alpha"
        assert hydrated[0].extra_data["raw"]["body"]["content"] == "original"
        assert links[0].external_id != hydrated[0].external_id
    with pytest.raises(api().ChatCatalogueConflict):
        await store.begin(first_scope)
    _, _, complete = await store.capture([message()])
    assert complete.outcome == "complete" and complete.inventory_id != partial.inventory_id


@pytest.mark.parametrize("evidence_changes", [
    {"enumeration_complete": False}, {"reconciliation_complete": False},
    {"reconciliation_sha256": "b" * 64}, {"conflicting_observations": 1},
])
async def test_incomplete_evidence_never_publishes_complete(store, evidence_changes):
    _, _, result = await store.capture([message()], **evidence_changes)
    assert result.outcome == "partial" and not result.reused


async def test_no_data_and_incomplete_enumeration_is_failed_not_empty_complete(store):
    _, _, result = await store.capture([], enumeration_complete=False)
    assert result.outcome == "failed" and result.inventory_id is None


@pytest.mark.parametrize("kind", ["binary_attachment", "hosted_content", "metadata_card", "no_manifest"])
async def test_persisted_expected_children_cannot_be_hidden_by_success_flags_and_zero_counts(store, kind):
    parent = message()
    if kind == "binary_attachment":
        parent = replace(parent, payload=parent.payload | {"attachments": [{"id": "missing-child", "contentType": "reference"}]})
    elif kind in {"hosted_content", "metadata_card"}:
        child_type = "CHAT_HOSTED_CONTENT" if kind == "hosted_content" else "CHAT_ATTACHMENT"
        parent = replace(parent, payload=parent.payload | {"chat_catalogue_expected_children": {
            "complete": True, "items": [{"item_type": child_type, "graph_id": "missing-child", "requires_content": kind == "hosted_content"}]}})
    else:
        parent = replace(parent, payload={"id": parent.graph_id, "body": {"content": "captured"}})
    _, _, partial = await store.capture([parent])
    assert partial.outcome == "partial" and not partial.reused
    async with store.factory() as session:
        inventory = await session.get(models.ChatInventory, partial.inventory_id)
        assert inventory.evidence["validation_gaps"]


async def test_detached_message_dto_preserves_public_raw_envelope_without_internal_manifest(store):
    from shared.schemas import SnapshotItemResponse
    scope, _, _ = await store.capture([message()])
    async with store.factory() as session:
        tracked = await session.scalar(select(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == scope.snapshot_id))
        dto, = await api().materialize_chat_items(session, [tracked])
        response = SnapshotItemResponse(id=str(dto.id), snapshotId=str(dto.snapshot_id),
            externalId=dto.external_id, itemType=dto.item_type, name=dto.name,
            metadata=dto.extra_data, createdAt=dto.created_at.isoformat(),
            contentSize=dto.content_size, isDeleted=dto.is_deleted)
        assert response.metadata["raw"]["body"]["content"] == "original"
        assert response.metadata["chatId"] == scope.chat_id
        assert "chat_catalogue_expected_children" not in response.metadata["raw"]
        assert tracked.external_id != dto.external_id and tracked.extra_data == {"chat_id": scope.chat_id}


async def test_mixed_legacy_hydration_preserves_existing_metadata_without_mutating_tracked_rows(store):
    scope = await store.new_scope()
    metadata = {"download_url": "https://files.invalid/legacy?sig=synthetic", "raw": {"body": "legacy"}}
    async with store.factory.begin() as session:
        snapshot = await session.get(models.Snapshot, scope.snapshot_id)
        snapshot.extra_data = {}
        session.add(models.SnapshotItem(snapshot_id=scope.snapshot_id, tenant_id=scope.tenant_id,
            external_id="legacy-message", item_type="TEAMS_CHAT_MESSAGE", name="Legacy",
            backend_id=scope.backend_id, extra_data=metadata))
    async with store.factory() as session:
        tracked = await session.scalar(select(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == scope.snapshot_id))
        dto, = await api().materialize_chat_items(session, [tracked])
        assert dto.extra_data == metadata and dto.external_id == "legacy-message"
        dto.extra_data["raw"]["body"] = "detached mutation"
        assert tracked.extra_data == metadata


async def test_caller_content_counts_cannot_hide_non_live_object(store):
    scope = await store.new_scope()
    lease = await store.begin(scope)
    binary = await store.binary(lease, state="STAGING")
    await store.stage(lease, [parent_for(binary), binary])
    await store.observe(scope, lease, [parent_for(binary), binary])
    result = await store.publish(lease)
    assert result.outcome == "partial" and not result.reused


async def test_repeated_object_id_cannot_hide_a_conflicting_content_descriptor(store):
    scope = await store.new_scope()
    lease = await store.begin(scope)
    binary = await store.binary(lease)
    conflicting = replace(binary, graph_id="child-beta", content=replace(binary.content, sha256="f" * 64))
    with pytest.raises(api().ChatCatalogueIntegrityError):
        await store.stage(lease, [message(), conflicting, binary])


async def test_shared_bytes_keep_distinct_file_source_identities_and_replay_upload_totals(store):
    scope = await store.new_scope()
    lease = await store.begin(scope)
    first = await store.binary(lease)
    second = replace(first, graph_id="child-beta", content=replace(first.content, source_item_id="file-beta"))
    items = [parent_for(first, second), first, second]
    await store.stage(lease, items)
    await store.observe(scope, lease, items)
    result = await store.publish(lease)
    replay = await store.publish(lease)
    assert result.outcome == "complete" and result.logical_bytes == replay.logical_bytes
    async with store.factory() as session:
        links = list((await session.scalars(select(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == scope.snapshot_id))).all())
        assert result.logical_bytes == sum(link.content_size for link in links)
        assert [link.content_size for link in links if link.item_type == "TEAMS_CHAT_MESSAGE"][0] > 0
        assert [link.content_size for link in links if link.item_type == "CHAT_ATTACHMENT"] == [3, 3]
    assert result.uploaded_bytes == replay.uploaded_bytes == 3


@pytest.mark.parametrize("change", [{"parent_external_id": "wrong-parent"}, {"item_type": "TEAMS_MESSAGE"}, {"is_deleted": True}, {"content_size": 999}])
async def test_publication_checks_link_parent_type_and_visibility_flags(store, change):
    scope = await store.new_scope()
    lease = await store.begin(scope)
    await store.stage(lease, [message()])
    await store.observe(scope, lease, [message()])
    async with store.factory.begin() as session:
        await session.execute(update(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == scope.snapshot_id).values(**change))
    with pytest.raises(api().ChatCatalogueIntegrityError):
        await store.publish(lease)


async def test_child_links_qualify_the_observed_existing_message_type(store):
    scope = await store.new_scope()
    lease = await store.begin(scope)
    parent = replace(message(), item_type="TEAMS_MESSAGE")
    child = CapturedChatItem("CHAT_ATTACHMENT", "card-alpha", parent.graph_id,
        {"contentType": "application/vnd.microsoft.card.adaptive"}, None)
    parent = parent_for(child, parent=parent)
    await store.stage(lease, [child, parent])
    await store.observe(scope, lease, [child, parent])
    assert (await store.publish(lease)).outcome == "complete"
    async with store.factory() as session:
        link = await session.scalar(select(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == scope.snapshot_id, models.SnapshotItem.item_type == "CHAT_ATTACHMENT"))
        assert link.parent_external_id == qualified_chat_item_key(scope.chat_id, parent.item_type, parent.graph_id)


async def test_complete_inventory_cannot_have_an_orphan_child(store):
    child = CapturedChatItem("CHAT_ATTACHMENT", "card-alpha", "missing-message",
        {"contentType": "application/vnd.microsoft.card.adaptive"}, None)
    with pytest.raises(api().ChatCatalogueIntegrityError):
        await store.capture([child])


@pytest.mark.parametrize("tamper", ["missing", "extra", "wrong_version", "missing_fk"])
async def test_exact_persisted_link_set_rejects_corruption_even_with_equal_counts(store, tamper):
    _, _, old = await store.capture([message("old")])
    scope = await store.new_scope()
    lease = await store.begin(scope)
    await store.stage(lease, [message()])
    await store.observe(scope, lease, [message()])
    async with store.factory.begin() as session:
        link = await session.scalar(select(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == scope.snapshot_id))
        if tamper == "missing":
            await session.delete(link)
        elif tamper == "extra":
            link.external_id = "unobserved-key"
        elif tamper == "missing_fk":
            link.chat_version_id = None
        else:
            link.chat_version_id = await session.scalar(select(models.ChatInventoryItem.version_id).where(models.ChatInventoryItem.inventory_id == old.inventory_id))
    with pytest.raises(api().ChatCatalogueIntegrityError):
        await store.publish(lease)


async def test_terminal_snapshot_chat_membership_cannot_be_rewritten_on_redelivery(store):
    scope, lease, first = await store.capture([message()])
    replay = await store.publish(lease)
    assert replay.inventory_id == first.inventory_id
    with pytest.raises(api().ChatCatalogueConflict):
        await store.begin(scope)
    with pytest.raises(api().ChatCatalogueConflict):
        await store.stage(lease, [message("new observation")])


async def test_expired_and_superseded_fences_cannot_stage_or_publish(store):
    scope = await store.new_scope()
    lease = await store.begin(scope)
    async with store.factory.begin() as session:
        await session.execute(update(models.ChatCaptureAttempt).where(models.ChatCaptureAttempt.id == lease.attempt_id).values(expires_at=datetime.now(timezone.utc) - timedelta(seconds=1)))
    with pytest.raises(api().ChatCatalogueConflict):
        await store.stage(lease, [message()])
    newer = await store.begin(scope)
    assert newer.generation > lease.generation
    with pytest.raises(api().ChatCatalogueConflict):
        await store.publish(lease)


async def test_lease_renewal_is_fenced_and_does_not_revive_an_expired_attempt(store):
    scope = await store.new_scope()
    lease = await store.begin(scope)
    async with store.factory.begin() as session:
        renewed = await api().renew_capture(session, lease=lease, lease_seconds=600)
    assert renewed.generation == lease.generation and renewed.expires_at > lease.expires_at
    async with store.factory.begin() as session:
        await session.execute(update(models.ChatCaptureAttempt).where(models.ChatCaptureAttempt.id == lease.attempt_id).values(expires_at=datetime.now(timezone.utc) - timedelta(seconds=1)))
    async with store.factory.begin() as session:
        with pytest.raises(api().ChatCatalogueConflict):
            await api().renew_capture(session, lease=renewed)


async def test_different_chats_under_same_job_stage_concurrently_and_cancellation_still_fences(store):
    first_scope = await store.new_scope()
    second_scope = replace(first_scope, chat_id="chat-beta")
    first, second = await store.begin(first_scope), await store.begin(second_scope)
    async def wait_for_lock(fragment):
        for _ in range(100):
            async with store.engine.connect() as observer:
                waiting = await observer.scalar(text("SELECT count(*) FROM pg_stat_activity WHERE application_name=:app AND wait_event_type='Lock' AND query LIKE :fragment"), {"app": store.schema, "fragment": "%" + fragment + "%"})
            if waiting:
                return
            await asyncio.sleep(0.01)
        pytest.fail("expected PostgreSQL row-lock wait")
    async def cancel():
        async with store.factory.begin() as session:
            snapshot = await session.get(models.Snapshot, first_scope.snapshot_id)
            await session.execute(update(models.Job).where(models.Job.id == snapshot.job_id).values(status=models.JobStatus.CANCELLED))
    async with store.factory.begin() as blocker:
        await blocker.scalar(select(models.ChatInventory).where(models.ChatInventory.id == first.staged_inventory_id).with_for_update())
        staging = asyncio.create_task(store.stage(first, [message()]))
        await wait_for_lock("chat_inventories")
        assert (await asyncio.wait_for(store.stage(second, [message()]), timeout=1)).staged_items == 1
        cancelling = asyncio.create_task(cancel())
        await wait_for_lock("UPDATE jobs")
        assert not cancelling.done()
    await staging
    await cancelling
    with pytest.raises(api().ChatCatalogueConflict):
        await store.publish(first)


@pytest.mark.parametrize("change", ["cancel", "snapshot_terminal", "snapshot_delete", "wrong_tenant", "wrong_backend", "partition_missing"])
async def test_capture_requires_current_snapshot_job_and_scope_eligibility(store, change):
    scope = await store.new_scope()
    if change in {"wrong_tenant", "wrong_backend", "partition_missing"}:
        broken = replace(scope, **{"wrong_tenant": {"tenant_id": uuid4()}, "wrong_backend": {"backend_id": uuid4()}, "partition_missing": {"partition_id": uuid4()}}[change])
        with pytest.raises(api().ChatCatalogueConflict):
            await store.begin(broken)
        return
    lease = await store.begin(scope)
    async with store.factory.begin() as session:
        snapshot = await session.get(models.Snapshot, scope.snapshot_id)
        if change == "cancel":
            await session.execute(update(models.Job).where(models.Job.id == snapshot.job_id).values(status=models.JobStatus.CANCELLED))
        elif change == "snapshot_terminal":
            snapshot.status = models.SnapshotStatus.FAILED
        else:
            await session.delete(snapshot)
    with pytest.raises(api().ChatCatalogueConflict):
        await store.publish(lease)


@pytest.mark.parametrize("change", ["wrong_principal", "wrong_resource_type", "resource_not_adopted"])
async def test_capture_scope_is_bound_to_the_adopted_user_chats_resource(store, change):
    scope = await store.new_scope()
    if change == "wrong_principal":
        scope = replace(scope, principal_id="principal-beta")
    else:
        async with store.factory.begin() as session:
            resource = await session.get(models.Resource, scope.resource_id)
            if change == "wrong_resource_type":
                resource.type = models.ResourceType.USER_MAIL
            else:
                resource.extra_data = {"user_id": "principal-alpha"}
    with pytest.raises(api().ChatCatalogueConflict):
        await store.begin(scope)


async def test_same_raw_ids_across_chats_and_parents_keep_distinct_links(store):
    first = await store.new_scope()
    second = replace(first, chat_id="chat-beta")
    first_lease = await store.begin(first)
    second_lease = await store.begin(second)
    child = lambda parent: CapturedChatItem("CHAT_ATTACHMENT", "card-alpha", parent,
        {"contentType": "application/vnd.microsoft.card.adaptive", "content": {"text": "card"}}, None)
    for scope, lease in ((first, first_lease), (second, second_lease)):
        items = [parent_for(child("message-alpha")), parent_for(child("message-beta")), child("message-alpha"), child("message-beta")]
        await store.stage(lease, items)
        await store.observe(scope, lease, items)
        assert (await store.publish(lease)).item_count == 4
    async with store.factory() as session:
        links = list((await session.scalars(select(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == first.snapshot_id))).all())
        assert len({i.external_id for i in links}) == 8
        for link in links:
            if link.item_type == "CHAT_ATTACHMENT":
                assert link.parent_external_id in {i.external_id for i in links if i.item_type == "TEAMS_CHAT_MESSAGE"}


async def test_same_source_under_different_recorded_scope_does_not_reuse_inventory(store):
    _, _, first = await store.capture([message()])
    scope = replace(await store.new_scope(), permission_scope_hash="b" * 64)
    _, _, second = await store.capture([message()], scope)
    assert second.inventory_id != first.inventory_id and not second.reused


async def test_foreign_version_cannot_be_smuggled_into_inventory(store):
    _, _, first = await store.capture([message()])
    scope = replace(await store.new_scope(), chat_id="chat-beta")
    lease = await store.begin(scope)
    await store.stage(lease, [message()])
    await store.observe(scope, lease, [message()])
    async with store.factory.begin() as session:
        foreign_version = await session.scalar(select(models.ChatInventoryItem.version_id).where(models.ChatInventoryItem.inventory_id == first.inventory_id))
        await session.execute(update(models.ChatInventoryItem).where(models.ChatInventoryItem.inventory_id == lease.staged_inventory_id).values(version_id=foreign_version))
    with pytest.raises(api().ChatCatalogueIntegrityError):
        await store.publish(lease)


async def test_stage_is_bounded_and_duplicate_delivery_reports_actual_successes(store):
    scope = await store.new_scope()
    lease = await store.begin(scope)
    items = [message(graph_id=f"message-{n}") for n in range(201)]
    with pytest.raises(ValueError, match="200"):
        await store.stage(lease, items)
    staged = await store.stage(lease, items[:200])
    replay = await store.stage(lease, items[:200])
    assert staged.staged_items == 200 and staged.new_versions == 200
    assert replay.staged_items == replay.new_versions == 0


async def test_abort_preserves_captured_items_as_partial(store):
    scope = await store.new_scope()
    lease = await store.begin(scope)
    await store.stage(lease, [message()])
    async with store.factory.begin() as session:
        await api().abort_capture(session, lease=lease, reason="synthetic source failure")
    async with store.factory() as session:
        association = await session.get(models.SnapshotChatInventory, (scope.snapshot_id, scope.chat_id))
        assert association.coverage_status == "PARTIAL"
        assert association.failure_reason == "synthetic source failure"


async def test_catalogue_fk_protects_referenced_versions_and_objects(store):
    scope = await store.new_scope()
    lease = await store.begin(scope)
    binary = await store.binary(lease)
    await store.stage(lease, [parent_for(binary), binary])
    await store.observe(scope, lease, [parent_for(binary), binary])
    await store.publish(lease)
    async with store.factory() as session:
        with pytest.raises(IntegrityError):
            await session.execute(delete(models.ChatBlobObject).where(models.ChatBlobObject.id == binary.content.object_id))
        await session.rollback()
        version_id = await session.scalar(select(models.SnapshotItem.chat_version_id).where(models.SnapshotItem.snapshot_id == scope.snapshot_id).limit(1))
        with pytest.raises(IntegrityError):
            await session.execute(delete(models.ChatItemVersion).where(models.ChatItemVersion.id == version_id))


async def test_hydration_reports_retired_objects_instead_of_serving_a_stale_descriptor(store):
    scope = await store.new_scope()
    lease = await store.begin(scope)
    binary = await store.binary(lease)
    await store.stage(lease, [parent_for(binary), binary])
    await store.observe(scope, lease, [parent_for(binary), binary])
    await store.publish(lease)
    async with store.factory.begin() as session:
        await session.execute(update(models.ChatBlobObject).where(models.ChatBlobObject.id == binary.content.object_id).values(state="DELETING"))
    async with store.factory() as session:
        links = list((await session.scalars(select(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == scope.snapshot_id))).all())
        with pytest.raises(api().ChatCatalogueIntegrityError):
            await api().materialize_chat_items(session, links)


async def test_simultaneous_publication_is_idempotent(store):
    scope = await store.new_scope()
    lease = await store.begin(scope)
    await store.stage(lease, [message()])
    await store.observe(scope, lease, [message()])
    first, second = await asyncio.gather(store.publish(lease), store.publish(lease))
    assert first.inventory_id == second.inventory_id and first.item_count == second.item_count == 1


async def test_simultaneous_independent_captures_deduplicate_complete_inventory_atomically(store):
    scopes = [await store.new_scope(), await store.new_scope()]
    leases = [await store.begin(scope) for scope in scopes]
    for scope, lease in zip(scopes, leases):
        await store.stage(lease, [message()])
        await store.observe(scope, lease, [message()])
    first, second = await asyncio.gather(*(store.publish(lease) for lease in leases))
    assert first.inventory_id == second.inventory_id
    assert sorted([first.reused, second.reused]) == [False, True]


async def test_publisher_waiting_on_object_lock_rechecks_retirement_state(store):
    scope = await store.new_scope()
    lease = await store.begin(scope)
    binary = await store.binary(lease)
    await store.stage(lease, [parent_for(binary), binary])
    await store.observe(scope, lease, [parent_for(binary), binary])
    async with store.factory.begin() as deletion:
        await deletion.execute(update(models.ChatBlobObject).where(models.ChatBlobObject.id == binary.content.object_id).values(state="DELETING"))
        publishing = asyncio.create_task(store.publish(lease))
        # Prove the publisher reached PostgreSQL's row-lock wait, not a sleep race.
        for _ in range(100):
            async with store.engine.connect() as observer:
                waiting = await observer.scalar(text("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%chat_blob_objects%'"))
            if waiting:
                break
            await asyncio.sleep(0.01)
        assert waiting
    with pytest.raises(api().ChatCatalogueIntegrityError):
        await publishing


async def test_publisher_rechecks_lease_expiry_after_waiting_for_an_object_lock(store):
    scope = await store.new_scope()
    lease = await store.begin(scope)
    binary = await store.binary(lease)
    await store.stage(lease, [parent_for(binary), binary])
    await store.observe(scope, lease, [parent_for(binary), binary])
    expires = datetime.now(timezone.utc) + timedelta(seconds=1)
    async with store.factory.begin() as session:
        await session.execute(update(models.ChatCaptureAttempt).where(models.ChatCaptureAttempt.id == lease.attempt_id).values(expires_at=expires))
    async with store.factory.begin() as blocker:
        await blocker.scalar(select(models.ChatBlobObject).where(models.ChatBlobObject.id == binary.content.object_id).with_for_update())
        publishing = asyncio.create_task(store.publish(lease))
        for _ in range(100):
            async with store.engine.connect() as observer:
                waiting = await observer.scalar(text("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%chat_blob_objects%'"))
            if waiting:
                break
            await asyncio.sleep(0.01)
        assert waiting
        await asyncio.sleep(max(0, (expires - datetime.now(timezone.utc)).total_seconds()) + 0.03)
    with pytest.raises(api().ChatCatalogueConflict):
        await publishing


async def test_migration_upgrade_preserves_legacy_rows_and_readiness_requires_catalogue(store, monkeypatch):
    import shared.database as database
    assert "chat_capture_attempts" in database.REQUIRED_TABLES
    assert "chat_version_id" in database.REQUIRED_COLUMNS["snapshot_items"]
    monkeypatch.setattr(database.settings, "DB_SCHEMA", store.schema)
    scope = await store.new_scope()
    async with store.factory.begin() as session:
        session.add(models.SnapshotItem(snapshot_id=scope.snapshot_id, tenant_id=scope.tenant_id,
            external_id="legacy-item", item_type="TEAMS_CHAT_MESSAGE", name="Legacy",
            backend_id=scope.backend_id, extra_data={"body": "retained legacy payload"}))
    async with store.engine.begin() as conn:
        assert await database._has_required_tables(conn)
        assert await database._has_required_columns(conn)
        await conn.execute(text("ALTER TABLE snapshot_items DROP COLUMN chat_version_id"))
        for table in ("chat_capture_attempts", "chat_blob_objects", "chat_item_versions", "chat_inventories",
                      "chat_inventory_items", "snapshot_chat_inventories", "chat_catalogue_pins"):
            await conn.execute(text(f"DROP TABLE IF EXISTS {table} CASCADE"))
        assert not await database._has_required_tables(conn)
        assert not await database._has_required_columns(conn)
    validation_locks = []
    def observe_validation(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("ALTER TABLE snapshot_items") and "fk_snapshot_items_chat_version" in statement:
            validated = conn.exec_driver_sql("SELECT convalidated FROM pg_constraint WHERE conrelid=to_regclass('snapshot_items') AND conname='fk_snapshot_items_chat_version'").scalar()
            if validated:
                validation_locks.append(set(conn.exec_driver_sql("SELECT mode FROM pg_locks WHERE pid=pg_backend_pid() AND relation=to_regclass('snapshot_items') AND granted").scalars()))
    event.listen(store.engine.sync_engine, "after_cursor_execute", observe_validation)
    try:
        async with store.engine.connect() as conn:
            await conn.run_sync(apply_catalogue_migration)
            await conn.commit()
    finally:
        event.remove(store.engine.sync_engine, "after_cursor_execute", observe_validation)
    assert validation_locks
    assert all(not modes.intersection({"AccessExclusiveLock", "ShareRowExclusiveLock"}) for modes in validation_locks)
    async with store.engine.begin() as conn:
        assert await database._has_required_tables(conn)
        assert await database._has_required_columns(conn)
        assert await database._has_required_catalogue_constraints(conn)
        legacy = (await conn.execute(text("SELECT metadata,chat_version_id FROM snapshot_items WHERE external_id='legacy-item'"))).one()
        assert legacy.metadata == {"body": "retained legacy payload"} and legacy.chat_version_id is None
        await conn.execute(text("ALTER TABLE snapshot_items DROP CONSTRAINT fk_snapshot_items_chat_version"))
        assert not await database._has_required_catalogue_constraints(conn)


async def test_migration_builds_and_attaches_valid_indexes_for_partitioned_snapshot_items(store):
    async with store.engine.begin() as conn:
        await conn.execute(text("DROP TABLE snapshot_items CASCADE"))
        await conn.execute(text("CREATE TABLE snapshot_items (id uuid NOT NULL,snapshot_id uuid NOT NULL, "
            "parent_external_id text,backend_id uuid,PRIMARY KEY(id,snapshot_id)) PARTITION BY HASH(snapshot_id)"))
        for n in range(2):
            await conn.execute(text(f"CREATE TABLE snapshot_items_part_{n} PARTITION OF snapshot_items FOR VALUES WITH(MODULUS 2,REMAINDER {n})"))
    async with store.engine.connect() as conn:
        await conn.run_sync(apply_catalogue_migration)
        await conn.commit()
    async with store.engine.connect() as conn:
        assert await conn.scalar(text("SELECT indisvalid FROM pg_index WHERE indexrelid=to_regclass('ix_snapshot_items_chat_version_id')"))
        assert await conn.scalar(text("SELECT count(*) FROM pg_inherits WHERE inhparent=to_regclass('ix_snapshot_items_chat_version_id')")) == 2


async def test_publication_does_not_retain_all_heavy_message_payloads(store):
    import gc
    import tracemalloc
    scope = await store.new_scope()
    lease = await store.begin(scope)
    items = [message("x" * 65536, graph_id=f"memory-{i}") for i in range(300)]
    for start in range(0, len(items), 100):
        await store.stage(lease, items[start:start + 100])
    await store.observe(scope, lease, items)
    del items
    gc.collect()
    tracemalloc.start()
    try:
        result = await store.publish(lease)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert result.item_count == 300
    assert peak < 12 * 1024 * 1024, f"publication retained {peak / 1024 / 1024:.1f} MiB"


@pytest.mark.parametrize("graph_type", [None, "reference", ""])
async def test_materialized_binary_mime_uses_verified_captured_content_type(store, graph_type):
    scope = await store.new_scope()
    lease = await store.begin(scope)
    child = await store.binary(lease)
    child = replace(child, payload={"contentType": graph_type, "captured_content_type": "image/png"})
    items = [parent_for(child), child]
    await store.stage(lease, items)
    await store.observe(scope, lease, items)
    await store.publish(lease)
    async with store.factory() as session:
        link = await session.scalar(select(models.SnapshotItem).where(
            models.SnapshotItem.snapshot_id == scope.snapshot_id, models.SnapshotItem.item_type == "CHAT_ATTACHMENT"))
        hydrated, = await api().materialize_chat_items(session, [link])
        assert hydrated.extra_data["content_type"] == "image/png"


@pytest.mark.parametrize("owner", ["job", "snapshot", "partition"])
async def test_capture_publication_rejects_reclaimed_work_lease(store, owner):
    scope = await store.new_scope()
    partition_id = None
    if owner == "partition":
        partition_id = uuid4()
        async with store.factory.begin() as session:
            snap = await session.get(models.Snapshot, scope.snapshot_id)
            session.add(models.SnapshotPartition(id=partition_id, snapshot_id=scope.snapshot_id,
                tenant_id=scope.tenant_id, resource_id=scope.resource_id, job_id=snap.job_id,
                partition_type="CHATS", partition_index=0, status="IN_PROGRESS",
                payload={"chat_ids": [scope.chat_id]}, lease_token=1,
                lease_owner_id=uuid4(), lease_expires_at=datetime.now(timezone.utc)+timedelta(minutes=5)))
        scope = replace(scope, partition_id=partition_id)
    lease = await store.begin(scope)
    await store.stage(lease, [message()])
    await store.observe(scope, lease, [message()])
    async with store.factory.begin() as session:
        snap = await session.get(models.Snapshot, scope.snapshot_id)
        model, identifier = {"job": (models.Job, snap.job_id), "snapshot": (models.Snapshot, snap.id),
                             "partition": (models.SnapshotPartition, partition_id)}[owner]
        await session.execute(update(model).where(model.id == identifier).values(
            lease_token=model.lease_token+1, lease_owner_id=uuid4()))
    with pytest.raises(api().ChatCatalogueConflict, match="lease|fence|eligible|partition"):
        await store.publish(lease)


async def test_single_child_staging_fetches_only_its_parent_from_inventory(store):
    import json
    scope = await store.new_scope()
    lease = await store.begin(scope)
    child = CapturedChatItem("CHAT_ATTACHMENT", "card", "parent-1",
        {"contentType": "application/vnd.microsoft.card.adaptive", "content": "{}"}, None)
    parents = [message("small", graph_id=f"parent-{i}") for i in range(500)]
    parents[1] = parent_for(child, parent=parents[1])
    for start in range(0, len(parents), 100):
        await store.stage(lease, parents[start:start+100])
    selected = []
    def record(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("SELECT chat_item_versions.graph_id,"):
            selected.append((statement, parameters))
    event.listen(store.engine.sync_engine, "before_cursor_execute", record)
    try:
        assert (await store.stage(lease, [child])).staged_items == 1
    finally:
        event.remove(store.engine.sync_engine, "before_cursor_execute", record)
    assert selected
    async with store.factory() as session:
        connection = await session.connection()
        driver = (await connection.get_raw_connection()).driver_connection
        for statement, parameters in selected:
            explained = await driver.fetchval("EXPLAIN (ANALYZE, FORMAT JSON) " + statement, *parameters)
            if isinstance(explained, str):
                explained = json.loads(explained)
            assert explained[0]["Plan"]["Actual Rows"] == 1
            nodes = [explained[0]["Plan"]]
            filtered = 0
            while nodes:
                node = nodes.pop()
                filtered += node.get("Rows Removed by Filter", 0)
                nodes.extend(node.get("Plans", []))
            assert filtered < 20, f"unrelated parent records inspected: {filtered}"


@pytest.mark.parametrize("replacement_body", ["original", "changed after restart"])
async def test_expired_staged_capture_can_be_replaced_without_touching_other_chat(store, replacement_body):
    scope = await store.new_scope()
    old = await store.begin(scope)
    await store.stage(old, [message()])
    other = replace(scope, chat_id="other-chat")
    other_lease = await store.begin(other)
    await store.stage(other_lease, [message("other saved body")])
    await store.observe(other, other_lease, [message("other saved body")])
    other_result = await store.publish(other_lease)
    async with store.factory.begin() as session:
        await session.execute(update(models.ChatCaptureAttempt).where(models.ChatCaptureAttempt.id == old.attempt_id)
            .values(expires_at=datetime.now(timezone.utc)-timedelta(seconds=1)))
    fresh = await store.begin(scope)
    assert fresh.generation == old.generation+1
    await store.stage(fresh, [message(replacement_body)])
    await store.observe(scope, fresh, [message(replacement_body)])
    assert (await store.publish(fresh)).outcome == "complete"
    with pytest.raises(api().ChatCatalogueWorkLeaseMoved):
        await store.stage(old, [message("stale worker")])
    async with store.factory() as session:
        assert (await session.get(models.SnapshotChatInventory, (scope.snapshot_id, other.chat_id))).inventory_id == other_result.inventory_id
        links = list((await session.scalars(select(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == scope.snapshot_id))).all())
        assert len(links) == 2
        hydrated = await api().materialize_chat_items(session, links)
        assert {i.extra_data["raw"]["body"]["content"] for i in hydrated} == {replacement_body, "other saved body"}
