"""Chat overlay snapshots: an incremental where some chats changed owns only the changed chats'
rows; every unchanged chat is inherited from the full base snapshot through
snapshot_items_effective() instead of being copied (TM scale: most users have one changed chat
a day, and the copy wrote every row of every unchanged chat every day)."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy import select, text, update

from test_chat_catalogue_postgres import store  # noqa: F401  (fixture)
from test_chat_catalogue_entry_postgres import api, delta_source, legacy_scope, source_adapter  # noqa: F401
from shared import models

pytestmark = pytest.mark.integration

OLD = datetime(2020, 1, 1, tzinfo=timezone.utc)


async def settle(store, snapshot_id):
    async with store.factory.begin() as session:
        await session.execute(update(models.Snapshot).where(models.Snapshot.id == snapshot_id)
                              .values(status=models.SnapshotStatus.COMPLETED))


async def own_rows(store, snapshot_id):
    async with store.factory() as session:
        return sorted(tuple(r) for r in (await session.execute(text(
            "SELECT metadata->>'chat_id', external_id, chat_version_id FROM snapshot_items WHERE snapshot_id = :s"),
            {"s": snapshot_id})).all())


async def effective(store, snapshot_id):
    async with store.factory() as session:
        return sorted((await session.execute(text(
            "SELECT external_id, chat_version_id FROM snapshot_items_effective(:s)"), {"s": snapshot_id})).all())


async def expected_rows(store, snapshot_id):
    """What a full copy holds: every published chat inventory's (key, version), one row each."""
    async with store.factory() as session:
        return sorted((await session.execute(select(models.ChatInventoryItem.logical_key, models.ChatInventoryItem.version_id)
            .join(models.SnapshotChatInventory, models.SnapshotChatInventory.inventory_id == models.ChatInventoryItem.inventory_id)
            .where(models.SnapshotChatInventory.snapshot_id == snapshot_id))).all())


def chats(source, *names):
    for name in names:
        source.chats.append({"id": name, "topic": name})
        source.messages[name] = [{"id": f"{name}-m{i}", "body": {"content": f"{name} {i}"}, "attachments": []}
                                 for i in range(3)]


def changed_only(source, *names):
    async def latest(chat_id):
        return datetime.now(timezone.utc) if chat_id in names else OLD
    source.latest_message_change = latest


@pytest.fixture(autouse=True)
def small_tenant(monkeypatch):
    # Three-message chats: one changed chat is far above the 25% compaction ratio meant for real users.
    monkeypatch.setenv("CHAT_OVERLAY_MAX_RATIO", "1")
    monkeypatch.setenv("CHAT_OVERLAY_ENABLED", "true")


async def test_incremental_with_one_changed_chat_owns_only_that_chats_rows(store, source_adapter):
    source = source_adapter.source
    chats(source, "chat-beta", "chat-gamma", "chat-delta")
    feed, rounds, checks = [], [], []
    delta_source(source, feed, rounds, checks, latest_at=OLD)
    first = await legacy_scope(store)
    assert (await source_adapter.run(first))["outcome"] == "complete"
    await settle(store, first.snapshot_id)

    # beta edited; delta left the user's chats: unchanged alpha + gamma are inherited, not copied.
    source.messages["chat-beta"][1]["body"] = {"content": "edited"}
    source.chats = [c for c in source.chats if c["id"] != "chat-delta"]
    changed_only(source, "chat-beta")
    feed.append({"chat-beta"})
    second = await store.new_scope()
    assert (await source_adapter.run(second))["outcome"] == "complete"
    async with store.factory() as session:
        snapshot = await session.get(models.Snapshot, second.snapshot_id)
    assert snapshot.overlay_base_id == first.snapshot_id
    own = await own_rows(store, second.snapshot_id)
    assert {chat for chat, *_ in own} == {"chat-beta"} and len(own) == 3
    assert await effective(store, second.snapshot_id) == await expected_rows(store, second.snapshot_id)
    async with store.factory() as session:
        counted = await api().catalogue_snapshot_result(session, second.snapshot_id)
    assert counted["item_count"] == 7 and counted["new_item_count"] == 1
    await settle(store, second.snapshot_id)

    # Next day gamma changes: still one level over the first full base; beta (changed since the base)
    # is carried whole, so a chat's messages and children always live in one snapshot.
    source.messages["chat-gamma"].append({"id": "chat-gamma-new", "body": {"content": "new"}, "attachments": []})
    changed_only(source, "chat-gamma")
    feed.append({"chat-gamma"})
    third = await store.new_scope()
    assert (await source_adapter.run(third))["outcome"] == "complete"
    async with store.factory() as session:
        assert (await session.get(models.Snapshot, third.snapshot_id)).overlay_base_id == first.snapshot_id
    own = await own_rows(store, third.snapshot_id)
    assert {chat for chat, *_ in own} == {"chat-beta", "chat-gamma"} and len(own) == 3 + 4
    assert await effective(store, third.snapshot_id) == await expected_rows(store, third.snapshot_id)
    # The earlier overlay is untouched.
    assert await effective(store, second.snapshot_id) == await expected_rows(store, second.snapshot_id)


@pytest.mark.parametrize("env", [{"CHAT_OVERLAY_ENABLED": "false"}, {"CHAT_OVERLAY_ENABLED": ""}, {"CHAT_OVERLAY_MAX_RATIO": "0.25"},
                                 {"CHAT_OVERLAY_MAX_AGE_DAYS": "0"}])
async def test_kill_switch_and_compaction_copy_every_unchanged_chat_as_before(store, source_adapter, monkeypatch, env):
    source = source_adapter.source
    chats(source, "chat-beta")
    feed, rounds, checks = [], [], []
    delta_source(source, feed, rounds, checks, latest_at=OLD)
    first = await legacy_scope(store)
    await source_adapter.run(first)
    await settle(store, first.snapshot_id)
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    changed_only(source, "chat-beta")
    feed.append({"chat-beta"})
    second = await store.new_scope()
    assert (await source_adapter.run(second))["outcome"] == "complete"
    async with store.factory() as session:
        assert (await session.get(models.Snapshot, second.snapshot_id)).overlay_base_id is None
    assert sorted((k, v) for _, k, v in await own_rows(store, second.snapshot_id)) == await expected_rows(store, second.snapshot_id)


async def test_per_chat_link_copies_from_an_overlay_source(store, source_adapter, caplog):
    """Without the feed, each unchanged chat links its prior capture one by one; when that capture's
    backup is an overlay, its rows are read through snapshot_items_effective (not re-staged)."""
    from shared.chat_capture_graph import ChatGraphError
    source = source_adapter.source
    chats(source, "chat-beta")
    feed, rounds, checks = [], [], []
    delta_source(source, feed, rounds, checks, latest_at=OLD)
    first = await legacy_scope(store)
    await source_adapter.run(first)
    await settle(store, first.snapshot_id)
    changed_only(source, "chat-beta")
    feed.append({"chat-beta"})
    second = await store.new_scope()
    await source_adapter.run(second)
    await settle(store, second.snapshot_id)
    async with store.factory() as session:
        assert (await session.get(models.Snapshot, second.snapshot_id)).overlay_base_id == first.snapshot_id

    changed_only(source)
    feed.append(ChatGraphError("gone", status_code=410))
    third = await store.new_scope()
    caplog.set_level("INFO")
    assert (await source_adapter.run(third))["outcome"] == "complete"
    assert "chat_incremental_linked" in caplog.text and "chat_incremental_reused" not in caplog.text
    assert await effective(store, third.snapshot_id) == await expected_rows(store, third.snapshot_id)


# ---- Overlay vs copy twin: every catalogue reader must answer the same for both. ----

CARD = {"id": "card-1", "contentType": "application/vnd.microsoft.card.adaptive", "content": "{}"}


async def overlay_and_twin(store, source_adapter, monkeypatch):
    """(base, overlay, twin): ``overlay`` inherits chat-alpha + chat-gamma from ``base`` and owns
    chat-beta; ``twin`` is the same backup written the copy way (every row its own). All COMPLETED."""
    monkeypatch.setenv("CHAT_OVERLAY_ENABLED", "true")
    monkeypatch.setenv("CHAT_OVERLAY_MAX_RATIO", "1")
    source = source_adapter.source
    chats(source, "chat-beta", "chat-gamma")
    source.messages["chat-alpha"][0]["attachments"] = [CARD]
    source.messages["chat-alpha"][0]["body"] = {"content": 'see <attachment id="card-1"></attachment>'}
    feed, rounds, checks = [], [], []
    delta_source(source, feed, rounds, checks, latest_at=OLD)
    base = await legacy_scope(store)
    assert (await source_adapter.run(base))["outcome"] == "complete"
    await settle(store, base.snapshot_id)
    source.messages["chat-beta"][1]["body"] = {"content": "edited"}
    changed_only(source, "chat-beta")
    feed.append({"chat-beta"})
    overlay = await store.new_scope()
    assert (await source_adapter.run(overlay))["outcome"] == "complete"
    await settle(store, overlay.snapshot_id)
    twin = await materialized_twin(store, overlay.snapshot_id)
    async with store.factory() as session:
        assert (await session.get(models.Snapshot, overlay.snapshot_id)).overlay_base_id == base.snapshot_id
    return base.snapshot_id, overlay.snapshot_id, twin


async def materialized_twin(store, overlay_id):
    """A full snapshot holding exactly the overlay's effective rows and chat associations."""
    from uuid import uuid4
    twin, job = uuid4(), uuid4()
    async with store.factory.begin() as session:
        source = await session.get(models.Snapshot, overlay_id)
        session.add(models.Job(id=job, type=models.JobType.BACKUP, tenant_id=store.tenant,
                               resource_id=source.resource_id, status=models.JobStatus.COMPLETED))
        await session.flush()
        extra = {k: v for k, v in (source.extra_data or {}).items() if not k.startswith("chat_overlay_")}
        session.add(models.Snapshot(id=twin, resource_id=source.resource_id, job_id=job, backend_id=source.backend_id,
                                    status=source.status, type=source.type, extra_data=extra,
                                    created_at=source.created_at, started_at=source.started_at))
        await session.flush()
        await session.execute(text(
            "INSERT INTO snapshot_chat_inventories (snapshot_id, chat_id, tenant_id, inventory_id, capture_attempt_id, "
            "coverage_status, started_at, finished_at, failure_reason, uploaded_bytes) "
            "SELECT :t, chat_id, tenant_id, inventory_id, capture_attempt_id, coverage_status, started_at, finished_at, "
            "failure_reason, uploaded_bytes FROM snapshot_chat_inventories WHERE snapshot_id = :o"), {"t": twin, "o": overlay_id})
        await session.execute(text(
            "INSERT INTO snapshot_items (id, snapshot_id, tenant_id, external_id, parent_external_id, item_type, name, "
            "folder_path, content_hash, content_checksum, content_size, blob_path, encryption_key_id, backup_version, "
            "metadata, is_deleted, backend_id, chat_version_id, created_at) "
            "SELECT gen_random_uuid(), :t, tenant_id, external_id, parent_external_id, item_type, name, folder_path, "
            "content_hash, content_checksum, content_size, blob_path, encryption_key_id, backup_version, metadata, "
            "is_deleted, backend_id, chat_version_id, created_at FROM snapshot_items_effective(:o)"), {"t": twin, "o": overlay_id})
    return twin


async def test_chat_rows_label_inherited_rows_with_the_logical_snapshot(store, source_adapter, monkeypatch):
    from shared.snapshot_reuse import chat_rows
    base, overlay, twin = await overlay_and_twin(store, source_adapter, monkeypatch)
    async with store.factory() as session:
        rows, scope = await chat_rows(session, [overlay])
        got = sorted((r.snapshot_id, r.external_id, r.chat_version_id) for r in (await session.scalars(
            select(rows).where(scope))).all())
        plain, plain_scope = await chat_rows(session, [twin])
        assert plain is models.SnapshotItem   # a full snapshot keeps today's plain query
        want = sorted((overlay, r.external_id, r.chat_version_id) for r in (await session.scalars(
            select(plain).where(plain_scope))).all())
    assert got == want and len(got) == 8   # alpha 1 + card, beta 3, gamma 3
