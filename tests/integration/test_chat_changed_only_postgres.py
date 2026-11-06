"""Changed-only chat reads: a chat the per-user delta feed reports changed is rebuilt from its prior
COMPLETE capture plus the feed's message payloads, reading Graph only for those messages (one edit
in a 1,369-message chat re-read ~28 pages). The result must equal a full re-read exactly; any doubt
reads the whole chat."""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select, update

from test_chat_catalogue_postgres import store  # noqa: F401  (fixture)
from test_chat_catalogue_entry_postgres import legacy_scope, source_adapter  # noqa: F401
from shared import models

pytestmark = pytest.mark.integration

# Created two days ago; edits land after the first backup read the chat (probe times are compared
# with real read times).
T0 = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(days=2)
HOSTED = "https://graph.microsoft.com/v1.0/chats/chat-alpha/messages/{}/hostedContents/aWQ9/$value"


def stamp(when):
    return when.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def msg(i, *, body=None, modified=None, hosted=False):
    created = T0 + timedelta(minutes=i)
    content = body or f"message {i}"
    if hosted:
        content += f' <img src="{HOSTED.format(f"m{i}")}">'
    return {"id": f"m{i}", "chatId": "chat-alpha", "messageType": "message", "replyToId": None,
            "createdDateTime": stamp(created), "lastModifiedDateTime": stamp(modified or created),
            "etag": stamp(modified or created), "deletedDateTime": None,
            "body": {"contentType": "html", "content": content}, "attachments": []}


class Feed:
    """The per-user delta feed: each round hands out the records queued for it."""

    def __init__(self, source):
        self.rounds, self.queue = [], []
        source.message_delta = self.delta

    async def delta(self, link, *, since=None):
        self.rounds.append(link)
        records = self.queue.pop(0) if link else []
        changed = {}
        for record in records:
            changed.setdefault(record["chatId"], []).append(deepcopy(record))
        return changed, f"link-{len(self.rounds)}"


async def settle(store, snapshot_id):
    async with store.factory.begin() as session:
        await session.execute(update(models.Snapshot).where(models.Snapshot.id == snapshot_id)
                              .values(status=models.SnapshotStatus.COMPLETED))


async def inventory(store, snapshot_id, chat_id="chat-alpha"):
    async with store.factory() as session:
        return sorted((await session.execute(
            select(models.ChatInventoryItem.logical_key, models.ChatInventoryItem.version_id)
            .join(models.SnapshotChatInventory, models.SnapshotChatInventory.inventory_id == models.ChatInventoryItem.inventory_id)
            .where(models.SnapshotChatInventory.snapshot_id == snapshot_id,
                   models.SnapshotChatInventory.chat_id == chat_id))).all())


@pytest.fixture
def big_chat(source_adapter):
    source = source_adapter.source
    source.messages["chat-alpha"] = [msg(i, hosted=i in (3, 40)) for i in range(120)]
    feed = Feed(source)
    latest = {"at": T0 + timedelta(minutes=119)}

    async def latest_message_change(chat_id):
        return latest["at"]
    source.latest_message_change = latest_message_change
    return source, feed, latest


async def full_reference(store, source_adapter):
    """A requested full backup re-reads the chat: the inventory a changed-only capture must equal."""
    scope = await store.new_scope()
    assert (await source_adapter.run(scope, full_backup=True))["outcome"] == "complete"
    return await inventory(store, scope.snapshot_id)


async def test_edit_new_and_deleted_message_read_only_from_the_feed(store, source_adapter, big_chat, caplog):
    source, feed, latest = big_chat
    first = await legacy_scope(store)
    assert (await source_adapter.run(first))["outcome"] == "complete"
    await settle(store, first.snapshot_id)

    now = datetime.now(timezone.utc).replace(microsecond=0) + timedelta(minutes=1)
    edited = msg(40, body="message 40 edited", modified=now, hosted=True)
    added = msg(500, modified=now + timedelta(seconds=1))
    deleted = {**msg(7, modified=now + timedelta(seconds=2)), "deletedDateTime": stamp(now + timedelta(seconds=2)),
               "body": {"contentType": "text", "content": ""}}
    messages = source.messages["chat-alpha"]
    messages[40], messages[7] = edited, deleted
    messages.insert(0, added)
    feed.queue.append([edited, added, deleted])
    latest["at"] = now + timedelta(seconds=2)
    reads, hosted = len(source.message_calls), len(source.hosted_calls)

    caplog.set_level("INFO")
    second = await store.new_scope()
    result = await source_adapter.run(second)
    assert result["outcome"] == "complete", result["failure_reason"]
    assert source.message_calls[reads:] == []                       # no page of the chat was read
    assert source.hosted_calls[hosted:] == [("chat-alpha", "m40")]  # only the edited message's images
    assert "chat_changed_only_capture chat=chat-alpha changed=2 deleted=1 reused=" in caplog.text
    await settle(store, second.snapshot_id)
    assert await inventory(store, second.snapshot_id) == await full_reference(store, source_adapter)


async def test_change_newer_than_the_feed_is_read_from_the_gap_not_the_whole_chat(store, source_adapter, big_chat, caplog):
    """The one-request check shows a change newer than the feed's newest record (the feed missed an
    edit, Railway 2026-11-05; or the user kept chatting during the run, Railway 2026-11-06: a ~4,000
    message 1:1 was re-read page by page for 3+ minutes). Only messages modified after the feed's
    newest record are listed, newest first, and the capture stays changed-only."""
    source, feed, latest = big_chat
    first = await legacy_scope(store)
    assert (await source_adapter.run(first))["outcome"] == "complete"
    await settle(store, first.snapshot_id)

    now = datetime.now(timezone.utc).replace(microsecond=0) + timedelta(minutes=1)
    edited = msg(5, body="edited", modified=now)
    missed = msg(9, body="edited too", modified=now + timedelta(hours=1))
    source.messages["chat-alpha"][5], source.messages["chat-alpha"][9] = edited, missed
    feed.queue.append([edited])
    latest["at"] = now + timedelta(hours=1)
    reads = len(source.message_calls)

    caplog.set_level("INFO")
    second = await store.new_scope()
    assert (await source_adapter.run(second))["outcome"] == "complete"
    assert source.message_calls[reads:] == []                  # no page of the whole chat
    assert source.topup_calls == ("chat-alpha",)
    assert "chat_changed_only_capture chat=chat-alpha changed=2 deleted=0 reused=" in caplog.text
    await settle(store, second.snapshot_id)
    assert await inventory(store, second.snapshot_id) == await full_reference(store, source_adapter)


@pytest.mark.parametrize("case", ["probe_newer", "system_event", "removed", "resumed", "kill_switch", "reconcile"])
async def test_any_doubt_reads_the_whole_chat(store, source_adapter, big_chat, caplog, monkeypatch, case):
    source, feed, latest = big_chat
    first = await legacy_scope(store)
    await source_adapter.run(first)
    await settle(store, first.snapshot_id)

    now = datetime.now(timezone.utc).replace(microsecond=0) + timedelta(minutes=1)
    edited = msg(5, body="edited", modified=now)
    source.messages["chat-alpha"][5] = edited
    records, latest["at"] = [edited], now
    if case == "probe_newer":      # the feed missed a later edit and the gap is too big to list
        missed = msg(9, body="edited too", modified=now + timedelta(hours=1))
        source.messages["chat-alpha"][9] = missed
        latest["at"] = now + timedelta(hours=1)
        monkeypatch.setenv("CHAT_CHANGED_ONLY_GAP_PAGES", "0")
    elif case == "system_event":
        records.append({**msg(600, modified=now), "messageType": "systemEventMessage"})
    elif case == "removed":
        records.append({"id": "m11", "chatId": "chat-alpha", "@removed": {"reason": "deleted"}})
    elif case == "kill_switch":
        monkeypatch.setenv("CHAT_CHANGED_ONLY_READ", "false")
    elif case == "reconcile":
        monkeypatch.setenv("CHAT_INCREMENTAL_RECONCILE_HOURS", "0")
    feed.queue.append(records)
    second = await store.new_scope()
    if case == "resumed":          # a resumed turn kept the changed chat ids, not the payloads
        feed.queue.clear()
        async with store.factory.begin() as session:
            resource = await session.get(models.Resource, second.resource_id)
            resource.extra_data = {**resource.extra_data, "chat_delta_pending": {
                "snapshot_id": str(second.snapshot_id), "link": "link-9", "since_snapshot": str(first.snapshot_id),
                "changed": ["chat-alpha"], "principal": second.principal_id, "scope": source.permission_scope_hash}}
    reads = len(source.message_calls)
    caplog.set_level("INFO")
    assert (await source_adapter.run(second))["outcome"] == "complete"
    assert "chat-alpha" in source.message_calls[reads:]
    if case != "kill_switch":
        assert "chat_changed_only_fallback chat=chat-alpha reason=" in caplog.text
    await settle(store, second.snapshot_id)
    assert await inventory(store, second.snapshot_id) == await full_reference(store, source_adapter)
