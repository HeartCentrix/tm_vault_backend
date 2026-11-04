"""Members who joined a group chat at different times: the member who sees the most history
captures it once in a backup run, the others copy their visible part of it.

Railway 2026-11-04 (11:56 run): 71 of 77 repeat chat captures were an earlier-joined member
re-capturing a chat a later member had captured first; a later member's capture lacks the
earlier history, so it could not be copied."""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from uuid import uuid4

import pytest
from sqlalchemy import select

from shared import chat_catalogue_entry, chat_capture_graph, models
from test_chat_catalogue_postgres import store

pytestmark = pytest.mark.integration

X, Y = "principal-x", "principal-y"
GROUP, Y_ONLY = "19:group@thread.v2", "19:y-only@thread.v2"
MEMBERS = [{"userId": X, "displayName": "X", "visibleHistoryStartDateTime": "0001-01-01T00:00:00Z"},
           {"userId": Y, "displayName": "Y", "visibleHistoryStartDateTime": "2026-06-01T00:00:00Z"}]


def message(mid, created):
    return {"id": mid, "createdDateTime": created, "lastModifiedDateTime": created,
            "body": {"content": mid}, "attachments": []}


class Source(chat_capture_graph.StrictChatGraphSource):
    permission_scope_hash = "a" * 64

    def __init__(self, principal, chats, gate=None):
        self.principal_id, self.chats, self.gate, self.reads = principal, chats, gate, []

    async def iter_chats(self):
        for cid in self.chats:
            yield {"id": cid, "chatType": "group", "topic": cid,
                   "members": MEMBERS if cid == GROUP else MEMBERS[1:]}

    async def iter_message_pages(self, chat_id):
        self.reads.append(chat_id)
        if chat_id == Y_ONLY and self.gate is not None:
            await self.gate.wait()
        yield [message("after-y-joined", "2026-07-01T00:00:00Z"), message("before-y-joined", "2026-01-01T00:00:00Z")]

    async def iter_hosted_contents(self, chat_id, message_id):
        if False:
            yield {}

    async def latest_message_change(self, chat_id):
        self.reads.append("latest:" + chat_id)
        from shared.chat_catalogue_entry import _graph_time
        return _graph_time("2026-07-01T00:00:00Z")


class Storage:
    def active_backend_id(self):
        return str(self.backend)

    def get_store_by_id(self, _backend):
        raise AssertionError("no binaries in these chats")


def members_storage(store):
    storage = Storage()
    storage.backend = store.backend
    return storage


@pytest.fixture
def members(store, monkeypatch):
    sources = {}

    @asynccontextmanager
    async def pinned(_client, *, principal_id):
        yield sources[principal_id]

    monkeypatch.setattr(chat_capture_graph, "pinned_chat_source", pinned)
    storage = Storage()
    storage.backend = store.backend

    async def snapshot(principal, job):
        resource, snap = uuid4(), uuid4()
        async with store.factory.begin() as session:
            session.add(models.Resource(id=resource, tenant_id=store.tenant, type=models.ResourceType.USER_CHATS,
                external_id=principal, display_name=principal,
                extra_data={"user_id": principal, "chat_inventory_format": 1}))
            await session.flush()
            session.add(models.Snapshot(id=snap, resource_id=resource, job_id=job, backend_id=store.backend,
                extra_data={"chat_inventory_format": 1}))
        return resource, snap

    async def job():
        job_id = uuid4()
        async with store.factory.begin() as session:
            session.add(models.Job(id=job_id, type=models.JobType.BACKUP, tenant_id=store.tenant,
                                   status=models.JobStatus.RUNNING))
        return job_id

    async def run(resource, snap):
        return await chat_catalogue_entry.run_chat_catalogue_resource(
            resource_id=resource, snapshot_id=snap, writer_enabled=True, graph_client=object(),
            session_factory=store.factory, storage=storage, concurrency=2, full_backup=True)

    return sources, snapshot, job, run


async def test_later_member_copies_the_earliest_members_capture_instead_of_capturing(store, members):
    sources, snapshot, job, run = members
    batch = await job()
    x_resource, x_snap = await snapshot(X, batch)
    y_resource, y_snap = await snapshot(Y, batch)
    gate = asyncio.Event()
    sources[X] = Source(X, [GROUP])
    sources[Y] = Source(Y, [GROUP, Y_ONLY], gate=gate)
    y_run = asyncio.create_task(run(y_resource, y_snap))
    for _ in range(200):                      # Y reaches its own chat; the group chat was left for X
        if Y_ONLY in sources[Y].reads:
            break
        await asyncio.sleep(0.01)
    assert sources[Y].reads == [Y_ONLY]
    x_result = await run(x_resource, x_snap)
    gate.set()
    y_result = await y_run
    assert x_result["outcome"] == y_result["outcome"] == "complete"
    assert sources[X].reads.count(GROUP) >= 1 and GROUP not in sources[Y].reads
    async with store.factory() as session:
        attempt = await session.scalar(select(models.ChatCaptureAttempt).where(
            models.ChatCaptureAttempt.snapshot_id == y_snap, models.ChatCaptureAttempt.chat_id == GROUP,
            models.ChatCaptureAttempt.state == "PUBLISHED"))
        from shared.chat_catalogue import materialize_chat_items
        rows = await materialize_chat_items(session, list((await session.scalars(select(models.SnapshotItem).where(
            models.SnapshotItem.snapshot_id == y_snap, models.SnapshotItem.item_type == "TEAMS_CHAT_MESSAGE",
            models.SnapshotItem.folder_path == "chats/" + GROUP))).all()))
        ids = {row.extra_data["raw"]["id"] for row in rows}
    assert any(e.get("derived_from_principal") == X for e in attempt.evidence_history)
    # Y keeps what it sees from when it joined (both members' messages), nothing older.
    assert ids == {"after-y-joined"}


@pytest.mark.parametrize("earliest", ["not in this run", "backup already ended"])
async def test_later_member_captures_itself_when_the_earliest_member_will_not(store, members, earliest):
    sources, snapshot, job, run = members
    batch = await job()
    if earliest == "backup already ended":
        _, x_snap = await snapshot(X, batch)
        async with store.factory.begin() as session:
            (await session.get(models.Snapshot, x_snap)).status = models.SnapshotStatus.COMPLETED
    else:
        await snapshot(X, await job())
    y_resource, y_snap = await snapshot(Y, batch)
    sources[Y] = Source(Y, [GROUP])
    result = await run(y_resource, y_snap)
    assert result["outcome"] == "complete" and sources[Y].reads.count(GROUP) >= 1


async def test_incremental_unchanged_chat_reuses_own_copy_without_waiting(store, members):
    """An incremental run checks the member's own copy first (one request): an unchanged
    chat is done at once, even while the earliest member has not reached it yet."""
    sources, snapshot, job, run = members
    first = await job()
    y_resource, y_snap = await snapshot(Y, first)
    sources[Y] = Source(Y, [GROUP])
    assert (await run(y_resource, y_snap))["outcome"] == "complete"
    second = await job()
    await snapshot(X, second)                 # X is in the run but has not started
    async with store.factory.begin() as session:
        y_snap = uuid4()
        session.add(models.Snapshot(id=y_snap, resource_id=y_resource, job_id=second, backend_id=store.backend,
                                    extra_data={"chat_inventory_format": 1}))
    sources[Y] = Source(Y, [GROUP])
    result = await chat_catalogue_entry.run_chat_catalogue_resource(
        resource_id=y_resource, snapshot_id=y_snap, writer_enabled=True, graph_client=object(),
        session_factory=store.factory, storage=members_storage(store), concurrency=2, full_backup=False)
    assert result["outcome"] == "complete" and sources[Y].reads == ["latest:" + GROUP]
