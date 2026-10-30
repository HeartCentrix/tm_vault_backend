"""Bounded resource scheduling with real catalogue transactions and gated source I/O."""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from sqlalchemy import select

from shared import chat_catalogue_entry, chat_capture_graph, models
from shared.chat_catalogue import ChatCatalogueIntegrityError, ChatCatalogueWorkLeaseMoved
from test_chat_catalogue_postgres import store

pytestmark = pytest.mark.integration


@pytest.fixture
def scheduled_source(store, monkeypatch):
    class Source(chat_capture_graph.StrictChatGraphSource):
        principal_id = "principal-alpha"
        permission_scope_hash = "a" * 64

        def __init__(self):
            self.chat_ids = ["chat-A", "chat-B", "chat-C", "chat-D", "chat-E"]
            self.started = {cid: asyncio.Event() for cid in self.chat_ids}
            self.release = {cid: asyncio.Event() for cid in self.chat_ids}
            self.cancelled = {cid: asyncio.Event() for cid in self.chat_ids}
            self.cleanup_release = asyncio.Event()
            self.cleanup_release.set()
            self.failures, self.capture_tasks = {}, {}
            self.peak, self.list_calls = 0, 0
            self.closed, self.drained_on_close = False, False

        async def iter_chats(self):
            self.list_calls += 1
            for cid in self.chat_ids:
                yield {"id": cid, "topic": cid}

        async def iter_message_pages(self, chat_id):
            self.capture_tasks[chat_id] = asyncio.current_task()
            self.peak = max(self.peak, len({task for task in self.capture_tasks.values() if not task.done()}))
            self.started[chat_id].set()
            try:
                await self.release[chat_id].wait()
            except asyncio.CancelledError:
                self.cancelled[chat_id].set()
                await self.cleanup_release.wait()
                raise
            if chat_id in self.failures:
                raise self.failures[chat_id]
            yield [{"id": "message-" + chat_id, "body": {"content": chat_id}, "attachments": []}]

        async def iter_hosted_contents(self, chat_id, message_id):
            if False:
                yield {}

    source = Source()

    @asynccontextmanager
    async def pinned(_client, *, principal_id):
        assert principal_id == source.principal_id
        try:
            yield source
        finally:
            source.drained_on_close = all(task.done() for task in source.capture_tasks.values())
            source.closed = True

    monkeypatch.setattr(chat_capture_graph, "pinned_chat_source", pinned)

    class Storage:
        def active_backend_id(self):
            return str(store.backend)

        def get_store_by_id(self, _backend):
            raise AssertionError("unexpected binary storage access")

    async def run(scope):
        return await chat_catalogue_entry.run_chat_catalogue_resource(
            resource_id=scope.resource_id, snapshot_id=scope.snapshot_id, writer_enabled=True,
            graph_client=object(), session_factory=store.factory, storage=Storage(), concurrency=2)

    return SimpleNamespace(source=source, run=run)


async def test_freed_slot_starts_next_chat_without_waiting_for_slow_peer(store, scheduled_source):
    scope = await store.new_scope()
    source = scheduled_source.source
    source.release["chat-B"].set()
    runner = asyncio.create_task(scheduled_source.run(scope))
    try:
        await asyncio.wait_for(source.started["chat-A"].wait(), timeout=5)
        await asyncio.wait_for(source.started["chat-C"].wait(), timeout=5)
        assert not source.release["chat-A"].is_set()
        assert not source.started["chat-D"].is_set()
        assert not source.started["chat-E"].is_set()
        assert source.peak == 2
        async with store.factory() as session:
            assert list((await session.scalars(select(models.SnapshotChatInventory.chat_id).where(
                models.SnapshotChatInventory.snapshot_id == scope.snapshot_id))).all()) == ["chat-B"]
    finally:
        for release in source.release.values():
            release.set()
        result = await runner
    assert result["outcome"] == "complete" and result["item_count"] == 5
    assert source.peak == 2 and source.list_calls == 2
    assert source.closed and source.drained_on_close
    async with store.factory() as session:
        snapshot = await session.get(models.Snapshot, scope.snapshot_id)
        record = snapshot.extra_data["chat_catalogue_scopes"]["resource"]
        assert record["complete"] and record["planned_chats"] == record["completed_chats"] == 5


@pytest.mark.parametrize("interruption", ["ownership", "capture_cancellation", "resource_cancellation"])
async def test_interruption_drains_pending_captures_before_closing_source(store, scheduled_source, interruption):
    scope = await store.new_scope()
    source = scheduled_source.source
    source.chat_ids = ["chat-A", "chat-B", "chat-C"]
    source.cleanup_release.clear()
    error = ChatCatalogueWorkLeaseMoved if interruption == "ownership" else asyncio.CancelledError
    if interruption != "resource_cancellation":
        source.failures["chat-A"] = error("capture interrupted")
    runner = asyncio.create_task(scheduled_source.run(scope))
    try:
        await asyncio.wait_for(source.started["chat-A"].wait(), timeout=5)
        await asyncio.wait_for(source.started["chat-B"].wait(), timeout=5)
        if interruption == "resource_cancellation":
            runner.cancel()
        else:
            source.release["chat-A"].set()
        await asyncio.wait_for(source.cancelled["chat-B"].wait(), timeout=5)
        assert not source.closed and not runner.done()
        assert not source.started["chat-C"].is_set()
        source.cleanup_release.set()
        with pytest.raises(error):
            await runner
    finally:
        source.cleanup_release.set()
        for release in source.release.values():
            release.set()
        await asyncio.gather(runner, return_exceptions=True)
    assert source.closed and source.drained_on_close
    async with store.factory() as session:
        snapshot = await session.get(models.Snapshot, scope.snapshot_id)
        assert "chat_catalogue_scopes" not in snapshot.extra_data


@pytest.mark.parametrize("failed_chats, outcome, item_count", [
    (["chat-A"], "partial", 2),
    (["chat-A", "chat-B", "chat-C"], "failed", 0),
])
async def test_individual_errors_leave_remaining_chats_and_honest_outcome(
        store, scheduled_source, failed_chats, outcome, item_count):
    scope = await store.new_scope()
    source = scheduled_source.source
    source.chat_ids = ["chat-A", "chat-B", "chat-C"]
    source.failures = {cid: ChatCatalogueIntegrityError("invalid source content") for cid in failed_chats}
    for release in source.release.values():
        release.set()
    result = await scheduled_source.run(scope)
    assert result["outcome"] == outcome and result["item_count"] == item_count
    assert result["failed"] == (outcome == "failed")
    assert result["failure_reason"] == "chat capture did not complete (ChatCatalogueIntegrityError)"
    assert all(source.started[cid].is_set() for cid in source.chat_ids)
    assert source.peak <= 2 and source.list_calls == 2
    assert source.closed and source.drained_on_close
    async with store.factory() as session:
        snapshot = await session.get(models.Snapshot, scope.snapshot_id)
        record = snapshot.extra_data["chat_catalogue_scopes"]["resource"]
        assert not record["complete"] and record["planned_chats"] == 3
        assert record["completed_chats"] == item_count
