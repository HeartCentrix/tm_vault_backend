"""Direct chat writes replay only owned DB work and keep the original owner."""
import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from sqlalchemy import select, text, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.sql.dml import Insert

from test_chat_catalogue_postgres import store
from test_copy_worker_handoff_postgres import bw
from shared import models
from shared.chat_catalogue import ChatCatalogueWorkLeaseMoved

pytestmark = pytest.mark.integration


def payload(scope, key="message"):
    return {"snapshot_id": scope.snapshot_id, "external_id": key,
            "item_type": "TEAMS_CHAT_MESSAGE", "name": key, "extra_data": {"chatId": "chat-alpha"}}


@pytest.mark.parametrize("state", ["40P01", "40001"])
async def test_direct_pointer_retry_uses_fresh_transactions_and_preserves_full_descriptor(store, bw, state):
    from shared.snapshot_item_writes import capture_chat_write_scope, persist_chat_snapshot_rows
    scope = await store.new_scope()
    owner = await capture_chat_write_scope(store.factory, scope.snapshot_id)
    attempts, transactions = [], []
    row = payload(scope) | {"parent_external_id": "chat-alpha", "blob_path": "original/object",
        "encryption_key_id": "dek-7", "backup_version": 7, "is_deleted": True}

    async def insert(session, rows):
        attempts.append(session)
        transactions.append(await session.scalar(text("SELECT txid_current()")))
        result = await bw._insert_snapshot_items(session, rows)
        if len(attempts) == 1:
            await session.execute(text(f"DO $$ BEGIN RAISE EXCEPTION 'synthetic abort' USING ERRCODE='{state}'; END $$"))
        return result

    assert await persist_chat_snapshot_rows(store.factory, [row], owner, insert) == 1
    assert len(attempts) == 2 and attempts[0] is not attempts[1]
    assert len(set(transactions)) == 2
    async with store.factory() as session:
        items = list((await session.scalars(select(models.SnapshotItem).where(
            models.SnapshotItem.snapshot_id == scope.snapshot_id))).all())
        assert len(items) == 1
        item = items[0]
        assert (item.backend_id, item.tenant_id) == (scope.backend_id, scope.tenant_id)
        assert (item.blob_path, item.encryption_key_id, item.backup_version) == ("original/object", "dek-7", 7)
        assert item.parent_external_id == "chat-alpha" and item.is_deleted


async def test_replacement_generation_after_rollback_is_never_adopted(store, bw, monkeypatch):
    from shared import snapshot_item_writes
    from shared.snapshot_item_writes import capture_chat_write_scope, persist_chat_snapshot_rows
    scope = await store.new_scope()
    owner = await capture_chat_write_scope(store.factory, scope.snapshot_id)
    attempts = []

    async def insert(session, rows):
        attempts.append(session)
        await bw._insert_snapshot_items(session, rows)
        await session.execute(text("DO $$ BEGIN RAISE EXCEPTION 'retry' USING ERRCODE='40P01'; END $$"))

    async def replace_owner(_delay):
        async with store.factory.begin() as session:
            await session.execute(update(models.Snapshot).where(models.Snapshot.id == scope.snapshot_id)
                                  .values(lease_token=owner.snapshot_token + 1))

    monkeypatch.setattr(snapshot_item_writes, "asyncio", SimpleNamespace(sleep=replace_owner))
    with pytest.raises(ChatCatalogueWorkLeaseMoved):
        await persist_chat_snapshot_rows(store.factory, [payload(scope)], owner, insert)
    assert len(attempts) == 1
    async with store.factory() as session:
        assert not list((await session.scalars(select(models.SnapshotItem))).all())


async def test_connection_loss_retries_same_prepared_pointer_without_replaying_source_work(store, bw):
    from shared.snapshot_item_writes import capture_chat_write_scope, persist_chat_snapshot_rows
    scope = await store.new_scope()
    owner = await capture_chat_write_scope(store.factory, scope.snapshot_id)
    attempts, ids = [], []
    async def insert(session, rows):
        attempts.append(session)
        ids.append(rows[0]["id"])
        result = await bw._insert_snapshot_items(session, rows)
        if len(attempts) == 1:
            pid = await session.scalar(text("SELECT pg_backend_pid()"))
            driver = (await (await session.connection()).get_raw_connection()).driver_connection
            async with store.factory() as killer:
                await killer.execute(text("SELECT pg_terminate_backend(:pid)"), {"pid": pid})
            async with asyncio.timeout(2):
                while not driver.is_closed():
                    await asyncio.sleep(0)
            await session.execute(text("SELECT 1"))
        return result
    assert await persist_chat_snapshot_rows(store.factory, [payload(scope)], owner, insert) == 1
    assert len(attempts) == 2 and attempts[0] is not attempts[1] and ids[0] == ids[1]
    async with store.factory() as session:
        assert len(list((await session.scalars(select(models.SnapshotItem))).all())) == 1


@pytest.mark.parametrize("state,expected_calls", [("40P01", 3), ("40001", 3), ("25P02", 1), ("55P03", 1), ("57014", 1), ("23503", 1)])
async def test_direct_write_combined_budget_is_bounded_and_preserves_permanent_error_policy(store, state, expected_calls):
    from shared.snapshot_item_writes import capture_chat_write_scope, persist_chat_snapshot_rows
    scope = await store.new_scope()
    owner = await capture_chat_write_scope(store.factory, scope.snapshot_id)
    calls = []
    async def insert(session, _rows):
        calls.append(session)
        await session.execute(text(f"DO $$ BEGIN RAISE EXCEPTION 'policy test' USING ERRCODE='{state}'; END $$"))
    with pytest.raises(DBAPIError):
        await persist_chat_snapshot_rows(store.factory, [payload(scope)], owner, insert)
    assert len(calls) == expected_calls and len({id(s) for s in calls}) == expected_calls


@pytest.mark.parametrize("change", ["cancel", "snapshot_generation", "job_generation", "delete"])
async def test_source_owner_is_captured_before_io_and_revalidated_before_mutation(store, bw, change):
    from shared.snapshot_item_writes import capture_chat_write_scope, persist_chat_snapshot_rows
    scope = await store.new_scope()
    owner = await capture_chat_write_scope(store.factory, scope.snapshot_id)
    async with store.factory.begin() as session:
        if change == "cancel":
            job = await session.get(models.Job, owner.job_id)
            job.status = models.JobStatus.CANCELLED
        elif change == "snapshot_generation":
            snapshot = await session.get(models.Snapshot, scope.snapshot_id)
            snapshot.lease_token += 1
        elif change == "job_generation":
            job = await session.get(models.Job, owner.job_id)
            job.lease_token += 1
        else:
            await session.execute(text("DELETE FROM snapshots WHERE id=:id"), {"id": scope.snapshot_id})
    with pytest.raises(ChatCatalogueWorkLeaseMoved):
        await persist_chat_snapshot_rows(store.factory, [payload(scope)], owner, bw._insert_snapshot_items)
    async with store.factory() as session:
        assert not list((await session.scalars(select(models.SnapshotItem))).all())


async def test_original_target_is_required_and_later_chunk_error_rolls_back_all_rows(store, bw, monkeypatch):
    from shared.snapshot_item_writes import capture_chat_write_scope, persist_chat_snapshot_rows
    scope = await store.new_scope()
    other = await store.new_scope()
    owner = await capture_chat_write_scope(store.factory, scope.snapshot_id)
    with pytest.raises(ChatCatalogueWorkLeaseMoved):
        await persist_chat_snapshot_rows(store.factory, [payload(other)], owner, bw._insert_snapshot_items)
    monkeypatch.setattr(bw, "_BULK_INSERT_CHUNK", 1)
    with pytest.raises(DBAPIError):
        await persist_chat_snapshot_rows(store.factory, [payload(scope, "A"), payload(scope, "B") | {"name": None}], owner, bw._insert_snapshot_items)
    async with store.factory() as session:
        assert not list((await session.scalars(select(models.SnapshotItem))).all())


async def test_duplicate_first_input_winner_survives_chunks_and_omitted_defaults(store, bw, monkeypatch):
    from shared.snapshot_item_writes import capture_chat_write_scope, persist_chat_snapshot_rows
    scope = await store.new_scope()
    owner = await capture_chat_write_scope(store.factory, scope.snapshot_id)
    monkeypatch.setattr(bw, "_BULK_INSERT_CHUNK", 1)
    rows = [payload(scope, "z") | {"backup_version": 7}, payload(scope, "A") | {"name": "first"},
            payload(scope, "A") | {"name": "second", "backup_version": 8}]
    assert await persist_chat_snapshot_rows(store.factory, rows, owner, bw._insert_snapshot_items) == 3
    async with store.factory() as session:
        rows = list((await session.scalars(select(models.SnapshotItem).order_by(models.SnapshotItem.external_id))).all())
        assert [(r.external_id, r.name, r.backup_version) for r in rows] == [("A", "first", 1), ("z", "z", 7)]


async def test_hosted_content_db_abort_does_not_repeat_graph_download_or_blob_upload(store, bw, monkeypatch):
    from shared.snapshot_item_writes import capture_chat_write_scope, chat_write_scope
    scope = await store.new_scope()
    owner = await capture_chat_write_scope(store.factory, scope.snapshot_id)
    calls = {"graph": 0, "upload": 0, "insert": 0}
    worker = bw.BackupWorker.__new__(bw.BackupWorker)

    async def stream():
        yield b"pdf-content"

    async def graph(*_args):
        calls["graph"] += 1
        return stream(), "application/pdf", 11

    async def upload(*_args, **_kwargs):
        calls["upload"] += 1

    async def exists(*_args):
        return False

    original = bw._insert_snapshot_items
    async def insert(session, rows):
        calls["insert"] += 1
        result = await original(session, rows)
        if calls["insert"] == 1:
            await session.execute(text("DO $$ BEGIN RAISE EXCEPTION 'abort pointer' USING ERRCODE='40P01'; END $$"))
        return result

    worker.graph = SimpleNamespace(get_hosted_content=graph)
    worker._upload_bytes, worker._exists_hosted_item = upload, exists
    monkeypatch.setattr(bw, "_maybe_inline_attachment", lambda _bytes: None)
    monkeypatch.setattr(bw, "_insert_snapshot_items", insert)
    monkeypatch.setattr(bw, "async_session_factory", store.factory)
    with chat_write_scope(owner):
        assert await worker._userchats_backup_hosted_contents(snapshot_id=str(scope.snapshot_id),
            user_id="principal-alpha", chat_id="chat-alpha", message_id="message", hosted_contents=[{"id": "image"}]) == (1, 11)
    assert calls == {"graph": 1, "upload": 1, "insert": 2}
    async with store.factory() as session:
        item = await session.scalar(select(models.SnapshotItem))
        assert item.backend_id == scope.backend_id and item.tenant_id == scope.tenant_id
        assert item.parent_external_id == "message" and item.content_size == 11


@pytest.mark.parametrize("copy_kind", ["carry", "attachments"])
async def test_actual_copy_and_opposite_input_bulk_overlap_finish_in_one_ordered_execution(store, bw, monkeypatch, copy_kind):
    from shared.snapshot_item_writes import capture_chat_write_scope, chat_write_scope, persist_chat_snapshot_rows
    source, target = await store.new_scope(), await store.new_scope()
    owner = await capture_chat_write_scope(store.factory, target.snapshot_id)
    is_attachment = copy_kind == "attachments"
    kind = "CHAT_ATTACHMENT" if is_attachment else "TEAMS_CHAT_MESSAGE"
    keys = ["é::attachment", "A::attachment"] if is_attachment else ["é", "A"]
    rows = [payload(target, key) | {"item_type": kind, "parent_external_id": key.split("::")[0] if is_attachment else "chat-alpha"}
            for key in keys]
    async with store.factory.begin() as session:
        snapshot = await session.get(models.Snapshot, source.snapshot_id)
        snapshot.status = models.SnapshotStatus.COMPLETED
        for row in rows:
            session.add(models.SnapshotItem(**(row | {"snapshot_id": source.snapshot_id,
                "tenant_id": store.tenant, "backend_id": store.backend})))
        if is_attachment:
            thread = models.ChatThread(tenant_id=store.tenant, chat_id="chat-alpha")
            session.add(thread)
            await session.flush()
            for key in ["A", "é"]:
                session.add(models.ChatThreadMessage(chat_thread_id=thread.id, message_external_id=key))

    first_inserted, copy_submitted = asyncio.Event(), asyncio.Event()
    state = {"copy_pid": None, "direct_inserts": 0, "copy_executions": 0}

    async def confirm_copy_is_waiting_on_database_lock():
        await copy_submitted.wait()
        async with store.factory() as observer:
            async with asyncio.timeout(5):
                while True:
                    waiting = await observer.scalar(text("SELECT wait_event_type FROM pg_stat_activity WHERE pid=:pid"), {"pid": state["copy_pid"]})
                    if waiting == "Lock":
                        return
                    # Refresh statistics snapshots; a timer is not the proof of overlap.
                    await observer.execute(text("SELECT pg_stat_clear_snapshot()"))
                    await asyncio.sleep(0)

    class DirectSession:
        def __init__(self, real): self.real = real
        def __getattr__(self, key): return getattr(self.real, key)
        async def execute(self, statement, *args, **kwargs):
            result = await self.real.execute(statement, *args, **kwargs)
            if isinstance(statement, Insert):
                state["direct_inserts"] += 1
                if state["direct_inserts"] == 1:
                    first_inserted.set()
                    await confirm_copy_is_waiting_on_database_lock()
            return result

    class CopySession:
        def __init__(self, real): self.real = real
        def __getattr__(self, key): return getattr(self.real, key)
        async def execute(self, statement, *args, **kwargs):
            if "INSERT INTO snapshot_items" in str(statement):
                state["copy_executions"] += 1
                copy_submitted.set()
            return await self.real.execute(statement, *args, **kwargs)

    @asynccontextmanager
    async def slot(*_args, **_kwargs):
        await first_inserted.wait()
        async with store.factory() as session:
            state["copy_pid"] = await session.scalar(text("SELECT pg_backend_pid()"))
            yield CopySession(session)

    monkeypatch.setattr(bw, "carry_forward_slot_session", slot)
    monkeypatch.setattr(bw, "_BULK_INSERT_CHUNK", 1)
    async def direct_insert(session, payloads):
        return await bw._insert_snapshot_items(DirectSession(session), payloads)

    async def copy():
        with chat_write_scope(owner):
            if is_attachment:
                return await bw._copy_chat_attachment_rows_to_snapshot(tenant_id=str(store.tenant),
                    target_snapshot_id=str(target.snapshot_id), chat_id="chat-alpha")
            return await bw._carry_forward_prior_chat_snapshot_items(tenant_id=str(store.tenant),
                resource_id=str(store.resource), target_snapshot_id=str(target.snapshot_id))

    results = await asyncio.wait_for(asyncio.gather(
        persist_chat_snapshot_rows(store.factory, rows, owner, direct_insert), copy()), 12)
    assert results == [2, 0]
    assert state["direct_inserts"] == 2 and state["copy_executions"] == 1
    async with store.factory() as session:
        saved = list((await session.scalars(select(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == target.snapshot_id))).all())
        assert {row.external_id for row in saved} == set(keys)
