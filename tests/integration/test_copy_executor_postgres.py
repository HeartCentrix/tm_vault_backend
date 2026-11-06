"""Copy manifests and checkpoints against an explicitly disposable PostgreSQL DB."""
import importlib
import json
import os
from types import SimpleNamespace
import uuid

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def store():
    url = os.environ.get("TMVAULT_TEST_DATABASE_URL")
    if not url:
        pytest.skip("requires an explicitly configured disposable PostgreSQL database")
    schema = "copy_executor_test_" + uuid.uuid4().hex
    engine = create_async_engine(url, connect_args={
        "server_settings": {"search_path": schema}, "statement_cache_size": 0,
    })
    async with engine.begin() as conn:
        await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        for ddl in (
            "CREATE TABLE resources (id uuid PRIMARY KEY, tenant_id uuid NOT NULL)",
            "CREATE TABLE snapshots (id uuid PRIMARY KEY, resource_id uuid NOT NULL, job_id uuid, "
            "status text, created_at timestamp, extra_data json, reuse_of_snapshot_id uuid, reuse_chain_root_id uuid)",
            "CREATE TABLE snapshot_partitions (snapshot_id uuid, partition_type text, payload json)",
            "CREATE TABLE chat_threads (id uuid PRIMARY KEY, tenant_id uuid, chat_id text)",
            "CREATE TABLE chat_thread_messages (chat_thread_id uuid, message_external_id text)",
            "CREATE TABLE snapshot_items (id uuid PRIMARY KEY, snapshot_id uuid NOT NULL REFERENCES snapshots(id), "
            "tenant_id uuid, item_type text NOT NULL, external_id text NOT NULL, parent_external_id text, "
            "name text NOT NULL, folder_path text, content_hash text, content_checksum text, content_size bigint, "
            "blob_path text, encryption_key_id text, backup_version integer, metadata json, is_deleted boolean, "
            "indexed_at timestamp, backend_id uuid, chat_version_id uuid, created_at timestamp, "
            "UNIQUE(snapshot_id,external_id,item_type))",
            "CREATE TABLE copy_workflows (id uuid PRIMARY KEY, tenant_id uuid, job_id uuid, resource_id uuid, "
            "target_snapshot_id uuid REFERENCES snapshots(id), state text DEFAULT 'DRAINING', capture_attempt bigint DEFAULT 0, "
            "parent_settle_pending boolean DEFAULT false)",
            "CREATE TABLE copy_work_tasks (id uuid PRIMARY KEY, workflow_id uuid REFERENCES copy_workflows(id), "
            "kind text, params json, prepared boolean DEFAULT false, state text DEFAULT 'READY', capture_attempt bigint DEFAULT 0)",
            "CREATE TABLE copy_work_items (task_id uuid REFERENCES copy_work_tasks(id), ordinal bigint, "
            "payload json NOT NULL, source_snapshot_id uuid REFERENCES snapshots(id) ON DELETE RESTRICT, "
            "phase integer DEFAULT 0, PRIMARY KEY(task_id,ordinal))",
        ):
            await conn.execute(text(ddl))
    factory = async_sessionmaker(engine, expire_on_commit=False)
    tenant, resource, source, target, job, workflow, backend, version = [uuid.uuid4() for _ in range(8)]
    async with factory() as s:
        await s.execute(text("INSERT INTO resources VALUES (:r,:t)"), {"r": resource, "t": tenant})
        await s.execute(text("INSERT INTO snapshots VALUES (:s,:r,:j,'COMPLETED','2026-01-01','{}',NULL,NULL)"),
                        {"s": source, "r": resource, "j": uuid.uuid4()})
        await s.execute(text("INSERT INTO snapshots VALUES (:s,:r,:j,'IN_PROGRESS','2026-01-02','{}',NULL,NULL)"),
                        {"s": target, "r": resource, "j": job})
        await s.execute(text("INSERT INTO copy_workflows(id,tenant_id,job_id,resource_id,target_snapshot_id) VALUES (:w,:t,:j,:r,:s)"),
                        {"w": workflow, "t": tenant, "j": job, "r": resource, "s": target})
        await s.commit()

    async def add(external, *, kind="TEAMS_CHAT_MESSAGE", snapshot=source, parent=None,
                  metadata=None, owner=tenant, name="original", folder="/Inbox", size=7):
        async with factory() as s:
            await s.execute(text("""
                INSERT INTO snapshot_items(id,snapshot_id,tenant_id,item_type,external_id,parent_external_id,
                    name,folder_path,content_hash,content_checksum,content_size,blob_path,encryption_key_id,
                    backup_version,metadata,is_deleted,indexed_at,backend_id,chat_version_id,created_at)
                VALUES (:i,:s,:t,:k,:e,:p,:n,:f,'hash','checksum',:z,'objects/original','dek-2',4,
                    CAST(:m AS json),false,'2026-01-01',:b,:v,'2026-01-01')
            """), {"i": uuid.uuid4(), "s": snapshot, "t": owner, "k": kind, "e": external,
                   "p": parent, "n": name, "f": folder, "z": size, "m": json.dumps(metadata or {}),
                   "b": backend, "v": version})
            await s.commit()

    async def task(kind, **params):
        value = SimpleNamespace(id=uuid.uuid4(), workflow_id=workflow, kind=kind,
                                params=params, prepared=False, checkpoint=0)
        async with factory() as s:
            await s.execute(text("INSERT INTO copy_work_tasks(id,workflow_id,kind,params) "
                                 "VALUES (:i,:w,:k,CAST(:p AS json))"),
                            {"i": value.id, "w": workflow, "k": kind, "p": json.dumps(params)})
            await s.commit()
        return value

    async def rows():
        async with factory() as s:
            return list((await s.execute(text("SELECT * FROM snapshot_items WHERE snapshot_id=:s "
                                              "ORDER BY item_type,external_id"), {"s": target})).mappings())

    yield SimpleNamespace(factory=factory, add=add, task=task, rows=rows, tenant=tenant,
                          resource=resource, source=source, target=target, backend=backend, version=version)
    async with engine.begin() as conn:
        await conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    await engine.dispose()


def executor():
    try:
        return importlib.import_module("shared.copy_executor")
    except ModuleNotFoundError:
        pytest.fail("durable SQL manifest executor is not implemented")


async def prepare(store, task):
    async with store.factory() as s:
        await executor().prepare_manifest(s, task)
        await s.commit()


async def batch(store, task, limit=1):
    async with store.factory() as s:
        result = await executor().copy_manifest_batch(s, task, limit=limit)
        await s.commit()
    task.checkpoint = result["cursor"]
    return result


async def test_manifest_freezes_payload_and_preserves_fresh_target(store):
    await store.add("a")
    await store.add("b")
    await store.add("a", snapshot=store.target, name="fresh")
    task = await store.task("CHAT_CARRY")
    await prepare(store, task)
    async with store.factory() as s:
        await s.execute(text("UPDATE snapshot_items SET name='mutated', metadata=CAST(:metadata AS json) "
                             "WHERE snapshot_id=:s"), {"s": store.source, "metadata": '{"changed":true}'})
        await s.commit()
    while not (await batch(store, task))["done"]:
        pass
    rows = await store.rows()
    assert [(r["external_id"], r["name"]) for r in rows] == [("a", "fresh"), ("b", "original")]
    assert rows[1]["metadata"] == {}
    assert rows[1]["encryption_key_id"] == "dek-2"
    assert rows[1]["backend_id"] == store.backend
    assert rows[1]["chat_version_id"] == store.version
    assert rows[1]["backup_version"] == 4
    assert task.params["prior_snapshot_id"] == str(store.source)


async def test_batch_rollback_replays_same_cursor_without_losing_rows(store):
    await store.add("a")
    task = await store.task("CHAT_CARRY")
    await prepare(store, task)
    async with store.factory() as s:
        result = await executor().copy_manifest_batch(s, task, limit=1)
        assert result["rows"] == 1
        await s.rollback()
    assert await store.rows() == []
    assert task.checkpoint == 0
    assert await batch(store, task) == {"done": True, "cursor": 1, "rows": 1, "bytes": 7}


async def test_mail_filters_are_frozen_and_all_parents_precede_attachments(store):
    for ext, folder, metadata in (
        ("kept", "/Inbox", {"raw": {"parentFolderId": "inbox"}}),
        ("tomb", "/Inbox", {"raw": {"parentFolderId": "inbox"}}),
        ("bootstrap", "/Fresh", {"raw": {"parentFolderId": "fresh"}}),
        ("archive", "/Archive", {"source_mailbox": "archive", "raw": {"parentFolderId": "inbox"}}),
        ("gone", "/Gone", {"raw": {"parentFolderId": "gone"}}),
    ):
        await store.add(ext, kind="EMAIL", folder=folder, metadata=metadata)
        await store.add(ext + "::att", kind="EMAIL_ATTACHMENT", parent=ext)
    task = await store.task("MAIL_CARRY", tombstoned_ids=["tomb"], excluded_paths=["/Fresh"],
                            keep_folder_ids=["inbox", "fresh"], exclude_pathless=True)
    await prepare(store, task)
    first = await batch(store, task, 20)
    assert first == {"done": False, "cursor": 1, "rows": 1, "bytes": 7}
    assert [r["item_type"] for r in await store.rows()] == ["EMAIL"]
    assert (await batch(store, task, 20))["done"]
    assert [r["external_id"] for r in await store.rows()] == ["kept", "kept::att"]


async def test_p8_unknown_provenance_rejects_manifest_before_target_writes(store):
    thread = uuid.uuid4()
    async with store.factory() as s:
        await s.execute(text("INSERT INTO chat_threads VALUES (:i,:t,'chat-a')"), {"i": thread, "t": store.tenant})
        await s.execute(text("INSERT INTO chat_thread_messages VALUES (:i,'m1'),(:i,'m2')"), {"i": thread})
        await s.commit()
    await store.add("m1::att", kind="CHAT_ATTACHMENT", parent="m1", metadata={"chat_id": "chat-a"})
    await store.add("m2::att", kind="CHAT_ATTACHMENT", parent="m2", metadata={})
    task = await store.task("LEGACY_CHAT_ATTACHMENTS", chat_id="chat-a")
    with pytest.raises(RuntimeError, match="provenance"):
        await prepare(store, task)
    assert await store.rows() == []
    async with store.factory() as s:
        assert await s.scalar(text("SELECT count(*) FROM copy_work_items WHERE task_id=:t"), {"t": task.id}) == 0


async def test_archive_counts_primary_inventory_and_preserves_children(store):
    await store.add("a", kind="ARCHIVE_ITEM")
    await store.add("a::att", kind="ARCHIVE_ATTACHMENT", parent="a")
    await store.add("changed", kind="ARCHIVE_ITEM")
    task = await store.task("ARCHIVE_CARRY", prior_snapshot_id=str(store.source), external_ids=["a"])
    await prepare(store, task)
    result = await batch(store, task, 20)
    assert result == {"done": True, "cursor": 2, "rows": 1, "bytes": 14}
    assert [r["external_id"] for r in await store.rows()] == ["a::att", "a"]


async def test_file_versions_only_copy_selected_parents(store):
    await store.add("file-a::v1", kind="FILE_VERSION", parent="file-a")
    await store.add("file-b::v1", kind="FILE_VERSION", parent="file-b")
    task = await store.task("FILE_VERSIONS", prior_snap_id=str(store.source), file_ids=["file-a"])
    await prepare(store, task)
    assert (await batch(store, task, 20))["rows"] == 1
    assert [r["external_id"] for r in await store.rows()] == ["file-a::v1"]


async def test_rename_changes_only_target_and_respects_legacy_predicate(store):
    await store.add("old", metadata={"chatId": "chat-a"}, folder="chats/Untitled chat #old", snapshot=store.target)
    await store.add("good", metadata={"chatId": "chat-a", "keep": True}, folder="chats/Resolved", snapshot=store.target)
    await store.add("history", metadata={"chatId": "chat-a"}, folder="chats/Untitled chat #old")
    task = await store.task("CHAT_RENAME", chat_ids=["chat-a"], names=["Untitled chat #hash"], legacy_patterns_only=True)
    await prepare(store, task)
    assert (await batch(store, task, 20))["rows"] == 1
    assert [(r["external_id"], r["folder_path"]) for r in await store.rows()] == [
        ("good", "chats/Resolved"), ("old", "chats/Untitled chat #hash")]


async def test_manifest_rejects_wrong_target_or_explicit_source_owner(store):
    task = await store.task("CHAT_CARRY", target_snapshot_id=str(uuid.uuid4()))
    with pytest.raises(ValueError, match="target"):
        await prepare(store, task)
    foreign_resource, foreign_source = uuid.uuid4(), uuid.uuid4()
    async with store.factory() as s:
        await s.execute(text("INSERT INTO resources VALUES (:r,:t)"), {"r": foreign_resource, "t": uuid.uuid4()})
        await s.execute(text("INSERT INTO snapshots VALUES (:s,:r,NULL,'COMPLETED','2026-01-01','{}',NULL,NULL)"),
                        {"s": foreign_source, "r": foreign_resource})
        await s.commit()
    task = await store.task("FILE_VERSIONS", prior_snap_id=str(foreign_source), file_ids=["file-a"])
    with pytest.raises(ValueError, match="source"):
        await prepare(store, task)


async def test_empty_mail_whitelist_never_carries_even_existing_parents_children(store):
    await store.add("a", kind="EMAIL")
    await store.add("a", kind="EMAIL", snapshot=store.target)
    await store.add("a::att", kind="EMAIL_ATTACHMENT", parent="a")
    task = await store.task("MAIL_CARRY", keep_folder_ids=[])
    await prepare(store, task)
    assert await batch(store, task, 20) == {"done": True, "cursor": 0, "rows": 0, "bytes": 0}


async def test_prepared_manifest_pins_source_and_never_reselects_newer_prior(store):
    await store.add("a")
    task = await store.task("CHAT_CARRY")
    await prepare(store, task)
    newer = uuid.uuid4()
    async with store.factory() as s:
        await s.execute(text("INSERT INTO snapshots VALUES (:s,:r,NULL,'COMPLETED','2026-01-01 12:00','{}',NULL,NULL)"),
                        {"s": newer, "r": store.resource})
        await s.commit()
    await store.add("newer-only", snapshot=newer)
    await prepare(store, task)
    async with store.factory() as s:
        with pytest.raises(IntegrityError):
            await s.execute(text("DELETE FROM snapshot_items WHERE snapshot_id=:s"), {"s": store.source})
            await s.execute(text("DELETE FROM snapshots WHERE id=:s"), {"s": store.source})
        await s.rollback()
    assert (await batch(store, task, 20))["rows"] == 1
    assert [r["external_id"] for r in await store.rows()] == ["a"]
    assert task.params["prior_snapshot_id"] == str(store.source)


async def test_p8_freezes_active_source_payload_and_newest_other_chat_falls_back(store):
    thread, active = uuid.uuid4(), uuid.uuid4()
    async with store.factory() as s:
        await s.execute(text("INSERT INTO chat_threads VALUES (:i,:t,'chat-a')"), {"i": thread, "t": store.tenant})
        await s.execute(text("INSERT INTO chat_thread_messages VALUES (:i,'m1')"), {"i": thread})
        await s.execute(text("INSERT INTO snapshots VALUES (:s,:r,NULL,'IN_PROGRESS','2026-01-01 12:00','{}',NULL,NULL)"),
                        {"s": active, "r": store.resource})
        await s.commit()
    await store.add("m1::att", kind="CHAT_ATTACHMENT", parent="m1", metadata={"chat_id": "chat-a"}, name="own")
    await store.add("m1::att", snapshot=active, kind="CHAT_ATTACHMENT", parent="m1",
                    metadata={"chat_id": "chat-b"}, name="other")
    async with store.factory() as s:
        await s.execute(text("UPDATE snapshot_items SET created_at='2026-01-01 12:00' WHERE snapshot_id=:s"), {"s": active})
        await s.commit()
    task = await store.task("LEGACY_CHAT_ATTACHMENTS", chat_id="chat-a")
    await prepare(store, task)
    async with store.factory() as s:
        await s.execute(text("UPDATE snapshot_items SET name='mutated' WHERE snapshot_id=:s"), {"s": store.source})
        await s.commit()
    assert (await batch(store, task, 20))["rows"] == 1
    assert [(r["external_id"], r["name"]) for r in await store.rows()] == [("m1::att", "own")]


async def test_source_row_wrong_tenant_is_rejected_instead_of_certifying_empty_copy(store):
    await store.add("a", owner=uuid.uuid4())
    task = await store.task("CHAT_CARRY")
    with pytest.raises(ValueError, match="tenant"):
        await prepare(store, task)
    assert await store.rows() == []


async def test_prior_with_required_pending_copy_is_never_selected(store):
    await store.add("pending-only")
    older, old_workflow = uuid.uuid4(), uuid.uuid4()
    async with store.factory() as s:
        await s.execute(text("INSERT INTO snapshots VALUES (:s,:r,NULL,'COMPLETED','2025-12-31','{}',NULL,NULL)"),
                        {"s": older, "r": store.resource})
        await s.execute(text("INSERT INTO copy_workflows(id,tenant_id,resource_id,target_snapshot_id,state) "
                             "VALUES (:w,:t,:r,:s,'WAITING')"),
                        {"w": old_workflow, "t": store.tenant, "r": store.resource, "s": store.source})
        await s.execute(text("INSERT INTO copy_work_tasks(id,workflow_id,kind,params,state) "
                             "VALUES (:i,:w,'CHAT_CARRY','{}','READY')"), {"i": uuid.uuid4(), "w": old_workflow})
        await s.commit()
    await store.add("complete-only", snapshot=older)
    task = await store.task("CHAT_CARRY")
    await prepare(store, task)
    await batch(store, task, 20)
    assert [r["external_id"] for r in await store.rows()] == ["complete-only"]
    assert task.params["prior_snapshot_id"] == str(older)


async def test_mail_missing_parent_does_not_checkpoint_attachment_as_completed(store):
    await store.add("a", kind="EMAIL")
    await store.add("a::att", kind="EMAIL_ATTACHMENT", parent="a")
    task = await store.task("MAIL_CARRY")
    await prepare(store, task)
    first = await batch(store, task, 20)
    assert first["cursor"] == 1
    async with store.factory() as s:
        await s.execute(text("DELETE FROM snapshot_items WHERE snapshot_id=:s"), {"s": store.target})
        await s.commit()
    with pytest.raises(RuntimeError, match="parent"):
        await batch(store, task, 20)
    assert task.checkpoint == 1


async def test_later_task_in_workflow_reuses_first_prepared_predecessor(store):
    await store.add("original-prior")
    first = await store.task("CHAT_CARRY")
    await prepare(store, first)
    newer = uuid.uuid4()
    async with store.factory() as s:
        await s.execute(text("INSERT INTO snapshots VALUES (:s,:r,NULL,'COMPLETED','2026-01-01 12:00','{}',NULL,NULL)"),
                        {"s": newer, "r": store.resource})
        await s.commit()
    await store.add("newer-prior", snapshot=newer)
    second = await store.task("CHAT_CARRY")
    await prepare(store, second)
    assert second.params["prior_snapshot_id"] == str(store.source)
    await batch(store, second, 20)
    assert [r["external_id"] for r in await store.rows()] == ["original-prior"]


async def test_workflow_explicit_predecessor_conflict_is_rejected(store):
    await store.add("file-a::v1", kind="FILE_VERSION", parent="file-a")
    first = await store.task("FILE_VERSIONS", prior_snap_id=str(store.source), file_ids=["file-a"])
    await prepare(store, first)
    older = uuid.uuid4()
    async with store.factory() as s:
        await s.execute(text("INSERT INTO snapshots VALUES (:s,:r,NULL,'COMPLETED','2025-12-31','{}',NULL,NULL)"),
                        {"s": older, "r": store.resource})
        await s.commit()
    second = await store.task("FILE_VERSIONS", prior_snap_id=str(older), file_ids=["file-b"])
    with pytest.raises(ValueError, match="source"):
        await prepare(store, second)


async def test_manifest_cannot_register_source_after_retention_claim(store):
    import asyncio
    await store.add('a')
    task=await store.task('CHAT_CARRY')
    tag='copy-pin-race-'+uuid.uuid4().hex
    entered=asyncio.Event()
    async with store.factory() as holder:
        await holder.execute(text("UPDATE snapshots SET status='PENDING_DELETION' WHERE id=:id"),{'id':store.source})
        async def prepare_while_claimed():
            async with store.factory() as session:
                try:
                    await session.execute(text("SELECT set_config('application_name',:tag,true)"),{'tag':tag})
                    entered.set()
                    await executor().prepare_manifest(session,task)
                    await session.commit()
                except BaseException:
                    await session.rollback();raise
        running=asyncio.create_task(prepare_while_claimed())
        await entered.wait()
        for _ in range(100):
            if running.done(): break
            async with store.factory() as probe:
                blocked=await probe.scalar(text("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=:tag AND wait_event_type='Lock')"),{'tag':tag})
            if blocked: break
            await asyncio.sleep(0.01)
        await holder.commit()
        with pytest.raises(ValueError,match='source'):
            await running
    assert await store.rows()==[]


async def test_batch_is_capped_by_stored_bytes_but_always_takes_one_row(store, monkeypatch):
    # Inline mail attachments put ~1 MB of base64 in each row; 512 of them in
    # one statement outran the 30 s copy statement timeout on every retry.
    for ext in ("a", "b", "c"):
        await store.add(ext, kind="EMAIL", metadata={"inline_b64": os.urandom(3000).hex()})
    monkeypatch.setattr(executor(), "BATCH_MAX_BYTES", 8000)
    task = await store.task("MAIL_CARRY")
    await prepare(store, task)
    first = await batch(store, task, 20)
    assert (first["rows"], first["done"]) == (2, False)
    monkeypatch.setattr(executor(), "BATCH_MAX_BYTES", 1)
    assert (await batch(store, task, 20))["rows"] == 1
    assert [r["external_id"] for r in await store.rows()] == ["a", "b", "c"]


def test_batch_byte_cap_is_configurable(monkeypatch):
    """COPY_BATCH_MAX_BYTES sizes copy batches without a rebuild (operators tune it against
    COPY_WORK_STATEMENT_TIMEOUT_MS)."""
    module = executor()
    monkeypatch.setenv("COPY_BATCH_MAX_BYTES", str(48 * 1024 * 1024))
    try:
        assert importlib.reload(module).BATCH_MAX_BYTES == 48 * 1024 * 1024
    finally:
        monkeypatch.delenv("COPY_BATCH_MAX_BYTES")
        importlib.reload(module)


async def test_attachment_parent_check_reads_each_stored_payload_once(store):
    """Local measurement 2026-11-06: with fresh copy_work_items statistics the planner checked each
    attachment's parent by walking the target's rows and re-reading the row's stored json payload
    for every comparison (130 attachments x ~236 target rows, 10.5 s of a 10.7 s batch, growing as
    the target fills). A whole batch must stay inside a few seconds of statement budget."""
    async with store.factory() as s:
        await s.execute(text("""
            INSERT INTO snapshot_items(id,snapshot_id,tenant_id,item_type,external_id,name,content_size,metadata)
            SELECT gen_random_uuid(),:s,:t,'EMAIL','m' || lpad(n::text,5,'0'),'m',1,'{}'
              FROM generate_series(1,2000) n CROSS JOIN (VALUES (CAST(:src AS uuid)),(CAST(:tgt AS uuid))) v(s)
        """.replace(":s,", "v.s,")), {"t": store.tenant, "src": store.source, "tgt": store.target})
        await s.commit()
    for n in range(150):
        await store.add(f"m{2000 - n:05d}::att", kind="EMAIL_ATTACHMENT", parent=f"m{2000 - n:05d}",
                        metadata={"inline_b64": os.urandom(40000).hex()})
    task = await store.task("MAIL_CARRY")
    await prepare(store, task)
    copied, result = 0, {"done": False}
    for _ in range(5):  # EMAIL phase, then byte-capped attachment batches
        async with store.factory() as s:
            await s.execute(text("SET statement_timeout = '5s'"))
            result = await executor().copy_manifest_batch(s, task, limit=20000)
            await s.commit()
        task.checkpoint, copied = result["cursor"], copied + result["rows"]
        if result["done"]:
            break
    assert result["done"] and copied == 150
