"""Real PostgreSQL coverage for chat attachment inventory copying.

Set TMVAULT_TEST_DATABASE_URL to a disposable PostgreSQL database. Every test
uses its own schema; no application tables or tenant data are read.
"""
from __future__ import annotations

import ast
from contextlib import asynccontextmanager
from datetime import datetime
import json
import os
from pathlib import Path
import uuid

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[2]


@pytest_asyncio.fixture
async def store():
    url = os.environ.get("TMVAULT_TEST_DATABASE_URL")
    if not url:
        pytest.skip("requires an explicitly configured disposable PostgreSQL database")
    schema = "chat_copy_test_" + uuid.uuid4().hex
    engine = create_async_engine(url, connect_args={
        "server_settings": {"search_path": schema}, "statement_cache_size": 0,
    })
    async with engine.begin() as conn:
        await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        for ddl in (
            "CREATE TABLE resources (id uuid PRIMARY KEY, tenant_id uuid NOT NULL)",
            "CREATE TABLE snapshots (id uuid PRIMARY KEY, resource_id uuid NOT NULL)",
            "CREATE TABLE chat_threads (id uuid PRIMARY KEY, tenant_id uuid, chat_id text)",
            "CREATE TABLE chat_thread_messages (chat_thread_id uuid, message_external_id text)",
            "CREATE TABLE snapshot_items (id uuid PRIMARY KEY, snapshot_id uuid NOT NULL, "
            "tenant_id uuid, item_type text NOT NULL, external_id text NOT NULL, "
            "parent_external_id text, name text NOT NULL, folder_path text, content_hash text, "
            "content_checksum text, content_size bigint, blob_path text, encryption_key_id text, "
            "backup_version integer, metadata json, is_deleted boolean, indexed_at timestamp, "
            "backend_id uuid, created_at timestamp, UNIQUE(snapshot_id, external_id, item_type))",
            "CREATE INDEX ON snapshot_items(parent_external_id)",
            "CREATE INDEX ON chat_thread_messages(chat_thread_id, message_external_id)",
        ):
            await conn.execute(text(ddl))
    factory = async_sessionmaker(engine, expire_on_commit=False)
    tenant, foreign, resource, source, target, backend = [uuid.uuid4() for _ in range(6)]
    async with factory() as session:
        await session.execute(text("INSERT INTO resources VALUES (:r, :t)"), {"r": resource, "t": tenant})
        for sid in (source, target):
            await session.execute(text("INSERT INTO snapshots VALUES (:s, :r)"), {"s": sid, "r": resource})
        thread = uuid.uuid4()
        await session.execute(text("INSERT INTO chat_threads VALUES (:i, :t, 'chat-a')"), {"i": thread, "t": tenant})
        for mid in ("m1", "m2"):
            await session.execute(text("INSERT INTO chat_thread_messages VALUES (:i, :m)"), {"i": thread, "m": mid})
        await session.commit()

    async def add(*, snapshot=source, external="m1::file", kind="CHAT_ATTACHMENT",
                  parent="m1", metadata=None, owner=tenant, name="report.pdf",
                  created=datetime(2026, 1, 1), size=7, blob="objects/report", **overrides):
        fields = dict(id=uuid.uuid4(), snapshot_id=snapshot, tenant_id=owner,
                      item_type=kind, external_id=external, parent_external_id=parent,
                      name=name, folder_path="chats/Project", content_hash="content-a",
                      content_checksum="checksum-a", content_size=size, blob_path=blob,
                      encryption_key_id="key-version-2", backup_version=4,
                      metadata=json.dumps({"chat_id": "chat-a"} if metadata is None else metadata),
                      is_deleted=False, indexed_at=datetime(2026, 1, 2),
                      backend_id=backend, created_at=created)
        fields.update(overrides)
        cols = ", ".join(fields)
        binds = ", ".join("CAST(:metadata AS json)" if k == "metadata" else ":" + k for k in fields)
        async with factory() as session:
            await session.execute(text(f"INSERT INTO snapshot_items ({cols}) VALUES ({binds})"), fields)
            await session.commit()

    @asynccontextmanager
    async def slot(*args, **kwargs):
        async with factory() as session:
            yield session

    # Execute the actual worker function, isolating import/startup side effects.
    tree = ast.parse((ROOT / "workers/backup-worker/main.py").read_text())
    fn = next(n for n in tree.body if isinstance(n, ast.AsyncFunctionDef)
              and n.name == "_copy_chat_attachment_rows_to_snapshot")
    ns = {"carry_forward_slot_session": slot, "text": text}
    exec(compile(ast.fix_missing_locations(ast.Module(body=[fn], type_ignores=[])),
                 "backup-worker/main.py", "exec"), ns)

    async def copy():
        return await ns[fn.name](tenant_id=str(tenant), target_snapshot_id=str(target), chat_id="chat-a")

    async def target_rows():
        async with factory() as session:
            return [dict(r) for r in (await session.execute(text(
                "SELECT * FROM snapshot_items WHERE snapshot_id=:s ORDER BY external_id, item_type"
            ), {"s": target})).mappings()]

    yield dict(factory=factory, engine=engine, add=add, copy=copy, rows=target_rows,
               tenant=tenant, foreign=foreign, resource=resource, source=source,
               target=target, backend=backend)
    async with engine.begin() as conn:
        await conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    await engine.dispose()


async def test_copy_preserves_fields_and_existing_destination(store):
    await store["add"]()
    await store["add"](snapshot=store["target"], metadata={"keep": "destination"})
    await store["add"](external="m2::legacy", parent=None, metadata={"chat_id": "chat-a", "inline_b64": "ZmlsZQ=="})
    assert await store["copy"]() == 1
    rows = await store["rows"]()
    assert rows[0]["metadata"] == {"keep": "destination"}
    legacy = rows[1]
    assert legacy["parent_external_id"] == "m2"
    assert legacy["metadata"]["inline_b64"] == "ZmlsZQ=="
    assert legacy["encryption_key_id"] == "key-version-2"
    assert legacy["backend_id"] == store["backend"]
    assert legacy["backup_version"] == 4
    assert legacy["content_checksum"] == "checksum-a"
    assert await store["copy"]() == 0


async def test_copy_recovers_null_tenant_hosted_content_from_verified_owner(store):
    await store["add"](kind="TEAMS_CHAT_MESSAGE", external="m1", parent="chat-a", metadata={})
    await store["add"](kind="CHAT_HOSTED_CONTENT", external="m1:1", owner=None,
                       metadata={"inline_b64": "aW1hZ2U=", "content_type": "image/png"}, blob=None)
    assert await store["copy"]() == 1
    row, = await store["rows"]()
    assert row["item_type"] == "CHAT_HOSTED_CONTENT"
    assert row["tenant_id"] == store["tenant"]
    assert row["metadata"]["inline_b64"] == "aW1hZ2U="


async def test_copy_never_uses_explicitly_different_chat(store):
    await store["add"](external="m1::foreign", metadata={"chat_id": "chat-b"})
    await store["add"](external="m1::own", metadata={"chat_id": "chat-a"})
    assert await store["copy"]() == 1
    assert [r["external_id"] for r in await store["rows"]()] == ["m1::own"]


async def test_copy_reports_ambiguous_chat_provenance_without_inserting(store):
    await store["add"](metadata={})
    with pytest.raises(RuntimeError, match="provenance"):
        await store["copy"]()
    assert await store["rows"]() == []


async def test_copy_uses_source_message_membership_when_metadata_has_no_chat(store):
    await store["add"](kind="TEAMS_CHAT_MESSAGE", external="m1", parent="chat-a", metadata={})
    await store["add"](metadata={"inline_b64": "eA=="})
    assert await store["copy"]() == 1


async def test_copy_excludes_other_tenant_even_with_matching_message_and_chat(store):
    foreign_resource, foreign_snapshot = uuid.uuid4(), uuid.uuid4()
    async with store["factory"]() as session:
        await session.execute(text("INSERT INTO resources VALUES (:r, :t)"),
                              {"r": foreign_resource, "t": store["foreign"]})
        await session.execute(text("INSERT INTO snapshots VALUES (:s, :r)"),
                              {"s": foreign_snapshot, "r": foreign_resource})
        await session.commit()
    await store["add"](snapshot=foreign_snapshot, owner=None)
    assert await store["copy"]() == 0
    assert await store["rows"]() == []


async def test_copy_concurrent_replays_insert_one_logical_reference(store):
    import asyncio
    await store["add"]()
    results = await asyncio.gather(store["copy"](), store["copy"]())
    assert sum(results) == 1
    assert len(await store["rows"]()) == 1
