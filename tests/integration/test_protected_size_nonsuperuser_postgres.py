"""Migration 0014 must install for an application role that owns the tables but is not a
superuser (Taylor Morrison's migrate job runs as plain LOGIN role `tmvault`). Only the two
partition-DDL event triggers need superuser; without them, writes through the tables are
still captured."""

import importlib
import os
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

pytestmark = pytest.mark.integration


async def test_ledger_installs_and_captures_without_superuser():
    url = os.environ.get("TMVAULT_TEST_DATABASE_URL")
    if not url:
        pytest.skip("requires an explicitly disposable PostgreSQL database")
    suffix = uuid4().hex[:12]
    schema, role = f"ps_plain_{suffix}", f"ps_plain_role_{suffix}"
    engine = create_async_engine(url, connect_args={"statement_cache_size": 0})
    module = importlib.import_module("shared.protected_size_schema")
    try:
        async with engine.begin() as conn:
            await conn.execute(text(f'CREATE ROLE "{role}" LOGIN NOSUPERUSER'))
            await conn.execute(text(f'CREATE SCHEMA "{schema}" AUTHORIZATION "{role}"'))
            await conn.execute(text(f'SET LOCAL ROLE "{role}"'))
            await conn.execute(text(f'SET LOCAL search_path TO "{schema}"'))
            await conn.execute(text("CREATE TABLE resources(id uuid PRIMARY KEY,tenant_id uuid,type text)"))
            await conn.execute(text("CREATE TABLE snapshots(id uuid PRIMARY KEY,resource_id uuid REFERENCES resources(id) ON DELETE CASCADE,status text,created_at timestamp) PARTITION BY HASH(id)"))
            await conn.execute(text("CREATE TABLE snapshots_p0 PARTITION OF snapshots FOR VALUES WITH(MODULUS 1,REMAINDER 0)"))
            await conn.execute(text("CREATE TABLE snapshot_items(id uuid NOT NULL,snapshot_id uuid NOT NULL,external_id text,item_type text,content_size bigint,is_deleted boolean,created_at timestamp) PARTITION BY HASH(snapshot_id)"))
            await conn.execute(text("CREATE TABLE snapshot_items_p0 PARTITION OF snapshot_items FOR VALUES WITH(MODULUS 1,REMAINDER 0)"))
            await conn.execute(text("CREATE TABLE resource_protected_bytes(resource_id uuid PRIMARY KEY REFERENCES resources(id) ON DELETE CASCADE,latest_snapshot_id uuid NOT NULL,bytes bigint NOT NULL DEFAULT 0,computed_at timestamptz NOT NULL DEFAULT now())"))
            await module.install(conn)
            triggers = {r[0] for r in await conn.execute(text(
                "SELECT tgname FROM pg_trigger WHERE tgrelid IN ('snapshot_items'::regclass,'snapshot_items_p0'::regclass) AND NOT tgisinternal"))}
            assert {"protected_size_insert", "protected_size_delete"} <= triggers
            events = await conn.scalar(text("SELECT count(*) FROM pg_event_trigger WHERE evtname = ANY(:n)"),
                                       {"n": list(module.event_trigger_names(schema))})
            assert events == 0
    finally:
        async with engine.begin() as conn:
            for trigger in module.event_trigger_names(schema):
                await conn.execute(text(f'DROP EVENT TRIGGER IF EXISTS "{trigger}"'))
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
            if await conn.scalar(text("SELECT count(*) FROM pg_roles WHERE rolname=:r"), {"r": role}):
                await conn.execute(text(f'DROP OWNED BY "{role}"'))
                await conn.execute(text(f'DROP ROLE "{role}"'))
        await engine.dispose()
