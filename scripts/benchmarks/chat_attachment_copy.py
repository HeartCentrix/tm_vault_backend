"""Compare old/new attachment-copy plans on synthetic, disposable history.

Requires TMVAULT_TEST_DATABASE_URL (SQLAlchemy asyncpg URL). Creates and drops
only a uniquely named schema. No application data or blob storage is touched.
"""
from __future__ import annotations

import argparse
import ast
import asyncio
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from shared.chat_attachment_copy import LEGACY_CHAT_ATTACHMENT_COPY_SQL


def baseline_sql(ref):
    source = subprocess.run(["git", "show", f"{ref}:workers/backup-worker/main.py"],
                            cwd=ROOT, capture_output=True, text=True, check=True).stdout
    fn = next(n for n in ast.parse(source).body if isinstance(n, ast.AsyncFunctionDef)
              and n.name == "_copy_chat_attachment_rows_to_snapshot")
    return next(n.args[0].value for n in ast.walk(fn) if isinstance(n, ast.Call)
                and isinstance(n.func, ast.Name) and n.func.id == "text"
                and n.args and isinstance(n.args[0], ast.Constant))


def plan_summary(document):
    plan = document[0]
    sorts = []

    def walk(node):
        if node.get("Node Type") in {"Sort", "Incremental Sort"}:
            sorts.append({k: node[k] for k in ("Plan Width", "Actual Rows", "Sort Method",
                                               "Sort Space Used", "Sort Space Type") if k in node})
        for child in node.get("Plans", []):
            walk(child)

    walk(plan["Plan"])
    return {"execution_ms": plan["Execution Time"], "sorts": sorts,
            "temp_read_blocks": plan["Plan"].get("Temp Read Blocks", 0),
            "temp_written_blocks": plan["Plan"].get("Temp Written Blocks", 0)}


async def benchmark(args):
    url = os.environ.get("TMVAULT_TEST_DATABASE_URL")
    if not url:
        raise SystemExit("Set TMVAULT_TEST_DATABASE_URL to a disposable local PostgreSQL database")
    schema = "chat_copy_bench_" + uuid.uuid4().hex
    engine = create_async_engine(url, connect_args={
        "server_settings": {"search_path": schema}, "statement_cache_size": 0,
    })
    tid, rid, tsid, backend = [uuid.uuid4() for _ in range(4)]
    binds = {"tid": tid, "cid": "synthetic-chat", "tsid": tsid}
    created = False
    try:
        async with engine.begin() as conn:
            await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
            created = True
            for ddl in (
                "CREATE TABLE resources (id uuid PRIMARY KEY, tenant_id uuid)",
                "CREATE TABLE snapshots (id uuid PRIMARY KEY, resource_id uuid)",
                "CREATE TABLE chat_threads (id uuid PRIMARY KEY, tenant_id uuid, chat_id text)",
                "CREATE TABLE chat_thread_messages (chat_thread_id uuid, message_external_id text)",
                "CREATE TABLE snapshot_items (id uuid PRIMARY KEY, snapshot_id uuid, tenant_id uuid, "
                "item_type text, external_id text, parent_external_id text, name text, folder_path text, "
                "content_hash text, content_checksum text, content_size bigint, blob_path text, "
                "encryption_key_id text, backup_version integer, metadata json, is_deleted boolean, "
                "indexed_at timestamp, backend_id uuid, created_at timestamp, "
                "UNIQUE(snapshot_id,external_id,item_type))",
                "CREATE INDEX ON snapshot_items(parent_external_id)",
                "CREATE INDEX ON chat_thread_messages(chat_thread_id,message_external_id)",
            ):
                await conn.execute(text(ddl))
            await conn.execute(text("INSERT INTO resources VALUES (:r,:t)"), {"r": rid, "t": tid})
            await conn.execute(text("INSERT INTO snapshots VALUES (:s,:r)"), {"s": tsid, "r": rid})
            thread = uuid.uuid4()
            await conn.execute(text("INSERT INTO chat_threads VALUES (:i,:t,:c)"),
                               {"i": thread, "t": tid, "c": binds["cid"]})
            await conn.execute(text("INSERT INTO chat_thread_messages SELECT :i, 'm'||g "
                                    "FROM generate_series(1,:n) g"), {"i": thread, "n": args.items})
            for _ in range(args.history):
                sid = uuid.uuid4()
                await conn.execute(text("INSERT INTO snapshots VALUES (:s,:r)"), {"s": sid, "r": rid})
                await conn.execute(text("INSERT INTO snapshot_items "
                    "(id,snapshot_id,tenant_id,item_type,external_id,parent_external_id,name,created_at) "
                    "SELECT gen_random_uuid(),:s,:t,'TEAMS_CHAT_MESSAGE','m'||g,:c,'message',NOW() "
                    "FROM generate_series(1,:n) g"), {"s": sid, "t": tid, "c": binds["cid"], "n": args.items})
                await conn.execute(text("INSERT INTO snapshot_items "
                    "(id,snapshot_id,tenant_id,item_type,external_id,parent_external_id,name,content_size,"
                    "metadata,backend_id,created_at) SELECT gen_random_uuid(),:s,:t,'CHAT_ATTACHMENT',"
                    "'m'||g||'::file','m'||g,'file.bin',:size,"
                    "json_build_object('chat_id',CAST(:c AS text),'inline_b64',repeat('A',:bytes)),:b,NOW() "
                    "FROM generate_series(1,:n) g"), {"s": sid, "t": tid, "c": binds["cid"],
                     "n": args.items, "bytes": args.payload_bytes, "size": args.payload_bytes, "b": backend})
            await conn.execute(text("ANALYZE"))

        async def run(sql):
            async with engine.begin() as conn:
                doc = (await conn.execute(text("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql), binds)).scalar_one()
                return plan_summary(json.loads(doc) if isinstance(doc, str) else doc)

        async def inventory():
            async with engine.connect() as conn:
                return tuple((await conn.execute(text("SELECT count(*), md5(COALESCE(string_agg("
                    "external_id||'|'||item_type||'|'||metadata::text, '' ORDER BY external_id,item_type),'')) "
                    "FROM snapshot_items WHERE snapshot_id=:s"), {"s": tsid})).one())

        result = {"history_snapshots": args.history, "items_per_chat": args.items,
                  "payload_bytes_per_item": args.payload_bytes}
        for name, sql in (("baseline", baseline_sql(args.baseline_ref)),
                          ("optimized", LEGACY_CHAT_ATTACHMENT_COPY_SQL)):
            async with engine.begin() as conn:
                await conn.execute(text("DELETE FROM snapshot_items WHERE snapshot_id=:s"), {"s": tsid})
            first = await run(sql)
            before = await inventory()
            repeat = await run(sql)
            assert await inventory() == before, "repeat changed the retained inventory"
            result[name] = {"first": first, "repeat": repeat, "inventory": before}
        assert result["baseline"]["inventory"] == result["optimized"]["inventory"], "output mismatch"
        assert result["optimized"]["inventory"][0] == args.items
        result["output_equivalent"] = True
        print(json.dumps(result, indent=2))
    finally:
        if created:
            async with engine.begin() as conn:
                await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-ref", default="6ca5010")
    parser.add_argument("--history", type=int, default=20)
    parser.add_argument("--items", type=int, default=100)
    parser.add_argument("--payload-bytes", type=int, default=16384)
    options = parser.parse_args()
    if min(options.history, options.items, options.payload_bytes) <= 0:
        parser.error("history, items and payload-bytes must be positive")
    asyncio.run(benchmark(options))
