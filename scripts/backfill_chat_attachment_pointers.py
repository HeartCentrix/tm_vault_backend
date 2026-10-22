"""Backfill CHAT_ATTACHMENT / CHAT_HOSTED_CONTENT pointer rows that the
per-chat P8 copy silently dropped (2026-10-15 RCA).

Between 2026-09-24 and the P8 predicate fix, ~80% of P8 copies hit
CARRY_FORWARD_WAIT_MAX_S and the exception was swallowed: the chat's
messages are pointed at by the snapshot but its attachment rows are not,
so restore/attachment chips for that chat depend on the tenant-wide
fallback and vanish the moment the original drainer's snapshot is deleted.

The P8 INSERT is idempotent (ON CONFLICT DO NOTHING on
(snapshot_id, external_id, item_type)), so this re-runs it per snapshot,
batching every chat the snapshot carries message rows for. Both branches of
the predicate are index-sargable (idx_snapshot_items_parent_ext).

Runs with the job-service pod's DB env (DB_DIRECT_HOST, DB_USERNAME, ...):

    kubectl exec -i -n itops <job-service-pod> -- python3 - \
        --since 2026-09-24 [--limit N] [--dry-run] [--snapshot <uuid> ...]
        [--chat-batch N] < scripts/backfill_chat_attachment_pointers.py

Serial by design (one statement at a time, statement_timeout per statement)
so it can never convoy the primary like the per-chat copies did. Re-runs
are safe; newest snapshots first so the current restore points heal first.
Snapshots with thousands of chats take --chat-batch N so each INSERT covers
at most N chats and stays under the statement timeout. --exclude-chat skips
a chat entirely: the 2026-10-16 profiling found one oneOnOne chat
(19:dcf372b3-..._512b84d2-...@unq.gbl.spaces, 34k bot messages x ~22 card
attachments = 757k pointer rows) that alone makes every Jason Rue copy run
for 10+ minutes; it is excluded rather than replicated 36 more times.

Point-in-time: a row is only copied when the TARGET snapshot holds the
parent message row, so a 09/24 restore point never gains attachments of
messages posted after it (the first full run lacked this guard and left
~2% such rows behind).
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
import uuid
from datetime import datetime

import asyncpg

# Drained message rows carry chatId in metadata; claim-skipped POINTER rows
# (the ones whose attachments P8 dropped) have metadata = {} and the chat id
# in parent_external_id ('19:...'). Take whichever is present.
CHAT_IDS_IN_SNAPSHOT = """
    SELECT DISTINCT COALESCE(si.metadata->>'chatId', si.parent_external_id) AS chat_id
      FROM snapshot_items si
     WHERE si.snapshot_id = $1
       AND si.item_type = 'TEAMS_CHAT_MESSAGE'
       AND COALESCE(si.metadata->>'chatId', si.parent_external_id) LIKE '19:%'
"""

# Same statement as workers/backup-worker/main.py::_copy_chat_attachment_rows_to_snapshot,
# with the chat predicate widened to every chat of the snapshot.
COPY_SQL = """
    INSERT INTO snapshot_items (
      id, snapshot_id, tenant_id, item_type, external_id,
      parent_external_id, name, folder_path, content_hash,
      content_checksum, content_size, blob_path,
      encryption_key_id, backup_version, metadata, is_deleted,
      indexed_at, backend_id, created_at
    )
    SELECT DISTINCT ON (u.external_id, u.item_type)
           gen_random_uuid(), $1, $2, u.item_type,
           u.external_id,
           COALESCE(u.parent_external_id, split_part(u.external_id, '::', 1)),
           u.name,
           u.folder_path, u.content_hash,
           u.content_checksum, u.content_size,
           u.blob_path, u.encryption_key_id,
           COALESCE(u.backup_version, 1), u.metadata,
           COALESCE(u.is_deleted, false), u.indexed_at,
           u.backend_id, NOW()
      FROM (
            SELECT src.* FROM snapshot_items src
             WHERE src.tenant_id = $2
               AND src.item_type IN ('CHAT_ATTACHMENT', 'CHAT_HOSTED_CONTENT')
               AND src.parent_external_id = ANY(ARRAY(
                     SELECT m.message_external_id
                       FROM chat_thread_messages m
                       JOIN chat_threads t ON t.id = m.chat_thread_id
                      WHERE t.tenant_id = $2 AND t.chat_id = ANY($3::text[])))
            UNION ALL
            SELECT src.* FROM snapshot_items src
             WHERE src.tenant_id = $2
               AND src.item_type IN ('CHAT_ATTACHMENT', 'CHAT_HOSTED_CONTENT')
               AND src.parent_external_id IS NULL
               AND split_part(src.external_id, '::', 1) = ANY(ARRAY(
                     SELECT m.message_external_id
                       FROM chat_thread_messages m
                       JOIN chat_threads t ON t.id = m.chat_thread_id
                      WHERE t.tenant_id = $2 AND t.chat_id = ANY($3::text[])))
           ) u
     WHERE EXISTS (SELECT 1 FROM snapshot_items m
                    WHERE m.snapshot_id = $1 AND m.item_type = 'TEAMS_CHAT_MESSAGE'
                      AND m.external_id = COALESCE(u.parent_external_id, split_part(u.external_id, '::', 1)))
    ON CONFLICT (snapshot_id, external_id, item_type) DO NOTHING
"""

SNAPSHOTS_SQL = """
    SELECT s.id, r.tenant_id, r.display_name, s.started_at, s.status, s.item_count
      FROM snapshots s
      JOIN resources r ON r.id = s.resource_id
     WHERE r.type = 'USER_CHATS'
       AND s.status IN ('COMPLETED', 'PARTIAL')
       AND s.started_at >= $1::timestamp
       AND s.item_count > 0
     ORDER BY s.started_at DESC
"""


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2026-09-24")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--snapshot", action="append", default=[],
                    help="only these snapshot ids (repeatable)")
    ap.add_argument("--chat-batch", type=int, default=0,
                    help="chats per INSERT statement (0 = all chats of the snapshot at once)")
    ap.add_argument("--exclude-chat", action="append", default=[],
                    help="chat id to skip entirely (repeatable)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--stmt-timeout-s", type=int, default=600)
    args = ap.parse_args()

    conn = await asyncpg.connect(
        host=os.environ["DB_DIRECT_HOST"], port=int(os.environ.get("DB_PORT", "5432")),
        user=os.environ["DB_USERNAME"], password=os.environ["DB_PASSWORD"],
        database=os.environ["DB_NAME"], timeout=20,
    )
    try:
        await conn.execute(f"SET statement_timeout = {args.stmt_timeout_s * 1000}")
        if args.snapshot:
            rows = await conn.fetch(
                "SELECT s.id, r.tenant_id, r.display_name, s.started_at, s.status, s.item_count "
                "FROM snapshots s JOIN resources r ON r.id = s.resource_id "
                "WHERE s.id = ANY($1::uuid[]) ORDER BY s.started_at DESC",
                [uuid.UUID(x) for x in args.snapshot],
            )
        else:
            # asyncpg binds timestamp parameters from datetime, not str.
            rows = await conn.fetch(SNAPSHOTS_SQL, datetime.fromisoformat(args.since))
        if args.limit:
            rows = rows[: args.limit]
        print(f"[backfill] {len(rows)} snapshot(s) in scope since {args.since} (dry_run={args.dry_run})",
              flush=True)
        total_rows = 0
        for i, s in enumerate(rows, 1):
            t0 = time.monotonic()
            chats = [r["chat_id"] for r in await conn.fetch(CHAT_IDS_IN_SNAPSHOT, s["id"])
                     if r["chat_id"] not in args.exclude_chat]
            if not chats:
                print(f"[backfill] {i}/{len(rows)} {s['id']} {s['display_name']} {s['started_at']:%m-%d %H:%M} "
                      f"0 chats — skip", flush=True)
                continue
            if args.dry_run:
                print(f"[backfill] {i}/{len(rows)} {s['id']} {s['display_name']} {s['started_at']:%m-%d %H:%M} "
                      f"{len(chats)} chats — dry run", flush=True)
                continue
            inserted = 0
            step = args.chat_batch if args.chat_batch > 0 else len(chats)
            try:
                for b in range(0, len(chats), step):
                    status = await conn.execute(COPY_SQL, s["id"], s["tenant_id"], chats[b:b + step])
                    inserted += int(status.rsplit(" ", 1)[-1]) if status.startswith("INSERT") else 0
                total_rows += inserted
                print(f"[backfill] {i}/{len(rows)} {s['id']} {s['display_name']} {s['started_at']:%m-%d %H:%M} "
                      f"{len(chats)} chats -> +{inserted} rows in {time.monotonic() - t0:.1f}s", flush=True)
            except Exception as exc:  # noqa: BLE001 — keep going, report at the end
                total_rows += inserted
                print(f"[backfill] {i}/{len(rows)} {s['id']} {s['display_name']} FAILED after +{inserted} rows "
                      f"{type(exc).__name__}: {exc}", flush=True)
        print(f"[backfill] done: +{total_rows} attachment pointer rows", flush=True)
        return 0
    finally:
        await conn.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
