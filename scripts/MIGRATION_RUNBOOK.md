# Alembic 0002 — partitioning migration runbook

Apply only after every gate below passes. The migration is destructive on the three target tables (`snapshot_items`, `chat_thread_messages`, `audit_events`). Treat it like a database-format change, not a code deploy.

## Step 0 — Verify the migration even applies to your environment

```bash
python3 scripts/audit_schema.py --tables snapshot_items chat_thread_messages audit_events
```


Exit code must be `0`. Any drift means the migration's INSERT…SELECT will fail or silently drop data. Resolve drift first (manual ALTER, or skip the migration for that table).

## Step 1 — Test the rollback path against a clone

```bash
# Clones the prod schema into a throwaway DB, runs upgrade + downgrade,
# verifies row counts are preserved. Drops the throwaway DB at end.
python3 scripts/test_partition_rollback.py
```

Exit `0` = the downgrade actually works on your data shape. If it fails, **do not proceed** — fix the downgrade() body before touching prod.

## Step 2 — Take a pg_dump

```bash
BACKUP_DIR=/mnt/backups bash scripts/pg_dump_pre_migration.sh
# → exports LAST_PG_DUMP_PATH for the pre-flight check
```

Dump goes to `/mnt/backups/tmvault-pre-0002-<UTC-timestamp>.dump`, custom-format with `-Z9`. Script verifies it's restorable via `pg_restore --list` before exiting. Plan disk: at 240 TiB the dump alone is ~hours and ~TBs — schedule offline.

## Step 3 — Tune Postgres for the migration window

```sql
-- 16 GB headroom prevents checkpoint stalls on the INSERT…SELECT.
ALTER SYSTEM SET max_wal_size = '16GB';
-- Slower checkpoints — fewer mid-migration WAL flushes.
ALTER SYSTEM SET checkpoint_timeout = '30min';
SELECT pg_reload_conf();
```

Verify:

```sql
SELECT name, setting FROM pg_settings WHERE name IN ('max_wal_size', 'checkpoint_timeout');
```

## Step 4 — Drain workers

```bash
# Stop everything that writes to the partition targets.
railway service stop backup_worker
railway service stop backup_worker_heavy
railway service stop discovery_worker
railway service stop restore_worker
railway service stop dr_replication

# Confirm no leftover RUNNING/RETRYING jobs.
python3 scripts/pre_migration_check.py
```

If any check fails, the script tells you what to fix. Do not bypass.

## Step 5 — Apply

```bash
export ALEMBIC_BACKUP_TAKEN=1
export ALEMBIC_DISK_HEADROOM_OK=1
export ALEMBIC_WAL_TUNED=1
# DO NOT set ALEMBIC_FORCE_PARTITIONING — that bypasses every gate.

alembic upgrade head
```

The migration prints per-table progress. Expected wall-time at 100k rows/sec:

| Table | 100M rows | 500M rows | 1B rows |
|-------|-----------|-----------|---------|
| `snapshot_items` | ~15 min | ~85 min | ~3 h |
| `chat_thread_messages` | ~15 min | ~85 min | ~3 h |
| `audit_events` | ~1 min | ~5 min | ~10 min |

PG disk usage peaks at ~2× current table sizes during CREATE+COPY+DROP. Confirm before starting.

## Step 6 — Sanity check post-apply

```bash
psql $DATABASE_URL -c "
  SELECT
    parent.relname AS parent,
    count(*) AS partitions
  FROM pg_inherits
  JOIN pg_class parent ON parent.oid = pg_inherits.inhparent
  JOIN pg_class child ON child.oid = pg_inherits.inhrelid
  WHERE parent.relname IN ('snapshot_items', 'chat_thread_messages', 'audit_events')
  GROUP BY parent.relname;
"
```

Expected output:

```
       parent        | partitions
---------------------+------------
 snapshot_items      |         16
 chat_thread_messages |          8
 audit_events        |       N+1  (one per seeded month + the default partition)
```

Row-count sanity (should match pre-migration counts):

```bash
psql $DATABASE_URL -c "SELECT count(*) FROM snapshot_items;"
psql $DATABASE_URL -c "SELECT count(*) FROM audit_events;"
psql $DATABASE_URL -c "SELECT count(*) FROM chat_thread_messages;"
```

## Step 7 — Restart workers

```bash
railway service start backup_worker
railway service start backup_worker_heavy
railway service start discovery_worker
railway service start restore_worker
railway service start dr_replication
```

Watch the first 15 minutes of logs for FK violations or unexpected query plans:

```bash
railway logs --service backup_worker | grep -iE "(error|fk_violation|sequential scan)" | head -50
```

## Rollback procedure

If anything is wrong in Step 6:

**Option A — in-place** (slow, same cost as forward):

```bash
alembic downgrade -1
```

Takes the same wall-time as upgrade. Tested by `test_partition_rollback.py`.

**Option B — restore from pg_dump** (faster on big DBs):

```bash
dropdb $DB_NAME
createdb $DB_NAME
pg_restore -d $DB_NAME --schema=tm_vault "$LAST_PG_DUMP_PATH"
```

Loses any writes that happened after Step 2. With workers drained throughout Steps 2–7, that's zero.

## Why each gate

| Gate | Failure mode without it |
|------|------------------------|
| `ALEMBIC_BACKUP_TAKEN` | Migration bug → unrecoverable data loss |
| `ALEMBIC_DISK_HEADROOM_OK` | DB runs out of disk mid-COPY → corrupt half-state |
| `ALEMBIC_WAL_TUNED` | Checkpoint stall → app sees 30s+ query freezes |
| Drain check | In-flight INSERT on `snapshot_items` collides with table swap → lock wait → app crash |
| Schema-drift check | Source columns ≠ destination columns → INSERT…SELECT raises or silently drops data |

`ALEMBIC_FORCE_PARTITIONING=1` bypasses every gate. Reserved for greenfield demo on an empty DB. Never use in prod.
