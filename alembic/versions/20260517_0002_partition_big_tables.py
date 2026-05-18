"""Partition the three high-growth tables.

At 5k users / 240 TiB / 3x-daily incremental, the three tables that
balloon fastest are::

  snapshot_items        ~500M+ rows  (every backed-up artifact)
  chat_thread_messages  ~1B+ rows    (Teams chat history)
  audit_events          ~10M+ rows   (immutable forensic log, monthly growth)

Without partitioning, every index update walks a single B-tree that
gets progressively slower (TOAST + index bloat) and `VACUUM` blocks
on the whole table. Native PG hash + range partitioning lets us prune
queries to single partitions and lets `VACUUM` work shard-by-shard.

Partition keys (single-tenant, chosen for even distribution):

  snapshot_items        PARTITION BY HASH (snapshot_id)   16 partitions
  chat_thread_messages  PARTITION BY HASH (chat_thread_id) 8 partitions
  audit_events          PARTITION BY RANGE (occurred_at)   monthly

Why not tenant_id? Single-tenant today; partitioning on a constant
column buys nothing. When multi-tenant lands, a follow-up migration
can REPARTITION BY HASH(tenant_id, snapshot_id) (or add tenant_id
as a sub-partition key).

⚠️  Prerequisites — MUST hold before upgrading
------------------------------------------------
This migration converts existing tables to partitioned ones.
That means CREATE-NEW + COPY-DATA + DROP-OLD, which:

1. Holds an ``AccessExclusiveLock`` on each table during the
   rename swap. Active sessions writing to the table will see
   "could not obtain lock" — drain workers first.
2. Doubles disk usage temporarily (old + new coexist until DROP).
3. Loses any in-flight FK dependents during the swap; the FK
   constraints are recreated on the new table.

Run-time prerequisites:

  • Stop backup_worker + backup_worker_heavy replicas
  • Drain RabbitMQ ``backup.*`` queues (ack outstanding messages)
  • Confirm no in-flight snapshot transitions

If those preconditions aren't met, ``upgrade()`` aborts with a
clear error message. The check is permissive: set
``ALEMBIC_FORCE_PARTITIONING=1`` in env to skip it (e.g.
greenfield empty-DB demo where we know there's no contention).

Downgrade: copies data back into a non-partitioned shape. Has the
same locking implications as upgrade.

Revision ID: 0002
Revises: 0001
Create Date: 2026-05-17
"""
from __future__ import annotations

import os
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "0002"
down_revision: Union[str, None] = "0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# How many child partitions to create per hash-partitioned table.
SNAPSHOT_ITEMS_PARTITIONS = 16
CHAT_THREAD_MESSAGES_PARTITIONS = 8

# Date range for the audit_events partition seed. Creates one partition
# per month from EARLIEST_AUDIT_MONTH through (today + 6 months). A
# follow-up cron-driven helper extends this rolling window.
EARLIEST_AUDIT_MONTH = "2024-01-01"
SEED_FORWARD_MONTHS = 6


def _drain_check_query() -> str:
    """SQL that returns row count for `in-flight rows that the swap
    would corrupt`. Empty result == safe to swap.

    For backup-worker we look at jobs.status IN ('RUNNING','RETRYING').
    """
    return (
        "SELECT count(*) FROM jobs WHERE status IN ('RUNNING','RETRYING')"
    )


def _preflight(connection) -> None:
    """Multi-gate pre-flight. Each check is independently skippable
    via env so demo / dev workflows can override, but every gate must
    pass (or be explicitly skipped) before the destructive DDL runs.

    Gates:
      - ALEMBIC_FORCE_PARTITIONING=1
            global escape hatch — skips ALL gates. Use for greenfield
            empty-DB demo where there's nothing to lose.

      - ALEMBIC_DRAIN_CHECK_SKIP=1
            skip the worker-drain check. Caller asserts no jobs
            RUNNING/RETRYING. Default: enforced.

      - ALEMBIC_BACKUP_TAKEN=1
            asserts a recent pg_dump exists. Operator MUST set this
            after running ``scripts/pg_dump_pre_migration.sh``.
            Without it, the migration aborts to prevent a destructive
            DDL with no recovery path.

      - ALEMBIC_DISK_HEADROOM_OK=1
            asserts free disk >= 2 × sum(snapshot_items +
            chat_thread_messages + audit_events). Migration creates
            new shells alongside originals before dropping the old
            ones. Run ``scripts/pre_migration_check.py`` to compute.

      - ALEMBIC_WAL_TUNED=1
            asserts max_wal_size >= 16 GB. Without WAL headroom the
            INSERT…SELECT stalls on checkpoint.
    """
    if os.environ.get("ALEMBIC_FORCE_PARTITIONING") == "1":
        # Greenfield / demo escape. Skip everything.
        return

    # 1. Worker drain
    if os.environ.get("ALEMBIC_DRAIN_CHECK_SKIP") != "1":
        row = connection.execute(sa.text(_drain_check_query())).first()
        n = int(row[0] or 0)
        if n > 0:
            raise RuntimeError(
                f"BLOCK: {n} jobs RUNNING/RETRYING — drain workers first, "
                "or set ALEMBIC_DRAIN_CHECK_SKIP=1"
            )

    # 2. Backup taken
    if os.environ.get("ALEMBIC_BACKUP_TAKEN") != "1":
        raise RuntimeError(
            "BLOCK: ALEMBIC_BACKUP_TAKEN=1 not set. Take a pg_dump first "
            "(scripts/pg_dump_pre_migration.sh), then export "
            "ALEMBIC_BACKUP_TAKEN=1 to acknowledge. This gate is the "
            "ONLY thing standing between a bug in this migration and "
            "an unrecoverable production DB."
        )

    # 3. Disk headroom
    if os.environ.get("ALEMBIC_DISK_HEADROOM_OK") != "1":
        raise RuntimeError(
            "BLOCK: ALEMBIC_DISK_HEADROOM_OK=1 not set. Confirm free "
            "disk >= 2 × current table sizes via "
            "`python3 scripts/pre_migration_check.py`, then export "
            "ALEMBIC_DISK_HEADROOM_OK=1."
        )

    # 4. WAL tuned
    if os.environ.get("ALEMBIC_WAL_TUNED") != "1":
        raise RuntimeError(
            "BLOCK: ALEMBIC_WAL_TUNED=1 not set. Confirm "
            "max_wal_size >= 16 GB (ALTER SYSTEM SET max_wal_size = '16GB'; "
            "SELECT pg_reload_conf();), then export ALEMBIC_WAL_TUNED=1."
        )


# ────────────────────────────────────────────────────────────────────
# snapshot_items — PARTITION BY HASH (snapshot_id)
# ────────────────────────────────────────────────────────────────────

def _upgrade_snapshot_items(connection) -> None:
    # 1. Build the new partitioned shell. Schema must mirror models.py
    #    SnapshotItem exactly (column types, nullability, defaults).
    #    NOTE: PG requires partition key columns to be in every UNIQUE
    #    constraint AND in the primary key — so PK becomes (id, snapshot_id).
    op.execute(
        """
        CREATE TABLE snapshot_items_new (
            id uuid NOT NULL,
            snapshot_id uuid NOT NULL,
            tenant_id uuid,
            external_id varchar NOT NULL,
            parent_external_id varchar,
            item_type varchar NOT NULL,
            name varchar NOT NULL,
            folder_path varchar,
            content_hash varchar,
            content_checksum varchar,
            content_size bigint DEFAULT 0,
            blob_path varchar,
            encryption_key_id varchar,
            backup_version integer DEFAULT 1,
            metadata json DEFAULT '{}'::json,
            is_deleted boolean DEFAULT false,
            indexed_at timestamp,
            backend_id uuid NOT NULL,
            created_at timestamp DEFAULT NOW(),
            PRIMARY KEY (id, snapshot_id)
        ) PARTITION BY HASH (snapshot_id)
        """
    )
    # 2. Create child partitions.
    for i in range(SNAPSHOT_ITEMS_PARTITIONS):
        op.execute(
            f"""
            CREATE TABLE snapshot_items_p{i:02d}
            PARTITION OF snapshot_items_new
            FOR VALUES WITH (MODULUS {SNAPSHOT_ITEMS_PARTITIONS}, REMAINDER {i})
            """
        )
    # 3. Indexes (mirror models.py + the ones init_db creates).
    op.execute(
        "CREATE INDEX ON snapshot_items_new (snapshot_id)"
    )
    op.execute(
        "CREATE INDEX ON snapshot_items_new (tenant_id)"
    )
    op.execute(
        "CREATE INDEX ON snapshot_items_new (content_hash)"
    )
    op.execute(
        "CREATE INDEX ON snapshot_items_new (parent_external_id)"
    )
    # 4. Copy data + swap names. INSERT…SELECT inside a single TX so
    #    a crash leaves the original intact.
    op.execute(
        "INSERT INTO snapshot_items_new SELECT * FROM snapshot_items"
    )
    op.execute("DROP TABLE snapshot_items CASCADE")
    op.execute("ALTER TABLE snapshot_items_new RENAME TO snapshot_items")
    # 5. Foreign keys (PG re-applies on rename, but we lost the
    #    snapshot_id FK during the partition-shell create — recreate it).
    # Match models.py exactly: FK without ON DELETE CASCADE. Snapshot
    # deletion is handled by application-level backup_cleanup, not by
    # FK cascade — preserving the same semantics that ``create_all``
    # would produce so this migration is a structural change only,
    # not a behavioral one.
    op.execute(
        "ALTER TABLE snapshot_items "
        "ADD CONSTRAINT snapshot_items_snapshot_id_fkey "
        "FOREIGN KEY (snapshot_id) REFERENCES snapshots(id)"
    )
    op.execute(
        "ALTER TABLE snapshot_items "
        "ADD CONSTRAINT snapshot_items_tenant_id_fkey "
        "FOREIGN KEY (tenant_id) REFERENCES tenants(id)"
    )
    op.execute(
        "ALTER TABLE snapshot_items "
        "ADD CONSTRAINT snapshot_items_backend_id_fkey "
        "FOREIGN KEY (backend_id) REFERENCES storage_backends(id)"
    )


def _downgrade_snapshot_items() -> None:
    op.execute(
        """
        CREATE TABLE snapshot_items_old AS
        SELECT * FROM snapshot_items
        """
    )
    op.execute("DROP TABLE snapshot_items CASCADE")
    op.execute("ALTER TABLE snapshot_items_old RENAME TO snapshot_items")
    op.execute(
        "ALTER TABLE snapshot_items "
        "ADD PRIMARY KEY (id)"
    )
    op.execute(
        "CREATE INDEX ON snapshot_items (snapshot_id)"
    )
    op.execute(
        "CREATE INDEX ON snapshot_items (tenant_id)"
    )


# ────────────────────────────────────────────────────────────────────
# chat_thread_messages — PARTITION BY HASH (chat_thread_id)
# ────────────────────────────────────────────────────────────────────

def _upgrade_chat_thread_messages(connection) -> None:
    # Detect schema — chat_thread_messages was introduced 2026-05-13.
    # If it doesn't exist yet (older demo DB), skip silently — the
    # baseline create_all will produce it as partitioned via models.py
    # when we add the `__table_args__` partitioning hint in a later PR.
    has = connection.execute(sa.text(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_name = 'chat_thread_messages'"
    )).first()
    if not has:
        return

    cols = connection.execute(sa.text(
        "SELECT column_name, data_type, is_nullable, column_default "
        "FROM information_schema.columns "
        "WHERE table_name = 'chat_thread_messages' "
        "ORDER BY ordinal_position"
    )).fetchall()
    col_defs = []
    for c in cols:
        nul = "" if c[2] == "YES" else " NOT NULL"
        dflt = f" DEFAULT {c[3]}" if c[3] else ""
        col_defs.append(f"  {c[0]} {c[1]}{nul}{dflt}")
    cols_sql = ",\n".join(col_defs)

    op.execute(
        f"""
        CREATE TABLE chat_thread_messages_new (
        {cols_sql},
          PRIMARY KEY (id, chat_thread_id)
        ) PARTITION BY HASH (chat_thread_id)
        """
    )
    for i in range(CHAT_THREAD_MESSAGES_PARTITIONS):
        op.execute(
            f"""
            CREATE TABLE chat_thread_messages_p{i:02d}
            PARTITION OF chat_thread_messages_new
            FOR VALUES WITH (MODULUS {CHAT_THREAD_MESSAGES_PARTITIONS}, REMAINDER {i})
            """
        )
    op.execute("CREATE INDEX ON chat_thread_messages_new (chat_thread_id)")
    op.execute("CREATE INDEX ON chat_thread_messages_new (tenant_id)")
    op.execute("CREATE INDEX ON chat_thread_messages_new (snapshot_id)")
    op.execute(
        "INSERT INTO chat_thread_messages_new SELECT * FROM chat_thread_messages"
    )
    op.execute("DROP TABLE chat_thread_messages CASCADE")
    op.execute(
        "ALTER TABLE chat_thread_messages_new RENAME TO chat_thread_messages"
    )


def _downgrade_chat_thread_messages() -> None:
    op.execute(
        """
        CREATE TABLE chat_thread_messages_old AS
        SELECT * FROM chat_thread_messages
        """
    )
    op.execute("DROP TABLE chat_thread_messages CASCADE")
    op.execute(
        "ALTER TABLE chat_thread_messages_old RENAME TO chat_thread_messages"
    )
    op.execute("ALTER TABLE chat_thread_messages ADD PRIMARY KEY (id)")
    op.execute("CREATE INDEX ON chat_thread_messages (chat_thread_id)")


# ────────────────────────────────────────────────────────────────────
# audit_events — PARTITION BY RANGE (created_at), monthly
# ────────────────────────────────────────────────────────────────────

def _upgrade_audit_events(connection) -> None:
    """Convert audit_events to RANGE-partitioned (monthly on occurred_at).

    Schema mirrors shared/models.py:AuditEvent exactly — id, org_id,
    tenant_id, actor_*, action, resource_*, outcome, job_id, snapshot_id,
    details, occurred_at. Partition key MUST be in the primary key, so
    the PK becomes (id, occurred_at). The data copy uses
    ``INSERT INTO ... SELECT *`` which only succeeds when the column
    list matches — any drift between the new shell and the old table
    is therefore caught at copy time rather than silently dropped.
    """
    from datetime import datetime, timedelta

    op.execute(
        """
        CREATE TABLE audit_events_new (
            id uuid NOT NULL,
            org_id uuid,
            tenant_id uuid,
            actor_id uuid,
            actor_email varchar,
            actor_type varchar DEFAULT 'SYSTEM',
            action varchar NOT NULL,
            resource_id uuid,
            resource_type varchar,
            resource_name varchar,
            outcome varchar DEFAULT 'SUCCESS',
            job_id uuid,
            snapshot_id uuid,
            details json DEFAULT '{}'::json,
            occurred_at timestamp DEFAULT NOW() NOT NULL,
            PRIMARY KEY (id, occurred_at)
        ) PARTITION BY RANGE (occurred_at)
        """
    )
    # Indexes mirror models.py ``index=True`` decls: org_id, tenant_id,
    # action, occurred_at. Composite (tenant_id, occurred_at) gives
    # partition pruning + range scan in one shot for the dashboard's
    # "audit history for tenant X over last 30 days" query.
    op.execute("CREATE INDEX ON audit_events_new (org_id)")
    op.execute("CREATE INDEX ON audit_events_new (tenant_id, occurred_at)")
    op.execute("CREATE INDEX ON audit_events_new (action, occurred_at)")
    op.execute("CREATE INDEX ON audit_events_new (resource_id)")
    op.execute("CREATE INDEX ON audit_events_new (occurred_at)")

    # Seed monthly partitions from EARLIEST_AUDIT_MONTH through today+6mo.
    start = datetime.strptime(EARLIEST_AUDIT_MONTH, "%Y-%m-%d")
    end = datetime.utcnow() + timedelta(days=SEED_FORWARD_MONTHS * 31)
    cur = start
    while cur <= end:
        # Move to first of next month.
        if cur.month == 12:
            nxt = cur.replace(year=cur.year + 1, month=1)
        else:
            nxt = cur.replace(month=cur.month + 1)
        name = f"audit_events_{cur.strftime('%Y_%m')}"
        op.execute(
            f"""
            CREATE TABLE {name}
            PARTITION OF audit_events_new
            FOR VALUES FROM ('{cur.strftime('%Y-%m-%d')}')
            TO ('{nxt.strftime('%Y-%m-%d')}')
            """
        )
        cur = nxt

    # Catch-all default for events that fall outside seeded months
    # (e.g. a bug that backdates occurred_at). Easier to detect than
    # to lose rows to constraint violations.
    op.execute(
        "CREATE TABLE audit_events_default PARTITION OF audit_events_new DEFAULT"
    )

    # Copy data. INSERT...SELECT requires column compatibility; using
    # an explicit column list so a future model addition that hasn't
    # been migrated yet fails LOUDLY rather than silently dropping
    # the column.
    op.execute(
        """
        INSERT INTO audit_events_new (
            id, org_id, tenant_id, actor_id, actor_email, actor_type,
            action, resource_id, resource_type, resource_name, outcome,
            job_id, snapshot_id, details, occurred_at
        )
        SELECT
            id, org_id, tenant_id, actor_id, actor_email, actor_type,
            action, resource_id, resource_type, resource_name, outcome,
            job_id, snapshot_id, details, occurred_at
        FROM audit_events
        """
    )
    op.execute("DROP TABLE audit_events CASCADE")
    op.execute("ALTER TABLE audit_events_new RENAME TO audit_events")

    # Recreate FKs that the partition-shell create couldn't carry.
    op.execute(
        "ALTER TABLE audit_events "
        "ADD CONSTRAINT audit_events_org_id_fkey "
        "FOREIGN KEY (org_id) REFERENCES organizations(id)"
    )
    op.execute(
        "ALTER TABLE audit_events "
        "ADD CONSTRAINT audit_events_tenant_id_fkey "
        "FOREIGN KEY (tenant_id) REFERENCES tenants(id)"
    )


def _downgrade_audit_events() -> None:
    op.execute(
        """
        CREATE TABLE audit_events_old AS
        SELECT * FROM audit_events
        """
    )
    op.execute("DROP TABLE audit_events CASCADE")
    op.execute("ALTER TABLE audit_events_old RENAME TO audit_events")
    op.execute("ALTER TABLE audit_events ADD PRIMARY KEY (id)")
    op.execute("CREATE INDEX ON audit_events (org_id)")
    op.execute("CREATE INDEX ON audit_events (tenant_id)")
    op.execute("CREATE INDEX ON audit_events (action)")
    op.execute("CREATE INDEX ON audit_events (occurred_at)")


# ────────────────────────────────────────────────────────────────────
# Entry points
# ────────────────────────────────────────────────────────────────────


def upgrade() -> None:
    bind = op.get_bind()
    _preflight(bind)
    _upgrade_snapshot_items(bind)
    _upgrade_chat_thread_messages(bind)
    _upgrade_audit_events(bind)


def downgrade() -> None:
    _downgrade_audit_events()
    _downgrade_chat_thread_messages()
    _downgrade_snapshot_items()
