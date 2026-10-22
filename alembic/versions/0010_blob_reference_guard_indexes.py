"""blob reference guard indexes

Revision ID: 0010
Revises: 0009
Create Date: 2026-07-31

Renumbered 2026-07-31: this file previously carried the id `20260731_0007` and
chained behind `down_revision="0007"` — an id that meant the Tier-2 unique
index on the `integrate/archive` lineage but `resource_protected_bytes` on
`integrate/perf`. Against the single shared database that resolves to two
different migrations, so the whole pending set moved onto one linear chain
(0006 -> 0007 -> 0008 -> 0009 -> 0010) that BOTH lineages carry in full. See
tests/shared/test_alembic_revision_chain.py.

Indexes the cancel/teardown blob reference guard
(``shared/backup_cleanup.py::select_deletable_blob_paths``) needs.

Two lookups have no index today:

  1. ``snapshot_items (blob_path)`` — the guard's exact-path fallback
     for candidates that arrive with no ``content_checksum``. The
     primary sharer probe deliberately drives off
     ``idx_snapshot_items_tenant_checksum`` instead, and the fallback is
     budget-capped (paths past the budget are RETAINED, never deleted
     unverified), so the guard is correct without this index — it is
     here to make the residue affordable and to give a future
     orphan-blob GC a usable access path.
  2. ``chat_url_cache (blob_path)`` — the guard must clear cache rows
     that point at a blob it is about to delete, otherwise a FUTURE
     backup stamps a new ``snapshot_items`` row onto a dead path
     (the bulk hit path in workers/backup-worker/main.py ~9855 reuses
     ``blob_path`` with no existence probe). The UPDATE is
     tenant-scoped, so pre-index it degrades to a scan of one tenant's
     cache slice per batch rather than a whole-table scan.

!! NOT APPLIED IN PROD BY THIS REVISION AS WRITTEN !!
``snapshot_items`` is a PLAIN table in production (verified 2026-07-31:
relkind='r', zero partitions, ~87GB, PK btree(id) alone — the partitioning
migration on the 20260517_0001 chain was never applied; alembic_version=0006).
There is therefore no per-partition attach dance, and no partition pruning to
reason about.

These indexes only make the blob reference guard FASTER. The guard is correct
without them, so this migration never trades availability for speed: it builds
the small chat_url_cache index inline and SKIPS the snapshot_items one on any
populated database, printing the runbook below.

OPERATOR RUNBOOK for a populated cluster (run from psql, outside any
transaction — CONCURRENTLY cannot run inside one):

    CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_snapshot_items_blob_path
        ON snapshot_items (blob_path) WHERE blob_path IS NOT NULL;

Then verify it came out valid:

    SELECT indexrelid::regclass, indisvalid
      FROM pg_index WHERE indexrelid = 'ix_snapshot_items_blob_path'::regclass;

A failed CONCURRENTLY build leaves an INVALID index that PG will not use and
that still costs write overhead — DROP INDEX and retry if indisvalid is false.
Expect a long build on ~87GB; it does not block reads or writes.
"""

from typing import Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0010"
down_revision: Union[str, None] = "0009"  # tier2 unique index; linearized 2026-07-31
branch_labels = None
depends_on = None

# Guard: above this row count, building the snapshot_items index inside a
# migration transaction would hold ShareLock and block every INSERT for the
# whole build. Production is ~87GB / tens of millions of rows, so it lands
# far above this and must use the CONCURRENTLY runbook instead.
SNAPSHOT_ITEMS_INLINE_BUILD_MAX_ROWS = 1_000_000


def upgrade() -> None:
    """Create the two blob_path lookup indexes, but ONLY where doing it
    inline is safe.

    These indexes are a PERFORMANCE aid for the blob reference guard; the
    guard is correct without them. That asymmetry is why this migration
    refuses to do damage: on a populated cluster a non-CONCURRENT
    CREATE INDEX on snapshot_items takes ShareLock and blocks every insert
    (i.e. every running backup) until it finishes. A boot-time/inline DDL
    stall of exactly this shape froze production for ~5 minutes before.

    chat_url_cache is small and tenant-scoped, so it is always built inline.
    snapshot_items is built inline only on an empty/small database (fresh
    dev, CI, or a DR rebuild); otherwise it is SKIPPED with a loud notice and
    must be created out-of-band with CREATE INDEX CONCURRENTLY.
    """
    conn = op.get_bind()

    # Small, safe, and needed by the guard's cache-invalidation path.
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_chat_url_cache_blob_path "
        "ON chat_url_cache (blob_path) "
        "WHERE blob_path IS NOT NULL"
    )

    est = conn.exec_driver_sql(
        "SELECT COALESCE(reltuples, -1)::bigint "
        "FROM pg_class WHERE relname = 'snapshot_items'"
    ).scalar()
    if est is None:
        est = -1

    if 0 <= est <= SNAPSHOT_ITEMS_INLINE_BUILD_MAX_ROWS:
        op.execute(
            "CREATE INDEX IF NOT EXISTS ix_snapshot_items_blob_path "
            "ON snapshot_items (blob_path) "
            "WHERE blob_path IS NOT NULL"
        )
        return

    print(
        "[alembic 0010] SKIPPED ix_snapshot_items_blob_path: "
        f"snapshot_items has ~{est} rows (> {SNAPSHOT_ITEMS_INLINE_BUILD_MAX_ROWS}). "
        "An inline build would ShareLock the table and stall every running "
        "backup. Create it out-of-band instead, from a psql session:\n"
        "  CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_snapshot_items_blob_path\n"
        "    ON snapshot_items (blob_path) WHERE blob_path IS NOT NULL;\n"
        "then verify with: SELECT indexrelid::regclass, indisvalid "
        "FROM pg_index WHERE indexrelid = 'ix_snapshot_items_blob_path'::regclass;\n"
        "(If indisvalid is false, DROP INDEX and re-run — a failed "
        "CONCURRENTLY build leaves an INVALID index behind.)",
        flush=True,
    )


def downgrade() -> None:
    # snapshot_items is a PLAIN table in prod (relkind='r', zero partitions),
    # so there are no per-partition child indexes to unwind — the previous
    # `for i in range(SNAPSHOT_ITEMS_PARTITIONS)` loop was a leftover of the
    # false partitioning premise and raised NameError, i.e. this downgrade
    # could not run at all. Both DROPs are IF EXISTS, so the downgrade is
    # correct whether upgrade() built ix_snapshot_items_blob_path inline or
    # skipped it for the CONCURRENTLY runbook.
    op.execute("DROP INDEX IF EXISTS ix_chat_url_cache_blob_path")
    op.execute("DROP INDEX IF EXISTS ix_snapshot_items_blob_path")
