"""unique Tier-2 child per (tenant, type, parent)

Revision ID: 0009
Revises: 0008
Create Date: 2026-07-31

Renumbered 2026-07-31: this file was authored as `0007` on the
`integrate/archive` lineage while `integrate/perf` independently authored a
DIFFERENT `0007` (resource_protected_bytes). One shared database cannot take
two revisions with one id, so the archive pair moved to 0009/0010 behind
perf's 0007/0008 and BOTH lineages now carry the full chain. See
tests/shared/test_alembic_revision_chain.py for the contract.

`ensure_tier2_children` (shared/tier2_discovery.py) is a check-then-INSERT and
FOUR callers can run it concurrently for the same user (tenant-service
/discover-content, tenant-service backup-now, the discovery-worker
discovery.tier2 consumer, the resource-service SLA hook). Without a unique
index the losers of that race create duplicate USER_* children.

Partial + UNIQUE on (tenant_id, type, parent_resource_id) for the six Tier-2
types only:
  * Tier-1 rows (parent_resource_id IS NULL) are untouched, and NULLs are
    distinct in a Postgres unique index anyway.
  * scoping to the six types leaves room for a future child type that is
    legitimately multi-row per parent (e.g. per-shard rows).

The application code is safe both WITH and WITHOUT this index: the lookup
tolerates duplicates (deterministic oldest survivor, extras preserved) and
the INSERT runs inside a SAVEPOINT so the loser adopts the winner's row.

Duplicates must be reconciled BEFORE this index can be built — the upgrade
refuses to run while any exist rather than deleting rows that may own
snapshots. Operator pre-check:

    SELECT tenant_id, parent_resource_id, type, count(*) AS n,
           array_agg(id ORDER BY created_at NULLS LAST, id) AS ids
      FROM resources
     WHERE parent_resource_id IS NOT NULL
       AND type IN ('USER_MAIL','USER_ONEDRIVE','USER_CONTACTS',
                    'USER_CALENDAR','USER_CHATS','USER_ARCHIVE')
     GROUP BY 1,2,3
    HAVING count(*) > 1;

Keep the first id in each `ids` array (that is the row the application
refreshes); re-point or retire the others deliberately — check snapshots
first (SELECT count(*) FROM snapshots WHERE resource_id = <dup id>).
"""

from typing import Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "0009"
down_revision: Union[str, None] = "0008"
branch_labels = None
depends_on = None


_TIER2_TYPES = (
    "'USER_MAIL','USER_ONEDRIVE','USER_CONTACTS',"
    "'USER_CALENDAR','USER_CHATS','USER_ARCHIVE'"
)

_INDEX_PREDICATE = (
    f"parent_resource_id IS NOT NULL AND type IN ({_TIER2_TYPES})"
)


def upgrade() -> None:
    conn = op.get_bind()
    dupes = conn.execute(
        sa.text(
            f"""
            SELECT count(*) FROM (
                SELECT 1
                  FROM resources
                 WHERE {_INDEX_PREDICATE}
                 GROUP BY tenant_id, type, parent_resource_id
                HAVING count(*) > 1
            ) d
            """
        )
    ).scalar()
    if dupes:
        raise RuntimeError(
            f"{dupes} (tenant_id, type, parent_resource_id) group(s) still hold "
            "duplicate Tier-2 children. Reconcile them first (see this "
            "revision's docstring for the pre-check query) — this migration "
            "will not delete resource rows, they may own snapshots."
        )

    # CONCURRENTLY: resources is large and hot; a plain CREATE UNIQUE INDEX
    # would hold ACCESS EXCLUSIVE for the whole build.
    with op.get_context().autocommit_block():
        op.execute(
            "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS "
            "uq_resource_tier2_child_per_parent "
            "ON resources (tenant_id, type, parent_resource_id) "
            f"WHERE {_INDEX_PREDICATE}"
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            "DROP INDEX CONCURRENTLY IF EXISTS uq_resource_tier2_child_per_parent"
        )
