"""SKIPPED snapshot status

Revision ID: 0022
Revises: 0020 (re-chain onto 0021 when the mail-overlay revision merges)
Create Date: 2026-11-06

Adds the SKIPPED snapshotstatus value. A job that finds its resource already
being backed up by another job writes a placeholder snapshot; it was COMPLETED
with 0 items and extra_data.skipped_concurrent=true, now it is SKIPPED (terminal,
never a restore point). TM log issue #1. Existing placeholders are backfilled.
Railway (no alembic) runs scripts/2026_11_06_snapshot_status_skipped.sql.
"""

from typing import Union

from alembic import op

revision: str = "0022"
down_revision: Union[str, None] = "0021"
branch_labels = None
depends_on = None


# Literal SQL: a migration must not change when application code does.
BACKFILL_SQL = """
    UPDATE snapshots SET status = 'SKIPPED'
     WHERE status = 'COMPLETED'
       AND COALESCE(json_extract_path_text(extra_data, 'skipped_concurrent'), 'false') IN ('true', 't', '1')
"""
# Code before 0022 cannot read SKIPPED; the placeholder was COMPLETED (flag kept) to that code.
DOWNGRADE_SQL = "UPDATE snapshots SET status = 'COMPLETED' WHERE status = 'SKIPPED'"


def upgrade() -> None:
    # ADD VALUE cannot run inside a transaction block on older PostgreSQL, and the value
    # must be committed before the backfill can use it.
    with op.get_context().autocommit_block():
        op.execute("ALTER TYPE snapshotstatus ADD VALUE IF NOT EXISTS 'SKIPPED'")
    op.execute(BACKFILL_SQL)


def downgrade() -> None:
    op.execute(DOWNGRADE_SQL)  # PG enums can't drop values; the unused value stays
