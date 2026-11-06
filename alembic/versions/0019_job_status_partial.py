"""PARTIAL job status

Revision ID: 0019
Revises: 0018
Create Date: 2026-11-06

Adds the PARTIAL jobstatus value: a backup job whose snapshots finished with
some PARTIAL/FAILED results (the activity feed's "Done with warnings") is
PARTIAL instead of COMPLETED. TM log issue #35. Existing jobs are backfilled.
"""

from typing import Union

from alembic import op

revision: str = "0019"
down_revision: Union[str, None] = "0018"
branch_labels = None
depends_on = None


# Finished backups that left a PARTIAL or FAILED snapshot become PARTIAL, the rule the finalizers
# now apply (shared.batch_rollup.job_partial_sql). Literal SQL: a migration must not change when
# that helper does.
BACKFILL_SQL = """
    UPDATE jobs j SET status = 'PARTIAL'
     WHERE j.status = 'COMPLETED'
       AND EXISTS (SELECT 1 FROM snapshots ps WHERE ps.job_id = j.id
                    AND ps.status::text IN ('PARTIAL','FAILED'))
"""
# Code before 0019 cannot read PARTIAL; it was COMPLETED to that code.
DOWNGRADE_SQL = "UPDATE jobs SET status = 'COMPLETED' WHERE status = 'PARTIAL'"


def upgrade() -> None:
    # ADD VALUE cannot run inside a transaction block on older PostgreSQL, and the value
    # must be committed before the backfill can use it.
    with op.get_context().autocommit_block():
        op.execute("ALTER TYPE jobstatus ADD VALUE IF NOT EXISTS 'PARTIAL'")
    op.execute(BACKFILL_SQL)


def downgrade() -> None:
    op.execute(DOWNGRADE_SQL)  # PG enums can't drop values; the unused value stays
