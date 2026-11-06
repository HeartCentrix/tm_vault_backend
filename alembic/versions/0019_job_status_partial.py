"""PARTIAL job status

Revision ID: 0019
Revises: 0018
Create Date: 2026-11-06

Adds the PARTIAL jobstatus value: a backup job whose snapshots finished with
some PARTIAL/FAILED results (the activity feed's "Done with warnings") is
PARTIAL instead of COMPLETED. TM log issue #35.
"""

from typing import Union

from alembic import op

revision: str = "0019"
down_revision: Union[str, None] = "0018"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ADD VALUE cannot run inside a transaction block on older PostgreSQL.
    with op.get_context().autocommit_block():
        op.execute("ALTER TYPE jobstatus ADD VALUE IF NOT EXISTS 'PARTIAL'")


def downgrade() -> None:
    pass  # PG enums can't drop values; harmless leftover
