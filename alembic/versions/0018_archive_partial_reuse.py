"""Reuse rule: an archive may point at a PARTIAL prior when its only gaps are the same Microsoft-side
folders (marker archive_reused_partial_prior). Same function text as the bootstrap (shared/database.py)."""

from alembic import op
from shared.database import SNAPSHOT_REUSE_VALIDATE_FUNCTION

revision = "0018"
down_revision = "0017"
branch_labels = None
depends_on = None


def upgrade():
    op.execute(SNAPSHOT_REUSE_VALIDATE_FUNCTION)


def downgrade():
    op.execute(SNAPSHOT_REUSE_VALIDATE_FUNCTION.replace(
        "OR (p.status = 'PARTIAL'::snapshotstatus\n"
        "                         AND COALESCE(NEW.extra_data::jsonb, '{}'::jsonb) ? 'archive_reused_partial_prior')",
        "OR FALSE"))
