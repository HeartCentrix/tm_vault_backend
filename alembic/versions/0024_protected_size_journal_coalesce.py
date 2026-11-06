"""Protected-size capture: one transaction's per-row statements share journal rows

Revision ID: 0024
Revises: 0023
Create Date: 2026-11-06

ORM add_all reaches asyncpg as executemany, one statement (and one statement-level
trigger fire) per row: each inserted snapshot_items row became its own journal row,
route row, target row and worker transaction. The capture function now extends the
transaction's own uncommitted journal row up to the 1000-id cap.
Railway (no alembic) runs scripts/2026_11_06_protected_size_journal_coalesce.sql.
"""

from alembic import op
from sqlalchemy import text

from shared.protected_size_schema import capture_function

revision = "0024"
down_revision = "0023"
branch_labels = None
depends_on = None

# Every role that may append journal rows may now extend its own transaction's row.
GRANT_SQL = """DO $fn$ DECLARE r text; BEGIN
  FOR r IN SELECT DISTINCT grantee FROM information_schema.role_table_grants
    WHERE table_schema=current_schema() AND table_name='protected_size_journal' AND privilege_type='INSERT'
  LOOP EXECUTE pg_catalog.format('GRANT UPDATE ON protected_size_journal TO %I',r); END LOOP;
END $fn$"""


def upgrade():
    schema = op.get_bind().scalar(text("SELECT current_schema()"))
    op.execute(capture_function(schema))
    op.execute(GRANT_SQL)


def downgrade():
    raise RuntimeError("Protected-size journal is durable; forward migration required")
