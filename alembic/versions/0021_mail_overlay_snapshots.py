"""Overlay mail snapshots

Revision ID: 0021
Revises: 0019
Create Date: 2026-11-06

A changed mailbox's incremental owns only the rows changed since a full base snapshot
(snapshots.overlay_base_id) plus the base rows it hides (snapshot_overlay_removals); every reader
goes through snapshot_items_effective(). docs/mail-overlay-snapshots.md. Same DDL as the bootstrap
(shared.database.MAIL_OVERLAY_DDL) and scripts/2026_11_06_mail_overlay_snapshots.sql.

0020 is reserved for another branch's index migration: when it lands, re-parent this revision
onto it (down_revision = "0020").
"""

from typing import Union

from alembic import op
from shared.database import MAIL_OVERLAY_DDL

revision: str = "0021"
down_revision: Union[str, None] = "0020"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for ddl in MAIL_OVERLAY_DDL:
        op.execute(ddl)


def downgrade() -> None:
    # An overlay's inventory needs its base rows and removals: refuse rather than strand it.
    op.execute("""DO $$ BEGIN
        IF EXISTS (SELECT 1 FROM snapshots WHERE overlay_base_id IS NOT NULL) THEN
            RAISE EXCEPTION 'overlay snapshots exist; materialise them before downgrading 0021';
        END IF; END $$""")
    op.execute("DROP TRIGGER IF EXISTS snapshots_overlay_validate ON snapshots")
    op.execute("DROP FUNCTION IF EXISTS validate_snapshot_overlay_base()")
    op.execute("DROP FUNCTION IF EXISTS snapshot_items_effective(uuid)")
    op.execute("DROP TABLE IF EXISTS snapshot_overlay_removals")
    op.execute("ALTER TABLE snapshots DROP COLUMN IF EXISTS overlay_base_id")
