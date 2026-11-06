"""Archive overlay snapshots

Revision ID: 0023
Revises: 0022
Create Date: 2026-11-06

snapshot_items_effective() also inherits ARCHIVE_ITEM / ARCHIVE_ATTACHMENT rows of an overlay's base, so
a changed in-place archive can be an overlay instead of copying every unchanged row (shared.archive_capture
ArchiveCapture._overlay). CREATE OR REPLACE of the shared definition (shared.database); Railway (no alembic)
re-applies scripts/2026_11_06_mail_overlay_snapshots.sql. Until then the archive overlay stays off (the
builder checks the installed body) and archives copy as before.

Numbered after the highest revision on this branch; may need renumbering at merge.
"""

from typing import Union

from alembic import op
from shared.database import SNAPSHOT_ITEMS_EFFECTIVE_FUNCTION

revision: str = "0023"
down_revision: Union[str, None] = "0022"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(SNAPSHOT_ITEMS_EFFECTIVE_FUNCTION)


def downgrade() -> None:
    # The mail-only body would drop archive overlays' inherited rows: refuse while any exist.
    op.execute("""DO $$ BEGIN
        IF EXISTS (SELECT 1 FROM snapshots s JOIN snapshot_items b ON b.snapshot_id = s.overlay_base_id
                    WHERE b.item_type = 'ARCHIVE_ITEM' LIMIT 1) THEN
            RAISE EXCEPTION 'archive overlay snapshots exist; materialise them before downgrading 0023';
        END IF; END $$""")
