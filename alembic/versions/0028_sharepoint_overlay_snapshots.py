"""SharePoint overlay snapshots

Revision ID: 0028
Revises: 0027
Create Date: 2026-11-08

snapshot_items_effective() also inherits SharePoint list rows (SHAREPOINT_LIST / _FILE / _FOLDER /
_LIST_ITEM), so a site incremental can overlay its prior full snapshot instead of copying every
unchanged list row (shared.sp_overlay). Railway (no alembic) runs
scripts/2026_11_08_sharepoint_overlay_snapshots.sql.
"""

from typing import Union

from alembic import op
from shared.database import SNAPSHOT_ITEMS_EFFECTIVE_FUNCTION

revision: str = "0028"
down_revision: Union[str, None] = "0027"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(SNAPSHOT_ITEMS_EFFECTIVE_FUNCTION)


def downgrade() -> None:
    # SharePoint overlays need the SharePoint branch of the function: refuse rather than hide their base rows.
    op.execute("""DO $$ BEGIN
        IF EXISTS (SELECT 1 FROM snapshots WHERE overlay_base_id IS NOT NULL
                   AND extra_data->>'sp_overlay_base' IS NOT NULL) THEN
            RAISE EXCEPTION 'SharePoint overlay snapshots exist; materialise them before downgrading 0028';
        END IF; END $$""")
