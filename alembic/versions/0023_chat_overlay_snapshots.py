"""Chat overlay snapshots

Revision ID: 0023
Revises: 0022
Create Date: 2026-11-07

snapshot_items_effective() also inherits Teams-chat base rows, so a chat incremental can overlay its
prior full snapshot instead of copying every unchanged chat's rows (shared.chat_catalogue
._overlay_unchanged). Railway (no alembic) runs scripts/2026_11_07_chat_overlay_snapshots.sql.
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
    # Chat overlays need the chat branch of the function: refuse rather than hide their base rows.
    op.execute("""DO $$ BEGIN
        IF EXISTS (SELECT 1 FROM snapshots WHERE overlay_base_id IS NOT NULL
                   AND extra_data->>'chat_overlay_base' IS NOT NULL) THEN
            RAISE EXCEPTION 'chat overlay snapshots exist; materialise them before downgrading 0023';
        END IF; END $$""")
