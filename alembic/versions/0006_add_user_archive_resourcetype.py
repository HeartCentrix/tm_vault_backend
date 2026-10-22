"""add USER_ARCHIVE resourcetype

Revision ID: 0006
Revises: 0005
Create Date: 2026-07-24

Adds the USER_ARCHIVE resource type enum value for Exchange Online Archive
backup as a Tier-2 workload under ENTRA_USER.
"""

from typing import Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0006"
down_revision: Union[str, None] = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("ALTER TYPE resourcetype ADD VALUE IF NOT EXISTS 'USER_ARCHIVE'")


def downgrade() -> None:
    pass  # PG enums can't drop values; harmless leftover
