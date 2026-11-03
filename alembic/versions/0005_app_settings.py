"""app_settings store

Revision ID: 0005
Revises: 20260616_0004
Create Date: 2026-06-16

Adds the global single-row `app_settings` table holding the daily discovery
time (HH:MM) and the application timezone. The table is guarded to a single
row via CHECK(id = 1); the singleton row is seeded in upgrade().
"""

from typing import Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "0005"
down_revision: Union[str, None] = "20260616_0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "app_settings",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "discovery_time", sa.Text(), nullable=False, server_default="02:00"
        ),
        sa.Column(
            "timezone", sa.Text(), nullable=False, server_default="America/Phoenix"
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint("id = 1", name="ck_app_settings_singleton"),
    )

    # Seed the singleton row (idempotent).
    op.execute(
        """
        INSERT INTO app_settings (id, discovery_time, timezone)
        VALUES (1, '02:00', 'America/Phoenix')
        ON CONFLICT (id) DO NOTHING
        """
    )


def downgrade() -> None:
    op.drop_table("app_settings")
