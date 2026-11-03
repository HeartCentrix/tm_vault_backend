"""resource_content_rollup durable rollup cache (Recovery page cold-load fix).

Revision ID: 0008
Revises: 0007
"""
from alembic import op

revision = "0008"
down_revision = "0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS resource_content_rollup (
            child_resource_id UUID NOT NULL REFERENCES resources(id) ON DELETE CASCADE,
            latest_snapshot_id UUID NOT NULL,
            types_key TEXT NOT NULL,
            item_count BIGINT NOT NULL DEFAULT 0,
            bytes BIGINT NOT NULL DEFAULT 0,
            computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (child_resource_id, types_key)
        )
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS resource_content_rollup")
