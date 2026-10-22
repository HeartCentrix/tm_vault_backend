"""resource_protected_bytes durable rollup cache (Overview cold-load fix).

Revision ID: 0007
Revises: 0006
"""
from alembic import op

revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS resource_protected_bytes (
            resource_id UUID PRIMARY KEY REFERENCES resources(id) ON DELETE CASCADE,
            latest_snapshot_id UUID NOT NULL,
            bytes BIGINT NOT NULL DEFAULT 0,
            computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS resource_protected_bytes")
