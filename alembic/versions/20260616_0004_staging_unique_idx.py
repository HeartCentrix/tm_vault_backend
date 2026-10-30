"""staging unique idx

Revision ID: 20260616_0004
Revises: 0003
Create Date: 2026-06-16

Adds a unique constraint on resource_discovery_staging (run_id, tenant_id,
resource_type, external_id) so the worker can stage batches independently
and let the DB collapse duplicates via ON CONFLICT instead of using an
in-memory dedup set.
"""

from typing import Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "20260616_0004"
down_revision: Union[str, None] = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Delete pre-existing duplicate rows, keeping the one with the lowest ctid
    # (i.e. the first inserted), so the unique constraint can be created cleanly.
    op.execute(
        """
        DELETE FROM resource_discovery_staging a
        USING resource_discovery_staging b
        WHERE a.ctid > b.ctid
          AND a.run_id         = b.run_id
          AND a.tenant_id      = b.tenant_id
          AND a.resource_type  = b.resource_type
          AND a.external_id    = b.external_id
        """
    )

    op.drop_index("idx_discovery_stage_lookup", table_name="resource_discovery_staging", if_exists=True)

    op.create_unique_constraint(
        "uq_staging_run_resource",
        "resource_discovery_staging",
        ["run_id", "tenant_id", "resource_type", "external_id"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_staging_run_resource",
        "resource_discovery_staging",
        type_="unique",
    )

    op.create_index("idx_discovery_stage_lookup", "resource_discovery_staging", ["run_id", "tenant_id", "resource_type", "external_id"], if_not_exists=True)
