"""tenant_dek and encryption_config tables

Revision ID: 0029
Revises: 0028
Create Date: 2026-11-19

Until now only the offline bootstrap (shared/database.py init_db) created these two tables, so a
database brought to head by Alembic alone (Taylor Morrison: old bootstrap, stamped 0006) failed
verify_runtime_schema(). Same DDL as the bootstrap, plus ix_tenant_dek_tenant_id from the model
(TenantDek.tenant_id index=True). Idempotent: a bootstrapped database already has them.
"""

from typing import Union

from alembic import op
import sqlalchemy as sa

revision: str = "0029"
down_revision: Union[str, None] = "0028"
branch_labels = None
depends_on = None

# Literal SQL kept in step with shared/database.py's bootstrap by hand.
DDL = (
    """CREATE TABLE IF NOT EXISTS tenant_dek (
        id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        tenant_id    VARCHAR NOT NULL,
        key_id       BYTEA NOT NULL UNIQUE,
        wrapped_dek  BYTEA NOT NULL,
        kek_id       VARCHAR NOT NULL,
        provider     VARCHAR NOT NULL,
        state        VARCHAR NOT NULL DEFAULT 'active',
        created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )""",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_tenant_dek_active ON tenant_dek(tenant_id) WHERE state = 'active'",
    "CREATE INDEX IF NOT EXISTS ix_tenant_dek_tenant_id ON tenant_dek(tenant_id)",
    """CREATE TABLE IF NOT EXISTS encryption_config (
        id              VARCHAR PRIMARY KEY DEFAULT 'singleton',
        provider        VARCHAR NOT NULL,
        key_reference   VARCHAR,
        enabled         BOOLEAN NOT NULL DEFAULT FALSE,
        verify_readback BOOLEAN NOT NULL DEFAULT TRUE,
        updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_by      VARCHAR
    )""",
)


def upgrade() -> None:
    for ddl in DDL:
        op.execute(ddl)


def downgrade() -> None:
    # Wrapped DEKs decrypt existing blobs: never drop a populated key table.
    conn = op.get_bind()
    for table in ("tenant_dek", "encryption_config"):
        if conn.scalar(sa.text(f"SELECT EXISTS(SELECT 1 FROM {table})")):
            raise RuntimeError(f"{table} holds rows; refusing to drop it in downgrade of 0029")
    op.execute("DROP TABLE IF EXISTS tenant_dek")
    op.execute("DROP TABLE IF EXISTS encryption_config")
