"""Cross-mailbox mail attachment reuse table (mail attachment contents)

Revision ID: 0026
Revises: 0025
Create Date: 2026-11-06

One email in N mailboxes downloaded each attachment N times (Railway 2026-11-06: 991 MB from
Graph for 413 MB of distinct attachment bytes). shared/mail_attachment_reuse.py records each
distinct stored file attachment once and later copies probe its primary key; snapshot_items
gets no new index. The worker skips reuse while the table is absent. Service boot's
Base.metadata.create_all also creates it.

Renumbered from 0023 at the TM-scale integration merge. Railway (no alembic) may run
scripts/2026_11_06_mail_attachment_reuse.sql.
"""

from typing import Union

from alembic import op

revision: str = "0026"
down_revision: Union[str, None] = "0025"
branch_labels = None
depends_on = None

# Literal SQL: a migration must not change when application code does.
CREATE_SQL = """
CREATE TABLE IF NOT EXISTS mail_attachment_contents (
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    internet_message_id VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    graph_size BIGINT NOT NULL,
    content_type VARCHAR NOT NULL,
    is_inline BOOLEAN NOT NULL,
    content_id VARCHAR,
    blob_path VARCHAR NOT NULL,
    content_hash VARCHAR NOT NULL,
    content_size BIGINT NOT NULL,
    backend_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, internet_message_id, name, graph_size, content_type, is_inline)
)
"""


def upgrade() -> None:
    op.execute(CREATE_SQL)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS mail_attachment_contents")
