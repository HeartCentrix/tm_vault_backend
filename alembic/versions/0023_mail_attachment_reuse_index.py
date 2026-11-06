"""Cross-mailbox mail attachment reuse index

Revision ID: 0023
Revises: 0022
Create Date: 2026-11-06

One email in N mailboxes downloaded each attachment N times (Railway 2026-11-06: 991 MB from
Graph for 413 MB of distinct attachment bytes). shared/mail_attachment_reuse.py looks up the
tenant's stored attachments by internetMessageId; this partial expression index serves that
lookup. The worker skips reuse while the index is absent or INVALID.

Renumber if another branch lands 0023 first. Railway (no alembic) runs
scripts/2026_11_06_mail_attachment_reuse.sql. Built CONCURRENTLY: a failed build leaves an
INVALID index; drop it and re-run.
"""

from typing import Union

from alembic import op

revision: str = "0023"
down_revision: Union[str, None] = "0022"
branch_labels = None
depends_on = None

# Literal SQL: a migration must not change when application code does.
INDEX_SQL = ("CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_snapshot_items_mail_att_imid ON snapshot_items "
             "(tenant_id, (metadata->>'internet_message_id')) WHERE item_type = 'EMAIL_ATTACHMENT'")


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(INDEX_SQL)


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_snapshot_items_mail_att_imid")
