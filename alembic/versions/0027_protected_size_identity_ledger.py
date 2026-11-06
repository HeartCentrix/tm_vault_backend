"""Protected-size identity ledger: one anchored winner per identity replaces per-item refs

Revision ID: 0027
Revises: 0026
Create Date: 2026-11-06

protected_size_refs kept one row per (resource, snapshot, item): every copy-forward of N
unchanged rows into a new snapshot cost N ref upserts. Winners now carry an anchor item and
the refresher writes only for real changes (shared.protected_size._references).

Additive and online: anchor columns (catalog-only), anchor index built CONCURRENTLY. A small
refs table (< 1M rows) is converted and dropped here. A bigger one (TM) is left to
scripts/2026_11_06_protected_size_identity_ledger.sql, batched and resumable; until that
script drops protected_size_refs the refresher idles ("converting") and the dashboard keeps
its last published totals.
"""

from alembic import op
from sqlalchemy import text

from shared.protected_size_schema import (
    ANCHOR_BACKFILL,
    ANCHOR_COLUMNS,
    ANCHOR_INDEX,
    build_index_online,
)

revision = "0027"
down_revision = "0026"
branch_labels = None
depends_on = None

INLINE_LIMIT = 1_000_000


def upgrade():
    op.execute("SET LOCAL lock_timeout='5s'")
    op.execute(ANCHOR_COLUMNS)
    with op.get_context().autocommit_block():
        build_index_online(op.get_bind(), *ANCHOR_INDEX)
    bind = op.get_bind()
    if bind.scalar(text("SELECT to_regclass('protected_size_refs') IS NULL")):
        return
    small = bind.scalar(text(
        f"SELECT count(*) FROM (SELECT 1 FROM protected_size_refs LIMIT {INLINE_LIMIT}) r")) < INLINE_LIMIT
    if small:
        op.execute(ANCHOR_BACKFILL)
        op.execute("DROP TABLE protected_size_refs")


def downgrade():
    raise RuntimeError("Protected-size ledger is durable; forward migration required")
