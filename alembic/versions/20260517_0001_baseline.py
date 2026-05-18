"""Baseline — stamp the current schema as alembic revision 0001.

This migration is intentionally a NO-OP. It exists so that:

1. Existing databases (already created by ``Base.metadata.create_all``)
   can be stamped at this revision via ``alembic stamp head`` without
   re-running any DDL. The next real migration starts from a known
   point.

2. Fresh databases get this revision applied to record the version
   table; the actual table creation still flows through
   ``init_db()`` for backward compat during the cutover window.

Cutover policy
--------------
Once every environment is stamped at >= 0001, the next code change
that needs a schema edit MUST author a new revision and the app
MUST stop calling ``Base.metadata.create_all`` for the affected
tables. ``shared/database.py`` continues to be the bootstrap path
for greenfield demos until that cutover is complete, at which point
``init_db()`` becomes a thin shim that does ``upgrade head``.

Revision ID: 0001
Revises:
Create Date: 2026-05-17
"""
from __future__ import annotations

from typing import Sequence, Union

# revision identifiers, used by Alembic.
revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # No-op: assumes ``Base.metadata.create_all`` already produced the
    # schema. Use ``alembic stamp head`` to seed the version table on
    # an existing DB without running DDL.
    pass


def downgrade() -> None:
    # No-op: this baseline has no DDL to reverse.
    pass
