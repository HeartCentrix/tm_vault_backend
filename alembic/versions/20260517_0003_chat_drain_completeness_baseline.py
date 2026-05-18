"""Add ``chat_threads.last_drained_msg_count`` for the drain completeness gate.

Background
----------
The chat-drain code in workers/backup-worker/main.py accepted partial
drains as success when a transient error consumed the retry budget mid-
pagination. The snapshot was marked COMPLETED with ``len(msgs_local)``
items even though hundreds-of-thousands of messages might be missing.
Observed: Amit Mishra's run varied 8k / 24k / 29k chat messages across
runs because the partial-drain probability per run is essentially random.

The completeness gate (in main.py at the end of _drain_one_chat) compares
the post-drain count against the prior fully-successful drain's count;
if it dropped by more than CHAT_DRAIN_COMPLETENESS_DROP_PCT% (default 50)
the drain is treated as failed and the cursor is not advanced. To do
that comparison we need a column on ``chat_threads`` recording the
baseline.

Migration shape
---------------
* Up: ADD COLUMN ``last_drained_msg_count INTEGER NULL`` to
  ``tm_vault.chat_threads``. No backfill — NULL is treated by the
  application as "no baseline yet, skip the gate."
* Down: DROP COLUMN. Safe; the application tolerates the column being
  absent (the ``_claim_or_load_chat_thread`` SELECT references the
  column by name, so downgrade would break the worker — only downgrade
  if you also revert the backup-worker image to a pre-fix build).
"""
from __future__ import annotations

import os
from typing import Union

import sqlalchemy as sa
from alembic import op

revision: str = "0003"
down_revision: Union[str, None] = "0002"
branch_labels = None
depends_on = None

_SCHEMA = os.environ.get("DB_SCHEMA", "tm_vault")


def upgrade() -> None:
    op.add_column(
        "chat_threads",
        sa.Column(
            "last_drained_msg_count",
            sa.Integer(),
            nullable=True,
        ),
        schema=_SCHEMA,
    )


def downgrade() -> None:
    op.drop_column(
        "chat_threads",
        "last_drained_msg_count",
        schema=_SCHEMA,
    )
