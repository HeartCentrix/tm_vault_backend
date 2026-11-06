"""Activity rollup lookup indexes

Revision ID: 0020
Revises: 0019
Create Date: 2026-11-06

The Activity rollup (shared.batch_rollup) joins snapshots and snapshot_partitions on
job_id = ANY(batch job ids). Its docstring assumed a job_id index existed, but none did, so every
batch rescanned both tables (54M join-filter rows at 5x TM's 30-day history). The
backup_batches queries (audit v2/export, job-service, scheduler) seq-scan jobs and re-cast
every spec per batch: 12 s at 10x. TM log catalogue #20.

Built CONCURRENTLY outside a transaction so a large snapshots table is never write-locked.
Needs only table ownership, not superuser. If a concurrent build fails it leaves an INVALID
index: drop it and re-run.
"""

from typing import Union

from alembic import op

revision: str = "0020"
down_revision: Union[str, None] = "0019"
branch_labels = None
depends_on = None

# Literal SQL, kept in step with shared/database.py's bootstrap list by hand: a migration must
# not change when application code does.
INDEXES = {
    "ix_snapshots_job_id": "snapshots (job_id)",
    "ix_snapshot_partitions_job_id": "snapshot_partitions (job_id)",
    "ix_jobs_spec_batch_id": "jobs ((COALESCE((spec)::jsonb ->> 'batch_id', '')))",
}


def upgrade() -> None:
    with op.get_context().autocommit_block():
        for name, target in INDEXES.items():
            op.execute(f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {name} ON {target}")


def downgrade() -> None:
    with op.get_context().autocommit_block():
        for name in INDEXES:
            op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {name}")
