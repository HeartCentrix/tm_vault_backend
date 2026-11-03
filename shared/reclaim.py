"""Startup reclaim — re-release work owned by this replica before its
last restart.

Called once per worker boot, BEFORE consuming starts. Finds work rows
whose ``lease_owner_id`` matches our previous worker_uuid (resolved
from ``replica_id`` via the ``worker_heartbeats`` table) and whose
lease has expired by definition (the worker that held it just
restarted). Clears the lease and bumps the fence token; the
reconciler's next sweep then picks them up and finalizes/re-enqueues
as appropriate.

This closes the redeploy gap to <1 s instead of waiting up to
``LEASE_TTL_S + sweep_interval`` for the periodic sweep.

Design ref: docs/superpowers/specs/2026-05-16-distributed-reconciliation-design.md §9.2.
"""
from __future__ import annotations

import uuid
from typing import Optional

from sqlalchemy import text

from shared.database import async_session_factory
from shared.copy_workflow import pending_copy_sql


async def reclaim_for_replica(
    *,
    replica_id: str,
    worker_uuid: uuid.UUID,
) -> int:
    """Re-release every lease previously held by this replica.

    Returns the total number of work rows touched across all three
    leased tables. Idempotent — re-running is a no-op once leases are
    cleared.

    Logic:
      1. Find ``worker_heartbeats`` rows for this replica with a
         different ``worker_id`` (= our previous process incarnation).
      2. For each, clear the lease on any IN_PROGRESS work it still
         holds; bump the fence token so any zombie write from the old
         process is rejected.
      3. Tombstone the old heartbeat row so the sweeper doesn't think
         we're alive on multiple worker_ids.
    """
    touched = 0
    async with async_session_factory() as session:
        old_workers = (
            await session.execute(
                text(
                    """
                    SELECT worker_id
                      FROM worker_heartbeats
                     WHERE replica_id = :rep
                       AND worker_id <> cast(:wid AS uuid)
                    """
                ),
                {"rep": replica_id, "wid": str(worker_uuid)},
            )
        ).all()
        old_ids = [str(r.worker_id) for r in old_workers]
        if not old_ids:
            return 0

        for table, in_prog, copy_scope in (
            ("snapshots", "IN_PROGRESS", "snapshot_expr"),
            ("snapshot_partitions", "IN_PROGRESS", "partition_expr"),
            ("jobs", "RUNNING", "job_expr"),
        ):
            res = await session.execute(
                text(
                    f"""
                    UPDATE {table}
                       SET lease_owner_id   = NULL,
                           lease_expires_at = NOW() - INTERVAL '1 second',
                           lease_token      = lease_token + 1
                     WHERE lease_owner_id = ANY(cast(:ids AS uuid[]))
                       AND status::text = :sts
                       AND {pending_copy_sql(**{copy_scope: f'{table}.id'})}
                    """
                ),
                {"ids": old_ids, "sts": in_prog},
            )
            touched += res.rowcount or 0

        # Tombstone the old heartbeat rows so the sweeper liveness
        # check treats them as dead.
        await session.execute(
            text(
                """
                UPDATE worker_heartbeats
                   SET last_seen_at = NOW() - INTERVAL '1 hour'
                 WHERE worker_id = ANY(cast(:ids AS uuid[]))
                """
            ),
            {"ids": old_ids},
        )

        await session.commit()
    return touched
