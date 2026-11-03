"""Drain phase — wait for inflight jobs, abort stragglers and enqueue retries."""
from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

log = logging.getLogger("toggle.drain")


@dataclass
class DrainResult:
    drained_count: int
    retried_count: int


class DrainAborted(Exception):
    """Operator aborted the toggle while the drain was still waiting.

    Raised so the orchestrator stands down before any promote/DNS/worker
    phase runs — previously the abort endpoint only rewrote the event row
    and the worker drove the flip to completion anyway."""


async def drain_inflight(db, event_id: str,
                         poll_interval_s: int = 5,
                         max_wait_s: int = 900) -> DrainResult:
    deadline = datetime.now(timezone.utc) + timedelta(seconds=max_wait_s)
    last_remaining = 0

    while datetime.now(timezone.utc) < deadline:
        status = await db.fetchval(
            "SELECT status FROM storage_toggle_events WHERE id=$1", event_id,
        )
        if status == "aborted":
            raise DrainAborted()
        rows = await db.fetch(
            "SELECT id FROM jobs WHERE status IN ('RUNNING','QUEUED','PENDING')"
        )
        remaining = len(rows)
        if remaining == 0:
            return DrainResult(drained_count=last_remaining, retried_count=0)
        last_remaining = remaining
        log.info("[drain] %d jobs still running (event %s)", remaining, event_id)
        await asyncio.sleep(max(poll_interval_s, 0))

    rows = await db.fetch(
        "SELECT id, type, tenant_id, resource_id, snapshot_id, spec "
        "FROM jobs WHERE status IN ('RUNNING','QUEUED','PENDING')"
    )
    retried = 0
    for r in rows:
        new_id = uuid.uuid4()
        await db.execute(
            "UPDATE jobs SET status='CANCELLED', updated_at=NOW(), "
            "retry_reason=$2 WHERE id=$1",
            r["id"], f"aborted_by_toggle_{event_id}",
        )
        await db.execute(
            "INSERT INTO jobs "
            "(id, type, tenant_id, resource_id, snapshot_id, status, spec, "
            " retry_reason, pre_toggle_job_id, created_at, updated_at) "
            "VALUES ($1,$2,$3,$4,$5,'QUEUED',$6,$7,$8,NOW(),NOW())",
            new_id, r["type"], r["tenant_id"], r["resource_id"], r["snapshot_id"],
            r["spec"], f"toggle_retry_from_event_{event_id}", r["id"],
        )
        retried += 1

    return DrainResult(drained_count=last_remaining - retried, retried_count=retried)
