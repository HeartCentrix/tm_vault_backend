"""Regression: scheduled dispatches and single-resource trigger jobs
must produce Activity rows that eventually flip to Done.

Two chronic "In Progress forever" classes (2026-07-14):

1. ``dispatch_policy_backups`` never stamped ``spec.batch_id``, so each
   of its Jobs rolled up alone. The Tier-1 ENTRA_USER job's expected
   Tier-2 children were never observed in its group (missing_t2 > 0)
   -> every daily 02:07/10:07/18:07 dispatch left a permanent
   "2 users / In Progress / 966.4 KiB so far" Activity row even though
   the jobs COMPLETED minutes later. The UI click path (job-service)
   has stamped batch_id since 2026-05-15; the scheduled path was missed.

2. ``POST /scheduler/resource/{id}`` creates its Job QUEUED with
   ``resource_id`` set, and no code path ever flipped it — jobs from
   7/13 sat QUEUED with COMPLETED/FAILED snapshots, failing the
   ``all_jobs_terminal`` rollup gate forever.

Source-inspection pins (the logic lives in async closures over live
sessions; the SQL-shape assertions mirror test_scheduler_sweep_lease_aware).
"""
from __future__ import annotations

import pathlib

_MAIN = (
    pathlib.Path(__file__).resolve().parents[2]
    / "services" / "backup-scheduler" / "main.py"
).read_text()


def _block(anchor: str, span: int = 4000) -> str:
    idx = _MAIN.find(anchor)
    assert idx != -1, f"anchor {anchor!r} not found"
    return _MAIN[max(0, idx - span): idx + span]


def test_dispatch_stamps_shared_batch_id():
    blk = _block('"triggered_by": "SCHEDULED"')
    assert '"batch_id": dispatch_batch_id' in blk, (
        "scheduled dispatch jobs must share one spec.batch_id per dispatch"
    )
    # one id per dispatch invocation, minted before the group loop
    assert "dispatch_batch_id = str(uuid.uuid4())" in _MAIN
    # propagated on the queue message for downstream Tier-2 spawns
    assert 'message["batchId"] = dispatch_batch_id' in _MAIN


def test_single_resource_job_reaper_exists_with_guards():
    blk = _block("single_resource_job_reaper")
    # scoped to the trigger-endpoint job shape only
    assert "j.resource_id IS NOT NULL" in blk
    assert "COALESCE(cardinality(j.batch_resource_ids), 0) = 0" in blk
    # must not finalize while anything is still moving
    assert "a.n_pending = 0" in blk
    assert "sp.status NOT IN ('COMPLETED','FAILED')" in blk
    # settled grace so we don't race a just-finished snapshot's retry
    assert "INTERVAL '5 minutes'" in blk
    # outcome mirrors snapshots: any success -> COMPLETED else FAILED
    assert "WHEN a.n_ok > 0" in blk


def test_single_resource_reaper_registered_on_interval():
    assert "_reap_settled_single_resource_jobs, \"interval\"" in _MAIN.replace("'", '"')
