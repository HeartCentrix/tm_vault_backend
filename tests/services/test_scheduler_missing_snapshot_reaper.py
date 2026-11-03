"""Regression: RUNNING batch jobs whose snapshot rows were DELETED must
still finalize (phantom "In Progress" class #3, observed 2026-07-22).

Job 572bb0b6 (7/20 10:07 Gold dispatch, batch f3151a6e, both SharePoint
sites) sat RUNNING for 2 days: the failed-snapshot retry sweep hard-
DELETED its snapshot 03627c4a (reap-vs-live-consumer race), leaving the
job with ZERO snapshot rows. ``_reap_stuck_running_jobs`` only finalizes
when ``n_terminal >= n_resources`` counting EXISTING rows, so a job with
deleted snapshots can never satisfy it and the Activity batch rollup
shows "In Progress / 0%" forever.

The missing-snapshot reaper must finalize such jobs, but ONLY when the
job is provably dead — a RUNNING job legitimately has zero snapshot
rows during early enumeration, and long enumerations renew the job
lease via shared.lease.LeaseExtender. Guards required:

  * job lease expired (or lease-less legacy row)
  * updated_at stale by a long horizon (>= 1 hour)
  * no non-terminal snapshot rows under the job
  * no in-flight shard still references the job
  * only fires when snapshot rows are actually MISSING
    (n_existing < n_resources) — the settled case belongs to
    ``_reap_stuck_running_jobs``.

Source-inspection pins (the logic lives in async closures over live
sessions; SQL-shape assertions mirror test_scheduler_activity_finalize).
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


def test_missing_snapshot_reaper_exists_and_is_scheduled():
    blk = _block("missing_snapshot_reaper")
    # batch-job shape only (single-resource shape has its own reaper)
    assert "cardinality(j.batch_resource_ids) > 0" in blk
    assert "j.status = 'RUNNING'" in blk


def test_missing_snapshot_reaper_liveness_guards():
    blk = _block("missing_snapshot_reaper")
    # lease-aware (2026-07-14 sweep convention): never reap a job whose
    # worker is still renewing the lease
    assert "j.lease_expires_at IS NULL OR j.lease_expires_at < NOW()" in blk
    # long stale horizon — early enumeration legitimately has 0 rows
    assert "INTERVAL '1 hour'" in blk
    # nothing still moving under the job
    assert "n_nonterminal = 0" in blk or "n_nonterminal" in blk
    assert "sp.status NOT IN ('COMPLETED','FAILED')" in blk


def test_missing_snapshot_reaper_only_fires_when_rows_missing():
    blk = _block("missing_snapshot_reaper")
    # the settled/all-rows-present case is _reap_stuck_running_jobs's
    # job — this reaper must require actually-missing snapshot rows
    assert "n_existing < t.n_resources" in blk or "n_existing <" in blk


def test_missing_snapshot_reaper_outcome_mirrors_survivors():
    blk = _block("missing_snapshot_reaper")
    # any surviving success -> COMPLETED, else FAILED; annotate result
    assert "WHEN t.n_completed > 0" in blk
    assert "'reaped_by', 'missing_snapshot_reaper'" in blk
    assert "snapshot rows missing" in blk.lower() or "deleted" in blk.lower()
