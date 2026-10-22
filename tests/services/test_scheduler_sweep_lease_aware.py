"""Regression: backup-scheduler's stale sweeps must honor row leases.

Bug (2026-07-14, jlucas 348GB force-full): neither scheduler sweep consulted
``lease_expires_at``, so both killed LIVE long-running work that workers were
actively renewing via shared.lease.LeaseExtender (120s renew / 600s TTL):

  * the snapshot sweep's only liveness signal was the newest
    snapshot_items.created_at — OneDrive enumeration writes no item rows, so
    any >30min delta walk was reaped (attempt #1);
  * the partition sweep reset any IN_PROGRESS shard on started_at age alone,
    bumping retry_count each 30min pass until max_retries_exceeded — the
    sweep itself produced all 5 "retries" on snapshot 370e1af6 (attempt #2).

Fix: every sweep predicate carries
``AND (lease_expires_at IS NULL OR lease_expires_at < NOW())`` so live-leased
rows are untouched while dead workers' rows (lease lapses <=10min) sweep as
before. Lease-less legacy rows keep the 30-min horizon.

These tests pin the SQL inside services/backup-scheduler/main.py by source
inspection (the statements are inline text() literals; importing the module
drags in the full scheduler runtime).
"""
from __future__ import annotations

import pathlib
import re

_MAIN = (
    pathlib.Path(__file__).resolve().parents[2]
    / "services" / "backup-scheduler" / "main.py"
).read_text()

_LEASE_RE = re.compile(
    r"lease_expires_at\s+IS\s+NULL\s+OR\s+(?:s\.)?lease_expires_at\s+<\s+NOW\(\)",
    re.IGNORECASE,
)


def _sql_block(anchor: str) -> str:
    """Return the text('''...''') SQL literal containing `anchor`."""
    idx = _MAIN.find(anchor)
    assert idx != -1, f"anchor {anchor!r} not found — sweep SQL moved?"
    start = _MAIN.rfind('"""', 0, idx)
    end = _MAIN.find('"""', idx)
    assert start != -1 and end != -1
    return _MAIN[start:end]


def test_snapshot_sweep_skips_live_leases():
    sql = _sql_block("handler orphaned by worker restart")
    assert _LEASE_RE.search(sql), "snapshot sweep must skip rows with a live lease"
    # existing guards must survive the change
    assert "INTERVAL '30 minutes'" in sql
    assert "snapshot_partitions" in sql  # bug #169 in-flight-shards guard


def test_partition_sweep_max_retries_step_skips_live_leases():
    sql = _sql_block("'max_retries_exceeded'")
    assert _LEASE_RE.search(sql), "partition FAIL step must skip live-leased shards"


def test_partition_sweep_reset_step_skips_live_leases():
    sql = _sql_block("SET status        = 'QUEUED'")
    assert _LEASE_RE.search(sql), "partition reset step must skip live-leased shards"


def test_cancelled_sweep_candidates_skip_live_leases():
    """Archive run #8 (2026-07-31): _sweep_cancelled_snapshots Population B
    (IN_PROGRESS snapshot under a CANCELLED/FAILED job) hard-DELETED a
    snapshot whose worker was alive and renewing — the worker then walked
    ~1.1M items blind for 7h and died on the first item INSERT's FK
    violation, and fail_snapshot's merge resurrected the deleted row.
    The candidate SELECT must never offer a live-leased snapshot."""
    sql = _sql_block("Population B: late-arrival orphans")
    assert _LEASE_RE.search(sql), (
        "cancelled-snapshot sweep candidates must skip live-leased rows"
    )


def test_cancelled_sweep_claims_before_destroying_anything():
    """The candidate SELECT's lease check is stale by deletion time (blob
    teardown takes minutes; a redelivered run re-claims a dead lease). The
    sweep must take ATOMIC ownership — re-verify candidacy + dead lease and
    flip to PENDING_DELETION — BEFORE deleting blobs or snapshot_items;
    otherwise a lease-guarded survivor keeps its row but loses its items
    and blobs, and finalizes COMPLETED with silent data loss."""
    claim_idx = _MAIN.find("SET status = 'PENDING_DELETION'")
    assert claim_idx != -1, "Step-0 claim (PENDING_DELETION flip) missing"
    # The claim must carry the dead-lease + candidacy recheck.
    claim_block = _MAIN[claim_idx - 2000:claim_idx + 2000]
    assert _LEASE_RE.search(claim_block), "claim must require a dead lease"
    assert "cancelled_at" in claim_block, "claim must re-verify candidacy"
    # The claim must precede blob deletion and both destructive deletes.
    items_del_idx = _MAIN.find("DELETE FROM snapshot_items si", claim_idx)
    row_del_idx = _MAIN.find("DELETE FROM snapshots s", claim_idx)
    assert items_del_idx != -1 and claim_idx < items_del_idx, (
        "items DELETE must come after the claim"
    )
    assert row_del_idx != -1 and claim_idx < row_del_idx, (
        "snapshot DELETE must come after the claim"
    )
    # Both destructive deletes key on the claimed status, not on a
    # re-evaluated (raceable) candidacy.
    items_seg = _MAIN[items_del_idx:items_del_idx + 400]
    row_seg = _MAIN[row_del_idx:row_del_idx + 300]
    assert "PENDING_DELETION" in items_seg
    assert "PENDING_DELETION" in row_seg


def test_cancelled_sweep_resumes_interrupted_teardowns():
    """A claimed (PENDING_DELETION) row whose sweep pass crashed must be
    re-offered as a candidate on the next pass, or it leaks forever."""
    sql = _sql_block("Population B: late-arrival orphans")
    assert "PENDING_DELETION" in sql, (
        "candidate SELECT must include Population D (teardown resumption)"
    )
