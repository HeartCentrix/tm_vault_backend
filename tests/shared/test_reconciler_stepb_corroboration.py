"""Regression pins for the 2026-08-25 reconciler STEP B corroboration fix.

Incident (8/24-8/25 pooler-starvation storms): STEP B's slow path settled
LIVE non-partitioned walks FAILED because the very connections that renew
``snapshots.lease_expires_at`` and ``worker_heartbeats.last_seen_at`` were the
ones query_wait_timeout killed — "lease expired + no live heartbeat" was true
of workers that were alive and progressing:

  * snapshot 1386f54c was flipped terminal at 19:15Z while its worker was
    mid-carry-forward (the worker's liveness probe then aborted the run);
  * snapshot 29c425d5 finished 856,655 items and sat IN_PROGRESS 8.2h before
    being orphaned FAILED — the completed run was discarded.

Fix (shared/reconciler.py STEP B slow path):

  * the dead lease must STAY dead for ``RECONCILER_SNAP_REAP_LEASE_GRACE_S``
    (default 900s) beyond its TTL before the row qualifies;
  * a snapshot with ``snapshot_items`` committed inside
    ``RECONCILER_SNAP_REAP_PROGRESS_GRACE_S`` (default 1800s) is proof-of-life
    and never reaped, regardless of lease state.

The fast path (partitioned + all shards terminal) is intentionally untouched:
no handler will ever finalize those.
"""
from __future__ import annotations

import pathlib
import re

_SRC = (
    pathlib.Path(__file__).resolve().parents[2] / "shared" / "reconciler.py"
).read_text()


def _step_b_block() -> str:
    start = _SRC.index("STEP B")
    end = _SRC.index("STEP C")
    return _SRC[start:end]


class TestLeaseGrace:
    def test_knobs_defined_with_defaults(self):
        assert re.search(
            r'SNAP_REAP_LEASE_GRACE_S\s*=\s*int\(os\.getenv\("RECONCILER_SNAP_REAP_LEASE_GRACE_S",\s*"900"\)\)',
            _SRC,
        )
        # 9000 matches ARCHIVE_STALL_ABORT_S — a shorter window is inert
        # for the archive walk's legitimate zero-progress quiet phases
        # (retry ladder ~3600s, carry-forward copy ~2880s+, whose rows
        # are invisible until the end-of-copy commit).
        assert re.search(
            r'SNAP_REAP_PROGRESS_GRACE_S\s*=\s*int\(os\.getenv\("RECONCILER_SNAP_REAP_PROGRESS_GRACE_S",\s*"9000"\)\)',
            _SRC,
        )

    def test_slow_path_lease_requires_grace_window(self):
        block = _step_b_block()
        assert re.search(
            r"s\.lease_expires_at\s*<\s*\n?\s*NOW\(\)\s*-\s*\(:reap_grace_s\s*\*\s*INTERVAL '1 second'\)",
            block,
        ), "slow path must require the lease dead beyond TTL + grace, not merely past TTL"

    def test_binds_are_passed(self):
        block = _step_b_block()
        assert '"reap_grace_s": SNAP_REAP_LEASE_GRACE_S' in block
        assert '"progress_grace_s": SNAP_REAP_PROGRESS_GRACE_S' in block


class TestProgressCorroboration:
    def test_recent_items_block_the_reap(self):
        block = _step_b_block()
        m = re.search(
            r"NOT EXISTS\s*\(\s*(?:--[^\n]*\n\s*)*SELECT 1 FROM snapshot_items si\s*"
            r"WHERE si\.snapshot_id = s\.id\s*"
            r"AND si\.created_at >\s*\n?\s*NOW\(\)\s*-\s*\(:progress_grace_s",
            block,
        )
        assert m, "recently-committed snapshot_items must veto the orphan settle"

    def test_fast_path_untouched(self):
        block = _step_b_block()
        fast = block[: block.index("Slow path")]
        assert "progress_grace_s" not in fast, (
            "all-shards-terminal fast path must not gain the item-recency veto "
            "(no handler will finalize those rows — delaying them helps nothing)"
        )
