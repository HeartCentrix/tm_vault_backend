"""Regression pins for the 2026-08-25 worker durability batch.

Root incident: the 8/24-8/25 pooler-starvation storms killed connections
under coroutines at exactly the moments the 8/20-wedge class needs:

  * exceptions between the committed partition claim and the drain
    try-block escaped to ``reject(requeue=True)``; redelivery ack-dropped
    against the still-leased row = invisible permanent wedge;
  * ``_requeue_partition_for_retry`` itself was a bare single-attempt
    write that died in the same outage that routed traffic to it;
  * ``_write_snapshot_terminal`` ran once on the caller's session —
    snapshot 29c425d5 finished a 9.5h/856k-item archive run and lost it
    to one killed finalize connection;
  * FAILED→FAILED re-fail overwrote the reconciler's restated
    ``item_count`` / markers with stale in-memory values (1386f54c:
    700→0);
  * ``complete_snapshot``'s delta-token persist raised AFTER successful
    finalize and reported a completed run as failed;
  * ``_finalize_bulk_parent_if_complete`` counted snapshot ROWS vs
    ``batch_resource_ids`` cardinality, so a resource with NO row at all
    (dispatch message dead-lettered) still yielded COMPLETED with
    ``snapshots_failed=0`` (job eb557418);
  * the create_snapshot dedup advisory lock was keyed with ``hash()``
    (PYTHONHASHSEED-randomized) — cross-pod exclusion never worked.

Source-inspection pins (importing the worker drags in the full runtime)
plus a behavioral pin for the stable lock key.
"""
from __future__ import annotations

import hashlib
import pathlib
import re

_SRC = (
    pathlib.Path(__file__).resolve().parents[2]
    / "workers" / "backup-worker" / "main.py"
).read_text()

_HANDLERS = ["onedrive", "chats", "mail", "sharepoint", "groups", "entra"]


def _handler_block(name: str) -> str:
    start = _SRC.index(f"async def _process_{name}_partition_message")
    m = re.search(r"\n    (?:async )?def ", _SRC[start + 10:])
    end = start + 10 + (m.start() if m else len(_SRC))
    return _SRC[start:end]


class TestPreDrainGuards:
    def test_every_handler_has_a_pre_drain_phase(self):
        for h in _HANDLERS:
            block = _handler_block(h)
            assert f'phase="{h} pre-drain"' in block, (
                f"{h}: claim→drain window unguarded — an escape there "
                f"reject(requeue=True)s and the redelivery ack-drops "
                f"against the still-leased row (8/20 wedge class)"
            )

    def test_guard_precedes_the_drain(self):
        for h in _HANDLERS:
            block = _handler_block(h)
            guard = block.index(f'phase="{h} pre-drain"')
            sem = block.index("tenant_sem = await self._acquire_tenant_slot")
            assert guard < sem, f"{h}: guard must cover the span BEFORE the drain"

    def test_guard_finalizes_without_tenant_resource(self):
        # tenant/resource are unbound when the resolve itself raised —
        # the guard's finalize must not reference them.
        for h in _HANDLERS:
            block = _handler_block(h)
            g = block.index(f'phase="{h} pre-drain"')
            tail = block[g:g + 800]
            assert "job_id=job_id," in tail
            assert "tenant=tenant" not in tail.split("raise")[0], (
                f"{h}: pre-drain finalize must not pass tenant= "
                f"(unbound if session.get raised)"
            )


class TestRequeueHardening:
    def _block(self) -> str:
        start = _SRC.index("async def _requeue_partition_for_retry")
        return _SRC[start:start + 14000]

    def test_requeue_is_retried(self):
        b = self._block()
        assert "run_with_db_retry" in b
        assert "partition_requeue:" in b

    def test_exhaustion_releases_to_queued(self):
        b = self._block()
        assert "partition_release:" in b
        assert re.search(r"SET status\s+=\s+'QUEUED'", b)

    def test_release_success_returns_queued_semantics(self):
        # A non-None return makes _partition_retry_or_fail take its
        # QUEUED branch — matching the row's actual state — instead of
        # the None branch that would mark the now-QUEUED row FAILED.
        # The release bumps + caps retry_count so a deterministic
        # non-transient requeue failure cannot loop on free retries.
        b = self._block()
        assert "return int(rel[0] or 0)" in b
        assert "retry_count       = retry_count + 1" in b
        assert "AND retry_count < :cap" in b
        # NUL bytes are a deterministic asyncpg DataError on jsonb params
        assert 'replace("\\x00", "")' in b


class TestSnapshotTerminalDurability:
    def _block(self) -> str:
        start = _SRC.index("async def _write_snapshot_terminal")
        return _SRC[start:start + 12000]

    def test_fresh_session_per_attempt(self):
        b = self._block()
        attempt = b.index("async def _attempt")
        assert "async_session_factory()" in b[attempt:attempt + 400], (
            "each attempt must open its OWN session — the caller's "
            "connection is exactly what is broken on retry"
        )

    def test_retried_storm_scale(self):
        # attempts=8/max_delay=15 — the default ~2s ladder only covers
        # blips; the storms that kill this write last minutes.
        b = self._block()
        assert re.search(r"attempts=8", b)
        assert "snapshot_terminal:" in b

    def test_replay_after_lost_commit_ack_reports_success(self):
        b = self._block()
        assert "prior attempt landed" in b

    def test_refail_is_additive_only(self):
        b = self._block()
        refail = b.index("_refail")
        tail = b[refail:refail + 3500]
        # counters never regress
        assert "max(row.item_count or 0" in tail
        # existing non-empty keys win; empty-string error treated absent
        assert re.search(r'v is not None and v != ""', tail)
        # stale cursors must not clobber the row's accumulated state
        assert "deliberately NOT set" in tail

    def test_never_merge_semantics_kept(self):
        b = self._block()
        assert "row missing" in b and "not resurrecting" in b


class TestDeltaTokenPersist:
    def test_own_session_and_swallow(self):
        start = _SRC.index("# Persist delta token on resource")
        b = _SRC[start:start + 2500]
        assert "async_session_factory()" in b
        assert "delta_token:" in b  # retry label
        assert "token dropped, next incremental re-walks" in b, (
            "delta-token loss must be logged and swallowed — it must "
            "never convert a finished run into a reported failure"
        )
        assert "**(res_row.extra_data or {})" in b, (
            "fresh-dict assignment — in-place JSON mutation is not "
            "change-tracked without MutableDict"
        )


class TestBulkParentCoverageHonesty:
    def _block(self) -> str:
        start = _SRC.index("async def _finalize_bulk_parent_if_complete")
        return _SRC[start:start + 16000]

    def test_missing_cte_derives_from_batch_resource_ids(self):
        b = self._block()
        assert "unnest(j2.batch_resource_ids)" in b
        # sibling-coverage aware: a resource snapshotted under ANOTHER
        # job since dispatch (G2 dedup skip) is covered, not missing
        assert "OR s.created_at >= j2.created_at" in b

    def test_flip_time_result_includes_missing(self):
        b = self._block()
        assert "'snapshots_missing', m.n_missing" in b
        assert "'missing_resource_ids', m.ids" in b
        assert "c.failed + m.n_missing" in b, (
            "snapshots_failed must include resources that never got a row"
        )

    def test_honesty_applies_only_at_flip_time(self):
        b = self._block()
        m = re.search(
            r"WHEN c\.terminal >= e\.n AND p\.n_pending = 0\s*\n\s*AND m\.n_missing > 0",
            b,
        )
        assert m, (
            "mid-drain refreshes must NOT count still-queued resources "
            "as failed — the honesty overlay fires only when everything "
            "else settled"
        )

    def test_flip_gate_unchanged(self):
        b = self._block()
        assert "WHEN c.terminal < e.n THEN j.status" in b, (
            "missing resources must not make the parent flip mid-drain"
        )

    def test_flip_is_retried_on_fresh_sessions(self):
        b = self._block()
        assert "bulk_finalize:" in b
        flip = b.index("async def _flip")
        assert "async_session_factory()" in b[flip:flip + 300]


class TestStableDedupLockKey:
    def test_no_pythonhash_advisory_keys_left(self):
        assert not re.search(r"_lock_key\s*=\s*hash\(", _SRC), (
            "hash() is PYTHONHASHSEED-randomized per interpreter — "
            "cross-pod advisory exclusion never worked with it"
        )

    def test_key_matches_md5_derivation(self):
        expected = int.from_bytes(
            hashlib.md5(b"j-1:r-2").digest()[:8], "big"
        ) & 0x7FFFFFFFFFFFFFFF
        # Hard-pinned constant: proves the derivation is stable across
        # interpreters/pods (hash() could never satisfy this — it is
        # PYTHONHASHSEED-randomized per process).
        assert expected == 4579498002691798075
        assert 'hashlib.md5(' in _SRC and '_lock_key = int.from_bytes(' in _SRC
