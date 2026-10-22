"""Regression: calendar/contacts delta streams must record deletions and
must never keep resuming a Graph-expired (410) deltaLink.

Two bug classes found by the 2026-08-04 full-app review:

1. TOMBSTONES DROPPED. The USER_CALENDAR walk skipped ``@removed`` entries
   ("they don't carry a subject"). But the read path UNIONs CALENDAR_EVENT rows
   across sibling snapshots with newest-wins dedup, so an event's last live row
   wins forever: a deleted event stayed visible — and restorable — in every
   later restore point. Contacts already emitted tombstones; calendar now does
   too.

2. EXPIRED CURSOR NEVER EVICTED. When Graph expires a saved deltaLink it
   answers 410 resyncRequired. Both handlers treated that like any other error
   and KEPT the token (calendar left the old map entry; contacts explicitly
   fell back to prior_deltas), so every subsequent run resumed the same dead
   cursor, failed on page 1, and completed "successfully" with zero items for
   that calendar/folder — silently, forever. The OneDrive handler already
   dropped the token and re-walked; these two now do the same.

Source-inspection tests: both code paths live inside multi-thousand-line
handler bodies that cannot be imported in isolation.
"""
from __future__ import annotations

import pathlib
import re

_SRC = (
    pathlib.Path(__file__).resolve().parents[2]
    / "workers" / "backup-worker" / "main.py"
).read_text()


def _region(anchor: str, size: int = 4000) -> str:
    i = _SRC.find(anchor)
    assert i != -1, f"anchor moved: {anchor!r}"
    return _SRC[i:i + size]


def test_calendar_emits_removed_tombstones():
    r = _region("cal_delta_link: Optional[str] = None")
    assert '"@removed" in e' in r, "calendar must still detect @removed blobs"
    assert '"is_deleted": True' in r, (
        "a removed event must persist a tombstone row — without it the read "
        "path's newest-wins union shows the deleted event forever"
    )
    assert '"(deleted event)"' in r
    # The old unconditional skip must be gone.
    assert 'if "@removed" in e or not e.get("id"):\n                                        continue' not in r, (
        "the blanket @removed skip must not come back"
    )


def test_calendar_evicts_410_expired_delta_tokens():
    r = _region("cal_delta_link: Optional[str] = None", 6000)
    assert "resyncrequired" in r.lower() and "_expired_delta_cal_ids" in r, (
        "a 410 must mark the calendar's token for eviction"
    )
    # And the persist step must actually evict before merging.
    p = _region('merged_deltas = dict(ed.get("calendar_delta_tokens")', 900)
    assert "_expired_delta_cal_ids" in p and "pop(" in p, (
        "expired tokens must be popped BEFORE the merge, or the stale value "
        "survives and the dead cursor is resumed again"
    )


def test_calendar_does_not_advance_cursor_on_a_failed_stream():
    r = _region("cal_delta_link: Optional[str] = None", 6000)
    assert re.search(r"if cal_delta_link and not cal_stream_failed", r), (
        "a partial walk's terminal deltaLink would skip everything the run missed"
    )


def test_contacts_signals_410_expiry_to_its_caller():
    r = _region("async def _fetch_contacts_delta")
    assert "expired_delta_keys" in r and "resyncrequired" in r.lower(), (
        "contacts must flag a 410-expired source key instead of silently "
        "falling back to the dead token"
    )
    assert "return out_rows, new_deltas, expired_delta_keys" in _SRC, (
        "the expiry set must reach the caller"
    )
    c = _region("out_rows, _new_contact_deltas, _expired_contact_deltas")
    assert "pop(_dead_key, None)" in c, "caller must evict expired keys before merging"


def test_chat_carry_forward_matches_the_type_writers_actually_persist():
    """Message pointer rows are persisted as TEAMS_CHAT_MESSAGE; the
    carry-forward filter listed only 'CHAT_MESSAGE', so it silently
    carried attachments but never messages — snapshots were not the
    point-in-time supersets retention assumes when reaping predecessors."""
    r = _region("FROM snapshot_items src")
    assert "'TEAMS_CHAT_MESSAGE'" in r, (
        "carry-forward must match the item_type the writers use"
    )


def test_partitioned_sharepoint_loss_becomes_snapshot_partial_without_a_new_status():
    """A partitioned site never calls complete_snapshot — the last finisher
    derives terminal status from snapshot_partitions — so a shard that lost
    files must still stop the parent claiming COMPLETED.

    It must NOT do that by inventing a new partition status: eight
    consumers across both deploy lineages (finalizer terminal-counts, job
    rollups, reconciler orphan derivation, batch rollups, scheduler
    sweeps) read any value outside COMPLETED/FAILED as "still in flight",
    which wedges snapshots and Activity rows forever — that regression was
    caught in review round 3. The loss rides in failure_state (a JSONB
    column nobody branches on) and is aggregated into snapshot-level
    PARTIAL, which every consumer already understands."""
    r = _region('shard_items = int((sp_result or {}).get("item_count")', 2200)
    assert 'status="COMPLETED"' in r, (
        "partition status must stay inside the two-value terminal domain"
    )
    assert '"files_failed": _shard_failed' in r and "failure_state=_shard_fs" in r, (
        "the shard must record its loss in failure_state"
    )
    assert '"PARTIAL"' not in r, "no new partition status value"

    # backup_sharepoint must expose the counters for that to be possible.
    ret = _region('"new_delta_token": stats["new_delta"],', 900)
    assert '"files_failed": _sp_failed' in ret

    # The finalizer aggregates failure_state — and checks it BEFORE the
    # all-completed branch, or a lossy-but-finished shard set derives
    # COMPLETED and the fix is inert (this exact ordering bug happened).
    # Both derivation sites must count it: the primary CTE (whose value is
    # the FIRST committed status) and the in-Python integrity re-derivation.
    #
    # Counted via the `AS lossy` projection rather than the raw jsonb_typeof
    # predicate: that predicate is now also used by the `AS lost_files` SUM
    # added 2026-08-13, which is the MAGNITUDE feeding degraded_reason and
    # lives inside the second derivation — not a third derivation site.
    # Counting the predicate made this assert fail on a change that left both
    # derivations fully intact.
    assert _SRC.count(") AS lossy,") == 2, (
        "the finalizer has two derivations; both need the lossy term"
    )
    d = _region("_d_lossy = int(", 900)
    order = d.find("_d_failed > 0 or _d_lossy > 0")
    allc = d.find("_d_completed == _d_n")
    assert order != -1 and (allc == -1 or order < allc), (
        "lossy must be evaluated before all-completed, or a lossy set that "
        "reports COMPLETED derives COMPLETED and the fix is inert"
    )


def test_reconciler_orphan_derivation_also_honors_shard_losses():
    """The reconciler finalizes ORPHANED snapshots (worker died before its
    own finalize), so it must apply the same rule or a dead run's lossy
    shards are derived COMPLETED. Both lineages carry a copy."""
    import pathlib
    for repo in (pathlib.Path(__file__).resolve().parents[2],):
        src = (repo / "shared" / "reconciler.py").read_text()
        # STEP B (orphan derivation): lossy checked before all-COMPLETED.
        i = src.find("jsonb_typeof(sp.failure_state::jsonb -> 'files_failed')")
        assert i != -1, f"{repo}: reconciler does not consult failure_state"
        # Scope to the STEP B statement and strip `--` comments, rather than
        # slicing a fixed byte window around the match. A window breaks the
        # moment anyone documents a branch: the 2026-08-10 degraded_reason
        # term pushed the all-COMPLETED branch past i+900, so `find` returned
        # -1 and the ordering assertion read `941 < -1`.
        import re as _re
        _b0 = src.find("UPDATE snapshots s")
        _b1 = src.find("RETURNING s.id, s.status::text", _b0)
        seg = "\n".join(
            _re.sub(r"--.*$", "", ln) for ln in src[_b0:_b1].splitlines()
        )
        assert "'PARTIAL'::snapshotstatus" in seg
        assert seg.find("files_failed") < seg.find("bool_and(sp.status = 'COMPLETED')"), (
            f"{repo}: the lossy check must precede the all-COMPLETED branch"
        )
        # STEP D (integrity sweep) must ALSO honor it — it re-derives status
        # for every partitioned snapshot every 60s and was silently
        # reverting the finalizer's PARTIAL back to COMPLETED.
        assert "any_lossy" in src, f"{repo}: STEP D has no lossy term"
        # Same reasoning as above: bound by the statement, not a byte count.
        d_i = src.find("AS any_lossy")
        d_end = src.find("RETURNING s.id, s.status::text AS new_status", d_i)
        assert d_end != -1, f"{repo}: could not delimit STEP D"
        d_seg = "\n".join(
            _re.sub(r"--.*$", "", ln) for ln in src[d_i:d_end].splitlines()
        )
        assert d_seg.find("pa.any_failed OR pa.any_lossy") < d_seg.find("pa.all_completed"), (
            f"{repo}: STEP D must evaluate lossy before all_completed"
        )
        # Both CASE copies (SET clause + the WHERE guard) must agree, or the
        # guard re-selects the row forever.
        assert src.count("WHEN pa.any_failed OR pa.any_lossy") == 2, (
            f"{repo}: STEP D has two CASE copies; both need the lossy term"
        )


def test_finalizer_primary_committed_status_is_already_partial():
    """The finalizer commits a terminal status in ONE transaction and only
    re-derived in a later one. A lossy set committed COMPLETED first, so the
    false COMPLETED was durably visible to retention and the read path —
    and permanent if the second transaction never ran."""
    r = _region("WHEN c.terminal < e.n THEN s.status", 700)
    assert "c.lossy > 0" in r, (
        "the FIRST committed derivation must carry the lossy term"
    )
