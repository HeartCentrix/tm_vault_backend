"""Tests for the *keep-set* safety rules in retention_cleanup.

Three production data-loss bugs, all in shared/retention_cleanup.py:

  1. The GFS / FLAT / archived keep-set selectors were snapshot-STATUS-
     BLIND: a newer worthless row (a FAILED attempt, or a SKIP_CONCURRENT
     placeholder the backup-worker writes as COMPLETED with item_count=0)
     claimed the calendar bucket, pushing the day's REAL data-bearing
     snapshot outside the keep set → hard-deleted with its items.
  2. No anchor-baseline protection survived in practice — the delta
     chain's de facto baseline aged out of the daily window and was
     hard-deleted (the 124GB → 63-files incident).
  3. enforce_retention_for_tenant selected snapshots with no status and
     no lease filter, so an IN_PROGRESS walk holding a LIVE lease could
     be hard-deleted mid-run (observed 2026-07-29).

Style follows tests/shared/test_retention_archived_branch.py: lightweight
stand-ins with the same attribute shape as the ORM rows the production
code reads, plus a stubbed AsyncSession for the tenant-level entry point.
"""
from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Set

import pytest

from shared.retention_cleanup import (
    _archived_keep_ids,
    _baseline_keep_ids,
    _delete_snapshots,
    _flat_keep_ids,
    _gfs_keep_ids,
    enforce_retention_for_tenant,
)


# ── snapshot stand-ins ──────────────────────────────────────────────────

def _snap(
    id_str: str,
    *,
    hours_ago: float = 0,
    days_ago: float = 0,
    status: str = "COMPLETED",
    item_count: Optional[int] = 500,
    bytes_total: Optional[int] = None,
    skipped_concurrent: bool = False,
    snap_type: str = "INCREMENTAL",
    lease_expires_at: Optional[datetime] = None,
    hc_drain_status: str = "NOT_APPLICABLE",
):
    """Stand-in Snapshot ORM row — the fields the keep-set selectors read."""
    ts = datetime.utcnow() - timedelta(days=days_ago, hours=hours_ago)
    extra: Dict[str, Any] = {}
    if skipped_concurrent:
        extra["skipped_concurrent"] = True
    return SimpleNamespace(
        id=id_str,
        started_at=ts,
        created_at=ts,
        status=status,
        type=snap_type,
        item_count=item_count,
        bytes_total=bytes_total,
        extra_data=extra,
        reuse_of_snapshot_id=None,
        reuse_chain_root_id=None,
        lease_expires_at=lease_expires_at,
        hc_drain_status=hc_drain_status,
    )


def _failed_mid_walk(id_str: str, **kw):
    """A snapshot the mail/chats stream-persist path left FAILED *with real
    persisted snapshot_items*: the handler writes item_count/bytes_total
    mid-walk (workers/backup-worker/main.py ~4374, ~6929) and a later
    EnumerationFailed flips the row to FAILED (~7826) WITHOUT deleting the
    items it already persisted."""
    kw.setdefault("status", "FAILED")
    return _snap(id_str, **kw)


def _skip_snap(id_str: str, **kw):
    """The SKIP_CONCURRENT placeholder backup-worker writes 3x/day on Gold:
    COMPLETED, zero items, extra_data.skipped_concurrent=true."""
    kw.setdefault("status", "COMPLETED")
    return _snap(id_str, item_count=0, skipped_concurrent=True, **kw)


def _policy(**kw):
    base = dict(
        id=uuid.uuid4(),
        tenant_id=None,
        is_default=True,
        retention_mode="FLAT",
        retention_days=None,
        retention_hot_days=0,
        retention_cool_days=0,
        retention_archive_days=0,
        gfs_daily_count=0,
        gfs_weekly_count=0,
        gfs_monthly_count=0,
        gfs_yearly_count=0,
        archived_retention_mode="SAME",
        archived_retention_days=None,
        legal_hold_enabled=False,
        legal_hold_until=None,
        immutability_mode="None",
    )
    base.update(kw)
    return SimpleNamespace(**base)


# ── BUG 1: status-blind bucket winners ──────────────────────────────────

def test_gfs_skip_snapshot_does_not_steal_the_daily_bucket():
    """Gold dispatches 3x/day; the evening run lands as a SKIP_CONCURRENT
    placeholder. It must not displace the morning's real backup."""
    real = _snap("real-today", hours_ago=18, item_count=41_000)
    skip = _skip_snap("skip-today", hours_ago=2)
    keep = _gfs_keep_ids([real, skip], _policy(retention_mode="GFS", gfs_daily_count=1))
    assert "real-today" in keep, (
        "the day's real data-bearing snapshot lost its daily bucket to a "
        "zero-item skip placeholder"
    )


def test_gfs_failed_row_does_not_steal_the_daily_bucket():
    real = _snap("real-today", hours_ago=20, item_count=1_200)
    failed = _snap("failed-today", hours_ago=1, status="FAILED", item_count=0)
    keep = _gfs_keep_ids([real, failed], _policy(retention_mode="GFS", gfs_daily_count=1))
    assert "real-today" in keep


def test_gfs_multi_day_window_keeps_every_real_daily():
    """14-daily window, two dispatches a day: real backup in the morning,
    skip placeholder in the evening. Every real daily must survive."""
    snaps: List[Any] = []
    for d in range(3):
        snaps.append(_snap(f"real-d{d}", days_ago=d, hours_ago=18, item_count=9_000))
        snaps.append(_skip_snap(f"skip-d{d}", days_ago=d, hours_ago=2))
    snaps.append(_snap("failed-d1", days_ago=1, hours_ago=1, status="FAILED", item_count=0))

    keep = _gfs_keep_ids(snaps, _policy(retention_mode="GFS", gfs_daily_count=3))
    missing = {f"real-d{d}" for d in range(3)} - keep
    assert not missing, f"real dailies dropped out of the keep set: {sorted(missing)}"


def test_flat_latest_safety_net_lands_on_a_real_backup():
    """FLAT's 'always keep the most recent' net protected whatever row was
    newest — even a FAILED one — so every real backup outside the window
    got deleted and the resource went empty."""
    snaps = [
        _snap("real-old", days_ago=30, item_count=60_000),
        _snap("real-older", days_ago=45, item_count=58_000),
        _snap("failed-today", hours_ago=1, status="FAILED", item_count=0),
    ]
    keep = _flat_keep_ids(snaps, _policy(retention_days=7))
    assert "real-old" in keep, (
        "the latest-snapshot safety net protected a FAILED row instead of "
        "the newest recoverable backup — resource would go empty"
    )


def test_archived_keep_last_keeps_a_real_backup():
    snaps = [_snap("real", days_ago=3, item_count=7_000), _skip_snap("skip", hours_ago=1)]
    keep = _archived_keep_ids(snaps, _policy(archived_retention_mode="KEEP_LAST"))
    assert "real" in keep


def test_legitimately_empty_resource_still_keeps_its_snapshots():
    """A genuinely empty mailbox produces real COMPLETED 0-item snapshots.
    Those are the true state — the keep set must not collapse to nothing."""
    snaps = [_snap("empty-today", item_count=0),
             _snap("empty-yesterday", item_count=0)]
    # Different calendar buckets even during the first hour after UTC midnight.
    for snapshot, day in zip(snaps, (3, 2)):
        snapshot.started_at = snapshot.created_at = datetime(2026, 1, day, 12)
    keep = _gfs_keep_ids(snaps, _policy(retention_mode="GFS", gfs_daily_count=2))
    assert keep == {"empty-today", "empty-yesterday"}


# ── stubbed session for the tenant-level entry point ────────────────────

class _FakeResult:
    def __init__(self, rows: List[Any]):
        self._rows = list(rows)

    def scalars(self):
        return self

    def all(self):
        return list(self._rows)

    def fetchall(self):
        return list(self._rows)


class _FakeSession:
    """Serves the three reads enforce_retention_for_tenant performs:
    policies, keyset-paged resources (the id page, then that page's rows —
    shared/retention_cleanup._iter_tenant_resources), per-resource
    snapshots. Deliberately no stream(): the sweep must not open a
    server-side cursor (tests/shared/test_retention_keyset_pagination.py)."""

    def __init__(self, policies: List[Any], resources: List[Any],
                 snaps_by_resource: Dict[Any, List[Any]]):
        self._policies = policies
        self._resources = resources
        self._snaps = snaps_by_resource
        self.commits = 0

    async def execute(self, statement, *a, **kw):
        sql = str(statement)
        if "sla_policies" in sql:
            return _FakeResult(self._policies)
        if "FROM resources" in sql:
            if "LIMIT" in sql:  # the id page
                return _FakeResult([r.id for r in self._resources])
            return _FakeResult(list(self._resources))  # that page's rows
        if "snapshots" in sql:
            rid = statement.compile().params.get("resource_id_1")
            return _FakeResult(self._snaps.get(rid, []))
        return _FakeResult([])

    async def commit(self):
        self.commits += 1


def _run_enforce(monkeypatch, policies, resources, snaps_by_resource) -> Set[Any]:
    """Run the real enforce_retention_for_tenant against stubs and return
    the set of snapshot ids it decided to delete."""
    import shared.retention_cleanup as rc

    deleted: Set[Any] = set()

    async def _fake_delete(session, snap_ids, *, audit_events=None):
        deleted.update(snap_ids)
        return len(snap_ids)

    monkeypatch.setattr(rc, "_delete_snapshots", _fake_delete)
    monkeypatch.setattr(rc.settings, "RETENTION_DELETE_ENABLED", True)
    session = _FakeSession(policies, resources, snaps_by_resource)
    asyncio.run(rc.enforce_retention_for_tenant(session, uuid.uuid4()))
    return deleted


def _resource(status: str = "ACTIVE", policy_id=None):
    return SimpleNamespace(id=uuid.uuid4(), status=status, sla_policy_id=policy_id)


# ── BUG 2: the anchor baseline is never protected ───────────────────────

def test_de_facto_baseline_survives_the_daily_window(monkeypatch):
    """Production types ZERO snapshots FULL (create_snapshot defaults
    INCREMENTAL), so the oldest data-bearing snapshot IS the baseline the
    whole delta chain resolves against. It must never be deleted by age."""
    pol = _policy(retention_mode="GFS", gfs_daily_count=14)
    res = _resource(policy_id=pol.id)
    snaps = [_snap("onedrive-baseline", days_ago=90, item_count=124_000)]
    snaps += [_snap(f"daily-{d}", days_ago=d, item_count=800) for d in range(14)]

    deleted = _run_enforce(monkeypatch, [pol], [res], {res.id: snaps})
    assert "onedrive-baseline" not in deleted, (
        "the delta chain's anchor baseline aged out and was hard-deleted"
    )


def test_baseline_protection_ignores_a_newer_worthless_full(monkeypatch):
    """A newer FULL that FAILED (or finalized 0 items) must not shadow the
    real COMPLETED baseline out of its protection."""
    pol = _policy(retention_days=7)
    res = _resource(policy_id=pol.id)
    snaps = [
        _snap("good-full", days_ago=60, snap_type="FULL", item_count=99_000),
        _snap("bad-full", days_ago=30, snap_type="FULL", status="FAILED", item_count=0),
        _snap("daily-0", hours_ago=2, item_count=400),
    ]
    deleted = _run_enforce(monkeypatch, [pol], [res], {res.id: snaps})
    assert "good-full" not in deleted, (
        "a FAILED newer FULL shadowed the real baseline out of protection"
    )


# ── BUG 3: in-flight / leased snapshots deleted mid-run ─────────────────

def test_in_progress_snapshot_with_live_lease_is_never_deleted(monkeypatch):
    """A multi-hour walk still holding a live lease was hard-deleted mid-run,
    FK-failing the worker's inserts and orphaning its uploaded blobs."""
    pol = _policy(retention_days=7)
    res = _resource(policy_id=pol.id)
    snaps = [
        _snap("running", days_ago=40, status="IN_PROGRESS", item_count=12_000,
              lease_expires_at=datetime.now(timezone.utc) + timedelta(minutes=8)),
        _snap("recent", hours_ago=2, item_count=900),
        _snap("baseline", days_ago=90, item_count=50_000),
    ]
    deleted = _run_enforce(monkeypatch, [pol], [res], {res.id: snaps})
    assert "running" not in deleted, (
        "hard-deleted an IN_PROGRESS snapshot whose lease is still live"
    )


def test_completed_snapshot_with_live_lease_is_never_deleted(monkeypatch):
    """Terminal status but a live lease means a finalizer/reconciler is
    still writing to the row — deleting it races that writer."""
    pol = _policy(retention_days=7)
    res = _resource(policy_id=pol.id)
    snaps = [
        _snap("finalizing", days_ago=40, item_count=3_000,
              lease_expires_at=datetime.now(timezone.utc) + timedelta(minutes=5)),
        _snap("recent", hours_ago=2, item_count=900),
        _snap("baseline", days_ago=90, item_count=50_000),
    ]
    deleted = _run_enforce(monkeypatch, [pol], [res], {res.id: snaps})
    assert "finalizing" not in deleted


def test_non_terminal_snapshot_without_lease_is_left_for_the_reconciler(monkeypatch):
    """An abandoned IN_PROGRESS row with an expired lease is the
    reconciler's job to flip to FAILED — retention must not silently
    hard-delete a row a worker may still be mid-insert against."""
    pol = _policy(retention_days=7)
    res = _resource(policy_id=pol.id)
    snaps = [
        _snap("abandoned", days_ago=40, status="IN_PROGRESS", item_count=0,
              lease_expires_at=datetime.now(timezone.utc) - timedelta(hours=3)),
        _snap("recent", hours_ago=2, item_count=900),
        _snap("baseline", days_ago=90, item_count=50_000),
    ]
    deleted = _run_enforce(monkeypatch, [pol], [res], {res.id: snaps})
    assert "abandoned" not in deleted


def test_stale_terminal_snapshots_are_still_deleted(monkeypatch):
    """Guard against over-correcting into a never-delete no-op: an old
    terminal snapshot with no live lease, outside the window and not the
    baseline, must still be pruned."""
    pol = _policy(retention_days=7)
    res = _resource(policy_id=pol.id)
    snaps = [
        _snap("baseline", days_ago=90, item_count=50_000),
        _snap("stale-mid", days_ago=40, item_count=7_000),
        _snap("recent", hours_ago=2, item_count=900),
    ]
    deleted = _run_enforce(monkeypatch, [pol], [res], {res.id: snaps})
    assert deleted == {"stale-mid"}


# ── REVIEW FINDING 1: a FAILED row can hold REAL persisted items ────────
#
# The stream-persist handlers set snap.item_count / bytes_total MID-walk and
# a later EnumerationFailed flips the row to FAILED without deleting the
# items already written. Excluding such a row from bucket eligibility is a
# NET-NEW deletion path: under GFS the buckets ARE the keep set (no age
# cutoff), so a row that wins no bucket is hard-deleted.

def _daily_wall(n_days: int, *, item_count: int = 800) -> List[Any]:
    """One ordinary COMPLETED daily per day for `n_days` days."""
    return [_snap(f"daily-{d}", days_ago=d, item_count=item_count)
            for d in range(n_days)]


def test_failed_snapshot_holding_persisted_items_is_not_deleted(monkeypatch):
    """A mid-walk FAILED row with 900 real snapshot_items sits inside the
    14-day daily window and is the only snapshot for its day. It must win
    that day's bucket instead of forfeiting it to an older day."""
    pol = _policy(retention_mode="GFS", gfs_daily_count=14)
    res = _resource(policy_id=pol.id)
    snaps = _daily_wall(20)
    snaps = [s for s in snaps if s.id != "daily-2"]
    snaps.append(_failed_mid_walk("failed-day2", days_ago=2, item_count=900))
    # A clearly-largest, clearly-oldest baseline so the anchor rule cannot
    # protect the FAILED row for an unrelated reason.
    snaps.append(_snap("baseline", days_ago=40, item_count=200_000))

    deleted = _run_enforce(monkeypatch, [pol], [res], {res.id: snaps})
    assert "failed-day2" not in deleted, (
        "hard-deleted a FAILED snapshot that holds 900 real persisted items "
        "— it lost its daily bucket purely because of its status"
    )


def test_failed_snapshot_with_only_bytes_recorded_is_not_deleted(monkeypatch):
    """Same shape, but the walk died before item_count was written and only
    bytes_total moved. Positive evidence of persisted content, either
    column, must keep the row bucket-eligible."""
    pol = _policy(retention_mode="GFS", gfs_daily_count=14)
    res = _resource(policy_id=pol.id)
    snaps = [s for s in _daily_wall(20) if s.id != "daily-2"]
    snaps.append(_failed_mid_walk("failed-day2", days_ago=2,
                                  item_count=None, bytes_total=5_000_000))
    snaps.append(_snap("baseline", days_ago=40, item_count=200_000))

    deleted = _run_enforce(monkeypatch, [pol], [res], {res.id: snaps})
    assert "failed-day2" not in deleted, (
        "hard-deleted a FAILED snapshot whose bytes_total proves it holds "
        "persisted content"
    )


def test_failed_partial_does_not_displace_the_same_day_completed_backup():
    """Re-opening bug (1) from the other side: once a FAILED row that holds
    items is bucket-eligible, the newest-wins rule would let a 900-item
    18:07 failure displace the day's 9,000-item COMPLETED backup. The
    restorable row must win the bucket."""
    real = _snap("real-today", hours_ago=18, item_count=9_000)
    failed = _failed_mid_walk("failed-today", hours_ago=1, item_count=900)
    keep = _gfs_keep_ids([real, failed], _policy(retention_mode="GFS", gfs_daily_count=1))
    assert "real-today" in keep, (
        "a newer FAILED partial stole the daily bucket from the same day's "
        "COMPLETED backup"
    )


def test_flat_latest_net_prefers_the_newest_restorable_over_a_failed_partial():
    """FLAT's 'never let the resource go empty' net must land on the newest
    fully-restorable snapshot, not on a newer FAILED partial."""
    snaps = [
        _snap("real-old", days_ago=30, item_count=60_000),
        _failed_mid_walk("failed-yesterday", days_ago=1, item_count=900),
    ]
    keep = _flat_keep_ids(snaps, _policy(retention_days=7))
    assert "real-old" in keep, (
        "latest-snapshot net protected a FAILED partial and let the newest "
        "COMPLETED backup be deleted"
    )


def test_zero_item_placeholders_still_cannot_win_a_bucket():
    """Guard on the reordering: positive-evidence-wins must not let a
    0-item FAILED row or a SKIP_CONCURRENT placeholder back into the
    competition (that is bug 1)."""
    real = _snap("real-today", hours_ago=20, item_count=1_200)
    failed0 = _snap("failed-empty", hours_ago=2, status="FAILED", item_count=0,
                    bytes_total=0)
    skip = _skip_snap("skip-today", hours_ago=1)
    keep = _gfs_keep_ids([real, failed0, skip],
                         _policy(retention_mode="GFS", gfs_daily_count=1))
    assert "real-today" in keep


# ── REVIEW FINDING 2: "oldest data-bearing" is the wrong anchor ─────────
#
# Verified against production 2026-07-31: for 4 of 6 SLA'd OneDrive
# resources the real baseline IS the oldest data-bearing row, but for
# jrue (8,239 items / 124 GB, oldest_rank=13) and jlucas (198,992 items /
# 199 GB, oldest_rank=8) the oldest data-bearing row is a tiny stub
# (5 items / 17 MB and 2 items / 31 MB). "Largest by item_count"
# identifies all six. The anchor set is the UNION of the plausible
# anchors, which is what makes it robust.

@pytest.mark.parametrize("label,stub_items,stub_days,base_items,base_days", [
    # modelled on Jason Rue: 5-item/17MB stub 06-30, 8,239-item/124GB 07-30
    ("jrue", 5, 40, 8_239, 20),
    # modelled on John Lucas: 2-item/31MB stub 07-12, 198,992-item/199GB 07-30
    ("jlucas", 2, 30, 198_992, 20),
])
def test_largest_data_bearing_baseline_survives_the_daily_window(
    monkeypatch, label, stub_items, stub_days, base_items, base_days,
):
    """The real baseline outlives the 14-daily GFS window only if the anchor
    rule finds it. With a tiny OLDER stub present, 'oldest data-bearing'
    protects the stub and lets the 124/199 GB baseline be hard-deleted."""
    pol = _policy(retention_mode="GFS", gfs_daily_count=14)
    res = _resource(policy_id=pol.id)
    snaps = _daily_wall(14, item_count=400)
    snaps.append(_snap(f"{label}-stub", days_ago=stub_days, item_count=stub_items,
                       bytes_total=17_000_000))
    snaps.append(_snap(f"{label}-baseline", days_ago=base_days,
                       item_count=base_items, bytes_total=124_000_000_000))

    deleted = _run_enforce(monkeypatch, [pol], [res], {res.id: snaps})
    assert f"{label}-baseline" not in deleted, (
        f"the real {base_items}-item baseline aged out of the daily window "
        f"while the {stub_items}-item stub kept the anchor protection"
    )
    assert f"{label}-stub" not in deleted, (
        "the oldest data-bearing row is also a plausible chain root and is "
        "cheap to keep"
    )


def test_baseline_anchor_is_the_union_of_full_largest_and_oldest():
    """The documented rule, pinned: newest data-bearing FULL + largest
    data-bearing by item_count + oldest data-bearing."""
    snaps = [
        _snap("oldest", days_ago=90, item_count=12),
        _snap("largest", days_ago=40, item_count=250_000),
        _snap("newest-full", days_ago=10, snap_type="FULL", item_count=700),
        _snap("older-full", days_ago=60, snap_type="FULL", item_count=650),
        _snap("noise", days_ago=2, item_count=300),
    ]
    assert _baseline_keep_ids(snaps) == {"oldest", "largest", "newest-full"}


def test_baseline_anchor_covers_largest_by_bytes_when_item_count_is_null():
    """A baseline whose item_count never got written but whose bytes_total
    did must still be reachable as an anchor."""
    snaps = [
        _snap("small-oldest", days_ago=90, item_count=3, bytes_total=1_000),
        _snap("bytes-baseline", days_ago=40, item_count=None,
              bytes_total=124_000_000_000),
        _snap("recent", days_ago=1, item_count=400, bytes_total=2_000),
    ]
    assert "bytes-baseline" in _baseline_keep_ids(snaps)


def test_baseline_anchor_never_lands_on_a_placeholder():
    """Anchor selection must stay data-aware: a 0-item skip placeholder or
    FAILED-empty row must never be picked as the chain root."""
    snaps = [
        _skip_snap("skip-oldest", days_ago=120),
        _snap("failed-empty", days_ago=110, status="FAILED", item_count=0,
              bytes_total=0),
        _snap("real-oldest", days_ago=90, item_count=5_000),
        _snap("recent", days_ago=1, item_count=400),
    ]
    anchors = _baseline_keep_ids(snaps)
    assert "skip-oldest" not in anchors and "failed-empty" not in anchors
    assert "real-oldest" in anchors


def test_baseline_anchor_tie_breaks_to_the_older_row():
    """Two rows with identical item_count: the older one is the chain root."""
    snaps = [
        _snap("root", days_ago=60, item_count=9_000),
        _snap("clone", days_ago=30, item_count=9_000),
    ]
    assert "root" in _baseline_keep_ids(snaps)


# ── REVIEW FINDING 4: further liveness signals + delete-time predicate ──

def test_hc_drain_pending_snapshot_is_never_deleted(monkeypatch):
    """hc_drain_status=PENDING means the detached hostedContent drain is
    still inserting snapshot_items into a row whose status already reads
    COMPLETED (workers/backup-worker/main.py ~7196). Deleting it FK-fails
    those inserts — the same failure mode as the non-terminal guard."""
    pol = _policy(retention_days=7)
    res = _resource(policy_id=pol.id)
    snaps = [
        _snap("draining", days_ago=40, item_count=3_000, hc_drain_status="PENDING"),
        _snap("recent", hours_ago=2, item_count=900),
        _snap("baseline", days_ago=90, item_count=50_000),
    ]
    deleted = _run_enforce(monkeypatch, [pol], [res], {res.id: snaps})
    assert "draining" not in deleted, (
        "hard-deleted a snapshot whose hostedContent drain is still in flight"
    )


class _RecordingDeleteSession:
    """Captures what _delete_snapshots actually issues: the guard SELECT's
    verdict is supplied by the test, the DELETE params are recorded."""

    def __init__(self, deletable_ids: List[Any]):
        self._deletable = list(deletable_ids)
        self.deleted_items: List[Any] = []
        self.deleted_snaps: List[Any] = []
        self.guard_selected = False
        self.added = []

    def add(self, event):
        self.added.append(event)

    async def execute(self, statement, params=None, *a, **kw):
        sql = str(statement)
        if sql.strip().upper().startswith("SELECT"):
            self.guard_selected = True
            return _FakeResult([(i,) for i in self._deletable])
        if sql.startswith("DELETE FROM snapshot_items"):
            self.deleted_items = list(statement.compile().params.get("snapshot_id_1") or [])
            return SimpleNamespace(rowcount=len(self.deleted_items))
        if sql.startswith("DELETE FROM snapshots"):
            self.deleted_snaps = list(statement.compile().params.get("id_1") or [])
            return SimpleNamespace(rowcount=len(self.deleted_snaps))
        return _FakeResult([])


def test_delete_re_asserts_the_in_flight_guard_at_delete_time(monkeypatch):
    """The read-time guard is not enough: a worker can claim a snapshot
    between the SELECT and the DELETE (observed 2026-07-29). The DELETE
    must carry its own status/lease predicate, and BOTH the items delete
    and the snapshots delete must be narrowed to the surviving set — never
    the items of a row we then decline to delete."""
    import shared.retention_cleanup as rc

    doomed_a = uuid.uuid4()
    went_live_b = uuid.uuid4()
    session = _RecordingDeleteSession([doomed_a])

    rehydrated: List[Any] = []

    async def _fake_rehydrate(sess, sid, *, doomed_ids):
        rehydrated.append(sid)
        return None

    monkeypatch.setattr(rc, "_rehydrate_reuse_heir", _fake_rehydrate)
    events = {sid: SimpleNamespace(snapshot_id=sid) for sid in (doomed_a, went_live_b)}
    n = asyncio.run(rc._delete_snapshots(
        session, {doomed_a, went_live_b}, audit_events=events,
    ))
    assert [event.snapshot_id for event in session.added] == [doomed_a]

    assert session.guard_selected, "no delete-time predicate was issued"
    assert session.deleted_snaps == [doomed_a], (
        f"deleted {session.deleted_snaps} — a row that went live after the "
        f"read must be left alone"
    )
    assert session.deleted_items == [doomed_a], (
        "snapshot_items were deleted for a snapshot row we declined to "
        "delete — that orphans the surviving row's inventory"
    )
    assert rehydrated == [doomed_a]
    assert n == 1


def test_pending_deletion_row_is_never_resurrected_by_the_keep_set():
    """PENDING_DELETION *asks* for the row to go away. Positive evidence of
    content must not override an explicit deletion request — otherwise the
    largest-by-item_count anchor silently resurrects it forever."""
    snaps = [
        _snap("purge-me", days_ago=20, status="PENDING_DELETION",
              item_count=500_000, bytes_total=99_000_000_000),
        _snap("real", days_ago=10, item_count=1_000),
    ]
    assert _baseline_keep_ids(snaps) == {"real"}
    keep = _gfs_keep_ids(snaps, _policy(retention_mode="GFS", gfs_daily_count=14))
    assert "purge-me" not in keep
