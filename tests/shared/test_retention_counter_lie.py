"""Regression: retention must never conclude "this snapshot holds nothing"
from the item_count / bytes_total counters alone.

THE BUG (measured on prod 2026-08-11). Those two counters are written
MID-walk by the stream-persist handlers as a per-process running total and
are only restated from a real COUNT(*) on the success path, so a snapshot
whose owning process died keeps whatever the last flush left — routinely 0
— while still owning every snapshot_items row it committed. Production held
25 such rows over 270,885 real snapshot_items:

  * 9 FAILED ROOM_MAILBOX runs   → 189,692 items
  * 16 COMPLETED USER_CHATS runs →  81,193 items

none of them partitioned and every one with reuse_chain_root_id NULL, so
neither "a reuse row legitimately owns zero items" nor a shard artefact
explains them. ``_holds_persisted_content`` read only those counters, so
each row looked worthless: it could win no GFS bucket
(``_bucket_candidates``), was invisible to the anchor rule
(``_baseline_keep_ids``), and FAILED is not in ``_RESTORABLE_STATUSES`` —
so it forfeited its bucket and ``_delete_snapshots`` hard-deleted the
snapshot together with its items.

The fix has two halves and both are pinned here:
  1. ``_restate_lying_counters`` replaces a zeroed counter with the truth
     from snapshot_items BEFORE the keep set is computed (and persists it,
     which is how the 25 rows already in prod heal — the reconciler's
     settle statements can only fix rows they are settling, and all 25 are
     long since terminal);
  2. the ``_delete_snapshots`` guard re-asserts it against the database
     inside the delete transaction, so any caller that skips step 1 defers
     instead of deleting.

Style follows tests/shared/test_retention_keepset_safety.py: lightweight
stand-ins with the same attribute shape as the ORM rows, plus a stubbed
AsyncSession for the tenant-level entry point.
"""
from __future__ import annotations

import asyncio
import os
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Set

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

import shared.retention_cleanup as rc  # noqa: E402  (env must precede import)


# ── stand-ins ───────────────────────────────────────────────────────────

def _snap(
    id_str: str,
    *,
    days_ago: float = 0,
    hours_ago: float = 0,
    status: str = "COMPLETED",
    item_count: Optional[int] = 500,
    bytes_total: Optional[int] = None,
    skipped_concurrent: bool = False,
    lease_expires_at: Optional[datetime] = None,
    hc_drain_status: str = "NOT_APPLICABLE",
):
    ts = datetime.utcnow() - timedelta(days=days_ago, hours=hours_ago)
    extra: Dict[str, Any] = {}
    if skipped_concurrent:
        extra["skipped_concurrent"] = True
    return SimpleNamespace(
        id=id_str,
        started_at=ts,
        created_at=ts,
        status=status,
        type="INCREMENTAL",
        item_count=item_count,
        bytes_total=bytes_total,
        extra_data=extra,
        reuse_of_snapshot_id=None,
        reuse_chain_root_id=None,
        lease_expires_at=lease_expires_at,
        hc_drain_status=hc_drain_status,
    )


def _policy(**kw):
    base = dict(
        id=uuid.uuid4(),
        is_default=True,
        retention_mode="GFS",
        retention_days=None,
        retention_hot_days=None,
        retention_cool_days=None,
        retention_archive_days=None,
        gfs_daily_count=7,
        gfs_weekly_count=0,
        gfs_monthly_count=0,
        gfs_yearly_count=0,
        archived_retention_mode="SAME",
        archived_retention_days=None,
        legal_hold_enabled=False,
        legal_hold_until=None,
        immutability_mode=None,
    )
    base.update(kw)
    return SimpleNamespace(**base)


def _resource(policy_id=None):
    return SimpleNamespace(id=uuid.uuid4(), status="ACTIVE", sla_policy_id=policy_id)


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
    """Serves the four reads the sweep performs: policies, the keyset-paged
    resources (id page, then that page's rows — no stream(), see
    tests/shared/test_retention_keyset_pagination.py), that resource's
    snapshots, and the snapshot_items truth query
    ``_restate_lying_counters`` issues."""

    def __init__(self, policies, resources, snaps_by_resource,
                 items_by_snapshot: Optional[Dict[str, tuple]] = None):
        self._policies = policies
        self._resources = resources
        self._snaps = snaps_by_resource
        self._items = dict(items_by_snapshot or {})
        self.commits = 0
        self.counter_queries: List[List[str]] = []

    async def execute(self, statement, params=None, *a, **kw):
        sql = str(statement)
        if "FROM snapshot_items" in sql and "GROUP BY si.snapshot_id" in sql:
            asked = list((params or {}).get("ids") or [])
            self.counter_queries.append(asked)
            return _FakeResult([
                (sid, n, b) for sid, (n, b) in self._items.items()
                if sid in asked
            ])
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


def _run_enforce(monkeypatch, policies, resources, snaps_by_resource,
                 items_by_snapshot=None) -> Set[Any]:
    """Run the real enforce_retention_for_tenant and return the ids it
    decided to delete."""
    deleted: Set[Any] = set()

    async def _fake_delete(session, snap_ids, *, audit_events=None):
        deleted.update(snap_ids)
        return len(snap_ids)

    monkeypatch.setattr(rc, "_delete_snapshots", _fake_delete)
    monkeypatch.setattr(rc.settings, "RETENTION_DELETE_ENABLED", True)
    session = _FakeSession(policies, resources, snaps_by_resource,
                           items_by_snapshot)
    asyncio.run(rc.enforce_retention_for_tenant(session, uuid.uuid4()))
    return deleted


# ── the prod population ─────────────────────────────────────────────────

def test_failed_room_mailbox_with_a_lying_zero_counter_is_not_deleted(monkeypatch):
    """The 9 FAILED ROOM_MAILBOX rows: item_count=0, 21,077 real EMAIL
    items each, older than the GFS daily window. Status-blind counters made
    them forfeit their bucket and they were hard-deleted with their items."""
    pol = _policy(retention_mode="GFS", gfs_daily_count=7)
    res = _resource(policy_id=pol.id)
    liar = _snap("room-mailbox-failed", days_ago=40, status="FAILED",
                 item_count=0, bytes_total=0)
    snaps = [liar] + [_snap(f"daily-{d}", days_ago=d, item_count=800)
                      for d in range(7)]

    deleted = _run_enforce(
        monkeypatch, [pol], [res], {res.id: snaps},
        items_by_snapshot={"room-mailbox-failed": (21_077, 9_400_000_000)},
    )
    assert "room-mailbox-failed" not in deleted, (
        "hard-deleted a snapshot holding 21,077 real snapshot_items because "
        "its item_count counter read 0"
    )
    assert liar.item_count == 21_077 and liar.bytes_total == 9_400_000_000, (
        "the counters were not restated, so the DB never self-heals and the "
        "next pass makes the same mistake"
    )


def test_completed_user_chats_with_a_lying_zero_counter_is_not_deleted(monkeypatch):
    """The 16 COMPLETED USER_CHATS rows: COMPLETED, item_count=0, ~5,075
    real chat-message items each. COMPLETED alone does not save them —
    _bucket_candidates only admits data-bearing rows, so a 0-counter
    COMPLETED row loses its day to any other row in the same day."""
    pol = _policy(retention_mode="GFS", gfs_daily_count=1)
    res = _resource(policy_id=pol.id)
    liar = _snap("chats-completed", hours_ago=6, item_count=0, bytes_total=0)
    snaps = [liar, _snap("chats-placeholder", hours_ago=1, item_count=0,
                         skipped_concurrent=True)]

    deleted = _run_enforce(
        monkeypatch, [pol], [res], {res.id: snaps},
        items_by_snapshot={"chats-completed": (5_075, 1_200_000)},
    )
    assert "chats-completed" not in deleted, (
        "a COMPLETED snapshot holding 5,075 chat messages was deleted "
        "because its counter read 0 while a SKIP_CONCURRENT placeholder "
        "took its bucket"
    )


def test_zero_item_placeholder_cleanup_still_works(monkeypatch):
    """The original purpose of the data-bearing test must survive: a
    SKIP_CONCURRENT placeholder owns no snapshot_items rows, so the truth
    query finds nothing, nothing is restated, and it still prunes."""
    pol = _policy(retention_mode="FLAT", retention_days=7)
    res = _resource(policy_id=pol.id)
    placeholder = _snap("placeholder", days_ago=30, item_count=0,
                        skipped_concurrent=True)
    snaps = [placeholder, _snap("real-recent", hours_ago=2, item_count=900),
             _snap("real-baseline", days_ago=90, item_count=50_000)]

    deleted = _run_enforce(monkeypatch, [pol], [res], {res.id: snaps},
                           items_by_snapshot={})
    assert "placeholder" in deleted, (
        "the 0-item placeholder cleanup regressed — an empty row outside "
        "the window is still garbage"
    )
    assert placeholder.item_count == 0


# ── _restate_lying_counters in isolation ────────────────────────────────

def _restate(snaps, items_by_snapshot):
    session = _FakeSession([], [], {}, items_by_snapshot)
    n = asyncio.run(rc._restate_lying_counters(session, snaps))
    return n, session


def test_restatement_never_lowers_a_counter_a_handler_wrote():
    """A handler that reported 40,000 items may legitimately have counted
    work that never became a snapshot_items row (pointer / dedup writes).
    Only rows with NO counter evidence at all are second-guessed."""
    honest = _snap("honest", days_ago=5, item_count=40_000, bytes_total=7)
    n, session = _restate([honest], {"honest": (12, 34)})
    assert n == 0 and honest.item_count == 40_000 and honest.bytes_total == 7
    assert session.counter_queries == [], (
        "issued the snapshot_items truth query for a row whose counters "
        "already claimed content — that is per-resource cost for nothing"
    )


def test_restatement_leaves_in_flight_rows_alone():
    """A live walk is still writing its own counters; retention must not
    race it. Those rows are never deletable anyway
    (_is_protected_in_flight), so there is nothing to fail safe about."""
    running = _snap("running", days_ago=1, status="IN_PROGRESS", item_count=0,
                    bytes_total=0)
    leased = _snap("leased", days_ago=1, item_count=0, bytes_total=0,
                   lease_expires_at=datetime.now(timezone.utc) + timedelta(minutes=8))
    draining = _snap("draining", days_ago=1, item_count=0, bytes_total=0,
                     hc_drain_status="PENDING")
    n, session = _restate(
        [running, leased, draining],
        {"running": (5, 5), "leased": (5, 5), "draining": (5, 5)},
    )
    assert n == 0
    assert session.counter_queries == []


def test_pending_deletion_is_restated_so_its_purge_is_not_deferred_forever():
    """PENDING_DELETION asks for the row to go away, and the delete-time
    guard refuses any row whose counters read empty while snapshot_items
    exist — so leaving a purge-requested row's counters lying would defer
    the purge on every pass, forever. Restating cannot resurrect it:
    _holds_recoverable_data rejects PENDING_DELETION before it consults the
    counters at all."""
    purge = _snap("purge-me", days_ago=5, status="PENDING_DELETION",
                  item_count=0, bytes_total=0)
    n, _ = _restate([purge], {"purge-me": (500, 5_000)})
    assert n == 1 and purge.item_count == 500
    assert not rc._holds_recoverable_data(purge), (
        "restating the counters made an explicitly purged row look like a "
        "restore point again"
    )


# ── delete-time backstop ────────────────────────────────────────────────

class _RecordingDeleteSession:
    def __init__(self, deletable_ids: List[Any]):
        self._deletable = list(deletable_ids)
        self.guard_sql = ""
        self.deleted_snaps: List[Any] = []

    async def execute(self, statement, params=None, *a, **kw):
        sql = str(statement)
        if sql.strip().upper().startswith("SELECT"):
            self.guard_sql = sql
            return _FakeResult([(i,) for i in self._deletable])
        if sql.startswith("DELETE FROM snapshots"):
            self.deleted_snaps = list(
                statement.compile().params.get("id_1") or []
            )
            return SimpleNamespace(rowcount=len(self.deleted_snaps))
        return SimpleNamespace(rowcount=0)


def test_delete_time_guard_second_guesses_an_empty_looking_row(monkeypatch):
    """The keep set is not the last word. Whatever decided this batch, the
    delete transaction re-checks the one claim that can destroy data — "no
    counter evidence of content" — against the actual snapshot_items rows,
    so a lying counter can no longer reach a DELETE."""
    doomed = uuid.uuid4()
    session = _RecordingDeleteSession([doomed])

    async def _fake_rehydrate(sess, sid, *, doomed_ids):
        return None

    monkeypatch.setattr(rc, "_rehydrate_reuse_heir", _fake_rehydrate)
    asyncio.run(rc._delete_snapshots(session, {doomed}))

    guard = " ".join(session.guard_sql.split())
    assert "COALESCE(snapshots.item_count, 0) = 0" in guard, (
        "delete-time guard no longer looks at the counter that lies"
    )
    assert "COALESCE(snapshots.bytes_total, 0) = 0" in guard
    assert "FROM snapshot_items si" in guard and "NOT (" in guard, (
        "delete-time guard does not verify the claim against snapshot_items "
        "— retention can again delete real items on a counter's word"
    )
