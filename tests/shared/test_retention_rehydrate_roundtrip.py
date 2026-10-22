"""Round-trip test: rehydrate a reuse heir, then re-run the keep-set decision.

The rehydration sequence (`_rehydrate_reuse_heir`) and the keep-set
classifier (`_holds_recoverable_data`) have to agree about one row: the
heir. Rehydration hands the heir the chain's ENTIRE inventory
(`snapshot_items` copied from the doomed chain root) and then NULLs its
reuse pointers. The classifier calls a COMPLETED row with zero
`item_count`/`bytes_total` and NULL `reuse_chain_root_id` worthless —
it can win no GFS bucket (`_bucket_candidates`) and is invisible to the
anchor rule (`_baseline_keep_ids`).

So the pass right after a rehydration selects for hard deletion the one
row that owns the chain's only copy of the data. What the DB then does
is bad in both directions: either the next pass rehydrates AGAIN into a
descendant (re-copying every `snapshot_items` row, nightly, forever), or
— when no surviving descendant is COMPLETED, which the honesty gates now
make routine by settling degraded runs PARTIAL — the heir has no
eligible heir of its own and the `ON DELETE RESTRICT` self-FK aborts the
whole tenant's retention pass with a ForeignKeyViolation. Neither is a
keep-more outcome, and it disproves the campaign's "strictly keep-more
for anything holding data" property.

Two halves have to hold, and both are tested here:
  1. rehydration must record what the heir now holds, so the classifier
     can see it (forward-looking);
  2. the classifier must recognise an ALREADY-rehydrated heir whose
     counters were never written — the rehydrate has been live since
     before this campaign (base 118f25b), so such rows can already exist
     in production and the first pass of the new selector would pick
     them for deletion (backward-looking).

No DB fixture: `_ChainDB` interprets the four statements
`_rehydrate_reuse_heir` issues against in-memory tables, so the real
promotion runs and its result is fed straight into the real
`enforce_retention_for_tenant`.
"""
from __future__ import annotations

import asyncio
import re
import uuid
from datetime import datetime, timedelta
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Set

import shared.retention_cleanup as rc


# ── in-memory stand-ins ─────────────────────────────────────────────────

def _snap_row(
    id_,
    *,
    days_ago: float,
    status: str = "COMPLETED",
    item_count: int = 0,
    bytes_total: int = 0,
    reuse_of=None,
    chain_root=None,
    extra: Optional[Dict[str, Any]] = None,
):
    """Stand-in `snapshots` row — the columns both the rehydrate SQL and
    the keep-set selectors read."""
    ts = datetime.utcnow() - timedelta(days=days_ago)
    return SimpleNamespace(
        id=id_,
        started_at=ts,
        created_at=ts,
        status=status,
        type="INCREMENTAL",
        item_count=item_count,
        bytes_total=bytes_total,
        extra_data=dict(extra or {}),
        reuse_of_snapshot_id=reuse_of,
        reuse_chain_root_id=chain_root,
        lease_expires_at=None,
        hc_drain_status="NOT_APPLICABLE",
    )


def _inventory(n: int, size: int = 15_000_000) -> List[Dict[str, Any]]:
    """`snapshot_items` rows for a chain root. Shaped like the jrue
    OneDrive baseline from the 2026-07-30 incident (8,239 items / 124 GB)."""
    return [{"external_id": f"item-{i}", "item_type": "FILE", "content_size": size}
            for i in range(n)]


class _Result:
    def __init__(self, rows: List[Any]):
        self._rows = list(rows)
        self.rowcount = len(self._rows)

    def first(self):
        return self._rows[0] if self._rows else None

    def all(self):
        return list(self._rows)

    def fetchall(self):
        return list(self._rows)

    def scalars(self):
        return self


class _ChainDB:
    """Interprets the four statements `_rehydrate_reuse_heir` issues.

    Only those four are understood; anything else raises, so a rehydrate
    that silently stops doing part of its job cannot pass as green.
    """

    def __init__(self, snaps: List[Any], items: Dict[Any, List[Dict[str, Any]]]):
        self.snaps = {s.id: s for s in snaps}
        self.items = {sid: [dict(r) for r in rows] for sid, rows in items.items()}
        self.statements: List[str] = []

    async def execute(self, statement, params=None, *a, **kw):
        sql = " ".join(str(statement).split())
        self.statements.append(sql)
        p = dict(params or {})
        if sql.startswith("SELECT id FROM snapshots"):
            return self._pick_heir(p)
        if sql.startswith("INSERT INTO snapshot_items"):
            return self._copy_items(p)
        if "reuse_rehydrated_from" in sql:            # step 4 — promote
            return self._promote(sql, p)
        if sql.startswith("UPDATE snapshots"):        # step 3 — repoint
            return self._repoint(p)
        raise AssertionError(f"_ChainDB got an unmodelled statement: {sql[:160]}")

    # step 1
    def _pick_heir(self, p):
        did, doomed = p["did"], set(p["doomed"])
        cands = [
            s for s in self.snaps.values()
            if str(s.reuse_chain_root_id) == did
            and str(s.id) != did
            and str(s.id) not in doomed
            and s.status == "COMPLETED"
        ]
        cands.sort(key=lambda s: s.started_at or s.created_at)
        return _Result([SimpleNamespace(id=cands[0].id)] if cands else [])

    # step 2 — ON CONFLICT DO NOTHING on uq_snapshot_items_snap_ext_type
    def _copy_items(self, p):
        heir, did = uuid.UUID(p["heir"]), uuid.UUID(p["did"])
        dst = self.items.setdefault(heir, [])
        have = {(r["external_id"], r["item_type"]) for r in dst}
        for row in self.items.get(did, []):
            key = (row["external_id"], row["item_type"])
            if key in have:
                continue
            dst.append(dict(row))
            have.add(key)
        return _Result([])

    # step 3
    def _repoint(self, p):
        did, heir = p["did"], uuid.UUID(p["heir"])
        for s in self.snaps.values():
            if str(s.reuse_chain_root_id) == did and s.id != heir:
                if str(s.reuse_of_snapshot_id) == did:
                    s.reuse_of_snapshot_id = heir
                s.reuse_chain_root_id = heir
        return _Result([])

    # step 4
    def _promote(self, sql: str, p):
        heir = uuid.UUID(p["heir"])
        row = self.snaps[heir]
        row.reuse_of_snapshot_id = None
        row.reuse_chain_root_id = None
        row.extra_data = {
            **(row.extra_data or {}),
            "reuse_rehydrated_from": p["did"],
            "reuse_rehydrated_at": datetime.utcnow().isoformat() + "Z",
        }
        # Counters are credited only when the UPDATE derives them from
        # snapshot_items — that is what makes them true of the heir's own
        # inventory (and self-correcting on an idempotent re-run). A
        # literal, or a copy of the doomed row's possibly-stale counter,
        # is not the same claim; if the UPDATE grows some other form this
        # fake says so out loud rather than passing it silently.
        owned = self.items.get(heir, [])
        counted = re.search(r"item_count\s*=\s*\(\s*SELECT\s+COUNT\(\s*\*\s*\)", sql, re.I)
        summed = re.search(
            r"bytes_total\s*=\s*\(\s*SELECT\s+COALESCE\(\s*SUM\(\s*content_size\s*\)", sql, re.I,
        )
        if counted:
            row.item_count = len(owned)
        if summed:
            row.bytes_total = sum(int(r["content_size"] or 0) for r in owned)
        if ("item_count" in sql or "bytes_total" in sql) and not (counted and summed):
            raise AssertionError(
                "the promote UPDATE sets the heir's counters in a form _ChainDB "
                "does not model — extend _promote to match the new SQL"
            )
        return _Result([])


# ── enforce_retention_for_tenant stubs (same shape as ────────────────────
#    tests/shared/test_retention_keepset_safety.py)

class _FakeSession:
    def __init__(self, policies, resources, snaps_by_resource):
        self._policies, self._resources = policies, resources
        self._snaps = snaps_by_resource
        self.commits = 0

    async def execute(self, statement, *a, **kw):
        sql = str(statement)
        if "sla_policies" in sql:
            return _Result(self._policies)
        if "FROM resources" in sql:
            # Keyset paging (no stream() — see
            # tests/shared/test_retention_keyset_pagination.py): the id
            # page, then that page's rows.
            if "LIMIT" in sql:
                return _Result([r.id for r in self._resources])
            return _Result(list(self._resources))
        if "snapshots" in sql:
            rid = statement.compile().params.get("resource_id_1")
            return _Result(self._snaps.get(rid, []))
        return _Result([])

    async def commit(self):
        self.commits += 1


def _gold_policy():
    """Gold, as configured in production: GFS daily 14 / weekly 8 /
    monthly 12 / yearly 7."""
    return SimpleNamespace(
        id=uuid.uuid4(), tenant_id=None, is_default=True,
        retention_mode="GFS", retention_days=None,
        retention_hot_days=0, retention_cool_days=0, retention_archive_days=0,
        gfs_daily_count=14, gfs_weekly_count=8,
        gfs_monthly_count=12, gfs_yearly_count=7,
        archived_retention_mode="SAME", archived_retention_days=None,
        legal_hold_enabled=False, legal_hold_until=None, immutability_mode="None",
    )


def _resource(policy_id, status: str = "ACTIVE"):
    return SimpleNamespace(id=uuid.uuid4(), status=status, sla_policy_id=policy_id)


def _retention_decision(monkeypatch, snaps, policy=None, resource_status="ACTIVE") -> Set[Any]:
    """Run the real enforce_retention_for_tenant over `snaps` and return the
    ids it decided to hard-delete."""
    pol = policy or _gold_policy()
    res = _resource(pol.id, status=resource_status)
    deleted: Set[Any] = set()

    async def _fake_delete(session, snap_ids, *, audit_events=None):
        deleted.update(snap_ids)
        return len(snap_ids)

    monkeypatch.setattr(rc, "_delete_snapshots", _fake_delete)
    monkeypatch.setattr(rc.settings, "RETENTION_DELETE_ENABLED", True)
    session = _FakeSession([pol], [res], {res.id: list(snaps)})
    asyncio.run(rc.enforce_retention_for_tenant(session, uuid.uuid4()))
    return deleted


# ── the chain fixtures ──────────────────────────────────────────────────

def _chain(*, heir_days: float, n_descendants: int, n_items: int = 8_239):
    """A doomed chain root that owns the inventory + `n_descendants + 1`
    reuse rows. The oldest reuse row (at `heir_days`) is the heir the
    rehydrate will pick; the rest are one per day, newest first."""
    root = uuid.uuid4()
    heir = uuid.uuid4()
    descendants = [uuid.uuid4() for _ in range(n_descendants)]
    items = _inventory(n_items)

    snaps = [_snap_row(root, days_ago=heir_days + 30,
                       item_count=len(items),
                       bytes_total=sum(r["content_size"] for r in items))]
    snaps.append(_snap_row(heir, days_ago=heir_days, reuse_of=root, chain_root=root))
    for i, d in enumerate(descendants):
        snaps.append(_snap_row(d, days_ago=n_descendants - 1 - i,
                               reuse_of=root, chain_root=root))
    return root, heir, descendants, snaps, {root: items}


def _rehydrate(root, snaps, items) -> tuple[_ChainDB, Any]:
    db = _ChainDB(snaps, items)
    heir_id = asyncio.run(rc._rehydrate_reuse_heir(db, root, doomed_ids={root}))
    return db, heir_id


def _survivors(db: _ChainDB, root) -> List[Any]:
    """The rows retention sees on the NEXT pass: the doomed chain root is
    gone, everything the rehydrate touched remains."""
    return [s for s in db.snaps.values() if s.id != root]


# ── 1. the round trip ───────────────────────────────────────────────────

def test_rehydrated_heir_and_its_descendants_survive_the_next_pass(monkeypatch):
    """Run the real rehydrate, then the real keep-set/delete decision over
    what it produced. The heir holds the chain's only copy of 8,239 items;
    it must not be selected for deletion, and neither must the descendants
    that now resolve their inventory through it."""
    root, heir, descendants, snaps, items = _chain(heir_days=13, n_descendants=13)
    db, heir_id = _rehydrate(root, snaps, items)

    assert heir_id == heir, "rehydrate picked the wrong heir"
    assert len(db.items[heir]) == 8_239, "heir did not receive the chain's inventory"

    deleted = _retention_decision(monkeypatch, _survivors(db, root))

    assert heir not in deleted, (
        "retention selected the freshly-rehydrated heir for hard deletion — "
        "it owns the chain's ONLY copy of 8,239 items / 124 GB, and every "
        "surviving descendant resolves its inventory through it"
    )
    assert not (set(descendants) & deleted), (
        f"deleted descendants of the rehydrated chain: "
        f"{sorted(str(d) for d in set(descendants) & deleted)}"
    )


def test_rehydrate_records_what_the_heir_now_holds():
    """The promotion must leave the heir's counters describing its own
    inventory — that is the only evidence the keep-set classifier has."""
    root, heir, _desc, snaps, items = _chain(heir_days=13, n_descendants=13)
    db, _ = _rehydrate(root, snaps, items)

    promoted = db.snaps[heir]
    assert promoted.reuse_chain_root_id is None  # promoted to a full row
    assert promoted.item_count == len(db.items[heir]) == 8_239
    assert promoted.bytes_total == sum(r["content_size"] for r in db.items[heir])
    assert rc._holds_recoverable_data(promoted), (
        "the keep-set classifier still calls the rehydrated heir worthless"
    )


def test_rehydrate_is_idempotent_on_the_heir_counters():
    """Safe under retry: the item copy is ON CONFLICT DO NOTHING, so a
    second run must not double the heir's counters."""
    root, heir, _desc, snaps, items = _chain(heir_days=13, n_descendants=13)
    db, _ = _rehydrate(root, snaps, items)
    first = (db.snaps[heir].item_count, db.snaps[heir].bytes_total)

    # Re-run against the SAME state, as a retry after a rolled-back partial
    # failure would: the heir is still pointing at the chain root.
    db.snaps[heir].reuse_of_snapshot_id = root
    db.snaps[heir].reuse_chain_root_id = root
    asyncio.run(rc._rehydrate_reuse_heir(db, root, doomed_ids={root}))

    assert (db.snaps[heir].item_count, db.snaps[heir].bytes_total) == first
    assert len(db.items[heir]) == 8_239


# ── 2. the anchor: age-immunity, not just "it fit in the window" ─────────

def test_rehydrated_heir_is_protected_as_the_chain_anchor(monkeypatch):
    """Same round trip, but the heir sits far outside the 14-day daily
    window with 20 newer reuse rows ahead of it, so it can win no daily
    bucket. Weekly/monthly/yearly are zeroed here on purpose so nothing
    else can rescue it: it survives only if the anchor rule
    (_baseline_keep_ids) sees it as the largest/oldest data-bearing row —
    which is what makes the rehydrated inventory age-immune, exactly like
    any other baseline."""
    root, heir, descendants, snaps, items = _chain(heir_days=40, n_descendants=20)
    db, _ = _rehydrate(root, snaps, items)
    survivors = _survivors(db, root)

    daily_only = _gold_policy()
    daily_only.gfs_weekly_count = 0
    daily_only.gfs_monthly_count = 0
    daily_only.gfs_yearly_count = 0
    deleted = _retention_decision(monkeypatch, survivors, policy=daily_only)

    assert heir not in deleted, (
        "the rehydrated baseline aged out of the daily window and was "
        "selected for hard deletion — the 2026-07-30 incident, one "
        "rehydration later"
    )
    # Not a never-delete no-op: pointer rows that hold nothing of their own
    # and won no bucket are still pruned.
    assert deleted, "retention deleted nothing at all — the test proves nothing"
    assert deleted <= set(descendants), (
        f"deleted something that is not a spent reuse pointer: {sorted(map(str, deleted))}"
    )


# ── 3. heirs the ALREADY-DEPLOYED rehydrate left behind ─────────────────

def test_heir_rehydrated_before_this_deploy_is_still_data_bearing(monkeypatch):
    """`_rehydrate_reuse_heir` has been live since before this campaign
    (base 118f25b), and it never wrote the counters. Production can
    therefore already hold promoted heirs with item_count=0 and NULL reuse
    pointers, whose `snapshot_items` are the chain's only inventory. The
    new classifier must not pick those for deletion on its first pass —
    the marker the promotion writes is the evidence."""
    heir = uuid.uuid4()
    legacy = _snap_row(
        heir, days_ago=40, item_count=0, bytes_total=0,
        extra={"reuse_rehydrated_from": str(uuid.uuid4()),
               "reuse_rehydrated_at": "2026-07-15T02:11:04.512Z"},
    )
    pointers = [
        _snap_row(uuid.uuid4(), days_ago=d, reuse_of=heir, chain_root=heir)
        for d in range(20)
    ]

    assert rc._holds_recoverable_data(legacy), (
        "a heir promoted by the previously-deployed rehydrate is classified "
        "worthless, so it can win no bucket and is invisible to the anchor rule"
    )
    deleted = _retention_decision(monkeypatch, [legacy] + pointers)
    assert heir not in deleted, (
        "hard-deleted a pre-existing rehydrated heir — this is the chain's "
        "whole inventory and every pointer row above resolves through it"
    )


# ── 4. the placeholder property must not regress ────────────────────────

def test_zero_item_placeholders_are_still_worthless():
    """The marker is not a blanket amnesty: a SKIP_CONCURRENT placeholder
    and an empty FAILED row must still lose every bucket (that is the bug
    the classifier exists for)."""
    skip = _snap_row(uuid.uuid4(), days_ago=1, item_count=0,
                     extra={"skipped_concurrent": True})
    failed_empty = _snap_row(uuid.uuid4(), days_ago=1, status="FAILED",
                             item_count=0, bytes_total=0)
    assert not rc._holds_recoverable_data(skip)
    assert not rc._holds_recoverable_data(failed_empty)
