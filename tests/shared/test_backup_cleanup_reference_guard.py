"""The reference-safety guard must be reusable, complete, and affordable.

Companion to ``test_backup_cleanup_dedup_safety.py`` (which covers the
dedup/content-addressed data-loss path through the whole
``cleanup_cancelled_snapshots`` function). This file targets the
extracted helper the backup-scheduler sweep adopts —
``select_deletable_blob_paths`` — plus the two holes the first pass
missed:

  * a RETAINED reuse-chain descendant whose ``reuse_chain_root_id``
    points INTO the teardown set owns zero ``snapshot_items`` rows, so
    neither the owner gate nor any snapshot_items probe can see it. Its
    root's blobs must survive.
  * ``chat_url_cache`` is a tenant-scoped, cross-snapshot
    (url_sha256 → blob_path) map with no existence probe on the bulk
    hit path. Deleting a blob without clearing the cache row points a
    FUTURE backup at a dead path.

...and the shape the guard must query in, because a
``blob_path IN (...) AND snapshot_id NOT IN (...)`` scan of
``snapshot_items`` (16 hash partitions, no blob_path index, sized for
hundreds of millions of rows) is not affordable on a 200k-item
cancelled snapshot.
"""
from __future__ import annotations

import asyncio
import uuid
from typing import Any, Dict, List, Optional, Set

import pytest

from shared.models import SnapshotItem, SnapshotStatus


def _run(coro):
    return asyncio.run(coro)


def _path(tenant, resource, snapshot, item=None):
    """``build_blob_path`` shape: tenant/resource/snapshot/ts/item."""
    return (
        f"{tenant}/{resource}/{snapshot}/20260731T101500/"
        f"{item or uuid.uuid4()}"
    )


# ─── Statement-recording fake session ────────────────────────────────


class _Rows:
    def __init__(self, rows: List[Any]):
        self._rows = list(rows)
        self.rowcount = len(rows)

    def scalars(self):
        return self

    def all(self):
        return list(self._rows)

    def first(self):
        return self._rows[0] if self._rows else None

    def scalar_one_or_none(self):
        return self._rows[0] if self._rows else None


class _Row:
    def __init__(self, **kw):
        self.__dict__.update(kw)

    def __iter__(self):
        return iter(self.__dict__.values())


class _GuardDB:
    """Row store for the guard's three read surfaces."""

    def __init__(self):
        # snapshot_items rows: dicts with snapshot_id/tenant_id/
        # content_checksum/blob_path
        self.items: List[Dict[str, Any]] = []
        # snapshots rows: dicts with id/reuse_chain_root_id/
        # reuse_of_snapshot_id
        self.snapshots: List[Dict[str, Any]] = []
        self.chat_cache: List[Dict[str, Any]] = []
        self.statements: List[str] = []
        self.cache_update_raises = False
        self.reuse_probe_raises = False
        self.checksum_probes = 0
        self.path_probes = 0

    def add_item(self, snapshot_id, blob_path, *, tenant_id=None,
                 content_checksum=None):
        self.items.append({
            "snapshot_id": snapshot_id, "blob_path": blob_path,
            "tenant_id": tenant_id, "content_checksum": content_checksum,
        })


class _GuardSession:
    def __init__(self, db: _GuardDB):
        self.db = db

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def expunge(self, obj):
        return None

    async def commit(self):
        return None

    async def execute(self, stmt, params=None):
        sql = str(stmt)
        self.db.statements.append(sql)
        descs = getattr(stmt, "column_descriptions", None)

        # --- ORM update on chat_url_cache ----------------------------
        if "chat_url_cache" in sql:
            if self.db.cache_update_raises:
                raise RuntimeError("simulated PG failure clearing chat_url_cache")
            binds = dict(stmt.compile().params)
            paths = {
                x for v in binds.values()
                for x in (v if isinstance(v, (list, tuple, set)) else [v])
                if isinstance(x, str)
            }
            nulls_size = "content_size" in sql
            n = 0
            for row in self.db.chat_cache:
                if row.get("blob_path") in paths:
                    row["blob_path"] = None
                    if nulls_size:
                        row["content_size"] = None
                    n += 1
            return _Rows([None] * n)

        # --- raw text() surfaces -------------------------------------
        if not descs:
            if "UPDATE snapshots" in sql and "reuse_protected" in sql:
                # Interpret the protective hold the way Postgres would:
                # IN_PROGRESS -> FAILED, extra_data minus 'cancelled_at'
                # plus the reuse_protected marker, counters untouched.
                ids = {
                    uuid.UUID(str(x)) for x in ((params or {}).get("ids") or [])
                }
                n = 0
                for s in self.db.snapshots:
                    if s["id"] not in ids:
                        continue
                    extra = dict(s.get("extra_data") or {})
                    was = extra.pop("cancelled_at", None)
                    extra["reuse_protected"] = True
                    extra["reuse_protected_cancelled_at"] = was
                    extra["cancel_phase"] = "reuse_protected_hold"
                    s["extra_data"] = extra
                    if s.get("status") == SnapshotStatus.IN_PROGRESS:
                        s["status"] = SnapshotStatus.FAILED
                    n += 1
                return _Rows([None] * n)
            if "FROM snapshot_items" in sql and "blob_path IS NOT NULL" in sql:
                sid = uuid.UUID(str((params or {}).get("sid")))
                return _Rows([
                    _Row(blob_path=i["blob_path"], snapshot_id=i["snapshot_id"],
                         tenant_id=i["tenant_id"],
                         content_checksum=i["content_checksum"],
                         backend_id=i.get("backend_id"))
                    for i in self.db.items
                    if i["snapshot_id"] == sid and i["blob_path"]
                ])
            if "reuse_chain_root_id" in sql and "FROM snapshots" in sql:
                if self.db.reuse_probe_raises:
                    raise RuntimeError(
                        "simulated PG failure on the reuse-chain probe"
                    )
                doomed = {
                    uuid.UUID(str(x))
                    for x in ((params or {}).get("doomed") or [])
                }
                hits = set()
                for s in self.db.snapshots:
                    if s["id"] in doomed:
                        continue
                    for col in ("reuse_chain_root_id", "reuse_of_snapshot_id"):
                        anc = s.get(col)
                        if anc is not None and anc in doomed:
                            hits.add(anc)
                return _Rows([_Row(ancestor_id=a) for a in sorted(hits, key=str)])
            return _Rows([])

        binds = dict(stmt.compile().params)
        names = [d["name"] for d in descs]

        # --- indexed (tenant_id, content_checksum) dedup-sharer probe -
        if names[:1] == ["snapshot_id"] and "content_checksum" in names:
            self.db.checksum_probes += 1
            tenants = {
                x for v in binds.values()
                for x in (v if isinstance(v, (list, tuple, set)) else [v])
                if isinstance(x, uuid.UUID)
            }
            sums = {
                x for v in binds.values()
                for x in (v if isinstance(v, (list, tuple, set)) else [v])
                if isinstance(x, str)
            }
            return _Rows([
                _Row(snapshot_id=i["snapshot_id"], blob_path=i["blob_path"],
                     content_checksum=i["content_checksum"])
                for i in self.db.items
                if i["content_checksum"] in sums and i["tenant_id"] in tenants
            ])

        # --- exact blob_path probe (unkeyed residue only) -------------
        if names[:1] == ["blob_path"]:
            self.db.path_probes += 1
            paths = {
                x for v in binds.values()
                for x in (v if isinstance(v, (list, tuple, set)) else [v])
                if isinstance(x, str)
            }
            return _Rows([
                _Row(blob_path=i["blob_path"], snapshot_id=i["snapshot_id"])
                for i in self.db.items if i["blob_path"] in paths
            ])

        return _Rows([])


def _plan(db, *, doomed, candidates, **kw):
    from shared.backup_cleanup import select_deletable_blob_paths
    return _run(select_deletable_blob_paths(
        _GuardSession(db), doomed_snapshot_ids=doomed,
        candidates=candidates, **kw,
    ))


def _cand(path, **kw):
    from shared.backup_cleanup import BlobCandidate
    return BlobCandidate(blob_path=path, **kw)


# ─── Hole 1: reuse-chain descendants ─────────────────────────────────


def test_retained_reuse_descendant_protects_its_chain_roots_blobs():
    """A reuse snapshot owns ZERO snapshot_items rows: it borrows its
    root's. If the root is torn down and the descendant is retained,
    deleting the root's blobs silently empties the descendant."""
    from shared.backup_cleanup import RETAIN_REUSE_DESCENDANT
    db = _GuardDB()
    tenant, resource = uuid.uuid4(), uuid.uuid4()
    root = uuid.uuid4()          # in the teardown set
    descendant = uuid.uuid4()    # RETAINED, points at root
    db.snapshots = [
        {"id": root, "reuse_chain_root_id": None,
         "reuse_of_snapshot_id": None},
        {"id": descendant, "reuse_chain_root_id": root,
         "reuse_of_snapshot_id": root},
    ]
    p = _path(tenant, resource, root)
    db.add_item(root, p, tenant_id=tenant, content_checksum="c1")

    plan = _plan(db, doomed=[root], candidates=[
        _cand(p, snapshot_id=root, tenant_id=tenant, content_checksum="c1"),
    ])

    assert p not in plan.deletable, (
        f"deleted the chain root's blob while reuse descendant "
        f"{descendant} still resolves to its items rows"
    )
    assert plan.retained.get(p) == RETAIN_REUSE_DESCENDANT
    assert root in plan.protected_roots


def test_reuse_descendant_inside_teardown_set_does_not_protect():
    """Negative: if the descendant is being torn down too, nothing
    outside the set depends on the root — reclaim the bytes."""
    db = _GuardDB()
    tenant, resource = uuid.uuid4(), uuid.uuid4()
    root, descendant = uuid.uuid4(), uuid.uuid4()
    db.snapshots = [
        {"id": root, "reuse_chain_root_id": None, "reuse_of_snapshot_id": None},
        {"id": descendant, "reuse_chain_root_id": root,
         "reuse_of_snapshot_id": root},
    ]
    p = _path(tenant, resource, root)
    db.add_item(root, p, tenant_id=tenant, content_checksum="c1")

    plan = _plan(db, doomed=[root, descendant], candidates=[
        _cand(p, snapshot_id=root, tenant_id=tenant, content_checksum="c1"),
    ])

    assert p in plan.deletable
    assert plan.protected_roots == frozenset()


# ─── Hole 2: query shape / affordability ─────────────────────────────


def test_dedup_sharer_is_found_through_the_tenant_checksum_index():
    """Same protection as before, but resolved on
    ``idx_snapshot_items_tenant_checksum`` instead of an unindexed
    blob_path scan. Every mechanism that hands one snapshot another
    snapshot's blob_path (blob_dedup, the Teams per-page bulk dedup,
    chat_url_cache, _rehydrate_reuse_heir) copies content_checksum
    along with it, so the checksum is a sound join key."""
    from shared.backup_cleanup import RETAIN_SHARED_REFERENCE
    db = _GuardDB()
    tenant, resource = uuid.uuid4(), uuid.uuid4()
    doomed_id, retained_id = uuid.uuid4(), uuid.uuid4()
    p = _path(tenant, resource, doomed_id)
    db.add_item(doomed_id, p, tenant_id=tenant, content_checksum="deadbeef")
    # A RETAINED snapshot dedup-inherited the same bytes.
    db.add_item(retained_id, p, tenant_id=tenant, content_checksum="deadbeef")

    plan = _plan(db, doomed=[doomed_id], candidates=[
        _cand(p, snapshot_id=doomed_id, tenant_id=tenant,
              content_checksum="deadbeef"),
    ])

    assert p not in plan.deletable
    assert plan.retained.get(p) == RETAIN_SHARED_REFERENCE
    assert db.checksum_probes >= 1


def test_guard_never_scans_snapshot_items_by_blob_path_when_keys_exist():
    """Performance contract. snapshot_items is PARTITION BY HASH
    (snapshot_id) with no index on blob_path; ``blob_path IN (...) AND
    snapshot_id NOT IN (...)`` prunes no partition and uses no index,
    so a 200k-item cancelled snapshot turns into ~400 full scans of
    the largest table in the product."""
    db = _GuardDB()
    tenant, resource, doomed_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    cands = []
    for n in range(3):
        p = _path(tenant, resource, doomed_id)
        db.add_item(doomed_id, p, tenant_id=tenant, content_checksum=f"c{n}")
        cands.append(_cand(p, snapshot_id=doomed_id, tenant_id=tenant,
                           content_checksum=f"c{n}"))

    plan = _plan(db, doomed=[doomed_id], candidates=cands)

    assert plan.deletable == frozenset(c.blob_path for c in cands)
    assert db.path_probes == 0, (
        "guard fell back to an unindexed blob_path scan of snapshot_items "
        "even though every candidate carried a (tenant_id, content_checksum) "
        "key"
    )
    items_scans = [
        s for s in db.statements
        if "snapshot_items" in s and "blob_path IN" in s
    ]
    assert not items_scans, f"unindexed blob_path scan issued: {items_scans}"


def test_candidate_without_a_dedup_key_still_gets_an_exact_path_check():
    """No content_checksum means the indexed probe can't see sharers,
    so fall back to the exact (unindexed) path check rather than
    assuming the blob is unshared."""
    from shared.backup_cleanup import RETAIN_SHARED_REFERENCE
    db = _GuardDB()
    tenant, resource = uuid.uuid4(), uuid.uuid4()
    doomed_id, retained_id = uuid.uuid4(), uuid.uuid4()
    p = _path(tenant, resource, doomed_id)
    db.add_item(doomed_id, p, tenant_id=tenant, content_checksum=None)
    db.add_item(retained_id, p, tenant_id=tenant, content_checksum=None)

    plan = _plan(db, doomed=[doomed_id], candidates=[
        _cand(p, snapshot_id=doomed_id, tenant_id=tenant),
    ])

    assert p not in plan.deletable
    assert plan.retained.get(p) == RETAIN_SHARED_REFERENCE
    assert db.path_probes == 1


def test_unkeyed_residue_past_the_probe_budget_is_retained_not_scanned():
    """The unindexed fallback is bounded: past the budget we keep the
    bytes instead of launching a scan storm. Retaining leaks storage;
    scanning at 200k paths takes the cluster down."""
    from shared.backup_cleanup import RETAIN_UNVERIFIABLE
    db = _GuardDB()
    tenant, resource, doomed_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    paths = [_path(tenant, resource, doomed_id) for _ in range(3)]
    for p in paths:
        db.add_item(doomed_id, p, tenant_id=tenant, content_checksum=None)

    plan = _plan(
        db, doomed=[doomed_id],
        candidates=[_cand(p, snapshot_id=doomed_id, tenant_id=tenant)
                    for p in paths],
        unkeyed_probe_budget=1,
    )

    assert len(plan.deletable) == 1
    over_budget = [p for p in paths if p not in plan.deletable]
    assert len(over_budget) == 2
    assert all(plan.retained.get(p) == RETAIN_UNVERIFIABLE
               for p in over_budget)
    assert db.path_probes == 1


def test_unkeyed_path_probes_are_bounded_per_run_not_per_batch():
    """``unkeyed_probe_budget`` bounds paths per CALL, and cleanup calls
    the guard once per batch — with batch_size == the budget, the cap can
    never trip, so a teardown of N unkeyed items issues N/500 unindexed
    scans of all 16 snapshot_items partitions with no ceiling. Those
    candidates are real and routine: every azure-workload-worker row
    (VM config/disks/volumes/NICs/public IPs, Azure DB config/schema/
    table artifacts) sets content_checksum=None. The residue must be
    bounded per RUN — and bounded by RETAINING, never by skipping the
    reference check."""
    from shared.backup_cleanup import RETAIN_UNVERIFIABLE
    w = _CacheWorld()
    paths = [w.path() for _ in range(5)]
    for p in paths:
        w.add(p, checksum=None)

    stats = w.run(batch_size=1, unkeyed_probe_budget=2)

    assert w.db.path_probes == 2, (
        f"{w.db.path_probes} unindexed blob_path scans for a 5-path run "
        "with a budget of 2 — the budget resets every batch"
    )
    # The 2 probed paths are provably unreferenced, so they go; the rest
    # are kept, not deleted on an unverified guess.
    assert len(w.shard.deleted) == 2
    assert stats["retained_reasons"][RETAIN_UNVERIFIABLE] == 3
    assert set(w.shard.deleted) <= set(paths)


def test_fetch_blob_candidates_carries_the_keys_the_guard_needs():
    """The sweep's adoption surface: one snapshot_id-equality read (hash
    partition pruned) that returns the dedup keys, so the caller never
    has to hand-roll the SELECT and lose content_checksum."""
    from shared.backup_cleanup import fetch_blob_candidates
    db = _GuardDB()
    tenant, resource, sid = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    other = uuid.uuid4()
    p = _path(tenant, resource, sid)
    db.add_item(sid, p, tenant_id=tenant, content_checksum="c1")
    db.add_item(other, _path(tenant, resource, other),
                tenant_id=tenant, content_checksum="c2")

    got = _run(fetch_blob_candidates(_GuardSession(db), snapshot_id=sid))

    assert [c.blob_path for c in got] == [p]
    assert got[0].tenant_id == tenant
    assert got[0].content_checksum == "c1", (
        "candidate lost its dedup key — the guard would fall back to an "
        "unindexed blob_path scan"
    )
    assert got[0].snapshot_id == sid


def test_foreign_owned_path_is_retained_without_any_query():
    """Owner gate short-circuits: a path whose embedded snapshot id is
    outside the teardown set never needs a probe."""
    from shared.backup_cleanup import RETAIN_FOREIGN_OWNER
    db = _GuardDB()
    tenant, resource = uuid.uuid4(), uuid.uuid4()
    doomed_id, foreign = uuid.uuid4(), uuid.uuid4()
    p = _path(tenant, resource, foreign)

    plan = _plan(db, doomed=[doomed_id], candidates=[
        _cand(p, snapshot_id=doomed_id, tenant_id=tenant,
              content_checksum="c1"),
    ])

    assert plan.deletable == frozenset()
    assert plan.retained.get(p) == RETAIN_FOREIGN_OWNER
    assert db.checksum_probes == 0
    assert db.path_probes == 0


# ─── Hole 3: chat_url_cache ──────────────────────────────────────────


class _CacheWorld:
    """Behavioral harness: real ``cleanup_cancelled_snapshots`` over a
    mini DB that also carries chat_url_cache rows."""

    def __init__(self):
        self.tenant = uuid.uuid4()
        self.resource = uuid.uuid4()
        self.job_id = uuid.uuid4()
        self.snapshot_id = uuid.uuid4()
        self.db = _GuardDB()
        self.db.snapshots = [
            # Shaped like a real post-cancel row: job-service flipped it
            # and stamped extra_data.cancelled_at (services/job-service/
            # main.py ~1192-1215), counters intact.
            {"id": self.snapshot_id, "job_id": self.job_id,
             "status": SnapshotStatus.IN_PROGRESS,
             "item_count": 1, "bytes_total": 4096,
             "extra_data": {"cancelled_at": "2026-07-31T10:15:00Z",
                            "cancel_phase": "flip_pending_sweep"},
             "reuse_chain_root_id": None, "reuse_of_snapshot_id": None},
        ]
        self.orm_items: List[SnapshotItem] = []
        self.events: List[str] = []

    def path(self, item=None):
        return _path(self.tenant, self.resource, self.snapshot_id, item)

    def add(self, blob_path, *, checksum="c1"):
        """``checksum=None`` reproduces the azure-workload-worker shape:
        blob_path set, content_checksum explicitly None (18 of the 49
        SnapshotItem constructions that set blob_path do exactly that)."""
        it = SnapshotItem(
            id=uuid.uuid4(), snapshot_id=self.snapshot_id,
            tenant_id=self.tenant, external_id=str(uuid.uuid4()),
            item_type="CHAT_ATTACHMENT", name="x",
            blob_path=blob_path, content_checksum=checksum,
        )
        self.orm_items.append(it)
        self.db.add_item(self.snapshot_id, blob_path,
                         tenant_id=self.tenant, content_checksum=checksum)
        return it

    def run(self, **kwargs):
        from shared.backup_cleanup import (
            cleanup_cancelled_snapshots, default_container_resolver,
        )
        world = self

        class _Shard:
            deleted: List[str] = []

            async def delete_blob(self, container, path):
                world.events.append(f"delete:{path}")
                self.deleted.append(path)

        class _Session(_GuardSession):
            async def execute(self, stmt, params=None):
                descs = getattr(stmt, "column_descriptions", None)
                if descs and descs[0]["name"] == "id":
                    return _Rows([world.snapshot_id])
                if descs and descs[0]["name"] == "SnapshotItem":
                    binds = dict(stmt.compile().params)
                    last = None
                    limit = None
                    for v in binds.values():
                        if isinstance(v, uuid.UUID) and v != world.snapshot_id:
                            last = v
                        elif isinstance(v, int):
                            limit = v
                    rows = sorted(world.orm_items, key=lambda i: i.id)
                    if last is not None:
                        rows = [r for r in rows if r.id > last]
                    if limit:  # honour the keyset LIMIT so batching is real
                        rows = rows[:limit]
                    return _Rows(rows)
                if "chat_url_cache" in str(stmt):
                    world.events.append("cache_invalidate")
                return await super().execute(stmt, params)

        self.shard = _Shard()
        self.shard.deleted = []
        return _run(cleanup_cancelled_snapshots(
            job_id=self.job_id,
            session_factory=lambda: _Session(self.db),
            shard=self.shard,
            container_resolver=default_container_resolver,
            **kwargs,
        ))


def _sweep_would_reap(snap_row, *, job_status="CANCELLED"):
    """Mirror of ``_sweep_cancelled_snapshots``'s candidate WHERE clause
    (services/backup-scheduler, 30s interval), which reaps every snapshot
    it selects: blobs, then the snapshot_items rows, then the snapshots
    row.

      Populations A + C: (extra_data->>'cancelled_at') IS NOT NULL
      Population B:      status = 'IN_PROGRESS' AND j.status IN
                         ('CANCELLED','FAILED')

    That sweep now runs the guard too (via
    ``teardown_cancelled_sweep_blobs``), but this module must still leave
    a protected snapshot in a state the predicate REJECTS: the sweep is
    deployed from the other integration lineage and a version-skewed or
    rolled-back scheduler pod is exactly the case the durable hold
    exists for."""
    extra = snap_row.get("extra_data") or {}
    if extra.get("cancelled_at") is not None:
        return True
    status = snap_row.get("status")
    name = getattr(status, "name", str(status))
    return name == "IN_PROGRESS" and job_status in ("CANCELLED", "FAILED")


def test_protected_reuse_root_is_put_out_of_the_unguarded_sweeps_reach():
    """Skipping the status flip is not protection. The 30s
    backup-scheduler sweep selects IN_PROGRESS snapshots of a cancelled
    job (and anything carrying extra_data.cancelled_at) and deletes
    exactly the blobs and rows this guard just refused to touch — so
    leaving the root IN_PROGRESS with its cancel marker hands it
    straight over, which is WORSE than the pre-guard behaviour. Take it
    out of the sweep's populations (terminal status, marker moved to
    reuse_protected_cancelled_at) WITHOUT deleting its rows or zeroing
    its counters, which is what the live descendant reads. (That sweep
    now consults the guard as well — see ``_sweep_would_reap`` — but it
    ships from the other integration lineage, so the durable hold is
    still what protects the root against a skewed or rolled-back
    scheduler pod.)"""
    w = _CacheWorld()
    descendant = uuid.uuid4()
    w.db.snapshots.append({
        "id": descendant, "job_id": uuid.uuid4(),
        "status": SnapshotStatus.COMPLETED,
        "reuse_chain_root_id": w.snapshot_id,
        "reuse_of_snapshot_id": w.snapshot_id,
    })
    root_row = w.db.snapshots[0]
    assert _sweep_would_reap(root_row), "precondition: the sweep wants it"
    p = w.path()
    w.add(p)

    stats = w.run()

    assert not _sweep_would_reap(root_row), (
        "protected reuse root left in a state the unguarded 30s sweep "
        f"reaps: status={root_row['status']} extra={root_row['extra_data']}"
    )
    # The whole point of holding it: the inventory the descendant reads
    # is still there.
    assert root_row["item_count"] == 1
    assert root_row["bytes_total"] == 4096
    assert root_row["extra_data"]["reuse_protected"] is True
    assert (root_row["extra_data"]["reuse_protected_cancelled_at"]
            == "2026-07-31T10:15:00Z"), "cancel history dropped, not moved"
    assert w.shard.deleted == []
    assert not [
        s for s in w.db.statements if s.startswith("DELETE FROM snapshot_items")
    ]
    assert stats["snapshots_skipped_reuse"] == 1


def test_protected_reuse_root_keeps_its_item_rows_not_just_its_blobs():
    """Retaining the bytes is only half the fix: this function also
    DELETEs snapshot_items and zeroes the snapshot row. Do that to a
    chain root and the retained descendant resolves to an empty
    inventory — the blobs survive but nothing points at them, which is
    the same unrestorable backup by a different route. Leave the root
    alone and say so loudly; an operator rehydrates the heir to reclaim."""
    w = _CacheWorld()
    descendant = uuid.uuid4()
    w.db.snapshots.append({
        "id": descendant, "job_id": uuid.uuid4(),
        "status": SnapshotStatus.COMPLETED,
        "reuse_chain_root_id": w.snapshot_id,
        "reuse_of_snapshot_id": w.snapshot_id,
    })
    p = w.path()
    w.add(p)

    stats = w.run()

    assert w.shard.deleted == []
    # "Destructive" means the row delete or the counter zeroing — the two
    # things that would empty the descendant's inventory. The protective
    # UPDATE from hold_protected_reuse_roots (status out of IN_PROGRESS +
    # cancel marker moved, counters untouched) is not destructive and is
    # required: see
    # test_protected_reuse_root_is_put_out_of_the_unguarded_sweeps_reach.
    destructive = [
        s for s in w.db.statements
        if s.startswith("DELETE FROM snapshot_items")
        or (s.startswith("UPDATE snapshots")
            and ("item_count" in s or "bytes_total" in s))
    ]
    assert destructive == [], (
        f"tore down the rows of a chain root that {descendant} still "
        f"resolves through: {destructive}"
    )
    assert stats["snapshots_skipped_reuse"] == 1


def test_unresolvable_reuse_protection_deletes_nothing_and_raises():
    """Fail CLOSED, not half-closed. If the reuse-chain probe can't
    answer, we don't know whether any target snapshot is the chain root
    of a live descendant — so the row delete and the counter-zeroing
    flip are exactly as unsafe as the blob delete. Retaining the bytes
    while still DELETEing snapshot_items and zeroing
    item_count/bytes_total leaves the descendant resolving to an empty
    inventory: bytes present, unreachable, unrestorable. Delete nothing
    and make the failure loud enough for the caller to retry."""
    from shared.backup_cleanup import BlobCleanupGuardUnavailable
    w = _CacheWorld()
    p = w.path()
    w.add(p)
    w.db.reuse_probe_raises = True

    raised = None
    try:
        w.run()
    except BlobCleanupGuardUnavailable as exc:
        raised = exc

    assert w.shard.deleted == []
    destructive = [
        s for s in w.db.statements
        if s.startswith("DELETE FROM snapshot_items")
        or s.startswith("UPDATE snapshots")
    ]
    assert destructive == [], (
        f"reap ran without knowing which snapshots are protected: {destructive}"
    )
    # ...and loudly: a silent "success" return hides the fact that the
    # job's bytes were never reclaimed and nothing will retry.
    assert isinstance(raised, BlobCleanupGuardUnavailable)
    assert "simulated" in str(raised), (
        "the underlying DB error must survive into the raised message"
    )


def test_chat_url_cache_row_is_cleared_before_its_blob_is_deleted():
    """chat_url_cache is a tenant-scoped, cross-snapshot
    (url_sha256 → blob_path) map and the worker's bulk hit path reuses
    blob_path with NO existence probe. If the row outlives the blob, a
    FUTURE backup stamps a brand-new snapshot_items row onto a dead
    path — unrestorable, and invisible until restore."""
    w = _CacheWorld()
    p = w.path("att_" + "a" * 40)
    w.add(p)
    w.db.chat_cache = [{"tenant_id": w.tenant, "blob_path": p}]

    stats = w.run()

    assert p in w.shard.deleted
    assert w.db.chat_cache[0]["blob_path"] is None, (
        "blob deleted but chat_url_cache still points at it"
    )
    assert w.events.index("cache_invalidate") < w.events.index(f"delete:{p}"), (
        "cache cleared AFTER the delete — a crash in between leaves the "
        "cache pointing at bytes that no longer exist"
    )
    assert stats["cache_rows_invalidated"] == 1


def test_cache_invalidation_keeps_content_size_of_rows_with_inline_bytes():
    """Clearing the dead pointer must not corrupt the surviving row. Both
    read paths hit on ``content_hash AND (blob_path OR inline_b64)``
    (workers/backup-worker/main.py:9408, 9884), so a row carrying BOTH
    stays a HIT with blob_path NULL — and ``content_size = row.content_size
    or 0`` (9418, 9891) then records that attachment with size 0 in every
    future backup. _write_tenant_url_cache's COALESCE merge can populate
    both columns, so this row shape is reachable. Nulling blob_path alone
    is sufficient and lossless."""
    w = _CacheWorld()
    p = w.path("att_" + "c" * 40)
    w.add(p)
    w.db.chat_cache = [{
        "tenant_id": w.tenant, "blob_path": p,
        "inline_b64": "QUJD", "content_size": 1234,
    }]

    w.run()

    assert w.db.chat_cache[0]["blob_path"] is None
    assert w.db.chat_cache[0]["content_size"] == 1234, (
        "cache row survives as a hit via inline_b64 but its recorded size "
        "was wiped — the next backup logs this attachment as 0 bytes"
    )


def test_chat_url_cache_invalidation_failure_retains_the_blob():
    """Can't clear the pointer ⇒ don't delete the target."""
    w = _CacheWorld()
    p = w.path("att_" + "b" * 40)
    w.add(p)
    w.db.chat_cache = [{"tenant_id": w.tenant, "blob_path": p}]
    w.db.cache_update_raises = True

    stats = w.run()

    assert w.shard.deleted == []
    assert stats["blobs_deleted"] == 0


# ─── NITs ────────────────────────────────────────────────────────────


def test_deleting_a_blob_evicts_the_in_process_dedup_lru_entry():
    """shared/blob_dedup.py caches (tenant, checksum) → blob_path for 5
    minutes per process. Without eviction, this same process can hand a
    just-deleted path to the next backup as a dedup hit."""
    from shared import blob_dedup
    w = _CacheWorld()
    p = w.path()
    w.add(p, checksum="lru1")
    _run(blob_dedup._lru_put(w.tenant, "lru1", p))
    hit, val = _run(blob_dedup._lru_get(w.tenant, "lru1"))
    assert (hit, val) == (True, p), "precondition: LRU seeded"

    w.run()

    assert p in w.shard.deleted
    hit, _ = _run(blob_dedup._lru_get(w.tenant, "lru1"))
    assert not hit, (
        "deleted blob_path is still served from the per-process dedup LRU"
    )


def test_retained_paths_are_counted_distinctly_from_skipped_deletes():
    """Two items sharing one retained path is ONE retained blob, not
    two. ``blobs_retained`` stays occurrence-shaped (symmetric with
    blobs_deleted = delete calls made); ``blob_paths_retained`` is the
    distinct-path number an operator reconciles against orphan GC."""
    w = _CacheWorld()
    foreign = _path(w.tenant, w.resource, uuid.uuid4())
    w.add(foreign, checksum="s1")
    w.add(foreign, checksum="s1")

    stats = w.run()

    assert w.shard.deleted == []
    assert stats["blobs_retained"] == 2
    assert stats["blob_paths_retained"] == 1
    assert stats["retained_reasons"]["foreign_owner"] == 1
