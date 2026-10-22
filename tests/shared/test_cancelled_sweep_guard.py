"""The 30-second cancelled-snapshot sweep must go through the reference
guard, and the guard's unindexed fallback must be affordable on the REAL
prod schema.

Two defects, one file, because they are the same code path.

IMPORTANT 0 — ``shared/backup_cleanup.py`` was ported onto the perf
lineage but nothing on that lineage calls it. The only blob-deleting
path perf deploys is ``_sweep_cancelled_snapshots`` inside
services/backup-scheduler (30s interval), which read
``blob_path, backend_id FROM snapshot_items`` and deleted every row's
path with no sharer probe, no reuse-root hold, no RETAIN_* accounting
and no fail-closed behaviour. Blobs are content-deduped, so an older
retained snapshot of the same user — or another account sharing the
checksum — keeps pointing at bytes the sweep removed: an unrestorable
backup, discovered at restore time. The module's own docstring calls
this "the path most cancelled bytes actually die on" and spells out the
adoption order; these tests drive that order end to end.

IMPORTANT 1 — the unkeyed (checksum-less) fallback was budget-sized
against a 16-way hash-partitioned ``snapshot_items``. Prod's
``snapshot_items`` is a PLAIN ~87GB table (relkind='r', zero
partitions), so each chunk is a full sequential scan of all 87GB, not
1/16th of it, and the old ``_UNKEYED_RUN_BUDGET = 2000`` bought FOUR of
them per teardown — with no ``statement_timeout``, fired as a detached
background task on every cancel, on the database the live Gold backups
share. Correctness was never the issue (past-budget paths are
RETAINED); cost was. These tests pin the three bounds that make it
affordable without deleting anything unproven: one chunk per run, a
statement timeout, and single-flight so a burst of cancels cannot fan
out concurrent scans.
"""
from __future__ import annotations

import asyncio
import uuid
from typing import Any, Dict, List, Optional

import pytest

from shared.models import SnapshotStatus


def _run(coro):
    return asyncio.run(coro)


def _path(tenant, resource, snapshot, item=None):
    """``build_blob_path`` shape: tenant/resource/snapshot/ts/item."""
    return (
        f"{tenant}/{resource}/{snapshot}/20260731T101500/"
        f"{item or uuid.uuid4()}"
    )


# ─── Fakes ───────────────────────────────────────────────────────────


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


class _Row:
    def __init__(self, **kw):
        self.__dict__.update(kw)

    def __iter__(self):
        return iter(self.__dict__.values())


class _DB:
    """Row store for every surface the sweep teardown touches."""

    def __init__(self):
        self.items: List[Dict[str, Any]] = []
        self.snapshots: List[Dict[str, Any]] = []
        self.chat_cache: List[Dict[str, Any]] = []
        self.statements: List[str] = []
        self.events: List[str] = []
        self.reuse_probe_raises = False
        self.hold_raises = False
        self.cache_raises_for: set = set()
        self.path_probe_raises = False
        self.checksum_probes = 0
        self.path_probes = 0
        self.path_probe_gate: Optional[asyncio.Event] = None
        self.savepoints = 0

    def add_item(self, snapshot_id, blob_path, *, tenant_id=None,
                 content_checksum=None, backend_id=None):
        self.items.append({
            "snapshot_id": snapshot_id, "blob_path": blob_path,
            "tenant_id": tenant_id, "content_checksum": content_checksum,
            "backend_id": backend_id or uuid.uuid4(),
        })


class _Nested:
    def __init__(self, db):
        self.db = db

    async def __aenter__(self):
        self.db.savepoints += 1
        return self

    async def __aexit__(self, *exc):
        return False


class _StreamResult:
    """Stand-in for ``AsyncResult`` from ``AsyncSession.stream`` — the
    sweep teardown reads its candidates through a server-side cursor so
    peak memory is one batch, not one snapshot."""

    def __init__(self, rows: List[Any]):
        self._rows = list(rows)

    async def partitions(self, size=None):
        n = int(size or 500)
        for start in range(0, len(self._rows), n):
            yield self._rows[start:start + n]


class _Session:
    def __init__(self, db: _DB):
        self.db = db

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def begin_nested(self):
        return _Nested(self.db)

    async def stream(self, stmt, params=None):
        self.db.statements.append(str(stmt))
        sids = {uuid.UUID(str(x)) for x in ((params or {}).get("sids") or [])}
        return _StreamResult([
            _Row(blob_path=i["blob_path"], snapshot_id=i["snapshot_id"],
                 tenant_id=i["tenant_id"],
                 content_checksum=i["content_checksum"],
                 backend_id=i["backend_id"])
            for i in self.db.items
            if i["snapshot_id"] in sids and i["blob_path"]
        ])

    def expunge(self, obj):
        return None

    async def commit(self):
        return None

    async def rollback(self):
        return None

    async def execute(self, stmt, params=None):
        sql = str(stmt)
        self.db.statements.append(sql)
        descs = getattr(stmt, "column_descriptions", None)

        if "chat_url_cache" in sql:
            binds = dict(stmt.compile().params)
            paths = {
                x for v in binds.values()
                for x in (v if isinstance(v, (list, tuple, set)) else [v])
                if isinstance(x, str)
            }
            if paths & self.db.cache_raises_for:
                raise RuntimeError("simulated PG failure clearing chat_url_cache")
            self.db.events.append("cache_invalidate")
            n = 0
            for row in self.db.chat_cache:
                if row.get("blob_path") in paths:
                    row["blob_path"] = None
                    n += 1
            return _Rows([None] * n)

        if not descs:
            if "UPDATE snapshots" in sql and "reuse_protected" in sql:
                if self.db.hold_raises:
                    raise RuntimeError("simulated PG failure on the protective hold")
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
                         backend_id=i["backend_id"])
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

        if names[:1] == ["blob_path"]:
            self.db.path_probes += 1
            if self.db.path_probe_gate is not None:
                await self.db.path_probe_gate.wait()
            if self.db.path_probe_raises:
                raise RuntimeError(
                    "canceling statement due to statement timeout"
                )
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


class _Store:
    def __init__(self, events: List[str]):
        self.events = events
        self.deleted: List[str] = []

    async def delete(self, container, path):
        self.events.append(f"delete:{path}")
        self.deleted.append(path)


class _World:
    """Drives the real sweep-teardown helper over the fake DB."""

    def __init__(self):
        self.tenant = uuid.uuid4()
        self.resource = uuid.uuid4()
        self.db = _DB()
        self.events = self.db.events
        self.store = _Store(self.events)

    def snapshot(self, *, status=SnapshotStatus.IN_PROGRESS,
                 cancelled=True, **kw):
        sid = uuid.uuid4()
        row = {
            "id": sid, "status": status,
            "item_count": 1, "bytes_total": 4096,
            "extra_data": ({"cancelled_at": "2026-07-31T10:15:00Z"}
                           if cancelled else {}),
            "reuse_chain_root_id": None, "reuse_of_snapshot_id": None,
        }
        row.update(kw)
        self.db.snapshots.append(row)
        return sid

    def path(self, sid, item=None):
        return _path(self.tenant, self.resource, sid, item)

    def add(self, sid, blob_path, *, checksum="c1", tenant=None):
        self.db.add_item(sid, blob_path,
                         tenant_id=self.tenant if tenant is None else tenant,
                         content_checksum=checksum)
        return blob_path

    def run(self, sids, **kw):
        from shared.backup_cleanup import teardown_cancelled_sweep_blobs
        return _run(teardown_cancelled_sweep_blobs(
            lambda: _Session(self.db),
            snapshot_ids=list(sids),
            container_for=lambda sid: "backup-user-mail-abcd1234",
            store_for=lambda backend_id: self.store,
            **kw,
        ))


def _sweep_would_reselect(snap_row, *, job_status="CANCELLED"):
    """``_sweep_cancelled_snapshots``'s candidate WHERE clause:
    Populations A+C = extra_data->>'cancelled_at' IS NOT NULL,
    Population B = status='IN_PROGRESS' AND job CANCELLED/FAILED."""
    extra = snap_row.get("extra_data") or {}
    if extra.get("cancelled_at") is not None:
        return True
    status = snap_row.get("status")
    name = getattr(status, "name", str(status))
    return name == "IN_PROGRESS" and job_status in ("CANCELLED", "FAILED")


# ─── IMPORTANT 0: the sweep must consult the guard ───────────────────


def test_sweep_teardown_retains_a_dedup_shared_blob():
    """The whole reason the guard exists. The sweep's raw
    ``SELECT blob_path ... WHERE snapshot_id = :sid`` deletes bytes a
    RETAINED snapshot still points at, because content dedup hands a new
    snapshot_items row an older snapshot's blob_path."""
    from shared.backup_cleanup import RETAIN_SHARED_REFERENCE
    w = _World()
    doomed = w.snapshot()
    retained = w.snapshot(cancelled=False, status=SnapshotStatus.COMPLETED)
    p = w.path(doomed)
    w.add(doomed, p, checksum="deadbeef")
    w.add(retained, p, checksum="deadbeef")   # dedup-inherited the bytes

    res = w.run([doomed])

    assert w.store.deleted == [], (
        "sweep deleted a blob a retained snapshot still references — that "
        "snapshot is now unrestorable"
    )
    assert res.retained_reasons.get(RETAIN_SHARED_REFERENCE) == 1
    # the doomed row itself is still reapable: the live referrer owns its
    # own snapshot_items row.
    assert doomed in res.reapable


def test_sweep_teardown_deletes_an_unshared_blob():
    """Negative control: nothing else references it, so the bytes go."""
    w = _World()
    doomed = w.snapshot()
    p = w.path(doomed)
    w.add(doomed, p, checksum="solo")

    res = w.run([doomed])

    assert w.store.deleted == [p]
    assert res.blobs_deleted == 1
    assert doomed in res.reapable


def test_sweep_teardown_treats_the_whole_batch_as_doomed():
    """Two cancelled snapshots in the same pass share a path. If the
    guard is called with one snapshot id at a time, the sibling looks
    like a live referrer and NOTHING is ever reclaimed — the module
    docstring calls this out explicitly."""
    w = _World()
    a = w.snapshot()
    b = w.snapshot()
    p = w.path(a)
    w.add(a, p, checksum="shared")
    w.add(b, p, checksum="shared")

    res = w.run([a, b])

    assert p in w.store.deleted, (
        "sibling doomed snapshot was mistaken for a live referrer; the "
        "sweep would leak these bytes forever"
    )
    assert res.reapable == frozenset({a, b})


def test_sweep_teardown_holds_a_protected_reuse_root_and_skips_it():
    """A doomed chain root whose LIVE descendant owns zero
    snapshot_items rows: its blobs AND its rows are load-bearing. Skip it
    wholesale and make the protection durable, or the next 30s pass
    reaps it anyway."""
    w = _World()
    root = w.snapshot()
    w.snapshot(cancelled=False, status=SnapshotStatus.COMPLETED,
               reuse_chain_root_id=root, reuse_of_snapshot_id=root)
    p = w.path(root)
    w.add(root, p)
    root_row = w.db.snapshots[0]
    assert _sweep_would_reselect(root_row), "precondition: the sweep wants it"

    res = w.run([root])

    assert w.store.deleted == []
    assert root in res.held
    assert root not in res.reapable, (
        "caller would DELETE the chain root's snapshot_items rows — the "
        "descendant then resolves to an empty inventory"
    )
    assert not _sweep_would_reselect(root_row), (
        f"held root still matches the sweep's own predicate: "
        f"status={root_row['status']} extra={root_row['extra_data']}"
    )
    assert root_row["item_count"] == 1 and root_row["bytes_total"] == 4096


def test_sweep_teardown_fails_closed_when_protection_is_unresolvable():
    """If the reuse-chain probe can't answer we do not know which
    snapshots are load-bearing, so delete NOTHING — not blobs, and the
    caller must not delete rows either — and raise loudly."""
    from shared.backup_cleanup import BlobCleanupGuardUnavailable
    w = _World()
    a = w.snapshot()
    w.add(a, w.path(a))
    w.db.reuse_probe_raises = True

    with pytest.raises(BlobCleanupGuardUnavailable) as exc:
        w.run([a])

    assert w.store.deleted == []
    assert "simulated" in str(exc.value), (
        "the underlying DB error must survive into the raised message"
    )


def test_sweep_teardown_fails_closed_when_the_hold_cannot_be_made_durable():
    """Refusing to touch a protected root is worthless if the row is
    left in a state the next 30s pass re-selects."""
    from shared.backup_cleanup import BlobCleanupGuardUnavailable
    w = _World()
    root = w.snapshot()
    w.snapshot(cancelled=False, status=SnapshotStatus.COMPLETED,
               reuse_chain_root_id=root, reuse_of_snapshot_id=root)
    w.add(root, w.path(root))
    w.db.hold_raises = True

    with pytest.raises(BlobCleanupGuardUnavailable):
        w.run([root])

    assert w.store.deleted == []


def test_sweep_teardown_clears_chat_url_cache_before_deleting():
    """chat_url_cache is a tenant-scoped, CROSS-snapshot
    (url_sha256 → blob_path) map and the worker's bulk hit path reuses
    blob_path with no existence probe. If the sweep deletes the blob and
    leaves the row, the NEXT backup stamps a fresh snapshot_items row
    onto a dead path."""
    w = _World()
    doomed = w.snapshot()
    p = w.path(doomed, "att_" + "a" * 40)
    w.add(doomed, p)
    w.db.chat_cache = [{"tenant_id": w.tenant, "blob_path": p}]

    res = w.run([doomed])

    assert p in w.store.deleted
    assert w.db.chat_cache[0]["blob_path"] is None, (
        "blob deleted but chat_url_cache still points at it"
    )
    assert w.events.index("cache_invalidate") < w.events.index(f"delete:{p}"), (
        "cache cleared AFTER the delete — a crash in between leaves a live "
        "pointer to bytes that no longer exist"
    )
    assert res.cache_rows_invalidated == 1


def test_sweep_teardown_blocks_only_the_snapshot_whose_guard_failed():
    """A per-snapshot guard failure must retain that snapshot's bytes AND
    leave its rows for the next pass (deleting the rows would orphan the
    bytes with nothing left to find them). It must not poison the rest
    of the pass."""
    w = _World()
    bad = w.snapshot()
    good = w.snapshot()
    p_bad = w.path(bad, "att_" + "b" * 40)
    p_good = w.path(good)
    w.add(bad, p_bad)
    w.add(good, p_good)
    w.db.chat_cache = [{"tenant_id": w.tenant, "blob_path": p_bad}]
    w.db.cache_raises_for = {p_bad}

    res = w.run([bad, good])

    assert p_bad not in w.store.deleted
    assert bad in res.blocked and bad not in res.reapable
    assert p_good in w.store.deleted
    assert good in res.reapable


# ─── IMPORTANT 1: the unkeyed fallback must be affordable ────────────


def test_unkeyed_run_budget_is_one_scan_of_the_real_plain_table():
    """``_UNKEYED_RUN_BUDGET = 2000`` was sized as "4 scans of every
    snapshot_items PARTITION". Prod has no partitions: snapshot_items is
    a plain ~87GB heap, so that budget is 4 full 87GB sequential scans
    per teardown, fired from a detached task on every cancel."""
    from shared.backup_cleanup import _PROBE_CHUNK, _UNKEYED_RUN_BUDGET
    assert _UNKEYED_RUN_BUDGET <= _PROBE_CHUNK, (
        f"budget {_UNKEYED_RUN_BUDGET} buys "
        f"{-(-_UNKEYED_RUN_BUDGET // _PROBE_CHUNK)} sequential scans of the "
        f"87GB snapshot_items heap per run"
    )


def test_unkeyed_probe_runs_under_a_statement_timeout():
    """No session-level statement_timeout exists (shared/database.py sets
    it only via SET LOCAL on DDL paths) and both callers run detached, so
    nothing bounds this scan's wall clock. Bound it here, and reset it so
    the rest of the transaction keeps the cluster default."""
    from shared.backup_cleanup import BlobCandidate, select_deletable_blob_paths
    db = _DB()
    sid = uuid.uuid4()
    tenant = uuid.uuid4()
    p = _path(tenant, uuid.uuid4(), sid)
    db.add_item(sid, p, tenant_id=tenant, content_checksum=None)

    _run(select_deletable_blob_paths(
        _Session(db), doomed_snapshot_ids=[sid],
        candidates=[BlobCandidate(blob_path=p, snapshot_id=sid,
                                  tenant_id=tenant)],
    ))

    assert db.path_probes == 1, "precondition: the unkeyed probe ran"
    stmts = [s for s in db.statements if "statement_timeout" in s]
    assert stmts, "unkeyed sequential scan issued with no statement_timeout"
    probe_at = next(i for i, s in enumerate(db.statements)
                    if "blob_path IN" in s)
    set_at = next(i for i, s in enumerate(db.statements)
                  if "statement_timeout" in s and "DEFAULT" not in s.upper())
    assert set_at < probe_at, "timeout set after the scan it is meant to bound"
    assert any("DEFAULT" in s.upper() for s in stmts), (
        "statement_timeout never reset — the rest of the transaction "
        "inherits it"
    )
    assert db.savepoints >= 1, (
        "the timed-out probe would abort the whole transaction; run it in "
        "a savepoint so the caller's session survives"
    )


def test_unkeyed_probe_timeout_retains_only_the_unverified_paths():
    """A timeout must not lose the keyed verdicts and must not delete
    anything unproven: the unkeyed residue is RETAINED as unverifiable
    and the keyed deletables still go."""
    from shared.backup_cleanup import (
        BlobCandidate, RETAIN_UNVERIFIABLE, select_deletable_blob_paths,
    )
    db = _DB()
    sid, tenant = uuid.uuid4(), uuid.uuid4()
    keyed = _path(tenant, uuid.uuid4(), sid)
    unkeyed = _path(tenant, uuid.uuid4(), sid)
    db.add_item(sid, keyed, tenant_id=tenant, content_checksum="k1")
    db.add_item(sid, unkeyed, tenant_id=tenant, content_checksum=None)
    db.path_probe_raises = True

    plan = _run(select_deletable_blob_paths(
        _Session(db), doomed_snapshot_ids=[sid],
        candidates=[
            BlobCandidate(blob_path=keyed, snapshot_id=sid, tenant_id=tenant,
                          content_checksum="k1"),
            BlobCandidate(blob_path=unkeyed, snapshot_id=sid,
                          tenant_id=tenant),
        ],
    ))

    assert plan.deletable == frozenset({keyed})
    assert plan.retained.get(unkeyed) == RETAIN_UNVERIFIABLE
    assert plan.unkeyed_probed == 0, (
        "a probe that timed out must not consume the run's scan budget"
    )


def test_concurrent_teardowns_cannot_fan_out_concurrent_scans():
    """Both callers fire the teardown as a DETACHED background task on
    every cancel, and the sweep re-fires every 30 seconds. Two of them
    scanning the 87GB heap at once is how the live Gold backups get
    starved, so the fallback is single-flight: a teardown that finds a
    scan already running RETAINS instead of piling on."""
    from shared.backup_cleanup import (
        BlobCandidate, RETAIN_UNVERIFIABLE, select_deletable_blob_paths,
    )

    async def _drive():
        db = _DB()
        db.path_probe_gate = asyncio.Event()
        tenant = uuid.uuid4()
        sids = [uuid.uuid4(), uuid.uuid4()]
        paths = []
        for sid in sids:
            p = _path(tenant, uuid.uuid4(), sid)
            db.add_item(sid, p, tenant_id=tenant, content_checksum=None)
            paths.append(p)

        async def _one(sid, p):
            return await select_deletable_blob_paths(
                _Session(db), doomed_snapshot_ids=[sid],
                candidates=[BlobCandidate(blob_path=p, snapshot_id=sid,
                                          tenant_id=tenant)],
            )

        first = asyncio.create_task(_one(sids[0], paths[0]))
        await asyncio.sleep(0)          # let it reach the probe
        second = asyncio.create_task(_one(sids[1], paths[1]))
        await asyncio.sleep(0)
        db.path_probe_gate.set()
        return db, paths, await asyncio.gather(first, second)

    db, paths, (plan_a, plan_b) = _run(_drive())

    assert db.path_probes == 1, (
        f"{db.path_probes} concurrent sequential scans of snapshot_items "
        "from one burst of cancels"
    )
    assert plan_a.deletable == frozenset({paths[0]})
    assert plan_b.retained.get(paths[1]) == RETAIN_UNVERIFIABLE
    assert plan_b.unkeyed_probed == 0
