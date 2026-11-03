"""The cancelled-snapshot sweep must be bounded in SCANS and in MEMORY,
not just in probed paths.

Both defects are properties of ONE pass of
``shared.backup_cleanup.teardown_cancelled_sweep_blobs`` — the helper
``_sweep_cancelled_snapshots`` (services/backup-scheduler, every 30
SECONDS) drives.

BLOCKER — the unkeyed budget bounds PATHS (500 per run) but not SCANS.
The teardown called ``select_deletable_blob_paths`` once per snapshot, so
every snapshot carrying even ONE checksum-less candidate issued its own
``blob_path IN (...)`` statement. On the real prod schema (PG 17.10,
``snapshot_items`` = PLAIN ~87GB heap, relkind='r', ZERO partitions, PK
btree(id) alone, NO index on blob_path — see shared/database.py) each of
those is a full sequential scan of all 87GB. A pass covering the
``LIMIT 200`` candidate batch therefore bought ~200 of them, every 30
seconds, on the database the live Gold backups share. The keyed
(``tenant_id``, ``content_checksum``) probe fanned out per snapshot the
same way. Both must be aggregated across the whole pass so the query
count stops scaling with the snapshot count.

IMPORTANT — memory. backup-scheduler's container limit is 512Mi. The
per-snapshot call shape read one snapshot's ENTIRE ``snapshot_items``
inventory into ``BlobCandidate`` objects and then built a whole-snapshot
plan on top of it (``by_path`` = 3 sets per path, keyed checksum sets,
survivors, retained). A cancelled 200k-item snapshot is a real shape, and
that multiplier OOM-kills the pod. The pass has to stream.

Correctness must not weaken anywhere: a path whose protection cannot be
computed is RETAINED, never deleted. tests/shared/test_cancelled_sweep_guard.py
owns those behaviours; this file owns the cost bounds and pins the
fail-closed side of every bound it introduces.
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


class _Row:
    def __init__(self, **kw):
        self.__dict__.update(kw)


class _StreamResult:
    """Stand-in for ``AsyncResult`` from ``AsyncSession.stream``."""

    def __init__(self, rows: List[Any], db: "_DB"):
        self._rows = list(rows)
        self._db = db

    async def partitions(self, size=None):
        n = int(size or 500)
        for start in range(0, len(self._rows), n):
            self._db.stream_partitions += 1
            yield self._rows[start:start + n]


class _DB:
    def __init__(self):
        self.items: List[Dict[str, Any]] = []
        self.snapshots: List[Dict[str, Any]] = []
        self.chat_cache: List[Dict[str, Any]] = []
        self.statements: List[str] = []
        self.events: List[str] = []
        self.checksum_probes = 0
        self.path_probes = 0
        self.per_snapshot_reads = 0
        self.streamed_reads = 0
        self.stream_partitions = 0
        self.cache_updates = 0
        self.cache_raises_for: set = set()
        self.stream_raises = False

    def add_item(self, snapshot_id, blob_path, *, tenant_id=None,
                 content_checksum=None, backend_id=None):
        self.items.append({
            "snapshot_id": snapshot_id, "blob_path": blob_path,
            "tenant_id": tenant_id, "content_checksum": content_checksum,
            "backend_id": backend_id or uuid.uuid4(),
        })


class _Nested:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _Session:
    def __init__(self, db: _DB):
        self.db = db

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def begin_nested(self):
        return _Nested()

    async def commit(self):
        return None

    async def rollback(self):
        return None

    def _candidate_rows(self, sids):
        return [
            _Row(blob_path=i["blob_path"], snapshot_id=i["snapshot_id"],
                 tenant_id=i["tenant_id"],
                 content_checksum=i["content_checksum"],
                 backend_id=i["backend_id"])
            for i in self.db.items
            if i["snapshot_id"] in sids and i["blob_path"]
        ]

    async def stream(self, stmt, params=None):
        sql = str(stmt)
        self.db.statements.append(sql)
        assert "FROM snapshot_items" in sql, sql
        if self.db.stream_raises:
            raise RuntimeError("simulated PG failure streaming candidates")
        self.db.streamed_reads += 1
        sids = {uuid.UUID(str(x)) for x in ((params or {}).get("sids") or [])}
        return _StreamResult(self._candidate_rows(sids), self.db)

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
            self.db.cache_updates += 1
            self.db.events.append("cache_invalidate")
            n = 0
            for row in self.db.chat_cache:
                if row.get("blob_path") in paths:
                    row["blob_path"] = None
                    n += 1
            return _Rows([None] * n)

        if not descs:
            if "UPDATE snapshots" in sql and "reuse_protected" in sql:
                return _Rows([])
            if "FROM snapshot_items" in sql and "blob_path IS NOT NULL" in sql:
                # The OLD per-snapshot read shape.
                self.db.per_snapshot_reads += 1
                sid = uuid.UUID(str((params or {}).get("sid")))
                return _Rows(self._candidate_rows({sid}))
            if "reuse_chain_root_id" in sql and "FROM snapshots" in sql:
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
    def __init__(self, events: List[str], gate: Optional[asyncio.Event] = None):
        self.events = events
        self.deleted: List[str] = []
        self.gate = gate

    async def delete(self, container, path):
        if self.gate is not None:
            self.events.append(f"gate:{path}")
            await self.gate.wait()
        self.events.append(f"delete:{path}")
        self.deleted.append(path)


class _World:
    def __init__(self, gate: Optional[asyncio.Event] = None):
        self.tenant = uuid.uuid4()
        self.resource = uuid.uuid4()
        self.db = _DB()
        self.events = self.db.events
        self.store = _Store(self.events, gate)

    def snapshot(self, *, status=SnapshotStatus.IN_PROGRESS, cancelled=True,
                 **kw):
        sid = uuid.uuid4()
        row = {
            "id": sid, "status": status, "item_count": 1, "bytes_total": 4096,
            "extra_data": ({"cancelled_at": "2026-07-31T10:15:00Z"}
                           if cancelled else {}),
            "reuse_chain_root_id": None, "reuse_of_snapshot_id": None,
        }
        row.update(kw)
        self.db.snapshots.append(row)
        return sid

    def add(self, sid, *, checksum="c1", item=None):
        p = _path(self.tenant, self.resource, sid, item)
        self.db.add_item(sid, p, tenant_id=self.tenant,
                         content_checksum=checksum)
        return p

    def coro(self, sids, **kw):
        from shared.backup_cleanup import teardown_cancelled_sweep_blobs
        return teardown_cancelled_sweep_blobs(
            lambda: _Session(self.db),
            snapshot_ids=list(sids),
            container_for=lambda sid: "backup-user-mail-abcd1234",
            store_for=lambda backend_id: self.store,
            **kw,
        )

    def run(self, sids, **kw):
        return _run(self.coro(sids, **kw))


# ─── BLOCKER: bound the SCANS, not just the paths ─────────────────────


def test_one_pass_issues_at_most_one_unindexed_blob_path_scan():
    """40 cancelled snapshots, each with a single checksum-less candidate
    (every azure-workload-worker row sets content_checksum=None, and so
    does every attachment fan-out — 18 of 49 blob_path writers).

    40 paths is well inside the 500-path run budget, so the OLD code
    happily probed all of them — as 40 separate ``blob_path IN (...)``
    statements, one per snapshot, i.e. 40 full sequential scans of the
    87GB heap in ONE pass of a sweep that fires every 30 seconds. The
    budget has to bound SCANS."""
    w = _World()
    sids = []
    for _ in range(40):
        sid = w.snapshot()
        w.db.add_item(sid, _path(w.tenant, w.resource, sid),
                      tenant_id=w.tenant, content_checksum=None)
        sids.append(sid)

    res = w.run(sids)

    assert w.db.path_probes <= 1, (
        f"{w.db.path_probes} unindexed sequential scans of the 87GB "
        f"snapshot_items heap in ONE 30-second sweep pass"
    )
    # ...and the bytes are still actually reclaimed, not silently kept.
    assert len(w.store.deleted) == 40
    assert res.reapable == frozenset(sids)


def test_keyed_sharer_probe_does_not_fan_out_per_snapshot():
    """Same defect on the indexed side: one ``select_deletable_blob_paths``
    call per snapshot means one ``(tenant_id, content_checksum) IN (...)``
    round trip per snapshot. 60 snapshots' worth of candidates fit in a
    single _PROBE_CHUNK, so one pass should cost a couple of probes, not
    60."""
    w = _World()
    sids = []
    for i in range(60):
        sid = w.snapshot()
        w.add(sid, checksum=f"sum{i}")
        sids.append(sid)

    res = w.run(sids)

    assert w.db.checksum_probes <= 2, (
        f"{w.db.checksum_probes} keyed probes for 60 snapshots — the "
        f"reference check still fans out per snapshot"
    )
    assert len(w.store.deleted) == 60
    assert res.reapable == frozenset(sids)


def test_pass_reads_candidates_once_for_the_whole_batch():
    """The per-snapshot ``WHERE snapshot_id = :sid`` read is what forces
    the per-snapshot plan (and therefore the per-snapshot probes). One
    streamed read for the pass replaces it."""
    w = _World()
    sids = [w.snapshot() for _ in range(25)]
    for sid in sids:
        w.add(sid, checksum="shared-sum")

    w.run(sids)

    assert w.db.per_snapshot_reads == 0, (
        "candidates are still read one snapshot at a time"
    )
    assert w.db.streamed_reads == 1, (
        f"expected exactly one streamed candidate read per pass, got "
        f"{w.db.streamed_reads}"
    )


# ─── IMPORTANT: peak memory on a 512Mi pod ───────────────────────────


def test_a_huge_snapshot_is_planned_in_bounded_batches(monkeypatch):
    """A cancelled 200k-item snapshot is the worst case, and
    backup-scheduler's limit is 512Mi. Never hold a whole snapshot's
    candidates — or a whole-snapshot plan — in memory at once."""
    import shared.backup_cleanup as bc

    sizes: List[int] = []
    real = bc.select_deletable_blob_paths

    async def _spy(session, **kw):
        cands = list(kw.pop("candidates"))
        sizes.append(len(cands))
        return await real(session, candidates=cands, **kw)

    monkeypatch.setattr(bc, "select_deletable_blob_paths", _spy)

    w = _World()
    sid = w.snapshot()
    for i in range(4000):
        w.add(sid, checksum=f"sum{i}")

    res = w.run([sid])

    assert sizes, "the guard was never called"
    assert max(sizes) <= bc._SWEEP_ROW_BATCH, (
        f"planned {max(sizes)} candidates in one call — a 200k-item "
        f"snapshot then builds a 200k-path plan inside a 512Mi pod"
    )
    assert len(sizes) >= 8, (
        f"4000 candidates were planned in {len(sizes)} call(s); the pass "
        f"is not streaming"
    )
    assert len(w.store.deleted) == 4000, "streaming must not lose deletions"
    assert res.reapable == frozenset({sid})


def test_streamed_pass_still_retains_a_dedup_shared_blob():
    """Regression pin for the streamed/batched shape: the whole point of
    the guard survives batching. A retained snapshot dedup-inherited the
    path, so the bytes must stay."""
    from shared.backup_cleanup import RETAIN_SHARED_REFERENCE
    w = _World()
    doomed = [w.snapshot() for _ in range(30)]
    live = w.snapshot(cancelled=False, status=SnapshotStatus.COMPLETED)
    shared_path = None
    for i, sid in enumerate(doomed):
        p = w.add(sid, checksum=f"sum{i}")
        if i == 17:
            shared_path = p
            w.db.add_item(live, p, tenant_id=w.tenant, content_checksum="sum17")

    res = w.run(doomed)

    assert shared_path not in w.store.deleted, (
        "batching lost the sharer verdict — a retained snapshot is now "
        "unrestorable"
    )
    assert res.retained_reasons.get(RETAIN_SHARED_REFERENCE) == 1
    assert len(w.store.deleted) == 29


def test_unkeyed_paths_past_the_pass_budget_are_retained_not_deleted():
    """Aggregating the unkeyed probe across the pass must not turn the
    budget into a bigger budget. Past the run ceiling the paths are
    RETAINED — the failure mode is leaked storage, never lost bytes."""
    from shared.backup_cleanup import (
        RETAIN_UNVERIFIABLE, _UNKEYED_RUN_BUDGET,
    )
    w = _World()
    over = _UNKEYED_RUN_BUDGET + 40
    sids = []
    for _ in range(8):
        sids.append(w.snapshot())
    for i in range(over):
        sid = sids[i % len(sids)]
        w.db.add_item(sid, _path(w.tenant, w.resource, sid),
                      tenant_id=w.tenant, content_checksum=None)

    res = w.run(sids)

    assert w.db.path_probes <= 1
    assert len(w.store.deleted) == _UNKEYED_RUN_BUDGET, (
        f"deleted {len(w.store.deleted)} paths on a {_UNKEYED_RUN_BUDGET}"
        f"-path budget"
    )
    assert res.retained_reasons.get(RETAIN_UNVERIFIABLE) == 40


def test_a_failed_candidate_stream_deletes_nothing_and_reaps_nothing():
    """Fail closed on the read itself: without the inventory we cannot
    tell which bytes are safe, so no blob goes and no snapshot becomes
    reapable (its rows must survive for the next pass)."""
    w = _World()
    sids = [w.snapshot() for _ in range(3)]
    for sid in sids:
        w.add(sid)
    w.db.stream_raises = True

    res = w.run(sids)

    assert w.store.deleted == []
    assert res.reapable == frozenset()
    assert res.blocked == frozenset(sids)
    assert w.db.path_probes == 0, (
        "spent the pass's one 87GB sequential scan on a batch it could not "
        "authorise a single delete for"
    )


def test_cache_failure_still_blocks_only_the_snapshot_it_belongs_to():
    """Batching the chat_url_cache invalidation must not let one bad
    snapshot poison the rest of the pass."""
    w = _World()
    bad, good = w.snapshot(), w.snapshot()
    p_bad = w.add(bad, checksum="bad", item="att_" + "b" * 40)
    p_good = w.add(good, checksum="good")
    w.db.chat_cache = [{"tenant_id": w.tenant, "blob_path": p_bad}]
    w.db.cache_raises_for = {p_bad}

    res = w.run([bad, good])

    assert p_bad not in w.store.deleted
    assert bad in res.blocked and bad not in res.reapable
    assert p_good in w.store.deleted
    assert good in res.reapable


# ─── Passes must not overlap ─────────────────────────────────────────


def test_a_second_pass_refuses_to_start_while_one_is_running():
    """The sweep fires every 30 SECONDS. A pass that takes longer than
    that (200 snapshots of blob deletes) must not have another pass
    stacked on top of it — that is how two teardowns end up scanning the
    heap and deleting the same batch's bytes concurrently."""
    async def _drive():
        gate = asyncio.Event()
        w = _World(gate=gate)
        sids = [w.snapshot() for _ in range(2)]
        for sid in sids:
            w.add(sid)

        first = asyncio.create_task(w.coro(sids))
        for _ in range(60):
            await asyncio.sleep(0)
            if any(e.startswith("gate:") for e in w.events):
                break
        assert any(e.startswith("gate:") for e in w.events), (
            "first pass never reached a blob delete"
        )
        # A guarded pass returns immediately. An unguarded one walks
        # straight into the same gated deletes and never returns.
        try:
            second = await asyncio.wait_for(w.coro(sids), timeout=1.0)
        except asyncio.TimeoutError:
            second = None
        gate.set()
        return w, second, await first

    w, second, first = _run(_drive())

    assert second is not None, (
        "the second pass ran the teardown concurrently with the first"
    )
    assert second.skipped is True, (
        "a second sweep pass started while one was still running"
    )
    assert second.reapable == frozenset(), (
        "the skipped pass told its caller to DELETE rows it never checked"
    )
    assert second.held == frozenset() and second.blocked == frozenset()
    assert first.skipped is False
    assert len(w.store.deleted) == 2
