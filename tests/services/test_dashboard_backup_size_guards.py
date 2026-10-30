"""Regression: /dashboard/backup/size must not hammer the primary.

Over 4 h on prod (2026-10-14) 15 of 30 calls returned 504: every
resource with a snapshot was walked serially, each cold one paid a
newest-version hash aggregate over all of its snapshot_items rows
(Parallel Seq Scan for mid-size resources), the api-gateway's 60 s read
timeout fired, the gateway retried the GET once, and each browser call
left two abandoned 60-120 s query chains on the primary. The only cache
was a per-process dict, invalidated by every completed snapshot and
lost on restart.

Pins (services/dashboard-service/main.py):
  * cache decision = the resource's newest eligible snapshot id, looked
    up with ORDER BY created_at DESC LIMIT 1 on the resource's rows
    (ix_snapshots_resource_created); a heavy rollup runs only when that
    id changed, and its result is written through to
    resource_protected_bytes so other workers / restarts reuse it;
  * the endpoint stops walking when the client disconnected;
  * a per-request time budget returns the partial total with
    `partial: true` instead of letting the gateway time out, and the
    heavy statement itself runs under SET LOCAL statement_timeout;
  * heavy rollups are bounded by a per-process Semaphore(2);
  * the background prewarm loop starts at most once per process.
"""
from __future__ import annotations

import asyncio
import contextlib
import importlib.util
import inspect
import os
import pathlib
import sys
import uuid

import pytest
from sqlalchemy.exc import DBAPIError

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "services" / "dashboard-service" / "main.py"
_spec = importlib.util.spec_from_file_location("dashboard_main_guards", _MAIN_PATH)
_mod = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["dashboard_main_guards"] = _mod
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(_mod)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import-env guard
    pytest.skip(f"dashboard-service failed to import: {exc}",
                allow_module_level=True)

_SRC = _MAIN_PATH.read_text()


# ── fakes ────────────────────────────────────────────────────────────────


class _Result:
    def __init__(self, rows=(), first=None, rowcount=0):
        self._rows = list(rows)
        self._first = first
        self.rowcount = rowcount

    def all(self):
        return list(self._rows)

    def first(self):
        return self._first


class _Savepoint:
    def __init__(self, db):
        self._db = db

    async def __aenter__(self):
        self._db.savepoints += 1
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if exc_type is not None:
            self._db.savepoint_rollbacks += 1
        return False


class _FakeDB:
    """Dispatches on statement text; records every execute().

    snaps:        {resource_id: [snapshot ids, newest first]}
    l2:           {(str(resource_id), str(snapshot_id)): bytes} — the
                  resource_protected_bytes rows
    rollup_bytes: {resource_id: bytes} — what the heavy aggregate returns
    """

    def __init__(self, snaps, *, l2=None, rollup_bytes=None,
                 rollup_delay=0.0, rollup_error=None):
        self.snaps = snaps
        self.l2 = dict(l2 or {})
        self.rollup_bytes = dict(rollup_bytes or {})
        self.rollup_delay = rollup_delay
        self.rollup_error = rollup_error
        self.calls = []
        self.commits = 0
        self.savepoints = 0
        self.savepoint_rollbacks = 0
        self.inflight = 0
        self.max_inflight = 0

    @property
    def rollups(self):
        return [c for c in self.calls if "FROM snapshot_items" in c[0]]

    @property
    def upserts(self):
        return [c for c in self.calls if "INSERT INTO resource_protected_bytes" in c[0]]

    @property
    def latest_lookups(self):
        return [c for c in self.calls
                if "ORDER BY snapshots.created_at DESC" in c[0] and "LIMIT" in c[0]]

    def begin_nested(self):
        return _Savepoint(self)

    async def commit(self):
        self.commits += 1

    async def rollback(self):
        pass

    async def execute(self, stmt, params=None):
        sql = str(stmt)
        self.calls.append((sql, params))
        if "FROM snapshot_items" in sql:
            self.inflight += 1
            self.max_inflight = max(self.max_inflight, self.inflight)
            try:
                if self.rollup_delay:
                    await asyncio.sleep(self.rollup_delay)
                if self.rollup_error is not None:
                    raise self.rollup_error
                rid = self._resource_for(params["snaps"])
                return _Result(first=(self.rollup_bytes.get(rid, 0),))
            finally:
                self.inflight -= 1
        if "INSERT INTO resource_protected_bytes" in sql:
            self.l2[(params["rid"], params["snap"])] = params["bytes"]
            return _Result()
        if "FROM resource_protected_bytes" in sql:
            val = self.l2.get((params["rid"], params["snap"]))
            return _Result(first=None if val is None else (val,))
        if sql.strip().upper().startswith("SET LOCAL"):
            return _Result()
        if "snapshots.completed_at" in sql:  # 30-day growth chart
            return _Result(rows=[])
        if "GROUP BY snapshots.resource_id" in sql:  # resources with a snapshot
            return _Result(rows=[(rid,) for rid in self.snaps])
        if "ORDER BY snapshots.created_at DESC" in sql:
            ids = self.snaps.get(self._bound_resource_id(stmt), [])
            if "LIMIT" in sql:
                return _Result(rows=[(s,) for s in ids[:1]],
                               first=(ids[0],) if ids else None)
            return _Result(rows=[(s,) for s in ids])
        raise AssertionError(f"unexpected statement: {sql[:80]}")

    @staticmethod
    def _bound_resource_id(stmt):
        for key, value in stmt.compile().params.items():
            if key.startswith("resource_id"):
                return value
        raise AssertionError("latest-snapshot lookup is not filtered by resource_id")

    def _resource_for(self, snap_ids):
        for rid, ids in self.snaps.items():
            if ids and ids[0] == snap_ids[0]:
                return rid
        raise AssertionError("rollup over an unknown snapshot set")


class _FakeRequest:
    """`is_disconnected()` flips to True after `disconnect_after` polls."""

    def __init__(self, disconnect_after=None):
        self.polls = 0
        self._after = disconnect_after

    async def is_disconnected(self):
        self.polls += 1
        return self._after is not None and self.polls > self._after


class _PgError(Exception):
    def __init__(self, sqlstate):
        super().__init__(sqlstate)
        self.sqlstate = sqlstate


def _dbapi_error(sqlstate):
    return DBAPIError("SELECT ...", {}, _PgError(sqlstate))


def _ids(n):
    """n resources, one eligible snapshot each."""
    return {uuid.uuid4(): [uuid.uuid4()] for _ in range(n)}


def _warm_rows(snaps, size):
    return {(str(rid), str(ids[0])): size for rid, ids in snaps.items()}


async def _call(db, request=None):
    return await _mod.get_backup_size(
        request=request or _FakeRequest(),
        tenantId=None, serviceType=None, tz=None, db=db,
    )


@pytest.fixture(autouse=True)
def _fresh_process_state(monkeypatch):
    """Per-test L1 cache and a semaphore bound to this test's loop.
    Tolerant of the attribute being absent so each test fails on its
    own assertion against a build without the fix."""
    _mod._protected_bytes_cache.clear()
    monkeypatch.setattr(
        _mod, "_heavy_rollup_sem",
        asyncio.Semaphore(getattr(_mod, "_PROTECTED_ROLLUP_CONCURRENCY", 2)),
        raising=False,
    )
    yield
    _mod._protected_bytes_cache.clear()


# ── cache decision: recompute only when the latest snapshot id changed ───


async def test_latest_snapshot_lookup_is_one_index_probe():
    snaps = _ids(1)
    rid = next(iter(snaps))
    db = _FakeDB(snaps, rollup_bytes={rid: 10})
    assert await _mod._protected_bytes_for_resource(db, rid) == 10
    sql, _ = db.calls[0]
    assert "snapshots.resource_id = :" in sql
    assert "ORDER BY snapshots.created_at DESC" in sql
    assert "LIMIT" in sql, "the cache decision must not fetch every sibling"


async def test_recompute_only_when_latest_snapshot_id_changed():
    rid, s1 = uuid.uuid4(), uuid.uuid4()
    db = _FakeDB({rid: [s1]}, rollup_bytes={rid: 123})

    assert await _mod._protected_bytes_for_resource(db, rid) == 123
    assert len(db.rollups) == 1 and len(db.upserts) == 1
    assert db.upserts[0][1] == {"rid": str(rid), "snap": str(s1), "bytes": 123}
    assert db.commits == 1

    # same latest snapshot: L1 hit, no table probe, no rollup
    assert await _mod._protected_bytes_for_resource(db, rid) == 123
    assert len(db.rollups) == 1

    # restart (L1 gone): the durable row answers, still no rollup
    _mod._protected_bytes_cache.clear()
    assert await _mod._protected_bytes_for_resource(db, rid) == 123
    assert len(db.rollups) == 1

    # a newer snapshot completes: exactly one recompute, row re-keyed
    s2 = uuid.uuid4()
    db.snaps[rid] = [s2, s1]
    db.rollup_bytes[rid] = 456
    assert await _mod._protected_bytes_for_resource(db, rid) == 456
    assert len(db.rollups) == 2
    assert db.upserts[-1][1]["snap"] == str(s2)
    assert list(db.rollups[-1][1]["snaps"]) == [s2, s1], (
        "the rollup spans every eligible sibling, newest first"
    )


async def test_durable_row_for_an_older_snapshot_is_a_miss():
    rid, s_old, s_new = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    db = _FakeDB({rid: [s_new, s_old]},
                 l2={(str(rid), str(s_old)): 99}, rollup_bytes={rid: 5})
    assert await _mod._protected_bytes_for_resource(db, rid) == 5
    assert len(db.rollups) == 1
    assert db.l2[(str(rid), str(s_new))] == 5


async def test_no_eligible_snapshot_is_zero_without_heavy_work():
    rid = uuid.uuid4()
    db = _FakeDB({rid: []})
    assert await _mod._protected_bytes_for_resource(db, rid) == 0
    assert db.rollups == [] and db.upserts == []


async def test_recompute_false_reports_a_cold_resource_without_touching_items():
    snaps = _ids(1)
    rid = next(iter(snaps))
    db = _FakeDB(snaps, rollup_bytes={rid: 7})
    assert await _mod._protected_bytes_for_resource(db, rid, recompute=False) is None
    assert db.rollups == []
    db.l2[(str(rid), str(snaps[rid][0]))] = 7
    assert await _mod._protected_bytes_for_resource(db, rid, recompute=False) == 7


# ── time budget → partial total, statement bounded server-side ───────────


async def test_budget_returns_partial_total_and_stops_heavy_rollups(monkeypatch):
    snaps = _ids(3)
    db = _FakeDB(snaps, rollup_bytes={rid: 100 for rid in snaps}, rollup_delay=0.05)
    monkeypatch.setattr(_mod, "_PROTECTED_REQUEST_BUDGET_S", 0.03)
    resp = await _call(db)
    assert resp["partial"] is True
    assert len(db.rollups) == 1, "the first rollup overran the budget; no further heavy work"
    assert resp["totalBytes"] == 100
    assert resp["total"] == _mod.format_bytes(100)
    # the heavy statement ran under a transaction-local timeout, inside a
    # savepoint, floored at 1 s
    set_local = [c[0] for c in db.calls if c[0].startswith("SET LOCAL statement_timeout = ")]
    assert set_local[0] == "SET LOCAL statement_timeout = 1000"
    assert set_local[-1] == "SET LOCAL statement_timeout = DEFAULT"
    assert db.savepoints == 1


async def test_within_budget_is_complete_and_not_flagged(monkeypatch):
    snaps = _ids(3)
    db = _FakeDB(snaps, rollup_bytes={rid: 100 for rid in snaps})
    monkeypatch.setattr(_mod, "_PROTECTED_REQUEST_BUDGET_S", 10.0)
    resp = await _call(db)
    assert resp["partial"] is False
    assert len(db.rollups) == 3 and resp["totalBytes"] == 300
    assert len(db.upserts) == 3


async def test_warm_resources_are_summed_before_any_cold_one_is_rolled(monkeypatch):
    """A budget-cut answer still carries every cached resource."""
    warm, cold = _ids(3), _ids(2)
    snaps = {**cold, **warm}  # cold rows come first in walk order
    db = _FakeDB(snaps, l2=_warm_rows(warm, 10),
                 rollup_bytes={rid: 1000 for rid in cold}, rollup_delay=0.05)
    monkeypatch.setattr(_mod, "_PROTECTED_REQUEST_BUDGET_S", 0.03)
    resp = await _call(db)
    assert resp["partial"] is True
    assert resp["totalBytes"] == 30 + 1000
    assert len(db.rollups) == 1


async def test_statement_cancelled_at_budget_is_skipped_not_raised():
    snaps = _ids(1)
    rid = next(iter(snaps))
    db = _FakeDB(snaps, rollup_error=_dbapi_error("57014"))
    deadline = asyncio.get_running_loop().time() + 5
    assert await _mod._protected_bytes_for_resource(db, rid, deadline=deadline) is None
    assert db.upserts == [] and db.commits == 0
    assert db.savepoint_rollbacks == 1, "the abort rolled back to the savepoint"
    assert _mod._protected_bytes_cache == {}, "a skipped resource must not be cached"


async def test_other_db_errors_still_raise():
    snaps = _ids(1)
    rid = next(iter(snaps))
    db = _FakeDB(snaps, rollup_error=_dbapi_error("42P01"))
    deadline = asyncio.get_running_loop().time() + 5
    with pytest.raises(DBAPIError):
        await _mod._protected_bytes_for_resource(db, rid, deadline=deadline)


async def test_cancel_without_a_budget_still_raises():
    """The prewarm path sets no deadline and no statement_timeout: a
    57014 there is an operator cancel and must surface in its log."""
    snaps = _ids(1)
    rid = next(iter(snaps))
    db = _FakeDB(snaps, rollup_error=_dbapi_error("57014"))
    with pytest.raises(DBAPIError):
        await _mod._protected_bytes_for_resource(db, rid)
    assert not any(c[0].startswith("SET LOCAL") for c in db.calls)


async def test_endpoint_marks_partial_when_a_rollup_is_cancelled():
    snaps = _ids(2)
    db = _FakeDB(snaps, rollup_error=_dbapi_error("57014"))
    resp = await _call(db)
    assert resp["partial"] is True and resp["totalBytes"] == 0
    assert len(db.rollups) == 1


async def test_waiting_for_a_rollup_slot_counts_against_the_budget(monkeypatch):
    monkeypatch.setattr(_mod, "_heavy_rollup_sem", asyncio.Semaphore(1))
    snaps = _ids(2)
    hog_rid, late_rid = list(snaps)
    db = _FakeDB(snaps, rollup_bytes={rid: 1 for rid in snaps}, rollup_delay=0.1)
    hog = asyncio.create_task(_mod._protected_bytes_for_resource(db, hog_rid))
    await asyncio.sleep(0)  # the hog now holds the only slot inside its rollup
    deadline = asyncio.get_running_loop().time() + 0.03
    assert await _mod._protected_bytes_for_resource(db, late_rid, deadline=deadline) is None
    assert len(db.rollups) == 1, "the late resource never reached snapshot_items"
    assert await hog == 1


# ── client disconnect stops the walk ─────────────────────────────────────


async def test_disconnect_stops_the_walk():
    snaps = _ids(5)
    db = _FakeDB(snaps, l2=_warm_rows(snaps, 10))
    req = _FakeRequest(disconnect_after=2)
    resp = await _call(db, req)
    assert resp["partial"] is True
    assert resp["totalBytes"] == 20
    assert len(db.latest_lookups) == 2, "no resource is looked up after the client left"
    assert req.polls == 3


async def test_disconnect_before_the_cold_pass_runs_no_rollup():
    snaps = _ids(3)
    db = _FakeDB(snaps, rollup_bytes={rid: 1 for rid in snaps})
    req = _FakeRequest(disconnect_after=3)  # warm pass completes, then the client is gone
    resp = await _call(db, req)
    assert resp["partial"] is True
    assert db.rollups == []


async def test_connected_client_gets_the_full_total():
    snaps = _ids(5)
    db = _FakeDB(snaps, l2=_warm_rows(snaps, 10))
    req = _FakeRequest()
    resp = await _call(db, req)
    assert resp["partial"] is False and resp["totalBytes"] == 50
    assert req.polls == 5, "the client is checked between resources"


# ── heavy rollups bounded per process ────────────────────────────────────


async def test_heavy_rollups_bounded_to_two_per_process():
    snaps = _ids(5)
    db = _FakeDB(snaps, rollup_bytes={rid: 1 for rid in snaps}, rollup_delay=0.02)
    results = await asyncio.gather(
        *[_mod._protected_bytes_for_resource(db, rid) for rid in snaps]
    )
    assert results == [1] * 5
    assert db.max_inflight == 2


async def test_concurrency_probe_measures_something(monkeypatch):
    """Negative control for the test above."""
    monkeypatch.setattr(_mod, "_heavy_rollup_sem", asyncio.Semaphore(5))
    snaps = _ids(5)
    db = _FakeDB(snaps, rollup_bytes={rid: 1 for rid in snaps}, rollup_delay=0.02)
    await asyncio.gather(*[_mod._protected_bytes_for_resource(db, rid) for rid in snaps])
    assert db.max_inflight == 5


# ── prewarm loop: one per process, kill switch, wiring ───────────────────


async def test_prewarm_starts_once_per_process(monkeypatch):
    monkeypatch.setattr(_mod, "_prewarm_task", None)
    monkeypatch.setattr(_mod, "_PREWARM_ENABLED", True)
    t1 = _mod._start_protected_prewarm()
    try:
        assert _mod._start_protected_prewarm() is t1
        assert not t1.done()
    finally:
        t1.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await t1
    t2 = _mod._start_protected_prewarm()
    try:
        assert t2 is not t1, "a finished loop is replaced, not treated as running"
    finally:
        t2.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await t2


def test_prewarm_kill_switch(monkeypatch):
    monkeypatch.setattr(_mod, "_prewarm_task", None)
    monkeypatch.setattr(_mod, "_PREWARM_ENABLED", False)
    assert _mod._start_protected_prewarm() is None


def test_lifespan_wires_the_prewarm_loop_through_the_start_guard():
    lifespan = _SRC[_SRC.find("async def lifespan"):_SRC.find("app = FastAPI")]
    assert "_start_protected_prewarm()" in lifespan
    assert ".cancel()" in lifespan


def test_prewarm_pass_refreshes_stale_rows_and_prunes_orphans():
    assert "LEFT JOIN resource_protected_bytes" in _mod._STALE_ROLLUPS_SQL
    assert "p.latest_snapshot_id <> l.latest_snapshot_id" in _mod._STALE_ROLLUPS_SQL
    assert _mod._PRUNE_ROLLUPS_SQL.strip().startswith("DELETE FROM resource_protected_bytes")
    assert "PROTECTED_PREWARM_INTERVAL_S" in _SRC


# ── source pins ──────────────────────────────────────────────────────────


def test_endpoint_checks_the_client_and_reports_partial():
    src = inspect.getsource(_mod.get_backup_size)
    assert "request.is_disconnected()" in src
    assert '"partial": partial' in src
    assert "_PROTECTED_REQUEST_BUDGET_S" in src


def test_read_through_table_is_probed_before_the_heavy_rollup():
    helper = inspect.getsource(_mod._protected_bytes_for_resource)
    assert "FROM resource_protected_bytes" in helper
    assert "_PROTECTED_ROLLUP_SQL" not in helper, "the heavy path lives behind the table probe"
    recompute = inspect.getsource(_mod._recompute_protected_bytes)
    # 2026-10-15: the heavy statement itself moved into _run_rollup so it
    # can execute on the read replica (_rollup_on_best_session); the
    # recompute path still owns the slot, the budget and the upsert.
    assert "_rollup_on_best_session" in recompute and "_PROTECTED_UPSERT_SQL" in recompute
    rollup = inspect.getsource(_mod._run_rollup)
    assert "_PROTECTED_ROLLUP_SQL" in rollup
    assert "SET LOCAL statement_timeout" in rollup
    assert "ON CONFLICT (resource_id) DO UPDATE" in _mod._PROTECTED_UPSERT_SQL
    assert _mod._PROTECTED_ROLLUP_CONCURRENCY == 2
