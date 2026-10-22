"""Protected-bytes rollups must not run on the primary when a read
replica is configured, and the prewarm loop must run in one place.

2026-10-15 RCA: the dashboard-service `_protected_prewarm_loop` was the
single largest sequential reader of snapshot_items on the primary
(Parallel Seq Scan per resource, 37.8 TB read through parallel workers in
32 h), ran in BOTH uvicorn workers (duplicate passes), and that read IO is
what stretched every carry-forward slot hold from seconds to minutes.

Contract pinned here:
  * `_recompute_protected_bytes` runs the heavy rollup SELECT on the replica
    session (`DB_RO_HOST`) when the replica already sees the latest eligible
    snapshot, falls back to the primary when it does not (replication lag),
    and always writes the resource_protected_bytes upsert on the primary.
  * `_protected_prewarm_pass` takes a transaction-scoped advisory lock on
    the primary and skips the pass when another worker/pod holds it.
"""
from __future__ import annotations

import contextlib
import importlib.util
import pathlib

import pytest

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
_MAIN_PATH = _REPO_ROOT / "services" / "dashboard-service" / "main.py"
_SRC = _MAIN_PATH.read_text()


@pytest.fixture(scope="module")
def ds():
    spec = importlib.util.spec_from_file_location("dashboard_prewarm_replica", _MAIN_PATH)
    mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    assert spec and spec.loader
    try:
        spec.loader.exec_module(mod)  # type: ignore[union-attr]
    except Exception as exc:  # pragma: no cover - import guard
        pytest.skip(f"dashboard-service module failed to import: {exc}")
    return mod


class _Res:
    def __init__(self, rows=None, scalar=None, rowcount=0):
        self._rows = rows or []
        self._scalar = scalar
        self.rowcount = rowcount

    def all(self):
        return list(self._rows)

    def first(self):
        return self._rows[0] if self._rows else None

    def scalar(self):
        return self._scalar


class _FakeDB:
    """Routes statements by shape so one fake serves primary and replica."""

    def __init__(self, *, visible=True, lock_ok=True, stale=(), latest="snap1", rollup=1234):
        self.visible = visible
        self.lock_ok = lock_ok
        self.stale = list(stale)
        self.latest = latest
        self.rollup = rollup
        self.sql = []
        self.commits = 0

    async def execute(self, stmt, params=None):
        s = str(stmt)
        self.sql.append(s)
        if "pg_try_advisory_xact_lock" in s:
            return _Res(scalar=self.lock_ok)
        if "SELECT 1 FROM snapshots" in s:
            return _Res(rows=[(1,)] if self.visible else [])
        if "SUM(size)" in s:
            return _Res(rows=[(self.rollup,)])
        if "INSERT INTO resource_protected_bytes" in s:
            return _Res(rowcount=1)
        if "DELETE FROM resource_protected_bytes" in s:
            return _Res(rowcount=0)
        if "latest_snapshot_id <> l.latest_snapshot_id" in s:
            return _Res(rows=[(r,) for r in self.stale])
        if "FROM snapshots" in s:  # _eligible_snapshots_stmt
            return _Res(rows=[(self.latest,)])
        if "SET LOCAL" in s:
            return _Res()
        raise AssertionError(f"unexpected statement: {s[:120]}")

    async def commit(self):
        self.commits += 1

    @contextlib.asynccontextmanager
    async def begin(self):
        yield self

    @contextlib.asynccontextmanager
    async def begin_nested(self):
        yield self


def _factory(db):
    @contextlib.asynccontextmanager
    async def _cm():
        yield db
    return _cm


def _saw(db, needle):
    return any(needle in s for s in db.sql)


# --------------------------------------------------------------- rollup routing

async def test_rollup_reads_on_replica_when_snapshot_visible(ds, monkeypatch):
    primary, replica = _FakeDB(), _FakeDB(visible=True)
    monkeypatch.setattr(ds, "_ro_session_factory", _factory(replica))
    out = await ds._recompute_protected_bytes(primary, "rid", "snap1", None)
    assert out == 1234
    assert _saw(replica, "SELECT 1 FROM snapshots")
    assert _saw(replica, "SUM(size)")
    assert not _saw(primary, "SUM(size)")
    assert _saw(primary, "INSERT INTO resource_protected_bytes")
    assert primary.commits == 1


async def test_rollup_falls_back_to_primary_when_replica_lags(ds, monkeypatch):
    primary, replica = _FakeDB(), _FakeDB(visible=False)
    monkeypatch.setattr(ds, "_ro_session_factory", _factory(replica))
    out = await ds._recompute_protected_bytes(primary, "rid", "snap1", None)
    assert out == 1234
    assert _saw(replica, "SELECT 1 FROM snapshots")
    assert not _saw(replica, "SUM(size)")
    assert _saw(primary, "SUM(size)")
    assert _saw(primary, "INSERT INTO resource_protected_bytes")


async def test_rollup_uses_primary_when_no_replica_configured(ds, monkeypatch):
    primary = _FakeDB()
    monkeypatch.setattr(ds, "_ro_session_factory", None)
    out = await ds._recompute_protected_bytes(primary, "rid", "snap1", None)
    assert out == 1234
    assert _saw(primary, "SUM(size)")
    assert _saw(primary, "INSERT INTO resource_protected_bytes")


# --------------------------------------------------------------- prewarm pass

async def test_prewarm_pass_skips_when_lock_held_elsewhere(ds, monkeypatch):
    db = _FakeDB(lock_ok=False, stale=["r1"])
    monkeypatch.setattr(ds, "async_session_factory", _factory(db))
    called = []

    async def fake_pbfr(session, rid, **kw):
        called.append(rid)
        return 1

    monkeypatch.setattr(ds, "_protected_bytes_for_resource", fake_pbfr)
    status, n = await ds._protected_prewarm_pass()
    assert status == "skipped" and n == 0
    assert not _saw(db, "latest_snapshot_id <> l.latest_snapshot_id")
    assert called == []


async def test_prewarm_pass_refreshes_stale_rollups_under_lock(ds, monkeypatch):
    db = _FakeDB(lock_ok=True, stale=["r1", "r2"])
    monkeypatch.setattr(ds, "async_session_factory", _factory(db))
    called = []

    async def fake_pbfr(session, rid, **kw):
        called.append(rid)
        return 1

    monkeypatch.setattr(ds, "_protected_bytes_for_resource", fake_pbfr)
    status, n = await ds._protected_prewarm_pass()
    assert status == "refreshed" and n == 2
    assert _saw(db, "pg_try_advisory_xact_lock")
    assert _saw(db, "latest_snapshot_id <> l.latest_snapshot_id")
    assert _saw(db, "DELETE FROM resource_protected_bytes")
    assert called == ["r1", "r2"]


# --------------------------------------------------------------- source pins

def test_prewarm_loop_delegates_to_locked_pass():
    loop = _SRC[_SRC.find("async def _protected_prewarm_loop"):]
    loop = loop[:loop.find("\n\n\n")]
    assert "_protected_prewarm_pass()" in loop


def test_replica_is_opt_in_via_db_ro_host():
    assert 'os.getenv("DB_RO_HOST"' in _SRC
    assert "_ro_session_factory" in _SRC
