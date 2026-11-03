"""_purge_orphan_staging must never delete a LIVE run's staging rows.

Periodic discovery, onboarding discovery and manual re-discovery all land on
the same discovery.m365 queue, so two runs for one tenant overlap routinely.
The purge ran at run start with no liveness check and no lock held (the
advisory lock is only taken at finalize), so run B deleted run A's staged
rows mid-flight; A then finalized against a partial staging set and
stale-marked live resources INACCESSIBLE en masse.

Only terminal runs (COMPLETED / FAILED), runs whose bookkeeping row is gone,
and RUNNING runs abandoned past the horizon (hard crash never reaches
_mark_discovery_failed) may be purged.
"""
from __future__ import annotations

import asyncio
import importlib.util
import os
import pathlib
import sys
import uuid
from datetime import datetime, timedelta, timezone

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "test_secret_for_unit_tests_only_x")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")
os.environ.setdefault("AUDIT_SERVICE_URL", "http://localhost:8000")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_REPO_ROOT / "workers" / "discovery-worker"))

_spec = importlib.util.spec_from_file_location(
    "discovery_worker_main",
    _REPO_ROOT / "workers" / "discovery-worker" / "main.py",
)


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return list(self._rows)

    def scalar(self):
        return 0


class _FakeDB:
    """Answers the candidate-run SELECT with `rows`, records every statement."""

    def __init__(self, rows):
        self._rows = rows
        self.calls = []

    async def execute(self, stmt, params=None):
        sql = getattr(stmt, "text", str(stmt))
        self.calls.append((sql, params))
        if sql.strip().upper().startswith("SELECT"):
            return _FakeResult(self._rows)
        return _FakeResult([])

    async def commit(self):
        self.calls.append(("COMMIT", None))


def _load():
    mod = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(mod)
    return mod


def _deleted_run_ids(db):
    deletes = [
        (sql, params) for sql, params in db.calls
        if "DELETE FROM resource_discovery_staging" in sql
    ]
    if not deletes:
        return set()
    assert len(deletes) == 1, f"expected at most one purge DELETE, got {len(deletes)}"
    params = deletes[0][1] or {}
    assert "run_ids" in params, (
        "purge DELETE is not scoped to a resolved list of dead run ids — it "
        f"deletes by tenant alone (params={params}), which wipes the staging "
        "rows of any concurrent live run"
    )
    return set(params["run_ids"])


def test_live_concurrent_run_staging_rows_are_kept():
    mod = _load()
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    current, live, failed, completed = (uuid.uuid4() for _ in range(4))
    db = _FakeDB([
        (live, "RUNNING", now - timedelta(minutes=4)),
        (failed, "FAILED", now - timedelta(hours=3)),
        (completed, "COMPLETED", now - timedelta(hours=9)),
    ])

    asyncio.run(mod._purge_orphan_staging(db, uuid.uuid4(), keep_run_id=current))

    purged = _deleted_run_ids(db)
    assert live not in purged, (
        "purged the staging rows of a run that is still in progress — the "
        "survivor finalizes against a partial staging set and mass-marks "
        "real resources INACCESSIBLE"
    )
    assert purged == {failed, completed}
    assert current not in purged


def test_nothing_deleted_when_every_other_run_is_live():
    mod = _load()
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    live_a, live_b = uuid.uuid4(), uuid.uuid4()
    db = _FakeDB([
        (live_a, "RUNNING", now - timedelta(minutes=1)),
        (live_b, "RUNNING", now - timedelta(hours=2)),
    ])

    asyncio.run(mod._purge_orphan_staging(db, uuid.uuid4(), keep_run_id=uuid.uuid4()))

    assert _deleted_run_ids(db) == set()
    assert not [c for c in db.calls if "DELETE FROM resource_discovery_staging" in c[0]], (
        "no dead runs ⇒ no DELETE should be issued at all"
    )


def test_abandoned_running_run_past_horizon_is_purged():
    """A hard crash (OOM / pod kill) leaves status=RUNNING forever; those
    staging rows must still be reclaimable."""
    mod = _load()
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    abandoned = uuid.uuid4()
    horizon = mod._ORPHAN_STAGING_ABANDON_HOURS
    db = _FakeDB([(abandoned, "RUNNING", now - timedelta(hours=horizon + 1))])

    asyncio.run(mod._purge_orphan_staging(db, uuid.uuid4(), keep_run_id=uuid.uuid4()))

    assert _deleted_run_ids(db) == {abandoned}


def test_staging_rows_without_a_run_row_are_purged():
    mod = _load()
    orphan = uuid.uuid4()
    db = _FakeDB([(orphan, None, None)])

    asyncio.run(mod._purge_orphan_staging(db, uuid.uuid4(), keep_run_id=uuid.uuid4()))

    assert _deleted_run_ids(db) == {orphan}


def test_candidate_query_excludes_the_current_run_and_scopes_to_tenant():
    mod = _load()
    tenant_id, current = uuid.uuid4(), uuid.uuid4()
    db = _FakeDB([])

    asyncio.run(mod._purge_orphan_staging(db, tenant_id, keep_run_id=current))

    selects = [(s, p) for s, p in db.calls if s.strip().upper().startswith("SELECT")]
    assert selects, "expected a liveness lookup before purging"
    sql, params = selects[0]
    assert params["tenant_id"] == tenant_id
    assert params["keep_run_id"] == current


def test_is_purgeable_staging_run_matrix():
    mod = _load()
    now = datetime.now(timezone.utc)
    horizon = mod._ORPHAN_STAGING_ABANDON_HOURS

    assert mod._is_purgeable_staging_run("COMPLETED", now, now) is True
    assert mod._is_purgeable_staging_run("FAILED", now, now) is True
    assert mod._is_purgeable_staging_run(None, None, now) is True
    assert mod._is_purgeable_staging_run("RUNNING", now, now) is False
    assert mod._is_purgeable_staging_run("RUNNING", None, now) is False
    assert mod._is_purgeable_staging_run(
        "RUNNING", now - timedelta(hours=horizon - 1), now
    ) is False
    assert mod._is_purgeable_staging_run(
        "RUNNING", now - timedelta(hours=horizon + 1), now
    ) is True
    # Naive timestamps come back from a `timestamp without time zone` column.
    assert mod._is_purgeable_staging_run(
        "RUNNING", (now - timedelta(hours=horizon + 1)).replace(tzinfo=None), now
    ) is True
