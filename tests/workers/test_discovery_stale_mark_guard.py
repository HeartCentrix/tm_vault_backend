"""Stale-mark safety guards (2026-07 archive-pilot wedge regression tests).

A discovery run that could not actually SEE the inventory (concurrent purge,
throttled/unauthorized API, empty result) must NEVER stale-mark: flipping
every ACTIVE/DISCOVERED resource INACCESSIBLE silently stops all scheduled
backups for the tenant. Pins:

  * _finalize_discovery: 0 staged rows + "successful" scopes ⇒ NO stale-mark
    UPDATE is issued, the run still COMPLETEs, and a breadcrumb lands in
    error_message.
  * _finalize_discovery: staged rows present ⇒ stale-mark still runs (the
    healthy path is preserved).
  * _purge_orphan_staging: the DELETE must spare rows belonging to a recent
    RUNNING run — purging a live concurrent run's staging is what blinded
    finalize in the first place.

Uses asyncio.run inside sync tests (pytest-asyncio is not installed here).
"""
from __future__ import annotations

import asyncio
import importlib.util
import os
import pathlib
import sys
import uuid
from datetime import datetime, timezone
from types import SimpleNamespace

# ── Set required env vars before any shared.config import ─────────────────
os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "test_secret_for_unit_tests_only_x")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")
os.environ.setdefault("AUDIT_SERVICE_URL", "http://localhost:8000")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_REPO_ROOT / "workers" / "discovery-worker"))

_WORKER_PATH = _REPO_ROOT / "workers" / "discovery-worker" / "main.py"
_spec = importlib.util.spec_from_file_location("discovery_worker_main_guard", _WORKER_PATH)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)


class _FakeResult:
    def __init__(self, scalar=0):
        self._s = scalar

    def scalar(self):
        return self._s


class _FakeDB:
    """Answers the staging COUNT with `staged_count`; every other statement
    returns scalar 0. Auto-protect's richer queries get an AttributeError
    (no .scalars/.all on _FakeResult) which _finalize_discovery swallows by
    design (auto-protect is non-fatal)."""

    def __init__(self, staged_count: int):
        self.staged_count = staged_count
        self.calls = []

    async def execute(self, stmt, params=None):
        text = getattr(stmt, "text", str(stmt))
        self.calls.append((text, params))
        if "SELECT COUNT(*) FROM resource_discovery_staging" in text:
            return _FakeResult(self.staged_count)
        return _FakeResult(0)

    async def commit(self):
        self.calls.append(("COMMIT", None))


class _LivenessFakeDB:
    """Answers the purge's candidate-run SELECT with `rows` (run_id, status,
    started_at) and records every statement."""

    def __init__(self, rows):
        self._rows = rows
        self.calls = []

    async def execute(self, stmt, params=None):
        text = getattr(stmt, "text", str(stmt))
        self.calls.append((text, params))

        rows = self._rows

        class _R:
            def all(self):
                return list(rows)

            def scalar(self):
                return 0

        return _R()

    async def commit(self):
        self.calls.append(("COMMIT", None))


def _fake_run_and_tenant():
    run = SimpleNamespace(
        id=uuid.uuid4(),
        status="RUNNING",
        fetched_count=0,
        staged_count=0,
        inserted_count=0,
        updated_count=0,
        unchanged_count=0,
        stale_marked_count=0,
        finished_at=None,
        error_message=None,
    )
    tenant = SimpleNamespace(status=None, last_discovery_at=None)
    return run, tenant


def _stale_mark_calls(db: _FakeDB):
    return [c for c in db.calls if "INACCESSIBLE" in c[0] and "UPDATE resources" in c[0]]


def test_zero_staged_rows_skips_stale_mark_but_completes():
    db = _FakeDB(staged_count=0)
    run, tenant = _fake_run_and_tenant()
    tenant_id = uuid.uuid4()

    staged, inserted, updated, stale = asyncio.run(
        _mod._finalize_discovery(
            db, tenant, run, tenant_id,
            {_mod.ResourceType.ENTRA_USER, _mod.ResourceType.SHAREPOINT_SITE},
            fetched_count=0,
        )
    )

    assert stale == 0
    assert not _stale_mark_calls(db), "no stale-mark UPDATE may run on an empty staging table"
    # The run still completes — but leaves a breadcrumb explaining the skip.
    assert run.status == "COMPLETED"
    assert run.error_message and "stale-mark skipped" in run.error_message


def test_staged_rows_present_still_stale_marks():
    db = _FakeDB(staged_count=42)
    run, tenant = _fake_run_and_tenant()
    tenant_id = uuid.uuid4()

    asyncio.run(
        _mod._finalize_discovery(
            db, tenant, run, tenant_id,
            {_mod.ResourceType.ENTRA_USER},
            fetched_count=42,
        )
    )

    assert _stale_mark_calls(db), "healthy path must still stale-mark vanished resources"
    assert run.status == "COMPLETED"
    assert run.error_message is None


def test_purge_orphan_staging_spares_recent_running_runs():
    """The purge must resolve liveness BEFORE deleting anything.

    The deployed implementation resolves it in Python — a candidate SELECT
    joined to discovery_runs, then a DELETE scoped to the run ids that
    _is_purgeable_staging_run cleared — so a live concurrent run's staging
    survives. Row-level behaviour (live / FAILED / abandoned / no-run-row) is
    pinned in tests/workers/test_purge_orphan_staging_liveness.py; this test
    only pins that the guard exists and the two windows stay ordered.
    """
    tid = uuid.uuid4()
    keep = uuid.uuid4()
    live_run = uuid.uuid4()
    db = _LivenessFakeDB([(live_run, "RUNNING", datetime.now(timezone.utc))])

    asyncio.run(_mod._purge_orphan_staging(db, tid, keep_run_id=keep))

    selects = [c for c in db.calls if c[0].strip().upper().startswith("SELECT")]
    assert selects, "expected a liveness lookup before purging"
    sql, params = selects[0]
    assert "discovery_runs" in sql, "liveness must be resolved against discovery_runs"
    assert params["tenant_id"] == tid and params["keep_run_id"] == keep

    dels = [c for c in db.calls if "DELETE FROM resource_discovery_staging" in c[0]]
    assert not dels, (
        "the only other run is RUNNING and recent — purging its staging is what "
        "blinded finalize in the first place (2026-07 archive-pilot wedge)"
    )

    # The purge waits LONGER than the single-flight window: a slow-but-live run
    # must keep its rows even after it stops blocking new runs.
    assert _mod._ORPHAN_STAGING_ABANDON_HOURS > _mod._RUNNING_RUN_STALE_HOURS
