"""Durable copy waits must survive legacy claim, reconciliation and cleanup paths."""
import asyncio
import ast
import pathlib
import uuid
from types import SimpleNamespace

import pytest


ROOT = pathlib.Path(__file__).resolve().parents[2]


class Rows:
    def __init__(self, rows=()):
        self.rows = list(rows)
        self.rowcount = len(self.rows)

    def first(self):
        return self.rows[0] if self.rows else None

    def all(self):
        return self.rows

    fetchall = all


class RecordingSession:
    def __init__(self):
        self.sql = []

    async def execute(self, stmt, params=None):
        self.sql.append(str(stmt))
        return Rows()

    async def commit(self):
        pass


def test_every_reconciler_transition_has_atomic_pending_copy_guard(monkeypatch):
    import shared.reconciler as reconciler

    def guard(**scope):
        return "COPY_BARRIER_" + next(iter(scope))

    monkeypatch.setattr(reconciler, "pending_copy_sql", guard, raising=False)
    session = RecordingSession()
    asyncio.run(reconciler.sweep_orphans(session))
    updates = [sql for sql in session.sql if "UPDATE " in sql]
    assert len(updates) == 8  # + the bounded DLQ replay (RECONCILER_DLQ_REPLAY_MAX)
    assert all("COPY_BARRIER_" in sql for sql in updates)


@pytest.mark.parametrize("table,scope", [
    ("jobs", "job_expr"), ("snapshots", "snapshot_expr"),
    ("snapshot_partitions", "partition_expr"),
])
def test_lease_claim_guards_copy_wait_in_atomic_update(monkeypatch, table, scope):
    import shared.lease as lease

    monkeypatch.setattr(lease, "pending_copy_sql", lambda **kw: "COPY_BARRIER_" + next(iter(kw)), raising=False)
    session = RecordingSession()
    asyncio.run(lease.claim(session, table=table, work_id=uuid.uuid4(),
                            owner_id=str(uuid.uuid4()), in_progress_status="IN_PROGRESS"))
    assert "AND COPY_BARRIER_" + scope in session.sql[0]


def test_retention_delete_requalifies_source_pins_under_row_lock(monkeypatch):
    import shared.retention_cleanup as retention

    monkeypatch.setattr(retention, "copy_source_pinned_sql", lambda expr: "COPY_SOURCE_BARRIER", raising=False)
    session = RecordingSession()
    assert asyncio.run(retention._delete_snapshots(session, {uuid.uuid4()})) == 0
    assert "AND COPY_SOURCE_BARRIER" in session.sql[0]
    assert "FOR UPDATE" in session.sql[0]


def test_blob_guard_retains_frozen_manifest_reference(monkeypatch):
    import shared.backup_cleanup as cleanup

    path = "content-addressed/blob"
    monkeypatch.setattr(cleanup, "copy_blob_pinned_sql", lambda expr: "COPY_BLOB_BARRIER", raising=False)

    class Session(RecordingSession):
        async def execute(self, stmt, params=None):
            self.sql.append(str(stmt))
            return Rows([SimpleNamespace(blob_path=path)] if "COPY_BLOB_BARRIER" in str(stmt) else [])

    session = Session()
    plan = asyncio.run(cleanup.select_deletable_blob_paths(
        session, doomed_snapshot_ids=[uuid.uuid4()], protected_roots=set(),
        candidates=[cleanup.BlobCandidate(blob_path=path, snapshot_id=None,
                                          tenant_id=uuid.uuid4(), content_checksum="checksum")],
    ))
    assert path not in plan.deletable
    assert plan.retained[path] == "pending_copy"


def _function_source(path, name):
    source = (ROOT / path).read_text()
    tree = ast.parse(source)
    return ast.get_source_segment(source, next(n for n in ast.walk(tree)
                                              if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == name))


@pytest.mark.parametrize("name", ["_sweep_stale_snapshots", "_sweep_cancelled_snapshots",
                                  "_reap_stuck_running_jobs", "_reap_missing_snapshot_jobs"])
def test_scheduler_legacy_sweeps_honor_copy_wait(name):
    source = _function_source("services/backup-scheduler/main.py", name)
    assert "pending_copy_sql(" in source or "copy_source_pinned_sql(" in source


def test_job_revert_defers_copy_pins_before_storage_access():
    source = _function_source("services/job-service/main.py", "_revert_snapshot_storage")
    assert source.index("copy_source_pinned_sql(") < source.index("res_row =")


def test_batch_completion_barrier_counts_pending_copy(monkeypatch):
    import shared.batch_rollup as rollup

    monkeypatch.setattr(rollup, "pending_copy_sql", lambda **kw: "COPY_BARRIER", raising=False)
    sql = str(rollup.build_batch_rollup_query(
        tenant_id=None, start_date=None, end_date=None, operation=None, size=10, offset=0,
    ))
    pending_count = sql[sql.index("AS snap_failed"):sql.index("AS snap_pending")]
    assert "NOT (COPY_BARRIER)" in pending_count
