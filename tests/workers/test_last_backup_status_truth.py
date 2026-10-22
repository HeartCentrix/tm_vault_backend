"""resources.last_backup_at / last_backup_status must be truthful.

Defect (2026-10-14): ``update_resource_backup_info`` wrote
``last_backup_at=now(), last_backup_status="COMPLETED"`` unconditionally and
was called from every finalize site — including the per-shard inline
finalize of a partitioned run (``deferred_partitioned``) and the per-type
handlers' early stamps before ``complete_snapshot``. Live: 427/427 SLA'd
resources read COMPLETED while 43 of their runs in the last 7 days were
PARTIAL or FAILED (none recorded), and a partitioned CHATS run wedged
IN_PROGRESS for 39h read "COMPLETED minutes ago" to the scheduler's
``check_sla_violations`` and ``backup_priority_key``.

Contract pinned here:
  * a terminal COMPLETED / PARTIAL / FAILED writes the true status;
  * FAILED writes status + job id only — last_backup_at keeps meaning
    "when a restore point was last produced" (no last_successful_backup_at
    column exists; every freshness consumer reads last_backup_at as that);
  * a non-terminal call touches storage_bytes only;
  * every call site hands the COMMITTED snapshot status to the stamp, and
    the fail paths stamp FAILED (never over an operator cancel).
"""
from __future__ import annotations

import asyncio
import importlib.util
import os
import pathlib
import sys
import uuid
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy.sql.dml import Update
from sqlalchemy.sql.elements import TextClause

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "workers" / "backup-worker" / "main.py"
_spec = importlib.util.spec_from_file_location("bw_last_backup_truth", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_last_backup_truth"] = _bw
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(_bw)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(
        f"backup-worker module failed to import: {exc}",
        allow_module_level=True,
    )

_SRC = _MAIN_PATH.read_text()
SnapshotStatus = _bw.SnapshotStatus
ResourceStatus = _bw.ResourceStatus


# ─── fakes ────────────────────────────────────────────────────────────────

class _Result:
    def __init__(self, value=None, rowcount=0):
        self._value = value
        self.rowcount = rowcount

    def scalar(self):
        return self._value


class _Session:
    """Records every UPDATE's bound values, answers the subtree-sum text()
    queries with a fixed number, and serves `get` per model name."""

    def __init__(self, *, subtree_sum=4096, resource=None, snapshot=None,
                 rowcount=0):
        self.subtree_sum = subtree_sum
        self._resource = resource
        self._snapshot = snapshot
        self.rowcount = rowcount
        self.updates = []   # (table, {column: value, ...})
        self.texts = []     # (sql, params)
        self.commits = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def execute(self, stmt, params=None):
        if isinstance(stmt, TextClause):
            self.texts.append((str(stmt), dict(params or {})))
            return _Result(self.subtree_sum, rowcount=self.rowcount)
        assert isinstance(stmt, Update), type(stmt)
        self.updates.append((stmt.table.name, dict(stmt.compile().params)))
        return _Result(None)

    async def commit(self):
        self.commits += 1

    async def get(self, model, _pk):
        name = getattr(model, "__name__", "")
        if name == "Resource":
            return self._resource
        if name == "Snapshot":
            return self._snapshot
        return None


def _resource(parent=None, status=ResourceStatus.DISCOVERED):
    return SimpleNamespace(
        id=uuid.uuid4(), status=status, storage_bytes=0,
        parent_resource_id=parent, type=_bw.ResourceType.USER_MAIL,
        display_name="amit.mishra@x.com",
    )


def _snapshot(resource_id=None, status=SnapshotStatus.IN_PROGRESS,
              extra_data=None):
    return SimpleNamespace(
        id=uuid.uuid4(), job_id=uuid.uuid4(),
        resource_id=resource_id or uuid.uuid4(), status=status,
        extra_data=dict(extra_data or {}),
        started_at=datetime.utcnow() - timedelta(minutes=2),
        completed_at=None, duration_secs=None, item_count=0,
    )


def _worker():
    w = _bw.BackupWorker.__new__(_bw.BackupWorker)
    w.worker_id = "test-worker"
    return w


def _spy_worker():
    """Worker whose update_resource_backup_info only records its arguments."""
    w = _worker()
    calls = []

    async def _spy(session, resource, job_id, snapshot_id, result=None, *,
                   status=None, terminal=None):
        calls.append({
            "resource": resource, "job_id": job_id,
            "snapshot_id": snapshot_id, "result": result,
            "status": status, "terminal": terminal,
        })

    w.update_resource_backup_info = _spy  # type: ignore[assignment]
    return w, calls


def _stamp(session, resource, *, job_id=None, result=None, **kw):
    return asyncio.run(_worker().update_resource_backup_info(
        session, resource,
        uuid.uuid4() if job_id is None else job_id,
        uuid.uuid4(),
        {"item_count": 1, "bytes_added": 10} if result is None else result,
        **kw,
    ))


def _resource_updates(session):
    ups = [p for t, p in session.updates if t == "resources"]
    assert ups, "no UPDATE resources issued"
    return ups


def _with_session_factory(session, coro):
    orig = _bw.async_session_factory
    _bw.async_session_factory = lambda: session  # type: ignore[assignment]
    try:
        return asyncio.run(coro)
    finally:
        _bw.async_session_factory = orig  # type: ignore[assignment]


# ─── resource_stamp_for_snapshot ──────────────────────────────────────────

@pytest.mark.parametrize("status, extra, expected", [
    (SnapshotStatus.COMPLETED, {}, ("COMPLETED", True)),
    ("COMPLETED", {}, ("COMPLETED", True)),
    (SnapshotStatus.PARTIAL, {"failure_reason": "2 of 40 folders"}, ("PARTIAL", True)),
    (SnapshotStatus.FAILED, {"error": "boom"}, ("FAILED", True)),
    # operator cancel coerced to FAILED: job-service already wrote CANCELLED
    (SnapshotStatus.FAILED, {"cancelled_at": "2026-10-14T22:00:00Z"}, (None, False)),
    (SnapshotStatus.FAILED, {"finalize_cancelled": True}, (None, False)),
    # the 39h-wedged partitioned parent
    (SnapshotStatus.IN_PROGRESS, {"partitioned": True}, (None, False)),
    (SnapshotStatus.PENDING_DELETION, {}, (None, False)),
    (None, None, (None, False)),
])
def test_resource_stamp_for_snapshot(status, extra, expected):
    assert _bw.resource_stamp_for_snapshot(status, extra) == expected


# ─── update_resource_backup_info ──────────────────────────────────────────

def test_terminal_completed_writes_status_time_and_promotes_discovered():
    res = _resource()
    s = _Session(subtree_sum=4096, resource=res)
    jid = uuid.uuid4()
    before = datetime.utcnow() - timedelta(seconds=1)
    _stamp(s, res, job_id=jid, status="COMPLETED")
    (p,) = _resource_updates(s)
    assert p["last_backup_status"] == "COMPLETED"
    assert p["last_backup_job_id"] == jid
    assert p["last_backup_at"] >= before
    assert p["storage_bytes"] == 4096
    assert p["status"] == ResourceStatus.ACTIVE
    assert s.commits == 1


def test_terminal_partial_is_recorded_as_partial_and_advances_freshness():
    """A PARTIAL run produced a restore point: last_backup_at advances (the
    SLA check still sees a backup happened) and the status says what it
    was. It used to be written as COMPLETED."""
    res = _resource(status=ResourceStatus.ACTIVE)
    s = _Session(resource=res)
    before = datetime.utcnow() - timedelta(seconds=1)
    _stamp(s, res, status=SnapshotStatus.PARTIAL)   # enum accepted too
    (p,) = _resource_updates(s)
    assert p["last_backup_status"] == "PARTIAL"
    assert p["last_backup_at"] >= before
    assert p["status"] == ResourceStatus.ACTIVE


def test_terminal_failed_writes_status_and_job_only():
    res = _resource()
    s = _Session(resource=res)
    jid = uuid.uuid4()
    _stamp(s, res, job_id=jid, status="FAILED", result=None)
    (p,) = _resource_updates(s)
    assert p["last_backup_status"] == "FAILED"
    assert p["last_backup_job_id"] == jid
    assert "last_backup_at" not in p, (
        "last_backup_at is the last restore point; a failure must not "
        "reset the SLA freshness clock"
    )
    assert "status" not in p, (
        "lifecycle status untouched — the outer handler may have just "
        "marked the resource INACCESSIBLE and the in-memory copy is stale"
    )
    assert "storage_bytes" not in p
    assert s.texts == [], "no subtree-sum query on the FAILED path"
    assert s.commits == 1


def test_non_terminal_touches_storage_only():
    """deferred_partitioned and the handlers' pre-complete_snapshot stamps."""
    res = _resource()
    s = _Session(subtree_sum=123, resource=res)
    _stamp(s, res, terminal=False)
    (p,) = _resource_updates(s)
    assert p["storage_bytes"] == 123
    for col in ("last_backup_at", "last_backup_status",
                "last_backup_job_id", "status"):
        assert col not in p, col


def test_no_status_means_non_terminal():
    res = _resource()
    s = _Session(resource=res)
    _stamp(s, res)
    (p,) = _resource_updates(s)
    assert "last_backup_status" not in p and "last_backup_at" not in p


def test_terminal_with_a_non_terminal_status_never_writes_a_status():
    res = _resource()
    s = _Session(resource=res)
    _stamp(s, res, status="IN_PROGRESS", terminal=True)
    (p,) = _resource_updates(s)
    assert "last_backup_status" not in p and "last_backup_at" not in p
    assert "storage_bytes" in p


def test_parent_rollup_still_bubbles_on_a_non_terminal_stamp():
    parent = uuid.uuid4()
    res = _resource(parent=parent)
    s = _Session(subtree_sum=7, resource=res)
    _stamp(s, res, terminal=False)
    assert [t for t, _p in s.updates] == ["resources", "resources"]
    assert s.updates[1][1]["storage_bytes"] == 7
    assert s.updates[1][1]["id_1"] == parent
    assert "last_backup_status" not in s.updates[1][1]


# ─── _stamp_resource_from_snapshot_row ────────────────────────────────────

def test_stamp_from_row_uses_the_committed_status_not_the_callers_copy():
    res = _resource()
    row = SimpleNamespace(status=SnapshotStatus.PARTIAL,
                          extra_data={"failure_reason": "x"})
    w, calls = _spy_worker()
    asyncio.run(w._stamp_resource_from_snapshot_row(
        _Session(resource=res, snapshot=row), res, uuid.uuid4(),
        uuid.uuid4(), {"item_count": 1},
    ))
    assert calls[0]["status"] == "PARTIAL" and calls[0]["terminal"] is True


def test_stamp_from_row_gone_or_in_progress_is_storage_only():
    res = _resource()
    w, calls = _spy_worker()
    asyncio.run(w._stamp_resource_from_snapshot_row(
        _Session(resource=res, snapshot=None), res, None, uuid.uuid4(),
    ))
    row = SimpleNamespace(status=SnapshotStatus.IN_PROGRESS,
                          extra_data={"partitioned": True})
    asyncio.run(w._stamp_resource_from_snapshot_row(
        _Session(resource=res, snapshot=row), res, None, uuid.uuid4(),
    ))
    assert [(c["status"], c["terminal"]) for c in calls] == [
        (None, False), (None, False),
    ]


# ─── fail_snapshot ────────────────────────────────────────────────────────

def _fail(error, *, landed=True, job_cancelled=False):
    res = _resource()
    snap = _snapshot(resource_id=res.id)
    s = _Session(resource=res)
    w, calls = _spy_worker()

    async def _fake_terminal(session, snapshot, allowed_from):
        return landed

    w._write_snapshot_terminal = _fake_terminal  # type: ignore[assignment]
    orig = _bw._is_job_cancelled

    async def _fake_cancelled(_job_id):
        return job_cancelled

    _bw._is_job_cancelled = _fake_cancelled  # type: ignore[assignment]
    try:
        asyncio.run(w.fail_snapshot(s, snap, error))
    finally:
        _bw._is_job_cancelled = orig  # type: ignore[assignment]
    return snap, res, calls


def test_fail_snapshot_stamps_the_resource_failed():
    snap, res, calls = _fail(RuntimeError("Graph 503 storm"))
    assert snap.status == SnapshotStatus.FAILED
    assert len(calls) == 1
    c = calls[0]
    assert c["resource"] is res and c["status"] == "FAILED"
    assert c["job_id"] == snap.job_id and c["snapshot_id"] == snap.id


def test_fail_snapshot_leaves_a_cancelled_run_to_job_service():
    _s, _r, calls = _fail(_bw.JobCancelledMidFlight("cancelled mid-flight"))
    assert calls == []
    _s, _r, calls = _fail(RuntimeError("x"), job_cancelled=True)
    assert calls == []


def test_fail_snapshot_does_not_stamp_when_the_flip_did_not_land():
    """COMPLETED/PARTIAL rows are never demoted — a run that finished must
    not read FAILED on the resource either."""
    _s, _r, calls = _fail(RuntimeError("x"), landed=False)
    assert calls == []


# ─── _record_enumeration_failure (whole-resource run) ─────────────────────

def test_enumeration_failure_stamps_the_resource_with_the_corrected_status():
    res = _resource()
    snap = _snapshot(resource_id=res.id)
    s = _Session(resource=res, snapshot=snap)
    w, calls = _spy_worker()
    out = _with_session_factory(s, w._record_enumeration_failure(
        snapshot_id=snap.id,
        reason="enumeration failed: USER_MAIL: Graph 401 mid-walk",
        partition_id=None,
        resource_label="[USER_MAIL] amit.mishra@x.com",
    ))
    assert out == "snapshot_failed"
    assert snap.status == SnapshotStatus.FAILED
    assert [(c["status"], c["job_id"]) for c in calls] == [
        ("FAILED", snap.job_id),
    ]


def test_enumeration_failure_on_a_shard_never_touches_the_resource():
    """The parent snapshot is shared with sibling shards; the partition
    finalizer owns its terminal state and the resource stamp."""
    res = _resource()
    snap = _snapshot(resource_id=res.id)
    s = _Session(resource=res, snapshot=snap)
    w, calls = _spy_worker()
    ledger: dict = {}
    w._shard_failure_ledger = lambda: ledger  # type: ignore[assignment]

    async def _fake_patch(_sid, _patch):
        return True

    w._patch_snapshot_extra_data = _fake_patch  # type: ignore[assignment]
    out = _with_session_factory(s, w._record_enumeration_failure(
        snapshot_id=snap.id, reason="enumeration failed: shard",
        partition_id=uuid.uuid4(), resource_label="[USER_MAIL] shard",
    ))
    assert out == "shard_failed"
    assert snap.status == SnapshotStatus.IN_PROGRESS
    assert calls == []


# ─── bulk parent flip: resources the job failed without a worker-side flip ─

def test_bulk_failed_sweep_sql_and_rowcount():
    s = _Session(rowcount=2)
    jid = uuid.uuid4()
    n = _with_session_factory(
        s, _worker()._stamp_unsettled_bulk_job_resources_failed(jid),
    )
    assert n == 2 and s.commits == 1
    ((sql, params),) = s.texts
    assert params == {"jid": jid}
    assert "last_backup_status = 'FAILED'" in sql
    assert "last_backup_job_id = j.id" in sql
    assert "last_backup_at" not in sql, (
        "a failure never moves the freshness clock"
    )
    assert "ANY(j.batch_resource_ids)" in sql
    # a sibling job's COMPLETED/PARTIAL/IN_PROGRESS row since dispatch means
    # the resource is covered, not failed
    assert "NOT EXISTS" in sql
    assert "s.created_at >= j.created_at" in sql
    assert "s.status <> 'FAILED'" in sql


# ─── call-site pins ───────────────────────────────────────────────────────

def _method_src(name: str) -> str:
    start = _SRC.index(f"    async def {name}(")
    end = _SRC.index("\n    async def ", start + 10)
    return _SRC[start:end]


def test_single_backup_finalize_stamps_from_the_committed_row():
    body = _method_src("_process_single_backup")
    assert "_stamp_resource_from_snapshot_row(" in body
    assert (
        "update_resource_backup_info(s, resource, job_id, snapshot.id, result)"
        not in body
    ), "the in-memory snapshot may say COMPLETED where the row says PARTIAL"


def test_inline_finalize_hands_the_settled_status_to_the_stamp():
    body = _method_src("_backup_user_content_parallel")
    i = body.index('if _settled != "skipped_terminal":')
    window = body[i:i + 700]
    assert "resource_stamp_for_snapshot(" in window
    assert "snap.status, snap.extra_data" in window
    assert "status=_rs_status, terminal=_rs_terminal" in window


def test_partition_finalizer_stamps_with_the_parent_status_once():
    body = _method_src("_finalize_partitioned_snapshot")
    assert "status=_pf_stamp_status, terminal=_pf_stamp_terminal" in body
    assert "if snap is not None else (None, False)" in body
    assert body.count("update_resource_backup_info(") == 1


def test_bulk_parent_flip_sweeps_unsettled_resources():
    body = _method_src("_finalize_bulk_parent_if_complete")
    i = body.index("[FANOUT/FINALIZE] job={job_id} ")
    assert "_stamp_unsettled_bulk_job_resources_failed(job_id)" in body[i:i + 1500]


def test_fail_snapshot_gates_the_resource_stamp():
    body = _method_src("fail_snapshot")
    assert "_landed = await self._write_snapshot_terminal(" in body
    assert 'status="FAILED"' in body
    assert "_is_job_cancelled(snapshot.job_id)" in body
    assert '"cancelled_at" not in existing' in body


@pytest.mark.parametrize("name", [
    "_backup_files_parallel",
    "_backup_entra_resource",
    "_backup_teams_resource",
    "_backup_teams_chat_resource",
    "backup_teams_chat_export",
])
def test_pre_terminal_handler_stamps_are_storage_only(name):
    """These run while the snapshot is still IN_PROGRESS; the outer finalize
    (or the partition finalizer) owns last_backup_* from the settled row."""
    body = _method_src(name)
    n_calls = body.count("update_resource_backup_info(")
    assert n_calls >= 1, name
    assert body.count("terminal=False)") == n_calls, name


def test_legacy_generic_and_mailbox_paths_stamp_from_the_row():
    for name in ("_backup_generic_parallel", "_backup_mailboxes_parallel"):
        body = _method_src(name)
        assert "_stamp_resource_from_snapshot_row(" in body, name
