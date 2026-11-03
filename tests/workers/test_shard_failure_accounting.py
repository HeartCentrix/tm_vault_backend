"""A partition shard must report its OWN failure, never the shared parent's.

Bug (adversarial review, 2026-07-31): the new drain-honesty gates made
`EnumerationFailed` reachable in NORMAL operation (a mail/chat shard whose
whole folder/chat set failed with 0 items now raises it). `_backup_one`
settled that by flipping the SHARED parent snapshot to FAILED — but
`create_snapshot`'s (job_id, resource_id) dedup only matches
IN_PROGRESS / COMPLETED / PARTIAL. A sibling shard entering `_backup_one`
during that window therefore found no prior, INSERTed a SECOND snapshot
row with no `partitioned` flag, and its inline finalize settled it
COMPLETED holding a fraction of the mailbox: a partial run reported as
success, plus a split snapshot identity.

Correct shape: per-shard failure accounting (the snapshot_partitions row
this shard owns) which `_finalize_partitioned_snapshot` already aggregates
(`completed = 0 → FAILED`, `failed > 0 → PARTIAL`). The parent stays
IN_PROGRESS so sibling shards keep binding to it.

These tests drive the real settlement helpers, plus AST pins on the shard
handlers' call sites (the wiring is where the bug lived).
"""
from __future__ import annotations

import asyncio
import ast
import importlib.util
import json
import os
import pathlib
import sys
import time
import uuid

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "workers" / "backup-worker" / "main.py"
_spec = importlib.util.spec_from_file_location("bw_shard_fail", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_shard_fail"] = _bw
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(_bw)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(
        f"backup-worker module failed to import: {exc}",
        allow_module_level=True,
    )

SnapshotStatus = _bw.SnapshotStatus


class _FakeSnap:
    def __init__(self, status=None, extra_data=None):
        self.id = uuid.uuid4()
        self.status = status or SnapshotStatus.IN_PROGRESS
        self.extra_data = dict(extra_data or {})
        self.completed_at = None


class _FakeSession:
    def __init__(self, snap):
        self._snap = snap
        self.statements = []
        self.commits = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, _model, _pk):
        return self._snap

    async def execute(self, stmt, params=None):
        self.statements.append((str(stmt), params or {}))
        # Emulate the atomic JSONB merge so callers can assert on state.
        if params and "patch" in params:
            self._snap.extra_data = {
                **(self._snap.extra_data or {}),
                **json.loads(params["patch"]),
            }
        return None

    async def commit(self):
        self.commits += 1


def _worker():
    w = _bw.BackupWorker.__new__(_bw.BackupWorker)
    w.worker_id = "test-worker"
    w._worker_region = "default"
    return w


def _record(*, snap, partition_id=None):
    """Drive the real _record_enumeration_failure against a fake session."""
    w = _worker()
    session = _FakeSession(snap)
    marked = []

    async def _fake_mark(pid, *, status, files_uploaded=0, bytes_uploaded=0,
                         failure_state=None):
        marked.append((str(pid), status, failure_state))

    w._mark_partition_terminal = _fake_mark  # type: ignore[assignment]

    orig = _bw.async_session_factory
    _bw.async_session_factory = lambda: session  # type: ignore[assignment]
    try:
        outcome = asyncio.run(
            w._record_enumeration_failure(
                snapshot_id=snap.id,
                reason="enumeration failed: USER_MAIL: 3 of 3 folder drain(s) failed",
                partition_id=partition_id,
                resource_label="[USER_MAIL] amit.mishra@x.com",
            )
        )
    finally:
        _bw.async_session_factory = orig  # type: ignore[assignment]
    return outcome, w, session, marked


# ─── whole-resource run: unchanged behaviour ──────────────────────────────

def test_whole_resource_enumeration_failure_still_fails_the_snapshot():
    snap = _FakeSnap()
    outcome, _w, _s, marked = _record(snap=snap, partition_id=None)
    assert outcome == "snapshot_failed"
    assert snap.status == SnapshotStatus.FAILED
    assert snap.extra_data.get("failure_reason")
    assert snap.extra_data.get(_bw.SNAPSHOT_DEGRADED_REASON_KEY)
    assert snap.completed_at is not None
    assert marked == [], "no partition row exists for a whole-resource run"


def test_whole_resource_failure_does_not_touch_an_already_terminal_snapshot():
    snap = _FakeSnap(status=SnapshotStatus.PARTIAL)
    _record(snap=snap, partition_id=None)
    assert snap.status == SnapshotStatus.PARTIAL


# ─── shard run: parent lifecycle must be untouched ────────────────────────

def test_shard_failure_leaves_the_parent_snapshot_bindable_by_siblings():
    """The whole bug: a FAILED parent is invisible to create_snapshot's
    dedup, so the next sibling shard mints a second snapshot row."""
    snap = _FakeSnap()
    pid = uuid.uuid4()
    outcome, _w, _s, _m = _record(snap=snap, partition_id=str(pid))
    assert outcome == "shard_failed"
    assert snap.status == SnapshotStatus.IN_PROGRESS
    assert snap.status in _bw.SNAPSHOT_DEDUP_STATUSES, (
        "parent snapshot must stay inside create_snapshot's dedup set or a "
        "sibling shard will create a second snapshot for this resource"
    )
    assert snap.completed_at is None


def test_shard_failure_is_recorded_against_this_shard_only():
    snap = _FakeSnap()
    pid = uuid.uuid4()
    _outcome, w, _s, _m = _record(snap=snap, partition_id=str(pid))
    ledger = w._shard_failure_ledger()
    assert str(pid) in ledger
    assert "EnumerationFailed" in json.dumps(ledger[str(pid)])
    assert len(ledger) == 1, "a shard may only account for its own failure"


def test_shard_failure_stamps_the_parent_reason_without_changing_status():
    """Operators still need the reason, and the partition finalize reads
    degraded_reason — but neither may cost the parent its IN_PROGRESS."""
    snap = _FakeSnap(extra_data={"partitioned": True})
    _outcome, _w, session, _m = _record(snap=snap, partition_id=str(uuid.uuid4()))
    assert snap.status == SnapshotStatus.IN_PROGRESS
    assert snap.extra_data.get("partitioned") is True
    assert snap.extra_data.get(_bw.SNAPSHOT_DEGRADED_REASON_KEY)
    assert snap.extra_data.get("failure_reason")
    sql = " ".join(s for s, _p in session.statements)
    assert "UPDATE snapshots" in sql
    assert "||" in sql, (
        "sibling shards write this concurrently — the merge must be one "
        "atomic statement, not a read-modify-write"
    )
    assert "status" not in sql.lower().split("where")[0], (
        "the shard's failure stamp must never write the parent's status"
    )


# ─── the shard settles its own partition row + audit ──────────────────────

def _settle(ledger_entry=None):
    w = _worker()
    pid = uuid.uuid4()
    if ledger_entry is not None:
        w._shard_failure_ledger()[str(pid)] = ledger_entry
    marked = []
    audits = []

    async def _fake_mark(p, *, status, files_uploaded=0, bytes_uploaded=0,
                         failure_state=None):
        marked.append((status, files_uploaded, bytes_uploaded, failure_state))

    async def _fake_audit(action, outcome, **kw):
        audits.append((action, outcome, kw.get("details") or {}))

    w._mark_partition_terminal = _fake_mark  # type: ignore[assignment]
    w._emit_partition_audit = _fake_audit  # type: ignore[assignment]

    status = asyncio.run(
        w._settle_partition_outcome(
            pid,
            tenant_id=str(uuid.uuid4()),
            resource_id=str(uuid.uuid4()),
            snapshot_id=uuid.uuid4(),
            job_id=uuid.uuid4(),
            partition_type="MAIL_FOLDERS",
            files_uploaded=41,
            bytes_uploaded=9000,
            started_at=time.monotonic(),
        )
    )
    return status, w, pid, marked, audits


def test_clean_shard_settles_completed():
    status, w, pid, marked, audits = _settle(ledger_entry=None)
    assert status == "COMPLETED"
    assert marked == [("COMPLETED", 41, 9000, None)]
    assert audits[0][0] == "PARTITION_COMPLETED"
    assert audits[0][1] == "SUCCESS"


def test_shard_that_recorded_an_enumeration_failure_settles_failed():
    entry = {"reason": "EnumerationFailed", "message": "USER_MAIL: all folders failed"}
    status, w, pid, marked, audits = _settle(ledger_entry=entry)
    assert status == "FAILED", (
        "a shard whose enumeration refused an empty-COMPLETED finalize must "
        "mark ITS OWN partition FAILED so the finalizer can aggregate"
    )
    assert marked[0][0] == "FAILED"
    assert marked[0][3] == entry
    assert audits[0][0] == "PARTITION_FAILED"
    assert audits[0][1] == "FAILURE"
    assert str(pid) not in w._shard_failure_ledger(), "ledger entry consumed"


def test_settle_does_not_leak_one_shards_failure_onto_another():
    w = _worker()
    mine = uuid.uuid4()
    other = uuid.uuid4()
    w._shard_failure_ledger()[str(other)] = {"reason": "EnumerationFailed"}
    marked = []

    async def _fake_mark(p, *, status, files_uploaded=0, bytes_uploaded=0,
                         failure_state=None):
        marked.append(status)

    async def _fake_audit(action, outcome, **kw):
        return None

    w._mark_partition_terminal = _fake_mark  # type: ignore[assignment]
    w._emit_partition_audit = _fake_audit  # type: ignore[assignment]
    status = asyncio.run(
        w._settle_partition_outcome(
            mine,
            tenant_id=None, resource_id=None,
            snapshot_id=uuid.uuid4(), job_id=uuid.uuid4(),
            partition_type="CHATS",
            files_uploaded=7, bytes_uploaded=70,
            started_at=time.monotonic(),
        )
    )
    assert status == "COMPLETED"
    assert marked == ["COMPLETED"]
    assert str(other) in w._shard_failure_ledger()


# ─── call-site wiring pins ────────────────────────────────────────────────

def _fn(name: str):
    tree = ast.parse(_MAIN_PATH.read_text())
    for node in ast.walk(tree):
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)) and node.name == name:
            return node
    return None


def _calls(node) -> set:
    out = set()
    for n in ast.walk(node):
        if isinstance(n, ast.Call):
            f = n.func
            if isinstance(f, ast.Attribute):
                out.add(f.attr)
            elif isinstance(f, ast.Name):
                out.add(f.id)
    return out


def test_backup_one_routes_enumeration_failures_through_the_recorder():
    src = _MAIN_PATH.read_text()
    start = src.find("except EnumerationFailed as enum_exc:")
    assert start != -1
    arm = src[start:start + 1200]
    assert "_record_enumeration_failure(" in arm
    assert 'partition_id=message.get("partitionId")' in arm, (
        "the recorder cannot distinguish shard from whole-resource runs "
        "without the partition id"
    )
    # No direct status flip left in the arm.
    assert "SnapshotStatus.FAILED" not in arm


def test_chats_and_mail_shard_handlers_settle_their_own_partition():
    for handler in (
        "_process_chats_partition_message",
        "_process_mail_partition_message",
    ):
        node = _fn(handler)
        assert node is not None, handler
        assert "_settle_partition_outcome" in _calls(node), (
            f"{handler} must settle through the helper so an enumeration "
            f"failure cannot be audited as PARTITION_COMPLETED/SUCCESS"
        )
        body = ast.get_source_segment(_MAIN_PATH.read_text(), node) or ""
        assert 'status="COMPLETED"' not in body, (
            f"{handler} still hard-marks COMPLETED somewhere — that write "
            f"bypasses the shard's own failure accounting"
        )


def test_tier1_mailbox_shard_failure_does_not_fail_the_shared_parent():
    """The Tier-1 (MAILBOX/SHARED/ROOM) path has the same shape: its
    failure arm called fail_snapshot on a snapshot shared with sibling mail
    shards. Same duplicate-snapshot window, same fix — and the ledger is
    what keeps the shard's own row from settling COMPLETED."""
    src = _MAIN_PATH.read_text()
    idx = src.find("Mailbox backup failed for ")
    assert idx != -1
    arm = src[idx: idx + 2200]
    assert "_record_enumeration_failure(" in arm, (
        "a mail shard must record its own failure, not flip the shared "
        "parent out of create_snapshot's dedup set"
    )
    assert "partition_id=" in arm
    # the whole-mailbox (non-shard) path must still fail its snapshot
    assert "fail_snapshot(" in arm


def test_create_snapshot_dedup_uses_the_shared_status_constant():
    node = _fn("create_snapshot")
    assert node is not None
    body = ast.get_source_segment(_MAIN_PATH.read_text(), node) or ""
    assert "SNAPSHOT_DEDUP_STATUSES" in body, (
        "the dedup status set must be a named constant so the shard-failure "
        "invariant test cannot drift away from the real query"
    )
    assert set(_bw.SNAPSHOT_DEDUP_STATUSES) == {
        SnapshotStatus.IN_PROGRESS,
        SnapshotStatus.COMPLETED,
        SnapshotStatus.PARTIAL,
    }
