"""Call-site wiring for the inline finalize: (A) honesty + (F) cursor owner.

The reviewer's test-depth finding: (A), (D), (F) and (G) were covered only
through their extracted pure functions, so nothing exercised the code that
CALLS them — which is exactly how the half-applied (D) fix passed a green
suite. This file drives the real inline-finalize decision
(`_settle_inline_snapshot_finalize`) against fakes, so the three guards and
the promotion are exercised together:

  * (A) a handler that recorded degraded_reason settles PARTIAL with a
    failure_reason, and a PARTIAL run must not promote its delta cursor;
  * (F) may_finalize_snapshot_status stops a zombie re-flipping an already
    terminal snapshot, and a pending cursor stashed by ANOTHER snapshot is
    left in place rather than promoted;
  * the cancel coercion's audit markers survive the degraded write-back
    (they were dropped by writing back a pre-coercion copy of extra_data).
"""
from __future__ import annotations

import ast
import asyncio
import importlib.util
import os
import pathlib
import sys
import uuid
from datetime import datetime, timedelta

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "workers" / "backup-worker" / "main.py"
_spec = importlib.util.spec_from_file_location("bw_inline_finalize", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_inline_finalize"] = _bw
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(_bw)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(
        f"backup-worker module failed to import: {exc}",
        allow_module_level=True,
    )

SnapshotStatus = _bw.SnapshotStatus
PENDING = _bw.ONEDRIVE_DELTA_LINK_PENDING_KEY
ACTIVE = _bw.ONEDRIVE_DELTA_LINK_KEY
OWNER = _bw.pending_cursor_owner_key(PENDING)

NEW_LINK = "https://graph/delta?token=RUN_2_PENDING"
OLD_LINK = "https://graph/delta?token=RUN_1_ACTIVE"


class _Snap:
    def __init__(self, *, status=None, extra_data=None, job_id=None):
        self.id = uuid.uuid4()
        self.job_id = job_id or uuid.uuid4()
        self.status = status or SnapshotStatus.IN_PROGRESS
        self.extra_data = dict(extra_data or {})
        self.started_at = datetime.utcnow() - timedelta(minutes=3)
        self.completed_at = None
        self.duration_secs = None


class _Res:
    def __init__(self, extra_data=None, rtype=None):
        self.id = uuid.uuid4()
        self.type = rtype or _bw.ResourceType.USER_ONEDRIVE
        self.display_name = "amit.mishra@x.com"
        self.extra_data = dict(extra_data or {})


class _Session:
    def __init__(self, resource):
        self._resource = resource
        self.commits = 0

    async def get(self, model, _pk):
        if getattr(model, "__name__", "") == "Resource":
            return self._resource
        return None

    async def commit(self):
        self.commits += 1


def _worker():
    w = _bw.BackupWorker.__new__(_bw.BackupWorker)
    w.worker_id = "test-worker"
    return w


def _settle(snap, resource, *, job_cancelled=False):
    orig = _bw._is_job_cancelled

    async def _fake_cancelled(_job_id):
        return job_cancelled

    _bw._is_job_cancelled = _fake_cancelled  # type: ignore[assignment]
    try:
        return asyncio.run(
            _worker()._settle_inline_snapshot_finalize(
                _Session(resource), snap, resource,
            )
        )
    finally:
        _bw._is_job_cancelled = orig  # type: ignore[assignment]


# ─── clean run ────────────────────────────────────────────────────────────

def test_clean_run_completes_and_promotes_its_own_pending_cursor():
    snap = _Snap()
    res = _Res({PENDING: NEW_LINK, OWNER: None})
    res.extra_data[OWNER] = str(snap.id)
    outcome = _settle(snap, res)
    assert outcome == SnapshotStatus.COMPLETED.value
    assert snap.status == SnapshotStatus.COMPLETED
    assert snap.completed_at is not None
    assert snap.duration_secs and snap.duration_secs > 0
    assert res.extra_data.get(ACTIVE) == NEW_LINK
    assert PENDING not in res.extra_data


def test_pending_cursor_owned_by_another_snapshot_is_left_alone():
    """(F) the pending slot is resource-level; a zombie / concurrent run
    must not promote someone else's un-uploaded cursor."""
    snap = _Snap()
    res = _Res({
        PENDING: NEW_LINK,
        OWNER: str(uuid.uuid4()),   # a different snapshot stashed it
        ACTIVE: OLD_LINK,
    })
    _settle(snap, res)
    assert snap.status == SnapshotStatus.COMPLETED
    assert res.extra_data.get(ACTIVE) == OLD_LINK, "cursor must not advance"
    assert res.extra_data.get(PENDING) == NEW_LINK, (
        "the real owner must still be able to finalize"
    )


# ─── (A) degraded run ─────────────────────────────────────────────────────

def test_degraded_run_settles_partial_and_never_promotes():
    snap = _Snap(extra_data={
        _bw.SNAPSHOT_DEGRADED_REASON_KEY: "2 of 40 folder drain(s) failed",
    })
    res = _Res({PENDING: NEW_LINK, ACTIVE: OLD_LINK})
    res.extra_data[OWNER] = str(snap.id)
    outcome = _settle(snap, res)
    assert outcome == SnapshotStatus.PARTIAL.value
    assert snap.status == SnapshotStatus.PARTIAL
    assert snap.extra_data.get("failure_reason") == (
        "2 of 40 folder drain(s) failed"
    )
    assert res.extra_data.get(ACTIVE) == OLD_LINK, (
        "a run that knows content is missing must leave the cursor where "
        "the last good run left it"
    )
    assert res.extra_data.get(PENDING) == NEW_LINK


def test_suppressed_only_run_still_completes_and_promotes():
    """Permanently-unreadable chats are recorded, not degraded — this is
    the (3) fix seen from the finalize side."""
    snap = _Snap(extra_data={
        _bw.SNAPSHOT_SUPPRESSED_REASON_KEY: "2 of 7 chat(s) skipped",
    })
    res = _Res({PENDING: NEW_LINK})
    res.extra_data[OWNER] = str(snap.id)
    outcome = _settle(snap, res)
    assert outcome == SnapshotStatus.COMPLETED.value
    assert res.extra_data.get(ACTIVE) == NEW_LINK
    assert snap.extra_data.get("failure_reason") is None


# ─── (F) guards ───────────────────────────────────────────────────────────

def test_already_terminal_snapshot_is_not_re_flipped_or_promoted():
    snap = _Snap(status=SnapshotStatus.FAILED)
    res = _Res({PENDING: NEW_LINK, ACTIVE: OLD_LINK})
    res.extra_data[OWNER] = str(snap.id)
    outcome = _settle(snap, res)
    assert outcome == "skipped_terminal"
    assert snap.status == SnapshotStatus.FAILED
    assert snap.completed_at is None
    assert res.extra_data.get(ACTIVE) == OLD_LINK
    assert res.extra_data.get(PENDING) == NEW_LINK


def test_partitioned_snapshot_defers_to_the_partition_finalizer():
    snap = _Snap(extra_data={"partitioned": True})
    res = _Res({PENDING: NEW_LINK})
    res.extra_data[OWNER] = str(snap.id)
    outcome = _settle(snap, res)
    assert outcome == "deferred_partitioned"
    assert snap.status == SnapshotStatus.IN_PROGRESS
    assert ACTIVE not in res.extra_data
    assert res.extra_data.get(PENDING) == NEW_LINK


# ─── cancel coercion ──────────────────────────────────────────────────────

def test_cancelled_run_fails_and_does_not_promote():
    snap = _Snap()
    res = _Res({PENDING: NEW_LINK})
    res.extra_data[OWNER] = str(snap.id)
    outcome = _settle(snap, res, job_cancelled=True)
    assert outcome == SnapshotStatus.FAILED.value
    assert snap.extra_data.get("finalize_cancelled") is True
    assert ACTIVE not in res.extra_data


def test_cancelled_and_degraded_keeps_the_cancel_audit_markers():
    """The degraded write-back used to copy a pre-coercion snapshot of
    extra_data, dropping finalize_cancelled / _at / original_proposed."""
    snap = _Snap(extra_data={
        _bw.SNAPSHOT_DEGRADED_REASON_KEY: "1 of 9 chat drain(s) failed",
    })
    res = _Res()
    _settle(snap, res, job_cancelled=True)
    assert snap.status == SnapshotStatus.FAILED
    assert snap.extra_data.get("failure_reason") == (
        "1 of 9 chat drain(s) failed"
    )
    assert snap.extra_data.get("finalize_cancelled") is True
    assert snap.extra_data.get("finalize_cancelled_at")
    assert snap.extra_data.get("original_proposed_status") == "PARTIAL"


# ─── call-site pin ────────────────────────────────────────────────────────

def test_backup_one_delegates_the_inline_finalize():
    src = _MAIN_PATH.read_text()
    tree = ast.parse(src)
    target = None
    for node in ast.walk(tree):
        if (isinstance(node, ast.AsyncFunctionDef)
                and node.name == "_backup_user_content_parallel"):
            target = node
            break
    assert target is not None
    calls = {
        n.func.attr for n in ast.walk(target)
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
    }
    assert "_settle_inline_snapshot_finalize" in calls
    # the decision must not be duplicated back inline
    seg = ast.get_source_segment(src, target) or ""
    assert "may_finalize_snapshot_status(" not in seg
    assert "promote_pending_cursors_for_type(" not in seg
