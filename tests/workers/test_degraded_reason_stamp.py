"""The degraded_reason DB write — the (A) call site the pure tests missed.

`evaluate_drain_honesty` deciding PARTIAL is worthless if the reason never
reaches the row both finalize sites re-read. The write itself was a
read-modify-write on snapshots.extra_data performed by every shard of a
partitioned user, so two shards stamping concurrently dropped one of the
two signals ("content is missing" quietly becoming COMPLETED).

Coverage only — the behaviour under test landed in the shard-accounting
commit; these tests pin the writer and its call sites so it cannot silently
regress to an RMW or to a key that settles the snapshot wrongly.
"""
from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import pathlib
import sys
import uuid

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "workers" / "backup-worker" / "main.py"
_spec = importlib.util.spec_from_file_location("bw_degraded_stamp", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_degraded_stamp"] = _bw
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(_bw)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(
        f"backup-worker module failed to import: {exc}",
        allow_module_level=True,
    )


class _Snap:
    def __init__(self, extra_data=None):
        self.id = uuid.uuid4()
        self.status = _bw.SnapshotStatus.IN_PROGRESS
        self.extra_data = dict(extra_data or {})


class _Session:
    def __init__(self, snap, *, raise_on_execute=False):
        self._snap = snap
        self.raise_on_execute = raise_on_execute
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
        if self.raise_on_execute:
            raise RuntimeError("column extra_data is of type json")
        if params and "patch" in params:
            self._snap.extra_data = {
                **(self._snap.extra_data or {}),
                **json.loads(params["patch"]),
            }

    async def commit(self):
        self.commits += 1


def _worker():
    w = _bw.BackupWorker.__new__(_bw.BackupWorker)
    w.worker_id = "test-worker"
    return w


def _stamp(snap, reason, *, break_atomic=False):
    session = _Session(snap, raise_on_execute=break_atomic)
    orig = _bw.async_session_factory
    _bw.async_session_factory = lambda: session  # type: ignore[assignment]
    try:
        ok = asyncio.run(_worker()._stamp_snapshot_degraded(snap.id, reason))
    finally:
        _bw.async_session_factory = orig  # type: ignore[assignment]
    return ok, session


def test_stamp_writes_the_degraded_key_atomically():
    snap = _Snap({"partitioned": True})
    ok, session = _stamp(snap, "2 of 40 folder drain(s) failed")
    assert ok is True
    assert snap.extra_data[_bw.SNAPSHOT_DEGRADED_REASON_KEY] == (
        "2 of 40 folder drain(s) failed"
    )
    assert snap.extra_data["partitioned"] is True, "siblings' keys survive"
    sql, _params = session.statements[0]
    assert "UPDATE snapshots" in sql
    assert "||" in sql, "must be a single merging statement, not an RMW"
    assert session.commits == 1


def test_stamp_falls_back_to_rmw_rather_than_losing_the_signal():
    """A backup product must not silently drop "content is missing"."""
    snap = _Snap({"partitioned": True})
    ok, session = _stamp(snap, "1 of 9 chat drain(s) failed", break_atomic=True)
    assert ok is True
    assert snap.extra_data[_bw.SNAPSHOT_DEGRADED_REASON_KEY] == (
        "1 of 9 chat drain(s) failed"
    )
    assert snap.extra_data["partitioned"] is True


def test_stamp_truncates_a_runaway_reason():
    snap = _Snap()
    _ok, session = _stamp(snap, "x" * 900)
    assert len(snap.extra_data[_bw.SNAPSHOT_DEGRADED_REASON_KEY]) == 300


def test_stamped_reason_settles_the_finalize_partial():
    """End-to-end of the (A) chain: gate → stamp → finalize decision."""
    status, reason = _bw.evaluate_drain_honesty(
        planned=40, drained_ok=38, failed=2, items_persisted=9000,
        unit="folder",
    )
    assert status is _bw.SnapshotStatus.PARTIAL
    snap = _Snap()
    _stamp(snap, reason)
    settled, settled_reason = _bw.terminal_status_for_extra_data(
        snap.extra_data,
    )
    assert settled is _bw.SnapshotStatus.PARTIAL
    assert settled_reason == reason


# ─── call sites ───────────────────────────────────────────────────────────

_SRC = _MAIN_PATH.read_text()


def test_both_honesty_gates_use_the_atomic_stamp():
    for anchor in (
        "_chat_honesty_status is SnapshotStatus.PARTIAL",
        "_mail_status is SnapshotStatus.PARTIAL",
    ):
        i = _SRC.find(anchor)
        assert i != -1, anchor
        blk = _SRC[i: i + 1400]
        assert "_stamp_snapshot_degraded(" in blk, anchor
        assert "_dg_snap.extra_data = {" not in blk, (
            f"{anchor} still read-modify-writes extra_data"
        )


def test_mail_drained_ok_requires_walk_and_persist():
    """`_mail_folders_drained_ok` feeds BOTH the fingerprint gate and the
    cursor gate, so its membership rule is load-bearing."""
    i = _SRC.find("_mail_folders_drained_ok.add(fid)")
    assert i != -1
    blk = _SRC[i - 700: i]
    assert "_folder_walk_ok" in blk
    assert "_folder_persisted_ok" in blk
    assert "not _mail_streaming_enabled" in blk
    j = _SRC.find("_mail_ok_token_keys = {")
    assert j != -1
    assert "_mail_folders_drained_ok" in _SRC[j: j + 200], (
        "the cursor gate must be derived from the drained-AND-persisted set"
    )
