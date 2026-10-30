"""A COUNTER-driven PARTIAL must record WHY, not just the status.

Bug (2026-08-11): `complete_snapshot` derives PARTIAL from
``snapshot.delta_tokens_json["files_failed"]`` — the counter every
file/item handler stamps (OneDrive Tier-1, SharePoint, OneNote, Planner,
USER_ARCHIVE). It wrote the status and nothing else, so the operator got a
PARTIAL snapshot with no explanation anywhere on the row:

  * USER_ARCHIVE "Archive — Jason Rue" — PARTIAL on 2026-08-10 18:11,
    2026-08-11 02:11 and 2026-08-11 10:11 with ``extra_data == {}``
    entirely, and ``item_count == count(snapshot_items)`` (so not even a
    count mismatch hinted at what was missed).

The handler-driven flips (the USER_MAIL / USER_CHATS honesty gates) always
stamp ``degraded_reason``; these now stamp the equivalent, synthesized from
the counters already on the row — no extra query, and it rides out on the
same terminal UPDATE (`_write_snapshot_terminal` persists extra_data).

``degraded_reason`` is written ONLY when PARTIAL is what actually settled:
it makes every other finalize site derive PARTIAL, so stamping it on a
cancel-coerced FAILED row would let a later re-derivation upgrade a
cancelled run into a partially-restorable one.
"""
from __future__ import annotations

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
_spec = importlib.util.spec_from_file_location("bw_counter_partial", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_counter_partial"] = _bw
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(_bw)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(
        f"backup-worker module failed to import: {exc}",
        allow_module_level=True,
    )


# ─── the pure synthesizer ─────────────────────────────────────────────────

def test_no_failures_means_no_reason():
    """A clean COMPLETED run must never get a reason stamped."""
    assert _bw.file_failure_degraded_reason(None) is None
    assert _bw.file_failure_degraded_reason({}) is None
    assert _bw.file_failure_degraded_reason({"files_failed": 0}) is None
    assert _bw.file_failure_degraded_reason(
        {"files_total": 900, "files_failed": 0},
    ) is None


def test_reason_carries_the_counters():
    r = _bw.file_failure_degraded_reason(
        {"files_total": 1204, "files_succeeded": 1187, "files_failed": 17},
    )
    assert r is not None
    assert "17 of 1204 item(s) failed to back up" in r


def test_reason_survives_a_missing_total():
    """The archive path stamps files_total, but OneNote/Planner may not."""
    r = _bw.file_failure_degraded_reason({"files_failed": 3})
    assert r == "3 item(s) failed to back up"


def test_reason_flags_incomplete_enumeration():
    """Worse than a failed download — the items were never even listed."""
    r = _bw.file_failure_degraded_reason(
        {"files_total": 40, "files_failed": 12, "enumeration_incomplete": True},
    )
    assert "enumeration incomplete" in r


def test_reason_names_the_first_failed_folders():
    r = _bw.file_failure_degraded_reason({
        "files_total": 300,
        "files_failed": 25,
        "folders_failed": 3,
        "folder_failures": [
            "/Inbox/Tickets/Halo", "/Archive/2019", "/Sent Items", "/Drafts",
        ],
    })
    assert "3 folder walk(s) failed" in r
    assert "/Inbox/Tickets/Halo" in r and "/Archive/2019" in r
    assert "/Drafts" not in r, "cap the list at 3 so the counters still fit"


def test_reason_falls_back_to_failed_item_names():
    r = _bw.file_failure_degraded_reason({
        "files_total": 9,
        "files_failed": 2,
        "failed_items": [
            {"id": "01ABC", "name": "Q3 forecast.xlsx", "reason": "503"},
            {"id": "01DEF", "name": "notes.docx", "reason": "timeout"},
        ],
    })
    assert "Q3 forecast.xlsx" in r and "notes.docx" in r


def test_reason_tolerates_garbage_counters():
    """Five handlers write these keys and none share a schema."""
    assert _bw.file_failure_degraded_reason({"files_failed": "nope"}) is None
    r = _bw.file_failure_degraded_reason(
        {"files_failed": 4, "files_total": "?", "failed_items": ["a", 7, None]},
    )
    assert r.startswith("4 item(s) failed to back up")
    assert "first: a" in r


def test_reason_is_capped_at_the_column_truncation():
    r = _bw.file_failure_degraded_reason({
        "files_total": 5000,
        "files_failed": 4999,
        "folder_failures": ["/" + "x" * 200, "/" + "y" * 200, "/" + "z" * 200],
    })
    assert len(r) <= 300


# ─── the real finalize path ───────────────────────────────────────────────


class _FakeSnapshot:
    def __init__(self, *, delta_tokens_json=None, extra_data=None):
        self.id = uuid.uuid4()
        self.resource_id = uuid.uuid4()
        self.job_id = None          # no cancel coercion
        self.status = _bw.SnapshotStatus.IN_PROGRESS
        self.type = _bw.SnapshotType.INCREMENTAL
        self.started_at = datetime.utcnow() - timedelta(seconds=30)
        self.completed_at = None
        self.duration_secs = None
        self.item_count = 0
        self.new_item_count = 0
        self.bytes_added = 0
        self.bytes_total = 0
        self.delta_token = None
        self.delta_tokens_json = dict(delta_tokens_json or {})
        self.extra_data = dict(extra_data or {})


class _FakeSession:
    async def get(self, _model, _pk):
        return None

    async def commit(self):
        return None

    async def rollback(self):
        return None


def _finalize(snapshot, result=None):
    """Drive the REAL complete_snapshot; return the extra_data it would
    persist (captured at the terminal write, i.e. exactly what lands)."""
    w = _bw.BackupWorker.__new__(_bw.BackupWorker)
    w.worker_id = "test-worker"
    captured = {}

    async def _fake_flush(force=False):
        return 0

    async def _fake_terminal(session, snap, allowed_from):
        captured["status"] = snap.status
        captured["extra_data"] = dict(snap.extra_data or {})
        return True

    async def _fake_anomaly(snap):
        captured["anomaly_checked"] = True

    async def _fake_delta(session, sid, rid, count, byte_count, **kwargs):
        return count, byte_count

    w._compute_snapshot_delta_from_prior = _fake_delta
    w.flush_version_rows = _fake_flush  # type: ignore[assignment]
    w._write_snapshot_terminal = _fake_terminal  # type: ignore[assignment]
    w._check_snapshot_anomaly = _fake_anomaly  # type: ignore[assignment]

    asyncio.run(
        w.complete_snapshot(
            _FakeSession(), snapshot,
            result or {"item_count": 0, "bytes_added": 0},
        )
    )
    return captured


def test_archive_partial_records_a_reason_instead_of_an_empty_extra_data():
    """THE regression: "Archive — Jason Rue" finalized PARTIAL three runs in
    a row with extra_data == {}."""
    snap = _FakeSnapshot(
        delta_tokens_json={
            "files_total": 1204, "files_succeeded": 1187, "files_failed": 17,
        },
    )
    got = _finalize(snap, {"item_count": 1187, "bytes_added": 4096})
    assert got["status"] == _bw.SnapshotStatus.PARTIAL
    ed = got["extra_data"]
    assert ed, "PARTIAL with an EMPTY extra_data is the bug"
    assert "17 of 1204 item(s) failed to back up" in ed["failure_reason"]
    assert ed[_bw.SNAPSHOT_DEGRADED_REASON_KEY] == ed["failure_reason"], (
        "degraded_reason is what the other finalize sites and the reconciler "
        "read to keep deriving PARTIAL"
    )


def test_clean_completed_snapshot_gets_no_reason_keys():
    snap = _FakeSnapshot(delta_tokens_json={"files_total": 900})
    got = _finalize(snap, {"item_count": 900, "bytes_added": 1})
    assert got["status"] == _bw.SnapshotStatus.COMPLETED
    assert "failure_reason" not in got["extra_data"]
    assert _bw.SNAPSHOT_DEGRADED_REASON_KEY not in got["extra_data"]


def test_reason_does_not_clobber_existing_extra_data():
    snap = _FakeSnapshot(
        delta_tokens_json={"files_total": 4, "files_failed": 1},
        extra_data={"partition_type": "SHAREPOINT_DRIVES", "cancelled_at": "x"},
    )
    ed = _finalize(snap)["extra_data"]
    assert ed["partition_type"] == "SHAREPOINT_DRIVES"
    assert ed["cancelled_at"] == "x"
    assert ed["failure_reason"]


def test_partitioned_coordinator_still_returns_untouched():
    """The last-finisher owns a partitioned snapshot's terminal state — the
    reason stamp must not sneak a write in ahead of it."""
    snap = _FakeSnapshot(
        delta_tokens_json={"files_failed": 3},
        extra_data={"partitioned": True},
    )
    got = _finalize(snap)
    assert got == {}, "no terminal write at all for a partitioned coordinator"
    assert snap.status == _bw.SnapshotStatus.IN_PROGRESS
    assert "failure_reason" not in (snap.extra_data or {})
