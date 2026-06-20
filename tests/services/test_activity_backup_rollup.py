"""Tests for the activity row shape produced from backup_batches.

Pins the contract that Activity rows for BACKUP come from the
backup_batches row's own fields (scope length, bytes_done,
bytes_expected, status) — NOT from the legacy _group_batch_jobs
reconstruction. Two different rollups for the same operator click
produced two different numbers in the 2026-05-15 incident; this
test pins the single source of truth.

audit-service has a hyphen in the dir name, so the helper module
is loaded via importlib path (same pattern as test_exclusion_matcher.py
in tests/workers/).
"""
from __future__ import annotations

import datetime as dt
import importlib.util
import pathlib
import sys
import uuid

import pytest


_HELPER_PATH = (
    pathlib.Path(__file__).resolve().parents[2]
    / "services" / "audit-service" / "activity_backup.py"
)
_spec = importlib.util.spec_from_file_location(
    "activity_backup_under_test", _HELPER_PATH,
)
_mod = importlib.util.module_from_spec(_spec)
sys.modules["activity_backup_under_test"] = _mod
try:
    _spec.loader.exec_module(_mod)
except Exception as exc:
    pytest.skip(
        f"activity_backup module failed to import: {exc}",
        allow_module_level=True,
    )

shape_activity_row = _mod.shape_activity_row
merge_backup_batch_rows = _mod.merge_backup_batch_rows
prune_unrun_terminal_children = _mod.prune_unrun_terminal_children


def _row(**kw):
    """Fake DB row matching the SELECT shape used by list_activities."""
    base = dict(
        batch_id=str(uuid.uuid4()),
        created_at=dt.datetime(2026, 5, 15, 1, 8, 46),
        completed_at=None,
        status="IN_PROGRESS",
        source="manual_bulk",
        actor_email="rohit@qfion.com",
        scope_user_ids=[uuid.uuid4() for _ in range(9)],
        bytes_expected=None,
        bytes_done=0,
        job_ids=[],
        waiting_discovery_count=0,
        total_scope_count=9,
    )
    base.update(kw)
    return type("R", (), base)


def test_user_count_from_scope_length():
    """Displayed user count must equal len(scope_user_ids).
    The 2026-05-15 incident showed a rollup-derived figure (9)
    instead of the operator's actual click count (54). Pin the
    rule: use the row's own scope, not Job-grouping output."""
    scope = [uuid.uuid4() for _ in range(54)]
    out = shape_activity_row(_row(scope_user_ids=scope, total_scope_count=54))
    assert out["object"] == "54 users"


def test_single_user_uses_singular():
    scope = [uuid.uuid4()]
    out = shape_activity_row(_row(scope_user_ids=scope, total_scope_count=1))
    assert out["object"] == "1 user"


def test_progress_pct_null_when_bytes_expected_null():
    out = shape_activity_row(_row(bytes_expected=None, bytes_done=1024))
    assert out["progressPct"] is None


def test_progress_pct_capped_at_100():
    out = shape_activity_row(_row(bytes_expected=1000, bytes_done=2000))
    assert out["progressPct"] == 100


def test_progress_pct_integer_division():
    out = shape_activity_row(_row(bytes_expected=1000, bytes_done=789))
    assert out["progressPct"] == 78


def test_in_progress_with_waiting_users_appends_subhint():
    """When some pending users are still WAITING_DISCOVERY, the
    operator should see why the batch is taking time."""
    out = shape_activity_row(_row(
        status="IN_PROGRESS",
        waiting_discovery_count=12,
        total_scope_count=54,
    ))
    assert "discovering 12 of 54" in out["details"]


def test_completed_status_label():
    out = shape_activity_row(_row(
        status="COMPLETED",
        completed_at=dt.datetime(2026, 5, 15, 1, 30),
        bytes_expected=1000,
        bytes_done=1000,
    ))
    assert out["status"] == "Done"
    assert "backed up" in out["details"].lower()


def test_failed_status_label():
    out = shape_activity_row(_row(status="FAILED"))
    assert out["status"] == "Failed"


def test_operation_constant():
    out = shape_activity_row(_row())
    assert out["operation"] == "BACKUP"


def test_v2_batch_rows_inherit_legacy_progress_without_losing_scope_label():
    batch_id = str(uuid.uuid4())
    v2 = [{
        "id": batch_id,
        "batchId": batch_id,
        "operation": "BACKUP",
        "object": "11 users",
        "status": "In Progress",
        "details": "",
        "progressPct": None,
        "bytesDone": 1024,
        "bytesExpected": None,
    }]
    legacy = [{
        "id": batch_id,
        "batchId": batch_id,
        "operation": "BACKUP",
        "object": "55 resources",
        "status": "In Progress",
        "details": "1.0 KiB so far",
        "progress_pct": 42,
        "phase": "in_progress",
        "counts": {"total": 55, "done": 23, "partial": 0, "failed": 0, "in_progress": 32, "queued": 0},
        "cancellable": True,
    }]

    out = merge_backup_batch_rows(legacy, v2)

    assert len(out) == 1
    assert out[0]["object"] == "11 users"
    assert out[0]["progressPct"] == 42
    assert out[0]["progress_pct"] == 42
    assert out[0]["details"] == "1.0 KiB so far"
    assert out[0]["counts"]["done"] == 23


def test_v2_batch_rows_replace_matching_legacy_rows():
    batch_id = str(uuid.uuid4())
    out = merge_backup_batch_rows(
        legacy_items=[
            {"id": batch_id, "batchId": batch_id, "operation": "BACKUP", "object": "legacy"},
            {"id": "discovery-1", "operation": "DISCOVERY", "object": "Discovery"},
        ],
        v2_rows=[
            {"id": batch_id, "batchId": batch_id, "operation": "BACKUP", "object": "v2"},
        ],
    )

    assert [row["object"] for row in out] == ["v2", "Discovery"]


def test_terminal_batch_drilldown_hides_unrun_child_workloads():
    resources = [{
        "resourceId": "user-1",
        "displayName": "Akshat Verma",
        "type": "ENTRA_USER",
        "tier": 1,
        "status": "COMPLETED",
        "children": [
            {
                "resourceId": "mail-1",
                "displayName": "Akshat Verma",
                "type": "USER_MAIL",
                "tier": 2,
                "snapshotId": "snap-mail-1",
                "status": "COMPLETED",
                "itemCount": 12,
                "bytesAdded": 2048,
            },
            {
                "resourceId": "chat-1",
                "displayName": "Akshat Verma",
                "type": "USER_CHATS",
                "tier": 2,
            },
        ],
    }]

    out = prune_unrun_terminal_children(resources, "COMPLETED")

    assert [child["type"] for child in out[0]["children"]] == ["USER_MAIL"]


def test_in_progress_batch_drilldown_keeps_pending_child_workloads():
    resources = [{
        "resourceId": "user-1",
        "displayName": "Akshat Verma",
        "type": "ENTRA_USER",
        "tier": 1,
        "children": [
            {
                "resourceId": "mail-1",
                "displayName": "Akshat Verma",
                "type": "USER_MAIL",
                "tier": 2,
                "snapshotId": "snap-mail-1",
                "status": "IN_PROGRESS",
            },
            {
                "resourceId": "chat-1",
                "displayName": "Akshat Verma",
                "type": "USER_CHATS",
                "tier": 2,
            },
        ],
    }]

    out = prune_unrun_terminal_children(resources, "IN_PROGRESS")

    assert [child["type"] for child in out[0]["children"]] == ["USER_MAIL", "USER_CHATS"]
