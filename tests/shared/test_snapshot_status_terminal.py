"""SnapshotStatus.terminal() — the set of statuses that mean a snapshot is no
longer an in-flight backup.

Root cause this guards: the backup-scheduler finished_nonterminal_reaper listed
'CANCELLED' in a `snapshotstatus` enum comparison, but CANCELLED is NOT a member
of the enum (a cancelled snapshot is flipped to FAILED). Postgres coerces every
IN-list literal to the enum type, so the invalid label raised
InvalidTextRepresentationError and the reaper crashed on EVERY run — jobs whose
snapshots had all finished stayed stuck QUEUED/RUNNING ("In Progress" forever).

Deriving the terminal set from the enum (instead of a hand-typed SQL literal
list) makes an invalid status a failing unit test rather than a production crash.
"""
from __future__ import annotations

import os

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")

from shared.models import SnapshotStatus


def test_terminal_statuses_are_all_valid_enum_members():
    all_vals = {s.value for s in SnapshotStatus}
    terminal_vals = {s.value for s in SnapshotStatus.terminal()}
    assert terminal_vals <= all_vals  # the bug: 'CANCELLED' was not a member


def test_cancelled_is_not_a_snapshot_status():
    # documents the root cause: snapshots never use 'CANCELLED' — cancel flips
    # the snapshot to FAILED (see backup-scheduler cancel endpoint).
    assert "CANCELLED" not in {s.value for s in SnapshotStatus}


def test_in_progress_is_never_terminal():
    assert SnapshotStatus.IN_PROGRESS not in SnapshotStatus.terminal()


def test_pending_deletion_counts_as_terminal():
    # a snapshot marked for deletion is not an in-flight backup — it must not
    # keep its owning job stuck "in progress" forever.
    assert SnapshotStatus.PENDING_DELETION in SnapshotStatus.terminal()
