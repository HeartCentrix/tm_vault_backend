"""Regression: a backup checkpoint must only resume the snapshot it describes.

Bug (RCA 2026-08-04): ``BackupCheckpoint`` lives on ``Job.result`` and its
``files_done_ext_ids`` set was keyed by external_id alone. When an attempt died
(stale-sweep, OOM — recurring on 500GB drives), the redelivered job got a NEW
snapshot (create_snapshot deliberately does not resume FAILED snapshots), and
the old checkpoint was replayed against it: every file downloaded by the dead
attempt was skipped, its freshly-inserted ONEDRIVE_FILE row kept
``blob_path`` NULL forever, and the first attempt's blobs were reaped with its
snapshot. The snapshot still flipped COMPLETED and the deltaLink still
promoted, so unchanged files never re-entered the delta stream: silently
unrestorable files, permanently.

Pins the binding contract; the worker-side consumption (discard + re-download)
is asserted by source inspection so this stays a pure unit test.
"""
from __future__ import annotations

import pathlib

from shared.backup_checkpoint import BackupCheckpoint


def test_checkpoint_carries_its_snapshot_id_through_roundtrip():
    cp = BackupCheckpoint.empty(resource_id="r1", drive_id="d1", snapshot_id="snap-A")
    cp.record_file_done(external_id="f1", size=10)
    revived = BackupCheckpoint.from_dict(cp.to_dict())
    assert revived.snapshot_id == "snap-A"
    assert revived.is_done("f1")


def test_belongs_to_requires_the_whole_identity_triple():
    """The guard validates resource_id AND drive_id AND snapshot_id. One Job
    id covers every resource of a bulk fan-out, so the single
    backup_checkpoint slot can hold a SIBLING's progress — snapshot_id alone
    is not enough to prove the done-set describes this run's rows."""
    cp = BackupCheckpoint.empty(resource_id="r1", drive_id="d1", snapshot_id="snap-A")
    ok = dict(resource_id="r1", drive_id="d1", snapshot_id="snap-A")
    assert cp.belongs_to(**ok) is True
    assert cp.belongs_to(**{**ok, "snapshot_id": "snap-B"}) is False, (
        "a checkpoint from a dead attempt's snapshot must not be resumed — "
        "its blobs were reaped with the old one"
    )
    assert cp.belongs_to(**{**ok, "resource_id": "r2"}) is False, (
        "a sibling resource's checkpoint must not be replayed here"
    )
    assert cp.belongs_to(**{**ok, "drive_id": "d2"}) is False


def test_legacy_payload_without_snapshot_id_is_not_resumable():
    """Pre-binding payloads may describe an already-reaped snapshot. Re-
    downloading is always cheaper than shipping metadata-only rows."""
    legacy = BackupCheckpoint.from_dict({
        "resource_id": "r1", "drive_id": "d1",
        "files_done_ext_ids": ["f1", "f2"], "files_done_count": 2, "bytes_done": 20,
    })
    assert not legacy.snapshot_id
    assert legacy.belongs_to(
        resource_id="r1", drive_id="d1", snapshot_id="snap-A",
    ) is False


def test_worker_gates_resume_on_identity_and_checkpoints_only_landed_rows():
    """Two properties the worker must hold.

    1. Resume is gated on the checkpoint's full identity (belongs_to), so a
       checkpoint written against a now-dead snapshot is discarded and its
       files are re-downloaded rather than skipped into metadata-only rows.
    2. A file is only recorded as done AFTER its blob_path row update has
       landed, and only when the UPDATE actually matched a row on THIS
       snapshot. The unmatched case is loud and deliberately left out of the
       checkpoint — bytes exist in storage but nothing points at them, so a
       resume must redo those files instead of skipping them. (This is the
       deployed lineage's design; it is strictly stronger than ordering a
       whole-batch flush before the checkpoint commit, which could still
       checkpoint ids whose row never matched.)
    """
    src = (
        pathlib.Path(__file__).resolve().parents[2]
        / "workers" / "backup-worker" / "main.py"
    ).read_text()
    i = src.find("# Load existing checkpoint (resume path).")
    assert i != -1, "OneDrive checkpoint load site moved"
    block = src[i:i + 2400]
    assert "belongs_to(" in block, (
        "resume must be gated on the checkpoint's identity triple"
    )
    assert "snapshot_id=str(snapshot.id)" in block, (
        "a fresh checkpoint must record which snapshot it describes"
    )

    j = src.find("checkpoint.record_file_done(")
    assert j != -1, "record_file_done call site moved"
    region = src[max(0, j - 2500): j + 1500]
    assert "_unmatched" in region, (
        "ids whose blob_path UPDATE matched no row must be excluded from the "
        "checkpoint, not silently marked done"
    )
    assert "not checkpointed" in region
