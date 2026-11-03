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

Pins legacy identity and current source-version matching. Worker behavior is
covered by worker unit tests and PostgreSQL resume tests.
"""
from __future__ import annotations

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


def test_version_matching_requires_a_durable_blob_and_current_source():
    from shared.backup_checkpoint import file_receipt_matches
    source = {"id": "f", "size": 10, "eTag": "v1", "parentReference": {"driveId": "d"}}
    values = dict(source=source, current=source, drive_id="d", blob_path="saved/blob", content_size=10)
    assert file_receipt_matches(**values)
    assert not file_receipt_matches(**{**values, "blob_path": None})
    assert not file_receipt_matches(**{**values, "blob_path": ""})
    assert not file_receipt_matches(**{**values, "content_size": 9})
    assert not file_receipt_matches(**{**values, "drive_id": "other"})
    assert not file_receipt_matches(**{**values, "current": {**source, "eTag": "v2"}})
    assert not file_receipt_matches(**{**values, "source": {"id": "f", "size": 10}})


def test_content_only_version_needs_matching_metadata_too():
    from shared.backup_checkpoint import file_receipt_matches
    source = {"id": "f", "name": "original", "size": 10, "cTag": "content-v1"}
    values = dict(source=source, current=source, drive_id="d", blob_path="saved/blob", content_size=10)
    assert file_receipt_matches(**values)
    assert not file_receipt_matches(**{**values, "current": {**source, "name": "renamed"}})
    assert not file_receipt_matches(**{**values, "current": {**source, "cTag": "content-v2"}})


def test_unknown_versions_never_skip_and_hash_fallback_is_content_bound():
    from shared.backup_checkpoint import file_receipt_matches
    source = {"id": "f", "name": "file", "size": 10}
    values = dict(source=source, current=source, drive_id="d", blob_path="saved/blob", content_size=10)
    assert not file_receipt_matches(**values)
    source = {**source, "file": {"hashes": {"sha256Hash": "a"}}}
    assert file_receipt_matches(**{**values, "source": source, "current": source})
    assert not file_receipt_matches(**{**values, "source": source,
        "current": {**source, "file": {"hashes": {"sha256Hash": "b"}}}})
