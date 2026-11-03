"""Per-job backup checkpoint shared by OneDrive + any future streaming-backup workload.

Lives on `Job.result.backup_checkpoint`. Commit cadence is "N files OR M bytes
since last commit, whichever first". Resume filters file metadata by the
`files_done_ext_ids` set so redeliveries don't re-download.

SNAPSHOT BINDING (2026-08-04): a checkpoint vouches for blobs recorded on ONE
snapshot's rows. A job whose snapshot died (swept/OOM) gets a NEW snapshot on
redelivery — create_snapshot deliberately does not resume FAILED snapshots — so
replaying the old checkpoint against it marked every previously-downloaded file
"done" while its row in the NEW snapshot kept blob_path NULL forever, and the
first attempt's blobs were reaped with its snapshot. The snapshot then completed
and the delta cursor advanced, so those files never returned: metadata-only rows
that restore to nothing, permanently. The identity triple
(resource_id, drive_id, snapshot_id) makes a checkpoint from a different
snapshot — or from a sibling resource of the same bulk fan-out — unusable
(see ``belongs_to``).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Optional


@dataclass
class BackupCheckpoint:
    resource_id: str
    drive_id: str
    # Identity triple with resource_id/drive_id. One Job id covers every
    # resource of a bulk fan-out, so the single `backup_checkpoint` slot
    # can hold a sibling's (or a previous attempt's) progress — the
    # `files_done_ext_ids` are only meaningful for the snapshot whose rows
    # they were written against.
    snapshot_id: str = ""
    delta_token_at_resume: Optional[str] = None
    files_done_ext_ids: set = field(default_factory=set)
    files_done_count: int = 0
    bytes_done: int = 0
    last_commit_at: Optional[str] = None

    _files_since_commit: int = field(default=0, repr=False)
    _bytes_since_commit: int = field(default=0, repr=False)

    @classmethod
    def empty(
        cls, *, resource_id: str, drive_id: str, snapshot_id: str = "",
    ) -> "BackupCheckpoint":
        return cls(
            resource_id=resource_id, drive_id=drive_id,
            snapshot_id=snapshot_id,
        )

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "BackupCheckpoint":
        return cls(
            resource_id=payload.get("resource_id", ""),
            drive_id=payload.get("drive_id", ""),
            snapshot_id=payload.get("snapshot_id", ""),
            delta_token_at_resume=payload.get("delta_token_at_resume"),
            files_done_ext_ids=set(payload.get("files_done_ext_ids", []) or []),
            files_done_count=int(payload.get("files_done_count", 0)),
            bytes_done=int(payload.get("bytes_done", 0)),
            last_commit_at=payload.get("last_commit_at"),
        )

    def belongs_to(
        self, *, resource_id: str, drive_id: str, snapshot_id: str,
    ) -> bool:
        """True only when this checkpoint's completed-file set was written
        against exactly this (resource, drive, snapshot). A payload with no
        snapshot_id predates the identity stamp and is unverifiable, so it
        never matches — re-downloading beats skipping a file whose
        blob_path row may not exist."""
        return bool(
            self.snapshot_id
            and self.resource_id == resource_id
            and self.drive_id == drive_id
            and self.snapshot_id == snapshot_id
        )

    def is_done(self, external_id: str) -> bool:
        return external_id in self.files_done_ext_ids

    def record_file_done(self, *, external_id: str, size: int) -> None:
        if external_id in self.files_done_ext_ids:
            return
        self.files_done_ext_ids.add(external_id)
        self.files_done_count += 1
        self.bytes_done += size
        self._files_since_commit += 1
        self._bytes_since_commit += size

    def should_commit(self, *, every_files: int, every_bytes: int) -> bool:
        return self._files_since_commit >= every_files or self._bytes_since_commit >= every_bytes

    def mark_committed(self) -> None:
        self.last_commit_at = datetime.now(timezone.utc).isoformat()
        self._files_since_commit = 0
        self._bytes_since_commit = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "resource_id": self.resource_id,
            "drive_id": self.drive_id,
            "snapshot_id": self.snapshot_id,
            "delta_token_at_resume": self.delta_token_at_resume,
            "files_done_ext_ids": sorted(self.files_done_ext_ids),
            "files_done_count": self.files_done_count,
            "bytes_done": self.bytes_done,
            "last_commit_at": self.last_commit_at,
        }
