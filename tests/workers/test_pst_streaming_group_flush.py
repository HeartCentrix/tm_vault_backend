"""Regression tests for streaming PST export group-flush accounting.

Pins the incident where ``completed_keys`` (the crash-resume set) also
acted as the live skip-filter: the moment a group hit the mid-stream
flush threshold it was marked completed, and every later item streamed
for that group was silently dropped — a 10k-item Inbox exported as a
1000-message PST with job status 'done' and failed=0.

Pinned behavior (not implementation):
  * every streamed item of a group larger than the flush threshold is
    written — none silently dropped, none double-written on the happy path;
  * each mid-stream flush emits a distinct PST filename (blob keys and
    ZIP member names are derived from it, so a collision overwrites an
    earlier part);
  * the crash-resume checkpoint marks a group completed only once its
    stream is exhausted — resuming after a mid-stream crash must re-export
    the group's remaining items rather than skip them.

restore-worker has a hyphen in its dir, so modules are imported by
putting the worker dir on sys.path (same trick the worker itself uses).
"""
from __future__ import annotations

import asyncio
import pathlib
import sys
from types import SimpleNamespace

_WORKER_DIR = pathlib.Path(__file__).resolve().parents[2] / "workers" / "restore-worker"
if str(_WORKER_DIR) not in sys.path:
    sys.path.insert(0, str(_WORKER_DIR))

import pst_export  # noqa: E402


class _FakeWriteResult:
    def __init__(self, item_count):
        self.item_count = item_count
        self.failed_count = 0
        self.skipped_count = 0
        self.pst_paths = []  # no PSTs on disk → upload loop is a no-op


class _RecordingWriter:
    """Stands in for MailPstWriter; records what each flush was asked
    to write."""

    calls: list = []  # (pst_filename, [external_id, ...])

    async def write(self, group, workdir, split_gb, shard, source_container):
        type(self).calls.append(
            (group.pst_filename, [it.external_id for it in group.items])
        )
        return _FakeWriteResult(len(group.items))


def _mk_item(i: int):
    return SimpleNamespace(
        item_type="EMAIL",
        snapshot_id="snap0001",
        external_id=f"msg-{i:04d}",
        folder_path="Inbox",
        name=f"msg {i}",
        blob_path=None,
    )


def _mk_orchestrator(items, checkpoints, tmp_path, batch_size=5):
    async def stream():
        for start in range(0, len(items), batch_size):
            yield items[start:start + batch_size]

    async def checkpoint_saver(cp):
        checkpoints.append(cp)

    orch = pst_export.PstExportOrchestrator(
        job_id="test-job",
        items=[],
        spec={"pstGranularity": "MAILBOX"},
        storage_shard=object(),  # pre-resolved → no azure_storage_manager lookup
        dest_container="exports",
        source_container="backup-email-test",
        tenant_id="",  # rate limiter no-ops on empty scope key
        checkpoint_saver=checkpoint_saver,
        item_stream_factory=stream,
    )
    orch.workdir = tmp_path / "pst-work"
    return orch


def test_group_larger_than_flush_threshold_exports_every_item(
    monkeypatch, tmp_path,
):
    monkeypatch.setenv("PST_GROUP_FLUSH_AT", "10")
    monkeypatch.setattr(pst_export, "MailPstWriter", _RecordingWriter)
    _RecordingWriter.calls = []

    items = [_mk_item(i) for i in range(25)]
    checkpoints: list = []
    orch = _mk_orchestrator(items, checkpoints, tmp_path)

    result = asyncio.run(orch.run())

    written = [ext for _, exts in _RecordingWriter.calls for ext in exts]
    # Every streamed item written exactly once — the pre-fix behavior
    # dropped everything after the first flush (10 written, 15 lost).
    assert sorted(written) == sorted(it.external_id for it in items)
    assert result["item_counts_by_type"] == {"EMAIL": 25}
    assert result["failed_counts_by_type"].get("EMAIL", 0) == 0
    assert result["items_processed"] == 25

    # Multiple flushes happened and every part got a distinct filename
    # (same name ⇒ same blob key ⇒ earlier part overwritten).
    filenames = [fn for fn, _ in _RecordingWriter.calls]
    assert len(filenames) >= 2
    assert len(set(filenames)) == len(filenames)


def test_checkpoint_marks_group_completed_only_after_stream_end(
    monkeypatch, tmp_path,
):
    monkeypatch.setenv("PST_GROUP_FLUSH_AT", "10")
    monkeypatch.setattr(pst_export, "MailPstWriter", _RecordingWriter)
    _RecordingWriter.calls = []

    items = [_mk_item(i) for i in range(25)]
    checkpoints: list = []
    orch = _mk_orchestrator(items, checkpoints, tmp_path)

    asyncio.run(orch.run())

    assert len(checkpoints) >= 2
    # Mid-stream flush checkpoints must NOT claim the group is done —
    # a crash-resume trusting them would skip the rest of the mailbox.
    for cp in checkpoints[:-1]:
        assert cp["completed_keys"] == []
    # After the final flush the group is resumable-complete.
    assert len(checkpoints[-1]["completed_keys"]) == 1


def test_small_group_keeps_clean_single_filename(monkeypatch, tmp_path):
    """Healthy path: a group under the threshold flushes once with its
    original (un-suffixed) filename."""
    monkeypatch.setenv("PST_GROUP_FLUSH_AT", "10")
    monkeypatch.setattr(pst_export, "MailPstWriter", _RecordingWriter)
    _RecordingWriter.calls = []

    items = [_mk_item(i) for i in range(4)]
    checkpoints: list = []
    orch = _mk_orchestrator(items, checkpoints, tmp_path)

    result = asyncio.run(orch.run())

    assert result["item_counts_by_type"] == {"EMAIL": 4}
    assert len(_RecordingWriter.calls) == 1
    filename = _RecordingWriter.calls[0][0]
    assert filename.endswith("-mail.pst")
    assert "part" not in filename
