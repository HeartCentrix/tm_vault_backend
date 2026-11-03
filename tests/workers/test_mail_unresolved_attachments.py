"""A failed attachment download/upload leaves an EMAIL_ATTACHMENT row with
resolved=false; the snapshot still settled COMPLETED and its totals included the
declared size (2026-11-04 local audit). Unresolved required rows must degrade the
snapshot and not count as captured bytes."""
import importlib.util
import sys
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


@pytest.fixture(scope="module")
def bw():
    path = Path(__file__).resolve().parents[2] / "workers/backup-worker/main.py"
    spec = importlib.util.spec_from_file_location("bw_mail_unresolved", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _row(resolved, size=100):
    return SimpleNamespace(item_type="EMAIL_ATTACHMENT", content_size=size, extra_data={"resolved": resolved})


def test_unresolved_rows_are_counted_and_their_bytes_zeroed(bw):
    rows = [_row(True), _row(False, 500), _row(False, 700), SimpleNamespace(item_type="EMAIL", content_size=9, extra_data={})]
    assert bw._mail_attachment_gap(rows) == 2
    assert [r.content_size for r in rows] == [100, 0, 0, 9]
    assert rows[1].extra_data["declared_size"] == 500


async def test_gap_degrades_snapshot(bw):
    worker = bw.BackupWorker.__new__(bw.BackupWorker)
    worker.worker_id = "regression"
    worker._stamp_snapshot_degraded = AsyncMock(return_value=True)
    snapshot_id = uuid.uuid4()
    await worker._degrade_for_attachment_gap(snapshot_id, [_row(True), _row(False)])
    worker._stamp_snapshot_degraded.assert_awaited_once()
    assert worker._stamp_snapshot_degraded.await_args.args[0] == snapshot_id
    assert "1 email attachment" in worker._stamp_snapshot_degraded.await_args.args[1]
    worker._stamp_snapshot_degraded.reset_mock()
    await worker._degrade_for_attachment_gap(snapshot_id, [_row(True)])
    worker._stamp_snapshot_degraded.assert_not_awaited()


def test_unresolved_reference_links_are_not_a_capture_gap(bw):
    """referenceAttachment rows are links to OneDrive/SharePoint files, persisted as
    metadata when the link cannot be resolved; they are not required mail content."""
    link = SimpleNamespace(item_type="EMAIL_ATTACHMENT", content_size=50,
                           extra_data={"resolved": False, "attachment_kind": "#microsoft.graph.referenceAttachment"})
    assert bw._mail_attachment_gap([link]) == 0 and link.content_size == 50
