"""OneDrive incremental Activity counters (Railway 2026-11-06): an edited .docx keeps its drive-item id,
so the (external_id, item_type) set-difference against the prior snapshot reported new_item_count=0 /
bytes_added=0 although a new file version was stored. Changed content must count; unchanged must not."""
from __future__ import annotations

import pytest

import shared.models as models
from tests.integration.test_mail_overlay_postgres import _worker_module, db  # noqa: F401 (fixture)

pytestmark = pytest.mark.integration


async def _file(db, sid, ext, sha, size, item_type="ONEDRIVE_FILE"):
    async with db.factory.begin() as session:
        session.add(models.SnapshotItem(
            snapshot_id=sid, tenant_id=db.tenant, backend_id=db.backend, item_type=item_type,
            external_id=ext, name=ext, folder_path="/", content_size=size, content_hash=sha,
            content_checksum=sha, blob_path=f"blob/{sha}" if sha else None, extra_data={}))


async def _delta(db, rid, sid, job):
    delta = _worker_module().BackupWorker._compute_snapshot_delta_from_prior
    async with db.factory() as session:
        return await delta(session, sid, rid, 0, 0, current_job_id=job)


async def _prior(db, rid):
    prior, _ = await db.snapshot(rid, models.SnapshotStatus.COMPLETED, item_count=2, bytes_total=300)
    await _file(db, prior, "doc", "aaa", 100)
    await _file(db, prior, "xls", "bbb", 200)


@pytest.mark.parametrize("item_type", ["ONEDRIVE_FILE", "FILE"])
async def test_edited_file_counts_as_new_version(db, item_type):
    rid = await db.resource(models.ResourceType.USER_ONEDRIVE)
    prior, _ = await db.snapshot(rid, models.SnapshotStatus.COMPLETED, item_count=1, bytes_total=100)
    await _file(db, prior, "doc", "aaa", 100, item_type)
    sid, job = await db.snapshot(rid)
    await _file(db, sid, "doc", "ccc", 11963, item_type)
    assert await _delta(db, rid, sid, job) == (1, 11963)


async def test_unchanged_relisted_file_and_empty_incremental_report_zero(db):
    rid = await db.resource(models.ResourceType.USER_ONEDRIVE)
    await _prior(db, rid)
    sid, job = await db.snapshot(rid)
    await _file(db, sid, "doc", "aaa", 100)          # same content: rename/metadata-only change
    await _file(db, sid, "xls", None, 200)           # listed but not blobbed this run
    assert await _delta(db, rid, sid, job) == (0, 0)
    empty, job2 = await db.snapshot(rid)
    assert await _delta(db, rid, empty, job2) == (0, 0)


async def test_new_and_edited_files_sum(db):
    rid = await db.resource(models.ResourceType.USER_ONEDRIVE)
    await _prior(db, rid)
    sid, job = await db.snapshot(rid)
    await _file(db, sid, "doc", "ddd", 150)
    await _file(db, sid, "xls", "bbb", 200)
    await _file(db, sid, "new", "eee", 50)
    assert await _delta(db, rid, sid, job) == (2, 200)
