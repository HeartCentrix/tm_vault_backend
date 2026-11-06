"""A OneDrive delta deletion is an is_deleted=true ONEDRIVE_FILE row (no blob) with the file's external_id.
Recovery and restore are newest-wins over every restore point up to the selected one: the newest row
decides, so the tombstone hides the file from that point on, and older points still show it."""
import pytest
from sqlalchemy import text

from tests.integration.test_mail_overlay_postgres import db, _load  # noqa: F401
from shared import models

pytestmark = pytest.mark.integration


async def _drive(db):
    rid = await db.resource(models.ResourceType.USER_ONEDRIVE)
    old, _ = await db.snapshot(rid, models.SnapshotStatus.COMPLETED)
    for f in ("gone", "kept"):
        await db.item(old, f, "ONEDRIVE_FILE", folder="/Documents", blob=f"od/{f}", name=f"{f}.docx")
    new, _ = await db.snapshot(rid, models.SnapshotStatus.COMPLETED)
    await db.item(new, "gone", "ONEDRIVE_FILE", folder="/Documents", name="gone.docx", size=0)
    await db.item(new, "added", "ONEDRIVE_FILE", folder="/Documents", blob="od/added", name="added.docx")
    async with db.factory.begin() as session:
        await session.execute(text("UPDATE snapshot_items SET is_deleted = true, blob_path = NULL "
                                   "WHERE snapshot_id = :s AND external_id = 'gone'"), {"s": new})
    return old, new


async def test_deleted_file_shows_at_older_points_and_not_from_its_deletion_on(db, monkeypatch):
    api = _load("onedrive_tombstone_api", "services/snapshot-service/main.py")
    monkeypatch.setattr(api, "_count_session_factory", db.factory)
    restore = _load("onedrive_tombstone_restore", "workers/restore-worker/main.py")
    worker = restore.RestoreWorker.__new__(restore.RestoreWorker)
    old, new = await _drive(db)
    seen = {}
    for point in (old, new):
        sid = str(point)
        async with db.factory() as session:
            listed = await api.list_snapshot_onedrive(sid, page=1, size=500, folder=None, search=None, sort=None,
                                                      createdSince=None, modifiedSince=None, db=session)
            folders = await api.get_snapshot_folders(snapshot_id=sid, item_type="ONEDRIVE_FILE", category=None,
                                                     page=1, size=50, db=session)
            ids = await api.list_onedrive_ids_by_prefix(sid, folder_prefix="/", db=session)
            names = dict((await session.execute(text("SELECT id::text, external_id FROM snapshot_items"))).all())
            fetched = await worker.fetch_snapshot_items(session, [sid], [])
            counted = await worker.count_snapshot_items(session, [sid], [])
        streamed = [i async for batch in worker.stream_snapshot_items_by_group(db.factory, [sid], []) for i in batch]
        content = listed["content"] if isinstance(listed, dict) else listed.content
        seen[point] = (sorted(i["externalId"] if isinstance(i, dict) else i.externalId for i in content),
                       [(f["path"], f["count"]) for f in folders["content"]],
                       sorted(names[i] for i in ids["ids"]),
                       sorted(i.external_id for i in fetched), sorted(i.external_id for i in streamed), counted)
    assert seen[old] == (["gone", "kept"], [("/Documents", 2)], ["gone", "kept"], ["gone", "kept"], ["gone", "kept"], 2)
    assert seen[new] == (["added", "kept"], [("/Documents", 2)], ["added", "kept"], ["added", "kept"], ["added", "kept"], 2)
