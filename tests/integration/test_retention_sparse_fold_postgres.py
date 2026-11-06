"""Retention must not lose data from SPARSE incremental workloads.

OneDrive files, SharePoint library files, calendar events and contacts are captured as deltas: a snapshot holds
only what changed and Recovery / restore rebuild a restore point by unioning every earlier snapshot of the
resource, newest row per external_id winning. Deleting an intermediate incremental without folding it forward
lost every file created there and reverted every file changed there. Retention now moves the doomed snapshot's
still-current rows into the next kept snapshot, so every kept restore point reads exactly as before."""
from sqlalchemy import text

import pytest

from tests.integration.test_mail_overlay_postgres import db, _load  # noqa: F401
from shared import models
from shared.retention_cleanup import _delete_snapshots

pytestmark = pytest.mark.integration
OK = models.SnapshotStatus.COMPLETED


async def _chain(db, kind, item_type, folder):
    """full (B, C) -> inc1 (create A, change B) -> inc2 (delete C, change B) -> inc3 (create D)."""
    rid = await db.resource(kind)
    snaps = []

    async def snap(rows):
        sid, _ = await db.snapshot(rid, OK, item_count=1)
        for ext, name, deleted in rows:
            await db.item(sid, ext, item_type, folder=folder, name=name, blob=None if deleted else f"b/{ext}/{name}")
            if deleted:
                async with db.factory.begin() as session:
                    await session.execute(text("UPDATE snapshot_items SET is_deleted = true, content_size = 0 "
                                               "WHERE snapshot_id = :s AND external_id = :e"), {"s": sid, "e": ext})
        snaps.append(sid)
    await snap([("B", "b-v0", False), ("C", "c-v0", False)])
    await snap([("A", "a-v1", False), ("B", "b-v1", False)])
    await snap([("C", "c-v0", True), ("B", "b-v2", False)])
    await snap([("D", "d-v3", False)])
    return rid, snaps


def _readers(db, monkeypatch):
    api = _load("retention_fold_api", "services/snapshot-service/main.py")
    monkeypatch.setattr(api, "_count_session_factory", db.factory)
    restore = _load("retention_fold_restore", "workers/restore-worker/main.py")
    worker = restore.RestoreWorker.__new__(restore.RestoreWorker)

    async def restored(sid):
        async with db.factory() as session:
            rows = await worker.fetch_snapshot_items(session, [str(sid)], [])
        return sorted((i.external_id, i.name) for i in rows)
    return api, restored


async def _retire(db, doomed):
    async with db.factory.begin() as session:
        return await _delete_snapshots(session, set(doomed))


async def _assert_folded(db, view, restored, snaps, *, view_inc3=None):
    full, inc1, inc2, inc3 = snaps
    before = {s: (await view(s), await restored(s)) for s in (full, inc3)}
    # C's tombstone hides it at inc3, and still does once folded into inc3 (asserted below)
    assert before[inc3][1] == [("A", "a-v1"), ("B", "b-v2"), ("D", "d-v3")]
    if view_inc3 is not None:
        assert before[inc3][0] == view_inc3
    assert await _retire(db, [inc1, inc2]) == 2
    assert {s: (await view(s), await restored(s)) for s in (full, inc3)} == before


async def test_onedrive_retention_folds_deleted_incrementals_into_the_next_kept_point(db, monkeypatch):
    api, restored = _readers(db, monkeypatch)
    _, snaps = await _chain(db, models.ResourceType.USER_ONEDRIVE, "ONEDRIVE_FILE", "/Documents")

    async def view(sid):
        async with db.factory() as session:
            listed = await api.list_snapshot_onedrive(str(sid), page=1, size=500, folder=None, search=None, sort=None,
                                                      createdSince=None, modifiedSince=None, db=session)
        content = listed["content"] if isinstance(listed, dict) else listed.content
        return sorted((i["externalId"], i["name"]) if isinstance(i, dict) else (i.externalId, i.name) for i in content)
    await _assert_folded(db, view, restored, snaps)
    assert await view(snaps[0]) == [("B", "b-v0"), ("C", "c-v0")]


@pytest.mark.parametrize("retired", [(0,), (0, 1), (2,), (1, 2), (0, 1, 2)],
                         ids=["base", "base+inc1", "middle", "inc1+inc2", "all-but-newest"])
async def test_chain_survives_any_retirement_and_takes_the_next_incremental(db, monkeypatch, retired):
    """Whatever retention retires - the base, the base and its first incremental, a middle incremental, or
    everything but the newest - every kept point reads as before, its blobs stay referenced, and the chain
    keeps working: the next incremental after retention unions onto the kept points."""
    from shared.backup_cleanup import BlobCandidate, select_deletable_blob_paths
    api, restored = _readers(db, monkeypatch)
    rid, snaps = await _chain(db, models.ResourceType.USER_ONEDRIVE, "ONEDRIVE_FILE", "/Documents")
    doomed = [snaps[i] for i in retired]
    kept = [s for s in snaps if s not in doomed]
    before = {s: await restored(s) for s in kept}
    assert await _retire(db, doomed) == len(doomed)
    assert {s: await restored(s) for s in kept} == before
    async with db.factory() as session:
        plan = await select_deletable_blob_paths(session, doomed_snapshot_ids=doomed, candidates=[
            BlobCandidate(blob_path=f"b/{ext}/{name}", snapshot_id=doomed[0], tenant_id=db.tenant)
            for s in kept for ext, name in before[s]])
    assert not plan.deletable
    nxt, _ = await db.snapshot(rid, OK, item_count=1)
    await db.item(nxt, "A", "ONEDRIVE_FILE", folder="/Documents", name="a-v4", blob="b/A/a-v4")
    await db.item(nxt, "D", "ONEDRIVE_FILE", folder="/Documents", name="d-v3")
    async with db.factory.begin() as session:
        await session.execute(text("UPDATE snapshot_items SET is_deleted = true WHERE snapshot_id = :s "
                                   "AND external_id = 'D'"), {"s": nxt})
    assert await restored(nxt) == [("A", "a-v4"), ("B", "b-v2")]


async def test_sharepoint_library_retention_folds_deleted_incrementals(db, monkeypatch):
    api, restored = _readers(db, monkeypatch)
    _, snaps = await _chain(db, models.ResourceType.SHAREPOINT_SITE, "FILE", "Site/lists/Documents")

    async def view(sid):
        async with db.factory() as session:
            body = await api.list_snapshot_files(snapshot_id=str(sid), page=1, size=500, search=None, db=session)
        return sorted(row.name if hasattr(row, "name") else row["name"] for row in body["content"])
    await _assert_folded(db, view, restored, snaps)
    assert await view(snaps[3]) == ["a-v1", "b-v2", "d-v3"]


async def test_calendar_retention_folds_deleted_incrementals(db, monkeypatch):
    api, restored = _readers(db, monkeypatch)
    _, snaps = await _chain(db, models.ResourceType.USER_CALENDAR, "CALENDAR_EVENT", "Calendar")

    async def view(sid):
        async with db.factory() as session:
            body = await api.list_snapshot_calendar(str(sid), page=1, size=1000, search=None, db=session)
        return sorted((e["externalId"], e["subject"]) for e in body["content"])
    await _assert_folded(db, view, restored, snaps, view_inc3=[("A", "a-v1"), ("B", "b-v2"), ("D", "d-v3")])


async def test_contacts_retention_folds_deleted_incrementals_and_keeps_deletions_hidden(db, monkeypatch):
    api, restored = _readers(db, monkeypatch)
    _, snaps = await _chain(db, models.ResourceType.USER_CONTACTS, "USER_CONTACT", "Contacts")

    async def view(sid):
        async with db.factory() as session:
            body = await api.list_snapshot_contacts(str(sid), page=1, size=500, folder=None, search=None, db=session)
        return sorted((c["externalId"], c["name"]) for c in body["content"])
    await _assert_folded(db, view, restored, snaps, view_inc3=[("A", "a-v1"), ("B", "b-v2"), ("D", "d-v3")])


async def test_copy_forward_rows_are_not_folded(db):
    """SharePoint list rows are re-read every run and the site view reads them from the point itself: a list row
    that only the doomed snapshot holds is gone from the source and must not reappear at the kept point."""
    rid = await db.resource(models.ResourceType.SHAREPOINT_SITE)
    old, _ = await db.snapshot(rid, OK, item_count=1)
    await db.item(old, "list-gone", "SHAREPOINT_LIST")
    await db.item(old, "m1", "EMAIL")
    kept, _ = await db.snapshot(rid, OK, item_count=1)
    await db.item(kept, "list-docs", "SHAREPOINT_LIST")
    assert await _retire(db, [old]) == 1
    async with db.factory() as session:
        rows = (await session.execute(text("SELECT external_id FROM snapshot_items WHERE snapshot_id = :s"),
                                      {"s": kept})).scalars().all()
    assert rows == ["list-docs"]


async def test_sparse_snapshot_without_a_later_kept_point_is_kept(db):
    rid = await db.resource(models.ResourceType.USER_ONEDRIVE)
    full, _ = await db.snapshot(rid, OK, item_count=1)
    await db.item(full, "B", "ONEDRIVE_FILE", blob="b/B")
    tail, _ = await db.snapshot(rid, OK, item_count=1)
    await db.item(tail, "A", "ONEDRIVE_FILE", blob="b/A")
    failed, _ = await db.snapshot(rid, models.SnapshotStatus.FAILED)
    assert await _retire(db, [tail]) == 0
    async with db.factory() as session:
        assert await session.scalar(text("SELECT count(*) FROM snapshot_items WHERE snapshot_id = :s"), {"s": tail}) == 1


async def test_folded_rows_keep_their_blobs_referenced(db):
    from shared.backup_cleanup import BlobCandidate, select_deletable_blob_paths
    _, snaps = await _chain(db, models.ResourceType.USER_ONEDRIVE, "ONEDRIVE_FILE", "/Documents")
    await _retire(db, snaps[1:3])
    async with db.factory() as session:
        plan = await select_deletable_blob_paths(session, doomed_snapshot_ids=snaps[1:3], candidates=[
            BlobCandidate(blob_path="b/A/a-v1", snapshot_id=snaps[1], tenant_id=db.tenant),
            BlobCandidate(blob_path="b/B/b-v2", snapshot_id=snaps[1], tenant_id=db.tenant)])
    assert not plan.deletable

