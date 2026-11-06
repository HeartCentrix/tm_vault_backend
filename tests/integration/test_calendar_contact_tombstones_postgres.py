"""A calendar event or contact deleted in M365 must disappear from the newest restore point.

Calendar and contacts snapshots are sparse: Recovery and restore union every restore point up to the selected
one, newest row per external_id winning. A deletion therefore has to be stored as an is_deleted row (no blob)
with the item's external_id, and every reader must dedup newest-wins FIRST and drop is_deleted rows AFTER.

Railway 2026-11-06: deleting one occurrence of "Daily Sync-ups" reached the delta as the re-expanded series
(131 occurrences, 0 is_deleted rows), and single-event / contact deletes were stored as ordinary rows."""
import pathlib
import re

import httpx
import pytest
from sqlalchemy import text

from tests.integration.test_mail_overlay_postgres import db, _load  # noqa: F401
from shared import models

pytestmark = pytest.mark.integration
OK = models.SnapshotStatus.COMPLETED


async def _tomb(db, sid, ext, item_type):
    await db.item(sid, ext, item_type, folder="Calendar/Calendar", size=0, metadata={"raw": {"id": ext, "@removed": {}}})
    async with db.factory.begin() as session:
        await session.execute(text("UPDATE snapshot_items SET is_deleted = true WHERE snapshot_id = :s "
                                   "AND external_id = :e"), {"s": sid, "e": ext})


def _ev(ext, **raw):
    return {"raw": {"id": ext, "subject": ext, **raw}}


def _readers(monkeypatch, db):
    api = _load("caltomb_api", "services/snapshot-service/main.py")
    monkeypatch.setattr(api, "_count_session_factory", db.factory)
    restore = _load("caltomb_restore", "workers/restore-worker/main.py")
    return api, restore.RestoreWorker.__new__(restore.RestoreWorker)


async def _restored(db, worker, sid):
    sid = str(sid)
    async with db.factory() as session:
        fetched = sorted(i.external_id for i in await worker.fetch_snapshot_items(session, [sid], []))
        counted = await worker.count_snapshot_items(session, [sid], [])
    streamed = sorted([i.external_id async for b in worker.stream_snapshot_items_by_group(db.factory, [sid], [])
                       for i in b])
    assert fetched == streamed and counted == len(fetched)
    return fetched


async def test_calendar_deletions_hide_events_from_their_deletion_on(db, monkeypatch):
    api, worker = _readers(monkeypatch, db)
    rid = await db.resource(models.ResourceType.USER_CALENDAR)
    old, _ = await db.snapshot(rid, OK, item_count=1)
    await db.item(old, "M", "CALENDAR_EVENT", folder="Calendar/Calendar", metadata=_ev("M", type="seriesMaster"))
    for ext in ("o1", "o2"):
        await db.item(old, ext, "CALENDAR_EVENT", folder="Calendar/Calendar",
                      metadata=_ev(ext, type="occurrence", seriesMasterId="M"))
    for ext in ("ev1", "ev2"):
        await db.item(old, ext, "CALENDAR_EVENT", folder="Calendar/Calendar", metadata=_ev(ext))
    mid, _ = await db.snapshot(rid, OK, item_count=1)
    await _tomb(db, mid, "ev1", "CALENDAR_EVENT")          # single event deleted
    await _tomb(db, mid, "o1", "CALENDAR_EVENT")           # one occurrence of the series deleted
    new, _ = await db.snapshot(rid, OK, item_count=1)
    await _tomb(db, new, "M", "CALENDAR_EVENT")            # whole series deleted
    await _tomb(db, new, "o2", "CALENDAR_EVENT")
    await db.item(new, "ev1", "CALENDAR_EVENT", folder="Calendar/Calendar", metadata=_ev("ev1"))  # re-created

    async def view(sid):
        async with db.factory() as session:
            body = await api.list_snapshot_calendar(str(sid), page=1, size=1000, search=None, db=session)
        return sorted(e["externalId"] for e in body["content"])
    expected = {old: ["M", "ev1", "ev2", "o1", "o2"], mid: ["M", "ev2", "o2"], new: ["ev1", "ev2"]}
    assert {s: (await view(s), await _restored(db, worker, s)) for s in expected} == \
        {s: (ids, ids) for s, ids in expected.items()}


async def test_contact_deletion_hides_the_contact_from_its_deletion_on(db, monkeypatch):
    api, worker = _readers(monkeypatch, db)
    rid = await db.resource(models.ResourceType.USER_CONTACTS)
    old, _ = await db.snapshot(rid, OK, item_count=1)
    for ext in ("c1", "c2"):
        await db.item(old, ext, "USER_CONTACT", folder="Contacts", metadata={"raw": {"id": ext, "displayName": ext}})
    new, _ = await db.snapshot(rid, OK, item_count=1)
    await _tomb(db, new, "c1", "USER_CONTACT")

    async def view(sid):
        async with db.factory() as session:
            body = await api.list_snapshot_contacts(str(sid), page=1, size=500, folder=None, search=None, db=session)
        return sorted(c["externalId"] for c in body["content"]), body["totalElements"]
    assert (await view(old), await _restored(db, worker, old)) == ((["c1", "c2"], 2), ["c1", "c2"])
    assert (await view(new), await _restored(db, worker, new)) == ((["c2"], 1), ["c2"])


def test_delta_deletions_are_stored_as_is_deleted_rows_unless_switched_off(monkeypatch):
    bw = _load("caltomb_bw", "workers/backup-worker/main.py")
    removed_event = {"raw": {"id": "e", "@removed": {"reason": "deleted"}}, "removed": True, "is_deleted": True}
    removed_contact = {"raw": {"id": "c", "@removed": True}}
    assert bw._records_deletion("CALENDAR_EVENT", removed_event)
    assert bw._records_deletion("USER_CONTACT", removed_contact)
    assert not bw._records_deletion("CALENDAR_EVENT", _ev("e"))
    assert not bw._records_deletion("USER_CONTACT", {"raw": {"id": "c"}})
    assert not bw._records_deletion("EMAIL", {"raw": {"id": "m", "@removed": {}}})
    monkeypatch.setenv("CALENDAR_RECORD_DELETIONS", "false")
    monkeypatch.setenv("CONTACTS_RECORD_DELETIONS", "0")
    assert not bw._records_deletion("CALENDAR_EVENT", removed_event)
    assert not bw._records_deletion("USER_CONTACT", removed_contact)
    # the content persist step stamps the column from it
    flat = re.sub(r"\s+", " ", (pathlib.Path(__file__).parents[2] / "workers/backup-worker/main.py").read_text())
    assert '"is_deleted": _records_deletion(item_type, extra),' in flat


class _Graph:
    GRAPH_URL = "https://graph.microsoft.com/v1.0"

    def __init__(self, instances):
        self.instances, self.urls = instances, []

    async def _iter_pages(self, url, params=None):
        self.urls.append(url)
        series = url.split("/events/")[1].split("/instances")[0]
        if series not in self.instances:
            raise httpx.HTTPStatusError("404", request=httpx.Request("GET", url),
                                        response=httpx.Response(404, request=httpx.Request("GET", url)))
        yield {"value": [{"id": i} for i in self.instances[series]]}


async def test_series_change_and_series_delete_tombstone_the_occurrences_graph_no_longer_has(db, monkeypatch):
    bw = _load("caltomb_bw", "workers/backup-worker/main.py")
    monkeypatch.setattr(bw, "async_session_factory", db.factory)
    rid = await db.resource(models.ResourceType.USER_CALENDAR)
    old, _ = await db.snapshot(rid, OK, item_count=1)

    def occ(ext, series, day):
        return _ev(ext, type="occurrence", seriesMasterId=series, start={"dateTime": f"{day}T09:00:00.0000000"})
    await db.item(old, "M", "CALENDAR_EVENT", folder="Calendar/Calendar", metadata=_ev("M", type="seriesMaster"))
    for ext, day in (("m1", "2026-11-02"), ("m2", "2026-11-03"), ("m3", "2026-11-04"), ("m-old", "2020-01-01"),
                     ("m-gone", "2026-11-05")):
        await db.item(old, ext, "CALENDAR_EVENT", folder="Calendar/Calendar", metadata=occ(ext, "M", day))
    await db.item(old, "N", "CALENDAR_EVENT", folder="Calendar/Calendar", metadata=_ev("N", type="seriesMaster"))
    for ext, day in (("n1", "2026-11-02"), ("n2", "2026-11-03")):
        await db.item(old, ext, "CALENDAR_EVENT", folder="Calendar/Calendar", metadata=occ(ext, "N", day))
    await db.item(old, "solo", "CALENDAR_EVENT", folder="Calendar/Calendar", metadata=_ev("solo"))
    earlier, _ = await db.snapshot(rid, OK, item_count=1)
    await _tomb(db, earlier, "m-gone", "CALENDAR_EVENT")    # already deleted: not tombstoned again
    cur, _ = await db.snapshot(rid)

    # This run: series M re-expanded without m1 (one occurrence deleted, no @removed); series N deleted, Graph
    # reported only n1 as @removed. M's instances omit m1 and the out-of-window m-old; N's master is gone (404).
    def row(ext, extra):
        return ("CALENDAR_EVENT", ext, ext, extra, "Calendar/Calendar")
    run = [row("m2", {**occ("m2", "M", "2026-11-03"), "seriesMasterId": "M"}),
           row("m3", {**occ("m3", "M", "2026-11-04"), "seriesMasterId": "M"}),
           row("n1", {"raw": {"id": "n1", "@removed": {"reason": "deleted"}}, "removed": True, "is_deleted": True})]
    graph = _Graph({"M": ["m2", "m3"]})
    tombs = await bw._calendar_series_tombstones(graph, "user-1", rid, cur, run,
                                                 "2024-11-06T00:00:00Z", "2027-11-06T00:00:00Z")
    assert sorted(t[2] for t in tombs) == ["N", "m1", "n2"]
    assert all(bw._records_deletion(t[0], t[3]) and t[0] == "CALENDAR_EVENT" for t in tombs)
    assert sorted(u.split("/events/")[1].split("/")[0] for u in graph.urls) == ["M", "N"]
    # a Graph failure other than 404 tombstones nothing for that series
    graph = _Graph({})
    graph._iter_pages = _boom
    assert await bw._calendar_series_tombstones(graph, "user-1", rid, cur, run,
                                                "2024-11-06T00:00:00Z", "2027-11-06T00:00:00Z") == []


async def _boom(url, params=None):
    raise httpx.HTTPStatusError("503", request=httpx.Request("GET", url),
                                response=httpx.Response(503, request=httpx.Request("GET", url)))
    yield {}
