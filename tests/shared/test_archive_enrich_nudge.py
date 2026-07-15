"""nudge_enrichment (shared/archive_enrichment.py) — the SINGLE on-demand
enrichment nudge shared by browse (snapshot-service) and export (restore-worker).

Given a page/selection of SnapshotItems, publish ``enrich.priority`` exactly once
per DISTINCT resource that still has un-enriched ARCHIVE_ITEM rows, so the rows an
EML/MBOX/PST export would otherwise silently skip actually get enriched and a
re-export succeeds. Read-only over the DB, idempotent, and MUST never raise — a
nudge failure must not break a browse or an export. DI-shaped: the publisher is
injected so this is unit-tested with fakes (no message bus, no giant worker
module import)."""
from __future__ import annotations

import pathlib
import sys

import pytest

_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from shared import archive_enrichment as eng  # noqa: E402


class _Item:
    def __init__(self, item_type, snapshot_id, extra_data=None):
        self.item_type = item_type
        self.snapshot_id = snapshot_id
        self.extra_data = extra_data or {}


class _Snap:
    def __init__(self, resource_id):
        self.resource_id = resource_id


class _FakeSession:
    """Resolves ``get(Snapshot, snapshot_id) -> _Snap(resource_id)`` from a map."""

    def __init__(self, snap_to_resource):
        self._map = snap_to_resource

    async def get(self, _model, sid):
        rid = self._map.get(sid)
        return _Snap(rid) if rid is not None else None


class _RecordingPublish:
    def __init__(self, raises=False):
        self.calls = []
        self._raises = raises

    async def __call__(self, routing_key, message, priority=5):
        self.calls.append((routing_key, message, priority))
        if self._raises:
            raise RuntimeError("broker down")


@pytest.mark.asyncio
async def test_nudges_once_per_distinct_unenriched_resource():
    session = _FakeSession({"s1": "rA", "s2": "rB"})
    items = [
        _Item("ARCHIVE_ITEM", "s1", {}),                        # rA, pending
        _Item("ARCHIVE_ITEM", "s2", {"enrich_status": "failed"}),  # rB, retry
    ]
    pub = _RecordingPublish()
    nudged = await eng.nudge_enrichment(session, items, pub)

    assert nudged == ["rA", "rB"]
    assert len(pub.calls) == 2
    assert {c[1]["resourceId"] for c in pub.calls} == {"rA", "rB"}


@pytest.mark.asyncio
async def test_skips_when_all_enriched_or_processing():
    session = _FakeSession({"s1": "rA", "s2": "rB"})
    items = [
        _Item("ARCHIVE_ITEM", "s1", {"enrich_status": "enriched"}),
        _Item("ARCHIVE_ITEM", "s2", {"enrich_status": "processing"}),
    ]
    pub = _RecordingPublish()
    nudged = await eng.nudge_enrichment(session, items, pub)

    assert nudged == []
    assert pub.calls == []


@pytest.mark.asyncio
async def test_ignores_non_archive_items():
    session = _FakeSession({"s1": "rA"})
    items = [
        _Item("EMAIL", "s1", {}),
        _Item("CALENDAR_EVENT", "s1", {}),
    ]
    pub = _RecordingPublish()
    nudged = await eng.nudge_enrichment(session, items, pub)

    assert nudged == []
    assert pub.calls == []


@pytest.mark.asyncio
async def test_dedupes_by_resource():
    # two un-enriched archive rows in the SAME snapshot → ONE nudge for that resource
    session = _FakeSession({"s1": "rA"})
    items = [
        _Item("ARCHIVE_ITEM", "s1", {}),
        _Item("ARCHIVE_ITEM", "s1", {"enrich_status": "failed"}),
    ]
    pub = _RecordingPublish()
    nudged = await eng.nudge_enrichment(session, items, pub)

    assert nudged == ["rA"]
    assert len(pub.calls) == 1


@pytest.mark.asyncio
async def test_body_shape_and_priority():
    session = _FakeSession({"s1": "rA"})
    items = [_Item("ARCHIVE_ITEM", "s1", {})]
    pub = _RecordingPublish()
    await eng.nudge_enrichment(session, items, pub)

    routing_key, message, priority = pub.calls[0]
    assert routing_key == "enrich.priority"
    assert message == {"resourceId": "rA", "restoreType": "ARCHIVE_ENRICH"}
    assert priority == 9


@pytest.mark.asyncio
async def test_never_raises_when_publish_throws():
    session = _FakeSession({"s1": "rA"})
    items = [_Item("ARCHIVE_ITEM", "s1", {})]
    pub = _RecordingPublish(raises=True)
    # must swallow the broker error — a nudge must never break a browse/export
    nudged = await eng.nudge_enrichment(session, items, pub)

    assert nudged == []  # nothing confirmed nudged
    assert len(pub.calls) == 1  # but the attempt was made


# ── nudge_unenriched_export: the EXPORT bug-locus, made testable (seam 2) ──
@pytest.mark.asyncio
async def test_export_nudges_and_logs_when_unenriched_present():
    session = _FakeSession({"s1": "rA"})
    items = [
        _Item("ARCHIVE_ITEM", "s1", {"enrich_status": "enriched", "raw": {"subject": "hi"}}),
        _Item("ARCHIVE_ITEM", "s1", {}),   # un-enriched (no raw) -> would be SKIPPED by export
        _Item("EMAIL", "s1", {}),
    ]
    pub = _RecordingPublish()
    logged = []
    nudged = await eng.nudge_unenriched_export(
        items, session, pub, log=lambda skipped, total: logged.append((skipped, total)))

    assert nudged == ["rA"]              # nudged the resource so a re-export can succeed
    assert logged == [(1, 2)]           # 1 of 2 archive rows un-enriched
    assert len(pub.calls) == 1


@pytest.mark.asyncio
async def test_export_does_nothing_when_all_archive_enriched():
    session = _FakeSession({"s1": "rA"})
    items = [_Item("ARCHIVE_ITEM", "s1", {"enrich_status": "enriched", "raw": {"subject": "x"}})]
    pub = _RecordingPublish()
    logged = []
    nudged = await eng.nudge_unenriched_export(items, session, pub, log=lambda s, t: logged.append((s, t)))

    assert nudged == []
    assert logged == []       # nothing skipped -> no skip message
    assert pub.calls == []


@pytest.mark.asyncio
async def test_export_ignores_selections_with_no_archive():
    session = _FakeSession({"s1": "rA"})
    items = [_Item("EMAIL", "s1", {}), _Item("EMAIL_ATTACHMENT", "s1", {})]
    pub = _RecordingPublish()
    nudged = await eng.nudge_unenriched_export(items, session, pub)

    assert nudged == []
    assert pub.calls == []
