"""DI enrichment engine (shared/archive_enrichment.py).

enrich_resource orchestrates: load the archive rows still needing enrichment →
resolve the user's PRIMARY mailbox → stage each opaque blob through the temp
folder → write the previewable Graph JSON back onto the source row. It's the
single per-resource unit reused by BOTH the on-demand consumer (user opened
their archive) and the bulk-parallel loop (background sweep).

Impure edges (row load, staging, DB write, mailbox resolution) are injected so
the orchestration is unit-tested with fakes. The two SQL helpers
(pending_archive_rows / find_pending_archive_resource_ids) are DB-bound and
proven by the live round-trip, not here."""
from __future__ import annotations

import pathlib
import sys

import pytest

_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from shared import archive_enrichment as eng  # noqa: E402
from shared import archive_enrich as ae  # noqa: E402


# ── fakes ────────────────────────────────────────────────────────────────
class _Row:
    def __init__(self, rid, external_id, extra_data=None):
        self.id = rid
        self.external_id = external_id
        self.extra_data = extra_data or {}
        self.item_type = "ARCHIVE_ITEM"
        self.name = None


class _Resource:
    def __init__(self, rid, extra_data):
        self.id = rid
        self.extra_data = extra_data
        self.tenant_id = "tenant-1"
        self.display_name = "Online Archive — X"


class _Session:
    """Doubles as load-session (has expunge_all) and write-session (get/commit)."""
    def __init__(self, store):
        self.store = store

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    def expunge_all(self):
        pass

    async def get(self, model, rid):
        return self.store["db_rows"].get(rid)

    async def commit(self):
        self.store["commits"] += 1


class _FakeStager:
    """Yields (source_row, graph_json) pairs — stands in for the real staging."""
    def __init__(self, pairs):
        self._pairs = pairs

    async def stage_and_map(self, items):
        for p in self._pairs:
            yield p


def _session_factory(store):
    return lambda: _Session(store)


async def _resolve_none(gc, user_id):
    return None


# ── enrich_resource ──────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_returns_zero_and_builds_no_stager_when_nothing_pending():
    """All rows already enriched → no staging, no mailbox resolution, count 0."""
    rows = [_Row("r1", "A", {"enrich_status": ae.ENRICHED})]
    built = {"stager": False}

    def _stager_factory(**kw):
        built["stager"] = True
        return _FakeStager([])

    res = await eng.enrich_resource(
        session_factory=_session_factory({"db_rows": {}, "commits": 0}),
        resource=_Resource("res-1", {"user_id": "u", "primary_mailbox_id": "p"}),
        gc=object(), download_blob=None,
        _load_pending=lambda s, rid: _async_return(rows),
        _stager_factory=_stager_factory,
    )
    assert res["enriched"] == 0 and res["pending"] == 0
    assert built["stager"] is False  # never touched the mailbox


@pytest.mark.asyncio
async def test_skips_gracefully_when_no_primary_mailbox():
    """Pending rows exist but the user's primary mailbox can't be resolved
    (no archive/permission) → skip, do NOT fail, build no stager."""
    rows = [_Row("r1", "A", {})]
    built = {"stager": False}

    def _stager_factory(**kw):
        built["stager"] = True
        return _FakeStager([])

    res = await eng.enrich_resource(
        session_factory=_session_factory({"db_rows": {}, "commits": 0}),
        resource=_Resource("res-1", {"user_id": "u"}),  # no primary_mailbox_id
        gc=object(), download_blob=None,
        resolve_primary_mailbox_id=_resolve_none,       # resolution fails
        _load_pending=lambda s, rid: _async_return(rows),
        _stager_factory=_stager_factory,
    )
    assert res["enriched"] == 0
    assert res["pending"] == 1
    assert res.get("skipped") == "no_primary_mailbox"
    assert built["stager"] is False


@pytest.mark.asyncio
async def test_enriches_pending_strips_attachment_bytes_and_persists():
    """The happy path: pending rows staged, contentBytes stripped from stored
    raw, name set to the subject, extra_data patched, count returned."""
    r1 = _Row("id-1", "A", {})
    db1 = _Row("id-1", "A", {})  # the row the write-session loads back
    store = {"db_rows": {"id-1": db1}, "commits": 0}

    staged_msg = {
        "id": "scratch-1",
        "subject": "Q3 Invoice",
        "hasAttachments": True,
        "attachments": [{"name": "inv.pdf", "contentBytes": "BIGBASE64=="}],
    }

    res = await eng.enrich_resource(
        session_factory=_session_factory(store),
        resource=_Resource("res-1", {"user_id": "u", "primary_mailbox_id": "p"}),
        gc=object(), download_blob=None,
        _load_pending=lambda s, rid: _async_return([r1]),
        _stager_factory=lambda **kw: _FakeStager([(r1, staged_msg)]),
    )

    assert res == {"enriched": 1, "pending": 1}
    assert store["commits"] == 1
    assert db1.name == "Q3 Invoice"
    assert db1.extra_data["enrich_status"] == ae.ENRICHED
    assert db1.extra_data["subject"] == "Q3 Invoice"
    assert db1.extra_data["has_attachments"] is True
    # attachment metadata kept, but the heavy bytes stripped from the stored raw
    stored_att = db1.extra_data["raw"]["attachments"][0]
    assert stored_att["name"] == "inv.pdf"
    assert "contentBytes" not in stored_att


@pytest.mark.asyncio
async def test_only_rows_needing_enrichment_are_staged():
    """Mixed batch: enrich_resource passes ONLY the un-enriched rows to staging."""
    fresh = _Row("id-1", "A", {})
    done = _Row("id-2", "B", {"enrich_status": ae.ENRICHED})
    seen = {}

    def _stager_factory(**kw):
        class _S:
            async def stage_and_map(self, items):
                seen["items"] = list(items)
                if False:
                    yield  # empty async-gen
        return _S()

    res = await eng.enrich_resource(
        session_factory=_session_factory({"db_rows": {}, "commits": 0}),
        resource=_Resource("res-1", {"user_id": "u", "primary_mailbox_id": "p"}),
        gc=object(), download_blob=None,
        _load_pending=lambda s, rid: _async_return([fresh, done]),
        _stager_factory=_stager_factory,
    )
    assert res["pending"] == 1
    assert [r.external_id for r in seen["items"]] == ["A"]  # 'done' excluded


# ── tiny async helper (avoid importing asyncio just for a coroutine) ──────
async def _async_return(v):
    return v
