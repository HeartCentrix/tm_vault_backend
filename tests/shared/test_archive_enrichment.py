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

import base64  # noqa: E402
import hashlib  # noqa: E402


# ── plan_archive_attachments: pure decode+hash of fileAttachments ──────────
def test_plan_archive_attachments_decodes_only_file_attachments():
    pdf = b"%PDF-1.7 fake bytes"
    b64 = base64.b64encode(pdf).decode()
    atts = [
        {"id": "att1", "name": "inv.pdf", "contentType": "application/pdf",
         "contentBytes": b64, "isInline": False, "contentId": "cid1",
         "@odata.type": "#microsoft.graph.fileAttachment"},
        # itemAttachment (nested email) — no contentBytes → skipped
        {"id": "att2", "name": "fwd.eml", "@odata.type": "#microsoft.graph.itemAttachment"},
        # referenceAttachment (cloud link) — no bytes → skipped
        {"id": "att3", "name": "link", "@odata.type": "#microsoft.graph.referenceAttachment"},
    ]
    plans = eng.plan_archive_attachments(atts)
    assert len(plans) == 1
    p = plans[0]
    assert p["att_id"] == "att1"
    assert p["name"] == "inv.pdf"
    assert p["content_type"] == "application/pdf"
    assert p["data"] == pdf
    assert p["content_hash"] == hashlib.sha256(pdf).hexdigest()
    assert p["content_id"] == "cid1"


def test_plan_archive_attachments_empty_and_bad_input():
    assert eng.plan_archive_attachments(None) == []
    assert eng.plan_archive_attachments([]) == []
    assert eng.plan_archive_attachments([{"name": "x"}]) == []          # no contentBytes
    assert eng.plan_archive_attachments([{"contentBytes": "!!notb64"}]) == []  # undecodable


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

    def add(self, obj):
        self.store.setdefault("added", []).append(obj)

    async def execute(self, stmt):
        self.store.setdefault("executed", []).append(stmt)
        return None

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
async def test_enrich_uploads_attachment_bytes_and_creates_sibling_row():
    """With upload_attachment injected, a fileAttachment's bytes are uploaded and
    a sibling ARCHIVE_ATTACHMENT row is created (mail-style) — while the parent
    row's raw is still stripped of contentBytes."""
    import base64 as _b64
    import uuid as _uuid
    pdf = b"%PDF-1.7 hello"
    r1 = _Row("id-1", "MSG1", {})
    db1 = _Row("id-1", "MSG1", {})
    db1.snapshot_id = _uuid.uuid4()
    db1.tenant_id = _uuid.uuid4()
    db1.backend_id = _uuid.uuid4()
    db1.folder_path = "Archive"
    store = {"db_rows": {"id-1": db1}, "commits": 0}

    staged_msg = {
        "id": "scratch-1", "subject": "Invoice", "hasAttachments": True,
        "attachments": [{"id": "att-9", "name": "inv.pdf", "contentType": "application/pdf",
                         "contentBytes": _b64.b64encode(pdf).decode()}],
    }
    uploads = []

    async def _fake_upload(tenant_id, content_hash, container, blob_path, data):
        uploads.append({"container": container, "blob_path": blob_path, "data": data, "hash": content_hash})

    res = await eng.enrich_resource(
        session_factory=_session_factory(store),
        resource=_Resource("res-1", {"user_id": "u", "primary_mailbox_id": "p"}),
        gc=object(), download_blob=None, upload_attachment=_fake_upload,
        _load_pending=lambda s, rid: _async_return([r1]),
        _stager_factory=lambda **kw: _FakeStager([(r1, staged_msg)]),
    )

    assert res == {"enriched": 1, "pending": 1}
    # exactly one attachment uploaded, bytes intact
    assert len(uploads) == 1 and uploads[0]["data"] == pdf
    assert "archive" in uploads[0]["container"]
    # exactly one sibling ARCHIVE_ATTACHMENT row added, keyed to the parent
    added = store.get("added", [])
    assert len(added) == 1
    sib = added[0]
    assert sib.item_type == "ARCHIVE_ATTACHMENT"
    assert sib.external_id == "MSG1::att-9"
    assert sib.extra_data["parent_item_id"] == "MSG1"
    assert sib.blob_path == uploads[0]["blob_path"]
    assert sib.content_size == len(pdf)
    # The blob KEY's final segment must be short + content-hashed (att_<hash40>),
    # NOT the raw external_id::att_id — Graph ids are ~150 chars each, so the
    # concatenation blew past SeaweedFS's 255-byte filer segment limit and every
    # PutObject failed with InternalError. Mirror the mail worker's key format.
    key = uploads[0]["blob_path"].rsplit("/", 1)[-1]
    assert key == f"att_{hashlib.sha256(pdf).hexdigest()[:40]}"
    assert len(key) < 60 and "::" not in key
    # parent row still stripped
    assert "contentBytes" not in db1.extra_data["raw"]["attachments"][0]


@pytest.mark.asyncio
async def test_enrich_stores_dedup_resolved_blob_path():
    """The uploader is dedup-aware: on a dedup_hit it writes nothing new and
    returns the EXISTING blob's path (which differs from the one we built). The
    sibling row MUST store the RETURNED path — else it points at a key that was
    never written and /content returns source:none (the '/./' legacy-path bug).
    Mirrors EMAIL_ATTACHMENT (workers/backup-worker/main.py:9328)."""
    import base64 as _b64
    import uuid as _uuid
    pdf = b"%PDF-1.7 hello"
    r1 = _Row("id-1", "MSG1", {})
    db1 = _Row("id-1", "MSG1", {})
    db1.snapshot_id = _uuid.uuid4()
    db1.tenant_id = _uuid.uuid4()
    db1.backend_id = _uuid.uuid4()
    db1.folder_path = "Archive"
    store = {"db_rows": {"id-1": db1}, "commits": 0}
    staged_msg = {
        "id": "scratch-1", "subject": "Invoice", "hasAttachments": True,
        "attachments": [{"id": "att-9", "name": "inv.pdf", "contentType": "application/pdf",
                         "contentBytes": _b64.b64encode(pdf).decode()}],
    }
    DEDUP_PATH = "tenant/res/snap/existing-blob-key"

    async def _dedup_upload(tenant_id, content_hash, container, blob_path, data):
        assert blob_path != DEDUP_PATH  # we constructed a different path
        return {"success": True, "blob_path": DEDUP_PATH, "method": "dedup_hit"}

    res = await eng.enrich_resource(
        session_factory=_session_factory(store),
        resource=_Resource("res-1", {"user_id": "u", "primary_mailbox_id": "p"}),
        gc=object(), download_blob=None, upload_attachment=_dedup_upload,
        _load_pending=lambda s, rid: _async_return([r1]),
        _stager_factory=lambda **kw: _FakeStager([(r1, staged_msg)]),
    )
    assert res == {"enriched": 1, "pending": 1}
    sib = store["added"][0]
    assert sib.blob_path == DEDUP_PATH  # stored the path the bytes ACTUALLY live at


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


# ── SQL builders compile to valid Postgres (no DB) ───────────────────────
# extra_data is a generic JSON column, not JSONB — `.astext` doesn't exist on
# it, so the bulk query MUST use `.as_string()` (emits ->>). These guard that
# both statements compile and the enrich-status filter extracts text (not the
# quoted-JSON `->`, which would never equal 'enriched').
def test_find_pending_stmt_compiles_and_extracts_text_not_quoted_json():
    from sqlalchemy.dialects import postgresql
    sql = str(eng.find_pending_archive_resource_ids_stmt(50).compile(
        dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
    assert "->>" in sql                  # text extraction (quote-stripped)
    assert "metadata -> 'enrich_status'" not in sql  # NOT the quoted-JSON form
    assert "ARCHIVE_ITEM" in sql


def test_pending_rows_stmt_compiles_distinct_on_external_id():
    from sqlalchemy.dialects import postgresql
    sql = str(eng.pending_archive_rows_stmt("res-1").compile(dialect=postgresql.dialect()))
    assert "DISTINCT ON" in sql.upper()
    assert "external_id" in sql


# ── tiny async helper (avoid importing asyncio just for a coroutine) ──────
async def _async_return(v):
    return v
