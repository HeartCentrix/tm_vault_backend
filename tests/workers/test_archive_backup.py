"""Task 2.2 — USER_ARCHIVE backup handler (archive_backup.run_archive_backup).

Exchange Online in-place archive backup via the Graph Mailbox Import/Export
APIs. Wire shapes match the official MS beta docs ("Handle archive mailbox
redirects", 2026-07-24): enumeration redirects are HTTP 308 auto-followed by
the shared httpx client, and exportItems returns **200 OK** with per-item
success (``data``) or per-item ``Error`` (capital-E) entries — the
``ErrorArchiveFolderMovedPermanently`` message carries the reissue URL.

Graph is fully mocked; DB/storage collaborators (``load_prev`` /
``persist_batch`` / ``carry_forward``) are injected. The default collaborators
are exercised directly against a stub AsyncSession + stub storage (the repo's
established DB-test convention — models are Postgres-only, there is no
SQLite/real-DB fixture), so their raw SQL + SnapshotItem construction actually
executes rather than being faked.

Coverage:
  (a) changeKey diff  — unchanged excluded, changed + new included
  (b) batching        — 45 ids ⇒ exportItems called 3× with 20/20/5
  (c) error mid-walk  — Graph transport error ⇒ EnumerationFailed, no finalize
  (d) empty archive   — 0 items + 0 errors ⇒ legitimate COMPLETED-empty
  (e) redirect        — 200/per-item-Error ⇒ reissued to URL + items stored
  (f) mixed batch     — 1 success + 1 redirect ⇒ both stored
  (g) redirect loop   — past hop cap ⇒ item counted failed ⇒ PARTIAL
  (h) partial upload  — failed blob upload ⇒ files_failed ⇒ PARTIAL
  (i) default collaborators execute (persist / load_prev / carry_forward)
"""
from __future__ import annotations

import asyncio
import importlib.util
import os
import pathlib
import sys
import types

import httpx
import pytest

# Env for the (i) tests that import real shared.database / shared.models.
os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://u:p@localhost/db")


_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
_MOD_PATH = _REPO_ROOT / "workers" / "backup-worker" / "archive_backup.py"
_spec = importlib.util.spec_from_file_location("archive_backup_under_test", _MOD_PATH)
ab = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["archive_backup_under_test"] = ab
assert _spec and _spec.loader
_spec.loader.exec_module(ab)  # type: ignore[union-attr]


# ─────────────────────────── fakes / fixtures ────────────────────────────

def _settings():
    return types.SimpleNamespace(
        ARCHIVE_GRAPH_VERSION="beta",
        ARCHIVE_IMPORT_TIMEOUT_S=120,
        ARCHIVE_FOLDER_CONCURRENCY=8,
        ARCHIVE_EXPORT_CONCURRENCY=8,
    )


def _resource():
    return types.SimpleNamespace(
        id="11111111-1111-1111-1111-111111111111",
        external_id="AAMkArchiveMbx",
        display_name="jrue archive",
        extra_data={"archiveMailboxId": "AAMkArchiveMbx"},
    )


def _snapshot():
    return types.SimpleNamespace(
        id="22222222-2222-2222-2222-222222222222",
        delta_tokens_json=None,
    )


def _tenant():
    return types.SimpleNamespace(id="33333333-3333-3333-3333-333333333333")


def _http_error(status: int):
    req = httpx.Request("GET", "https://graph.microsoft.com/beta/x")
    resp = httpx.Response(status, request=req)
    return httpx.HTTPStatusError(f"HTTP {status}", request=req, response=resp)


def _ok_entry(iid):
    return {"itemId": iid, "changeKey": f"ck-{iid}", "data": f"blob-{iid}"}


def _redirect_entry(iid, url):
    return {"itemId": iid, "Error": {
        "code": "ErrorArchiveFolderMovedPermanently", "message": url,
    }}


class FakeGraph:
    """URL-routed stand-in for GraphClient exposing async ``_get`` /
    ``_post_json``. ``folders`` maps folder_id → {"items":[...], "children":[ids]}.
    ``export_behavior(url, ids) -> value_list`` overrides exportItems responses;
    the default succeeds every id."""

    def __init__(self, folders, top_ids, *, export_behavior=None):
        self.folders = folders
        self.top_ids = top_ids
        self.export_behavior = export_behavior
        self.post_calls = []          # list[(url, [ids])]
        self.get_error = None         # httpx error to raise on the items call

    async def _get(self, url, params=None, on_activity=None):
        if url.endswith("/folders"):
            return {"value": [self._folder_obj(fid) for fid in self.top_ids]}
        if url.endswith("/childFolders"):
            fid = url.split("/folders/")[1].split("/childFolders")[0]
            kids = self.folders[fid].get("children", [])
            return {"value": [self._folder_obj(k) for k in kids]}
        if url.endswith("/items"):
            fid = url.split("/folders/")[1].split("/items")[0]
            if self.get_error is not None:
                raise self.get_error
            return {"value": list(self.folders[fid].get("items", []))}
        raise AssertionError(f"unexpected GET {url}")

    def _folder_obj(self, fid):
        return {
            "id": fid,
            "displayName": fid,
            "childFolderCount": len(self.folders[fid].get("children", [])),
        }

    async def _post_json(self, url, body, timeout=None):
        ids = list(body["itemIds"])
        self.post_calls.append((url, ids))
        if self.export_behavior is not None:
            return {"value": self.export_behavior(url, ids)}
        return {"value": [_ok_entry(i) for i in ids]}


def _run(coro):
    # Self-contained loop per call — NOT asyncio.get_event_loop(), which raises
    # "no current event loop" on Python 3.12 once an earlier pytest-asyncio
    # test (e.g. tests/shared/test_tier2_archive_child.py) has closed its loop
    # and left MainThread with none. We create and close our own loop and never
    # touch the global current-loop pointer, so ordering can't affect us.
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


class _RecordingPersist:
    def __init__(self, *, fail_ids=None):
        self.calls = 0
        self.stored_ids = []
        self.fail_ids = set(fail_ids or [])

    async def __call__(self, success_pairs):
        self.calls += 1
        stored = 0
        failed = 0
        total_bytes = 0
        for it, entry in success_pairs:
            iid = it["id"]
            if iid in self.fail_ids:          # simulate a failed blob upload
                failed += 1
                continue
            self.stored_ids.append(iid)
            stored += 1
            total_bytes += len(entry["data"])
        return stored, total_bytes, failed


async def _no_prev(_rid, _csid):
    return None, {}


async def _no_carry(_tsid, _psid, _ids):
    return 0


def _drive(graph, *, persist=None, load_prev=None, carry=None, snapshot=None):
    return _run(ab.run_archive_backup(
        graph, _resource(), snapshot or _snapshot(), _tenant(),
        settings=_settings(),
        load_prev=load_prev or _no_prev,
        persist_batch=persist or _RecordingPersist(),
        carry_forward=carry or _no_carry,
    ))


# ─────────────────────────────── tests ───────────────────────────────────

def test_a_changekey_diff_selects_changed_and_new_only():
    items = [
        {"id": "a", "changeKey": "1"},   # unchanged
        {"id": "b", "changeKey": "9"},   # changed (prev was 2)
        {"id": "c", "changeKey": "3"},   # new (not in prev)
    ]
    prev = {"a": "1", "b": "2"}
    selected = ab._select_items_to_export(items, prev)
    assert {s["id"] for s in selected} == {"b", "c"}


def test_b_batching_45_ids_three_calls_20_20_5():
    items = [{"id": f"i{n}", "changeKey": f"ck{n}"} for n in range(45)]
    graph = FakeGraph({"F": {"items": items}}, ["F"])
    persist = _RecordingPersist()
    result = _drive(graph, persist=persist)
    sizes = [len(ids) for (_u, ids) in graph.post_calls]
    assert sorted(sizes) == [5, 20, 20]
    assert len(graph.post_calls) == 3
    assert result["item_count"] == 45
    assert result["files_failed"] == 0


def test_c_graph_error_midwalk_raises_enumeration_failed_no_finalize():
    items = [{"id": "i1", "changeKey": "ck1"}]
    graph = FakeGraph({"F": {"items": items}}, ["F"])
    graph.get_error = _http_error(503)
    persist = _RecordingPersist()
    with pytest.raises(ab.EnumerationFailed):
        _drive(graph, persist=persist)
    assert persist.calls == 0
    assert graph.post_calls == []


def test_d_empty_archive_is_legit_completed_empty():
    graph = FakeGraph({}, [])
    persist = _RecordingPersist()
    snap = _snapshot()
    result = _drive(graph, persist=persist, snapshot=snap)
    assert result["item_count"] == 0
    assert result["files_failed"] == 0
    assert persist.calls == 0
    # No failures ⇒ no files_failed key ⇒ complete_snapshot finalizes
    # COMPLETED. Coverage numbers ARE stamped on every run since ITO-10146
    # (a clean-looking run with a shrunken enumeration is exactly the case
    # the operator needs numbers for) — they must not imply PARTIAL.
    dt = snap.delta_tokens_json or {}
    assert "files_failed" not in dt
    assert dt.get("items_enumerated") == 0


def test_j1_duplicate_enumerated_ids_deduped_before_export():
    """$skip drift on live folders re-returns items during long walks
    (2026-07-29 run #6: uq_snapshot_items_snap_ext_type killed the run
    4,080 items into exports). Duplicate enumerated ids must be deduped
    BEFORE export — one metered export and one persisted row per id."""
    folders = {"A": {"items": [
        {"id": "i1", "changeKey": "ck-old", "size": 1, "type": "IPM.Note"},
        {"id": "i2", "changeKey": "ck2", "size": 1, "type": "IPM.Note"},
        {"id": "i1", "changeKey": "ck-new", "size": 1, "type": "IPM.Note"},
    ]}}
    graph = FakeGraph(folders, ["A"])
    rec = _RecordingPersist()
    result = _drive(graph, persist=rec)

    exported = [i for _, ids in graph.post_calls for i in ids]
    assert sorted(exported) == ["i1", "i2"]
    assert sorted(rec.stored_ids) == ["i1", "i2"]
    assert result["item_count"] == 2


def test_j2_dedupe_keeps_last_occurrence():
    """Drift re-reads trail the original pass, so the LAST occurrence
    carries the freshest changeKey — dedupe must keep it. With the prior
    snapshot holding the stale key, i1 must still be selected for export
    (a keep-first dedupe would wrongly drop it as unchanged)."""
    folders = {"A": {"items": [
        {"id": "i1", "changeKey": "ck-old", "size": 1, "type": "IPM.Note"},
        {"id": "i1", "changeKey": "ck-new", "size": 1, "type": "IPM.Note"},
    ]}}
    graph = FakeGraph(folders, ["A"])

    async def prev(_rid, _csid):
        return "prior-snap", {"i1": "ck-old"}

    rec = _RecordingPersist()
    _drive(graph, persist=rec, load_prev=prev)

    exported = [i for _, ids in graph.post_calls for i in ids]
    assert exported == ["i1"]


def test_k_export_chunk_error_marks_chunk_failed_partial_not_fatal():
    """A 504 that exhausts _post_json's retries on ONE exportItems chunk
    must cost that chunk only (files_failed → PARTIAL) — not the whole
    run (2026-07-30 run #7 died 3,380 items into exports on one such
    call, forcing a full re-walk). Failed items self-heal next run: they
    have no row in this snapshot, so the changeKey diff re-selects them."""
    items = [{"id": f"i{n}", "changeKey": f"c{n}"} for n in range(25)]
    folders = {"A": {"items": items}}
    calls = {"n": 0}

    def behavior(url, ids):
        calls["n"] += 1
        if calls["n"] == 1:  # first 20-id chunk: retries exhausted → raises
            raise _http_error(504)
        return [_ok_entry(i) for i in ids]

    graph = FakeGraph(folders, ["A"], export_behavior=behavior)
    rec = _RecordingPersist()
    snap = _snapshot()
    result = _drive(graph, persist=rec, snapshot=snap)

    assert result["files_failed"] == 20
    assert sorted(rec.stored_ids) == [f"i{n}" for n in range(20, 25)]
    assert snap.delta_tokens_json["files_failed"] == 20


def test_k2_corrupt_export_json_marks_chunk_failed_partial_not_fatal():
    """A truncated/corrupt exportItems response body raises
    json.JSONDecodeError (a ValueError) OUTSIDE the httpx error family —
    run #9 (2026-08-04) died at 81% (898k/1.11M items, ~18h in) parsing a
    ~31MB response for one giant item, and the kill is DETERMINISTIC per
    item so every retry re-paid the full walk and died at the same spot.
    Must cost only the ≤20-item chunk (files_failed → PARTIAL)."""
    import json as _json
    items = [{"id": f"i{n}", "changeKey": f"c{n}"} for n in range(25)]
    folders = {"A": {"items": items}}
    calls = {"n": 0}

    def behavior(url, ids):
        calls["n"] += 1
        if calls["n"] == 1:  # first 20-id chunk: 31MB body cut mid-stream
            raise _json.JSONDecodeError(
                "Expecting ',' delimiter", '{"value": [', 11,
            )
        return [_ok_entry(i) for i in ids]

    graph = FakeGraph(folders, ["A"], export_behavior=behavior)
    rec = _RecordingPersist()
    snap = _snapshot()
    result = _drive(graph, persist=rec, snapshot=snap)

    assert result["files_failed"] == 20
    assert sorted(rec.stored_ids) == [f"i{n}" for n in range(20, 25)]
    assert snap.delta_tokens_json["files_failed"] == 20


def test_e_autoexpand_redirect_reissued_and_items_stored():
    items = [{"id": f"i{n}", "changeKey": f"ck{n}"} for n in range(3)]
    new_url = "https://graph.microsoft.com/beta/NEWLOC/exportItems"

    def behavior(url, ids):
        if "NEWLOC" in url:
            return [_ok_entry(i) for i in ids]
        return [_redirect_entry(i, new_url) for i in ids]   # 200 + per-item Error

    graph = FakeGraph({"F": {"items": items}}, ["F"], export_behavior=behavior)
    persist = _RecordingPersist()
    result = _drive(graph, persist=persist)
    assert len(graph.post_calls) == 2
    assert graph.post_calls[0][0].endswith("/exportItems") and "NEWLOC" not in graph.post_calls[0][0]
    assert "NEWLOC" in graph.post_calls[1][0]
    assert sorted(persist.stored_ids) == ["i0", "i1", "i2"]
    assert result["item_count"] == 3
    assert result["files_failed"] == 0


def test_f_mixed_batch_success_plus_redirect_both_exported():
    items = [{"id": "i0", "changeKey": "c0"}, {"id": "i1", "changeKey": "c1"}]
    new_url = "https://graph.microsoft.com/beta/AUX/exportItems"

    def behavior(url, ids):
        if "AUX" in url:
            return [_ok_entry(i) for i in ids]
        return [
            _redirect_entry(i, new_url) if i == "i1" else _ok_entry(i)
            for i in ids
        ]

    graph = FakeGraph({"F": {"items": items}}, ["F"], export_behavior=behavior)
    persist = _RecordingPersist()
    result = _drive(graph, persist=persist)
    assert sorted(persist.stored_ids) == ["i0", "i1"]
    assert result["item_count"] == 2
    assert result["files_failed"] == 0
    # reissue POST carried only the redirected item
    assert graph.post_calls[1][1] == ["i1"]


def test_g_redirect_loop_past_cap_counts_item_failed_partial():
    items = [{"id": "i1", "changeKey": "c1"}]
    loop_url = "https://graph.microsoft.com/beta/LOOP/exportItems"

    def behavior(url, ids):
        # Always redirect — even the reissue URL redirects again.
        return [_redirect_entry(i, loop_url) for i in ids]

    graph = FakeGraph({"F": {"items": items}}, ["F"], export_behavior=behavior)
    persist = _RecordingPersist()
    snap = _snapshot()
    result = _drive(graph, persist=persist, snapshot=snap)
    # original + 2 reissues (MAX_EXPORT_HOPS=2) then given up = 3 POSTs
    assert len(graph.post_calls) == 3
    assert result["files_failed"] == 1
    assert result["item_count"] == 0
    # PARTIAL-finalize convention: files_failed stamped on the snapshot.
    assert snap.delta_tokens_json["files_failed"] == 1
    assert snap.delta_tokens_json["files_total"] == 1


def test_h_failed_blob_upload_counts_failed_partial():
    items = [{"id": "i0", "changeKey": "c0"}, {"id": "i1", "changeKey": "c1"}]
    graph = FakeGraph({"F": {"items": items}}, ["F"])
    persist = _RecordingPersist(fail_ids={"i1"})   # i1 upload fails
    snap = _snapshot()
    result = _drive(graph, persist=persist, snapshot=snap)
    assert result["files_succeeded"] == 1
    assert result["files_failed"] == 1
    assert result["item_count"] == 1
    assert snap.delta_tokens_json["files_failed"] == 1


def test_chunks_helper_respects_cap():
    assert list(ab._chunks(list(range(45)), 20)) == [
        list(range(0, 20)), list(range(20, 40)), list(range(40, 45)),
    ]


def test_export_redirect_url_extractor():
    assert ab._export_redirect_url({
        "code": "ErrorArchiveFolderMovedPermanently",
        "message": "https://graph.microsoft.com/beta/AUX/exportItems",
    }) == "https://graph.microsoft.com/beta/AUX/exportItems"
    # URL embedded in a sentence.
    assert ab._export_redirect_url({
        "code": "ErrorArchiveFolderMovedPermanently",
        "message": "reissue at https://graph.microsoft.com/beta/X/exportItems now",
    }) == "https://graph.microsoft.com/beta/X/exportItems"
    # Any other per-item error code ⇒ not a redirect (caller fails the item).
    assert ab._export_redirect_url({"code": "ErrorItemNotFound", "message": "x"}) is None
    assert ab._export_redirect_url(None) is None


# ── (i) default collaborators actually execute: raw SQL + SnapshotItem build.
#    Repo has no real-DB fixture (Postgres-only models); use the established
#    stub-AsyncSession convention (tests/shared/test_snapshot_reuse.py) so the
#    module's real code — SQL text, bind params, SnapshotItem construction,
#    upload/add_all calls — runs, only DB/storage I/O is stubbed.

class _StubResult:
    def __init__(self, rows):
        self._rows = rows
        self.rowcount = len(rows)

    def first(self):
        return self._rows[0] if self._rows else None

    def all(self):
        return list(self._rows)

    def scalar(self):
        # carry_forward_slot_session probes pg_try_advisory_xact_lock and
        # reads a bool; the stub always grants the slot.
        return self._rows[0] if self._rows else True


class _StubSession:
    def __init__(self, rows_by_fragment=None, rowcount=0):
        self._map = rows_by_fragment or {}
        self._rowcount = rowcount
        self.executed = []       # list[(sql, binds)]
        self.statements = []     # raw statement objects (for values inspection)
        self.added = []          # SnapshotItem rows via add_all
        self.committed = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def execute(self, statement, binds=None):
        sql = str(statement)
        self.executed.append((sql, dict(binds or {})))
        self.statements.append(statement)
        for frag, rows in self._map.items():
            if frag in sql:
                return _StubResult(rows)
        r = _StubResult([])
        r.rowcount = self._rowcount
        return r

    def add_all(self, rows):
        self.added.extend(rows)

    async def commit(self):
        self.committed += 1


def _install_fake_azure_storage(monkeypatch, upload):
    """Inject a fake ``shared.azure_storage`` into sys.modules so persist's lazy
    import resolves without the azure SDK (absent in this sandbox, and not the
    subject under test)."""
    fake = types.ModuleType("shared.azure_storage")
    fake.azure_storage_manager = types.SimpleNamespace(
        get_shard_for_resource=lambda rid, tid: "shard0",
        get_container_name=lambda tid, wl: f"backup-{wl}-{tid}",
        build_blob_path=lambda tid, rid, sid, iid: f"{tid}/{rid}/{sid}/{iid}",
    )
    async def dedup_upload(tenant_id, content_hash, container, blob_path, content, shard, **kwargs):
        import hashlib
        assert content_hash == hashlib.sha256(content).hexdigest()
        return await upload(container, blob_path, content, shard, **kwargs)

    fake.upload_blob_with_retry = upload
    fake.upload_blob_with_dedup = dedup_upload
    monkeypatch.setitem(sys.modules, "shared.azure_storage", fake)


def test_i1_default_persist_builds_snapshot_items_and_uploads(monkeypatch):
    import shared.database as dbmod
    import shared.models as models

    calls = {"uploads": []}

    async def fake_upload(container, blob_path, content, shard, max_retries=3):
        calls["uploads"].append((container, blob_path, content))
        return {"success": True}

    stub = _StubSession(rowcount=2)
    _install_fake_azure_storage(monkeypatch, fake_upload)
    monkeypatch.setattr(dbmod, "async_session_factory", lambda: stub, raising=False)

    persist = ab._make_default_persist(_resource(), _snapshot(), _tenant(), "wk")
    pairs = [
        ({"id": "i0", "_folder_path": "/Inbox", "_folder_id": "folder-0", "type": "IPM.Note", "size": 10},
         {"itemId": "i0", "changeKey": "CK0", "data": "AAH/b3BhcXVlLTA="}),
        ({"id": "i1", "_folder_path": "/Inbox/Sub", "type": "Message", "size": 20},
         {"itemId": "i1", "changeKey": "CK1", "data": "AAH/b3BhcXVlLTE="}),
    ]
    stored, added_bytes, failed = _run(persist(pairs))

    assert (stored, failed) == (2, 0)
    assert added_bytes == 22
    assert len(calls["uploads"]) == 2
    assert calls["uploads"][0][0] == f"backup-archive-{_tenant().id}"
    # Rows go through INSERT .. ON CONFLICT DO NOTHING (uq_snapshot_items_
    # snap_ext_type backstop — run #6 2026-07-29), not session.add_all().
    assert stub.committed == 1 and len(stub.executed) == 1
    sql, _binds = stub.executed[0]
    assert "INSERT INTO snapshot_items" in sql and "ON CONFLICT" in sql
    (rowlist,) = stub.statements[0]._multi_values
    row0 = rowlist[0]
    assert row0["item_type"] == "ARCHIVE_ITEM"
    assert row0["external_id"] == "i0"
    assert row0["folder_path"] == "/Inbox"
    import hashlib
    payload = b"\x00\x01\xffopaque-0"
    assert row0["content_hash"] == row0["content_checksum"] == hashlib.sha256(payload).hexdigest()
    assert row0["metadata"]["changeKey"] == "CK0"
    assert row0["metadata"]["change_key"] == "CK0"
    assert row0["metadata"]["opaque"] is True
    assert row0["metadata"]["mailbox_id"] == "AAMkArchiveMbx"
    assert row0["metadata"]["mapi_type"] == "IPM.Note"
    assert row0["metadata"]["kind"] == row0["name"] == "Email"
    assert row0["parent_external_id"] == "folder-0"
    # The restore/import path re-encodes these exact opaque bytes once.
    assert calls["uploads"][0][2] == payload


def test_i2_default_persist_counts_failed_uploads(monkeypatch):
    import shared.database as dbmod

    async def fake_upload(container, blob_path, content, shard, max_retries=3):
        return {"success": False, "error": "boom"} if "i1" in blob_path else {"success": True}

    stub = _StubSession(rowcount=1)
    _install_fake_azure_storage(monkeypatch, fake_upload)
    monkeypatch.setattr(dbmod, "async_session_factory", lambda: stub, raising=False)

    persist = ab._make_default_persist(_resource(), _snapshot(), _tenant(), "wk")
    pairs = [
        ({"id": "i0"}, {"itemId": "i0", "changeKey": "C0", "data": "ZDA="}),
        ({"id": "i1"}, {"itemId": "i1", "changeKey": "C1", "data": "ZDE="}),
    ]
    stored, _b, failed = _run(persist(pairs))
    assert (stored, failed) == (1, 1)
    (rowlist,) = stub.statements[0]._multi_values
    assert len(rowlist) == 1 and rowlist[0]["external_id"] == "i0"


def test_i3_default_load_prev_executes_sql(monkeypatch):
    import shared.database as dbmod

    stub = _StubSession(rows_by_fragment={
        "FROM snapshots": [("prior-snap-id",)],
        "FROM snapshot_items": [("ext-a", "CKA"), ("ext-b", None), ("ext-c", "CKC")],
    })
    monkeypatch.setattr(dbmod, "async_session_factory", lambda: stub, raising=False)

    prior_id, mapping = _run(ab._default_load_prev("res-1", "cur-snap"))
    assert prior_id == "prior-snap-id"
    # None changeKey rows dropped.
    assert mapping == {"ext-a": "CKA", "ext-c": "CKC"}
    # The prior-snapshot query ran with the right binds and item_type filter.
    snap_sql = [(s, b) for (s, b) in stub.executed if "FROM snapshots" in s][0]
    assert snap_sql[1] == {"rid": "res-1", "csid": "cur-snap", "days": 30}
    item_sql = [s for (s, _b) in stub.executed if s.startswith("SELECT external_id")][0]
    assert "item_type = 'ARCHIVE_ITEM'" in item_sql
    assert "COALESCE(metadata->>'changeKey', metadata->>'change_key')" in item_sql


def test_i4_default_load_prev_no_prior_returns_empty(monkeypatch):
    import shared.database as dbmod
    stub = _StubSession()  # no rows for "FROM snapshots"
    monkeypatch.setattr(dbmod, "async_session_factory", lambda: stub, raising=False)
    prior_id, mapping = _run(ab._default_load_prev("res-1", "cur-snap"))
    assert prior_id is None and mapping == {}


def test_i5_default_carry_forward_executes_insert(monkeypatch):
    import shared.database as dbmod
    stub = _StubSession(rows_by_fragment={"SELECT COUNT(*) FROM carried": [3]})
    monkeypatch.setattr(dbmod, "async_session_factory", lambda: stub, raising=False)

    n = _run(ab._default_carry_forward("tgt", "prior", ["a", "b", "c"]))
    assert n == 3
    assert stub.committed == 1
    # executed[0] is the cluster-wide advisory herd slot probe (2026-08-18
    # convoy fix; 2026-08-24: try_ form so waiters don't pin pooler slots);
    # the INSERT follows in the same transaction.
    assert "pg_try_advisory_xact_lock" in stub.executed[0][0]
    sql, binds = next((q, b) for q, b in stub.executed
                      if "INSERT INTO snapshot_items" in q)
    assert "item_type = 'ARCHIVE_ITEM'" in sql
    assert "external_id = ANY(:ids)" in sql
    assert "src.item_type = 'ARCHIVE_ATTACHMENT'" in sql
    assert "src.parent_external_id = ANY(:ids)" in sql
    assert "SELECT COUNT(*) FROM carried WHERE item_type = 'ARCHIVE_ITEM'" in sql
    assert binds == {"tsid": "tgt", "psid": "prior", "ids": ["a", "b", "c"]}
    # empty id list is a no-op (no SQL).
    assert _run(ab._default_carry_forward("tgt", "prior", [])) == 0


# ── 2026-08-03 doom-loop hardening pins ─────────────────────────────────


def _archive_src() -> str:
    import pathlib
    return (
        pathlib.Path(__file__).resolve().parents[2]
        / "workers" / "backup-worker" / "archive_backup.py"
    ).read_text()


def test_baseline_selection_skips_rowless_snapshots():
    """Gold dispatch mints 13ms COMPLETED skip-snapshots (0 items) while a
    run is in flight. Choosing one as the changeKey baseline makes prev={}
    and forces a FULL re-export of the whole archive. The baseline query
    must require actual ARCHIVE_ITEM rows."""
    src = _archive_src()
    i = src.find("def _default_load_prev")
    j = src.find("async def", i + 10)
    block = src[i:j]
    assert "EXISTS" in block and "ARCHIVE_ITEM" in block, (
        "_default_load_prev must EXISTS-filter on ARCHIVE_ITEM rows"
    )


def test_liveness_guard_aborts_when_snapshot_row_is_lost():
    """Run #8 (2026-07-31): the cancelled-sweep deleted the snapshot row
    <=30s into a ~7h walk; the worker walked ~1.1M items blind and died on
    the first item INSERT's FK. The walk must probe its own row, renew the
    lease by snapshot ID, and abort fast when the row is gone."""
    src = _archive_src()
    assert "_liveness_guard" in src
    i = src.find("async def _liveness_guard")
    j = src.find("_guard = asyncio.create_task", i)
    block = src[i:j]
    assert "UPDATE snapshots" in block and "lease_expires_at" in block, (
        "probe must double as an owner-independent lease heartbeat"
    )
    assert "status = 'IN_PROGRESS'" in block, (
        "probe must treat a terminal/absent row as lost"
    )
    assert ".cancel()" in block, "guard must abort the run, not just log"
    # CancelledError from the guard converts to EnumerationFailed so the
    # normal failure path (not a raw cancellation) finalizes the run.
    assert "except asyncio.CancelledError" in src
    assert "_liveness_lost.is_set()" in src


"""Pure incremental-core helpers for the Exchange ARCHIVE backup handler
(workers/backup-worker/archive_backup.py). No DB / storage / Graph — just the
changeKey state-advance logic (the incremental-correctness heart) and the
SnapshotItem row shape. Orchestration is tested by integration elsewhere.

workers/backup-worker has a hyphen, so it's put on sys.path and imported
directly (same pattern as test_mail_item_attachment.py)."""

import pathlib
import sys

_ROOT = pathlib.Path(__file__).resolve().parents[2]
_WORKER = _ROOT / "workers" / "backup-worker"
for _p in (str(_ROOT), str(_WORKER)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from archive_backup import (  # noqa: E402
    ARCHIVE_ITEM_TYPE,
    advance_state,
    build_archive_item_row,
    build_folder_path_map,
    is_skippable_error,
    item_kind,
    state_blob_path,
)


# ── is_skippable_error: permission/no-access => skip gracefully, don't FAIL ──

def test_skippable_errors_are_access_and_license_signals():
    # perms not consented / mailbox unlicensed / access denied => skip
    assert is_skippable_error("404 MailboxNotEnabledForRESTAPI")
    assert is_skippable_error("HTTP 403 Forbidden")
    assert is_skippable_error("401 Unauthorized")
    assert is_skippable_error("Authorization_RequestDenied: insufficient privileges")
    assert is_skippable_error("ErrorAccessDenied")
    assert is_skippable_error("Access is denied")


def test_transient_and_real_errors_are_NOT_skippable():
    # these should FAIL (and retry), not be silently skipped
    assert not is_skippable_error("503 Service Unavailable")
    assert not is_skippable_error("500 Internal Server Error")
    assert not is_skippable_error("ConnectTimeout")
    assert not is_skippable_error("KeyError: 'id'")
    assert not is_skippable_error("")
    assert not is_skippable_error(None)


# ── item_kind: MAPI class -> human kind (the only "what is it" we can show) ──

def test_item_kind_maps_common_mapi_classes():
    assert item_kind("IPM.Note") == "Email"
    assert item_kind("IPM.Note.SMIME") == "Email"
    assert item_kind("IPM.Contact") == "Contact"
    assert item_kind("IPM.Appointment") == "Calendar event"
    assert item_kind("IPM.Schedule.Meeting.Request") == "Calendar event"
    assert item_kind("IPM.Task") == "Task"
    assert item_kind("IPM.StickyNote") == "Note"
    # unknown / empty fall back to the raw class or a generic
    assert item_kind("IPM.Weird") == "IPM.Weird"
    assert item_kind("") == "Item"
    assert item_kind(None) == "Item"


# ── advance_state: the incremental engine ────────────────────────────────

def test_first_run_takes_all_stored():
    current = {"a": "k1", "b": "k2"}
    assert advance_state(None, current, {"a": "k1", "b": "k2"}) == {"a": "k1", "b": "k2"}


def test_unchanged_items_keep_prev_key_without_re_export():
    prev = {"a": "k1", "b": "k2"}
    current = {"a": "k1", "b": "k2"}  # nothing changed
    assert advance_state(prev, current, {}) == {"a": "k1", "b": "k2"}  # stored nothing


def test_changed_item_advances_only_when_stored():
    prev = {"a": "old"}
    current = {"a": "new"}  # changeKey moved
    # export FAILED (not in stored) -> keep OLD key so select_changed re-flags it
    assert advance_state(prev, current, {}) == {"a": "old"}
    # export OK -> advance to the new key
    assert advance_state(prev, current, {"a": "new"}) == {"a": "new"}


def test_deleted_item_dropped_from_state():
    prev = {"a": "k1", "gone": "k9"}
    current = {"a": "k1"}  # 'gone' no longer in the archive
    out = advance_state(prev, current, {})
    assert out == {"a": "k1"} and "gone" not in out


def test_new_item_that_failed_export_is_omitted_for_retry():
    prev: dict = {}
    current = {"new": "k1"}
    assert advance_state(prev, current, {}) == {}  # failed new -> retry next run
    assert advance_state(prev, current, {"new": "k1"}) == {"new": "k1"}  # stored -> tracked


# ── build_folder_path_map: full nested path from parentFolderId chain ─────

def test_folder_path_map_builds_full_nested_path():
    # Live shape: displayName is only the LEAF name; the mailbox root is NOT in
    # the list (its id appears as some folder's parentFolderId but resolves to
    # nothing) -> a top-level folder maps to just its own name.
    folders = [
        {"id": "root_archive", "displayName": "Archive", "parentFolderId": "MBXROOT"},
        {"id": "proj", "displayName": "Projects", "parentFolderId": "root_archive"},
        {"id": "y2024", "displayName": "2024", "parentFolderId": "proj"},
        {"id": "deleted", "displayName": "Deleted Items", "parentFolderId": "MBXROOT"},
    ]
    m = build_folder_path_map(folders)
    assert m["root_archive"] == "Archive"
    assert m["proj"] == "Archive/Projects"
    assert m["y2024"] == "Archive/Projects/2024"
    assert m["deleted"] == "Deleted Items"


def test_folder_path_map_is_cycle_and_orphan_safe():
    # A malformed parent cycle must terminate, not hang; a missing id is skipped.
    folders = [
        {"id": "a", "displayName": "A", "parentFolderId": "b"},
        {"id": "b", "displayName": "B", "parentFolderId": "a"},
        {"displayName": "no-id"},
    ]
    m = build_folder_path_map(folders)
    # both resolve to a bounded (non-infinite) path containing their own name
    assert "A" in m["a"] and "B" in m["b"]
    assert None not in m  # id-less folder skipped


def test_row_uses_explicit_folder_path_over_leaf_display_name():
    # When the caller supplies the reconstructed full path, it wins over the
    # folder's leaf displayName (which is only the leaf, e.g. "2024").
    row = build_archive_item_row(
        snapshot_id="S", tenant_id="T",
        folder={"id": "y2024", "displayName": "2024"},
        item={"id": "I", "changeKey": "CK", "type": "IPM.Note"},
        blob_path="p", content_hash="H", blob_len=1, mailbox_id="M",
        graph_version="beta", folder_path="Archive/Projects/2024",
    )
    assert row["folder_path"] == "Archive/Projects/2024"
    assert row["parent_external_id"] == "y2024"


# ── build_archive_item_row: SnapshotItem shape ───────────────────────────

def test_row_shape_uses_extra_data_key_not_metadata():
    row = build_archive_item_row(
        snapshot_id="S", tenant_id="T",
        folder={"id": "F", "displayName": "Archive/2019"},
        item={"id": "I", "changeKey": "CK", "type": "IPM.Note", "size": 1234,
              "lastModifiedDateTime": "2026-07-14T00:00:00Z"},
        blob_path="T/R/S/I", content_hash="H", blob_len=2048,
        mailbox_id="MBX", graph_version="beta",
    )
    assert row["external_id"] == "I"
    assert row["item_type"] == ARCHIVE_ITEM_TYPE == "ARCHIVE_ITEM"
    assert row["parent_external_id"] == "F"
    assert row["folder_path"] == "Archive/2019"
    assert row["blob_path"] == "T/R/S/I"
    assert row["content_size"] == 2048
    assert row["content_hash"] == row["content_checksum"] == "H"
    # name = friendly kind (no subject is available from the opaque endpoint)
    assert row["name"] == "Email"
    # mapped-class insert -> MUST be the ORM attribute 'extra_data', never the
    # column name 'metadata' (that key would silently vanish on insert).
    assert "extra_data" in row and "metadata" not in row
    assert row["extra_data"]["change_key"] == "CK"
    assert row["extra_data"]["opaque"] is True
    assert row["extra_data"]["mailbox_id"] == "MBX"
    # "what it is" — kind + raw MAPI class exposed for the browse UI
    assert row["extra_data"]["kind"] == "Email"
    assert row["extra_data"]["mapi_type"] == "IPM.Note"


def test_row_name_is_friendly_kind_not_raw_mapi_class():
    # archive item listing $selects id,changeKey,size,type — NO subject; the
    # name must be a human kind, never the raw "IPM.*" class.
    row = build_archive_item_row(
        snapshot_id="S", tenant_id="T", folder={"id": "F"},
        item={"id": "I", "changeKey": "CK", "type": "IPM.Contact"},
        blob_path="p", content_hash="H", blob_len=1, mailbox_id="M", graph_version="v1.0",
    )
    assert row["name"] == "Contact"
    assert row["folder_path"] == "F"  # falls back to folder id when no displayName


# ── state_blob_path: fixed & overwritable ────────────────────────────────

def test_state_blob_path_is_deterministic_and_overwritable():
    assert state_blob_path("T", "R") == state_blob_path("T", "R")  # no timestamp
    assert state_blob_path("T", "R") == "T/R/_archive_state/changekeys.json"


def test_invalid_archive_export_data_is_counted_without_upload(monkeypatch):
    import shared.database as dbmod

    async def unexpected_upload(*args, **kwargs):
        raise AssertionError("invalid base64 must not be uploaded")

    stub = _StubSession()
    _install_fake_azure_storage(monkeypatch, unexpected_upload)
    monkeypatch.setattr(dbmod, "async_session_factory", lambda: stub)
    persist = ab._make_default_persist(_resource(), _snapshot(), _tenant(), "wk")
    assert _run(persist([
        ({"id": "broken"}, {"data": "not valid base64!"}),
        ({"id": "empty"}, {"data": ""}),
    ])) == (0, 0, 2)
    assert stub.executed == []


def test_forced_full_exports_unchanged_items_and_retains_coverage_guard():
    item = {"id": "i0", "changeKey": "CK0"}
    graph = FakeGraph({"F": {"items": [item]}}, ["F"])
    persist = _RecordingPersist()

    async def prior(_rid, _sid):
        return "prior", {"i0": "CK0"}

    async def unexpected_carry(*args):
        raise AssertionError("forced full must export every item")

    result = _run(ab.run_archive_backup(
        graph, _resource(), _snapshot(), _tenant(),
        settings=_settings(), force_full=True, load_prev=prior,
        persist_batch=persist, carry_forward=unexpected_carry,
    ))
    assert persist.stored_ids == ["i0"]
    assert result["archive_carried_forward"] == 0

    async def richer_prior(_rid, _sid):
        return "prior", {"i0": "CK0", "missing": "CK1"}

    with pytest.raises(ab.EnumerationFailed, match="coverage"):
        _run(ab.run_archive_backup(
            graph, _resource(), _snapshot(), _tenant(),
            settings=_settings(), force_full=True, load_prev=richer_prior,
            persist_batch=persist, carry_forward=unexpected_carry,
        ))


def test_archive_persist_uses_blob_path_returned_by_dedup(monkeypatch):
    import shared.database as dbmod

    async def dedup_hit(*args, **kwargs):
        return {"success": True, "method": "dedup_hit", "blob_path": "prior-snapshot/opaque"}

    stub = _StubSession(rowcount=1)
    _install_fake_azure_storage(monkeypatch, dedup_hit)
    monkeypatch.setattr(dbmod, "async_session_factory", lambda: stub)
    persist = ab._make_default_persist(_resource(), _snapshot(), _tenant(), "wk")
    assert _run(persist([({"id": "i0"}, {"data": "ZDA="})])) == (1, 2, 0)
    (rows,) = stub.statements[0]._multi_values
    assert rows[0]["blob_path"] == "prior-snapshot/opaque"
