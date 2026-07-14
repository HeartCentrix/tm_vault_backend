"""In-place ARCHIVE restore engine (workers/restore-worker/archive_restore.py).

Restores opaque MAPI blobs verbatim into a mailbox via a reused Graph import
session. No network: archive_graph.create_import_session / import_item are
monkeypatched; the blob download + import POST are injected fakes.

workers/restore-worker has a hyphen, so it's put on sys.path and imported
directly (same pattern as test_mail_item_attachment.py)."""
from __future__ import annotations

import pathlib
import sys

import pytest

_ROOT = pathlib.Path(__file__).resolve().parents[2]
_WORKER = _ROOT / "workers" / "restore-worker"
for _p in (str(_ROOT), str(_WORKER)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import archive_restore as ar  # noqa: E402
from shared import archive_graph  # noqa: E402


class _Item:
    def __init__(self, ext_id, folder_id, mailbox_id="MBX", blob=b"OPAQUE"):
        self.external_id = ext_id
        self.parent_external_id = folder_id
        self.extra_data = {"mailbox_id": mailbox_id, "opaque": True}
        self._blob = blob


def _dl(item):
    async def _inner(it):
        return it._blob
    return _inner


# ── pure helpers ─────────────────────────────────────────────────────────

def test_resolve_target_mailbox_id_from_extra_data():
    items = [_Item("a", "f", mailbox_id="MBX:arch@t")]
    assert ar.resolve_target_mailbox_id(items) == "MBX:arch@t"
    # absent -> None
    bad = type("X", (), {"extra_data": {}})()
    assert ar.resolve_target_mailbox_id([bad]) is None


def test_parse_expiry_handles_z_and_bad():
    assert ar._parse_expiry("2026-07-14T19:00:00Z") is not None
    assert ar._parse_expiry(None) is None
    assert ar._parse_expiry("not-a-date") is None


# ── engine ───────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_run_creates_one_session_and_imports_each_blob(monkeypatch):
    sessions = []
    imports = []

    async def _fake_session(gc, mailbox_id, **kw):
        sessions.append(mailbox_id)
        return ("https://outlook.office365.com/import?t=1", "2999-01-01T00:00:00Z")

    async def _fake_import(post_json, url, folder_id, blob, **kw):
        imports.append((url, folder_id, blob, kw.get("mode")))
        return {"itemId": "NEW", "changeKey": "CK"}

    monkeypatch.setattr(archive_graph, "create_import_session", _fake_session)
    monkeypatch.setattr(archive_graph, "import_item", _fake_import)

    items = [_Item("a", "F1"), _Item("b", "F2")]
    eng = ar.ArchiveRestoreEngine(object(), download_blob=_dl(None), post_json=None)
    out = await eng.run(items)

    assert out == {"created": 2, "failed": 0}
    assert sessions == ["MBX"]          # ONE session reused for both items
    assert [i[1] for i in imports] == ["F1", "F2"]   # each into its origin folder
    assert all(i[3] == "create" for i in imports)


@pytest.mark.asyncio
async def test_run_counts_failures_and_keeps_going(monkeypatch):
    async def _fake_session(gc, mailbox_id, **kw):
        return ("https://o/import", "2999-01-01T00:00:00Z")

    async def _fake_import(post_json, url, folder_id, blob, **kw):
        if folder_id == "BOOM":
            raise RuntimeError("import 500")
        return {"itemId": "x"}

    monkeypatch.setattr(archive_graph, "create_import_session", _fake_session)
    monkeypatch.setattr(archive_graph, "import_item", _fake_import)

    items = [
        _Item("ok", "F1"),
        _Item("nofolder", None),      # no folder -> failed, no import attempted
        _Item("boom", "BOOM"),        # import raises -> failed
        _Item("empty", "F2", blob=b""),  # empty blob -> failed
    ]
    eng = ar.ArchiveRestoreEngine(object(), download_blob=_dl(None))
    out = await eng.run(items)
    assert out["created"] == 1 and out["failed"] == 3


@pytest.mark.asyncio
async def test_run_no_mailbox_id_fails_all(monkeypatch):
    called = []
    monkeypatch.setattr(archive_graph, "create_import_session",
                        lambda *a, **k: called.append(1))
    bad = type("X", (), {"extra_data": {}, "parent_external_id": "F"})()
    eng = ar.ArchiveRestoreEngine(object(), download_blob=_dl(None))
    out = await eng.run([bad])
    assert out["created"] == 0 and out["failed"] == 1
    assert not called  # never opened a session with no target mailbox


@pytest.mark.asyncio
async def test_run_refreshes_session_before_expiry(monkeypatch):
    created_sessions = []

    async def _fake_session(gc, mailbox_id, **kw):
        # first session already expired -> forces a refresh before item 1
        exp = "2000-01-01T00:00:00Z" if not created_sessions else "2999-01-01T00:00:00Z"
        created_sessions.append(exp)
        return (f"https://o/import?n={len(created_sessions)}", exp)

    async def _fake_import(post_json, url, folder_id, blob, **kw):
        return {"itemId": "x"}

    monkeypatch.setattr(archive_graph, "create_import_session", _fake_session)
    monkeypatch.setattr(archive_graph, "import_item", _fake_import)

    eng = ar.ArchiveRestoreEngine(object(), download_blob=_dl(None))
    out = await eng.run([_Item("a", "F1")])
    assert out == {"created": 1, "failed": 0}
    assert len(created_sessions) == 2  # initial (expired) + one refresh


@pytest.mark.asyncio
async def test_post_json_no_auth_sends_no_authorization_header():
    import httpx
    seen = {}

    def _handler(request: httpx.Request) -> httpx.Response:
        seen["auth"] = request.headers.get("Authorization")
        seen["body"] = request.read()
        return httpx.Response(200, json={"itemId": "OK"})

    transport = httpx.MockTransport(_handler)
    res = await ar.post_json_no_auth(
        "https://outlook.office365.com/import?t=1", {"FolderId": "F", "Data": "x"},
        _client_transport=transport,
    )
    assert res == {"itemId": "OK"}
    assert seen["auth"] is None  # preauthenticated URL -> no Authorization header
    assert b"FolderId" in seen["body"]


# ── restore to a NAMED folder in the user's PRIMARY (active) mailbox ────────

@pytest.mark.asyncio
async def test_run_target_folder_lands_in_primary_named_folder(monkeypatch):
    """recover-to-a-separate-folder: the archived items go into a NEW folder we
    create in the ACTIVE (primary) mailbox, NOT their origin archive folder.
    (The in-place archive has no folder-create API.)"""
    imports = []
    sessions = []

    async def _fake_session(gc, mailbox_id, **kw):
        sessions.append(mailbox_id)
        return ("https://o/import", "2999-01-01T00:00:00Z")

    async def _fake_import(post_json, url, folder_id, blob, **kw):
        imports.append(folder_id)
        return {"itemId": "x"}

    async def _fake_primary(gc, user_id, **kw):
        return "MBX:primary"

    async def _fake_list(gc, mailbox_id, **kw):
        # I/E listing of the PRIMARY mailbox includes our just-created folder.
        return [{"displayName": "dddd", "id": "IE_DDDD"}]

    monkeypatch.setattr(archive_graph, "create_import_session", _fake_session)
    monkeypatch.setattr(archive_graph, "import_item", _fake_import)
    monkeypatch.setattr(archive_graph, "get_primary_mailbox_id", _fake_primary)
    monkeypatch.setattr(archive_graph, "list_archive_folders", _fake_list)

    class _GC:
        GRAPH_URL = "https://graph.microsoft.com/v1.0"
        async def _post(self, url, body):
            return {"id": "STD_DDDD"}

    items = [_Item("a", "origin1"), _Item("b", "origin2")]
    eng = ar.ArchiveRestoreEngine(_GC(), download_blob=_dl(None))
    out = await eng.run(items, target_folder="dddd", user_id="user-1")

    assert out == {"created": 2, "failed": 0}
    assert sessions == ["MBX:primary"]        # session opened on the PRIMARY mailbox
    assert imports == ["IE_DDDD", "IE_DDDD"]  # both into the named folder, not origin


@pytest.mark.asyncio
async def test_run_target_folder_unavailable_fails_cleanly(monkeypatch):
    async def _fake_primary(gc, user_id, **kw):
        return None  # no primary mailbox -> can't prepare target
    monkeypatch.setattr(archive_graph, "get_primary_mailbox_id", _fake_primary)

    class _GC:
        GRAPH_URL = "https://graph.microsoft.com/v1.0"
        async def _post(self, url, body):
            return {"id": "X"}

    eng = ar.ArchiveRestoreEngine(_GC(), download_blob=_dl(None))
    out = await eng.run([_Item("a", "F1")], target_folder="dddd", user_id="u1")
    assert out["created"] == 0 and out["failed"] == 1
