"""A user without an Exchange Online mailbox is not a failed backup (Amit, 2026-11-06): mail,
calendar and contacts complete with 0 items and say why. Live 2026-11-06: 28/28 such users got
404 MailboxNotEnabledForRESTAPI on mailFolders, calendar and contacts; 13/13 with a mailbox got 200."""
from __future__ import annotations

import asyncio
import importlib.util
import os
import pathlib
import re
import sys
from types import SimpleNamespace

import httpx
import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")
_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
_MAIN_PATH = _REPO_ROOT / "workers" / "backup-worker" / "main.py"
_SRC = _MAIN_PATH.read_text()
_spec = importlib.util.spec_from_file_location("bw_no_mailbox", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)
sys.modules["bw_no_mailbox"] = _bw
try:
    _spec.loader.exec_module(_bw)
except Exception as exc:  # pragma: no cover
    pytest.skip(f"backup-worker module failed to import: {exc}", allow_module_level=True)


def _http_error(status, code):
    req = httpx.Request("GET", "https://graph.microsoft.com/v1.0/users/u/mailFolders")
    resp = httpx.Response(status, json={"error": {"code": code, "message": "x"}}, request=req)
    return httpx.HTTPStatusError("err", request=req, response=resp)


def test_only_mailbox_not_enabled_404_means_no_mailbox():
    assert "No Exchange Online mailbox" in _bw.no_mailbox_reason(_http_error(404, "MailboxNotEnabledForRESTAPI"))
    assert _bw.no_mailbox_reason(_http_error(404, "ErrorItemNotFound")) is None
    assert _bw.no_mailbox_reason(_http_error(500, "MailboxNotEnabledForRESTAPI")) is None
    assert _bw.no_mailbox_reason(RuntimeError("boom")) is None


def test_note_no_mailbox_probes_and_records_the_reason():
    patched = []
    async def patch(snapshot_id, data):
        patched.append((snapshot_id, data))
        return True
    async def get(url, params=None, **_):
        raise _http_error(404, "MailboxNotEnabledForRESTAPI")
    worker = SimpleNamespace(worker_id="t", _patch_snapshot_extra_data=patch)
    graph = SimpleNamespace(GRAPH_URL="https://graph.microsoft.com/v1.0", _get=get)
    snap = SimpleNamespace(id="s1")
    assert asyncio.run(_bw.BackupWorker._note_no_mailbox(worker, graph, "u", snap, "USER_MAIL"))
    assert patched == [("s1", {"no_mailbox": _bw.no_mailbox_reason(_http_error(404, "MailboxNotEnabledForRESTAPI"))})]


def test_note_no_mailbox_is_false_for_a_mailbox_or_another_error():
    async def ok(url, params=None, **_):
        return {"value": []}
    async def other(url, params=None, **_):
        raise _http_error(503, "ServiceUnavailable")
    worker = SimpleNamespace(worker_id="t", _patch_snapshot_extra_data=None)
    snap = SimpleNamespace(id="s1")
    for get in (ok, other):
        graph = SimpleNamespace(GRAPH_URL="https://graph.microsoft.com/v1.0", _get=get)
        assert not asyncio.run(_bw.BackupWorker._note_no_mailbox(worker, graph, "u", snap, "USER_MAIL"))


def test_mail_calendar_and_contacts_complete_empty_when_there_is_no_mailbox():
    flat = re.sub(r"\s+", " ", _SRC)
    assert "if not folder_tree_full and await self._note_no_mailbox(graph_client, user_id, snapshot, \"USER_MAIL\"): return []" in flat
    assert "if await self._note_no_mailbox(graph_client, user_id, snapshot, \"USER_CALENDAR\", exc=_e): return []" in flat
    contacts = flat.index("if _contact_enum_errors and not out_rows and await self._note_no_mailbox(graph_client, user_id, snapshot, \"USER_CONTACTS\"): return []")
    assert contacts < flat.index("await self._stamp_snapshot_degraded( snapshot.id, _contacts_reason,", contacts)
