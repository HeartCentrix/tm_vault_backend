"""USER_ARCHIVE autoexpanded (auxiliary) archive mailbox re-basing.

Prod incident 2026-08-06 (batch bdd57c19, resource "Archive — Jason Rue"):
after Exchange auto-expanded the archive, 7 of 10 top-level folders were
relocated into an AUXILIARY archive mailbox. Graph answers a request for a
relocated folder with ``308 Permanent Redirect`` +
``ErrorArchiveFolderMovedPermanently``, and the shared httpx client follows it
transparently — so ``_walk_folders`` received the AUX mailbox's child folders
but kept addressing them against the PRIMARY mailbox base it computed once at
the top. Graph answered every such child with
``404 ErrorItemNotFound`` -> ``EnumerationFailed`` -> snapshot FAILED, 138
times in 7h.

Captured from prod (probe, 2026-08-06):
  GET  {primary}/folders/{moved_id}/items      -> 308, Location: {aux}/folders('{id}')/items
       ... follow_redirects -> 200, @odata.context names the AUX mailbox
  GET  {primary}/folders/{aux_child_id}/items  -> 404 ErrorItemNotFound
  GET  {aux}/folders/{aux_child_id}/items      -> 200

The fix reads the serving mailbox back off each response's
``@odata.context`` and recurses with THAT base. This test pins the behaviour
end to end plus the pure helper.
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

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://u:p@localhost/db")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
_MOD_PATH = _REPO_ROOT / "workers" / "backup-worker" / "archive_backup.py"
_spec = importlib.util.spec_from_file_location("archive_backup_rebase_uut", _MOD_PATH)
ab = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["archive_backup_rebase_uut"] = ab
assert _spec and _spec.loader
_spec.loader.exec_module(ab)  # type: ignore[union-attr]


GRAPH = "https://graph.microsoft.com/beta"
PRIMARY_MBX = "MBX:primary@tenant"
AUX_MBX = "MBX:aux@tenant"
PRIMARY_BASE = f"{GRAPH}/admin/exchange/mailboxes/{PRIMARY_MBX}/folders"
AUX_BASE = f"{GRAPH}/admin/Exchange/Mailboxes/{AUX_MBX}/folders"


def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _not_found(url: str) -> httpx.HTTPStatusError:
    """The exact prod shape: 404 ErrorItemNotFound."""
    req = httpx.Request("GET", url)
    resp = httpx.Response(
        404, request=req,
        json={"error": {"code": "ErrorItemNotFound",
                        "message": "The specified object was not found in the store."}},
    )
    return httpx.HTTPStatusError("404 Not Found", request=req, response=resp)


class AutoexpandGraph:
    """Models an auto-expanded archive.

    * ``stay`` folders live in the PRIMARY mailbox.
    * ``moved`` folders have been relocated to the AUX mailbox: a request for
      them against the primary base transparently resolves (httpx follows the
      308) and the response's ``@odata.context`` names the AUX mailbox.
    * The children of a moved folder are AUX-only: addressing them against the
      PRIMARY base raises 404, exactly as prod does.
    """

    def __init__(self):
        # folder id -> (items, children)
        self.tree = {
            "stay1": ([{"id": "i-stay1", "changeKey": "ck1", "size": 1, "type": "m"}], []),
            "moved1": ([], ["auxkid1"]),
            "auxkid1": ([{"id": "i-aux1", "changeKey": "ck2", "size": 2, "type": "m"}], []),
        }
        self.aux_only = {"auxkid1"}
        self.moved = {"moved1"}
        self.bad_requests: list[str] = []   # primary-base requests for aux ids

    def _serving_base(self, fid: str) -> str:
        return AUX_BASE if (fid in self.aux_only or fid in self.moved) else PRIMARY_BASE

    def _ctx(self, fid: str, leaf: str) -> str:
        base = self._serving_base(fid)
        root, path = base.split("/admin/", 1)
        return f"{root}/$metadata#admin/{path}('{fid}')/{leaf}"

    def _folder_obj(self, fid):
        return {"id": fid, "displayName": fid,
                "childFolderCount": len(self.tree[fid][1])}

    async def _get(self, url, params=None, on_activity=None):
        if url == PRIMARY_BASE:
            return {"value": [self._folder_obj(f) for f in ("stay1", "moved1")],
                    "@odata.context": f"{GRAPH}/$metadata#admin/exchange/mailboxes/{PRIMARY_MBX}/folders"}
        if url == AUX_BASE:
            # The union walk (ITO-10146) enumerates every discovered location
            # from its own root. This fake serves all aux content inline via
            # the rebase path, so its root legitimately lists nothing extra.
            return {"value": [],
                    "@odata.context": f"{GRAPH}/$metadata#admin/Exchange/Mailboxes/{AUX_MBX}/folders"}

        for leaf in ("items", "childFolders"):
            if not url.endswith(f"/{leaf}"):
                continue
            head = url[: -len(f"/{leaf}")]
            # split "<base>/<fid>" -> base, fid
            base, _, fid = head.rpartition("/")
            if fid not in self.tree:
                raise AssertionError(f"unknown folder in {url}")
            # An AUX-resident child addressed against the PRIMARY base is the bug.
            if fid in self.aux_only and base == PRIMARY_BASE:
                self.bad_requests.append(url)
                raise _not_found(url)
            items, kids = self.tree[fid]
            payload = ([{**it} for it in items] if leaf == "items"
                       else [self._folder_obj(k) for k in kids])
            return {"value": payload, "@odata.context": self._ctx(fid, leaf)}

        raise AssertionError(f"unexpected GET {url}")


# ───────────────────────── pure helper ──────────────────────────

def test_base_from_odata_context_extracts_serving_mailbox():
    ctx = (f"{GRAPH}/$metadata#admin/Exchange/Mailboxes/{AUX_MBX}"
           f"/folders('AQMkAD==')/childFolders")
    assert ab._base_from_odata_context(ctx, PRIMARY_BASE) == AUX_BASE


@pytest.mark.parametrize("ctx", [None, "", "not-a-context", 42, {}])
def test_base_from_odata_context_falls_back_when_unusable(ctx):
    assert ab._base_from_odata_context(ctx, PRIMARY_BASE) == PRIMARY_BASE


# ───────────────────────── the walk ──────────────────────────

def _walk(g, **kw):
    return _run(ab._walk_folders(g, GRAPH, PRIMARY_MBX, concurrency=4,
                                 worker_id="w", **kw))


def test_walk_folders_rebases_children_onto_aux_mailbox():
    """The regression: children of a relocated folder must be requested
    against the mailbox that actually served the parent."""
    g = AutoexpandGraph()
    items, failures = _walk(g)
    ids = sorted(i["id"] for i in items)
    assert ids == ["i-aux1", "i-stay1"], (
        "aux-resident subtree was dropped — children were not re-based"
    )
    assert g.bad_requests == [], (
        f"aux folder ids were addressed to the PRIMARY mailbox: {g.bad_requests}"
    )
    assert failures == []


def test_walk_folders_still_walks_primary_only_archive():
    """No autoexpand ⇒ behaviour unchanged (no regression for the common case)."""
    g = AutoexpandGraph()
    g.aux_only = set()
    g.moved = set()
    items, failures = _walk(g)
    assert sorted(i["id"] for i in items) == ["i-aux1", "i-stay1"]
    assert g.bad_requests == [] and failures == []


def test_walk_folders_tags_folder_paths():
    """Re-basing must not disturb the _folder_path tagging."""
    g = AutoexpandGraph()
    items, _f = _walk(g)
    by_id = {i["id"]: i["_folder_path"] for i in items}
    assert by_id["i-stay1"] == "/stay1"
    assert by_id["i-aux1"] == "/moved1/auxkid1"


# ─────────── per-folder tolerance (persistent Graph 500) ───────────
#
# Verified live 2026-08-06: 3 folders in jrue's archive answer
#   500 {"error":{"code":"ErrorInternalServerError",
#        "message":"… The operation failed., Cannot query rows in a table."}}
# on 4/4 attempts. Aborting the walk on those meant the 1.1M-item archive
# could never complete — the failure must be recorded and finalize PARTIAL.


def _status_error(url: str, status: int, code: str) -> httpx.HTTPStatusError:
    req = httpx.Request("GET", url)
    resp = httpx.Response(status, request=req,
                          json={"error": {"code": code, "message": "boom"}})
    return httpx.HTTPStatusError(f"{status}", request=req, response=resp)


class BrokenFolderGraph(AutoexpandGraph):
    """``break_map`` maps folder id -> (status, graph code) for its /items."""

    def __init__(self, break_map):
        super().__init__()
        self.break_map = break_map

    async def _get(self, url, params=None, on_activity=None):
        if url.endswith("/items"):
            fid = url[: -len("/items")].rpartition("/")[2]
            if fid in self.break_map:
                status, code = self.break_map[fid]
                raise _status_error(url, status, code)
        return await super()._get(url, params)


def test_persistent_500_on_one_folder_does_not_kill_the_walk():
    g = BrokenFolderGraph({"auxkid1": (500, "ErrorInternalServerError")})
    items, failures = _walk(g)
    assert [i["id"] for i in items] == ["i-stay1"], "healthy folders still captured"
    assert len(failures) == 1
    f = failures[0]
    assert f["status"] == 500
    assert f["code"] == "ErrorInternalServerError"
    assert f["path"] == "/moved1/auxkid1"


def test_auth_failure_still_fails_the_whole_walk():
    """401/403 is a whole-walk problem, not a bad folder — it must NOT be
    downgraded to a PARTIAL, or an expired credential would silently produce
    a near-empty 'successful' archive."""
    for status in (401, 403):
        g = BrokenFolderGraph({"auxkid1": (status, "InvalidAuthenticationToken")})
        with pytest.raises(httpx.HTTPStatusError):
            _walk(g)


def test_walk_fails_when_every_top_level_folder_is_broken():
    """Nothing captured at all is a failure, never a PARTIAL."""
    g = BrokenFolderGraph({
        "stay1": (500, "ErrorInternalServerError"),
        "moved1": (500, "ErrorInternalServerError"),
        "auxkid1": (500, "ErrorInternalServerError"),
    })
    with pytest.raises(ab.EnumerationFailed):
        _walk(g)


def test_failure_cap_is_enforced():
    g = BrokenFolderGraph({"auxkid1": (500, "ErrorInternalServerError")})
    with pytest.raises(ab.EnumerationFailed):
        _walk(g, max_folder_failures=0)
