"""ITO-10277: saved deltaLinks must be resumed VERBATIM (params=None).

httpx 0.28.0 changed `params=` semantics: it REPLACES the request URL's
query string instead of merging into it. Every delta site that resumed a
stored deltaLink while still passing `params={"$top": ...}` therefore sent
Graph a token-less `/delta` — a silent full re-enumeration. Proven live on
IT OPS NOTES (resource 6584d4a9): the stored Documents deltaLink replayed
verbatim returned 9 items with a terminal deltaLink, while the worker's
same-token walk streamed 2,174 items in its first 60s and re-uploaded the
same unchanged multi-GB files (identical eTag + sha256 across runs, fresh
blob_path every night, ~41.5 GB/night since at least 8/9 — the chronic
7-23h nightly walk collapse).

Same defect family as the contacts baseline fix
(test_contacts_delta_no_top.py) and the mail/contacts resume arms, which
already do `url = saved; params = None`. These tests pin the remaining
delta sites to the verbatim-resume shape, plus:

  * a one-shot 410/resyncRequired fallback in ``iter_drive_items_by_id``
    (once tokens actually reach Graph, expired ones 410 — uncaught, that
    would PARTIAL the walk and retry the same dead token forever), and
  * the httpx <0.28 pin in the backup-worker image (defense in depth for
    any site these tests miss).
"""
from __future__ import annotations

import os
import pathlib

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

from shared.graph_client import GraphClient  # noqa: E402

REPO = pathlib.Path(__file__).resolve().parents[2]

TOKEN_URL = (
    "https://graph.microsoft.com/v1.0/drives/b!abc/root/delta"
    "?%24top=999&token=NDslMjM7JTIzOyUyMzQ7"
)


class _Recorder:
    """Captures every (url, params) handed to GraphClient._get and serves
    canned pages. Async-generator sites consume pages one at a time; plain
    sites read .value + follow nextLink."""

    def __init__(self, pages=None, raise_first=None):
        self.calls = []
        self._pages = list(pages or [])
        self._raise_first = raise_first

    async def __call__(self, url, params=None, **kw):
        self.calls.append((url, params))
        if self._raise_first is not None:
            exc = self._raise_first
            self._raise_first = None
            raise exc
        if self._pages:
            return self._pages.pop(0)
        return {"value": [], "@odata.deltaLink": "https://g/delta?token=NEW"}


def _client(recorder) -> GraphClient:
    gc = object.__new__(GraphClient)  # no auth — only _get is exercised
    gc._get = recorder
    return gc


# ── the async-generator drive walks ──────────────────────────────────


async def test_iter_drive_items_by_id_resumes_verbatim():
    rec = _Recorder()
    gc = _client(rec)
    async for _ in gc.iter_drive_items_by_id("b!abc", delta_token=TOKEN_URL):
        pass
    url, params = rec.calls[0]
    assert url == TOKEN_URL
    assert params is None, (
        "httpx>=0.28 REPLACES the query when params is passed — "
        "$top here strips token=… and turns the incremental into a full walk"
    )


async def test_iter_drive_items_by_id_baseline_keeps_top():
    rec = _Recorder()
    gc = _client(rec)
    async for _ in gc.iter_drive_items_by_id("b!abc"):
        pass
    url, params = rec.calls[0]
    assert url.endswith("/drives/b!abc/root/delta")
    assert params == {"$top": "999"}


async def test_iter_drive_items_by_id_410_falls_back_to_full_walk_once():
    boom = RuntimeError(
        "Graph GET 410 on /drives/b!abc/root/delta: resyncRequired — "
        "The sync state generation is not found"
    )
    rec = _Recorder(
        pages=[{
            "value": [{"id": "f1", "file": {}}],
            "@odata.deltaLink": "https://g/delta?token=FRESH",
        }],
        raise_first=boom,
    )
    gc = _client(rec)
    holder = {}
    items = []
    async for it in gc.iter_drive_items_by_id(
        "b!abc", delta_token=TOKEN_URL, delta_holder=holder
    ):
        items.append(it)
    # First call = the (dead) token; second = bare full walk with $top.
    assert rec.calls[0] == (TOKEN_URL, None)
    url2, params2 = rec.calls[1]
    assert url2.endswith("/drives/b!abc/root/delta")
    assert params2 == {"$top": "999"}
    assert [i["id"] for i in items] == ["f1"]
    # The full walk's terminal deltaLink replaces the dead token upstream.
    assert holder.get("deltaLink") == "https://g/delta?token=FRESH"


async def test_iter_drive_items_by_id_410_without_token_propagates():
    # A 410 on the BARE url is a different failure — never loop on it.
    boom = RuntimeError("Graph GET 410 Gone: resyncRequired")
    rec = _Recorder(raise_first=boom)
    gc = _client(rec)
    with pytest.raises(RuntimeError, match="410"):
        async for _ in gc.iter_drive_items_by_id("b!abc"):
            pass
    assert len(rec.calls) == 1


async def test_resync_arm_ignores_410_substring_in_unrelated_errors():
    # Drive ids / tokens / request ids routinely contain "410" as a
    # substring — an unrelated failure must PROPAGATE, not trigger a
    # full re-walk (multi-GB re-transfer) that masks the real error.
    boom = RuntimeError(
        "Client error '404 Not Found' for url "
        "'https://graph.microsoft.com/v1.0/drives/b!x4102ycd/root/delta"
        "?token=NDs410LTIz'"
    )
    rec = _Recorder(raise_first=boom)
    gc = _client(rec)
    with pytest.raises(RuntimeError, match="404"):
        async for _ in gc.iter_drive_items_by_id("b!abc", delta_token=TOKEN_URL):
            pass
    assert len(rec.calls) == 1, "false-positive resync fired a second walk"


@pytest.mark.parametrize(
    "method,args",
    [("get_drive_items_delta", ("b!abc",)),
     ("get_sharepoint_site_drives", ("host,site",))],
)
async def test_live_coroutine_paths_have_410_resync_arm(method, args):
    # These are the two token-resume paths production actually calls
    # (backup_onedrive, _backup_files_parallel) — their callers keep the
    # stored token on failure, so an expired token would otherwise fail
    # the same resource every nightly run forever.
    boom = RuntimeError("Client error '410 Gone': resyncRequired")
    rec = _Recorder(
        pages=[{"value": [{"id": "f1"}],
                "@odata.deltaLink": "https://g/delta?token=FRESH"}],
        raise_first=boom,
    )
    gc = _client(rec)
    result = await getattr(gc, method)(*args, delta_token=TOKEN_URL)
    assert rec.calls[0] == (TOKEN_URL, None)
    url2, params2 = rec.calls[1]
    assert url2.endswith("/root/delta") and params2 == {"$top": "999"}
    assert result.get("@odata.deltaLink") == "https://g/delta?token=FRESH"


async def test_iter_sharepoint_site_drive_items_resumes_verbatim():
    rec = _Recorder()
    gc = _client(rec)
    async for _ in gc.iter_sharepoint_site_drive_items(
        "host,site", delta_token=TOKEN_URL
    ):
        pass
    assert rec.calls[0] == (TOKEN_URL, None)


# ── the buffering coroutine sites ────────────────────────────────────

_COROUTINE_SITES = [
    ("get_sharepoint_site_drives", ("host,site",)),
    ("get_sharepoint_site_list_items", ("host,site", "list1")),
    ("get_channel_messages", ("team1", "chan1")),
    ("get_teams_chats", ()),
    ("get_calendar_events_delta", ("user1",)),
    ("get_drive_items_delta", ("b!abc",)),
    ("get_group_mailbox_messages", ("group1",)),
]


@pytest.mark.parametrize("method,args", _COROUTINE_SITES)
async def test_coroutine_sites_resume_verbatim(method, args):
    rec = _Recorder()
    gc = _client(rec)
    await getattr(gc, method)(*args, delta_token=TOKEN_URL)
    url, params = rec.calls[0]
    assert url == TOKEN_URL
    assert params is None, (
        f"{method} resume must not pass params — httpx>=0.28 would strip "
        "token=… from the deltaLink (and the calendar window bounds with it)"
    )


@pytest.mark.parametrize("method,args", _COROUTINE_SITES)
async def test_coroutine_sites_baseline_still_sends_params(method, args):
    rec = _Recorder()
    gc = _client(rec)
    await getattr(gc, method)(*args)
    url, params = rec.calls[0]
    assert params, f"{method} baseline lost its query options"


# ── image defense in depth ───────────────────────────────────────────


@pytest.mark.parametrize("worker", ["backup-worker", "restore-worker"])
def test_worker_images_pin_httpx_below_028(worker):
    # Both images build tonight from their own requirements.txt (the base
    # ==0.26.0 pin is NOT installed by the worker Dockerfiles) — a floating
    # httpx is exactly how 0.28.1 and its params-REPLACE semantics shipped.
    req = (REPO / "workers" / worker / "requirements.txt").read_text()
    line = next(l for l in req.splitlines() if l.strip().startswith("httpx"))
    assert "<0.28" in line.replace(" ", ""), (
        f"{worker} floats httpx across the 0.28 query-semantics change "
        "that stripped deltaLink tokens fleet-wide"
    )
