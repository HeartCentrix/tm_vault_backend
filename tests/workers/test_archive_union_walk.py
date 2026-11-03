"""ITO-10146 — multi-location union enumeration of an autoexpanded archive.

Reproduced live 2026-08-18 against jrue's archive (162GB, 1,114,978 items):

  * Graph's folder listings return only the children RESIDENT in the
    mailbox location that serves the call — ``childFolders`` declared 23,
    returned 2, HTTP 200, **no nextLink**, ``$top``-independent (3/3
    attempts). The lost residents ARE served when their own location's root
    is enumerated directly.
  * Folder ids are LOCATION-SCOPED: an aux-minted id 404s against the
    primary base. The same logical folder therefore has one FACET per
    location, and the tree must be unioned by PATH.
  * Items live in exactly one location, so deduping by item id collapses
    facet overlap.
  * These endpoints default to 10-per-page and honor ``$top`` (folders
    $top=100 returned 5 top-level folders the default page never
    surfaced; items $top=1000 → 1000/page).
  * No API lists auxiliary mailboxes. They are discovered organically from
    ``@odata.context`` re-basing — and, for prior-snapshot folders that no
    walked location lists, from the reissue URL inside an exportItems
    per-item ``ErrorArchiveFolderMovedPermanently`` error (the probe).
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
_spec = importlib.util.spec_from_file_location("archive_backup_union_uut", _MOD_PATH)
ab = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["archive_backup_union_uut"] = ab
assert _spec and _spec.loader
_spec.loader.exec_module(ab)  # type: ignore[union-attr]

GRAPH = "https://graph.microsoft.com/beta"
PRI = "MBX:primary@t"
AUX = "MBX:aux@t"
AUX2 = "MBX:aux2@t"


def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _http(status, url="https://graph.microsoft.com/beta/x"):
    req = httpx.Request("GET", url)
    resp = httpx.Response(status, request=req, json={
        "error": {"code": "ErrorInternalServerError", "message": "boom"}})
    return httpx.HTTPStatusError(f"HTTP {status}", request=req, response=resp)


class Loc:
    """One mailbox location: a root listing plus facet specs.

    facet spec keys: name, declared (childFolderCount), children ([facet ids]),
    children_loc (Loc whose ids/context serve the childFolders response —
    models the 308-rebase), items ([dicts]), items_error (HTTP status)."""

    def __init__(self, mbx):
        self.mbx = mbx
        self.root: list[str] = []
        self.f: dict[str, dict] = {}

    def base(self) -> str:
        return f"{GRAPH}/admin/exchange/mailboxes/{self.mbx}/folders"

    def ctx(self, leaf: str) -> str:
        return (f"{GRAPH}/$metadata#admin/exchange/mailboxes/{self.mbx}"
                f"/folders('AQx')/{leaf}")


class MultiLocGraph:
    def __init__(self, *locs, pages_per_get: int = 1):
        self.locs = {l.base().lower(): l for l in locs}
        self.by_obj = list(locs)
        self.get_calls: list[tuple[str, dict]] = []
        self.post_calls: list[tuple[str, list]] = []
        self.pages_per_get = pages_per_get
        self.export_behavior = None
        self.root_errors: dict[str, int] = {}  # base().lower() -> HTTP status

    def _fobj(self, loc: Loc, fid: str) -> dict:
        spec = loc.f[fid]
        return {"id": fid, "displayName": spec["name"],
                "childFolderCount": spec.get(
                    "declared", len(spec.get("children", [])))}

    async def _get(self, url, params=None, on_activity=None):
        self.get_calls.append((url, dict(params or {})))
        if on_activity is not None:
            for _ in range(self.pages_per_get):
                on_activity()
        low = url.lower()
        for b, loc in self.locs.items():
            if low == b:
                if b in self.root_errors:
                    raise _http(self.root_errors[b], url)
                return {"value": [self._fobj(loc, fid) for fid in loc.root],
                        "@odata.context": loc.ctx("")}
            if low.startswith(b + "/"):
                tail = url[len(b) + 1:]
                fid, _, leaf = tail.partition("/")
                spec = loc.f.get(fid)
                if spec is None:
                    raise _http(404, url)
                if leaf == "items":
                    if spec.get("items_error"):
                        raise _http(spec["items_error"], url)
                    return {"value": [dict(i) for i in spec.get("items", [])],
                            "@odata.context": loc.ctx("items")}
                if leaf == "childFolders":
                    serve = spec.get("children_loc", loc)
                    kids = spec.get("children", [])
                    return {"value": [self._fobj(serve, k) for k in kids],
                            "@odata.context": serve.ctx("childFolders")}
        raise _http(404, url)

    async def _post_json(self, url, body, timeout=None):
        ids = list(body["itemIds"])
        self.post_calls.append((url, ids))
        if self.export_behavior is not None:
            return {"value": self.export_behavior(url, ids)}
        return {"value": [
            {"itemId": i, "changeKey": f"ck-{i}", "data": f"blob-{i}"}
            for i in ids
        ]}


def _leaf(name, *items):
    return {"name": name, "declared": 0,
            "items": [{"id": i, "changeKey": f"ck-{i}", "size": 1, "type": "m"}
                      for i in items]}


def _two_location_archive(*, aux_children=("a-c", "a-d", "a-e")):
    """Primary lists facet 'Term - X' (declared 5) whose childFolders are
    SERVED by the aux (rebase) and carry only aux residents A,B; the aux
    root's own facet of 'Term - X' serves the rest. Plus a plain primary
    Inbox. Mirrors the live shape exactly."""
    pri, aux = Loc(PRI), Loc(AUX)
    pri.root = ["p-termx", "p-inbox"]
    pri.f["p-inbox"] = _leaf("Inbox", "i1", "i2")
    pri.f["p-termx"] = {"name": "Term - X", "declared": 5,
                        "children": ["a-a", "a-b"], "children_loc": aux,
                        "items": []}
    aux.root = ["a-termx"]
    aux.f["a-termx"] = {"name": "Term - X", "declared": 5,
                        "children": list(aux_children),
                        "items": [{"id": "tx1", "changeKey": "ck-tx1",
                                   "size": 1, "type": "m"}]}
    aux.f["a-a"] = _leaf("A", "a1")
    aux.f["a-b"] = _leaf("B", "b1")
    aux.f["a-c"] = _leaf("C", "c1")
    aux.f["a-d"] = _leaf("D", "d1")
    aux.f["a-e"] = _leaf("E", "e1")
    return pri, aux


def _walk(graph, **kw):
    return _run(ab._walk_folders(
        graph, GRAPH, PRI, concurrency=4, worker_id="w", **kw))


# ───────────────────────── union enumeration ─────────────────────────

def test_union_walk_recovers_aux_resident_subtrees():
    pri, aux = _two_location_archive()
    g = MultiLocGraph(pri, aux)
    items, failures = _walk(g)
    assert sorted(i["id"] for i in items) == [
        "a1", "b1", "c1", "d1", "e1", "i1", "i2", "tx1",
    ], "aux-root residents must be unioned into the enumeration"
    assert failures == [], (
        f"union satisfied the declared count — no failure expected: {failures}"
    )


def test_union_shortfall_recorded_once_with_union_numbers():
    pri, aux = _two_location_archive(aux_children=("a-c",))
    g = MultiLocGraph(pri, aux)
    items, failures = _walk(g)
    trunc = [f for f in failures if f.get("kind") == "subtree_truncated"]
    assert len(trunc) == 1
    assert trunc[0]["path"] == "/Term - X/*"
    assert trunc[0]["declared_child_folders"] == 5
    assert trunc[0]["returned_child_folders"] == 3  # A,B (pri facet) + C (aux)


def test_items_failure_healed_by_another_location():
    pri, aux = _two_location_archive()
    pri.f["p-termx"]["items_error"] = 500  # aux facet still serves tx1
    g = MultiLocGraph(pri, aux)
    items, failures = _walk(g)
    assert "tx1" in {i["id"] for i in items}
    assert failures == [], (
        "an items 500 on one facet is not a loss when another location "
        f"serves the folder: {failures}"
    )


def test_items_failure_everywhere_counts_once():
    pri, aux = _two_location_archive()
    pri.f["p-termx"]["items_error"] = 500
    aux.f["a-termx"]["items_error"] = 500
    g = MultiLocGraph(pri, aux)
    items, failures = _walk(g)
    bad = [f for f in failures if f.get("kind") == "items_unreadable"]
    assert len(bad) == 1 and bad[0]["path"] == "/Term - X"
    assert "tx1" not in {i["id"] for i in items}


def test_walk_sends_explicit_page_sizes():
    pri, aux = _two_location_archive()
    g = MultiLocGraph(pri, aux)
    _walk(g, folder_page_size=200, item_page_size=1000)
    for url, params in g.get_calls:
        if url.endswith("/items"):
            assert params.get("$top") == "1000", f"items call missing $top: {url}"
            assert params.get("$select") == "id,changeKey,size,type"
        else:
            assert params.get("$top") == "200", f"folder call missing $top: {url}"


def test_pagination_counts_as_progress():
    """562-event root cause: one _get paginating a giant folder must tick
    progress per page, not once per folder."""
    pri, aux = _two_location_archive()
    g = MultiLocGraph(pri, aux, pages_per_get=7)
    ticks = {"n": 0}
    _walk(g, on_progress=lambda: ticks.__setitem__("n", ticks["n"] + 1))
    n_gets = len(g.get_calls)
    assert ticks["n"] >= n_gets * 7, (
        f"expected ≥{n_gets * 7} page ticks, saw {ticks['n']} — "
        "on_activity is not threaded through the walk"
    )


def test_location_cap_is_recorded():
    pri, aux = _two_location_archive()
    g = MultiLocGraph(pri, aux)
    items, failures = _walk(g, max_locations=1)  # primary only
    over = [f for f in failures if f.get("kind") == "locations_over_cap"]
    assert over, "hitting ARCHIVE_MAX_LOCATIONS must be visible in failures"
    # DISTINCT locations, not sightings: the same over-cap location is
    # re-discovered on every response it serves (review finding).
    assert "1 discovered mailbox" in over[0]["message"], over[0]["message"]


def test_resident_facet_failure_is_not_healed_by_an_empty_200():
    """Review finding (2026-08-18): items live in exactly ONE location, so a
    non-resident facet answers 200-EMPTY by design — an empty 200 must not
    suppress the resident facet's persistent 500, or the folder's entire
    content vanishes from a snapshot that finalizes COMPLETED."""
    pri, aux = _two_location_archive()
    aux.f["a-termx"]["items_error"] = 500  # the RESIDENT facet (holds tx1)
    # primary facet answers 200-empty (items=[]) — non-resident by design
    g = MultiLocGraph(pri, aux)
    items, failures = _walk(g)
    assert "tx1" not in {i["id"] for i in items}
    bad = [f for f in failures if f.get("kind") == "items_unreadable"]
    assert len(bad) == 1 and bad[0]["path"] == "/Term - X", (
        "a 200-empty facet healed a resident-location 500 — silent data loss"
    )


def test_unreadable_location_root_is_a_recorded_loss():
    """Review finding: a discovered location whose ROOT listing fails loses
    every resident it holds — that must surface in folder_failures (→
    files_failed>0 → PARTIAL), never a clean COMPLETED."""
    pri, aux = _two_location_archive()
    g = MultiLocGraph(pri, aux)
    g.root_errors[aux.base().lower()] = 503
    items, failures = _walk(g)
    lost = [f for f in failures if f.get("kind") == "location_root_unreadable"]
    assert len(lost) == 1 and lost[0]["status"] == 503, (
        f"aux root failure must be a recorded loss: {failures}"
    )


# ───────────────────────── the location probe ─────────────────────────

def _settings(**over):
    ns = types.SimpleNamespace(
        ARCHIVE_GRAPH_VERSION="beta",
        ARCHIVE_IMPORT_TIMEOUT_S=120,
        ARCHIVE_FOLDER_CONCURRENCY=4,
        ARCHIVE_EXPORT_CONCURRENCY=4,
        ARCHIVE_STALL_ABORT_S=0,
    )
    for k, v in over.items():
        setattr(ns, k, v)
    return ns


def _resource():
    return types.SimpleNamespace(
        id="11111111-1111-1111-1111-111111111111",
        external_id=PRI, display_name="jrue archive",
        extra_data={"archiveMailboxId": PRI},
    )


def _snapshot():
    return types.SimpleNamespace(
        id="22222222-2222-2222-2222-222222222222", delta_tokens_json=None)


class _Persist:
    def __init__(self):
        self.stored_ids = []

    async def __call__(self, pairs):
        n = 0
        for it, entry in pairs:
            self.stored_ids.append(it["id"])
            n += 1
        return n, n, 0


async def _no_carry(_t, _p, _i):
    return 0


def test_probe_discovers_location_from_export_redirect():
    """A prior-snapshot folder that NO walked location lists any more is
    located by exporting one of its known item ids: the per-item
    ErrorArchiveFolderMovedPermanently reissue URL names the owning
    mailbox, whose root is then walked and unioned."""
    pri = Loc(PRI)
    pri.root = ["p-inbox"]
    pri.f["p-inbox"] = _leaf("Inbox", "i1")
    aux2 = Loc(AUX2)
    aux2.root = ["z-termy"]
    aux2.f["z-termy"] = {"name": "Term - Y", "declared": 0,
                         "items": [{"id": "y1", "changeKey": "ck-new",
                                    "size": 1, "type": "m"}]}
    g = MultiLocGraph(pri, aux2)

    probe_urls = []

    def export_behavior(url, ids):
        if ids == ["y1"]:  # the probe batch: sampled ids from missing paths only
            probe_urls.append(url)
            return [{"itemId": "y1", "Error": {
                "code": "ErrorArchiveFolderMovedPermanently",
                "message": f"{GRAPH}/admin/exchange/mailboxes/{AUX2}/exportItems",
            }}]
        return [{"itemId": i, "changeKey": f"ck-{i}", "data": f"blob-{i}"}
                for i in ids]

    g.export_behavior = export_behavior

    async def load_prev(_rid, _csid):
        return "prior-snap", {"y1": "ck-old"}

    async def load_prior_paths(_psid):
        return {"/Term - Y": ["y1"], "/Inbox": ["i1"]}

    persist = _Persist()
    result = _run(ab.run_archive_backup(
        g, _resource(), _snapshot(), types.SimpleNamespace(id="t"),
        settings=_settings(),
        load_prev=load_prev,
        load_prior_paths=load_prior_paths,
        persist_batch=persist,
        carry_forward=_no_carry,
    ))
    assert probe_urls, "the probe exportItems call never fired"
    # y1's changeKey changed (ck-old → ck-new) so it must be re-exported
    # from the probed location; i1 is new.
    assert sorted(persist.stored_ids) == ["i1", "y1"]
    assert result["item_count"] == 2


def test_probe_fires_for_listed_but_emptied_path():
    """Review finding: a prior path can be LISTED by a non-resident facet
    (200-empty) while its resident location is undiscovered — probing only
    paths absent from paths_seen never finds it. The probe must also fire
    when none of a path's sampled prior items were enumerated."""
    pri = Loc(PRI)
    pri.root = ["p-inbox", "p-termy"]
    pri.f["p-inbox"] = _leaf("Inbox", "i1")
    pri.f["p-termy"] = {"name": "Term - Y", "declared": 0, "items": []}
    aux2 = Loc(AUX2)
    aux2.root = ["z-termy"]
    aux2.f["z-termy"] = {"name": "Term - Y", "declared": 0,
                         "items": [{"id": "y1", "changeKey": "ck-new",
                                    "size": 1, "type": "m"}]}
    g = MultiLocGraph(pri, aux2)

    def export_behavior(url, ids):
        # y1 lives in AUX2: any POST for it at the PRIMARY url redirects
        # (probe AND real export); the reissue at AUX2 succeeds.
        if ids == ["y1"] and PRI.lower() in url.lower():
            return [{"itemId": "y1", "Error": {
                "code": "ErrorArchiveFolderMovedPermanently",
                "message": f"{GRAPH}/admin/exchange/mailboxes/{AUX2}/exportItems",
            }}]
        return [{"itemId": i, "changeKey": f"ck-{i}", "data": f"blob-{i}"}
                for i in ids]

    g.export_behavior = export_behavior

    async def load_prev(_rid, _csid):
        return "prior-snap", {"y1": "ck-old", "i1": "ck-i1"}

    async def load_prior_paths(_psid):
        # "/Term - Y" IS listed (paths_seen) but serves nothing.
        return {"/Term - Y": ["y1"], "/Inbox": ["i1"]}

    persist = _Persist()
    result = _run(ab.run_archive_backup(
        g, _resource(), _snapshot(), types.SimpleNamespace(id="t"),
        settings=_settings(),
        load_prev=load_prev,
        load_prior_paths=load_prior_paths,
        persist_batch=persist,
        carry_forward=_no_carry,
    ))
    assert "y1" in persist.stored_ids, (
        "listed-but-emptied path never probed — resident location undiscovered"
    )


def test_probe_stamps_progress_per_batch():
    """Review finding: several sequential probe POSTs, each entitled to a
    full retry budget, ran with zero progress stamps — eating into the
    stall guard's window. Every batch (returned or failed) must stamp."""
    class _Client:
        def __init__(self):
            self.n = 0

        async def _post_json(self, url, body, timeout=None):
            self.n += 1
            if self.n == 1:
                raise httpx.ReadError("boom")
            return {"value": []}

    ticks = {"n": 0}
    bases = _run(ab._probe_missing_locations(
        _Client(), "https://x/exportItems", [["a"], ["b"], ["c"]],
        timeout=1, worker_id="w",
        on_activity=lambda: ticks.__setitem__("n", ticks["n"] + 1),
    ))
    assert ticks["n"] == 3 and bases == set()


def test_probe_failure_costs_nothing():
    pri = Loc(PRI)
    pri.root = ["p-inbox"]
    pri.f["p-inbox"] = _leaf("Inbox", "i1")
    g = MultiLocGraph(pri)

    def export_behavior(url, ids):
        raise httpx.ReadError("probe transport died")

    g.export_behavior = export_behavior

    async def load_prev(_rid, _csid):
        return "prior-snap", {"i1": "ck-i1"}

    async def load_prior_paths(_psid):
        return {"/Gone": ["ghost1"], "/Inbox": ["i1"]}

    persist = _Persist()
    result = _run(ab.run_archive_backup(
        g, _resource(), _snapshot(), types.SimpleNamespace(id="t"),
        settings=_settings(ARCHIVE_MIN_COVERAGE_PCT=0),
        load_prev=load_prev,
        load_prior_paths=load_prior_paths,
        persist_batch=persist,
        carry_forward=_no_carry,
    ))
    assert result["item_count"] >= 0  # run survived the dead probe
