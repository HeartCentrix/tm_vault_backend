"""USER_ARCHIVE: a truncated walk must not become a restore point.

Prod incident 2026-08-07 → 08-10 ("Archive — Jason Rue"). Four consecutive
runs finalized PARTIAL holding **515,649 of 1,114,978 items (46%)** — a
byte-identical item-id set each time — while reporting `folders_failed=3`.
Two whole top-level trees produced nothing:

    /Term - aconfalone - Archive   337,497 items / 27 folders  -> 0
    /Term - Han Kim - Archive      261,978 items / 24 folders  -> 0

Two independent defects, both traced to the 2026-08-06 aux-rebase commit:

  (a) **items-failure discarded the subtree.** On a failed `/items` GET the
      walk called `_record_failure` and `return`ed, never issuing the
      `childFolders` GET. One unreadable folder therefore lost every folder
      beneath it while counting 1 against `max_folder_failures`
      (343,154 items).

  (b) **`childFolders` was silently truncated.** The aux mailbox answered
      `childFolderCount=23` with 2 children and no `@odata.nextLink`.
      `_get` already follows nextLink, so this is truncation, not paging —
      and nothing compared the declared count to the returned list
      (256,321 items, including Han Kim's 255,870-item `Recoverable Items`
      tree, vanished with no `folder_failures` entry at all).

Plus the two guards that would have stopped it being served:
  (c) a coverage floor against the prior snapshot's item count, and
  (d) subtree-level accounting so PARTIAL stops under-reporting ~75x.
"""
from __future__ import annotations

import asyncio
import importlib.util
import os
import pathlib
import sys

import httpx
import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://u:p@localhost/db")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
_MOD_PATH = _REPO_ROOT / "workers" / "backup-worker" / "archive_backup.py"
_spec = importlib.util.spec_from_file_location("archive_backup_coverage_uut", _MOD_PATH)
ab = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["archive_backup_coverage_uut"] = ab
assert _spec and _spec.loader
_spec.loader.exec_module(ab)  # type: ignore[union-attr]

GRAPH = "https://graph.microsoft.com/beta"
MBX = "MBX:aux@tenant"
BASE = f"{GRAPH}/admin/exchange/mailboxes/{MBX}/folders"


def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _walk(g, **kw):
    return _run(ab._walk_folders(g, GRAPH, MBX, concurrency=4,
                                 worker_id="w", **kw))


def _http_error(url: str, status: int, code: str) -> httpx.HTTPStatusError:
    """The exact prod shape for the three poisoned folders:
    500 ErrorInternalServerError 'Cannot query rows in a table.'"""
    req = httpx.Request("GET", url)
    resp = httpx.Response(
        status, request=req,
        json={"error": {"code": code, "message": "Cannot query rows in a table."}},
    )
    return httpx.HTTPStatusError(f"{status}", request=req, response=resp)


class _Tree:
    """Graph over a folder tree.

    `items_fail` folders raise 500 on their `/items` GET but still have
    children — the shape that lost 343,154 items.
    `truncate_to` caps how many children a folder's `childFolders` GET
    returns while `childFolderCount` still declares the real number — the
    shape that lost 256,321.
    """

    def __init__(self, tree, *, items_fail=(), truncate_to=None):
        self.tree = tree                      # fid -> (items, [child fids])
        self.items_fail = set(items_fail)
        self.truncate_to = truncate_to or {}  # fid -> n
        self.gets: list[str] = []

    def _folder_obj(self, fid):
        return {"id": fid, "displayName": fid,
                "childFolderCount": len(self.tree[fid][1])}

    async def _get(self, url, params=None, on_activity=None):
        self.gets.append(url)
        if url == BASE:
            tops = [f for f in self.tree if f.startswith("top")]
            return {"value": [self._folder_obj(f) for f in tops]}
        for leaf in ("items", "childFolders"):
            if not url.endswith(f"/{leaf}"):
                continue
            fid = url[: -len(f"/{leaf}")].rpartition("/")[2]
            assert fid in self.tree, f"unknown folder {fid}"
            if leaf == "items":
                if fid in self.items_fail:
                    raise _http_error(url, 500, "ErrorInternalServerError")
                return {"value": [{**it} for it in self.tree[fid][0]]}
            kids = self.tree[fid][1]
            cap = self.truncate_to.get(fid)
            served = kids[:cap] if cap is not None else kids
            # Deliberately NO @odata.nextLink — that is what makes it
            # truncation rather than pagination.
            return {"value": [self._folder_obj(k) for k in served]}
        raise AssertionError(f"unexpected GET {url}")


def _item(n):
    return {"id": f"i-{n}", "changeKey": f"ck-{n}", "size": 1, "type": "m"}


# ─── (a) an unreadable folder must not discard its subtree ────────────────

def test_items_failure_still_walks_the_subtree():
    """The 343,154-item defect: `/items` 500s on a top folder that has 2
    children holding real mail. The children must still be captured."""
    g = _Tree(
        {
            "top1": ([], ["kid1", "kid2"]),
            "kid1": ([_item(1)], []),
            "kid2": ([_item(2)], []),
        },
        items_fail={"top1"},
    )
    items, failures = _walk(g)

    got = sorted(i["id"] for i in items)
    assert got == ["i-1", "i-2"], (
        f"subtree was discarded on an items failure; captured {got}"
    )
    # The folder's own loss is still recorded so the run finalizes PARTIAL.
    assert len(failures) == 1 and failures[0]["status"] == 500
    assert any("childFolders" in u for u in g.gets), (
        "childFolders was never requested for the failed folder"
    )


def test_items_failure_deep_in_the_tree_still_recurses():
    """Same defect one level down — the loss was recursive."""
    g = _Tree(
        {
            "top1": ([_item(0)], ["mid"]),
            "mid": ([], ["leaf"]),
            "leaf": ([_item(9)], []),
        },
        items_fail={"mid"},
    )
    items, failures = _walk(g)
    assert sorted(i["id"] for i in items) == ["i-0", "i-9"]
    assert len(failures) == 1


def test_folder_path_is_still_tagged_after_an_items_failure():
    """Children of a failed folder must keep a correct `_folder_path`, or the
    restore tree is wrong even though the rows exist."""
    g = _Tree(
        {"top1": ([], ["kid1"]), "kid1": ([_item(1)], [])},
        items_fail={"top1"},
    )
    items, _ = _walk(g)
    assert items[0]["_folder_path"] == "/top1/kid1"


# ─── (b) a truncated childFolders list must be detected ──────────────────

def test_truncated_child_folder_list_is_recorded():
    """The 256,321-item defect: Graph declares 3 children, hands back 1, no
    nextLink. The shortfall must be recorded, not silently accepted."""
    g = _Tree(
        {
            "top1": ([], ["kid1", "kid2", "kid3"]),
            "kid1": ([_item(1)], []),
            "kid2": ([_item(2)], []),
            "kid3": ([_item(3)], []),
        },
        truncate_to={"top1": 1},
    )
    items, failures = _walk(g)

    assert sorted(i["id"] for i in items) == ["i-1"]  # only what Graph served
    trunc = [f for f in failures if f.get("kind") == "subtree_truncated"]
    assert len(trunc) == 1, (
        "a silently short childFolders list produced no failure record — "
        "PARTIAL would report folders_failed=0 while whole trees are missing"
    )
    assert trunc[0]["declared_child_folders"] == 3
    assert trunc[0]["returned_child_folders"] == 1
    assert trunc[0]["code"] == "ChildFolderListTruncated"


def test_complete_child_folder_list_records_nothing():
    """No false positives when Graph is honest."""
    g = _Tree({
        "top1": ([], ["kid1", "kid2"]),
        "kid1": ([_item(1)], []),
        "kid2": ([_item(2)], []),
    })
    items, failures = _walk(g)
    assert sorted(i["id"] for i in items) == ["i-1", "i-2"]
    assert failures == []


def test_truncation_counts_toward_the_folder_failure_cap():
    """A truncated tree is coverage loss, so it must be able to abort a walk
    rather than quietly producing a tiny snapshot."""
    tree = {"top1": ([], [f"k{i}" for i in range(6)])}
    for i in range(6):
        tree[f"k{i}"] = ([_item(i)], [f"g{i}"])
        tree[f"g{i}"] = ([_item(100 + i)], [f"h{i}"])
        tree[f"h{i}"] = ([_item(200 + i)], [])
    # every kN declares 1 child and serves 0
    g = _Tree(tree, truncate_to={f"k{i}": 0 for i in range(6)})
    with pytest.raises(ab.EnumerationFailed) as ei:
        _walk(g, max_folder_failures=3)
    assert "truncated" in str(ei.value).lower()


# ─── (c) the coverage floor ───────────────────────────────────────────────

def test_coverage_floor_default_is_configured():
    from shared.config import settings
    assert getattr(settings, "ARCHIVE_MIN_COVERAGE_PCT", None) == 90


def test_coverage_floor_can_be_disabled():
    """A legitimately bulk-purged mailbox must have an escape hatch, or the
    gate fails every run forever."""
    import importlib
    import shared.config as cfg
    os.environ["ARCHIVE_MIN_COVERAGE_PCT"] = "0"
    try:
        importlib.reload(cfg)
        assert cfg.Settings().ARCHIVE_MIN_COVERAGE_PCT == 0
    finally:
        del os.environ["ARCHIVE_MIN_COVERAGE_PCT"]
        importlib.reload(cfg)


def test_coverage_gate_is_wired_into_the_run_before_export():
    """The gate must fire on the ENUMERATED count, before the export spend —
    the 46% run burned 4.3h of exportItems before finalizing."""
    src = _MOD_PATH.read_text()
    i_gate = src.index("ARCHIVE_MIN_COVERAGE_PCT")
    i_export = src.index("_select_items_to_export(items, prev)")
    assert i_gate < i_export, (
        "coverage gate must precede _select_items_to_export so a truncated "
        "walk fails before paying for the export"
    )
    assert "refusing to finalize a partial restore point" in src


# ─── (d) accounting makes the shortfall visible ───────────────────────────

def test_partial_accounting_reports_coverage_not_just_failure_counts():
    src = _MOD_PATH.read_text()
    for key in ("subtrees_truncated", "child_folders_unreachable",
                "items_enumerated", "items_baseline", "coverage_pct"):
        assert f'"{key}"' in src, (
            f"{key} missing — PARTIAL reported folders_failed=3 while ~48 "
            f"folders produced nothing (a 75x under-report)"
        )
