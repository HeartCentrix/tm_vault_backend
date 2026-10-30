"""Regression: get_mail_folder_tree must never request ``sizeInBytes``
on v1.0, and a stats-select 400 must degrade — not disable — the walk.

Bug (RCA 2026-08-03): commit 239d2d4 (2026-07-15) added ``sizeInBytes``
to the mailFolders ``$select``. The property exists only on the BETA
mailFolder type; v1.0 rejects the whole request with 400 "Could not
find a property named 'sizeInBytes'". backup-worker's tree fetch
caught the error, set ``folder_tree = {}``, and fell back to a
tokenless whole-mailbox ``/messages`` pull:

  * per-folder delta tokens stopped refreshing (mail_folder_delta —
    newest row 2026-07-15 across ALL mailboxes),
  * fingerprint skips stopped working (nothing to fingerprint),
  * every run re-fetched the full mailbox from Graph (~38 min / 82k
    messages for one pilot user, 3x per day) and re-persisted a full
    ~82k-row set per snapshot → 1.22M snapshot_items rows for one
    user's mail, which is what made the Recovery mail tab take 6-8s
    per account click.

Pins:
  * the stats select asks only for v1.0 properties (totalItemCount,
    unreadItemCount) — never sizeInBytes;
  * with_stats entries keep a literal ``sizeInBytes: 0`` so the
    "total|unread|size" fingerprint format stays stable;
  * if Graph still 400s the stats select (future property drift), the
    walk retries select-minimal and returns PATH-ONLY entries — the
    fingerprint builder only consumes dict entries, so a degraded tree
    drains everything (conservative) instead of skipping everything on
    bogus all-zero fingerprints;
  * 403/404 keep returning {} (no mailbox / no license).
"""
from __future__ import annotations

import asyncio
import types

import httpx
import pytest

from shared.graph_client import GraphClient


def _mk_client() -> GraphClient:
    return GraphClient(client_id="x", client_secret="y", tenant_id="z")


def _http_error(status: int) -> httpx.HTTPStatusError:
    req = httpx.Request("GET", "https://graph.microsoft.com/v1.0/x")
    resp = httpx.Response(status, request=req)
    return httpx.HTTPStatusError(f"{status}", request=req, response=resp)


_ROOTS_PAGE = {
    "value": [
        {
            "id": "f-inbox",
            "displayName": "Inbox",
            "childFolderCount": 0,
            "totalItemCount": 41,
            "unreadItemCount": 3,
        },
        {
            "id": "f-sent",
            "displayName": "Sent Items",
            "childFolderCount": 0,
            "totalItemCount": 17,
            "unreadItemCount": 0,
        },
    ]
}


def _run_tree(monkeypatch, fake_get, **kw):
    monkeypatch.setattr(GraphClient, "_get", fake_get)
    client = _mk_client()
    return asyncio.run(client.get_mail_folder_tree("uid-1", **kw))


def test_v1_select_never_asks_for_sizeinbytes(monkeypatch):
    calls = []

    async def fake_get(self, url, params=None, **kwargs):
        calls.append((url, dict(params or {})))
        if "/beta/" in url:
            return {"value": []}
        return _ROOTS_PAGE

    tree = _run_tree(monkeypatch, fake_get, with_stats=True)

    v1_calls = [(u, p) for u, p in calls if "/beta/" not in u]
    assert v1_calls, "tree walk never called Graph v1.0"
    for _url, params in v1_calls:
        sel = params.get("$select", "")
        assert "sizeInBytes" not in sel, (
            "sizeInBytes is BETA-only on mailFolder; requesting it on v1.0 "
            f"400s the whole walk (got $select={sel!r})"
        )
        assert "totalItemCount" in sel and "unreadItemCount" in sel, (
            "stats mode must still fetch the fingerprint counters"
        )

    entry = tree["f-inbox"]
    assert entry["path"] == "/Inbox"
    assert entry["totalItemCount"] == 41
    assert entry["unreadItemCount"] == 3
    assert entry["sizeInBytes"] == 0, (
        "fingerprint format is 'total|unread|size' — without beta sizes the "
        "key must survive as a literal 0"
    )


def test_beta_enrichment_fills_toplevel_sizes(monkeypatch):
    """The post-walk beta call restores size-sensitive fingerprints for
    top-level folders — draft edits and same-window add+delete change
    sizeInBytes while leaving both counts untouched."""

    async def fake_get(self, url, params=None, **kwargs):
        if "/beta/" in url:
            assert params.get("$select") == "id,sizeInBytes"
            return {"value": [
                {"id": "f-inbox", "sizeInBytes": 123456},
                {"id": "f-unknown", "sizeInBytes": 999},
            ]}
        return _ROOTS_PAGE

    tree = _run_tree(monkeypatch, fake_get, with_stats=True)
    assert tree["f-inbox"]["sizeInBytes"] == 123456
    assert tree["f-sent"]["sizeInBytes"] == 0, "folders beta didn't return keep 0"
    assert "f-unknown" not in tree, "enrichment must never add folders"


def test_beta_enrichment_failure_is_advisory(monkeypatch):
    async def fake_get(self, url, params=None, **kwargs):
        if "/beta/" in url:
            raise _http_error(503)
        return _ROOTS_PAGE

    tree = _run_tree(monkeypatch, fake_get, with_stats=True)
    assert tree["f-inbox"]["sizeInBytes"] == 0
    assert tree["f-inbox"]["totalItemCount"] == 41, (
        "a failed beta call must leave the v1.0 walk untouched"
    )


def test_no_beta_call_without_stats_or_when_degraded(monkeypatch):
    urls = []

    async def fake_get(self, url, params=None, **kwargs):
        urls.append(url)
        return _ROOTS_PAGE

    _run_tree(monkeypatch, fake_get)  # with_stats=False
    assert not any("/beta/" in u for u in urls), (
        "path-only walks must not pay the beta round trip"
    )


def test_stats_400_degrades_to_pathonly_not_empty(monkeypatch):
    """Future property drift: first stats select 400s → retry minimal
    select → path-only entries. NOT {} (that killed delta+fingerprints
    for 3 weeks), and NOT dicts with zeroed counters (all-zero
    fingerprints would match each other run-over-run and wrongly SKIP
    every folder)."""
    attempts = []

    async def fake_get(self, url, params=None, **kwargs):
        params = dict(params or {})
        attempts.append(params.get("$select", ""))
        if "totalItemCount" in params.get("$select", ""):
            raise _http_error(400)
        return _ROOTS_PAGE

    tree = _run_tree(monkeypatch, fake_get, with_stats=True)

    assert len(attempts) >= 2, "400 on the stats select must retry, not bail"
    assert tree, "degraded walk must still return the folder paths"
    assert tree["f-inbox"] == "/Inbox", (
        "degraded entries must be path strings — the fingerprint builder "
        "keys off isinstance(entry, dict)"
    )


def test_403_still_returns_empty_tree(monkeypatch):
    async def fake_get(self, url, params=None, **kwargs):
        raise _http_error(403)

    tree = _run_tree(monkeypatch, fake_get, with_stats=True)
    assert tree == {}


def test_plain_walk_unchanged(monkeypatch):
    calls = []

    async def fake_get(self, url, params=None, **kwargs):
        calls.append(dict(params or {}))
        return _ROOTS_PAGE

    tree = _run_tree(monkeypatch, fake_get)  # with_stats=False
    assert tree["f-inbox"] == "/Inbox"
    for params in calls:
        assert params.get("$select") == "id,displayName,childFolderCount"
