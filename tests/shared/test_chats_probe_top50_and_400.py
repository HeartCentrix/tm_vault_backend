"""Regression: USER_CHATS probe must respect Graph's /chats $top cap and a
Graph 400 must not be classified as a permanent license failure.

Bug (RCA 2026-07-30): the per-user chats probe sent ``$top=999`` to
``GET /users/{id}/chats``; Graph caps ``$top`` at 50 on /chats and returns
HTTP 400. ``"400"`` sat in ``_PERMANENT_STATUSES``, so the probe stamped
``license_missing=True`` and tier2_discovery mapped the row to INACCESSIBLE —
silently blocking chat backups for 7 USER_CHATS resources. The trigger was
36b0bcf (the params fix): before it, ``$top`` was silently dropped and the
malformed value never reached Graph.

A 400 is a malformed request (our bug), not a license signal — it must fall
through to the self-healing ``discovery_pending`` marker instead.
"""
from __future__ import annotations

import asyncio
import pathlib
from urllib.parse import parse_qsl, urlsplit

from shared.graph_client import GraphClient

_SRC = (
    pathlib.Path(__file__).resolve().parents[2] / "shared" / "graph_client.py"
).read_text()


def _mk_client() -> GraphClient:
    return GraphClient(client_id="x", client_secret="y", tenant_id="z")


def _patch_fast_sleep(monkeypatch):
    async def _nosleep(_secs):
        return None

    monkeypatch.setattr(asyncio, "sleep", _nosleep)


def _run_discover(monkeypatch, fake_get):
    _patch_fast_sleep(monkeypatch)
    monkeypatch.setattr(GraphClient, "_get", fake_get)
    client = _mk_client()
    return asyncio.run(
        client.discover_user_content("uid-1", "user.one@example.com", "User One")
    )


def test_chats_probe_sends_top_50(monkeypatch):
    calls = []

    async def fake_get(self, url, params=None, **kwargs):
        calls.append((url, dict(params or {})))
        return {"value": []}

    _run_discover(monkeypatch, fake_get)

    chats_calls = [(u, p) for u, p in calls if urlsplit(u).path.endswith("/users/uid-1/chats")]
    assert chats_calls, "chats probe never hit /users/{id}/chats"
    for _url, params in chats_calls:
        params = {**dict(parse_qsl(urlsplit(_url).query)), **params}
        assert params.get("$top") == "50", (
            f"/chats $top must be 50 (Graph cap); got {params.get('$top')!r}"
        )


def test_chats_probe_400_marks_discovery_pending_not_license_missing(monkeypatch):
    async def fake_get(self, url, params=None, **kwargs):
        if urlsplit(url).path.endswith("/chats"):
            raise Exception(
                "Client error '400 Bad Request' for url "
                "'https://graph.microsoft.com/v1.0/users/uid-1/chats'"
            )
        return {"value": []}

    children = _run_discover(monkeypatch, fake_get)

    chats = [c for c in children if c.get("type") == "USER_CHATS"]
    assert chats, "400 on the chats probe must still emit a marker row"
    meta = chats[0]["metadata"]
    assert not meta.get("license_missing"), (
        "400 must NOT be treated as a license failure (it is a malformed "
        "request); got license_missing=True"
    )
    assert meta.get("discovery_pending") is True, (
        "400 must fall through to the self-healing discovery_pending marker"
    )


def test_chats_probe_403_still_marks_license_missing(monkeypatch):
    async def fake_get(self, url, params=None, **kwargs):
        if urlsplit(url).path.endswith("/chats"):
            raise Exception(
                "Client error '403 Forbidden' for url "
                "'https://graph.microsoft.com/v1.0/users/uid-1/chats'"
            )
        return {"value": []}

    children = _run_discover(monkeypatch, fake_get)

    chats = [c for c in children if c.get("type") == "USER_CHATS"]
    assert chats
    meta = chats[0]["metadata"]
    assert meta.get("license_missing") is True
    assert meta.get("probe_status") == "403"


def test_permanent_statuses_exclude_400():
    assert (
        '_PERMANENT_STATUSES = ("401", "403", "404", "410", "423")' in _SRC
    ), "400 must not be in _PERMANENT_STATUSES (malformed request != permanent)"


def test_bulk_chats_discovery_uses_top_50():
    idx = _SRC.find("async def _process_user_chats")
    assert idx != -1
    blk = _SRC[idx : idx + 1500]
    assert '"$top": "50"' in blk
    assert '"$top": "999"' not in blk
