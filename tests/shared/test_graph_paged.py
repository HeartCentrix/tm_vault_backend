"""Tests for paged async-generator Graph discovery (Task 4).

discover_users_paged / discover_mailboxes_paged yield one normalized batch
per Graph page so the worker can stage per page and never materialize all
users at once. We build a GraphClient without running __init__/network via
GraphClient.__new__ and monkeypatch the page iterator (_iter_pages).
"""
from __future__ import annotations

import asyncio
import os

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "test_secret_for_unit_tests_only_x")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")
os.environ.setdefault("AUDIT_SERVICE_URL", "http://localhost:8000")

from shared.graph_client import GraphClient


def _page_stream(fetch):
    async def pages(url, params=None):
        while url:
            page = await fetch(url, params=params)
            yield page
            url = page.get("@odata.nextLink")
            params = None
    return pages


def test_discover_users_paged_yields_batches():
    g = GraphClient.__new__(GraphClient)

    # Two fake Graph pages: page1 = 2 users + nextLink, page2 = 1 user, no nextLink.
    page1 = {
        "value": [
            {"id": "u1", "displayName": "User One", "mail": "u1@x.com",
             "userPrincipalName": "u1@x.com", "userType": "Member", "accountEnabled": True},
            {"id": "u2", "displayName": "User Two", "mail": "u2@x.com",
             "userPrincipalName": "u2@x.com", "userType": "Member", "accountEnabled": True},
        ],
        "@odata.nextLink": "https://graph.microsoft.com/v1.0/users?$skiptoken=abc",
    }
    page2 = {
        "value": [
            {"id": "u3", "displayName": "User Three", "mail": "u3@x.com",
             "userPrincipalName": "u3@x.com", "userType": "Member", "accountEnabled": True},
        ],
    }

    calls = {"n": 0}

    async def fake_get(url, params=None):
        calls["n"] += 1
        return page1 if calls["n"] == 1 else page2

    g._iter_pages = _page_stream(fake_get)

    async def _collect():
        return [len(b) async for b in g.discover_users_paged()]

    # Drive on a dedicated loop so we leave the global default loop untouched
    # (sibling shared tests reuse asyncio.get_event_loop()).
    batches = asyncio.new_event_loop().run_until_complete(_collect())
    assert batches == [2, 1]
    assert calls["n"] == 2  # one GET per page (initial + nextLink follow)


GRAPH_URL = "https://graph.microsoft.com/v1.0"


def _user(uid, *, mail, name="Name", utype="Member", upn=None):
    return {
        "id": uid,
        "displayName": name,
        "mail": mail,
        "userPrincipalName": upn or (mail or f"{uid}@x.com"),
        "userType": utype,
        "accountEnabled": True,
        "jobTitle": "Engineer",
        "department": "IT",
        "createdDateTime": "2020-01-01T00:00:00Z",
    }


class _FakeBatch:
    """Records every BatchClient call and answers from a status/body map.

    Each call to ``batch(reqs)`` records the list of (id, method, url)
    tuples so tests can assert which endpoints were batched and that no
    url contains ``$top``. Responses are produced from a per-url-substring
    table keyed by what the url contains ("mailboxSettings" / "messages").
    """

    def __init__(self):
        from shared.graph_batch import BatchResponse
        self._BR = BatchResponse
        self.calls = []  # list of {"kind": str, "reqs": [BatchRequest...]}
        # id -> (status, body) for mailboxSettings calls
        self.settings = {}
        # id -> (status, body) for messages fallback calls
        self.messages = {}

    async def __call__(self, reqs):
        kind = "messages" if reqs and "/messages" in reqs[0].url else "mailboxSettings"
        self.calls.append({"kind": kind, "reqs": list(reqs)})
        table = self.messages if kind == "messages" else self.settings
        out = {}
        for r in reqs:
            status, body = table.get(r.id, (404, {}))
            out[r.id] = self._BR(id=r.id, status=status, headers={}, body=body)
        return out


def test_discover_mailboxes_paged_batched_flow():
    """discover_mailboxes_paged batched-flow behavior:
    1. Yields one batch per Graph users page.
    2. Guest + system mailboxes pre-filtered (no batch request built).
    3. Users with no mail field skipped (no batch request, not emitted).
    4. mailboxSettings 200 + userPurpose → typed mailbox; 200 + no userPurpose
       → MAILBOX (no fallback issued).
    5. mailboxSettings 404 → skipped, NO fallback /messages issued for it.
    6. mailboxSettings 403 → fallback /messages batch; 200 + non-empty value
       → emitted as MAILBOX (purpose probed).
    7. The primary path uses the batch helper (not per-user _get); no
       BatchRequest url contains $top.
    """
    g = GraphClient.__new__(GraphClient)

    page1_users = {
        "value": [
            _user("u1", mail="alice@x.com", name="Alice"),          # 200 shared
            _user("u_np", mail="np@x.com", name="No Purpose"),       # 200 no userPurpose
            _user("u_404", mail="gone@x.com", name="Gone"),          # 404 → skip
            _user("u_403", mail="amb@x.com", name="Ambiguous"),      # 403 → fallback
            _user("u2", mail=None, name="No Mail User"),             # no mail → skip
            _user("u_guest", mail="guest@external.com", utype="Guest"),
            _user("u_sys", mail="system@x.com", name="SystemMailbox{abc}",
                  upn="SystemMailbox{abc}@x.com"),
        ],
        "@odata.nextLink": f"{GRAPH_URL}/users?$skiptoken=tok1",
    }
    page2_users = {
        "value": [_user("u3", mail="bob@x.com", name="Bob")],  # 200 user → MAILBOX
    }

    user_calls = {"n": 0}

    async def fake_get(url, params=None):
        if url == f"{GRAPH_URL}/users":
            user_calls["n"] += 1
            return page1_users
        if "skiptoken" in url:
            user_calls["n"] += 1
            return page2_users
        raise AssertionError(f"Unexpected per-user _get call (should be batched): {url!r}")

    fake_batch = _FakeBatch()
    fake_batch.settings = {
        "u1": (200, {"userPurpose": "shared"}),
        "u_np": (200, {}),               # 200, no userPurpose → MAILBOX
        "u_404": (404, {}),
        "u_403": (403, {}),
        "u3": (200, {"userPurpose": "user"}),
    }
    fake_batch.messages = {
        "u_403": (200, {"value": [{"id": "m1"}]}),  # has messages → emit MAILBOX
    }

    g._iter_pages = _page_stream(fake_get)
    g.batch = fake_batch

    async def _collect():
        out = []
        async for b in g.discover_mailboxes_paged():
            out.append(b)
        return out

    loop = asyncio.new_event_loop()
    batches = loop.run_until_complete(_collect())
    loop.close()

    # 2 users pages → 2 yielded batches
    assert len(batches) == 2, f"expected 2 batches, got {len(batches)}"

    by_email = {r["email"]: r for b in batches for r in b}

    # Page 1 emitted: alice (shared), no-purpose (MAILBOX), ambiguous (probed MAILBOX)
    assert by_email["alice@x.com"]["type"] == "SHARED_MAILBOX"
    assert by_email["np@x.com"]["type"] == "MAILBOX"
    assert by_email["amb@x.com"]["type"] == "MAILBOX"
    assert by_email["amb@x.com"]["metadata"]["mailbox_purpose"] == "probed"
    # Page 2
    assert by_email["bob@x.com"]["type"] == "MAILBOX"

    # Excluded entirely
    assert "gone@x.com" not in by_email, "404 must be skipped"
    assert "guest@external.com" not in by_email, "guest must be excluded"
    assert "system@x.com" not in by_email, "system mailbox must be excluded"
    assert by_email.get("np@x.com") is not None
    # u2 (no mail) never emitted
    assert all(r["email"] for b in batches for r in b)

    # --- primary path used the batch helper, not per-user _get ---
    assert user_calls["n"] == 2, "only the 2 users-pages should hit _get"

    settings_calls = [c for c in fake_batch.calls if c["kind"] == "mailboxSettings"]
    messages_calls = [c for c in fake_batch.calls if c["kind"] == "messages"]
    assert len(settings_calls) == 2, "one mailboxSettings batch per users page"

    # Fallback only fired for the 403 user (u_403); 404 (u_404) must NOT be probed.
    probed_ids = {r.id for c in messages_calls for r in c["reqs"]}
    assert probed_ids == {"u_403"}, f"only 403 user should be probed, got {probed_ids}"
    assert "u_404" not in probed_ids, "404 must NOT trigger a /messages fallback"

    # --- $top never appears in any BatchRequest url (validator would reject it) ---
    all_urls = [r.url for c in fake_batch.calls for r in c["reqs"]]
    assert all_urls, "expected some batch requests"
    assert all("$top=" not in u for u in all_urls), "no batch url may contain $top"
    # mailboxSettings sub-requests target the right endpoint with $select
    assert any("mailboxSettings" in u and "$select" in u for u in all_urls)


def test_discover_mailboxes_paged_no_userpurpose_makes_no_fallback():
    """A 200 with no userPurpose proves the mailbox exists → MAILBOX, and must
    NOT trigger any /messages fallback batch."""
    g = GraphClient.__new__(GraphClient)

    async def fake_get(url, params=None):
        if url == f"{GRAPH_URL}/users":
            return {"value": [_user("u1", mail="a@x.com")]}
        raise AssertionError(f"unexpected _get {url!r}")

    fake_batch = _FakeBatch()
    fake_batch.settings = {"u1": (200, {})}  # 200, no userPurpose

    g._iter_pages = _page_stream(fake_get)
    g.batch = fake_batch

    async def _collect():
        rows = []
        async for b in g.discover_mailboxes_paged():
            rows.extend(b)
        return rows

    loop = asyncio.new_event_loop()
    rows = loop.run_until_complete(_collect())
    loop.close()

    assert len(rows) == 1 and rows[0]["type"] == "MAILBOX"
    assert not [c for c in fake_batch.calls if c["kind"] == "messages"], \
        "200-without-userPurpose must not issue a /messages fallback"


def test_discover_mailboxes_paged_429_routes_to_fallback():
    """mailboxSettings 429 (throttled) → /messages fallback; probe 200 with
    non-empty value → emitted as MAILBOX.  Stale-mark safety: a transient
    throttle must not silently drop a real mailbox."""
    g = GraphClient.__new__(GraphClient)

    async def fake_get(url, params=None):
        if url == f"{GRAPH_URL}/users":
            return {"value": [_user("u_throttled", mail="throttled@x.com", name="Throttled")]}
        raise AssertionError(f"unexpected _get {url!r}")

    fake_batch = _FakeBatch()
    fake_batch.settings = {
        "u_throttled": (429, {}),  # throttled → must go to fallback
    }
    fake_batch.messages = {
        "u_throttled": (200, {"value": [{"id": "m1"}]}),  # probe succeeds
    }

    g._iter_pages = _page_stream(fake_get)
    g.batch = fake_batch

    async def _collect():
        rows = []
        async for b in g.discover_mailboxes_paged():
            rows.extend(b)
        return rows

    loop = asyncio.new_event_loop()
    rows = loop.run_until_complete(_collect())
    loop.close()

    # The throttled user must appear in the fallback /messages batch.
    messages_calls = [c for c in fake_batch.calls if c["kind"] == "messages"]
    assert messages_calls, "429 must trigger a /messages fallback batch"
    probed_ids = {r.id for c in messages_calls for r in c["reqs"]}
    assert "u_throttled" in probed_ids, "throttled user must be in /messages probe"

    # And must be emitted after a successful probe.
    assert len(rows) == 1, f"expected 1 emitted mailbox, got {len(rows)}"
    assert rows[0]["email"] == "throttled@x.com"
    assert rows[0]["type"] == "MAILBOX"
    assert rows[0]["metadata"]["mailbox_purpose"] == "probed"


def test_discover_mailboxes_paged_503_routes_to_fallback():
    """mailboxSettings 503 (transient gateway error) → /messages fallback;
    probe 200 with non-empty value → emitted as MAILBOX.  Stale-mark safety:
    a transient 5xx must not silently drop a real mailbox."""
    g = GraphClient.__new__(GraphClient)

    async def fake_get(url, params=None):
        if url == f"{GRAPH_URL}/users":
            return {"value": [_user("u_503", mail="gw_error@x.com", name="GatewayError")]}
        raise AssertionError(f"unexpected _get {url!r}")

    fake_batch = _FakeBatch()
    fake_batch.settings = {
        "u_503": (503, {}),  # transient gateway error → must go to fallback
    }
    fake_batch.messages = {
        "u_503": (200, {"value": [{"id": "m2"}]}),  # probe succeeds
    }

    g._iter_pages = _page_stream(fake_get)
    g.batch = fake_batch

    async def _collect():
        rows = []
        async for b in g.discover_mailboxes_paged():
            rows.extend(b)
        return rows

    loop = asyncio.new_event_loop()
    rows = loop.run_until_complete(_collect())
    loop.close()

    # The 503 user must appear in the fallback /messages batch.
    messages_calls = [c for c in fake_batch.calls if c["kind"] == "messages"]
    assert messages_calls, "503 must trigger a /messages fallback batch"
    probed_ids = {r.id for c in messages_calls for r in c["reqs"]}
    assert "u_503" in probed_ids, "503 user must be in /messages probe"

    # And must be emitted after a successful probe.
    assert len(rows) == 1, f"expected 1 emitted mailbox, got {len(rows)}"
    assert rows[0]["email"] == "gw_error@x.com"
    assert rows[0]["type"] == "MAILBOX"
    assert rows[0]["metadata"]["mailbox_purpose"] == "probed"
