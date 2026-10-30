"""Two GraphClient defects found while root-causing the 2026-08-05/06 backup
failures.

(1) ``@odata.context`` was dropped from the paginated result. It is the ONLY
    signal identifying which mailbox actually served a response after httpx
    transparently follows an autoexpanded-archive 308 — without it
    ``archive_backup._walk_folders`` cannot re-base child folder ids and every
    aux-resident subtree 404s (resource "Archive — Jason Rue", 138 consecutive
    FAILED snapshots on 2026-08-06).

(2) ``resp.json()`` was assumed infallible. A truncated / non-JSON 200 body
    raises ``json.JSONDecodeError`` (a ValueError) which escaped the hardened
    path's retry policy entirely and surfaced as a permanent handler failure.
    Prod (Corp_Esplanade ROOM_MAILBOX, 2026-08-06, both attempts)::

        [GraphClient/hardened] transient ReadTimeout on …/messages; sleep 2.3s
        [GraphClient/hardened] 504 on …/messages — migrating app
        Handler FAILED for ROOM_MAILBOX: Corp_Esplanade —
            Expecting ',' delimiter: line 1 column 392644 (char 392643)
          File "shared/graph_client.py", line 762, in _get_hardened
            data = resp.json() or {}

    A different byte offset on each attempt (392644 / 4826748) ⇒ a truncated
    stream, i.e. a TRANSIENT, which must be retried like any other transport
    transient rather than failing the whole backup.
"""
from __future__ import annotations

import asyncio
import json
import os

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "test_secret_for_unit_tests_only_x")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")
os.environ.setdefault("AUDIT_SERVICE_URL", "http://localhost:8000")

import httpx
import pytest

from shared.graph_client import GraphClient, GraphRetryExhaustedError

CTX = ("https://graph.microsoft.com/beta/$metadata#admin/Exchange/Mailboxes/"
       "MBX:aux@tenant/folders('AQMk==')/childFolders")


class _Resp:
    """Fake httpx response. ``raw`` (str) is decoded by json() so we can model a
    truncated body exactly as httpx would."""

    def __init__(self, payload=None, status=200, raw=None, headers=None):
        self._payload = payload
        self._raw = raw
        self.status_code = status
        self.headers = headers or {}
        self.elapsed = None

    def json(self):
        if self._raw is not None:
            return json.loads(self._raw)      # raises JSONDecodeError
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise httpx.HTTPStatusError(
                f"Client error '{self.status_code}'", request=None, response=None)


class _Client:
    def __init__(self, script):
        self.script = list(script)
        self.calls = []

    async def get(self, url, headers=None, params=None):
        self.calls.append(url)
        item = self.script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class _Session:
    def __init__(self, c):
        self.c = c

    async def __aenter__(self):
        return self.c

    async def __aexit__(self, *e):
        return False


class _Bucket:
    async def acquire(self, priority=None):
        return None


class _Action:
    def __init__(self, exhausted=False):
        self.exhausted = exhausted
        self.sleep_seconds = 0.0


class _Policy:
    """``transient_exhausted`` flips decide_transient_error into the
    budget-exhausted branch."""

    def __init__(self, transient_exhausted=False):
        self.stream_bucket = _Bucket()
        self.transient_exhausted = transient_exhausted
        self.transient_calls = 0

    def reset_on_success(self):
        return None

    def decide_transient_error(self):
        self.transient_calls += 1
        return _Action(exhausted=self.transient_exhausted)

    def decide(self, status_code=None, retry_after=None):
        return _Action()


def _bare(script, monkeypatch, policy=None):
    g = GraphClient.__new__(GraphClient)
    g.client_id = "app-1"
    fake = _Client(script)
    pol = policy or _Policy()
    g._http_session = lambda: _Session(fake)
    g._effective_priority = lambda: 5
    # NB: the _policy property reads getattr(self, "__policy_cached") — the
    # UNMANGLED literal — while its setter writes the mangled
    # _GraphClient__policy_cached. Injecting the mangled name (as
    # test_graph_paged_token_refresh.py does) silently fails to take effect, so
    # set the name the getter actually reads.
    setattr(g, "__policy_cached", pol)

    async def _tok():
        return "tok"

    g._get_token = _tok

    async def _no_mig(_a):
        return None, None

    g._try_migrate_app = _no_mig

    async def _noop(*a, **k):
        return None

    monkeypatch.setattr("shared.graph_rate_limiter.graph_rate_limiter.acquire", _noop)
    monkeypatch.setattr(
        "shared.multi_app_manager.multi_app_manager.acquire_app_token", _noop)
    monkeypatch.setattr(
        "shared.multi_app_manager.multi_app_manager.mark_success", lambda *a, **k: None)
    monkeypatch.setattr(
        "shared.multi_app_manager.multi_app_manager.mark_throttled", lambda *a, **k: None)
    monkeypatch.setattr("asyncio.sleep", _noop)
    return g, fake, pol


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


# ───────────────── (1) @odata.context preservation ─────────────────

@pytest.mark.parametrize("path", ["_get_hardened", "_get_legacy"])
def test_odata_context_is_preserved(path, monkeypatch):
    g, _f, _p = _bare([_Resp({"value": [{"id": "f1"}], "@odata.context": CTX})],
                      monkeypatch)
    out = _run(getattr(g, path)("https://graph.microsoft.com/beta/x"))
    assert out.get("@odata.context") == CTX, (
        "@odata.context dropped — archive aux-mailbox re-basing cannot work"
    )
    assert [i["id"] for i in out["value"]] == ["f1"]


def test_odata_context_absent_stays_absent(monkeypatch):
    """Must not invent the key when Graph didn't send one."""
    g, _f, _p = _bare([_Resp({"value": [{"id": "f1"}]})], monkeypatch)
    out = _run(g._get_hardened("https://graph.microsoft.com/beta/x"))
    assert "@odata.context" not in out


def test_single_object_response_unaffected(monkeypatch):
    """A non-collection response returns verbatim (early return path)."""
    g, _f, _p = _bare([_Resp({"id": "u1", "@odata.context": CTX})], monkeypatch)
    out = _run(g._get_hardened("https://graph.microsoft.com/beta/users/u1"))
    assert out == {"id": "u1", "@odata.context": CTX}


# ───────────────── (2) malformed body is transient ─────────────────

TRUNCATED = '{"value":[{"id":"i1"},{"id":"i2"'      # stream cut mid-array


def test_truncated_body_is_retried_and_succeeds(monkeypatch):
    g, fake, pol = _bare(
        [_Resp(raw=TRUNCATED), _Resp({"value": [{"id": "i1"}, {"id": "i2"}]})],
        monkeypatch,
    )
    out = _run(g._get_hardened("https://graph.microsoft.com/beta/x"))
    assert [i["id"] for i in out["value"]] == ["i1", "i2"]
    assert len(fake.calls) == 2, "the truncated page was not retried"
    assert pol.transient_calls == 1, "not accounted against the transient budget"


def test_truncated_body_exhausted_raises_typed_error_not_valueerror(monkeypatch):
    g, _f, _p = _bare([_Resp(raw=TRUNCATED)], monkeypatch,
                      policy=_Policy(transient_exhausted=True))
    with pytest.raises(GraphRetryExhaustedError):
        _run(g._get_hardened("https://graph.microsoft.com/beta/x"))


def test_truncated_body_does_not_leak_jsondecodeerror(monkeypatch):
    """The prod symptom: a bare JSONDecodeError reaching the handler."""
    g, _f, _p = _bare([_Resp(raw=TRUNCATED)], monkeypatch,
                      policy=_Policy(transient_exhausted=True))
    try:
        _run(g._get_hardened("https://graph.microsoft.com/beta/x"))
    except json.JSONDecodeError as e:  # pragma: no cover - the regression
        pytest.fail(f"JSONDecodeError escaped the retry policy: {e}")
    except GraphRetryExhaustedError:
        pass
