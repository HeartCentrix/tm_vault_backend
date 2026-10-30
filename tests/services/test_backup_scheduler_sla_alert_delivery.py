"""backup-scheduler → alert-service SLA delivery: only a 2xx is "sent", a
rejected POST says so, and the internal key rides along.

THE BUG (prod, since the SLA check shipped). send_violations_to_alert_service
POSTed to {ALERT_SERVICE_URL}/api/v1/alerts and counted every response as
sent — httpx does not raise on 4xx/5xx and nothing read ``status_code``.
alert-service had no POST route on that path, so every POST was answered 405,
every violation was "sent", and 0 SLA_VIOLATION alerts were ever stored (prod
``alerts`` on 2026-10-14 holds BACKUP_DLQ and WORK_DEAD_LETTER rows only).

The new ``POST /api/v1/alerts`` gates on X-Internal-Api-Key (or an admin
token), so the scheduler must also send the key. The last test validates the
exact JSON the scheduler sends against alert-service's own request model —
the two services deploy independently and this is the only place the
contract is pinned.

Module loading follows tests/services/test_backup_scheduler_deferred_drain.py.
"""
from __future__ import annotations

import asyncio
import importlib.util
import os
import pathlib
import sys
import types
import uuid

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")

_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _load(name: str, path: pathlib.Path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    sys.modules[name] = mod
    assert spec and spec.loader
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


sched = _load("backup_scheduler_main_sla_alerts",
              _ROOT / "services" / "backup-scheduler" / "main.py")

KEY = "test-internal-key-sla"
ALERT_URL = "http://alert-service:8007"


def _violation(**over):
    """A violation exactly as check_sla_violations builds it."""
    v = {
        "resource_id": str(uuid.uuid4()),
        "tenant_id": str(uuid.uuid4()),
        "resource_type": "USER_MAIL",
        "sla_policy_id": str(uuid.uuid4()),
        "sla_policy_name": "Gold",
        "last_backup_at": None,
        "never_backed_up": True,
        "backup_in_progress": False,
        "hours_overdue": None,
        "severity": "CRITICAL",
    }
    v.update(over)
    return v


class _Resp:
    def __init__(self, status_code: int, text: str = ""):
        self.status_code = status_code
        self.text = text


class _FakeClient:
    """Stands in for httpx.AsyncClient: records every POST and answers (or
    raises) from a script, one entry per call."""

    def __init__(self, script):
        self.script = list(script)
        self.posts = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def post(self, url, json=None, headers=None):
        self.posts.append({"url": url, "json": json, "headers": dict(headers or {})})
        nxt = self.script.pop(0)
        if isinstance(nxt, BaseException):
            raise nxt
        return nxt


def _wire(monkeypatch, script, key: str = KEY) -> _FakeClient:
    client = _FakeClient(script)
    # Scoped to the scheduler module's own `httpx` name — not the shared
    # httpx package.
    monkeypatch.setattr(sched, "httpx",
                        types.SimpleNamespace(AsyncClient=lambda *a, **kw: client))
    monkeypatch.setattr(sched.settings, "INTERNAL_API_KEY", key)
    monkeypatch.setattr(sched.settings, "ALERT_SERVICE_URL", ALERT_URL)
    return client


def _send(violations):
    asyncio.run(sched.send_violations_to_alert_service(violations))


# ── status handling ─────────────────────────────────────────────────────

def test_a_4xx_answer_is_a_failure_not_a_send(monkeypatch, capsys):
    """The exact prod shape: alert-service answers 405 — before the fix this
    counted as sent and printed nothing."""
    _wire(monkeypatch, [_Resp(405, "Method Not Allowed")])

    _send([_violation()])

    out = capsys.readouterr().out
    assert "[SLA] alert POST failed status=405 body=Method Not Allowed" in out
    assert "[SLA] Alert delivery: 0 sent, 1 failed" in out


def test_2xx_answers_are_sent_and_nothing_is_reported_failed(monkeypatch, capsys):
    client = _wire(monkeypatch, [_Resp(201), _Resp(200)])

    _send([_violation(), _violation()])

    assert len(client.posts) == 2
    out = capsys.readouterr().out
    assert "failed" not in out


def test_one_bad_answer_does_not_drop_the_rest_of_the_batch(monkeypatch, capsys):
    """Per-violation handling is kept: a transport error, a 500 and a 201 in
    one batch = three attempts, one sent, two failed; the failure body is
    capped at 200 characters."""
    client = _wire(monkeypatch, [RuntimeError("boom"), _Resp(500, "x" * 300), _Resp(201)])

    _send([_violation(), _violation(), _violation()])

    assert len(client.posts) == 3
    out = capsys.readouterr().out
    assert "[SLA] Failed to send violation for resource" in out
    assert "[SLA] alert POST failed status=500 body=" + "x" * 200 + "\n" in out
    assert "[SLA] Alert delivery: 1 sent, 2 failed" in out


# ── the internal key ────────────────────────────────────────────────────

def test_the_internal_key_is_sent_to_the_alert_route(monkeypatch):
    client = _wire(monkeypatch, [_Resp(201)])

    _send([_violation()])

    post = client.posts[0]
    assert post["url"] == f"{ALERT_URL}/api/v1/alerts"
    assert post["headers"] == {"X-Internal-Api-Key": KEY}


def test_no_configured_key_means_no_header(monkeypatch):
    """An empty INTERNAL_API_KEY must not send an empty header — the route
    then answers 401, which the status check now makes visible."""
    client = _wire(monkeypatch, [_Resp(401, "Authentication required")], key="")

    _send([_violation()])

    assert "X-Internal-Api-Key" not in client.posts[0]["headers"]


# ── the cross-service contract ──────────────────────────────────────────

def test_payload_is_accepted_by_alert_services_request_model(monkeypatch):
    """What the scheduler sends must validate against alert-service's
    AlertCreate (extra="forbid"), or the POST is a 422 in production."""
    alert_svc = _load("alert_main_sla_contract",
                      _ROOT / "services" / "alert-service" / "main.py")
    client = _wire(monkeypatch, [_Resp(201)])
    violation = _violation(never_backed_up=False, hours_overdue=13.5,
                           last_backup_at="2026-10-13T01:00:00")

    _send([violation])

    body = client.posts[0]["json"]
    parsed = alert_svc.AlertCreate.model_validate(body)
    assert parsed.type == "SLA_VIOLATION"
    assert parsed.severity == "CRITICAL"
    assert str(parsed.tenant_id) == violation["tenant_id"]
    assert str(parsed.resource_id) == violation["resource_id"]
    assert parsed.resource_type == "USER_MAIL"
    assert parsed.details == violation
    assert parsed.message  # sla_alert_message() tolerated the float hours_overdue
