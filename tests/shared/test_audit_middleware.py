"""Unit tests for shared.audit_middleware — the Ops/Data Check 3 mutation
backstop. Self-contained: patches shared.audit.emit_audit_event so it runs
without a live audit-service and independent of the rest of the suite's env.

Covers: mutating vs non-mutating methods, excluded prefixes (incl. the
recursion guard on /api/v1/audit and the no-dup guard on /api/v1/auth), status
capture, outcome mapping, actor extraction (cookie + bearer + unknown), target
templating, that the request/response body is NEVER buffered (streaming/SSE
safe), and that an app exception propagates while still being audited.

Each case runs inside its own ``asyncio.run`` loop and drains the middleware's
fire-and-forget emit tasks in-loop before asserting — deterministic regardless
of the surrounding suite's event-loop policy.
"""
import asyncio
import os

# Match scripts/run-tests.sh: allow a dev signing secret so importing
# shared.security (in the actor-resolution test) doesn't trip the prod boot
# guard when this file is run via a bare `pytest` invocation.
os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "test-" + "0" * 32)

import pytest

from shared import audit
from shared import audit_middleware as mw


# ---------------------------------------------------------------- harness
class _Recorder:
    def __init__(self):
        self.events = []

    async def __call__(self, **fields):
        self.events.append(fields)


@pytest.fixture()
def recorder(monkeypatch):
    rec = _Recorder()
    monkeypatch.setattr(audit, "emit_audit_event", rec)
    mw._BG_TASKS.clear()
    return rec


def _scope(method="POST", path="/api/v1/tenants", headers=None, client=("10.1.2.3", 5555)):
    hdrs = [(k.encode(), v.encode()) for k, v in (headers or {}).items()]
    return {"type": "http", "method": method, "path": path,
            "headers": hdrs, "client": client}


async def _drive(app, scope, body_chunks=(b'{"ok":1}',)):
    """Invoke an ASGI app once, feeding a request body, collecting response
    messages, then draining the middleware's fire-and-forget emit tasks on the
    SAME loop so assertions are deterministic."""
    sent = []
    recv_queue = list(body_chunks)

    async def receive():
        if recv_queue:
            chunk = recv_queue.pop(0)
            return {"type": "http.request", "body": chunk, "more_body": bool(recv_queue)}
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        sent.append(message)

    exc = None
    try:
        await app(scope, receive, send)
    except Exception as e:  # capture so we can still drain before re-raising
        exc = e
    for _ in range(10):
        pend = [t for t in list(mw._BG_TASKS) if not t.done()]
        if not pend:
            break
        await asyncio.gather(*pend, return_exceptions=True)
    if exc is not None:
        raise exc
    return sent


def _drive_sync(app, scope, **kw):
    return asyncio.run(_drive(app, scope, **kw))


def _terminal_app(status=200, chunks=(b"hello",), read_body=True):
    async def app(scope, receive, send):
        if read_body:
            more = True
            while more:
                msg = await receive()
                more = msg.get("more_body", False)
        await send({"type": "http.response.start", "status": status,
                    "headers": [(b"content-type", b"application/json")]})
        for i, ch in enumerate(chunks):
            await send({"type": "http.response.body", "body": ch,
                        "more_body": i < len(chunks) - 1})
    return app


# ---------------------------------------------------------------- tests
def test_mutating_request_emits_one_event(recorder):
    app = mw.AuditMiddleware(_terminal_app(status=201), service_name="tenant-service")
    _drive_sync(app, _scope(method="POST", path="/api/v1/tenants"))
    assert len(recorder.events) == 1
    ev = recorder.events[0]
    assert ev["action"] == "POST /api/v1/tenants"
    assert ev["outcome"] == "SUCCESS"
    assert ev["details"]["status"] == 201
    assert ev["details"]["service"] == "tenant-service"
    assert ev["details"]["source_ip"] == "10.1.2.3"


@pytest.mark.parametrize("method", ["GET", "HEAD", "OPTIONS"])
def test_non_mutating_methods_never_audited(recorder, method):
    app = mw.AuditMiddleware(_terminal_app(), service_name="x")
    _drive_sync(app, _scope(method=method, path="/api/v1/tenants"))
    assert recorder.events == []


@pytest.mark.parametrize("path", [
    "/health", "/healthz", "/metrics", "/ping",
    "/api/v1/audit/log",          # recursion guard
    "/api/v1/auth/callback",      # semantic events cover this
    "/api/v1/auth/logout",
])
def test_excluded_paths_never_audited(recorder, path):
    app = mw.AuditMiddleware(_terminal_app(), service_name="x")
    _drive_sync(app, _scope(method="POST", path=path))
    assert recorder.events == []


def test_audit_admin_routes_now_audited_but_log_sink_excluded(recorder):
    # Narrowed exclude: only /api/v1/audit/log (the emit target) is excluded;
    # audit-service's own admin mutations ARE audited now.
    app = mw.AuditMiddleware(_terminal_app(), service_name="audit-service")
    _drive_sync(app, _scope(method="POST", path="/api/v1/audit/log"))
    assert recorder.events == []            # recursion guard holds
    _drive_sync(app, _scope(method="POST", path="/api/v1/audit/ingest/graph/t1"))
    assert len(recorder.events) == 1        # ingest is audited
    assert recorder.events[0]["action"] == "POST /api/v1/audit/ingest/graph/t1"


def test_extra_excludes_suppress_hot_path(recorder):
    app = mw.AuditMiddleware(_terminal_app(), service_name="graph-proxy",
                             extra_excludes=("/graph",))
    _drive_sync(app, _scope(method="POST", path="/graph/batch"))
    _drive_sync(app, _scope(method="POST", path="/graph/v1.0/users/x"))
    assert recorder.events == []            # /graph passthrough not audited
    # a non-excluded route on the same service still is
    _drive_sync(app, _scope(method="POST", path="/api/v1/other"))
    assert len(recorder.events) == 1


def test_failure_status_maps_to_failure_outcome(recorder):
    app = mw.AuditMiddleware(_terminal_app(status=403), service_name="x")
    _drive_sync(app, _scope(method="DELETE", path="/api/v1/policies/abc"))
    assert recorder.events[0]["outcome"] == "FAILURE"
    assert recorder.events[0]["details"]["status"] == 403


def test_app_exception_propagates_and_is_audited(recorder):
    async def boom(scope, receive, send):
        raise RuntimeError("kaboom")
    app = mw.AuditMiddleware(boom, service_name="x")
    with pytest.raises(RuntimeError, match="kaboom"):
        _drive_sync(app, _scope(method="POST", path="/api/v1/tenants"))
    assert len(recorder.events) == 1
    assert recorder.events[0]["outcome"] == "FAILURE"
    assert recorder.events[0]["details"]["status"] == 500


def test_target_templating_and_resource_extraction(recorder):
    app = mw.AuditMiddleware(_terminal_app(), service_name="x")
    uid = "163c487b-14b9-4105-bf06-3494cf11190d"
    sid = "c4fec5a4-e9d0-4755-84fe-5c7cdb31e720"
    _drive_sync(app, _scope(method="DELETE",
                            path=f"/api/v1/tenants/{uid}/secrets/{sid}"))
    ev = recorder.events[0]
    assert ev["action"] == "DELETE /api/v1/tenants/{id}/secrets/{id}"
    assert ev["resource_type"] == "tenants"
    assert ev["resource_id"] == uid
    assert ev["tenant_id"] == uid


def test_actor_unknown_when_no_token(recorder):
    app = mw.AuditMiddleware(_terminal_app(), service_name="x")
    _drive_sync(app, _scope(method="POST", path="/api/v1/tenants"))
    ev = recorder.events[0]
    assert ev["actor_type"] == "UNKNOWN"
    assert ev["actor_id"] is None


def test_actor_unknown_for_garbage_bearer(recorder):
    app = mw.AuditMiddleware(_terminal_app(), service_name="x")
    _drive_sync(app, _scope(method="POST", path="/api/v1/tenants",
                            headers={"authorization": "Bearer not-a-jwt"}))
    assert recorder.events[0]["actor_type"] == "UNKNOWN"


def test_actor_resolved_from_valid_token(recorder, monkeypatch):
    import shared.security as sec
    monkeypatch.setattr(sec, "decode_token",
                        lambda tok, expected_type="access": {
                            "sub": "user-123", "email": "jrue@taylormorrison.com",
                            "orgId": "org-1", "type": "access"})
    app = mw.AuditMiddleware(_terminal_app(), service_name="x")
    _drive_sync(app, _scope(method="POST", path="/api/v1/tenants",
                            headers={"cookie": "foo=bar; access_token=goodtoken; x=y"}))
    ev = recorder.events[0]
    assert ev["actor_type"] == "USER"
    assert ev["actor_id"] == "user-123"
    assert ev["actor_email"] == "jrue@taylormorrison.com"
    assert ev["org_id"] == "org-1"


def test_streaming_body_not_buffered_and_passthrough_intact(recorder):
    chunks = (b"chunk-a", b"chunk-b", b"chunk-c")
    app = mw.AuditMiddleware(_terminal_app(status=200, chunks=chunks, read_body=True),
                             service_name="snapshot-service")
    sent = _drive_sync(app, _scope(method="POST", path="/api/v1/exports"),
                       body_chunks=(b"part1", b"part2", b"part3"))
    body_msgs = [m for m in sent if m["type"] == "http.response.body"]
    assert [m["body"] for m in body_msgs] == list(chunks)
    start = [m for m in sent if m["type"] == "http.response.start"][0]
    assert start["status"] == 200
    assert len(recorder.events) == 1
