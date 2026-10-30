"""Unit tests for shared.auth_enforcement.AuthMiddleware (Ops/Data Check 17).

Verifies: public paths bypass; missing/invalid token -> 401; valid JWT and
valid internal key -> pass with the principal stashed; the principal is
readable via verified_user; require_admin gates on role; streaming responses
are not buffered.
"""
import asyncio
import json
import os

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "test-" + "0" * 32)
os.environ["INTERNAL_API_KEY"] = os.environ.get("INTERNAL_API_KEY") or "test-internal-key-abc"

import pytest

from shared import auth_enforcement as ae


def _scope(method="GET", path="/api/v1/tenants", headers=None):
    hdrs = [(k.encode(), v.encode()) for k, v in (headers or {}).items()]
    return {"type": "http", "method": method, "path": path, "headers": hdrs}


async def _drive(app, scope):
    sent = []
    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}
    async def send(m):
        sent.append(m)
    await app(scope, receive, send)
    return sent


def _run(app, scope):
    return asyncio.run(_drive(app, scope))


def _ok_app(captured=None):
    async def app(scope, receive, send):
        if captured is not None:
            captured["user"] = scope.get(ae._SCOPE_KEY)
        await send({"type": "http.response.start", "status": 200,
                    "headers": [(b"content-type", b"text/plain")]})
        await send({"type": "http.response.body", "body": b"ok"})
    return app


def _status(sent):
    return next(m["status"] for m in sent if m["type"] == "http.response.start")


@pytest.mark.parametrize("path", ["/health", "/healthz", "/health/deep", "/metrics", "/ping"])
def test_public_paths_bypass(path):
    app = ae.AuthMiddleware(_ok_app(), service_name="snapshot-service")
    assert _status(_run(app, _scope(path=path))) == 200


def test_missing_credential_rejected_401():
    app = ae.AuthMiddleware(_ok_app(), service_name="snapshot-service")
    sent = _run(app, _scope(path="/api/v1/resources/snapshots/x/items/y/content"))
    assert _status(sent) == 401
    body = next(m["body"] for m in sent if m["type"] == "http.response.body")
    assert json.loads(body)["detail"] == "Authentication required"


def test_garbage_bearer_rejected_401():
    app = ae.AuthMiddleware(_ok_app(), service_name="x")
    sent = _run(app, _scope(path="/api/v1/tenants",
                            headers={"authorization": "Bearer not-a-jwt"}))
    assert _status(sent) == 401


def test_valid_internal_key_passes_as_admin_system_principal():
    cap = {}
    app = ae.AuthMiddleware(_ok_app(cap), service_name="audit-service")
    sent = _run(app, _scope(method="POST", path="/api/v1/audit/log",
                            headers={"x-internal-api-key": ae._internal_key()}))
    assert _status(sent) == 200
    assert cap["user"]["_principal"] == "internal"
    assert ae.is_admin(cap["user"]) is True


def test_wrong_internal_key_rejected():
    app = ae.AuthMiddleware(_ok_app(), service_name="audit-service")
    sent = _run(app, _scope(method="POST", path="/api/v1/audit/log",
                            headers={"x-internal-api-key": "wrong"}))
    assert _status(sent) == 401


def test_valid_jwt_passes_and_stashes_principal(monkeypatch):
    import shared.security as sec
    monkeypatch.setattr(sec, "decode_token",
                        lambda tok, expected_type="access": {
                            "sub": "u-1", "email": "jrue@taylormorrison.com",
                            "roles": ["USER"], "orgId": "o-1",
                            "tenantIds": ["t-1"], "type": "access"})
    cap = {}
    app = ae.AuthMiddleware(_ok_app(cap), service_name="snapshot-service")
    sent = _run(app, _scope(path="/api/v1/tenants",
                            headers={"cookie": "access_token=good"}))
    assert _status(sent) == 200
    u = cap["user"]
    assert u["id"] == "u-1" and u["tenant_ids"] == ["t-1"] and u["_principal"] == "user"


def test_require_admin_and_verified_user_helpers():
    from fastapi import HTTPException

    class Req:
        def __init__(self, user): self.scope = {ae._SCOPE_KEY: user} if user else {}

    # no principal stashed -> 500 (middleware not mounted)
    with pytest.raises(HTTPException) as e1:
        ae.verified_user(Req(None))
    assert e1.value.status_code == 500

    # regular user -> require_admin 403
    with pytest.raises(HTTPException) as e2:
        ae.require_admin(Req({"roles": ["USER"]}))
    assert e2.value.status_code == 403

    # admin -> passes
    assert ae.require_admin(Req({"roles": ["ORG_ADMIN"]}))["roles"] == ["ORG_ADMIN"]
    # internal principal -> admin-equivalent
    assert ae.require_admin(Req({"roles": ["SUPER_ADMIN"], "_principal": "internal"}))


def test_streaming_not_buffered(monkeypatch):
    import shared.security as sec
    monkeypatch.setattr(sec, "decode_token",
                        lambda tok, expected_type="access": {"sub": "u", "type": "access"})
    async def stream_app(scope, receive, send):
        await send({"type": "http.response.start", "status": 200, "headers": []})
        for c in (b"a", b"b", b"c"):
            await send({"type": "http.response.body", "body": c, "more_body": c != b"c"})
    app = ae.AuthMiddleware(stream_app, service_name="snapshot-service")
    sent = _run(app, _scope(path="/api/v1/exports", headers={"cookie": "access_token=good"}))
    bodies = [m["body"] for m in sent if m["type"] == "http.response.body"]
    assert bodies == [b"a", b"b", b"c"]


def test_admin_for_mutations_gate(monkeypatch):
    import shared.security as sec
    monkeypatch.setattr(sec, "decode_token",
                        lambda tok, expected_type="access": {
                            "sub": "u", "roles": ["USER"], "tenantIds": [], "type": "access"})
    app = ae.AuthMiddleware(_ok_app(), service_name="tenant-service",
                            require_admin_for_mutations=True)
    # regular user: GET ok, POST/DELETE forbidden
    assert _status(_run(app, _scope(method="GET", path="/api/v1/tenants",
                                    headers={"cookie": "access_token=g"}))) == 200
    assert _status(_run(app, _scope(method="POST", path="/api/v1/tenants",
                                    headers={"cookie": "access_token=g"}))) == 403
    assert _status(_run(app, _scope(method="DELETE", path="/api/v1/tenants/x",
                                    headers={"cookie": "access_token=g"}))) == 403

def test_admin_for_mutations_allows_admin(monkeypatch):
    import shared.security as sec
    monkeypatch.setattr(sec, "decode_token",
                        lambda tok, expected_type="access": {
                            "sub": "a", "roles": ["ORG_ADMIN"], "tenantIds": [], "type": "access"})
    app = ae.AuthMiddleware(_ok_app(), service_name="tenant-service",
                            require_admin_for_mutations=True)
    assert _status(_run(app, _scope(method="POST", path="/api/v1/tenants",
                                    headers={"cookie": "access_token=g"}))) == 200
    # internal key also allowed to mutate
    assert _status(_run(app, _scope(method="POST", path="/api/v1/tenants",
                                    headers={"x-internal-api-key": ae._internal_key()}))) == 200


def test_confine_guest_path_blocks_traversal():
    from fastapi import HTTPException
    # windows: escape attempts rejected
    for bad in ["C:\\Windows\\NTDS\\ntds.dit", "..\\..\\Windows\\System32\\config\\SAM",
                "\\\\attacker\\share\\x", "D:\\other"]:
        with pytest.raises(HTTPException):
            ae.confine_guest_path("C:\\mnt\\vol1", bad, True)
    # posix escapes rejected
    for bad in ["/etc/shadow", "../../etc/passwd", "/root/.ssh/id_rsa"]:
        with pytest.raises(HTTPException):
            ae.confine_guest_path("/mnt/vol1", bad, False)
    # in-bounds allowed
    assert ae.confine_guest_path("/mnt/vol1", "docs/report.txt", False) == "/mnt/vol1/docs/report.txt"
    assert ae.confine_guest_path("/mnt/vol1", "", False) == "/mnt/vol1"
    assert ae.confine_guest_path("C:\\mnt\\vol1", "Users\\a\\n.txt", True).lower() == "c:\\mnt\\vol1\\users\\a\\n.txt"


def test_enforce_tenant_scope(monkeypatch):
    import shared.security as sec
    TA="163c487b-14b9-4105-bf06-3494cf11190d"; TB="99999999-9999-4999-8999-999999999999"
    monkeypatch.setattr(sec, "decode_token",
                        lambda tok, expected_type="access": {
                            "sub":"u","roles":["USER"],"orgId":"o","tenantIds":[TA],"type":"access"})
    app = ae.AuthMiddleware(_ok_app(), service_name="resource-service", enforce_tenant_scope=True)
    H={"cookie":"access_token=g"}
    # entitled tenant in query -> ok
    assert _status(_run(app, _scope(path="/api/v1/resources", headers=H, method="GET"))) == 200  # no tenantId -> no-op
    assert _status(_run(app, _scope(path=f"/api/v1/resources?tenantId={TA}", headers=H))) == 200
    # NOTE: _scope puts the query in path; middleware reads scope['query_string'] — set it:
def _scope_q(path, query, headers=None, method="GET"):
    s=_scope(method=method, path=path, headers=headers); s["query_string"]=query.encode(); return s

def test_enforce_tenant_scope_query_and_path(monkeypatch):
    import shared.security as sec
    TA="163c487b-14b9-4105-bf06-3494cf11190d"; TB="99999999-9999-4999-8999-999999999999"
    monkeypatch.setattr(sec, "decode_token",
                        lambda tok, expected_type="access": {
                            "sub":"u","roles":["USER"],"orgId":"o","tenantIds":[TA],"type":"access"})
    app = ae.AuthMiddleware(_ok_app(), service_name="resource-service", enforce_tenant_scope=True)
    H={"cookie":"access_token=g"}
    assert _status(_run(app, _scope_q("/api/v1/resources", f"tenantId={TA}", H))) == 200
    assert _status(_run(app, _scope_q("/api/v1/resources", f"tenantId={TB}", H))) == 403
    assert _status(_run(app, _scope(path=f"/api/v1/tenants/{TA}/secrets", headers=H))) == 200
    assert _status(_run(app, _scope(path=f"/api/v1/tenants/{TB}/secrets", headers=H))) == 403
    # admin bypasses
    monkeypatch.setattr(sec, "decode_token",
                        lambda tok, expected_type="access": {"sub":"a","roles":["ORG_ADMIN"],"tenantIds":[],"type":"access"})
    assert _status(_run(app, _scope_q("/api/v1/resources", f"tenantId={TB}", H))) == 200
