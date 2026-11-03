"""Regression: fixing the empty `tenantIds` claim un-bricks alert-service writes.

Context (2026-07-31): every alert-service route gates on tenant membership
only — `_user_tenant_uuids(user)` from the JWT claim, no role check. Because
the claim was ALWAYS empty (auth-service read the never-written
`PlatformUser.tenant_id`), `_authorize_tenant_filter` raised 403 "No tenant
access" and `_ensure_tenant_member` raised 404 for literally every caller. The
whole service was unreachable, so nothing in it has ever been exercised in
production.

Populating the claim therefore does not "fix" these routes, it OPENS them —
including `DELETE /api/v1/access-groups/{id}`, which really does
`db.delete(group)`. Access-group CRUD is access-control administration, so it
now requires an elevated role (grantable with one INSERT into `user_roles` —
see services/job-service/export_authz.py). That keeps it exactly as reachable
as it is today (i.e. not) instead of silently handing a destructive DELETE to
every authenticated employee.

Deliberately NOT gated, and disclosed rather than changed:
  - `POST /api/v1/alerts/{id}/resolve` — acknowledging your own tenant's alert
    is ordinary product behaviour for a tenant member.
  - the member add/remove routes, which are verified no-ops: they validate and
    return, and never touch `db` (main.py:435-472).
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import sys
import uuid

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.sql import visitors

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_SVC_DIR = _REPO_ROOT / "services" / "alert-service"
_spec = importlib.util.spec_from_file_location("alert_main_under_test", _SVC_DIR / "main.py")
_al = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["alert_main_under_test"] = _al
assert _spec and _spec.loader
_spec.loader.exec_module(_al)  # type: ignore[union-attr]

from shared.database import get_db  # noqa: E402
from shared.models import AccessGroup, Alert  # noqa: E402
from shared.security import get_current_user_from_token  # noqa: E402

ORG_A = uuid.UUID("00000000-0000-0000-0000-0000000000a0")
TENANT_A = uuid.UUID("00000000-0000-0000-0000-0000000000a1")


class _FakeResult:
    def __init__(self, rows):
        self._rows = list(rows)

    def scalar_one_or_none(self):
        return self._rows[0] if self._rows else None

    def scalars(self):
        return self

    def all(self):
        return list(self._rows)


class _FakeSession:
    def __init__(self, *, groups=(), alerts=()):
        self.groups = {g.id: g for g in groups}
        self.alerts = {a.id: a for a in alerts}
        self.added = []
        self.deleted = []

    @staticmethod
    def _binds(stmt):
        vals = []
        visitors.traverse(stmt, {}, {"bindparam": lambda b: vals.append(b.value)})
        return vals

    async def execute(self, stmt):
        entity = stmt.column_descriptions[0]["entity"]
        binds = self._binds(stmt)
        if entity is AccessGroup:
            return _FakeResult([self.groups[b] for b in binds if b in self.groups])
        if entity is Alert:
            return _FakeResult([self.alerts[b] for b in binds if b in self.alerts])
        raise AssertionError(f"unexpected query entity {entity!r}")

    def add(self, obj):
        self.added.append(obj)

    async def delete(self, obj):
        self.deleted.append(obj)

    async def flush(self):
        pass


def _group() -> AccessGroup:
    return AccessGroup(
        id=uuid.uuid4(),
        org_id=ORG_A,
        tenant_id=TENANT_A,
        name="Payroll viewers",
        description="",
        member_ids=[],
    )


def _alert() -> Alert:
    return Alert(
        id=uuid.uuid4(),
        tenant_id=TENANT_A,
        severity="HIGH",
        message="backup failed",
        resolved=False,
        created_at=__import__("datetime").datetime(2026, 7, 31, 12, 0, 0),
    )


def _token(roles=("USER",)):
    return {
        "id": str(uuid.uuid4()),
        "email": "member@taylormorrison.com",
        "roles": list(roles),
        "org_id": str(ORG_A),
        "tenant_ids": [str(TENANT_A)],
    }


def _client(sess, user) -> TestClient:
    _al.app.dependency_overrides[get_db] = lambda: sess
    _al.app.dependency_overrides[get_current_user_from_token] = lambda: user
    return TestClient(_al.app)


@pytest.fixture(autouse=True)
def _clear_overrides():
    yield
    _al.app.dependency_overrides.clear()


# ------------------------------------------------- plain member is refused


def test_plain_member_cannot_create_an_access_group():
    sess = _FakeSession()
    r = _client(sess, _token()).post("/api/v1/access-groups", json={"name": "New group"})
    assert r.status_code == 403
    assert sess.added == []


def test_plain_member_cannot_rename_an_access_group():
    g = _group()
    sess = _FakeSession(groups=[g])
    r = _client(sess, _token()).put(f"/api/v1/access-groups/{g.id}", json={"name": "hijacked"})
    assert r.status_code == 403
    assert g.name == "Payroll viewers"


def test_plain_member_cannot_delete_an_access_group():
    """The destructive one: `db.delete(group)`."""
    g = _group()
    sess = _FakeSession(groups=[g])
    r = _client(sess, _token()).delete(f"/api/v1/access-groups/{g.id}")
    assert r.status_code == 403
    assert sess.deleted == []


# ------------------------------------------------------- admin still works


def test_tenant_admin_may_create_an_access_group():
    sess = _FakeSession()
    r = _client(sess, _token(roles=["TENANT_ADMIN"])).post(
        "/api/v1/access-groups", json={"name": "New group"}
    )
    assert r.status_code == 200
    assert len(sess.added) == 1


def test_tenant_admin_may_delete_an_access_group():
    g = _group()
    sess = _FakeSession(groups=[g])
    r = _client(sess, _token(roles=["TENANT_ADMIN"])).delete(f"/api/v1/access-groups/{g.id}")
    assert r.status_code == 204
    assert sess.deleted == [g]


def test_admin_rights_do_not_cross_tenants():
    """Elevation is not global: the group must still be in a tenant the
    admin's claim covers."""
    g = _group()
    g.tenant_id = uuid.UUID("00000000-0000-0000-0000-0000000000b1")
    sess = _FakeSession(groups=[g])
    r = _client(sess, _token(roles=["TENANT_ADMIN"])).delete(f"/api/v1/access-groups/{g.id}")
    assert r.status_code == 404  # _ensure_tenant_member hides existence
    assert sess.deleted == []


# ---------------------------------------------- deliberately left reachable


def test_tenant_member_may_still_resolve_their_tenants_alert():
    a = _alert()
    sess = _FakeSession(alerts=[a])
    r = _client(sess, _token()).post(f"/api/v1/alerts/{a.id}/resolve")
    assert r.status_code == 204
    assert a.resolved is True
