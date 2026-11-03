"""Regression: the JWT `tenantIds` claim was always empty.

Bug (found 2026-07-31, code review):
`auth-service._load_token_claims_from_db` built the claim as
`[str(user.tenant_id)] if user.tenant_id else []` — but nothing in the
product ever writes `PlatformUser.tenant_id`. `oauth_callback` inserts
email/name/external_user_id/org_id only; there is no admin endpoint and no
migration that assigns it. So the claim was `[]` for every real user, and
every tenant-scoping check downstream became dead code (alert-service
fails closed for everyone; chat-export's gate rejected nothing at all).

Contract pinned here: entitlement comes from data that actually exists —
`PlatformUser.org_id` -> `Tenant.org_id` — with `PlatformUser.tenant_id`
honoured as an explicit narrowing pin when an operator does set it.
Archived (soft-deleted) tenants are never entitled, and a user with no org
gets an empty list (fail closed, not fail open).
"""
from __future__ import annotations

import asyncio
import importlib.util
import os
import pathlib
import sys
import uuid
from datetime import datetime, timezone

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")

from sqlalchemy.sql import visitors

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_SVC_DIR = _REPO_ROOT / "services" / "auth-service"
_spec = importlib.util.spec_from_file_location("auth_main_under_test", _SVC_DIR / "main.py")
_auth = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["auth_main_under_test"] = _auth
assert _spec and _spec.loader
_spec.loader.exec_module(_auth)  # type: ignore[union-attr]

from shared.models import PlatformUser, Tenant, UserRole, UserRoleMapping  # noqa: E402

ORG_A = uuid.UUID("00000000-0000-0000-0000-0000000000a0")
ORG_B = uuid.UUID("00000000-0000-0000-0000-0000000000b0")
TENANT_A1 = uuid.UUID("00000000-0000-0000-0000-0000000000a1")
TENANT_A2 = uuid.UUID("00000000-0000-0000-0000-0000000000a2")
TENANT_B1 = uuid.UUID("00000000-0000-0000-0000-0000000000b1")


class _FakeResult:
    def __init__(self, rows):
        self._rows = list(rows)

    def scalar_one_or_none(self):
        return self._rows[0] if self._rows else None

    def scalars(self):
        return self

    def all(self):
        return list(self._rows)

    def __iter__(self):
        return iter(self._rows)


class _FakeDB:
    """Stub AsyncSession answering by selected entity + bind values."""

    def __init__(self, *, users=(), roles=(), tenants=()):
        self.users = {u.id: u for u in users}
        self.roles = list(roles)
        self.tenants = list(tenants)

    @staticmethod
    def _binds(stmt):
        vals = []
        visitors.traverse(stmt, {}, {"bindparam": lambda b: vals.append(b.value)})
        return vals

    async def execute(self, stmt):
        entity = stmt.column_descriptions[0]["entity"]
        binds = self._binds(stmt)
        if entity is PlatformUser:
            return _FakeResult([self.users[b] for b in binds if b in self.users])
        if entity is UserRoleMapping:
            return _FakeResult([r for r in self.roles if r.user_id in binds])
        if entity is Tenant:
            return _FakeResult(
                [
                    t.id
                    for t in self.tenants
                    if t.archived_at is None and (t.id in binds or t.org_id in binds)
                ]
            )
        raise AssertionError(f"unexpected query entity {entity!r}")


def _user(org_id=ORG_A, tenant_id=None) -> PlatformUser:
    return PlatformUser(
        id=uuid.uuid4(),
        email="victim@taylormorrison.com",
        name="Victim",
        org_id=org_id,
        tenant_id=tenant_id,
    )


def _tenant(tid, org_id, archived=False) -> Tenant:
    return Tenant(
        id=tid,
        org_id=org_id,
        display_name=str(tid),
        archived_at=datetime.now(timezone.utc) if archived else None,
    )


def _claims(db, user_id):
    return asyncio.run(_auth._load_token_claims_from_db(db, user_id))


def test_normal_user_gets_org_tenants_in_claim():
    """The bug: this returned [] for every real user, because nothing writes
    PlatformUser.tenant_id."""
    u = _user()
    db = _FakeDB(
        users=[u],
        roles=[UserRoleMapping(user_id=u.id, role=UserRole.USER)],
        tenants=[_tenant(TENANT_A1, ORG_A), _tenant(TENANT_A2, ORG_A), _tenant(TENANT_B1, ORG_B)],
    )
    claims = _claims(db, u.id)
    assert set(claims["tenantIds"]) == {str(TENANT_A1), str(TENANT_A2)}
    assert claims["orgId"] == str(ORG_A)
    assert claims["roles"] == ["USER"]
    assert claims["sub"] == str(u.id)


def test_other_orgs_tenants_never_appear():
    u = _user(org_id=ORG_B)
    db = _FakeDB(
        users=[u],
        roles=[UserRoleMapping(user_id=u.id, role=UserRole.USER)],
        tenants=[_tenant(TENANT_A1, ORG_A), _tenant(TENANT_B1, ORG_B)],
    )
    assert _claims(db, u.id)["tenantIds"] == [str(TENANT_B1)]


def test_explicit_tenant_pin_narrows_the_claim():
    """If an operator does set PlatformUser.tenant_id it must WIN (narrow),
    not be widened back out to the whole org."""
    u = _user(tenant_id=TENANT_A2)
    db = _FakeDB(
        users=[u],
        roles=[UserRoleMapping(user_id=u.id, role=UserRole.USER)],
        tenants=[_tenant(TENANT_A1, ORG_A), _tenant(TENANT_A2, ORG_A)],
    )
    assert _claims(db, u.id)["tenantIds"] == [str(TENANT_A2)]


def test_archived_tenants_are_not_entitled():
    u = _user()
    db = _FakeDB(
        users=[u],
        roles=[UserRoleMapping(user_id=u.id, role=UserRole.USER)],
        tenants=[_tenant(TENANT_A1, ORG_A), _tenant(TENANT_A2, ORG_A, archived=True)],
    )
    assert _claims(db, u.id)["tenantIds"] == [str(TENANT_A1)]


def test_user_without_org_gets_no_entitlement():
    u = _user(org_id=None)
    db = _FakeDB(
        users=[u],
        roles=[UserRoleMapping(user_id=u.id, role=UserRole.USER)],
        tenants=[_tenant(TENANT_A1, ORG_A)],
    )
    assert _claims(db, u.id)["tenantIds"] == []


def test_unknown_user_still_returns_none():
    db = _FakeDB(tenants=[_tenant(TENANT_A1, ORG_A)])
    assert _claims(db, uuid.uuid4()) is None


# ------------------------------------------------------- claim order (deploy)
#
# The claim order is load-bearing for two consumers that take `[0]`:
#   - a job-service that has NOT yet been upgraded past c8fa014 still does
#     `_user_tenant_id(user) = tenant_ids[0]` and 403s an export whose
#     resource lives in a different tenant. Deploy job-service first (see the
#     commit message), but an org holding both an M365 and an AZURE tenant row
#     should not be at the mercy of Postgres row order in the meantime: chat /
#     mailbox / OneDrive resources only ever live in the M365 tenant.
#   - alert-service.create_access_group files a new group in
#     `_user_tenant_uuids(user)[0]`, which was nondeterministic.
# So the entitlement query must be deterministically ordered, M365 first.


def _entitlement_stmt():
    from shared.tenant_entitlement import entitled_tenant_ids_for_user

    captured = {}

    class _Capture:
        async def execute(self, stmt):
            captured["stmt"] = stmt
            return _FakeResult([])

    asyncio.run(entitled_tenant_ids_for_user(_Capture(), _user()))
    return captured["stmt"]


def _compiled_entitlement_sql() -> str:
    from sqlalchemy.dialects import postgresql

    return str(
        _entitlement_stmt().compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )


def test_entitlement_query_is_deterministically_ordered():
    sql = _compiled_entitlement_sql()
    assert "ORDER BY" in sql.upper()
    # created_at + id tiebreakers keep the order stable across calls even when
    # every row has the same type.
    assert "created_at" in sql
    assert "tenants.id" in sql.split("ORDER BY")[1]


def test_m365_tenants_sort_ahead_of_azure_in_the_claim():
    order_by = _compiled_entitlement_sql().split("ORDER BY")[1]
    assert "M365" in order_by
    m365_rank = order_by.index("M365")
    # The M365 arm must rank 0 (i.e. sort first), not merely be mentioned.
    assert "0" in order_by[m365_rank:m365_rank + 40]
