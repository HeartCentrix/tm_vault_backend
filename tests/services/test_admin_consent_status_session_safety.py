"""QA-01 (2026-08-11, v0.1.371): "Settings > Admin Consent force-logs you out".

What was actually measured against prod that day, before any change:

  * the three status reads are HEALTHY. With a valid bearer, auth-service
    answers 200 for all of them (m365 -> the 2026-06-16 consent row,
    azure -> null, power-bi -> action_required). Reproduced through
    api-gateway with the bearer AND with only the `access_token` cookie, so
    the gateway's cookie -> Authorization translation works on this route
    exactly as it does on /auth/me.
  * the QA 401s were `{"detail":"Not authenticated"}` — shared.security.
    _HTTPBearer401 firing because no credential reached the service at all.
    Not an authz decision by these handlers, not a Graph/Power BI 401 leaking
    through, not a proxy path/header problem.
  * the session died one hop later: the SPA saw 401, called /auth/refresh,
    that returned 401, and main.tsx's `_redirectToSignIn()` wiped everything.
    The refresh 401s because the *deployed* api-gateway comma-joins
    Set-Cookie (`response.headers.items()`), so `refresh_token` never reaches
    the browser — one merged header out of the gateway vs three out of
    auth-service, observed live. Fixed on this branch (multi_items()),
    awaiting deploy.

So the 401 on a missing credential stays: it is the trigger for the SPA's
silent re-auth, and answering 200 instead would make the read lie (a null
body renders "consent not granted" and invites an admin to re-run a
state-changing consent grant on an already-consented tenant).

What this file pins is the other half: no failure of a *status read* may
present as an auth failure or a 500. An authenticated caller with no usable
orgId claim used to fall through to `org_id = None` — the filters degraded to
`org_id IS NULL` and the readiness read dropped its org filter entirely, so
any tenant in any org was readable. That is now 403, the one status the SPA's
fetch shim deliberately ignores ("403 = role denied (refresh doesn't help)"),
so it can never take the session down. A non-UUID `tenantId` used to 500.
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import sys
import uuid
from datetime import datetime

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.sql.elements import BinaryExpression, BindParameter, BooleanClauseList

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_SVC_DIR = _REPO_ROOT / "services" / "auth-service"
_spec = importlib.util.spec_from_file_location("auth_main_consent_under_test", _SVC_DIR / "main.py")
_auth = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["auth_main_consent_under_test"] = _auth
assert _spec and _spec.loader
_spec.loader.exec_module(_auth)  # type: ignore[union-attr]

from shared.config import settings  # noqa: E402
from shared.database import get_db  # noqa: E402
from shared.models import (  # noqa: E402
    AdminConsentToken,
    Resource,
    Tenant,
    TenantType,
)
from shared.security import get_current_user_from_token  # noqa: E402

ORG_A = uuid.UUID("00000000-0000-0000-0000-0000000000a0")
ORG_B = uuid.UUID("00000000-0000-0000-0000-0000000000b0")
TENANT_A = uuid.UUID("00000000-0000-0000-0000-0000000000a1")
TENANT_B = uuid.UUID("00000000-0000-0000-0000-0000000000b1")

M365_STATUS = "/api/v1/admin-consent/m365/status"
AZURE_STATUS = "/api/v1/admin-consent/azure/status"
READINESS = "/api/v1/admin-consent/power-bi/readiness"


class _FakeResult:
    def __init__(self, rows):
        self._rows = list(rows)

    def scalar_one_or_none(self):
        return self._rows[0] if self._rows else None

    def scalars(self):
        return self

    def all(self):
        return list(self._rows)


def _eq_pairs(stmt):
    """(column_name, value) for every `col == <bind>` in the WHERE clause.

    Emulating the real filter set matters here: the pre-fix readiness query
    dropped `Tenant.org_id == org_id` entirely when the caller had no org, and
    a fake that only sniffed bind values would have matched the tenant either
    way and hidden the cross-org read.
    """
    pairs = []

    def walk(node):
        if node is None:
            return
        if isinstance(node, BooleanClauseList):
            for clause in node.clauses:
                walk(clause)
            return
        if isinstance(node, BinaryExpression):
            left, right = node.left, node.right
            if isinstance(right, BindParameter) and getattr(left, "key", None):
                pairs.append((left.key, right.value))

    walk(stmt.whereclause)
    return pairs


class _FakeSession:
    def __init__(self, *, consents=(), tenants=(), resources=()):
        self.rows = {
            AdminConsentToken: list(consents),
            Tenant: list(tenants),
            Resource: list(resources),
        }

    async def execute(self, stmt):
        entity = stmt.column_descriptions[0]["entity"]
        candidates = self.rows.get(entity)
        if candidates is None:
            raise AssertionError(f"unexpected query entity {entity!r}")
        pairs = _eq_pairs(stmt)

        def matches(row):
            for key, value in pairs:
                if not hasattr(row, key):
                    continue
                if getattr(row, key) != value:
                    return False
            return True

        return _FakeResult([r for r in candidates if matches(r)])


def _consent(org_id=ORG_A, consent_type="M365", tenant_id=TENANT_A) -> AdminConsentToken:
    return AdminConsentToken(
        id=uuid.uuid4(),
        org_id=org_id,
        tenant_id=tenant_id,
        consent_type=consent_type,
        granted_by="jrue@taylormorrison.com",
        consented_at=datetime(2026, 6, 16, 18, 33, 7),
        is_active=True,
        scope="https://graph.microsoft.com/.default",
    )


def _tenant(tid=TENANT_A, org_id=ORG_A, ttype=TenantType.M365) -> Tenant:
    return Tenant(
        id=tid,
        org_id=org_id,
        type=ttype,
        display_name=str(tid),
        external_tenant_id="163c487b-14b9-4105-bf06-3494cf11190d",
        created_at=datetime(2026, 1, 1),
        updated_at=datetime(2026, 6, 1),
    )


def _user(org_id=ORG_A):
    # `org_id` is a *string* in a real token dict — _get_user_from_token reads
    # it straight out of the JWT `orgId` claim. Keep the fixture faithful so
    # these tests exercise the same path the browser does.
    return {
        "id": str(uuid.uuid4()),
        "email": "jrue@taylormorrison.com",
        "roles": ["USER"],
        "org_id": str(org_id) if isinstance(org_id, uuid.UUID) else org_id,
        "tenant_ids": [str(TENANT_A)],
    }


def _client(session, user=None) -> TestClient:
    _auth.app.dependency_overrides[get_db] = lambda: session
    if user is not None:
        _auth.app.dependency_overrides[get_current_user_from_token] = lambda: user
    return TestClient(_auth.app)


@pytest.fixture(autouse=True)
def _clear_overrides():
    yield
    _auth.app.dependency_overrides.clear()


# ------------------------------------------------------- the healthy contract


def test_signed_in_caller_reads_all_three_statuses():
    """Baseline measured in prod: with a credential these are 200, not 401.

    Pins that the fix below did not make an ordinary signed-in read any
    harder.
    """
    session = _FakeSession(consents=[_consent()], tenants=[_tenant()], resources=[])
    client = _client(session, _user())

    m365 = client.get(M365_STATUS)
    assert m365.status_code == 200, m365.text
    assert m365.json()["granted_by"] == "jrue@taylormorrison.com"

    azure = client.get(AZURE_STATUS)
    assert azure.status_code == 200, azure.text
    assert azure.json() is None          # no AZURE consent and no AZURE tenant

    readiness = client.get(READINESS, params={"tenantId": str(TENANT_A)})
    assert readiness.status_code == 200, readiness.text
    assert readiness.json()["tenantId"] == str(TENANT_A)


def test_missing_credential_is_still_401_so_silent_reauth_keeps_working():
    """Deliberately NOT changed to 403/200 — this 401 is what makes the SPA
    refresh and retry instead of stranding the user on a stale panel."""
    client = _client(_FakeSession(tenants=[_tenant()]))
    for path in (M365_STATUS, AZURE_STATUS):
        res = client.get(path)
        assert res.status_code == 401, f"{path} -> {res.status_code} {res.text}"
    res = client.get(READINESS, params={"tenantId": str(TENANT_A)})
    assert res.status_code == 401


# ------------------------- authenticated-but-not-authorised must never be 401


def test_org_less_token_gets_403_not_a_silent_null():
    """Pre-fix: org_id became None, the filter became `org_id IS NULL`, and the
    panel rendered "consent not granted" for a tenant that IS consented."""
    session = _FakeSession(consents=[_consent()], tenants=[_tenant()])
    client = _client(session, _user(org_id=None))

    for path in (M365_STATUS, AZURE_STATUS):
        res = client.get(path)
        assert res.status_code == 403, f"{path} -> {res.status_code} {res.text}"
        assert "organization" in res.json()["detail"]


def test_malformed_org_claim_is_403_not_500():
    """Pre-fix: `UUID("not-a-uuid")` raised ValueError out of the handler."""
    session = _FakeSession(consents=[_consent()], tenants=[_tenant()])
    client = _client(session, _user(org_id="not-a-uuid"))
    assert client.get(M365_STATUS).status_code == 403


def test_org_less_token_cannot_read_another_orgs_power_bi_readiness():
    """Pre-fix the readiness query only applied `Tenant.org_id == org_id`
    `if org_id:` — so a caller with no org read ANY tenant's readiness,
    including the connected Power BI service-user email."""
    session = _FakeSession(tenants=[_tenant(TENANT_B, org_id=ORG_B)])
    client = _client(session, _user(org_id=None))
    res = client.get(READINESS, params={"tenantId": str(TENANT_B)})
    assert res.status_code == 403, res.text


def test_foreign_tenant_is_404_for_a_normal_caller():
    session = _FakeSession(tenants=[_tenant(TENANT_B, org_id=ORG_B)])
    client = _client(session, _user(org_id=ORG_A))
    assert client.get(READINESS, params={"tenantId": str(TENANT_B)}).status_code == 404


# ------------------------------------------------- caller input must not 500


def test_malformed_tenant_id_is_400_not_500():
    """Pre-fix: `UUID(tenant_id)` on the query param raised ValueError, which
    uvicorn turned into a 500 from a pure status read."""
    session = _FakeSession(tenants=[_tenant()])
    client = _client(session, _user())
    res = client.get(READINESS, params={"tenantId": "163c487b-not-a-uuid"})
    assert res.status_code == 400, res.text
    assert "UUID" in res.json()["detail"]


def test_power_bi_client_construction_failure_degrades_to_a_check(monkeypatch):
    """Pre-fix `PowerBIClient(...)` was built OUTSIDE the try, so a bad
    POWER_BI_* value 500'd the readiness read instead of being reported as the
    misconfiguration it is."""

    class _ExplodingClient:
        @staticmethod
        def get_refresh_token_from_tenant(tenant):
            return None

        def __init__(self, *args, **kwargs):
            raise RuntimeError("AADSTS700016: application not found in directory")

    monkeypatch.setattr(settings, "POWER_BI_CLIENT_ID", "test-client-id", raising=False)
    monkeypatch.setattr(settings, "POWER_BI_CLIENT_SECRET", "test-secret", raising=False)
    monkeypatch.setattr(settings, "POWER_BI_TENANT_ID", "test-tenant", raising=False)
    monkeypatch.setattr(_auth, "PowerBIClient", _ExplodingClient)

    session = _FakeSession(tenants=[_tenant()])
    client = _client(session, _user())
    res = client.get(READINESS, params={"tenantId": str(TENANT_A)})

    assert res.status_code == 200, res.text
    body = res.json()
    assert body["status"] == "action_required"
    workspace_check = next(c for c in body["checks"] if c["key"] == "workspace_api")
    assert workspace_check["status"] == "action_required"
    # The upstream failure is described, never re-emitted as this endpoint's
    # own status code.
    assert "access token" in workspace_check["detail"].lower()
