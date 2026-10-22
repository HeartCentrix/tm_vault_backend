"""search-service hardening (2026-10-14).

Four defects in one service (scaled to zero in prod; these make it safe to
scale back up):

1. The startup hook called create_search_index(), which ran — in ONE
   transaction, at every pod start — a plain (non-CONCURRENT) CREATE INDEX,
   an ALTER TABLE snapshot_items ADD COLUMN IF NOT EXISTS (ACCESS EXCLUSIVE
   even as a no-op) and another CREATE INDEX on the hottest table; the
   liveness probe SIGKILLed it mid-DDL (11 restarts). Startup must run no
   search DDL at all; future indexes go through alembic + CONCURRENTLY.
2. format_search_result read ``item.metadata`` — SQLAlchemy's declarative
   MetaData, not the row's JSON (shared/models.py maps DB column
   ``metadata`` to attribute ``extra_data``) — so every hit 500'd.
3. POST /api/v1/search/reindex matched the given snapshot ids against
   snapshot_items.id (never a hit) and, given none, selected the WHOLE
   table. It is now snapshot_id-scoped with a bounded LIMIT.
4. GET /api/v1/search/suggestions had no tenant filter: a per-keystroke
   full seq scan across every tenant. It is now scoped to the verified
   principal's entitled tenants (same mechanism as /api/v1/search).

search-service has a hyphen in the dir name, so main.py is loaded via
importlib (same pattern as test_audit_log_auth_and_cef.py).
"""
from __future__ import annotations

import asyncio
import importlib.util
import os
import pathlib
import sys
import uuid
from types import SimpleNamespace

import pytest

# ── Env required before any shared.config import ──────────────────────────
os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")
os.environ.setdefault("AUDIT_SERVICE_URL", "http://localhost:8000")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "services" / "search-service" / "main.py"
_spec = importlib.util.spec_from_file_location("search_main_under_test", _MAIN_PATH)
_sm = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["search_main_under_test"] = _sm
try:
    _spec.loader.exec_module(_sm)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(
        f"search-service module failed to import: {exc}",
        allow_module_level=True,
    )

from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy.sql import Select  # noqa: E402

import shared.audit as _audit_mod  # noqa: E402
import shared.core_metrics as _core_metrics  # noqa: E402
from shared.security import create_access_token  # noqa: E402

KEY = "unit-test-internal-key"
_TENANT_A = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
_TENANT_B = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"


# ── Fake DB session ───────────────────────────────────────────────────────


class _FakeResult:
    def __init__(self, rows):
        self._rows = list(rows)

    def scalars(self):
        return self

    def all(self):
        return list(self._rows)

    def first(self):
        return self._rows[0] if self._rows else None

    def scalar(self):
        return 0


class _FakeSession:
    """Minimal async-session stand-in: records every statement, returns
    canned rows, counts commits."""

    def __init__(self, rows=None):
        self.rows = list(rows or [])
        self.executed: list = []
        self.committed = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def execute(self, stmt, params=None):
        self.executed.append(stmt)
        return _FakeResult(self.rows)

    async def commit(self):
        self.committed += 1

    async def rollback(self):
        return None


@pytest.fixture
def sess(monkeypatch):
    s = _FakeSession()
    monkeypatch.setattr(_sm, "async_session_factory", lambda: s)
    # Internal-key principal path for AuthMiddleware; audit emit is a
    # network call — stub it so the POST tests stay hermetic.
    monkeypatch.setenv("INTERNAL_API_KEY", KEY)

    async def _no_emit(**_kw):
        return None

    monkeypatch.setattr(_audit_mod, "emit_audit_event", _no_emit)
    return s


def _client() -> TestClient:
    # No `with`: the startup hook must not run here.
    return TestClient(_sm.app)


def _internal_headers():
    return {"X-Internal-Api-Key": KEY}


def _user_headers(tenant_ids, roles=("USER",)):
    tok = create_access_token({
        "sub": str(uuid.uuid4()),
        "email": "viewer@example.test",
        "roles": list(roles),
        "tenantIds": list(tenant_ids),
    })
    return {"Authorization": f"Bearer {tok}"}


def _selects(session):
    return [s for s in session.executed if isinstance(s, Select)]


def _sql(stmt) -> str:
    return str(stmt)


# ── 1. Startup runs NO search DDL ─────────────────────────────────────────


def test_startup_runs_no_search_ddl(monkeypatch, sess):
    calls = []

    async def _fake_init_db():
        calls.append("init_db")

    monkeypatch.setattr(_sm, "init_db", _fake_init_db)
    monkeypatch.setattr(_core_metrics, "init", lambda *a, **k: True)

    async def _boom(*_a, **_k):
        raise AssertionError("create_search_index() was called from startup")

    # raising=False: the symbol must be GONE after the fix; if any code path
    # still reaches for it, the sentinel fires.
    monkeypatch.setattr(_sm, "create_search_index", _boom, raising=False)

    asyncio.run(_sm.startup())

    assert calls == ["init_db"]
    assert sess.executed == [], f"startup executed SQL: {sess.executed}"


def test_create_search_index_removed_and_no_ddl_in_source():
    assert not hasattr(_sm, "create_search_index") or _sm.create_search_index is None
    code = "\n".join(
        line for line in _MAIN_PATH.read_text().splitlines()
        if not line.lstrip().startswith("#")
    )
    assert "ALTER TABLE" not in code
    assert "CREATE INDEX" not in code


# ── 2. format_search_result reads extra_data (DB column 'metadata') ──────


def test_format_search_result_reads_extra_data():
    item = _sm.SnapshotItem(
        id=uuid.uuid4(),
        snapshot_id=uuid.uuid4(),
        tenant_id=uuid.UUID(_TENANT_A),
        item_type="EMAIL",
        name="Quarterly report",
        external_id="AAMkAGI2",
        content_size=1234,
        blob_path="tenant/snap/item",
        extra_data={"raw": {"subject": "Quarterly report", "bodyPreview": "Numbers attached"}},
    )
    snapshot = SimpleNamespace(id=item.snapshot_id, type="FULL", snapshot_label="daily", started_at=None)
    resource = SimpleNamespace(id=uuid.uuid4(), display_name="Jane Doe", type="MAILBOX")
    tenant = SimpleNamespace(id=uuid.UUID(_TENANT_A), display_name="Contoso")

    out = _sm.format_search_result(item, snapshot, resource, tenant, "report")

    assert out["id"] == str(item.id)
    assert out["preview"] == "Subject: Quarterly report\nNumbers attached"
    assert out["source"]["tenantId"] == _TENANT_A


def test_format_search_result_tolerates_missing_metadata():
    item = _sm.SnapshotItem(
        id=uuid.uuid4(), snapshot_id=uuid.uuid4(), item_type="FILE",
        name="budget.xlsx", external_id="01ABC", content_size=1, extra_data=None,
    )
    snapshot = SimpleNamespace(id=item.snapshot_id, type="FULL", snapshot_label=None, started_at=None)
    resource = SimpleNamespace(id=uuid.uuid4(), display_name="r", type="ONEDRIVE")
    tenant = SimpleNamespace(id=uuid.UUID(_TENANT_A), display_name="t")

    out = _sm.format_search_result(item, snapshot, resource, tenant, "budget")

    assert out["preview"] == "File: budget.xlsx"


# ── 3. Reindex is snapshot-scoped and bounded ─────────────────────────────


def test_reindex_without_snapshot_ids_is_rejected_not_table_wide(sess):
    r = _client().post("/api/v1/search/reindex", headers=_internal_headers())

    assert r.status_code == 400, r.text
    assert sess.executed == [], "reindex with no ids selected the whole table"


def test_reindex_filters_by_snapshot_id_with_limit(sess):
    sid = str(uuid.uuid4())

    r = _client().post(
        "/api/v1/search/reindex", json=[sid], params={"limit": 250},
        headers=_internal_headers(),
    )

    assert r.status_code == 200, r.text
    assert r.json() == {"indexed": 0, "limit": 250}
    selects = _selects(sess)
    assert len(selects) == 1
    sql = _sql(selects[0])
    assert "snapshot_items.snapshot_id IN" in sql, sql
    assert "snapshot_items.id IN" not in sql, sql
    assert "LIMIT" in sql, sql
    params = selects[0].compile().params
    assert params["snapshot_id_1"] == [uuid.UUID(sid)]
    assert params["param_1"] == 250
    assert sess.committed == 1, "per-item search_vector UPDATEs were never committed"


def test_reindex_limit_is_bounded(sess):
    r = _client().post(
        "/api/v1/search/reindex", json=[str(uuid.uuid4())], params={"limit": 50000},
        headers=_internal_headers(),
    )

    assert r.status_code == 422, r.text  # Query(le=10000)
    assert sess.executed == []


def test_reindex_rejects_malformed_snapshot_id(sess):
    r = _client().post(
        "/api/v1/search/reindex", json=["not-a-uuid"], headers=_internal_headers(),
    )

    assert r.status_code == 400, r.text
    assert sess.executed == []


# ── 4. Suggestions are tenant-scoped ──────────────────────────────────────


def test_suggestions_requires_principal(sess):
    r = _client().get("/api/v1/search/suggestions", params={"q": "rep"})

    assert r.status_code == 401, r.text
    assert sess.executed == []


def test_suggestions_user_is_scoped_to_entitled_tenants(sess):
    sess.rows = [("Quarterly report", "EMAIL", 3)]

    r = _client().get(
        "/api/v1/search/suggestions", params={"q": "rep"},
        headers=_user_headers([_TENANT_A]),
    )

    assert r.status_code == 200, r.text
    assert r.json()["suggestions"] == [{"text": "Quarterly report", "type": "EMAIL", "count": 3}]
    selects = _selects(sess)
    assert len(selects) == 1
    sql = _sql(selects[0])
    assert "snapshot_items.tenant_id IN" in sql, sql
    params = selects[0].compile().params
    assert params["tenant_id_1"] == [uuid.UUID(_TENANT_A)]
    assert params["name_1"] == "%rep%"


def test_suggestions_user_cannot_name_a_foreign_tenant(sess):
    r = _client().get(
        "/api/v1/search/suggestions", params={"q": "rep", "tenantId": _TENANT_B},
        headers=_user_headers([_TENANT_A]),
    )

    assert r.status_code == 403, r.text
    assert sess.executed == []


def test_suggestions_user_with_no_tenants_never_queries(sess):
    r = _client().get(
        "/api/v1/search/suggestions", params={"q": "rep"},
        headers=_user_headers([]),
    )

    assert r.status_code == 403, r.text
    assert sess.executed == []


def test_suggestions_internal_principal_must_name_a_tenant(sess):
    """Admin/internal principals carry no tenant claims; without a tenantId
    the old code ran the query unscoped (full seq scan). Now: 400, no query."""
    r = _client().get(
        "/api/v1/search/suggestions", params={"q": "rep"}, headers=_internal_headers(),
    )

    assert r.status_code == 400, r.text
    assert sess.executed == []


def test_suggestions_internal_principal_with_tenant_is_scoped(sess):
    r = _client().get(
        "/api/v1/search/suggestions", params={"q": "rep", "tenantId": _TENANT_B},
        headers=_internal_headers(),
    )

    assert r.status_code == 200, r.text
    selects = _selects(sess)
    assert len(selects) == 1
    assert "snapshot_items.tenant_id IN" in _sql(selects[0])
    assert selects[0].compile().params["tenant_id_1"] == [uuid.UUID(_TENANT_B)]


def test_suggestion_tenant_scope_helper():
    scope = _sm._suggestion_tenant_scope
    user = {"roles": ["USER"], "tenant_ids": [_TENANT_A, "garbage"], "_principal": "user"}
    assert scope(user, None) == [uuid.UUID(_TENANT_A)]
    assert scope(user, _TENANT_A) == [uuid.UUID(_TENANT_A)]
    with pytest.raises(_sm.HTTPException) as e:
        scope(user, _TENANT_B)
    assert e.value.status_code == 403
    with pytest.raises(_sm.HTTPException) as e:
        scope(user, "not-a-uuid")
    assert e.value.status_code == 400
    admin = {"roles": ["SUPER_ADMIN"], "tenant_ids": [], "_principal": "user"}
    assert scope(admin, _TENANT_B) == [uuid.UUID(_TENANT_B)]
    with pytest.raises(_sm.HTTPException) as e:
        scope(admin, None)
    assert e.value.status_code == 400
