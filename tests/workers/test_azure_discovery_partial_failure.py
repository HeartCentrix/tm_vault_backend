"""Azure ARM walk must degrade per resource family, not per tenant.

Regression pins for the all-or-nothing raise_for_status: a single 403/404/429
anywhere in the nested ARM walk aborted discover_azure_tenant, so every
subscription already enumerated was thrown away, the run went FAILED and no
Azure resource was refreshed anywhere in the tenant. The walk must now keep
what it saw and report the families it could not fully enumerate, and those
families must leave the stale-mark set (an unproven family may never be
mass-marked INACCESSIBLE).

Loaded via importlib by file path like test_discovery_azure_staging.py (the
worker modules have heavy deps).
"""
from __future__ import annotations

import asyncio
import importlib.util
import os
import pathlib
import sys
import types
import uuid

# ── Set required env vars before any shared.config import ─────────────────
os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "test_secret_for_unit_tests_only_x")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")
os.environ.setdefault("AUDIT_SERVICE_URL", "http://localhost:8000")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_REPO_ROOT / "workers" / "discovery-worker"))

_WORKER_DIR = _REPO_ROOT / "workers" / "discovery-worker"


def _load(name: str, path: pathlib.Path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_az = _load("azure_discovery_partial", _WORKER_DIR / "azure_discovery.py")
_main = _load("discovery_worker_main_partial", _WORKER_DIR / "main.py")

_SUB = "00000000-1111-2222-3333-444444444444"
_PG_ID = (
    f"/subscriptions/{_SUB}/resourceGroups/rg1"
    "/providers/Microsoft.DBforPostgreSQL/flexibleServers/pgsrv1"
)


class _FakeResponse:
    def __init__(self, payload=None, error: str | None = None):
        self._payload = payload or {}
        self._error = error

    def raise_for_status(self):
        if self._error:
            raise RuntimeError(self._error)

    def json(self):
        return self._payload


class _FakeClient:
    """Routes ARM GETs on URL substring; records every URL requested."""

    def __init__(self, routes):
        self.routes = routes
        self.urls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, url, headers=None):
        self.urls.append(url)
        for fragment, response in self.routes:
            if fragment in url:
                return response
        return _FakeResponse({"value": []})


def _run_walk(routes):
    client = _FakeClient(routes)
    real_httpx = _az.httpx
    real_token = _az._get_arm_access_token
    prev_provisioning = sys.modules.get("shared.azure_provisioning")

    async def _fake_token(tenant):
        return "token"

    async def _noop(*args, **kwargs):
        return True

    fake_provisioning = types.ModuleType("shared.azure_provisioning")
    fake_provisioning.ensure_sql_server_backup_ready = _noop
    fake_provisioning.ensure_pg_server_backup_ready = _noop

    _az.httpx = types.SimpleNamespace(AsyncClient=lambda *a, **k: client)
    _az._get_arm_access_token = _fake_token
    sys.modules["shared.azure_provisioning"] = fake_provisioning
    try:
        return asyncio.run(_az.discover_azure_tenant(object())), client
    finally:
        _az.httpx = real_httpx
        _az._get_arm_access_token = real_token
        if prev_provisioning is None:
            sys.modules.pop("shared.azure_provisioning", None)
        else:
            sys.modules["shared.azure_provisioning"] = prev_provisioning


_SUBSCRIPTIONS = ("/subscriptions?api-version", _FakeResponse({
    "value": [{"subscriptionId": _SUB, "displayName": "sub-one"}]
}))
_VMS_OK = ("Microsoft.Compute/virtualMachines", _FakeResponse({
    "value": [{
        "id": f"/subscriptions/{_SUB}/resourceGroups/rg1/providers/Microsoft.Compute/virtualMachines/vm1",
        "name": "vm1",
        "location": "eastus",
    }]
}))


def test_sql_403_keeps_vms_and_degrades_only_sql():
    discovered, client = _run_walk([
        _SUBSCRIPTIONS,
        _VMS_OK,
        ("Microsoft.Sql/servers", _FakeResponse(error="403 Forbidden")),
    ])

    # The walk survived and kept everything it could see.
    assert [vm["name"] for vm in discovered["vms"]] == ["vm1"]
    assert discovered["degraded_families"] == [_az.ARM_FAMILY_SQL]
    assert discovered["errors"], "the failure must be reported, not swallowed"
    # It also kept walking PAST the failure (postgres providers were queried).
    assert any("flexibleServers" in u for u in client.urls)


def test_nested_database_404_degrades_only_that_family():
    server_id = f"/subscriptions/{_SUB}/resourceGroups/rg1/providers/Microsoft.Sql/servers/srv-a"
    discovered, _client = _run_walk([
        _SUBSCRIPTIONS,
        _VMS_OK,
        ("/databases?api-version=2022-05-01-preview", _FakeResponse(error="404 Not Found")),
        ("Microsoft.Sql/servers", _FakeResponse({"value": [{
            "id": server_id,
            "name": "srv-a",
            "location": "eastus",
            "properties": {"fullyQualifiedDomainName": "srv-a.database.windows.net"},
        }]})),
        ("/databases?api-version=2023-03-01-preview", _FakeResponse({"value": []})),
        ("Microsoft.DBforPostgreSQL/flexibleServers", _FakeResponse({"value": [{
            "id": _PG_ID, "name": "pgsrv1", "location": "eastus",
        }]})),
    ])

    assert [vm["name"] for vm in discovered["vms"]] == ["vm1"]
    assert discovered["degraded_families"] == [_az.ARM_FAMILY_SQL]
    # The flexible server still enumerated (empty db list ⇒ default row).
    assert [p["name"] for p in discovered["postgres_servers"]] == ["pgsrv1"]


def test_subscription_list_failure_still_raises():
    """No subscriptions ⇒ nothing is provably enumerated ⇒ fail the run."""
    try:
        _run_walk([("/subscriptions?api-version", _FakeResponse(error="401 Unauthorized"))])
    except RuntimeError as exc:
        assert "401" in str(exc)
    else:
        raise AssertionError("a failed subscription list must fail the run")


def test_degraded_family_leaves_stale_mark_set_but_rows_are_kept():
    tenant_id = uuid.uuid4()
    discovered = {
        "subscriptions": [],
        "vms": [{
            "id": f"/subscriptions/{_SUB}/resourceGroups/rg1/providers/Microsoft.Compute/virtualMachines/vm1",
            "name": "vm1", "location": "eastus", "resource_group": "rg1",
            "subscription_id": _SUB, "type": "Microsoft.Compute/virtualMachines",
        }],
        "sql_dbs": [],
        "postgres_servers": [],
        "degraded_families": [_az.ARM_FAMILY_SQL, _az.ARM_FAMILY_PG_FLEXIBLE],
        "errors": ["databases on SQL server srv-a: 404"],
    }

    rows, types_ = _main._prepare_azure_staging_rows(tenant_id, discovered)

    assert [r["external_id"] for r in rows] == ["vm1"], "staged rows must survive"
    assert types_ == {
        _main.ResourceType.AZURE_VM,
        _main.ResourceType.AZURE_POSTGRESQL_SINGLE,
    }, "only families that were provably enumerated may be stale-marked"


def test_unknown_degraded_family_disables_all_stale_marking():
    rows, types_ = _main._prepare_azure_staging_rows(
        uuid.uuid4(),
        {"subscriptions": [], "vms": [], "sql_dbs": [], "postgres_servers": [],
         "degraded_families": ["something_new"]},
    )
    assert rows == []
    assert types_ == set()


def test_clean_walk_still_stale_marks_every_azure_type():
    _rows, types_ = _main._prepare_azure_staging_rows(
        uuid.uuid4(),
        {"subscriptions": [], "vms": [], "sql_dbs": [], "postgres_servers": [],
         "degraded_families": []},
    )
    assert types_ == set(_main.AZURE_DISCOVERY_RESOURCE_TYPES)
