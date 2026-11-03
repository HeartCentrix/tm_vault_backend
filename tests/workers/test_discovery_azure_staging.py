"""_prepare_azure_staging_rows external_id collision regression tests.

Pins the fix for the inventory-loss defect: PG flexible-server databases all
shared external_id = server name, and AZURE_SQL_DB was keyed by the bare
database name, so _dedupe_resources silently dropped every collision — those
databases were never backed up. Rows must now be keyed uniquely per DATABASE
(ARM ids), and the connectable database name must travel in metadata for the
backup handlers.

Loaded via importlib by file path like test_discovery_streaming.py (the
worker module has heavy deps).
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import sys
import uuid

# ── Set required env vars before any shared.config import ─────────────────
os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "test_secret_for_unit_tests_only_x")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")
os.environ.setdefault("AUDIT_SERVICE_URL", "http://localhost:8000")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_REPO_ROOT / "workers" / "discovery-worker"))

_WORKER_PATH = _REPO_ROOT / "workers" / "discovery-worker" / "main.py"
_spec = importlib.util.spec_from_file_location("discovery_worker_main_azure", _WORKER_PATH)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)


_SUB = "00000000-1111-2222-3333-444444444444"


def _pg_flex_row(db_name: str) -> dict:
    return {
        "id": f"/subscriptions/{_SUB}/resourceGroups/rg1/providers/Microsoft.DBforPostgreSQL/flexibleServers/pgsrv1",
        "name": "pgsrv1",
        "location": "eastus",
        "resource_group": "rg1",
        "subscription_id": _SUB,
        "type": "Microsoft.DBforPostgreSQL/flexibleServers",
        "database_name": db_name,
    }


def test_pg_flexible_databases_get_distinct_external_ids():
    tenant_id = uuid.uuid4()
    discovered = {
        "subscriptions": [],
        "vms": [],
        "sql_dbs": [],
        "postgres_servers": [_pg_flex_row("app"), _pg_flex_row("billing"), _pg_flex_row("reporting")],
    }
    rows, _types = _mod._prepare_azure_staging_rows(tenant_id, discovered)
    pg_rows = [r for r in rows if r["resource_type"] == _mod.ResourceType.AZURE_POSTGRESQL.value]
    assert len(pg_rows) == 3, f"every database must survive dedupe, got {len(pg_rows)}"
    external_ids = {r["external_id"] for r in pg_rows}
    assert len(external_ids) == 3, f"external_ids must be unique per database: {external_ids}"
    # Handlers read the connectable db name from metadata
    assert {r["metadata"]["database_name"] for r in pg_rows} == {"app", "billing", "reporting"}


def test_sql_dbs_same_name_on_different_servers_both_survive():
    tenant_id = uuid.uuid4()

    def sql_row(server: str) -> dict:
        server_id = f"/subscriptions/{_SUB}/resourceGroups/rg1/providers/Microsoft.Sql/servers/{server}"
        return {
            "server_id": server_id,
            "server_fqdn": f"{server}.database.windows.net",
            "server_name": server,
            "database_name": "appdb",
            "database_id": f"{server_id}/databases/appdb",
            "resource_group": "rg1",
            "subscription_id": _SUB,
            "type": "Microsoft.Sql/servers/databases",
            "location": "eastus",
        }

    discovered = {
        "subscriptions": [],
        "vms": [],
        "sql_dbs": [sql_row("srv-a"), sql_row("srv-b")],
        "postgres_servers": [],
    }
    rows, _types = _mod._prepare_azure_staging_rows(tenant_id, discovered)
    sql_rows = [r for r in rows if r["resource_type"] == _mod.ResourceType.AZURE_SQL_DB.value]
    assert len(sql_rows) == 2, "same-named DBs on different servers must both survive dedupe"
    assert len({r["external_id"] for r in sql_rows}) == 2
    # The backup handler connects using metadata.database_name
    assert all(r["metadata"]["database_name"] == "appdb" for r in sql_rows)


def test_pg_single_server_keyed_by_arm_id():
    tenant_id = uuid.uuid4()
    single = {
        "id": f"/subscriptions/{_SUB}/resourceGroups/rg1/providers/Microsoft.DBforPostgreSQL/servers/oldsrv",
        "name": "oldsrv",
        "location": "eastus",
        "resource_group": "rg1",
        "subscription_id": _SUB,
        "type": "Microsoft.DBforPostgreSQL/servers",
        "database_name": "oldsrv.postgres.database.azure.com",
    }
    discovered = {"subscriptions": [], "vms": [], "sql_dbs": [], "postgres_servers": [single]}
    rows, _types = _mod._prepare_azure_staging_rows(tenant_id, discovered)
    assert len(rows) == 1
    assert rows[0]["resource_type"] == _mod.ResourceType.AZURE_POSTGRESQL_SINGLE.value
    assert rows[0]["external_id"] == single["id"]
