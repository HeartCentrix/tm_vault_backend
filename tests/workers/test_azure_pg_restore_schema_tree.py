"""Regression tests for the Azure PostgreSQL restore schema path.

The backup handler used to store schema_blobs[db] = files[0]["blob_path"]
— the FIRST per-object schema file (one CREATE TABLE), not a folder. The
restore trusted any truthy schema_blobs value and applied just that one
file as "the schema", so a FULL restore onto a clean database produced
exactly one table, every data insert failed with 'relation does not
exist', and the job still reported success.

Pinned behavior: restore always tries the per-object schema TREE first
and only falls back to the stored monolithic blob when the tree applies
nothing (legacy snapshots).
"""
import asyncio
import os
import sys
import types
import uuid

# Make the azure-workload-worker package importable (dir name has dashes).
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
_WORKER_DIR = os.path.join(_REPO_ROOT, "workers", "azure-workload-worker")
for p in (_REPO_ROOT, _WORKER_DIR):
    if p not in sys.path:
        sys.path.insert(0, p)

from handlers.postgres_restore_handler import PostgresRestoreHandler  # noqa: E402


def _make_handler(tree_applied: int, calls: dict):
    h = PostgresRestoreHandler("test")

    async def _ensure_target_database(*a, **k):
        return None

    async def _connect(*a, **k):
        class _Conn:
            async def close(self):
                return None
        return _Conn()

    async def _apply_schema_tree(conn, tenant, data_prefix):
        calls["tree"] = data_prefix
        return tree_applied

    async def _apply_schema(conn, tenant, schema_blob):
        calls["blob"] = schema_blob
        return 1

    async def _apply_data(conn, tenant, data_prefix):
        calls["data"] = data_prefix
        return {"tables": 2, "rows": 10}

    h._ensure_target_database = _ensure_target_database
    h._connect = _connect
    h._apply_schema_tree = _apply_schema_tree
    h._apply_schema = _apply_schema
    h._apply_data = _apply_data
    return h


def _snapshot(extra_data):
    return types.SimpleNamespace(id=uuid.uuid4(), extra_data=extra_data)


_PARAMS = {
    "target_server_name": "dst-srv",
    "target_database_name": "dst-db",
    "source_database_name": "db1",
    "mode": "FULL",
}


def test_full_restore_prefers_schema_tree_over_stored_single_file():
    """A poisoned legacy schema_blobs entry (single file) must NOT keep
    the restore from replaying the full per-object schema tree."""
    calls = {}
    h = _make_handler(tree_applied=7, calls=calls)
    snap = _snapshot({
        "server_type": "FLEXIBLE",
        "schema_blobs": {"db1": "srv/db1/abc/schema/sql/public/Tables/one_table"},
        "data_prefixes": {"db1": "srv/db1/abc/"},
    })
    result = asyncio.run(h.restore(types.SimpleNamespace(id=uuid.uuid4()), snap, dict(_PARAMS)))
    assert result["success"] is True
    assert calls["tree"] == "srv/db1/abc/"
    assert "blob" not in calls, "single-file schema blob must not be applied when the tree exists"
    assert result["schema_statements_applied"] == 7


def test_full_restore_falls_back_to_monolithic_blob_when_tree_empty():
    """Legacy snapshots with a genuinely monolithic schema.sql (no
    schema/ folder) must still restore via the stored blob."""
    calls = {}
    h = _make_handler(tree_applied=0, calls=calls)
    snap = _snapshot({
        "server_type": "FLEXIBLE",
        "schema_blob": "srv/db1/abc/schema.sql",
        "database_name": "db1",
        "blob_path_prefix": "srv/db1/abc/",
    })
    result = asyncio.run(h.restore(types.SimpleNamespace(id=uuid.uuid4()), snap, dict(_PARAMS)))
    assert result["success"] is True
    assert calls["blob"] == "srv/db1/abc/schema.sql"


def test_current_format_snapshot_without_schema_blobs_uses_tree():
    """New backups store schema_prefixes (a folder), never a single-file
    schema_blobs value — the tree path must carry the restore alone."""
    calls = {}
    h = _make_handler(tree_applied=4, calls=calls)
    snap = _snapshot({
        "server_type": "FLEXIBLE",
        "schema_prefixes": {"db1": "srv/db1/abc/schema/"},
        "data_prefixes": {"db1": "srv/db1/abc/"},
    })
    result = asyncio.run(h.restore(types.SimpleNamespace(id=uuid.uuid4()), snap, dict(_PARAMS)))
    assert result["success"] is True
    assert calls["tree"] == "srv/db1/abc/"
    assert "blob" not in calls
    assert result["schema_statements_applied"] == 4
