"""Regression: init_db's schema-sync must not take AccessExclusiveLock
on hot tables when there is nothing to migrate.

Bug (2026-07-21): every service boot ran dozens of
``ALTER TABLE ... ADD COLUMN IF NOT EXISTS`` inside ONE transaction.
ALTER TABLE takes AccessExclusiveLock even when the column already
exists, and the single transaction holds every lock until commit — so
a booting service that had locked ``snapshots`` and then waited on
``snapshot_items`` ABBA-deadlocked against any worker INSERT (which
holds snapshot_items and needs the FK RowShare on snapshots). PG
killed the worker's INSERT: the 30h ITOPS SharePoint walk logged a
[SP_FILE FAIL] DeadlockDetectedError per file every time the (then
crash-looping) snapshot-service restarted and re-ran schema-sync.

Pins:
  * ``_prune_existing_add_columns`` drops ADD COLUMN IF NOT EXISTS
    statements whose (table, column) already exists, passes everything
    else through untouched;
  * init_db consults the catalog and prunes before executing, so a
    fully-migrated production boot issues ZERO ALTER TABLE locks.
"""
from __future__ import annotations

import importlib.util
import inspect
import os
import pathlib
import sys

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import shared.database as db_mod  # noqa: E402


def test_prune_drops_existing_and_keeps_missing():
    stmts = [
        "ALTER TABLE snapshot_items ADD COLUMN IF NOT EXISTS parent_external_id VARCHAR;",
        "ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS azure_restore_point_id VARCHAR;",
        "ALTER TABLE snapshot_partitions "
        "ADD COLUMN IF NOT EXISTS partition_type VARCHAR NOT NULL DEFAULT 'ONEDRIVE_FILES';",
    ]
    existing = {
        ("snapshot_items", "parent_external_id"),
        ("snapshot_partitions", "partition_type"),
    }
    out = db_mod._prune_existing_add_columns(stmts, existing)
    assert out == [stmts[1]], "only the genuinely-missing column may run"


def test_prune_passes_through_non_add_column_statements():
    stmts = [
        "UPDATE sla_policies SET service_type = 'm365' WHERE service_type IS NULL;",
        "CREATE INDEX IF NOT EXISTS ix_foo ON snapshot_items(parent_external_id);",
    ]
    out = db_mod._prune_existing_add_columns(stmts, {("snapshot_items", "parent_external_id")})
    assert out == stmts, "non-ADD-COLUMN statements must pass through untouched"


def test_prune_is_case_and_whitespace_tolerant():
    stmts = [
        "alter table  Tenants  add column if not exists customer_id VARCHAR;",
    ]
    assert db_mod._prune_existing_add_columns(stmts, {("tenants", "customer_id")}) == []
    assert db_mod._prune_existing_add_columns(stmts, set()) == stmts


def test_init_db_prunes_against_catalog():
    src = inspect.getsource(db_mod.init_db)
    assert "_prune_existing_add_columns" in src, (
        "init_db must prune ADD COLUMNs against the catalog before executing"
    )
    assert "information_schema.columns" in src or "_existing_columns" in src


def test_sp_rest_row_insert_is_conflict_tolerant():
    """The SP REST list-row persist raced the drive producer on the same
    (snapshot_id, external_id, item_type) and blew up with UniqueViolation
    per duplicate. It must use the same ON CONFLICT DO NOTHING upsert as
    the drive-file path."""
    src = (_REPO_ROOT / "workers" / "backup-worker" / "main.py").read_text()
    marker = 'logger.warning("sp_rest_row persist failed'
    assert marker in src, "sp_rest persist block moved — update this pin"
    block = src[max(0, src.index(marker) - 4000): src.index(marker)]
    assert "on_conflict_do_nothing" in block, (
        "sp_rest_row persist must tolerate duplicate (snapshot, external_id, type)"
    )
