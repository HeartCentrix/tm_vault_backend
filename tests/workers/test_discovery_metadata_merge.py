"""resources.metadata must be MERGED by the discovery upsert, never replaced.

The column is co-owned: discovery writes identity/Graph keys, the backup-worker
writes incremental state into the same JSON (onedrive_delta_link[_pending],
mail_delta_token, per-folder mail deltas, calendar_delta_tokens,
channel_delta_tokens, chat cursors). `metadata = s.metadata` wiped every cursor
on each discovery run and degraded every incremental backup to a full re-walk —
this fix lives on the other lineage and must not be lost when this branch builds
the discovery-worker image.
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import sys

# ── Set required env vars before any shared.config import ─────────────────
os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "test_secret_for_unit_tests_only_x")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")
os.environ.setdefault("AUDIT_SERVICE_URL", "http://localhost:8000")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_REPO_ROOT / "workers" / "discovery-worker"))

_WORKER_PATH = _REPO_ROOT / "workers" / "discovery-worker" / "main.py"
_spec = importlib.util.spec_from_file_location("discovery_worker_main_merge", _WORKER_PATH)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

_UPDATE_SQL = str(_mod.MERGE_UPDATED_COUNT_STMT)


def test_merge_expression_preserves_both_sides():
    expr = _mod.METADATA_MERGE_EXPR
    # jsonb || jsonb, right side wins per key ⇒ discovery keys refresh, every
    # backup cursor the discovery payload doesn't mention survives.
    assert "||" in expr
    assert "COALESCE(r.metadata::jsonb" in expr
    assert "COALESCE(s.metadata::jsonb" in expr


def test_update_sets_merged_metadata_not_wholesale():
    assert "metadata = s.metadata::json" not in _UPDATE_SQL, (
        "wholesale assignment deletes the backup-worker's delta cursors"
    )
    assert _UPDATE_SQL.count(_mod.METADATA_MERGE_EXPR) == 2, (
        "the merge must appear in BOTH the SET clause and the change-detection "
        "predicate, or every row looks changed forever"
    )


def test_change_detection_compares_against_the_merged_value():
    assert "IS DISTINCT FROM s.metadata" not in _UPDATE_SQL, (
        "comparing the stored row against the un-merged payload makes rows "
        "with live backup cursors report as changed on every run"
    )
    assert f"r.metadata::jsonb IS DISTINCT FROM {_mod.METADATA_MERGE_EXPR}" in _UPDATE_SQL


def test_insert_path_still_takes_the_payload_verbatim():
    # A brand-new resource row has no prior metadata to preserve.
    assert "s.metadata::json" in str(_mod.MERGE_INSERTED_COUNT_STMT)
