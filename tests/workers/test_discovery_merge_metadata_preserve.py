"""Tier-1 finalize MERGE must MERGE resources.metadata, never replace it.

`resources.metadata` is co-owned:

  * discovery writes stable identity / Graph keys (drive_id, web_url, …)
  * the backup-worker persists incremental state into the SAME column
    (onedrive_delta_link, onedrive_delta_link_pending, mail_delta_token,
    mail_folder_delta cursors, calendar_delta_tokens, channel_delta_tokens,
    chat cursors, …)

`metadata = s.metadata::json` in MERGE_UPDATED_COUNT_STMT wiped every
worker-written key on every discovery run — forcing full re-walks and
stranding drives (lost-baseline defect class) — and made the change
predicate `r.metadata IS DISTINCT FROM s.metadata` permanently true for
any row carrying worker keys, so the UPDATE fired on EVERY run.

These tests drive the real `_finalize_discovery`, capture the UPDATE it
actually executes, and *evaluate* its metadata expression under Postgres
jsonb-concat semantics (`a || b` == `{**a, **b}`) rather than matching a
literal SQL string.
"""
from __future__ import annotations

import asyncio
import importlib.util
import os
import pathlib
import re
import sys
import uuid

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "test_secret_for_unit_tests_only_x")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")
os.environ.setdefault("AUDIT_SERVICE_URL", "http://localhost:8000")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_REPO_ROOT / "workers" / "discovery-worker"))

_spec = importlib.util.spec_from_file_location(
    "discovery_worker_main",
    _REPO_ROOT / "workers" / "discovery-worker" / "main.py",
)


# Delta cursors the backup-worker owns. Losing these = full re-walk.
WORKER_META = {
    "onedrive_delta_link": "https://graph.microsoft.com/v1.0/me/drive/root/delta?token=abc",
    "mail_delta_token": "mdt-1",
    "calendar_delta_tokens": {"cal-1": "ct-1"},
}
# Keys Tier-1 discovery owns.
DISCOVERY_META = {"drive_id": "b!driveid", "web_url": "https://contoso.sharepoint.com/x"}


class _FakeScalars:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return list(self._rows)


class _FakeResult:
    def __init__(self, scalar=0, rows=None):
        self._scalar = scalar
        self._rows = rows or []

    def scalar(self):
        return self._scalar

    def all(self):
        return list(self._rows)

    def scalars(self):
        return _FakeScalars(self._rows)


class _RecordingDB:
    """Records every statement _finalize_discovery executes."""

    def __init__(self):
        self.statements = []

    async def execute(self, stmt, params=None):
        self.statements.append((getattr(stmt, "text", str(stmt)), params))
        return _FakeResult(0)

    async def commit(self):
        pass


def _capture_merge_update_sql() -> str:
    """Run the real finalizer and return the UPDATE resources SQL it issued."""
    mod = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(mod)

    db = _RecordingDB()
    tenant = mod.Tenant(id=uuid.uuid4(), display_name="t")
    run = mod.DiscoveryRun(id=uuid.uuid4(), tenant_id=tenant.id, scope=[], status="RUNNING")

    asyncio.run(
        mod._finalize_discovery(db, tenant, run, tenant.id, None, fetched_count=0)
    )

    updates = [s for s, _ in db.statements if "UPDATE resources AS r" in s]
    assert len(updates) == 1, f"expected exactly one resources UPDATE, got {len(updates)}"
    return updates[0]


def _eval_pg_json_expr(expr: str, r_meta: dict, s_meta: dict) -> dict:
    """Evaluate a Postgres json/jsonb expression over `r.metadata` / `s.metadata`.

    Supports casts, COALESCE(x, '{}') wrappers and the `||` concat operator
    (right operand wins — same as Python's {**a, **b}).
    """
    e = expr.strip().rstrip(",")
    e = re.sub(r"::\s*jsonb?\b", "", e, flags=re.I)
    e = re.sub(r"COALESCE\(\s*([A-Za-z_.]+)\s*,\s*'\{\}'\s*\)", r"\1", e, flags=re.I)
    merged: dict = {}
    for token in e.split("||"):
        token = token.strip().strip("()").strip()
        if token == "r.metadata":
            merged.update(r_meta)
        elif token == "s.metadata":
            merged.update(s_meta)
        elif token in ("'{}'", "{}"):
            continue
        else:
            raise AssertionError(f"unsupported operand {token!r} in expression {expr!r}")
    return merged


def _metadata_set_expr(sql: str) -> str:
    m = re.search(r"\n\s*metadata\s*=\s*(?P<expr>[^\n]+?),\s*\n", sql)
    assert m, f"no `metadata = ...` assignment found in:\n{sql}"
    return m.group("expr")


def _metadata_predicate_expr(sql: str) -> str:
    m = re.search(
        r"r\.metadata(?:::jsonb)?\s+IS DISTINCT FROM\s+(?P<expr>[^\n]+)",
        sql,
        flags=re.I,
    )
    assert m, f"no metadata change-predicate found in:\n{sql}"
    return m.group("expr")


def test_merge_preserves_worker_written_metadata_keys():
    """The UPDATE must not clobber backup-worker delta cursors."""
    sql = _capture_merge_update_sql()
    existing = {**DISCOVERY_META, **WORKER_META}

    written = _eval_pg_json_expr(_metadata_set_expr(sql), existing, DISCOVERY_META)

    assert written == {**existing, **DISCOVERY_META}, (
        "finalize MERGE replaced resources.metadata instead of merging it — "
        f"lost worker keys: {sorted(set(existing) - set(written))}"
    )
    for key, value in WORKER_META.items():
        assert written.get(key) == value, f"delta cursor {key} was wiped by discovery"


def test_merge_lets_discovery_keys_win_over_stale_values():
    """Discovery-owned keys must overwrite whatever the row carried before."""
    sql = _capture_merge_update_sql()
    existing = {**WORKER_META, "drive_id": "b!STALE", "web_url": "https://old"}

    written = _eval_pg_json_expr(_metadata_set_expr(sql), existing, DISCOVERY_META)

    assert written["drive_id"] == DISCOVERY_META["drive_id"]
    assert written["web_url"] == DISCOVERY_META["web_url"]
    assert written["mail_delta_token"] == WORKER_META["mail_delta_token"]


def test_change_predicate_does_not_fire_when_discovery_data_unchanged():
    """A row carrying worker keys whose discovery data is identical must be
    reported unchanged — otherwise the MERGE rewrites every row every run."""
    sql = _capture_merge_update_sql()
    existing = {**DISCOVERY_META, **WORKER_META}

    compared_to = _eval_pg_json_expr(_metadata_predicate_expr(sql), existing, DISCOVERY_META)

    assert compared_to == existing, (
        "metadata change-predicate compares the row against discovery-only "
        "metadata, so it is ALWAYS true for rows carrying worker-written keys "
        f"(row={sorted(existing)} vs predicate={sorted(compared_to)})"
    )


def test_change_predicate_still_fires_when_discovery_data_changed():
    sql = _capture_merge_update_sql()
    existing = {**DISCOVERY_META, **WORKER_META}
    changed = {**DISCOVERY_META, "web_url": "https://contoso.sharepoint.com/moved"}

    compared_to = _eval_pg_json_expr(_metadata_predicate_expr(sql), existing, changed)

    assert compared_to != existing, "real discovery-side changes must still update the row"
