"""Regression: the Recovery version dropdown must offer PARTIAL restore
points, and must not let a degraded row outrank an equally-full clean one.

Bug (found 2026-08-11 while fixing "the SharePoint folders all look empty"):
``list_snapshots`` gated its status filter on a flag named ``include_failed``
but implemented it as

    if not include_failed:
        filters.append(Snapshot.status == SnapshotStatus.COMPLETED)

PARTIAL is neither FAILED nor RUNNING — it is a recoverable point in time
that merely missed some content — yet this bare equality swept it up with
them. SHAREPOINT_SITE "IT OPS NOTES" had PARTIAL snapshots for its three most
recent runs (08-10 18:11, 08-11 02:11, 08-11 10:11), so the dropdown offered
none of them and the newest *browsable* backup was two days stale. Fixing
search_snapshot_items alone was therefore only half the fix: the user could
not select the snapshot to search in the first place.

Second half: with PARTIAL rows now candidates, a dedup group can hold a
COMPLETED and a PARTIAL row with an identical item_count, and which one won
depended on created_at alone. A clean restore point must never lose to an
equally-full degraded one, so status is now an explicit tiebreak ordered
after item_count and before recency.
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import sys
import uuid

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "services" / "snapshot-service" / "main.py"
_spec = importlib.util.spec_from_file_location("ss_main_dropdown_under_test", _MAIN_PATH)
_ss = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["ss_main_dropdown_under_test"] = _ss
assert _spec and _spec.loader
_spec.loader.exec_module(_ss)  # type: ignore[union-attr]


class _Result:
    def __init__(self, rows=None, scalar=0):
        self._rows = rows or []
        self._scalar = scalar

    def all(self):
        return self._rows

    def scalar(self):
        return self._scalar


class _StubDB:
    """Captures every statement so the test can inspect the compiled SQL."""

    def __init__(self):
        self.statements = []

    async def execute(self, stmt):
        self.statements.append(stmt)
        return _Result()


def _literal_sql(stmt) -> str:
    return str(stmt.compile(compile_kwargs={"literal_binds": True}))


async def _call(**overrides):
    db = _StubDB()
    kwargs = dict(
        resource_id=str(uuid.uuid4()),
        page=1,
        size=50,
        include_children=False,
        include_failed=False,
        include_empty=False,
        db=db,
    )
    kwargs.update(overrides)
    await _ss.list_snapshots(**kwargs)
    return db


@pytest.mark.asyncio
async def test_partial_snapshots_appear_in_the_dropdown_by_default():
    db = await _call()
    sql = _literal_sql(db.statements[0]).upper()
    assert "PARTIAL" in sql, sql
    assert "COMPLETED" in sql, sql


@pytest.mark.asyncio
async def test_failed_and_running_are_still_excluded_by_default():
    """The flag's actual purpose must survive: nothing unrecoverable."""
    db = await _call()
    sql = _literal_sql(db.statements[0]).upper()
    assert "FAILED" not in sql, sql
    assert "RUNNING" not in sql, sql


@pytest.mark.asyncio
async def test_completed_outranks_an_equally_full_partial():
    """Status must be a tiebreak in the dedup ranking, ordered after
    item_count and before created_at."""
    db = await _call()
    sql = _literal_sql(db.statements[0]).upper()
    assert "ROW_NUMBER" in sql, sql
    order_clause = sql.split("ORDER BY", 1)[1]
    # item_count first, then the CASE on status, then recency.
    i_items = order_clause.find("ITEM_COUNT")
    i_case = order_clause.find("CASE")
    i_created = order_clause.find("CREATED_AT")
    assert -1 < i_items < i_case < i_created, order_clause


@pytest.mark.asyncio
async def test_include_failed_still_widens_to_everything():
    """Admin/debug callers opting in must not be narrowed by this change."""
    db = await _call(include_failed=True)
    sql = _literal_sql(db.statements[0]).upper()
    # No status predicate at all when the caller opts in.
    assert "STATUS IN" not in sql, sql
