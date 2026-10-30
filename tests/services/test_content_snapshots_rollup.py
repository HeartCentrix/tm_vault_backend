"""Regression: /resources/{id}/content-snapshots must aggregate per-tab
item counts in SQL, not by materializing every sibling snapshot_items row
into Python.

Bug (2026-07-14): the per-tab rollup did
``select(external_id, content_size, created_at)`` across ALL sibling
snapshots and deduped in a Python loop. jrue's USER_CHATS child had
11.0M TEAMS_CHAT_MESSAGE pointer rows (the chats backup re-captures
~570k rows/day), so one Recovery resource-select loaded >2 GB of rows
into a 512Mi pod → snapshot-service OOMKilled → api-gateway 503 →
the Recovery UI rendered My Drive as an empty list.

Pins:
  * the rollup helper issues exactly ONE SQL statement that GROUPs /
    COUNTs / SUMs database-side and returns two scalars;
  * newest-wins semantics live in the SQL (ORDER BY created_at DESC);
  * results are cached per (child resource, latest snapshot) so repeat
    Recovery selects don't rescan millions of rows;
  * a missing row degrades to (0, 0) instead of raising.
"""
from __future__ import annotations

import asyncio
import importlib.util
import os
import pathlib
import sys
import uuid

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "services" / "snapshot-service" / "main.py"
_spec = importlib.util.spec_from_file_location("ss_main_rollup_under_test", _MAIN_PATH)
_ss = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["ss_main_rollup_under_test"] = _ss
assert _spec and _spec.loader
_spec.loader.exec_module(_ss)  # type: ignore[union-attr]


class _Result:
    def __init__(self, row):
        self._row = row

    def first(self):
        return self._row


class _FakeDB:
    """Records every execute(); returns a canned scalar row."""

    def __init__(self, row=(628267, 1011773407)):
        self.calls = []
        self._row = row

    async def execute(self, sql, params=None):
        self.calls.append((str(sql), params))
        return _Result(self._row)


def _fresh_ids():
    return uuid.uuid4(), uuid.uuid4(), [uuid.uuid4(), uuid.uuid4()]


def _data_calls(db):
    """Everything except SET LOCAL session tuning (work_mem bump)."""
    return [c for c in db.calls if not c[0].strip().upper().startswith("SET LOCAL")]


def test_rollup_is_single_sql_side_aggregate():
    db = _FakeDB()
    child_id, snap_id, sib_ids = _fresh_ids()
    count, size = asyncio.run(_ss._rollup_latest_items(
        db, child_id, snap_id, sib_ids, ["TEAMS_CHAT_MESSAGE"],
    ))
    assert (count, size) == (628267, 1011773407)
    assert len(_data_calls(db)) == 1, "rollup must be one SQL statement"
    sql = _data_calls(db)[0][0].upper()
    assert "COUNT(" in sql and "SUM(" in sql and "GROUP BY" in sql
    assert "CREATED_AT DESC" in sql, "newest-wins must happen in SQL"
    params = _data_calls(db)[0][1]
    assert list(params["snaps"]) == sib_ids
    assert params["types"] == ["TEAMS_CHAT_MESSAGE"]


def test_rollup_cached_per_latest_snapshot():
    db = _FakeDB()
    child_id, snap_id, sib_ids = _fresh_ids()
    args = (db, child_id, snap_id, sib_ids, ["EMAIL"])
    asyncio.run(_ss._rollup_latest_items(*args))
    asyncio.run(_ss._rollup_latest_items(*args))
    assert len(_data_calls(db)) == 1, "second select of the same snapshot must hit the cache"

    # a NEW latest snapshot for the same child must recompute
    asyncio.run(_ss._rollup_latest_items(db, child_id, uuid.uuid4(), sib_ids, ["EMAIL"]))
    assert len(_data_calls(db)) == 2


def test_rollup_all_types_mode_drops_type_filter():
    """count_types=None -> subtree storage rollup: one statement, no
    item_type filter, distinct cache key from any typed call."""
    db = _FakeDB(row=(9000, 133_000_000_000))
    child_id, snap_id, sib_ids = _fresh_ids()
    count, size = asyncio.run(_ss._rollup_latest_items(
        db, child_id, snap_id, sib_ids, None,
    ))
    assert (count, size) == (9000, 133_000_000_000)
    assert len(_data_calls(db)) == 1
    sql, params = _data_calls(db)[0]
    assert "item_type" not in sql, "all-types mode must not filter item_type"
    assert "types" not in (params or {})
    # typed call on the same (child, snapshot) must NOT hit the all-types cache
    asyncio.run(_ss._rollup_latest_items(db, child_id, snap_id, sib_ids, ["EMAIL"]))
    assert len(_data_calls(db)) == 2


def test_storage_summary_headline_uses_protected_rollup():
    """totalBytes must be the newest-version content-size rollup, not a
    sum of bytes_added (dedup + retention pruning made that read 32.9GB
    against a 124GB OneDrive, 2026-07-14)."""
    import inspect
    src = inspect.getsource(_ss.storage_summary)
    assert "_rollup_latest_items" in src
    assert "total_bytes = sum(b for _, b in pairs)" not in src
    # deltas / sparkline legitimately stay on bytes_added
    assert "bytes_added" in src.lower()


def test_rollup_missing_row_degrades_to_zero():
    db = _FakeDB(row=None)
    child_id, snap_id, sib_ids = _fresh_ids()
    count, size = asyncio.run(_ss._rollup_latest_items(
        db, child_id, snap_id, sib_ids, ["CALENDAR_EVENT"],
    ))
    assert (count, size) == (0, 0)


def test_endpoint_no_longer_materializes_rows_in_python():
    """The old code selected per-row (external_id, content_size,
    created_at) tuples and deduped in a Python set loop inside
    get_content_snapshots. That exact pattern must stay dead."""
    import inspect
    src = inspect.getsource(_ss.get_content_snapshots)
    assert "seen: set = set()" not in src
    assert "_rollup_latest_items" in src
