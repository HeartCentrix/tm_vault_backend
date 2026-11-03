"""Regression: Recovery search / folder drill-down must not 500, and must
see PARTIAL restore points.

Bug (found 2026-08-11, reported as "the search box doesn't work" and "the
SharePoint folders all look empty"):

``search_snapshot_items`` built its snapshot-id list with

    snapshot_ids_stmt = select(Snapshot.id).where(...)
    snapshot_ids = [str(s.id) for s in snapshot_ids_result.scalars().all()]

``select(Snapshot.id)`` + ``.scalars()`` yields BARE UUIDs, not ORM rows, so
``s.id`` raised

    AttributeError: 'asyncpg.pgproto.pgproto.UUID' object has no attribute 'id'

500ing every non-POWER_BI recovery search — and every folder drill-down,
which routes through the same handler with ``folder_path`` set. The
empty-result case masked it: with no matching snapshot the comprehension
never dereferenced, so the endpoint returned a plausible 200/total=0 instead
of the 500. Both branches were observed live against SHAREPOINT_SITE
"IT OPS NOTES" (resource 6584d4a9): COMPLETED snapshot -> 500,
PARTIAL snapshot -> 200 with totalElements=0.

Second defect, same function: the id query filtered
``status == COMPLETED``, hiding PARTIAL snapshots from Recovery entirely.
That site's three most recent snapshots (08-10 18:11, 08-11 02:11,
08-11 10:11) were all PARTIAL, so even with the AttributeError fixed the
newest backup stayed invisible. The POWER_BI branch in the same function
already accepted both statuses.
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
_spec = importlib.util.spec_from_file_location("ss_main_search_under_test", _MAIN_PATH)
_ss = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["ss_main_search_under_test"] = _ss
assert _spec and _spec.loader
_spec.loader.exec_module(_ss)  # type: ignore[union-attr]


class _FakeType:
    """Stands in for the ResourceType enum member on a Resource row."""

    value = "SHAREPOINT_SITE"


class _FakeResource:
    type = _FakeType()


class _Scalars:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class _Result:
    def __init__(self, rows=None, scalar=None):
        self._rows = rows or []
        self._scalar = scalar

    def scalars(self):
        return _Scalars(self._rows)

    def scalar(self):
        return self._scalar


class _StubDB:
    """Replays the three execute() calls the non-POWER_BI path makes:
    snapshot ids, then COUNT, then the item page."""

    def __init__(self, snapshot_ids):
        self.statements = []
        self._snapshot_ids = snapshot_ids

    async def get(self, _model, _pk):
        return _FakeResource()

    async def execute(self, stmt):
        self.statements.append(stmt)
        if len(self.statements) == 1:
            # select(Snapshot.id) -> scalars() yields bare UUID objects,
            # exactly as asyncpg/SQLAlchemy return them.
            return _Result(rows=list(self._snapshot_ids))
        if len(self.statements) == 2:
            return _Result(scalar=len(self._snapshot_ids))
        return _Result(rows=[])


def _sql(stmt) -> str:
    return str(stmt.compile(compile_kwargs={"literal_binds": False}))


@pytest.mark.asyncio
async def test_search_does_not_raise_on_bare_uuid_scalars():
    """The regression itself: a NON-EMPTY snapshot-id result must not 500."""
    ids = [uuid.uuid4(), uuid.uuid4()]
    db = _StubDB(ids)

    resp = await _ss.search_snapshot_items(
        resource_id=str(uuid.uuid4()),
        query="invoice",
        item_type=None,
        folder_path=None,
        snapshot_id=None,
        page=1,
        size=50,
        db=db,
    )

    # Reached the item query at all => the AttributeError is gone.
    assert len(db.statements) == 3
    assert resp.totalElements == len(ids)


@pytest.mark.asyncio
async def test_folder_drill_down_does_not_raise():
    """Folder navigation routes through the same handler with folder_path
    set — it 500'd for the same reason."""
    ids = [uuid.uuid4()]
    db = _StubDB(ids)

    await _ss.search_snapshot_items(
        resource_id=str(uuid.uuid4()),
        query=None,
        item_type="FILE",
        folder_path="/drives/b!abc/root:/Datacenter Information",
        snapshot_id=None,
        page=1,
        size=50,
        db=db,
    )

    assert len(db.statements) == 3


@pytest.mark.asyncio
async def test_empty_result_still_returns_empty_page():
    """No matching snapshot must short-circuit to an empty page, not crash
    and not fall through to the item query."""
    db = _StubDB([])

    resp = await _ss.search_snapshot_items(
        resource_id=str(uuid.uuid4()),
        query="anything",
        item_type=None,
        folder_path=None,
        snapshot_id=None,
        page=1,
        size=50,
        db=db,
    )

    assert resp.totalElements == 0
    assert resp.content == []
    assert len(db.statements) == 1


@pytest.mark.asyncio
async def test_partial_snapshots_are_searchable():
    """PARTIAL is a real restore point. The id query must accept COMPLETED
    *and* PARTIAL, or a resource whose recent snapshots are all PARTIAL
    (e.g. SHAREPOINT_SITE "IT OPS NOTES") shows nothing in Recovery."""
    db = _StubDB([uuid.uuid4()])

    await _ss.search_snapshot_items(
        resource_id=str(uuid.uuid4()),
        query="x",
        item_type=None,
        folder_path=None,
        snapshot_id=None,
        page=1,
        size=50,
        db=db,
    )

    sql = _sql(db.statements[0])
    # An IN over the status enum rather than a single equality.
    assert " IN " in sql.upper(), sql


@pytest.mark.asyncio
async def test_snapshot_ids_are_passed_through_as_uuids():
    """The item filter must be built from the UUIDs themselves — the old
    code round-tripped them through str() and back."""
    ids = [uuid.uuid4()]
    db = _StubDB(ids)

    await _ss.search_snapshot_items(
        resource_id=str(uuid.uuid4()),
        query=None,
        item_type=None,
        folder_path=None,
        snapshot_id=None,
        page=1,
        size=50,
        db=db,
    )

    # The COUNT statement carries the snapshot_id IN (...) filter.
    assert "snapshot_id" in _sql(db.statements[1]).lower()
