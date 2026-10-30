"""Regression: the Tier-2 list endpoints (/mail /calendar /onedrive
/contacts) must dedup + sort + paginate in SQL, not by materializing
every sibling snapshot_items row into Python.

Bug (2026-07-21, recurrence of 2026-07-20): ``list_snapshot_emails``
ran an unbounded ``select(SnapshotItem)`` across ALL sibling snapshots
and deduped/sorted in Python to serve one 500-row page. jrue's
USER_MAIL child holds 353,139 rows / 1,365 MB of raw-body JSON (only
~40k distinct emails — 9x delta re-capture duplication), so a single
Recovery mail view expanded to several GB of Python heap and
OOMKilled the 3 Gi snapshot-service pod (exit 137). /calendar,
/onedrive shared the exact pattern; /contacts paged without dedup.

Pins:
  * a shared ``_paged_latest_items`` helper does newest-wins dedup
    (DISTINCT ON), display sort, and LIMIT/OFFSET database-side and
    only hydrates the one page of ORM rows by id;
  * page order is decided by SQL — the helper preserves it even when
    the hydrate SELECT returns rows in arbitrary order;
  * all four endpoints route through the helper; the old
    ``scalars().all()`` + ``seen: set = set()`` Python dedup is dead;
  * /mail sorts by receivedDateTime/sentDateTime in SQL (page 1 =
    newest inbox mail, the pre-fix behavior).
"""
from __future__ import annotations

import asyncio
import importlib.util
import inspect
import os
import pathlib
import sys
import uuid
from types import SimpleNamespace

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "services" / "snapshot-service" / "main.py"
_spec = importlib.util.spec_from_file_location("ss_main_paged_under_test", _MAIN_PATH)
_ss = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["ss_main_paged_under_test"] = _ss
assert _spec and _spec.loader
_spec.loader.exec_module(_ss)  # type: ignore[union-attr]


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def first(self):
        return self._rows[0] if self._rows else None

    def all(self):
        return list(self._rows)

    def scalars(self):
        return self

    def scalar(self):
        row = self.first()
        return row


class _FakeSavepoint:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _FakeDB:
    """Returns queued results in execute() order; records every call.

    `SET LOCAL` statements are recorded but never consume a queued
    result — they're advisory session tuning, not data queries.
    `begin_nested` mimics the SAVEPOINT wrapper _set_local_work_mem
    uses to keep a failed SET from aborting the transaction.
    """

    def __init__(self, results):
        self._results = list(results)
        self.calls = []

    def begin_nested(self):
        return _FakeSavepoint()

    async def execute(self, sql, params=None):
        self.calls.append((str(sql), params))
        if str(sql).strip().upper().startswith("SET LOCAL"):
            return _Result([])
        return _Result(self._results.pop(0) if self._results else [])


class _FakeSessionCtx:
    """Async-context wrapper so a _FakeDB can stand in for
    async_session_factory() in _paged_latest_items' concurrent count."""

    def __init__(self, db):
        self._db = db

    async def __aenter__(self):
        return self._db

    async def __aexit__(self, *exc):
        return False


def _ids(n):
    return [uuid.uuid4() for _ in range(n)]


def _data_calls(db):
    return [c for c in db.calls if not c[0].strip().upper().startswith("SET LOCAL")]


def _fingerprint_rows(sibs, status="COMPLETED"):
    return [(s, status) for s in sibs]


def _run_helper(db, count_db=None, *, clear_cache=True, **kw):
    """Drive _paged_latest_items with fakes.

    Queue layout (main db): [sibling-state fingerprint], [page ids],
    [hydrate rows]. The COUNT runs on `count_db` via the module's
    injectable _count_session_factory.
    """
    sibs = kw.pop("sibling_ids", _ids(2))
    types = kw.pop("item_types", ["EMAIL"])
    if clear_cache:
        _ss._paged_items_cache.clear()
    count_db = count_db if count_db is not None else _FakeDB([[SimpleNamespace(n=0)]])
    prev_factory = _ss._count_session_factory
    _ss._count_session_factory = lambda: _FakeSessionCtx(count_db)
    try:
        return asyncio.run(_ss._paged_latest_items(db, sibs, types, **kw))
    finally:
        _ss._count_session_factory = prev_factory


def test_helper_count_and_page_are_sql_side():
    sibs = _ids(2)
    page_ids = _ids(3)
    orm_rows = [SimpleNamespace(id=i) for i in page_ids]
    db = _FakeDB([
        _fingerprint_rows(sibs),                         # cache-version token
        [SimpleNamespace(id=i) for i in page_ids],       # page ids
        orm_rows,                                        # hydrate
    ])
    count_db = _FakeDB([[SimpleNamespace(n=41230)]])
    items, total = _run_helper(db, count_db, sibling_ids=sibs, page=2, size=500)
    assert total == 41230
    assert [i.id for i in items] == page_ids
    assert len(_data_calls(db)) == 3, "fingerprint + page + hydrate, nothing else"

    count_sql = _data_calls(count_db)[0][0].upper()
    assert "GROUP BY" in count_sql, "count must hash-aggregate via GROUP BY"
    assert "COUNT(DISTINCT" not in count_sql, (
        "COUNT(DISTINCT) sort-aggregates and spills to disk — stay on the "
        "GROUP BY form"
    )

    page_sql, page_params = _data_calls(db)[1]
    up = page_sql.upper()
    assert "DISTINCT ON" in up, "newest-wins dedup must happen in SQL"
    assert "LIMIT" in up and "OFFSET" in up, "pagination must happen in SQL"
    assert page_params["limit"] == 500
    assert page_params["offset"] == 500, "page 2 with size 500"


def test_helper_hydrates_only_page_rows_and_preserves_sql_order():
    sibs = _ids(2)
    page_ids = _ids(4)
    shuffled = [SimpleNamespace(id=i) for i in reversed(page_ids)]
    db = _FakeDB([
        _fingerprint_rows(sibs),
        [SimpleNamespace(id=i) for i in page_ids],
        shuffled,
    ])
    count_db = _FakeDB([[SimpleNamespace(n=4)]])
    items, total = _run_helper(db, count_db, sibling_ids=sibs)
    assert [i.id for i in items] == page_ids, (
        "SQL decides page order; hydrate row order must not leak through"
    )


def test_helper_empty_page_skips_hydrate():
    sibs = _ids(2)
    db = _FakeDB([
        _fingerprint_rows(sibs),
        [],  # no page ids
    ])
    count_db = _FakeDB([[SimpleNamespace(n=0)]])
    items, total = _run_helper(db, count_db, sibling_ids=sibs)
    assert items == [] and total == 0
    assert len(_data_calls(db)) == 2, "no hydrate query for an empty page"


def test_helper_filters_reach_sql_params():
    sibs = _ids(2)
    page_ids = _ids(1)
    db = _FakeDB([
        _fingerprint_rows(sibs),
        [SimpleNamespace(id=page_ids[0])],
        [SimpleNamespace(id=page_ids[0])],
    ])
    count_db = _FakeDB([[SimpleNamespace(n=1)]])
    _run_helper(db, count_db, sibling_ids=sibs,
                folder="/Inbox", search="invoice", exclude_deleted=True)
    page_sql, params = _data_calls(db)[1]
    assert params["folder"] == "/Inbox"
    assert params["search"] == "%invoice%"
    assert "is_deleted" in page_sql.lower()
    # count query must use the same filters or totalPages lies
    count_sql, count_params = _data_calls(count_db)[0]
    assert count_params["folder"] == "/Inbox"
    assert count_params["search"] == "%invoice%"


def test_helper_caches_page_and_count_by_sibling_state():
    """Second identical request must serve page ids + total from the
    cache: no count, no page query — only the fingerprint probe and a
    fresh hydrate (ORM rows must never outlive their session)."""
    sibs = _ids(2)
    page_ids = _ids(3)
    fp = _fingerprint_rows(sibs)
    db = _FakeDB([
        fp,
        [SimpleNamespace(id=i) for i in page_ids],
        [SimpleNamespace(id=i) for i in page_ids],   # hydrate (1st)
        fp,                                          # fingerprint (2nd)
        [SimpleNamespace(id=i) for i in page_ids],   # hydrate (2nd)
    ])
    count_db = _FakeDB([[SimpleNamespace(n=3)]])
    _ss._paged_items_cache.clear()
    items1, total1 = _run_helper(db, count_db, sibling_ids=sibs, clear_cache=False)
    items2, total2 = _run_helper(db, count_db, sibling_ids=sibs, clear_cache=False)
    assert total1 == total2 == 3
    assert [i.id for i in items1] == [i.id for i in items2] == page_ids
    assert len(_data_calls(db)) == 5, (
        "second call = fingerprint + hydrate only (no page query)"
    )
    assert len(_data_calls(count_db)) == 1, "count must not rerun on a cache hit"


def test_helper_recomputes_when_sibling_status_changes():
    """An IN_PROGRESS sibling flipping to COMPLETED changes the
    fingerprint — rows stream in under an unchanged snapshot id, so the
    id set alone must NOT satisfy the cache."""
    sibs = _ids(2)
    page_ids = _ids(2)
    db = _FakeDB([
        _fingerprint_rows(sibs, status="IN_PROGRESS"),
        [SimpleNamespace(id=i) for i in page_ids],
        [SimpleNamespace(id=i) for i in page_ids],
        _fingerprint_rows(sibs, status="COMPLETED"),   # status flipped
        [SimpleNamespace(id=i) for i in page_ids],
        [SimpleNamespace(id=i) for i in page_ids],
    ])
    count_db = _FakeDB([[SimpleNamespace(n=2)], [SimpleNamespace(n=2)]])
    _ss._paged_items_cache.clear()
    _run_helper(db, count_db, sibling_ids=sibs, clear_cache=False)
    _run_helper(db, count_db, sibling_ids=sibs, clear_cache=False)
    assert len(_data_calls(count_db)) == 2, (
        "status change must invalidate the cached count/page"
    )


def test_helper_cache_ttl_bounds_staleness():
    sibs = _ids(2)
    page_ids = _ids(1)
    fp = _fingerprint_rows(sibs)
    db = _FakeDB([
        fp,
        [SimpleNamespace(id=i) for i in page_ids],
        [SimpleNamespace(id=i) for i in page_ids],
        fp,
        [SimpleNamespace(id=i) for i in page_ids],
        [SimpleNamespace(id=i) for i in page_ids],
    ])
    count_db = _FakeDB([[SimpleNamespace(n=1)], [SimpleNamespace(n=1)]])
    _ss._paged_items_cache.clear()
    prev_ttl = _ss._LIST_CACHE_TTL_S
    _ss._LIST_CACHE_TTL_S = 0  # everything is instantly stale
    try:
        _run_helper(db, count_db, sibling_ids=sibs, clear_cache=False)
        _run_helper(db, count_db, sibling_ids=sibs, clear_cache=False)
    finally:
        _ss._LIST_CACHE_TTL_S = prev_ttl
    assert len(_data_calls(count_db)) == 2, "TTL-expired entries must recompute"


def test_helper_bumps_work_mem_for_heavy_queries():
    """The union sort spilled 405MB to disk at the 4MB default work_mem
    — both the page session and the count session must SET LOCAL."""
    sibs = _ids(2)
    page_ids = _ids(1)
    db = _FakeDB([
        _fingerprint_rows(sibs),
        [SimpleNamespace(id=i) for i in page_ids],
        [SimpleNamespace(id=i) for i in page_ids],
    ])
    count_db = _FakeDB([[SimpleNamespace(n=1)]])
    _run_helper(db, count_db, sibling_ids=sibs)
    assert any(c[0].strip().upper().startswith("SET LOCAL WORK_MEM") for c in db.calls)
    assert any(c[0].strip().upper().startswith("SET LOCAL WORK_MEM") for c in count_db.calls)


def test_helper_never_caches_unsettled_sibling_sets():
    """An IN_PROGRESS sibling's rows stream in under an unchanged
    (id, status) pair — caching would freeze an empty/partial first
    backup for the whole TTL (the 'backup looks broken' escalation).
    Requests during a backup run live, exactly the pre-cache behavior."""
    sibs = _ids(2)
    page_ids = _ids(1)
    fp = _fingerprint_rows(sibs, status="IN_PROGRESS")
    db = _FakeDB([
        fp,
        [SimpleNamespace(id=i) for i in page_ids],
        [SimpleNamespace(id=i) for i in page_ids],
        fp,  # identical fingerprint on the second call
        [SimpleNamespace(id=i) for i in page_ids],
        [SimpleNamespace(id=i) for i in page_ids],
    ])
    count_db = _FakeDB([[SimpleNamespace(n=1)], [SimpleNamespace(n=1)]])
    _ss._paged_items_cache.clear()
    _run_helper(db, count_db, sibling_ids=sibs, clear_cache=False)
    _run_helper(db, count_db, sibling_ids=sibs, clear_cache=False)
    assert len(_data_calls(count_db)) == 2, (
        "identical IN_PROGRESS fingerprints must NOT serve from cache"
    )
    assert not _ss._paged_items_cache, "unsettled sets must not be stored either"


def test_work_mem_value_is_range_validated():
    """SET with an out-of-range value ERRORs, and any errored statement
    aborts the open transaction — every later query in the request
    would fail. The parser must reject what Postgres would reject."""
    assert _ss._work_mem_kb("128MB") == 131072
    assert _ss._work_mem_kb("64kB") == 64
    assert _ss._work_mem_kb("2GB") == 2097152
    assert _ss._work_mem_kb("63kB") is None, "below Postgres's 64kB minimum"
    assert _ss._work_mem_kb("999999GB") is None, "beyond the 2147483647kB max"
    assert _ss._work_mem_kb("0kB") is None
    assert _ss._work_mem_kb("128mb") is None, "units are case-sensitive"
    assert _ss._work_mem_kb("; DROP TABLE snapshot_items;") is None
    assert _ss._work_mem_kb("") is None


def test_endpoint_size_params_are_capped():
    """The cache stores tuple(page_ids) per (…, page, size) key — an
    uncapped size lets one /mail?size=100000 request pin an 80k-id tuple in
    process memory. Every list endpoint must therefore declare an `le=`.

    The ceiling is per-endpoint rather than a single literal 500: calendar
    carries le=1000 because the Recovery Calendar view asks for exactly 1000
    (tm_vault/src/pages/Recovery.tsx:1633), and against le=500 FastAPI 422'd
    before the handler ran — calendar recovery was categorically impossible,
    not merely slow (QA-03, 2026-08-11). What must not regress is the
    PRINCIPLE: bounded, and bounded low enough that one request cannot pin an
    unbounded id tuple. So assert an explicit `le` exists and is <= 1000
    rather than pinning one number, which is what made this test fire on a
    deliberate, documented ceiling change.
    """
    import inspect
    import re

    ceilings = {
        "list_snapshot_emails": 500,
        "list_snapshot_messages": 500,
        "list_snapshot_calendar": 1000,
        "list_snapshot_onedrive": 500,
        "list_snapshot_contacts": 500,
    }
    for fn in (
        _ss.list_snapshot_emails,
        _ss.list_snapshot_messages,
        _ss.list_snapshot_calendar,
        _ss.list_snapshot_onedrive,
        _ss.list_snapshot_contacts,
    ):
        src = inspect.getsource(fn)
        m = re.search(r"size: int = Query\(\s*\d+\s*,\s*ge=1\s*,\s*le=(\d+)\s*\)", src)
        assert m, (
            f"{fn.__name__} must cap `size` with an explicit le= — uncapped "
            "pages become resident cache entries"
        )
        le = int(m.group(1))
        assert le <= 1000, (
            f"{fn.__name__} caps size at {le}; anything above 1000 pins an "
            "unreasonable id tuple in the page cache"
        )
        assert le == ceilings[fn.__name__], (
            f"{fn.__name__} ceiling changed to {le}; if that is deliberate, "
            f"update this table and say why in the commit message"
        )


def test_mail_endpoint_is_sql_side_and_sorts_by_received_time():
    src = inspect.getsource(_ss.list_snapshot_emails)
    assert "_paged_latest_items" in src
    assert "seen: set = set()" not in src, "Python dedup loop must stay dead"
    assert ".scalars().all()" not in src, "unbounded materialization must stay dead"
    assert "receivedDateTime" in src and "sentDateTime" in src, (
        "page 1 = newest inbox mail — sort key lives in the SQL now"
    )


def test_calendar_endpoint_is_sql_side():
    src = inspect.getsource(_ss.list_snapshot_calendar)
    assert "_paged_latest_items" in src
    assert "seen: set = set()" not in src
    assert ".scalars().all()" not in src


def test_onedrive_endpoint_is_sql_side_with_sort_modes():
    src = inspect.getsource(_ss.list_snapshot_onedrive)
    assert "_paged_latest_items" in src
    assert "seen: set = set()" not in src
    assert ".scalars().all()" not in src
    assert "created_desc" in src and "modified_desc" in src, (
        "the three sort modes must survive the SQL port"
    )


def test_contacts_endpoint_dedups_in_sql():
    """Contacts was bounded (LIMIT/OFFSET) but paged duplicate sibling
    rows with no dedup and no deterministic order."""
    src = inspect.getsource(_ss.list_snapshot_contacts)
    assert "_paged_latest_items" in src
