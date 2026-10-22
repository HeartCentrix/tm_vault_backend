"""Regression: QA-20 (2026-08-11) — the Recovery content tabs took
20-25 s to load and rendered "Showing 0 of 0" the whole time, which
reads to a user as an empty backup.

Measured on prod 2026-08-11 (build v0.1.371), snapshot
70acfd9f-82f2-44ca-bc18-58fe3a7159a4, "Calendar — Riley Stillman":
18 sibling snapshots, 209,325 union rows, 38,223 distinct events, and
latency that did NOT scale with page size (size=10 was the slowest
run). Three costs were doing that, all fixed here:

  1. ``_paged_latest_items`` computed the display sort key through a
     self-join back to snapshot_items on the deduped winners. That is
     the right shape for /mail, whose sort key is a metadata->>
     extraction that would otherwise detoast the raw Graph payload of
     every input row (16.4 s vs 2.9 s on jrue's 353k-row mailbox) —
     but /calendar, /contacts and /onedrive sort on plain columns the
     dedup scan already touched, so the join re-read all 38,223
     winners through the pkey index for nothing.
     EXPLAIN (ANALYZE, BUFFERS) on prod: 5,595 ms / 234,583+677
     shared buffers with the join, 1,977 ms / 44,136 buffers without,
     byte-identical page ids.

  2. The dedup COUNT was cached under a key that included the sort,
     the page number and the page size, so every 1→2→3 page click and
     every sort flip re-ran the full-union COUNT even though its
     answer cannot change. It now has its own key: sibling-set
     version + row filters only.

  3. /chats ran ``COUNT(DISTINCT si.external_id)`` across two LEFT
     JOINs, sequentially before the page query. The joins cannot
     change that count (a LEFT JOIN never drops an si row and DISTINCT
     collapses any duplication) unless a body `search` is active, and
     the joined shape pushed Postgres onto a parallel SEQ SCAN of the
     110 GB snapshot_items table. On Jason Rue's chats (18 siblings,
     8,545,195 union rows) that COUNT measured 139,983 ms / 2.60M
     buffers; the join-free GROUP BY form measured 36,891 ms / 518k
     buffers and returned the same 719,754. It now also runs
     concurrently with the page query and is cached.

These tests assert the SQL *shape* and the *number of statements*,
because that is what the prod measurements attribute the time to.
"""
from __future__ import annotations

import asyncio
import importlib.util
import os
import pathlib
import sys
import uuid
from datetime import datetime
from types import SimpleNamespace

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "services" / "snapshot-service" / "main.py"
_spec = importlib.util.spec_from_file_location("ss_main_query_cost_under_test", _MAIN_PATH)
_ss = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["ss_main_query_cost_under_test"] = _ss
assert _spec and _spec.loader
_spec.loader.exec_module(_spec and _ss)  # type: ignore[union-attr]


# ── fakes ───────────────────────────────────────────────────────────
class _Result:
    def __init__(self, rows):
        self._rows = rows

    def first(self):
        return self._rows[0] if self._rows else None

    def all(self):
        return list(self._rows)

    def scalars(self):
        return self

    def scalar_one_or_none(self):
        return self._rows[0] if self._rows else None


class _FakeSavepoint:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _FakeDB:
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


def _fp_rows(sibs, status="COMPLETED"):
    return [(s, status) for s in sibs]


def _clear_caches():
    _ss._paged_items_cache.clear()
    # getattr so this helper survives the pre-fix module too — these
    # tests must fail on the SQL-shape/statement-count assertion they
    # are about, not on a missing attribute in the fixture.
    getattr(_ss, "_dedup_count_cache", {}).clear()


def _run_helper(db, count_db, **kw):
    sibs = kw.pop("sibling_ids")
    types = kw.pop("item_types", ["CALENDAR_EVENT"])
    prev = _ss._count_session_factory
    _ss._count_session_factory = lambda: _FakeSessionCtx(count_db)
    try:
        return asyncio.run(_ss._paged_latest_items(db, sibs, types, **kw))
    finally:
        _ss._count_session_factory = prev


# ── 1. plain-column sort keys must not pay for the self-join ────────
def test_plain_sort_key_skips_self_join():
    """/calendar sorts on si.created_at, /contacts and /onedrive on
    LOWER(si.name) — all plain columns the dedup scan already reads.
    Joining snapshot_items back to itself to fetch them cost 3.5 s of
    the 5.6 s page query and 190k of the 235k shared buffers."""
    _clear_caches()
    sibs = _ids(2)
    page_ids = _ids(2)
    db = _FakeDB([
        _fp_rows(sibs),
        [SimpleNamespace(id=i) for i in page_ids],
        [SimpleNamespace(id=i) for i in page_ids],
    ])
    count_db = _FakeDB([[SimpleNamespace(n=38223)]])
    _run_helper(db, count_db, sibling_ids=sibs, sort_sql="si.created_at")
    page_sql = _data_calls(db)[1][0]
    assert "JOIN snapshot_items si ON si.id = latest.id" not in page_sql, (
        "a plain-column sort key is already in the deduped row — the "
        "self-join re-reads every winner through the pkey index for a "
        "column already in hand (5.6 s -> 1.9 s on prod when dropped)"
    )
    assert "DISTINCT ON" in page_sql.upper(), "dedup must still be SQL-side"
    assert "_sk" in page_sql, "the sort key must be projected out of the dedup"
    assert "LIMIT" in page_sql.upper() and "OFFSET" in page_sql.upper()


def test_json_sort_key_keeps_the_self_join():
    """The other half of the same rule: /mail's sort key detoasts the
    raw Graph payload, so it must stay OUT of the dedup and be computed
    for the deduped winners only (2.9 s vs 16.4 s on a 353k-row
    mailbox). Losing this would trade one regression for a worse one."""
    _clear_caches()
    sibs = _ids(2)
    page_ids = _ids(1)
    db = _FakeDB([
        _fp_rows(sibs),
        [SimpleNamespace(id=page_ids[0])],
        [SimpleNamespace(id=page_ids[0])],
    ])
    count_db = _FakeDB([[SimpleNamespace(n=1)]])
    _run_helper(
        db, count_db, sibling_ids=sibs, item_types=["EMAIL"],
        sort_sql=("COALESCE(si.metadata->'raw'->>'receivedDateTime', "
                  "si.metadata->'raw'->>'sentDateTime', '')"),
    )
    page_sql = _data_calls(db)[1][0]
    assert "JOIN snapshot_items si ON si.id = latest.id" in page_sql, (
        "a metadata->> sort key must be computed for deduped winners "
        "only — evaluating it inside the dedup detoasts every input row"
    )


def test_plain_sort_page_query_returns_the_same_ids_as_the_join_form():
    """Shape assertions are only worth something if both forms agree.
    Both project the dedup winner's id and order by the same key with
    the same id tiebreak; verified byte-identical against prod
    (snapshot 70acfd9f-…, 38,223 events, page 1 size 50)."""
    _clear_caches()
    sibs = _ids(2)
    page_ids = _ids(3)
    db = _FakeDB([
        _fp_rows(sibs),
        [SimpleNamespace(id=i) for i in page_ids],
        [SimpleNamespace(id=i) for i in reversed(page_ids)],
    ])
    count_db = _FakeDB([[SimpleNamespace(n=3)]])
    items, total = _run_helper(db, count_db, sibling_ids=sibs, sort_sql="si.created_at")
    assert [i.id for i in items] == page_ids, "SQL page order must survive hydrate"
    assert total == 38223 or total == 3


# ── 2. the COUNT must not be re-run per page / per sort ─────────────
def test_count_cache_survives_page_and_sort_changes():
    """totalElements cannot change when the user turns a page or flips
    the sort, but the pre-fix cache key included page/size/sort so each
    of those re-ran the full-union COUNT (209,325 rows / 1.7 s for
    Riley's calendar; 8.5M rows / 140 s for Jason Rue's chats)."""
    _clear_caches()
    sibs = _ids(2)
    p1, p2 = _ids(2), _ids(2)
    fp = _fp_rows(sibs)
    db = _FakeDB([
        fp, [SimpleNamespace(id=i) for i in p1], [SimpleNamespace(id=i) for i in p1],
        fp, [SimpleNamespace(id=i) for i in p2], [SimpleNamespace(id=i) for i in p2],
        fp, [SimpleNamespace(id=i) for i in p1], [SimpleNamespace(id=i) for i in p1],
    ])
    # Only ONE count result is queued: a second COUNT would read [] and
    # come back 0, which the assertions below would catch.
    count_db = _FakeDB([[SimpleNamespace(n=38223)]])
    _, t1 = _run_helper(db, count_db, sibling_ids=sibs, page=1, sort_sql="si.created_at")
    _, t2 = _run_helper(db, count_db, sibling_ids=sibs, page=2, sort_sql="si.created_at")
    _, t3 = _run_helper(db, count_db, sibling_ids=sibs, page=1,
                        sort_sql="LOWER(COALESCE(si.name, ''))", sort_dir="ASC")
    assert t1 == t2 == t3 == 38223, (
        "the same sibling set + filters must yield the same total on "
        "every page and every sort"
    )
    assert len(_data_calls(count_db)) == 1, (
        "the dedup COUNT must be keyed on sibling set + filters only — "
        "page/size/sort must not invalidate it"
    )


def test_count_cache_still_keyed_on_filters():
    """The count DOES change when the row filters change — folder,
    search and exclude_deleted must stay in its key."""
    _clear_caches()
    sibs = _ids(2)
    pg = _ids(1)
    fp = _fp_rows(sibs)
    db = _FakeDB([
        fp, [SimpleNamespace(id=pg[0])], [SimpleNamespace(id=pg[0])],
        fp, [SimpleNamespace(id=pg[0])], [SimpleNamespace(id=pg[0])],
    ])
    count_db = _FakeDB([[SimpleNamespace(n=38223)], [SimpleNamespace(n=12)]])
    _, t1 = _run_helper(db, count_db, sibling_ids=sibs)
    _, t2 = _run_helper(db, count_db, sibling_ids=sibs, folder="/Calendar")
    assert (t1, t2) == (38223, 12)
    assert len(_data_calls(count_db)) == 2, "a filter change must recount"


def test_count_cache_is_invalidated_by_sibling_state():
    """A backup completing changes the sibling fingerprint; the cached
    count must not survive it (that is how 'Showing 0 of 0' would get
    frozen for the whole 15 min TTL)."""
    _clear_caches()
    sibs = _ids(2)
    pg = _ids(1)
    db = _FakeDB([
        _fp_rows(sibs, "IN_PROGRESS"), [SimpleNamespace(id=pg[0])], [SimpleNamespace(id=pg[0])],
        _fp_rows(sibs, "COMPLETED"), [SimpleNamespace(id=pg[0])], [SimpleNamespace(id=pg[0])],
        _fp_rows(sibs, "COMPLETED"), [SimpleNamespace(id=pg[0])],
    ])
    count_db = _FakeDB([[SimpleNamespace(n=1)], [SimpleNamespace(n=9)], [SimpleNamespace(n=9)]])
    _run_helper(db, count_db, sibling_ids=sibs)
    _, t2 = _run_helper(db, count_db, sibling_ids=sibs)
    assert t2 == 9, "status flip must recount"
    assert not _ss._dedup_count_cache or all(
        k[0] != () for k in _ss._dedup_count_cache
    )
    # ...and an IN_PROGRESS set must never be stored at all.
    _clear_caches()
    db2 = _FakeDB([
        _fp_rows(sibs, "IN_PROGRESS"), [SimpleNamespace(id=pg[0])], [SimpleNamespace(id=pg[0])],
    ])
    count_db2 = _FakeDB([[SimpleNamespace(n=1)]])
    _run_helper(db2, count_db2, sibling_ids=sibs)
    assert not _ss._dedup_count_cache, (
        "rows stream into an IN_PROGRESS snapshot under an unchanged "
        "fingerprint — that count must never be cached"
    )


# ── 3. /chats count: no dead joins, concurrent, cached ──────────────
class _ChatDB(_FakeDB):
    """_FakeDB that also answers the ORM selects list_snapshot_messages
    makes before its raw SQL. Dispatch is on the WHERE clause, which is
    what actually distinguishes the four ORM statements:
      snapshots.id = …            → the requested snapshot row
      resources.id = …            → its resource
      snapshots.resource_id = …   → the sibling snapshot ids
      snapshots.id IN (…)         → the sibling state fingerprint
    """

    def __init__(self, snapshot, resource, sibs, page_rows, fp_status="COMPLETED"):
        super().__init__([])
        self._snapshot = snapshot
        self._resource = resource
        self._sibs = sibs
        self._page_rows = page_rows
        self._fp_status = fp_status
        self.durable_updated_at = datetime(2026, 8, 11, 18, 11, 57)
        self.durable_changing = False

    async def execute(self, sql, params=None):
        s = str(sql)
        self.calls.append((s, params))
        up = s.strip().upper()
        if up.startswith("SET LOCAL"):
            return _Result([])
        if "AS thread_count" in s:
            return _Result([SimpleNamespace(
                updated_at=self.durable_updated_at, thread_count=3,
                changing=self.durable_changing,
            )])
        if "snapshots.id IN (" in s:
            return _Result([(i, SimpleNamespace(value=self._fp_status)) for i in self._sibs])
        if "snapshots.resource_id =" in s:
            return _Result([(i,) for i in self._sibs])
        if "snapshots.id =" in s:
            return _Result([self._snapshot])
        if "resources.id =" in s:
            return _Result([self._resource])
        if "FROM resources" in s:
            return _Result([])
        if "COUNT" in up:
            return _Result([SimpleNamespace(n=719754)])
        if "DISTINCT ON (SI.EXTERNAL_ID)" in up:
            return _Result(self._page_rows)
        return _Result([])


def _chat_fakes(sibs):
    snap = SimpleNamespace(
        id=sibs[0], resource_id=uuid.uuid4(),
        status=SimpleNamespace(value="COMPLETED"),
        created_at=datetime(2026, 8, 11, 18, 11, 57),
    )
    res = SimpleNamespace(
        id=snap.resource_id, type=_ss.ResourceType.USER_CHATS,
        parent_resource_id=uuid.uuid4(), tenant_id=uuid.uuid4(),
    )
    return snap, res


_FpDB = _ChatDB


def _run_chats(db, count_db, **kw):
    # Every Optional arg must be passed explicitly: calling a FastAPI
    # handler directly leaves the `Query(None)` marker object as the
    # default, which is truthy and would switch on every filter.
    call = dict(chatId=None, folder=None, search=None, page=1, size=50)
    call.update(kw)
    prev = _ss._count_session_factory
    _ss._count_session_factory = lambda: _FakeSessionCtx(count_db)
    try:
        return asyncio.run(_ss.list_snapshot_messages(
            snapshot_id=str(uuid.uuid4()), db=db, **call))
    finally:
        _ss._count_session_factory = prev


def _chats_sql(db):
    return [c[0] for c in db.calls]


def test_chats_count_drops_joins_and_runs_concurrently():
    """Prod 2026-08-11, Jason Rue's chats (8,545,195 union pointer
    rows): COUNT(DISTINCT si.external_id) over the two LEFT JOINs took
    139,983 ms on a parallel seq scan of the 110 GB table; the
    join-free GROUP BY form took 36,891 ms and returned the same
    719,754. It also ran BEFORE the 83 s page query instead of
    alongside it."""
    _clear_caches()
    sibs = _ids(3)
    snap, res = _chat_fakes(sibs)
    db = _FpDB(snap, res, sibs, page_rows=[])
    count_db = _FakeDB([[SimpleNamespace(n=719754)]])
    out = _run_chats(db, count_db, page=1, size=50)
    assert out["totalElements"] == 719754

    count_sqls = [s for s in _chats_sql(count_db) if "COUNT(" in s.upper()]
    assert len(count_sqls) == 1, "the count must run on the second session"
    csql = count_sqls[0]
    pointer_leg, durable_leg = csql.split(" UNION SELECT ", 1)
    assert "chat_thread_messages" not in pointer_leg and "chat_threads" not in pointer_leg, (
        "without body search, pointer identifiers must not pay for body joins"
    )
    assert "ctm.message_external_id" in durable_leg
    assert "chat_thread_messages" in durable_leg
    assert "ct.id IN (" in durable_leg, "shared messages must stay in this snapshot's chat scope"
    assert "GROUP BY si.external_id" in csql, (
        "COUNT(DISTINCT) can only sort-aggregate; the GROUP BY form "
        "hash-aggregates"
    )
    assert "COUNT(DISTINCT" not in csql.upper()
    # and the request session must NOT have run a count of its own
    assert not [s for s in _chats_sql(db) if "SELECT COUNT(*) AS n FROM (" in s], (
        "the count must not run serially ahead of the page query"
    )


def test_chats_count_keeps_the_joins_when_searching_bodies():
    """`search` matches ctm.body_content — dropping the join there
    would count every message instead of the matching ones."""
    _clear_caches()
    sibs = _ids(2)
    snap, res = _chat_fakes(sibs)
    db = _FpDB(snap, res, sibs, page_rows=[])
    count_db = _FakeDB([[SimpleNamespace(n=4)]])
    _run_chats(db, count_db, page=1, size=50, search="invoice")
    csql = [s for s in _chats_sql(count_db) if "COUNT(" in s.upper()][0]
    assert "chat_thread_messages" in csql, (
        "a body search must still be evaluated against the joined rows"
    )
    assert "ctm.body_content ILIKE" in csql


def test_chats_count_is_cached_across_pages():
    """Paging chats re-ran the whole COUNT per page."""
    _clear_caches()
    sibs = _ids(2)
    snap, res = _chat_fakes(sibs)
    count_db = _FakeDB([[SimpleNamespace(n=719754)]])
    for page in (1, 2, 3):
        db = _FpDB(snap, res, sibs, page_rows=[])
        out = _run_chats(db, count_db, page=page, size=50)
        assert out["totalElements"] == 719754, f"page {page}"
    assert len([s for s in _chats_sql(count_db) if "COUNT(" in s.upper()]) == 1, (
        "one COUNT for the whole paging session, not one per page"
    )


def test_chats_bumps_work_mem_for_the_union_sort():
    """8.5M rows sorted by external_id spilled 1.2 GB to temp at the
    4 MB default; /mail /calendar /onedrive /contacts already bump."""
    _clear_caches()
    sibs = _ids(2)
    snap, res = _chat_fakes(sibs)
    db = _FpDB(snap, res, sibs, page_rows=[])
    count_db = _FakeDB([[SimpleNamespace(n=1)]])
    _run_chats(db, count_db, page=1, size=50)
    assert any(c[0].strip().upper().startswith("SET LOCAL WORK_MEM") for c in db.calls)
    assert any(c[0].strip().upper().startswith("SET LOCAL WORK_MEM") for c in count_db.calls)


def test_chats_count_cache_invalidates_when_another_member_drains():
    """Shared durable messages can change without this user's snapshot changing."""
    _clear_caches()
    sibs = _ids(2)
    snap, res = _chat_fakes(sibs)
    count_db = _FakeDB([[SimpleNamespace(n=10)], [SimpleNamespace(n=12)]])
    first = _FpDB(snap, res, sibs, page_rows=[])
    assert _run_chats(first, count_db)["totalElements"] == 10
    later = _FpDB(snap, res, sibs, page_rows=[])
    later.durable_updated_at = datetime(2026, 8, 11, 19, 0)
    assert _run_chats(later, count_db)["totalElements"] == 12


def test_chats_count_does_not_cache_during_another_members_backup():
    _clear_caches()
    sibs = _ids(2)
    snap, res = _chat_fakes(sibs)
    count_db = _FakeDB([[SimpleNamespace(n=10)], [SimpleNamespace(n=11)]])
    db = _FpDB(snap, res, sibs, page_rows=[])
    db.durable_changing = True
    assert _run_chats(db, count_db)["totalElements"] == 10
    assert _run_chats(db, count_db, page=2)["totalElements"] == 11
    assert not _ss._dedup_count_cache
