"""Regression: the retention sweep must not walk resources through a
server-side cursor that its own mid-loop COMMIT invalidates.

THE BUG (prod, every daily run until 2026-10-14). enforce_retention_for_tenant
iterated ``session.stream(select(Resource)...).execution_options(yield_per=500)``
and committed every COMMIT_EVERY=100 resources inside that loop. Behind the
transaction-mode pooler the COMMIT ends the transaction that owns the asyncpg
cursor's portal, so the next fetch raised
``portal "__asyncpg_portal_..." does not exist``: the tenant was recorded as
``{"error": ...}``, the run ended with errors=1 and deleted nothing.

The fix pages by keyset over resource ids (``_iter_tenant_resources``): every
page is a self-contained ``SELECT id ... WHERE id > :last ORDER BY id LIMIT n``
plus one SELECT of that page's rows, so nothing spans a commit. These tests
drive the REAL sweep through a fake session that has no ``stream()`` at all,
serves the id page with real keyset semantics (tenant filter, ``id > :last``
cursor, LIMIT) and serves the rows query from exactly the ids asked for — so a
cursor that spans a commit, a page that re-reads rows, or a lost/duplicated
resource all fail here. Commit batching (COMMIT_EVERY=100) is pinned unchanged.

Style follows tests/shared/test_retention_keepset_safety.py.
"""
from __future__ import annotations

import asyncio
import os
import uuid
from datetime import datetime
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

import shared.retention_cleanup as rc  # noqa: E402  (env must precede import)

TENANT = uuid.UUID("00000000-0000-0000-0000-0000000000c1")
OTHER_TENANT = uuid.UUID("00000000-0000-0000-0000-0000000000c2")


# ── stand-ins ───────────────────────────────────────────────────────────

def _policy():
    return SimpleNamespace(
        id=uuid.uuid4(), is_default=True, retention_mode="GFS", retention_days=None,
        retention_hot_days=None, retention_cool_days=None, retention_archive_days=None,
        gfs_daily_count=7, gfs_weekly_count=0, gfs_monthly_count=0, gfs_yearly_count=0,
        archived_retention_mode="SAME", archived_retention_days=None,
        legal_hold_enabled=False, legal_hold_until=None, immutability_mode=None,
    )


def _snap(rid):
    """One fresh, honest, COMPLETED snapshot: inside every window, so the keep
    set keeps it and no counter restatement query is issued."""
    ts = datetime.utcnow()
    return SimpleNamespace(
        id=f"snap-{rid}", started_at=ts, created_at=ts, status="COMPLETED",
        type="INCREMENTAL", item_count=500, bytes_total=1024, extra_data={},
        reuse_of_snapshot_id=None, reuse_chain_root_id=None,
        lease_expires_at=None, hc_drain_status="NOT_APPLICABLE",
    )


def _resources(n: int, policy_id, tenant=TENANT) -> List[Any]:
    return [
        SimpleNamespace(id=uuid.uuid4(), tenant_id=tenant, status="ACTIVE",
                        sla_policy_id=policy_id)
        for _ in range(n)
    ]


class _Result:
    def __init__(self, rows: List[Any]):
        self._rows = list(rows)

    def scalars(self):
        return self

    def all(self):
        return list(self._rows)

    def fetchall(self):
        return list(self._rows)


class _KeysetSession:
    """Serves the sweep's reads with REAL keyset semantics and no stream().

    The id page honours the tenant filter, the ``id > :last`` cursor and the
    LIMIT; the rows query returns exactly the ids asked for, in id order.
    Every call is recorded so a test can pin the shape of the paging, and
    every commit records how many id pages had been fetched by then — a
    commit that lands mid-page is exactly what the old cursor could not
    survive.
    """

    def __init__(self, policies, resources, snaps_by_resource: Dict[Any, List[Any]]):
        self._policies = policies
        self._by_id = {r.id: r for r in resources}
        self._snaps = snaps_by_resource
        self.commits = 0
        self.id_pages: List[Tuple[Optional[uuid.UUID], int]] = []
        self.row_fetches: List[List[uuid.UUID]] = []
        self.snapshot_reads: List[uuid.UUID] = []
        self.pages_at_commit: List[int] = []

    async def execute(self, statement, params=None, *a, **kw):
        sql = str(statement)
        if "sla_policies" in sql:
            return _Result(self._policies)
        if "FROM resources" in sql:
            p = statement.compile().params
            if "LIMIT" in sql:
                tenant, last, limit = p["tenant_id_1"], p.get("id_1"), p["param_1"]
                self.id_pages.append((last, limit))
                ids = sorted(
                    i for i, r in self._by_id.items()
                    if r.tenant_id == tenant and (last is None or i > last)
                )
                return _Result(ids[:limit])
            ids = list(p["id_1"])
            self.row_fetches.append(ids)
            return _Result(sorted((self._by_id[i] for i in ids), key=lambda r: r.id))
        if "FROM snapshots" in sql:
            rid = statement.compile().params.get("resource_id_1")
            self.snapshot_reads.append(rid)
            return _Result(self._snaps.get(rid, []))
        return _Result([])

    async def commit(self):
        self.commits += 1
        self.pages_at_commit.append(len(self.id_pages))


def _run_sweep(monkeypatch, session) -> Dict[str, int]:
    async def _fake_delete(sess, snap_ids, *, audit_events=None):
        return 0

    monkeypatch.setattr(rc, "_delete_snapshots", _fake_delete)
    monkeypatch.setattr(rc.settings, "RETENTION_DELETE_ENABLED", True)
    return asyncio.run(rc.enforce_retention_for_tenant(session, TENANT))


async def _collect(session, page_size=None) -> List[Any]:
    return [r async for r in rc._iter_tenant_resources(session, TENANT, page_size)]


# ── the sweep ───────────────────────────────────────────────────────────

def test_sweep_pages_by_keyset_and_commits_mid_page_without_a_cursor(monkeypatch):
    """1,200 resources = three id pages of 500; COMMIT_EVERY=100 fires five
    times while page 1 is still being walked. The old stream() died on the
    first fetch after that first commit."""
    pol = _policy()
    res = _resources(1_200, pol.id)
    session = _KeysetSession([pol], res, {r.id: [_snap(r.id)] for r in res})
    assert not hasattr(session, "stream"), "the fake must not offer a cursor"

    stats = _run_sweep(monkeypatch, session)

    ordered = sorted(r.id for r in res)
    assert stats["checked_resources"] == 1_200
    # Three self-contained id pages; each cursor is the last id of the page
    # before it, and the final (short) page ends the walk without a 4th query.
    assert session.id_pages == [(None, 500), (ordered[499], 500), (ordered[999], 500)]
    assert session.row_fetches == [ordered[:500], ordered[500:1000], ordered[1000:]]
    # Every resource visited exactly once, in id order.
    assert session.snapshot_reads == ordered
    # COMMIT_EVERY batching is unchanged: 12 in-loop commits + the final one...
    assert session.commits == 13
    # ...and they land MID-page (five inside page 1, five inside page 2, two
    # inside page 3, then the closing commit) — none of which a server-side
    # cursor spanning the loop could survive.
    assert session.pages_at_commit == [1] * 5 + [2] * 5 + [3] * 3


def test_sweep_only_visits_the_tenants_own_resources(monkeypatch):
    pol = _policy()
    mine = _resources(10, pol.id)
    theirs = _resources(5, pol.id, tenant=OTHER_TENANT)
    session = _KeysetSession([pol], mine + theirs,
                             {r.id: [_snap(r.id)] for r in mine + theirs})

    stats = _run_sweep(monkeypatch, session)

    assert stats["checked_resources"] == 10
    assert set(session.snapshot_reads) == {r.id for r in mine}
    assert session.commits == 1  # 10 < COMMIT_EVERY: just the closing commit


# ── the pagination helper ───────────────────────────────────────────────

def test_iter_tenant_resources_ends_a_walk_whose_last_page_is_full():
    """1,000 rows at page 500: the third id page comes back empty and ends
    the walk; no rows query is issued for an empty page."""
    pol = _policy()
    res = _resources(1_000, pol.id)
    session = _KeysetSession([pol], res, {})

    seen = asyncio.run(_collect(session))

    ordered = sorted(r.id for r in res)
    assert [r.id for r in seen] == ordered
    assert session.id_pages == [(None, 500), (ordered[499], 500), (ordered[999], 500)]
    assert session.row_fetches == [ordered[:500], ordered[500:]]


def test_iter_tenant_resources_with_nothing_to_walk():
    session = _KeysetSession([_policy()], [], {})

    assert asyncio.run(_collect(session)) == []
    assert session.id_pages == [(None, 500)]
    assert session.row_fetches == []


def test_page_size_is_read_at_call_time(monkeypatch):
    """RESOURCE_PAGE_SIZE is consulted per call (not bound as a default), so
    a smaller page is honoured: 20 rows at page 7 = 7 + 7 + 6."""
    monkeypatch.setattr(rc, "RESOURCE_PAGE_SIZE", 7)
    pol = _policy()
    res = _resources(20, pol.id)
    session = _KeysetSession([pol], res, {})

    seen = asyncio.run(_collect(session))

    ordered = sorted(r.id for r in res)
    assert [r.id for r in seen] == ordered
    assert session.id_pages == [(None, 7), (ordered[6], 7), (ordered[13], 7)]
    assert [len(f) for f in session.row_fetches] == [7, 7, 6]


# ── the per-tenant error line ───────────────────────────────────────────

class _TenantSession:
    def __init__(self, tenants):
        self._tenants = tenants

    async def execute(self, statement, *a, **kw):
        return _Result(self._tenants)

    async def commit(self):
        pass


class _Factory:
    """async_session_factory stand-in: every ``async with factory() as s``
    yields a session that answers the Tenant select."""

    def __init__(self, tenants):
        self._tenants = tenants

    def __call__(self):
        session = _TenantSession(self._tenants)

        class _Ctx:
            async def __aenter__(self_):
                return session

            async def __aexit__(self_, *exc):
                return False

        return _Ctx()


def test_tenant_failure_is_printed_with_its_type_and_the_others_still_run(monkeypatch, capsys):
    """The scheduler's summary only counts errors ("errors=1"); the per-tenant
    line must name the exception TYPE and message so the next portal-style
    failure is diagnosable from the pod log alone."""
    bad, good = SimpleNamespace(id=uuid.uuid4()), SimpleNamespace(id=uuid.uuid4())
    msg = 'portal "__asyncpg_portal_1" does not exist'

    async def _enforce(session, tenant_id):
        if tenant_id == bad.id:
            raise RuntimeError(msg)
        return {"checked_resources": 3, "held": 0, "deleted_snapshots": 2, "kept_snapshots": 1}

    monkeypatch.setattr(rc, "enforce_retention_for_tenant", _enforce)

    results = asyncio.run(rc.enforce_retention_all_tenants(_Factory([bad, good])))

    out = capsys.readouterr().out
    assert f"[RETENTION] ERROR tenant={bad.id}: RuntimeError: {msg}" in out
    assert results[str(bad.id)] == {"error": msg}
    assert results[str(good.id)]["deleted_snapshots"] == 2
