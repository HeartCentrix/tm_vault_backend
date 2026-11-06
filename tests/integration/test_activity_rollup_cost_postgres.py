"""Activity batch rollup: same rows, bounded work (TM log catalogue #20).

Taylor Morrison's activity feed returned 504s. On TM's own 30-day export (2,289 jobs, 15,772
snapshots) the rollup statement took 11.5 s, ~50 s at 5x history and over 22 min at 10x
(api-gateway gives up at 60 s). Two defects did most of it:

* every array element unnested from a job (deferred / overlap-held ids, batch_resource_ids)
  re-detoasted and re-parsed the job's whole json ``spec`` to compute its batch key: 197,868
  parses of a ~4 KB spec, 5.4 s;
* ``fanout`` LEFT JOINed expected Tier-2 children to observed / deferred / held children; the
  planner estimates the CTE at 1 row and nested-loops, rescanning the other side per row:
  66M join-filter comparisons, 5.5 s.

The first test locks the rows the rollup returns, so the rewrite cannot change what the feed shows.
The other two measure the defects (buffer hits, join-filter comparisons) rather than wall time.
"""
import json
import os
from datetime import datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
import pytest_asyncio
import sqlalchemy.dialects.postgresql as pg_dialect
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

import shared.models as models
from shared.batch_rollup import build_batch_rollup_query
from shared.database import Base

pytestmark = pytest.mark.integration
NOW = datetime.utcnow()


@pytest_asyncio.fixture
async def db():
    url = os.environ.get("TMVAULT_TEST_DATABASE_URL")
    if not url:
        pytest.skip("requires explicitly configured disposable PostgreSQL")
    schema = "activity_rollup_" + uuid4().hex
    engine = create_async_engine(url, connect_args={"server_settings": {"search_path": schema}, "statement_cache_size": 0})
    async with engine.begin() as conn:
        await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    tenant, other_tenant, backend = uuid4(), uuid4(), uuid4()
    async with factory.begin() as s:
        org = models.Organization(name="Synthetic", slug=uuid4().hex)
        s.add(org)
        await s.flush()
        s.add_all([models.Tenant(id=t, org_id=org.id, display_name="Synthetic") for t in (tenant, other_tenant)])
        s.add(models.StorageBackend(id=backend, kind="azure", name=uuid4().hex, endpoint="https://storage.invalid",
                                    config={}, secret_ref="synthetic-reference"))
    yield Seed(factory, tenant, other_tenant, backend)
    async with engine.begin() as conn:
        await conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    await engine.dispose()


class Seed:
    def __init__(self, factory, tenant, other_tenant, backend):
        self.factory, self.tenant, self.other_tenant, self.backend = factory, tenant, other_tenant, backend
        self.labels, self.names = {}, 0

    async def resource(self, rtype, parent=None, status="ACTIVE", license_missing=False):
        rid, self.names = uuid4(), self.names + 1
        async with self.factory.begin() as s:
            s.add(models.Resource(id=rid, tenant_id=self.tenant, type=getattr(models.ResourceType, rtype),
                                  status=getattr(models.ResourceStatus, status), external_id=str(rid),
                                  display_name=f"{rtype} {self.names}", parent_resource_id=parent,
                                  extra_data={"license_missing": True} if license_missing else {},
                                  created_at=NOW - timedelta(days=30)))
        return rid

    async def job(self, label, *, ago, status="COMPLETED", resource_ids=None, resource_id=None, spec=None,
                  type="BACKUP", tenant=None):
        jid = uuid4()
        self.labels[str(jid)] = label
        async with self.factory.begin() as s:
            s.add(models.Job(id=jid, type=getattr(models.JobType, type), tenant_id=tenant or self.tenant,
                             status=getattr(models.JobStatus, status), resource_id=resource_id,
                             batch_resource_ids=resource_ids, spec=spec or {}, created_at=NOW - ago,
                             completed_at=None if status == "RUNNING" else NOW - ago + timedelta(minutes=20)))
        return jid

    async def snapshot(self, job_id, resource_id, status="COMPLETED", extra=None, partitions=()):
        sid = uuid4()
        async with self.factory.begin() as s:
            s.add(models.Snapshot(id=sid, job_id=job_id, resource_id=resource_id, backend_id=self.backend,
                                  status=getattr(models.SnapshotStatus, status), bytes_added=7, item_count=3,
                                  extra_data=extra or {},
                                  completed_at=None if status == "IN_PROGRESS" else NOW - timedelta(hours=1)))
            await s.flush()
            for i, pstatus in enumerate(partitions):
                s.add(models.SnapshotPartition(snapshot_id=sid, tenant_id=self.tenant, resource_id=resource_id,
                                               job_id=job_id, partition_index=i, status=pstatus))
        return sid


async def _rollup(seed):
    stmt = build_batch_rollup_query(tenant_id=str(seed.tenant), start_date=None, end_date=None,
                                    operation="BACKUP", size=5000, offset=0)
    async with seed.factory() as s:
        rows = (await s.execute(stmt)).mappings().all()
    out = {}
    for r in rows:
        jobs = sorted(seed.labels[str(j)] for j in r["job_ids"])
        key = jobs[0].split(":")[0]
        out[key] = {k: v for k, v in r.items() if k not in {
            "batch_id", "tenant_id", "job_ids", "started_at", "jobs_max_completed_at",
            "snaps_max_completed_at", "parts_max_completed_at"}} | {"jobs": jobs}
    return out


async def _seed_feed(seed):
    """Four batches covering every input the rewrite touches."""
    u1, u2, u3 = [await seed.resource("ENTRA_USER") for _ in range(3)]
    mail1 = await seed.resource("USER_MAIL", u1, status="DISCOVERED")
    chats1 = await seed.resource("USER_CHATS", u1, status="DISCOVERED")
    await seed.resource("USER_ONEDRIVE", u1, status="DISCOVERED")     # A: neither run nor withheld -> missing
    mail2 = await seed.resource("USER_MAIL", u2)
    chats2 = await seed.resource("USER_CHATS", u2)
    await seed.resource("USER_CALENDAR", u2, license_missing=True)     # unlicensed: skipped, never expected
    mail3 = await seed.resource("USER_MAIL", u3)
    site = await seed.resource("SHAREPOINT_SITE")

    # A: two Tier-1 jobs carrying the same stamped lists (one id deferred, one held, plus junk).
    a, held = str(uuid4()), {"overlap_held_resource_ids": [str(chats2)]}
    stamped = {"batch_id": a, "triggered_by": "SCHEDULED", "deferred_resource_ids": [str(chats1), "not-a-uuid"]}
    ja1 = await seed.job("A:1", ago=timedelta(hours=2), resource_ids=[u1, u2], spec=stamped | held)
    ja2 = await seed.job("A:2", ago=timedelta(hours=2), resource_ids=[mail1, mail2], spec=stamped | held)
    await seed.snapshot(ja1, u1)
    await seed.snapshot(ja1, u2, "PARTIAL")
    await seed.snapshot(ja2, mail1, extra={"skipped_concurrent": True}, partitions=["COMPLETED", "IN_PROGRESS"])
    await seed.snapshot(ja2, mail2, "FAILED")
    # B: older, malformed (non-array) lists, one failed and one still running job.
    b = {"batch_id": str(uuid4()), "deferred_resource_ids": "oops", "overlap_held_resource_ids": {"x": 1}}
    jb1 = await seed.job("B:1", ago=timedelta(days=3), status="FAILED", resource_ids=[u3], spec=b)
    jb2 = await seed.job("B:2", ago=timedelta(days=3), status="RUNNING", resource_ids=[mail3],
                         spec={"batch_id": b["batch_id"], "tier2": True})
    await seed.snapshot(jb1, u3, "FAILED")
    await seed.snapshot(jb2, mail3, "IN_PROGRESS", partitions=["QUEUED"])
    # C: legacy single-resource job with no batch_id: the job id is the batch key.
    jc = await seed.job("C:1", ago=timedelta(days=1), resource_id=site, spec={"triggered_by": "PREEMPTIVE"})
    await seed.snapshot(jc, site)
    # D: Tier-2 discovery still running for its user, nothing dispatched yet.
    d = str(uuid4())
    await seed.job("D:1", ago=timedelta(hours=1), status="RUNNING", resource_ids=[u2], spec={"batch_id": d})
    async with seed.factory.begin() as s:
        await s.execute(text("INSERT INTO backup_batches (id, tenant_id, created_at, status, source, scope_user_ids) "
                             "VALUES (:b, :t, now(), 'IN_PROGRESS', 'manual_user', ARRAY[CAST(:u AS uuid)])"),
                        {"b": d, "t": seed.tenant, "u": u2})
        await s.execute(text("INSERT INTO batch_pending_users (batch_id, user_id, state, deadline_at, updated_at) "
                             "VALUES (:b, :u, 'WAITING_DISCOVERY', now() + interval '1 hour', now())"), {"b": d, "u": u2})
    # Never in the feed: another tenant's backup and this tenant's restore.
    await seed.job("X:other-tenant", ago=timedelta(hours=1), resource_ids=[u1], tenant=seed.other_tenant)
    await seed.job("X:restore", ago=timedelta(hours=1), resource_ids=[u1], type="RESTORE")


# Captured from the rollup BEFORE the rewrite (6cb87e0) and checked by hand against the seed:
# A misses only the OneDrive (chats deferred, chats held, calendar unlicensed); B's malformed lists
# are ignored; C is keyed by its job id; D waits on Tier-2 discovery.
_ZERO = dict(any_cancelled=False, any_preemptive=False, single_resource_name=None, single_resource_type=None,
             entra_user_name=None, snap_done=0, snap_failed=0, snap_partial=0, snap_pending=0, snap_skipped=0,
             parts_pending=0, missing_t2=0, skipped_no_license=0, discovery_pending=False)
LOCKED_ROWS = {
    "A": _ZERO | dict(jobs=["A:1", "A:2"], all_jobs_terminal=True, any_job_completed=True, any_job_failed=False,
                      any_tier2_jobs=False, batch_recent=True, bytes_added=Decimal(28), items_added=12,
                      entra_user_count=2, total_resource_count=4, expected_count=9, missing_t2=1,
                      skipped_no_license=1, parts_pending=1, snap_total=4, snap_done=1, snap_skipped=1,
                      snap_partial=1, snap_failed=1, tier1_total=2, tier1_terminal=2, tier2_total=9,
                      tier2_terminal=2),
    "B": _ZERO | dict(jobs=["B:1", "B:2"], all_jobs_terminal=False, any_job_completed=False, any_job_failed=True,
                      any_tier2_jobs=True, batch_recent=False, bytes_added=Decimal(14), items_added=6,
                      entra_user_count=1, entra_user_name="ENTRA_USER 3", total_resource_count=2,
                      expected_count=2, parts_pending=1, snap_total=2, snap_failed=1, snap_pending=1,
                      tier1_total=1, tier1_terminal=1, tier2_total=5, tier2_terminal=0),
    "C": _ZERO | dict(jobs=["C:1"], all_jobs_terminal=True, any_job_completed=True, any_job_failed=False,
                      any_tier2_jobs=False, any_preemptive=True, batch_recent=True, bytes_added=Decimal(7),
                      items_added=3, entra_user_count=0, total_resource_count=0, expected_count=0,
                      single_resource_name="SHAREPOINT_SITE 11", single_resource_type="SHAREPOINT_SITE",
                      snap_total=1, snap_done=1, tier1_total=0, tier1_terminal=0, tier2_total=1, tier2_terminal=1),
    "D": _ZERO | dict(jobs=["D:1"], all_jobs_terminal=False, any_job_completed=False, any_job_failed=False,
                      any_tier2_jobs=False, batch_recent=True, bytes_added=Decimal(0), items_added=0,
                      entra_user_count=1, entra_user_name="ENTRA_USER 2", total_resource_count=1,
                      single_resource_name="ENTRA_USER 2", single_resource_type="ENTRA_USER",
                      discovery_pending=True, expected_count=3, missing_t2=2, skipped_no_license=1,
                      snap_total=0, tier1_total=1, tier1_terminal=0, tier2_total=4, tier2_terminal=0),
}


async def test_rollup_rows_are_unchanged(db):
    await _seed_feed(db)
    assert await _rollup(db) == LOCKED_ROWS


async def _seed_history(seed, batches=6, jobs_per_batch=8, users=10, withheld=300):
    """TM's scheduled shape: every job of a batch carries the same stamped deferred list (TM averaged
    86 ids, up to 324) and each user in scope has Tier-2 children, half of them dispatched."""
    people = [await seed.resource("ENTRA_USER") for _ in range(users)]
    kids = [await seed.resource(t, u) for u in people for t in ("USER_MAIL", "USER_CHATS", "USER_ONEDRIVE")]
    for b in range(batches):
        spec = {"batch_id": str(uuid4()), "deferred_resource_ids": [str(uuid4()) for _ in range(withheld)]}
        for j in range(jobs_per_batch):
            scope = people if j == 0 else kids[j::2 * jobs_per_batch]
            await seed.job(f"H{b}:{j}", ago=timedelta(days=b, hours=1), resource_ids=scope, spec=spec)


async def _plan(seed):
    stmt = build_batch_rollup_query(tenant_id=str(seed.tenant), start_date=None, end_date=None,
                                    operation="BACKUP", size=5000, offset=0)
    sql = str(stmt.compile(dialect=pg_dialect.dialect(), compile_kwargs={"literal_binds": True}))
    async with seed.factory() as s:
        await s.execute(text("ANALYZE jobs, snapshots, snapshot_partitions, resources"))
        return (await s.execute(text("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql))).scalar()[0]["Plan"]


def _nodes(plan):
    yield plan
    for child in plan.get("Plans", ()):
        yield from _nodes(child)


async def test_withheld_ids_do_not_reparse_the_job_spec_per_id(db):
    # 48 jobs x 300 ids. Re-reading the TOASTed spec per id costs ~4 buffer hits each (~58k);
    # reading it once per job and once per distinct stamped list costs a few thousand.
    await _seed_history(db)
    hits = (await _plan(db))["Shared Hit Blocks"]
    assert hits < 15_000, f"{hits} shared buffer hits: the job spec is re-read per withheld id"


async def test_missing_children_are_not_found_by_comparing_every_pair(db):
    # The fanout used to nest-loop expected children against every observed / withheld child
    # (66M comparisons on TM's 30 days). A set difference makes no such comparisons.
    await _seed_history(db)
    compared = sum(n.get("Rows Removed by Join Filter", 0) for n in _nodes(await _plan(db))
                   if "child_resource_id" in n.get("Join Filter", ""))
    assert compared < 1_000, f"{compared} child-id comparisons in fanout joins"
