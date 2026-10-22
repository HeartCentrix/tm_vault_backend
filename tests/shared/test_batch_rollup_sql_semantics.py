"""Behavioural tests for the Activity batch-rollup SQL, run on a real Postgres.

WHY THIS FILE EXISTS
--------------------
Every previous pin on ``build_batch_rollup_query`` asserted on the TEXT of
the generated SQL (``sql.index("deferred_t2")``, ``"jsonb_typeof" in
block``). A reviewer demonstrated on 2026-08-11 that those pins survive a
mutation that inverts the logic: the strings are still there, so the tests
still pass while the rollup lies. String-matching a compiled SQL string is
not a test of the decision the SQL makes.

So this module boots a throwaway Postgres cluster (``initdb`` +
``pg_ctl``, unix socket, deleted afterwards), creates the handful of
tables the rollup reads, inserts batches modelled on the real prod rows,
EXECUTES the query and asserts on the numbers that come back — and on the
status / progress_pct that ``shape_batch_row`` derives from them.

Skips (loudly) if no Postgres server binaries are installed. It needs no
network, no docker and no fixture database: ~3 s of initdb per session.
"""
from __future__ import annotations

import glob
import os
import shutil
import subprocess
import tempfile
import types
import uuid
from datetime import datetime, timedelta

import pytest

import sqlalchemy.dialects.postgresql as pg_dialect

from shared.batch_rollup import build_batch_rollup_query, shape_batch_row

asyncpg = pytest.importorskip("asyncpg")


# ── throwaway cluster ──────────────────────────────────────────────────

def _pg_bindir() -> str | None:
    """Locate server binaries. ``initdb``/``pg_ctl`` are NOT on PATH on
    Debian/Ubuntu — only the client tools are — so glob the versioned
    directories too."""
    if shutil.which("initdb") and shutil.which("pg_ctl"):
        return os.path.dirname(shutil.which("initdb"))
    for pat in ("/usr/lib/postgresql/*/bin", "/usr/pgsql-*/bin",
                "/opt/homebrew/opt/postgresql*/bin"):
        for d in sorted(glob.glob(pat), reverse=True):
            if os.path.exists(os.path.join(d, "initdb")):
                return d
    return None


# Minimal schema: only the columns build_batch_rollup_query reads. Enum
# columns are plain text — the query always compares them as ``::text``,
# which is what makes this substitution safe (and is itself worth
# pinning: a bare enum comparison would fail here).
_DDL = """
CREATE TABLE resources (
    id uuid PRIMARY KEY,
    tenant_id uuid,
    type text,
    status text,
    display_name text,
    parent_resource_id uuid,
    metadata json,
    created_at timestamp
);
CREATE TABLE jobs (
    id uuid PRIMARY KEY,
    type text,
    tenant_id uuid,
    status text,
    resource_id uuid,
    batch_resource_ids uuid[],
    spec json,
    created_at timestamp,
    completed_at timestamp
);
CREATE TABLE snapshots (
    id uuid PRIMARY KEY,
    job_id uuid,
    resource_id uuid,
    status text,
    completed_at timestamp,
    bytes_added bigint DEFAULT 0,
    item_count bigint DEFAULT 0,
    -- json, NOT jsonb, matching prod. The rollup reads
    -- extra_data->>'skipped_concurrent' to keep concurrency
    -- placeholders out of the COMPLETED count.
    extra_data json
);
CREATE TABLE snapshot_partitions (
    id uuid PRIMARY KEY,
    job_id uuid,
    snapshot_id uuid,
    status text,
    completed_at timestamp
);
CREATE TABLE batch_pending_users (
    batch_id uuid,
    user_id uuid,
    state text
);
"""


@pytest.fixture(scope="session")
def pg_socket_dir():
    bindir = _pg_bindir()
    if not bindir:
        pytest.skip("no Postgres server binaries (initdb/pg_ctl) installed")
    root = tempfile.mkdtemp(prefix="tmvrollup-")
    data = os.path.join(root, "d")
    # The unix socket path has a 107-byte ceiling, so the socket dir must
    # live directly under the system temp root, never under a long
    # scratchpad path.
    sock = tempfile.mkdtemp(prefix="pgs-")
    try:
        subprocess.run(
            [os.path.join(bindir, "initdb"), "-D", data, "-U", "postgres",
             "--auth=trust", "-E", "UTF8"],
            check=True, capture_output=True,
        )
        subprocess.run(
            [os.path.join(bindir, "pg_ctl"), "-D", data, "-w", "start",
             "-l", os.path.join(root, "log"),
             "-o", f"-p 55433 -k {sock} -c listen_addresses='' -c fsync=off"],
            check=True, capture_output=True,
        )
    except (subprocess.CalledProcessError, OSError) as exc:  # pragma: no cover
        shutil.rmtree(root, ignore_errors=True)
        shutil.rmtree(sock, ignore_errors=True)
        pytest.skip(f"could not start a throwaway Postgres: {exc}")
    try:
        yield sock
    finally:
        subprocess.run([os.path.join(bindir, "pg_ctl"), "-D", data,
                        "-m", "immediate", "stop"], capture_output=True)
        shutil.rmtree(root, ignore_errors=True)
        shutil.rmtree(sock, ignore_errors=True)


@pytest.fixture
async def conn(pg_socket_dir):
    """A connection to a FRESH database, so tests can't see each other."""
    admin = await asyncpg.connect(host=pg_socket_dir, port=55433,
                                  user="postgres", database="postgres")
    dbname = "t" + uuid.uuid4().hex[:12]
    await admin.execute(f'CREATE DATABASE "{dbname}"')
    await admin.close()
    c = await asyncpg.connect(host=pg_socket_dir, port=55433,
                              user="postgres", database=dbname)
    await c.execute(_DDL)
    try:
        yield c
    finally:
        await c.close()


# ── fixture builders ───────────────────────────────────────────────────

TENANT = uuid.uuid4()
NOW = datetime.utcnow()
STARTED = NOW - timedelta(hours=2)          # inside the 48h batch_recent horizon


class Fixture:
    """Builds one batch's worth of rows. Mirrors the SCHEDULED dispatch
    shape: one job per resource type, every job carrying the whole
    per-type slice in ``batch_resource_ids`` and sharing ``spec.batch_id``."""

    def __init__(self, conn):
        self.conn = conn
        self.batch_id = uuid.uuid4()
        self._jobs: list[uuid.UUID] = []

    async def resource(self, rtype, parent=None, status="ACTIVE",
                       created_at=None):
        rid = uuid.uuid4()
        await self.conn.execute(
            "INSERT INTO resources (id, tenant_id, type, status, display_name,"
            " parent_resource_id, created_at) VALUES ($1,$2,$3,$4,$5,$6,$7)",
            rid, TENANT, rtype, status, f"{rtype}-{str(rid)[:8]}", parent,
            created_at or (STARTED - timedelta(days=30)),
        )
        return rid

    async def job(self, resource_ids, *, status="COMPLETED", spec_extra=None):
        jid = uuid.uuid4()
        spec = {"batch_id": str(self.batch_id), "triggered_by": "SCHEDULED"}
        spec.update(spec_extra or {})
        await self.conn.execute(
            "INSERT INTO jobs (id, type, tenant_id, status, batch_resource_ids,"
            " spec, created_at, completed_at)"
            " VALUES ($1,'BACKUP',$2,$3,$4,$5::json,$6,$7)",
            jid, TENANT, status, list(resource_ids),
            __import__("json").dumps(spec), STARTED,
            STARTED + timedelta(minutes=20) if status != "RUNNING" else None,
        )
        self._jobs.append(jid)
        return jid

    async def snapshot(self, job_id, resource_id, status="COMPLETED"):
        sid = uuid.uuid4()
        await self.conn.execute(
            "INSERT INTO snapshots (id, job_id, resource_id, status,"
            " completed_at, bytes_added, item_count) VALUES ($1,$2,$3,$4,$5,1,1)",
            sid, job_id, resource_id, status,
            STARTED + timedelta(minutes=10) if status != "IN_PROGRESS" else None,
        )
        return sid


async def run_rollup(conn):
    """Execute the real builder output and shape every row."""
    stmt = build_batch_rollup_query(
        tenant_id=None, start_date=None, end_date=None,
        operation=None, size=50, offset=0,
    )
    sql = str(stmt.compile(dialect=pg_dialect.dialect(),
                           compile_kwargs={"literal_binds": True}))
    records = await conn.fetch(sql)
    out = {}
    for rec in records:
        row = types.SimpleNamespace(**dict(rec))
        out[str(row.batch_id)] = (row, shape_batch_row(row))
    return out


async def build_gold_batch(conn, *, chats_held: int | None = None,
                           dispatch_chats: bool = False,
                           held_key: bool = True):
    """The prod Gold shape, scaled 1:1 with batch 667734c3 / 2123e84f.

    50 policy resources: 7 ENTRA_USER, 8 USER_MAIL, 8 USER_CALENDAR,
    8 USER_CONTACTS, 8 USER_CHATS, 6 USER_ONEDRIVE, 2 SHAREPOINT_SITE,
    1 USER_ARCHIVE, 1 ROOM_MAILBOX, 1 ENTRA_GROUP. Seven of each USER_*
    type are children of the seven ENTRA_USERs; the 8th belongs to a
    user outside the scope (a shared mailbox), exactly as in prod.

    ``dispatch_chats=False`` reproduces the 10:07 UTC tick: the overlap
    guard withheld the USER_CHATS resources, so they are absent from the
    dispatched scope but still expected as Tier-2 children.
    """
    f = Fixture(conn)
    users = [await f.resource("ENTRA_USER") for _ in range(7)]
    outsider = await f.resource("ENTRA_USER", status="INACCESSIBLE")

    children = {}
    for t in ("USER_MAIL", "USER_CALENDAR", "USER_CONTACTS",
              "USER_CHATS", "USER_ONEDRIVE"):
        n = 7 if t != "USER_ONEDRIVE" else 6
        children[t] = [await f.resource(t, parent=users[i]) for i in range(n)]
        # the 8th of each mail-ish type hangs off a user outside the scope
        if t != "USER_ONEDRIVE":
            children[t].append(await f.resource(t, parent=outsider))

    flat = [await f.resource("SHAREPOINT_SITE") for _ in range(2)]
    flat.append(await f.resource("ROOM_MAILBOX"))
    flat.append(await f.resource("ENTRA_GROUP"))
    flat.append(await f.resource("USER_ARCHIVE", parent=users[0]))

    scope_groups = [("ENTRA_USER", users)]
    for t, ids in children.items():
        if t == "USER_CHATS" and not dispatch_chats:
            continue
        scope_groups.append((t, ids))
    for rid in flat:
        scope_groups.append(("FLAT", [rid]))

    held_ids = []
    if not dispatch_chats and chats_held is not None:
        held_ids = [str(r) for r in children["USER_CHATS"][:chats_held]]
    spec_extra = {"overlap_held_resource_ids": held_ids} if held_key else {}

    jobs = []
    for _, ids in scope_groups:
        jobs.append((await f.job(ids, spec_extra=spec_extra), ids))
    return f, users, children, flat, jobs


# ── DEFECT 1: overlap-held children must not read as a lost fan-out ────

async def test_overlap_hold_without_the_spec_key_still_wedges(conn):
    """Requirement (b): batches dispatched BEFORE the writer shipped have
    no ``overlap_held_resource_ids`` key, and must behave exactly as they
    do today — the phantom persists. Absent must never be read as
    "expect nothing", which would flip settled PARTIAL / In-Progress rows
    to a clean Done across all history."""
    f, users, children, flat, jobs = await build_gold_batch(
        conn, held_key=False)
    for jid, ids in jobs:
        for rid in ids:
            await f.snapshot(jid, rid)
    rows = await run_rollup(conn)
    row, shaped = rows[str(f.batch_id)]
    assert row.missing_t2 == 7, (
        "the 7 withheld USER_CHATS children are still expected — an "
        "un-stamped legacy batch must keep today's behaviour"
    )
    assert shaped["status"] == "In Progress"


async def test_recorded_overlap_hold_lets_the_batch_go_terminal(conn):
    """DEFECT 1. Prod batch 667734c3 (Gold, 2026-08-11 10:07 UTC): 8 jobs
    all COMPLETED, 0 pending snapshots, 0 pending partitions, and
    missing_t2 = 7 — the 7 users' USER_CHATS, which the overlap guard
    withheld because the 02:07 cycle still held them. The row read
    "BACKUP · In Progress · 15 %" and could never clear, because those
    chats were dispatched under a DIFFERENT batch_id at 18:07.

    With the dispatcher's RECORDED hold in the job spec, the rollup can
    tell the deliberate hold from a lost fan-out and the batch settles."""
    f, users, children, flat, jobs = await build_gold_batch(conn, chats_held=7)
    for jid, ids in jobs:
        for rid in ids:
            await f.snapshot(jid, rid)
    rows = await run_rollup(conn)
    row, shaped = rows[str(f.batch_id)]
    assert row.missing_t2 == 0, (
        "a child the DISPATCHER declared withheld is not a lost fan-out"
    )
    assert shaped["status"] == "Done"
    assert shaped["progress_pct"] == 100


async def test_a_child_nobody_recorded_is_still_counted_missing(conn):
    """The anti-inference pin. The REJECTED first attempt inferred the
    hold from job coverage + timing; measured over full prod history that
    heuristic matched 597 (batch, resource) pairs of which 452 (76 %) had
    NO snapshot from the "covering" job and 331 of those jobs had FAILED
    — it would have turned batches that captured nothing into a clean
    Done. Only the dispatcher's own record excuses a child: withhold six
    of the seven and the seventh must still wedge the batch."""
    f, users, children, flat, jobs = await build_gold_batch(conn, chats_held=6)
    for jid, ids in jobs:
        for rid in ids:
            await f.snapshot(jid, rid)
    rows = await run_rollup(conn)
    row, shaped = rows[str(f.batch_id)]
    assert row.missing_t2 == 1
    assert shaped["status"] == "In Progress"


@pytest.mark.parametrize("key", ["overlap_held_resource_ids",
                                 "deferred_resource_ids"])
async def test_a_malformed_hold_list_cannot_break_other_batches(conn, key):
    """j.spec is plain JSON, so a scalar under either withhold key makes
    jsonb_array_elements raise and takes down the rollup for EVERY batch
    on the page — not just the bad one. Same posture as the
    stale-snapshot sweep's failure_state handling.

    Parametrised over both keys because the two CTEs are now generated
    from one helper: a guard dropped from the generator would break both
    at once, and the old pin (a ``"jsonb_typeof" in source`` grep) could
    not see inside a helper at all."""
    f, users, children, flat, jobs = await build_gold_batch(conn, chats_held=7)
    for jid, ids in jobs:
        for rid in ids:
            await f.snapshot(jid, rid)
    # A second, healthy batch that must survive the bad neighbour.
    g = Fixture(conn)
    site = await g.resource("SHAREPOINT_SITE")
    gj = await g.job([site])
    await g.snapshot(gj, site)
    await conn.execute(
        "UPDATE jobs SET spec = jsonb_set(spec::jsonb,"
        f" '{{{key}}}', '\"not-a-list\"')::json"
        " WHERE spec->>'batch_id' = $1", str(g.batch_id))

    rows = await run_rollup(conn)
    assert str(f.batch_id) in rows and str(g.batch_id) in rows
    assert rows[str(f.batch_id)][0].missing_t2 == 0
    assert rows[str(g.batch_id)][1]["status"] == "Done"


async def test_hold_and_cap_stay_separate_reasons(conn):
    """The two withhold reasons are stamped under separate keys on
    purpose (cap = capacity signal, overlap = duration signal). Both must
    be honoured, and neither may be read out of the other's key."""
    f, users, children, flat, jobs = await build_gold_batch(conn, chats_held=7)
    for jid, ids in jobs:
        for rid in ids:
            await f.snapshot(jid, rid)
    rows = await run_rollup(conn)
    assert rows[str(f.batch_id)][0].missing_t2 == 0, (
        "the OVERLAP key alone must excuse the children — it must not "
        "depend on anything also being written to the cap key"
    )
    # Move the ids into the CAP key instead: the batch must still settle,
    # because the cap CTE subtracts them too.
    await conn.execute(
        "UPDATE jobs SET spec = jsonb_set("
        "   jsonb_set(spec::jsonb, '{deferred_resource_ids}',"
        "             spec::jsonb->'overlap_held_resource_ids'),"
        "   '{overlap_held_resource_ids}', '[]')::json"
        " WHERE spec->>'batch_id' = $1", str(f.batch_id))
    rows = await run_rollup(conn)
    assert rows[str(f.batch_id)][0].missing_t2 == 0
    # ...and with the ids in NEITHER key the batch wedges again, proving
    # the subtraction is driven by the recorded fact and nothing else.
    await conn.execute(
        "UPDATE jobs SET spec = jsonb_set(spec::jsonb,"
        " '{deferred_resource_ids}', '[]')::json"
        " WHERE spec->>'batch_id' = $1", str(f.batch_id))
    rows = await run_rollup(conn)
    assert rows[str(f.batch_id)][0].missing_t2 == 7


# ── DEFECT 2: the progress bar must mean something ─────────────────────

async def test_progress_reflects_real_completion_not_a_frozen_15pct(conn):
    """DEFECT 2. Prod 2026-08-11: both live Gold batches computed exactly
    15 %:

        10*(7/50) + 90*(38/250) = 15      (batch 2123e84f)
        10*(7/40) + 90*(31/200) = 15      (batch 667734c3)

    tier1_total was the click-time SCOPE RESOURCE count (50 / 40) while
    tier1_terminal counted only ENTRA_USER snapshots (7) — different
    units — and the Tier-2 denominator was floored at
    scope_resources x 5 = 250 against ~39 real children. Meanwhile 44 of
    50 snapshots were already COMPLETED.

    Here: 50 scope resources, 7 users, 43 content resources; everything
    terminal except two still IN_PROGRESS."""
    f, users, children, flat, jobs = await build_gold_batch(
        conn, chats_held=None, dispatch_chats=True)
    stragglers = flat[:1] + children["USER_ONEDRIVE"][:1]
    running_job = None
    for jid, ids in jobs:
        for rid in ids:
            if rid in stragglers:
                await f.snapshot(jid, rid, status="IN_PROGRESS")
                running_job = jid
            else:
                await f.snapshot(jid, rid)
    await conn.execute("UPDATE jobs SET status='RUNNING', completed_at=NULL"
                       " WHERE id=$1", running_job)

    rows = await run_rollup(conn)
    row, shaped = rows[str(f.batch_id)]
    assert row.tier1_total == 7, "Tier-1 denominator must be USERS, not scope"
    assert row.tier1_terminal == 7
    assert row.tier2_total == 43, (
        "Tier-2 denominator must be the real content set, not scope x 5"
    )
    assert row.tier2_terminal == 41
    assert shaped["status"] == "In Progress"
    # 10*(7/7) + 90*(41/43) = 95.8 -> 95
    assert shaped["progress_pct"] == 95, shaped["progress_pct"]


async def test_progress_is_monotone_and_never_exceeds_100(conn):
    """The two hard-won properties the surrounding comments name: the bar
    must be monotonically non-decreasing through a healthy run (the
    2026-05-15 "16 % -> 90 % -> 30 % -> 99 %" incident) and must never
    exceed 100. Replays a whole batch snapshot-by-snapshot and re-reads
    the bar after every write."""
    f, users, children, flat, jobs = await build_gold_batch(
        conn, chats_held=None, dispatch_chats=True)
    await conn.execute("UPDATE jobs SET status='RUNNING', completed_at=NULL")

    seen = []
    for jid, ids in jobs:
        for rid in ids:
            # in-flight first, then terminal: the bar sees both states
            snap = await f.snapshot(jid, rid, status="IN_PROGRESS")
            rows = await run_rollup(conn)
            seen.append(rows[str(f.batch_id)][1]["progress_pct"])
            await conn.execute(
                "UPDATE snapshots SET status='COMPLETED', completed_at=$2"
                " WHERE id=$1", snap, STARTED + timedelta(minutes=10))
            rows = await run_rollup(conn)
            seen.append(rows[str(f.batch_id)][1]["progress_pct"])

    assert all(0 <= p <= 100 for p in seen), seen
    regressions = [(a, b) for a, b in zip(seen, seen[1:]) if b < a]
    assert not regressions, f"progress went backwards: {regressions}"
    assert seen[-1] >= 95, f"a nearly-complete run must read high: {seen[-1]}"


async def test_tier2_counts_every_workload_not_a_hardcoded_type_list(conn):
    """The Tier-2 numerator used to be an explicit USER_* type list, so a
    workload absent from it (USER_ARCHIVE was bolted on exactly that way,
    and SHAREPOINT_SITE / ROOM_MAILBOX / ENTRA_GROUP never were) could
    complete without moving the bar at all. Replaces the old
    ``count("'USER_ARCHIVE'") >= 2`` source grep with the behaviour it
    was trying to protect."""
    f = Fixture(conn)
    user = await f.resource("ENTRA_USER")
    archive = await f.resource("USER_ARCHIVE", parent=user)
    site = await f.resource("SHAREPOINT_SITE")
    room = await f.resource("ROOM_MAILBOX")
    j = await f.job([user, archive, site, room], status="RUNNING")
    await f.snapshot(j, user)
    before = (await run_rollup(conn))[str(f.batch_id)][0]
    assert before.tier2_terminal == 0
    for rid in (archive, site, room):
        await f.snapshot(j, rid)
    after, shaped = (await run_rollup(conn))[str(f.batch_id)]
    assert after.tier2_terminal == 3, (
        "USER_ARCHIVE / SHAREPOINT_SITE / ROOM_MAILBOX completions must "
        "advance the Tier-2 bar"
    )
    assert shaped["progress_pct"] > before.tier2_terminal


async def test_pre_discovery_floor_keeps_the_bar_off_100(conn):
    """Click-path shape: scope is 7 ENTRA_USERs, Tier-1 snapshots done,
    Tier-2 discovery has not produced children yet. Without a Tier-2
    denominator floor the bar takes the Tier-1-only branch, reads 100 %
    and then collapses when the children land."""
    f = Fixture(conn)
    users = [await f.resource("ENTRA_USER") for _ in range(7)]
    j = await f.job(users, status="RUNNING")
    for u in users:
        await f.snapshot(j, u)
    row, shaped = (await run_rollup(conn))[str(f.batch_id)]
    assert row.tier1_total == 7 and row.tier1_terminal == 7
    assert row.tier2_total == 35, "floor is 5 workloads per USER"
    assert shaped["progress_pct"] == 10, (
        "Tier-1 is worth 10 % of the bar; Tier-2 has not started"
    )


async def test_license_skips_shrink_customer_progress_floor_without_hiding_failure(conn):
    """Local license skips and customer resource-count progress use one scope."""
    f = Fixture(conn)
    user = await f.resource("ENTRA_USER")
    mail = await f.resource("USER_MAIL", parent=user)
    for kind in ("USER_ONEDRIVE", "USER_CHATS", "USER_CALENDAR", "USER_CONTACTS"):
        skipped = await f.resource(kind, parent=user)
        await conn.execute(
            "UPDATE resources SET metadata = '{\"license_missing\": true}'::json WHERE id=$1",
            skipped,
        )
    job = await f.job([user])
    await f.snapshot(job, user)
    row, shaped = (await run_rollup(conn))[str(f.batch_id)]
    assert row.skipped_no_license == 4
    assert row.missing_t2 == 1
    assert row.tier2_total == 1
    assert shaped["status"] == "In Progress"

    mail_job = await f.job([mail], spec_extra={"tier2": True})
    await f.snapshot(mail_job, mail, "FAILED")
    row, shaped = (await run_rollup(conn))[str(f.batch_id)]
    assert row.missing_t2 == 0
    assert shaped["status"] == "Done"
    assert shaped["warnings"] == {"partial": 0, "failed": 1, "skipped_no_license": 4}


async def test_scheduled_archive_flag_controls_missing_child_on_real_sql(conn, monkeypatch):
    f = Fixture(conn)
    user = await f.resource("ENTRA_USER")
    await f.resource("USER_ARCHIVE", parent=user)
    job = await f.job([user])
    await f.snapshot(job, user)
    monkeypatch.setenv("ARCHIVE_BACKUP_ENABLED", "true")
    row, shaped = (await run_rollup(conn))[str(f.batch_id)]
    assert row.missing_t2 == 1
    assert shaped["status"] == "In Progress"
    monkeypatch.setenv("ARCHIVE_BACKUP_ENABLED", "false")
    row, shaped = (await run_rollup(conn))[str(f.batch_id)]
    assert row.missing_t2 == 0
    assert shaped["status"] == "Done"
