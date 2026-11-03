"""Resource-granular in-flight accounting for backup-scheduler.

Defect (2026-10-14, Gold USER_CHATS): a wave is ONE 8-member bulk job.
When one member's chat walk ran 12-38 h, the other 7 — already
COMPLETED — stayed "in flight" for every later tick: held by the cron
overlap hold and the drain hold (``partition_overlap_holds``), charged
against the cap budget (``plan_concurrency_budget``) and classified
in_progress by ``check_sla_violations``. ``build_inflight_resource_ids_query``
returned every member of every QUEUED/RUNNING job and
``build_inflight_resource_count_query`` summed
``array_length(batch_resource_ids, 1)``, so neither could tell a finished
member from the one still running. Measured: 72 of 164 held
resource-ticks in 7 days were resources whose snapshot had already
finished.

Contract now:

  * QUEUED job   → every member is in flight.
  * RUNNING job  → a member is in flight only while its own snapshot FOR
                   THAT JOB is not terminal: no snapshot row yet, or a
                   live IN_PROGRESS one. ``skipped_concurrent``
                   placeholders are never in flight.
  * the id set and the count derive from the same member rows and the
    same predicate, so they cannot disagree.

Compile checks run under the postgresql dialect (no DB). The behavioural
half executes the compiled *PostgreSQL text* of the shared predicate
against an in-memory SQLite database with ``json_extract_path_text``
registered as a Python shim — the approach
test_resources_with_backups_restore_point_filter.py already uses.
``unnest`` / ``UNION ALL`` are PostgreSQL-only, so the member expansion
is stood in for by a plain table; the predicate — the part that decides —
runs verbatim (EXISTS / NOT EXISTS / COALESCE / IN behave identically in
both engines).
"""
from __future__ import annotations

import importlib.util
import json
import os
import pathlib
import sqlite3
import sys
import uuid

from sqlalchemy import column, distinct, func, select, table
from sqlalchemy.dialects import postgresql

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret-value")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")

_HELPERS = (
    pathlib.Path(__file__).resolve().parents[2]
    / "services" / "backup-scheduler" / "dispatch_policy.py"
)


def _load():
    spec = importlib.util.spec_from_file_location(
        "backup_scheduler_dispatch_policy_inflightq", _HELPERS
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


dp = _load()
PID = uuid.uuid4()


def _sql(stmt) -> str:
    # literal_binds=True so the JSON path key (Job.spec["sla_policy_id"])
    # and the enum literals render inline — same reasoning as
    # test_backup_scheduler_drain_queries.py.
    return str(stmt.compile(dialect=postgresql.dialect(),
                            compile_kwargs={"literal_binds": True}))


def _predicate_part(sql: str) -> str:
    """Everything after the member subquery alias — the shared WHERE."""
    marker = ") AS inflight_members"
    assert marker in sql, sql
    return sql.split(marker, 1)[1]


# ─── SQL contracts ─────────────────────────────────────────────────────

def test_ids_query_is_resource_granular_with_a_per_job_snapshot_anti_join():
    sql = _sql(dp.build_inflight_resource_ids_query())
    low = sql.lower()

    assert low.startswith("select distinct inflight_members.resource_id")
    # Both job shapes, kept as separate rows so each is judged against
    # its own job's snapshot.
    assert "unnest(jobs.batch_resource_ids)" in low
    assert "jobs.resource_id as resource_id" in low
    assert "union all" in low
    assert "jobs.status in ('queued', 'running')" in low
    # QUEUED members are always in flight.
    assert "inflight_members.job_status = 'queued'" in low
    # RUNNING members: anti-join on THIS job + THIS member, by status.
    assert "snapshots.job_id = inflight_members.job_id" in low
    assert "snapshots.resource_id = inflight_members.resource_id" in low
    assert "not (exists (select 1" in low, "anti-join must be NOT EXISTS"
    assert "not in (select" not in low, (
        "NOT IN over a subquery returns zero rows on one NULL and would "
        "silently release every hold"
    )
    assert "snapshots.status in ('completed', 'partial', 'failed')" in low
    assert "snapshots.status = 'in_progress'" in low
    assert "skipped_concurrent" in low
    # The old job-granular shape must be gone.
    assert "array_length" not in low
    # NOT policy-scoped: a resource busy under ANY job is held.
    assert "sla_policy_id" not in low


def test_terminal_statuses_are_exactly_the_three_settled_outcomes():
    S = dp.SnapshotStatus
    assert set(dp.SNAPSHOT_TERMINAL_STATUSES) == {S.COMPLETED, S.PARTIAL, S.FAILED}
    assert S.IN_PROGRESS not in dp.SNAPSHOT_TERMINAL_STATUSES


def test_count_query_is_policy_scoped_and_counts_distinct_in_flight_members():
    sql = _sql(dp.build_inflight_resource_count_query(PID))
    low = sql.lower()

    assert low.startswith("select count(distinct inflight_members.resource_id)")
    assert "unnest(jobs.batch_resource_ids)" in low
    assert "union all" in low
    assert "jobs.spec ->> 'sla_policy_id'" in low
    assert str(PID) in sql
    # Same anti-join as the id set.
    assert "not (exists (select 1" in low
    assert "snapshots.status in ('completed', 'partial', 'failed')" in low
    assert "skipped_concurrent" in low
    # array_length(batch_resource_ids) is job-granular: it kept a bulk
    # job's COMPLETED members charged against the cap until its slowest
    # member finished.
    assert "array_length" not in low


def test_count_and_ids_queries_share_one_in_flight_predicate():
    ids_sql = _sql(dp.build_inflight_resource_ids_query())
    count_sql = _sql(dp.build_inflight_resource_count_query(PID))
    assert _predicate_part(ids_sql) == _predicate_part(count_sql), (
        "the id set and the count must derive from the same predicate or "
        "the holds and the cap budget drift apart"
    )


# ─── behavioural: run the compiled predicate ───────────────────────────

def _pg_json_extract_path_text(doc, key):
    """SQLite stand-in for PostgreSQL json_extract_path_text(json, text)."""
    if doc is None:
        return None
    try:
        parsed = json.loads(doc)
    except (TypeError, ValueError):  # pragma: no cover - malformed fixture
        return None
    if not isinstance(parsed, dict) or key not in parsed:
        return None
    value = parsed[key]
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return value
    return json.dumps(value)


def _members_stand_in():
    """A plain table with the three columns ``_inflight_members`` exposes,
    typed like the real ones so enum literals render identically."""
    return table(
        "inflight_members",
        column("job_id", dp.Job.id.type),
        column("job_status", dp.Job.status.type),
        column("resource_id", dp.Job.resource_id.type),
    )


def _run(members, snapshots):
    """Execute the compiled predicate over the given rows.

    ``members``: (job_id, job_status, resource_id)
    ``snapshots``: (job_id, resource_id, status, extra_data dict | None)
    Returns (in-flight resource id set, count(DISTINCT) from the count shape).
    """
    m = _members_stand_in()
    ids_sql = _sql(select(m.c.resource_id).where(dp._member_in_flight(m)).distinct())
    count_sql = _sql(
        select(func.count(distinct(m.c.resource_id))).where(dp._member_in_flight(m))
    )
    conn = sqlite3.connect(":memory:")
    try:
        conn.create_function("json_extract_path_text", 2, _pg_json_extract_path_text)
        conn.execute(
            "CREATE TABLE inflight_members (job_id TEXT, job_status TEXT, resource_id TEXT)"
        )
        conn.execute(
            "CREATE TABLE snapshots (job_id TEXT, resource_id TEXT, status TEXT, extra_data TEXT)"
        )
        conn.executemany("INSERT INTO inflight_members VALUES (?, ?, ?)", members)
        conn.executemany(
            "INSERT INTO snapshots VALUES (?, ?, ?, ?)",
            [(j, r, s, None if x is None else json.dumps(x)) for j, r, s, x in snapshots],
        )
        ids = {row[0] for row in conn.execute(ids_sql).fetchall()}
        count = conn.execute(count_sql).fetchone()[0]
    finally:
        conn.close()
    return ids, count


def test_running_job_releases_completed_member_and_holds_in_progress_member():
    """The case from the ticket: one bulk job, member A done, member B
    still walking. Only B is in flight."""
    ids, count = _run(
        members=[("J1", "RUNNING", "A"), ("J1", "RUNNING", "B")],
        snapshots=[("J1", "A", "COMPLETED", None), ("J1", "B", "IN_PROGRESS", None)],
    )
    assert ids == {"B"}
    assert count == 1


def test_running_member_with_no_snapshot_yet_is_still_in_flight():
    ids, _ = _run(
        members=[("J1", "RUNNING", "A"), ("J1", "RUNNING", "C")],
        snapshots=[("J1", "A", "PARTIAL", None)],
    )
    assert ids == {"C"}, "a member the worker has not reached yet is owed, so it is held"


def test_failed_member_is_released_unless_it_is_being_retried_in_the_same_job():
    """FAILED is terminal. But a member retried inside the same job
    carries FAILED + a newer IN_PROGRESS (20 such pairs in prod) — the
    live row wins."""
    ids, _ = _run(
        members=[("J1", "RUNNING", "A"), ("J1", "RUNNING", "D")],
        snapshots=[
            ("J1", "A", "FAILED", None),
            ("J1", "D", "FAILED", None),
            ("J1", "D", "IN_PROGRESS", None),
        ],
    )
    assert ids == {"D"}


def test_queued_job_holds_every_member_regardless_of_snapshots():
    """A QUEUED job has not started; all of it is owed. Resource A is
    COMPLETED under the RUNNING job J1 but queued again under J2, so it
    is in flight through J2 — each row is judged against its own job."""
    ids, count = _run(
        members=[
            ("J1", "RUNNING", "A"),
            ("J2", "QUEUED", "A"),
            ("J2", "QUEUED", "G"),
        ],
        snapshots=[("J1", "A", "COMPLETED", None)],
    )
    assert ids == {"A", "G"}
    assert count == 2


def test_member_is_judged_against_its_own_job_not_any_job():
    """H finished under J1 but J3 (RUNNING) has not produced a snapshot
    for it yet: H is in flight through J3. The anti-join is keyed on
    (job_id, resource_id), never on resource_id alone."""
    ids, _ = _run(
        members=[("J1", "RUNNING", "H"), ("J3", "RUNNING", "H")],
        snapshots=[("J1", "H", "COMPLETED", None)],
    )
    assert ids == {"H"}


def test_skipped_concurrent_placeholder_is_never_in_flight():
    """backup-worker's cross-job guard writes a snapshot row with
    extra_data.skipped_concurrent=true and marks it COMPLETED in ~19 ms.
    It holds no worker: a placeholder-only member is released whether
    the row is already COMPLETED (E) or still IN_PROGRESS (F). Rows that
    predate the flag (extra_data NULL) keep their status meaning (A, B)."""
    ids, count = _run(
        members=[
            ("J1", "RUNNING", "A"),
            ("J1", "RUNNING", "B"),
            ("J1", "RUNNING", "E"),
            ("J1", "RUNNING", "F"),
        ],
        snapshots=[
            ("J1", "A", "COMPLETED", None),
            ("J1", "B", "IN_PROGRESS", None),
            ("J1", "E", "COMPLETED", {"skipped_concurrent": True, "concurrent_job_id": "x"}),
            ("J1", "F", "IN_PROGRESS", {"skipped_concurrent": True}),
        ],
    )
    assert ids == {"B"}
    assert count == 1


def test_real_snapshot_with_an_explicit_false_flag_keeps_its_status_meaning():
    ids, _ = _run(
        members=[("J1", "RUNNING", "A"), ("J1", "RUNNING", "B")],
        snapshots=[
            ("J1", "A", "COMPLETED", {"skipped_concurrent": False, "files_failed": 3}),
            ("J1", "B", "IN_PROGRESS", {"skipped_concurrent": False}),
        ],
    )
    assert ids == {"B"}
