"""Unit tests for the Activity-Manager batch-rollup state machine.

Pure-function tests — no DB. Each test pins one branch of the design's
§7 state machine so an accidental refactor that breaks the contract
fails loudly.

Imports from ``shared.batch_rollup`` — the same import pattern every
other shared helper uses.
"""
from __future__ import annotations

from shared.batch_rollup import (
    derive_batch_status,
    RollupCounts,
    build_batch_rollup_query,
)


def _r(**kw):
    """Helper: build a RollupCounts with sane defaults."""
    defaults = dict(
        all_jobs_terminal=True, any_cancelled=False, any_job_failed=False,
        snap_total=0, snap_done=0, snap_partial=0, snap_failed=0,
        snap_pending=0, parts_pending=0, missing_t2=0,
        expected_total=0, discovery_pending=False, skipped_no_license=0,
    )
    defaults.update(kw)
    return RollupCounts(**defaults)


# ─── USER_ARCHIVE expected-set gating (ARCHIVE_BACKUP_ENABLED) ──────────

def _rollup_sql():
    return str(build_batch_rollup_query(
        tenant_id=None, start_date=None, end_date=None,
        operation=None, size=10, offset=0,
    ))


def test_archive_counts_toward_batch_when_flag_on(monkeypatch):
    # Default (and explicit true) → archive is an expected Tier-2 child, so a
    # batch is not "Done" until the archive snapshots are terminal.
    monkeypatch.setenv("ARCHIVE_BACKUP_ENABLED", "true")
    assert "USER_ARCHIVE" in _rollup_sql()


def test_archive_dropped_from_expected_when_flag_off(monkeypatch):
    # Flag off → archive is silently dropped from EXPECTED at query-build time,
    # so a batch never pins to In Progress waiting on archive snapshots that
    # will never come (no hang). This is what makes the flag safely reversible.
    monkeypatch.setenv("ARCHIVE_BACKUP_ENABLED", "false")
    sql = _rollup_sql()
    expected = sql[sql.index("expected_t2 AS ("):sql.index("license_skip AS (")]
    assert "'USER_ARCHIVE'" not in expected


# ─── state machine: In Progress branches ───────────────────────────────

def test_pending_jobs_means_in_progress():
    r = _r(all_jobs_terminal=False)
    assert derive_batch_status(r) == ("In Progress", None)


def test_pending_snapshots_means_in_progress():
    r = _r(snap_pending=1)
    assert derive_batch_status(r) == ("In Progress", None)


def test_pending_partitions_means_in_progress():
    r = _r(parts_pending=3)
    assert derive_batch_status(r) == ("In Progress", None)


def test_fanout_incomplete_means_in_progress_even_when_all_terminal():
    # The exact bug we're fixing: Tier-1 done, Tier-2 not yet spawned.
    r = _r(all_jobs_terminal=True, snap_done=9, missing_t2=45)
    assert derive_batch_status(r) == ("In Progress", None)


def test_no_children_yet_means_in_progress():
    # Click landed; Jobs created but not yet started.
    r = _r(all_jobs_terminal=False, snap_total=0)
    assert derive_batch_status(r) == ("In Progress", None)


def test_in_flight_beats_failed():
    # 4 jobs done, 1 failed, 1 still running — must show In Progress.
    r = _r(all_jobs_terminal=False, snap_done=4, snap_failed=1, snap_pending=1)
    assert derive_batch_status(r) == ("In Progress", None)


# ─── state machine: terminal branches ──────────────────────────────────

def test_all_cancelled_means_canceled():
    r = _r(any_cancelled=True, snap_total=0)
    assert derive_batch_status(r) == ("Canceled", None)


def test_cancelled_but_partial_success_is_done_with_warnings():
    # Operator cancelled mid-run; 3 snapshots completed before cancel landed.
    r = _r(any_cancelled=True, snap_done=3, snap_total=10)
    status, warnings = derive_batch_status(r)
    assert status == "Done"
    assert warnings is not None
    assert warnings["failed"] == 0


def test_all_failed_means_failed():
    r = _r(snap_failed=5, snap_total=5)
    assert derive_batch_status(r) == ("Failed", None)


def test_retention_pruned_batch_is_expired_not_failed():
    # Root cause of "2 of 3 daily backups Failed": all jobs COMPLETED (no
    # failure), but GFS retention later DELETED every snapshot of this fire, so
    # snap_total==0. The backup succeeded and its restore point aged out under
    # policy — that is Expired, NOT Failed. Mislabeling a policy-pruned backup
    # as a failure is what alarmed the operator (every non-newest daily GFS
    # fire showed "Failed").
    r = _r(all_jobs_terminal=True, any_job_failed=False,
           snap_total=0, snap_done=0, snap_partial=0, snap_failed=0)
    assert derive_batch_status(r) == ("Expired", None)


def test_job_failed_with_no_snapshots_is_failed_not_expired():
    # A job that errored before writing any snapshot is a REAL failure, not an
    # expiry — any_job_failed guards the Expired branch.
    r = _r(all_jobs_terminal=True, any_job_failed=True,
           snap_total=0, snap_done=0, snap_partial=0)
    assert derive_batch_status(r) == ("Failed", None)


def test_clean_done():
    r = _r(snap_done=10, snap_total=10)
    assert derive_batch_status(r) == ("Done", None)


def test_partial_and_done_means_done_with_warnings():
    r = _r(snap_done=7, snap_partial=2, snap_failed=1, snap_total=10)
    status, warnings = derive_batch_status(r)
    assert status == "Done"
    assert warnings == {"partial": 2, "failed": 1}


def test_partial_only_no_clean_done_still_done_with_warnings():
    # Mailbox folder 403s — all PARTIAL, no FAILED, no clean COMPLETED.
    r = _r(snap_partial=5, snap_total=5)
    status, warnings = derive_batch_status(r)
    assert status == "Done"
    assert warnings == {"partial": 5, "failed": 0}


def test_discovery_pending_keeps_batch_in_progress_even_when_tier1_done():
    """The 2026-05-16 incident: 9 Tier-1 ENTRA_USERs all COMPLETED, no
    Tier-2 children discovered yet, batch_pending_users still says
    WAITING_DISCOVERY for some user. Without the discovery_pending gate
    the rollup would compute missing_t2=0 (no Tier-2 resources exist
    yet) and falsely flip to "Done"."""
    r = _r(
        all_jobs_terminal=True,
        snap_done=9, snap_total=9,
        snap_pending=0, parts_pending=0, missing_t2=0,
        discovery_pending=True,
    )
    assert derive_batch_status(r) == ("In Progress", None)


def test_discovery_complete_unblocks_done_branch():
    """Mirror of the above: same state but discovery is finished. Now
    the existing branches apply normally and we settle as Done."""
    r = _r(
        all_jobs_terminal=True,
        snap_done=54, snap_total=54,
        snap_pending=0, parts_pending=0, missing_t2=0,
        discovery_pending=False,
    )
    assert derive_batch_status(r) == ("Done", None)


# ─── license-missing: skipped-workload accounting ──────────────────────

def test_clean_done_with_license_skips_shows_skipped_chip():
    """A user whose Mail/OneDrive/Calendar are unlicensed: those workloads
    are excluded from expected (missing_t2 stays 0 — see SQL), the backable
    workloads all completed cleanly, and the row settles Done with a neutral
    'N skipped (no license)' chip instead of stalling In Progress. This is the
    Amit-Kumar / Lyly '14% forever' bug."""
    r = _r(snap_done=2, snap_total=2, missing_t2=0, skipped_no_license=3)
    status, warnings = derive_batch_status(r)
    assert status == "Done"
    assert warnings == {"skipped_no_license": 3}


def test_license_skips_merge_with_partial_failed_warnings():
    """Skipped-no-license must not clobber the existing partial/failed chip —
    both are surfaced together."""
    r = _r(snap_done=1, snap_partial=1, snap_failed=0, snap_total=2,
           skipped_no_license=2)
    status, warnings = derive_batch_status(r)
    assert status == "Done"
    assert warnings == {"partial": 1, "failed": 0, "skipped_no_license": 2}


def test_no_license_skips_keeps_clean_done_warnings_none():
    """Regression guard: with zero skips a clean batch is still ('Done', None)
    — the skipped key is only added when there is something to report."""
    r = _r(snap_done=5, snap_total=5, skipped_no_license=0)
    assert derive_batch_status(r) == ("Done", None)


def test_all_backable_skipped_but_clean_is_done_not_failed():
    """Edge: a batch whose only backable work already succeeded elsewhere so
    snap_total==0 here, and the remaining workloads are all license-skipped.
    That is a successful (fully-skipped) run, NOT a Failed/Expired one."""
    r = _r(all_jobs_terminal=True, any_job_failed=False,
           snap_total=0, snap_done=0, skipped_no_license=2)
    status, warnings = derive_batch_status(r)
    assert status == "Done"
    assert warnings == {"skipped_no_license": 2}


def test_sql_excludes_license_missing_and_surfaces_skip_count():
    """The SQL must (a) exclude license_missing Tier-2 children from the
    expected/fanout set so they never count as 'missing', and (b) surface a
    skipped_no_license count column for the chip."""
    stmt = build_batch_rollup_query(
        tenant_id=None, start_date=None, end_date=None,
        operation=None, size=50, offset=0,
    )
    sql = str(stmt)
    assert "license_missing" in sql, "SQL must reference license_missing to exclude it"
    assert "skipped_no_license" in sql, "SQL must surface skipped_no_license column"


# ─── SQL builder smoke ─────────────────────────────────────────────────

def test_build_batch_rollup_query_returns_a_sql_object_with_named_params():
    """Builder must produce a parameterised statement containing every
    CTE the design promised. We don't execute it against a real DB
    here — that's the integration test's job. We assert structure so
    the handler can rely on the contract."""
    stmt = build_batch_rollup_query(
        tenant_id="00000000-0000-0000-0000-000000000000",
        start_date=None,
        end_date=None,
        operation=None,
        size=50,
        offset=0,
    )
    sql = str(stmt)
    for cte in (
        "filtered_jobs", "batches", "snap_roll",
        "parts_roll", "fanout",
    ):
        assert cte in sql, f"missing CTE: {cte}"
    # The status / warnings derivation is in Python, but the SQL must
    # surface the column inputs.
    for col in ("snap_pending", "parts_pending", "missing_t2"):
        assert col in sql, f"missing column: {col}"


def test_expected_t2_only_counts_dispatchable_children():
    """expected_t2 must mirror the backup-scheduler's status filter
    (ResourceStatus DISCOVERED/ACTIVE). Discovery creates INACCESSIBLE
    USER_CHATS/USER_ONEDRIVE children under unlicensed identity accounts
    (shared/room mailboxes); the scheduler refuses to dispatch those, so
    expecting them wedges every batch containing the parent at
    "In Progress" forever — retroactively too, because expectations are
    evaluated against the current resources table (2026-07-15,
    Corp_Esplanade / DBA Requests)."""
    stmt = build_batch_rollup_query(
        tenant_id="00000000-0000-0000-0000-000000000000",
        start_date=None,
        end_date=None,
        operation=None,
        size=50,
        offset=0,
    )
    sql = str(stmt)
    fanout_src = sql[sql.index("expected_t2"):sql.index("observed_t2")]
    assert "r2.status::text IN ('DISCOVERED', 'ACTIVE')" in fanout_src, (
        "expected_t2 lost its dispatchable-status filter — INACCESSIBLE "
        "children will wedge batch rollups at In Progress"
    )
    assert "r2.created_at <= b.started_at" in fanout_src, (
        "expected_t2 lost its existed-at-dispatch gate — children added "
        "by a LATER discovery run retroactively wedge finished batches"
    )


def test_expected_t2_excludes_user_archive_when_scheduled_coverage_disabled(monkeypatch):
    """Local archive scheduling is reversible through the shared flag."""
    monkeypatch.setenv("ARCHIVE_BACKUP_ENABLED", "false")
    stmt = build_batch_rollup_query(
        tenant_id="00000000-0000-0000-0000-000000000000",
        start_date=None,
        end_date=None,
        operation=None,
        size=50,
        offset=0,
    )
    sql = str(stmt)
    fanout_src = sql[sql.index("expected_t2"):sql.index("observed_t2")]
    type_list = fanout_src.split("AND r2.type::text IN")[1].split(")")[0]
    assert "'USER_ARCHIVE'" not in type_list, (
        "expected_t2 must not expect USER_ARCHIVE while nothing "
        "dispatches it — it wedges batches at In Progress forever"
    )


def test_tier_split_has_no_workload_allowlist():
    """SUPERSEDES ``test_tier_split_counts_user_archive_children``.

    That pin required ``'USER_ARCHIVE'`` to appear twice in tier_split,
    because the Tier-2 numerator and denominator were an explicit
    USER_* type allowlist that had to be edited by hand every time a
    workload was added — USER_ARCHIVE was bolted on exactly that way,
    and SHAREPOINT_SITE / ROOM_MAILBOX / ENTRA_GROUP never were, so on
    the scheduled path their completions moved the bar by zero.

    2026-08-11: the allowlist is gone. Tier-2 is now defined by
    EXCLUSION — every resource that is not the ENTRA_USER identity
    record — so no workload can ever be forgotten again. This pin keeps
    the allowlist from creeping back; the behaviour it protects
    (a USER_ARCHIVE / SHAREPOINT_SITE / ROOM_MAILBOX completion advances
    the bar) is proved by executing the query against a real Postgres in
    tests/shared/test_batch_rollup_sql_semantics.py::
    test_tier2_counts_every_workload_not_a_hardcoded_type_list.
    """
    stmt = build_batch_rollup_query(
        tenant_id="00000000-0000-0000-0000-000000000000",
        start_date=None,
        end_date=None,
        operation=None,
        size=50,
        offset=0,
    )
    sql = str(stmt)
    tier_split_src = sql[sql.index("tier_split AS ("):sql.index("total_expected AS (")]
    code = "\n".join(ln for ln in tier_split_src.splitlines()
                     if not ln.strip().startswith("--"))
    for workload in ("'USER_MAIL'", "'USER_ARCHIVE'", "'USER_ONEDRIVE'",
                     "'SHAREPOINT_SITE'"):
        assert workload not in code, (
            f"tier_split names {workload} again — a hand-maintained "
            f"workload allowlist is how whole workloads stopped counting "
            f"toward the progress bar in the first place"
        )
    assert "<> 'ENTRA_USER'" in code, (
        "Tier-2 must be defined as everything that is not the ENTRA_USER "
        "identity record"
    )


# ─── query shape: page-scoped batch cap (perf, 2026-07-13) ─────────────
#
# The final SELECT joins ~10 materialized CTEs on batch_id. CTE stats
# are opaque to the planner (rows=1 estimates), so it nests loops —
# with N batches that's O(N^2) CTE rescans: at 475 batches the Activity
# query ran 79s, and it grows with every scheduled job forever. The fix
# caps the `batches` CTE to the page window (size+offset, newest first)
# BEFORE the downstream CTEs fan out; the final ORDER BY/LIMIT/OFFSET
# semantics are unchanged (top size+offset batches, then slice).

def _compiled_sql(**kw):
    import sqlalchemy.dialects.postgresql as pg
    defaults = dict(tenant_id=None, start_date=None, end_date=None,
                    operation="BACKUP", size=20, offset=0)
    defaults.update(kw)
    stmt = build_batch_rollup_query(**defaults)
    return str(stmt.compile(dialect=pg.dialect(),
                            compile_kwargs={"literal_binds": True}))


def test_batches_cte_is_capped_to_page_window():
    sql = _compiled_sql(size=20, offset=0)
    assert "batches_all" in sql, "expected uncapped aggregate CTE batches_all"
    # the capped batches CTE selects the newest page window
    assert "LIMIT 20" in sql.replace("\n", " ") or ":batch_cap" in sql


def test_batch_cap_covers_offset_pages():
    sql = _compiled_sql(size=20, offset=40)
    # page 3: downstream CTEs must see the top 60 batches
    assert "LIMIT 60" in sql.replace("\n", " ")


def test_final_pagination_unchanged():
    sql = _compiled_sql(size=20, offset=40)
    flat = " ".join(sql.split())
    assert "LIMIT 20 OFFSET 40" in flat


# --- retention-pruned history fallback (2026-07-14) --------------------
# Retention deletes snapshot rows of old batches while their jobs stay
# COMPLETED. Judging success by snap_done flipped every aged-out Done
# batch to "Failed" on the Activity page. With zero snapshot rows the
# terminal Job statuses are the durable record -- trust them.

def test_retention_pruned_history_trusts_completed_jobs():
    r = _r(snap_total=0, any_job_completed=True)
    assert derive_batch_status(r) == ("Done", None)


def test_no_snapshots_and_no_completed_jobs_is_failed():
    r = _r(snap_total=0, any_job_failed=True)
    assert derive_batch_status(r) == ("Failed", None)


def test_no_snapshots_all_cancelled_is_canceled():
    r = _r(snap_total=0, any_cancelled=True)
    assert derive_batch_status(r) == ("Canceled", None)


def test_pruned_fallback_never_preempts_in_flight_work():
    # snapshots gone but a job is still moving -> In Progress wins
    r = _r(snap_total=0, any_job_completed=True, all_jobs_terminal=False)
    assert derive_batch_status(r) == ("In Progress", None)


# ── 2026-08-03 retroactive-wedge pins ────────────────────────────────


def _rollup_sql() -> str:
    """The SQL the builder actually EMITS.

    Was ``inspect.getsource(build_batch_rollup_query)`` — the Python
    source of the function. That silently stopped covering anything the
    builder assembles from a helper: on 2026-08-11 the two withhold-
    reason CTEs moved into ``_withheld_children_cte`` and every pin that
    read the function body went blind (they failed loudly here only
    because the strings vanished outright; a helper that emitted WRONG
    SQL would have passed). Read the output, not the source.

    Structural pins like these are a backstop only — the executable
    checks live in tests/shared/test_batch_rollup_sql_semantics.py,
    which runs the query on a real Postgres.
    """
    from shared.batch_rollup import build_batch_rollup_query as _b
    return str(_b(tenant_id=None, start_date=None, end_date=None,
                  operation=None, size=50, offset=0))


def test_missing_t2_wedges_only_recent_batches():
    """Expectations are computed against the CURRENT resources table, so
    a status flip there rewrites history: re-enabling a long-INACCESSIBLE
    child (2026-08-03 discovery reset) flipped EVERY settled Gold batch
    back to 'In Progress'. The missing_t2 gate exists for the
    minutes-scale post-click flicker — it must not wedge old batches."""
    # Fresh batch: missing children hold it In Progress (anti-flicker).
    status, _ = derive_batch_status(_r(
        snap_total=3, snap_done=3, missing_t2=2,
        batch_recent=True, any_tier2_jobs=True,
    ))
    assert status == "In Progress"
    # Old batch whose fan-out DID run: missing children = expectation
    # drift from later resource-status changes — clean Done.
    status, warnings = derive_batch_status(_r(
        snap_total=3, snap_done=3, missing_t2=2,
        batch_recent=False, any_tier2_jobs=True,
    ))
    assert status == "Done" and warnings is None
    # The SQL must expose the horizon flag for the derive to consume.
    sql = _rollup_sql()
    assert "NOW() - INTERVAL '48 hours'" in sql and "batch_recent" in sql


def test_lost_fanout_is_a_visible_warning_not_a_silent_done():
    """A batch whose Tier-2 fan-out NEVER spawned (trigger-bulk POST lost
    after BACKUP_ENQUEUED committed) has only ~1KB Tier-1 metadata
    persisted. Past the horizon it must not wedge — but a clean Done
    would be a false success on a backup product. Warnings chip instead,
    with the unspawned children counted as failed."""
    status, warnings = derive_batch_status(_r(
        snap_total=3, snap_done=3, missing_t2=5,
        batch_recent=False, any_tier2_jobs=False,
    ))
    assert status == "Done"
    assert warnings == {"partial": 0, "failed": 5}
    # And while still recent, the same batch stays In Progress.
    status, _ = derive_batch_status(_r(
        snap_total=3, snap_done=3, missing_t2=5,
        batch_recent=True, any_tier2_jobs=False,
    ))
    assert status == "In Progress"


def test_user_archive_is_an_expected_child_with_scheduled_coverage(monkeypatch):
    """Scheduled archive coverage ships with the local archive flag."""
    monkeypatch.setenv("ARCHIVE_BACKUP_ENABLED", "true")
    sql = _rollup_sql()
    i = sql.find("expected_t2")
    block = sql[i:sql.find("observed_t2")]
    assert "USER_ARCHIVE" in block.replace(
        "-- USER_ARCHIVE is deliberately absent", ""
    ).split("AND r2.type::text IN")[1].split(")")[0], (
        "enabled USER_ARCHIVE must be in expected_t2's type list"
    )


# ─── scheduler-deferred children must not read as lost fan-out ─────────

def test_deferred_resources_are_excluded_from_missing_t2():
    """2026-08-05 RCA: the backup-scheduler's max_concurrent_backups trim
    silently held one resource back per tick. The rollup computes
    expected_t2 from the CURRENT resources table, so a DELIBERATELY
    deferred child was indistinguishable from a LOST fan-out — it landed
    in missing_t2 and wedged the Activity row at "In Progress · 15 %" for
    the full 48 h batch_recent horizon while every job and snapshot was
    already terminal. That is the ghost "previous run still going" row
    the operator reported (Gold, Aug 4 02:07 UTC, John Lucas USER_MAIL).

    The scheduler now stamps spec.deferred_resource_ids on every job in
    the batch; the rollup must subtract them."""
    sql = _rollup_sql()
    assert "deferred_t2" in sql, (
        "rollup lost its deferred-children CTE — scheduler-deferred "
        "resources will wedge batches at In Progress for 48h"
    )
    assert "deferred_resource_ids" in sql, (
        "deferred_t2 must read spec.deferred_resource_ids"
    )
    fanout = sql[sql.index("fanout AS"):sql.index("entra_count")]
    assert "df.child_resource_id IS NULL" in fanout, (
        "missing_t2 must not count children the scheduler deferred"
    )


def test_deferred_t2_survives_a_malformed_spec_value():
    """A non-array deferred_resource_ids (hand-edited row, older writer)
    must not abort the rollup for EVERY other batch — jsonb_array_elements
    raises on a scalar. Mirrors the type-checking the stale-snapshot
    sweep already does on failure_state."""
    sql = _rollup_sql()
    block = sql[sql.index("deferred_t2"):sql.index("observed_t2")]
    assert "jsonb_typeof" in block, (
        "deferred_t2 must type-check the JSON value before unnesting it"
    )


def test_deferred_children_do_not_suppress_a_genuine_lost_fanout():
    """The deferral exclusion must be surgical: a batch whose fan-out
    genuinely never spawned still has to surface as a warning, not a
    clean Done. Pins the derive side stays untouched."""
    status, warnings = derive_batch_status(_r(
        snap_total=3, snap_done=3, missing_t2=5,
        batch_recent=False, any_tier2_jobs=False,
    ))
    assert status == "Done" and warnings == {"partial": 0, "failed": 5}


# ─── operation filter must stay sargable (2026-08-13) ──────────────────
# `op_filter` used to emit `AND j.type::text = :op`. Casting the enum
# COLUMN to text is non-sargable against idx_jobs_tenant_type_status and,
# worse, destroys the planner's selectivity estimate — the ~10 materialized
# CTEs downstream then get rows=1 estimates and nest-loop, which is the
# O(N^2) blowup the 2026-07-13 page-cap comment above already warned about.
# Measured on prod 2026-08-13, identical query, only this predicate differing:
#   operation=None   cap=200 -> 0.25s
#   operation=BACKUP cap=20  -> 2.68s
#   operation=BACKUP cap=200 -> >60s (statement timeout)
# That is what took /api/v1/activity down: every request held a connection
# for 70+ minutes until the 40+40 pool exhausted and the endpoint 500'd.

def test_operation_filter_does_not_cast_the_type_column_to_text():
    """The enum column must be compared directly so the index is usable."""
    sql = _compiled_sql(operation="BACKUP")
    flat = " ".join(sql.split())
    assert "j.type::text = " not in flat, (
        "op_filter casts the enum column to text — non-sargable, and it "
        "wrecks the row estimates that keep the downstream CTEs from "
        "nest-looping. Compare the column to an enum literal instead."
    )


def test_operation_filter_still_restricts_by_operation():
    """Losing the cast must not lose the filter.

    Asserted with RESTORE, not BACKUP: the query carries an unconditional
    ``AND j.type = 'BACKUP'``, so a BACKUP assertion would pass even if
    op_filter were deleted outright and would prove nothing.
    """
    sql = _compiled_sql(operation="RESTORE")
    flat = " ".join(sql.split())
    assert "j.type = 'RESTORE'" in flat


def test_unknown_operation_is_rejected_not_interpolated():
    """Operation reaches SQL as a literal, so it must be allow-listed."""
    import pytest
    with pytest.raises(ValueError):
        _compiled_sql(operation="BACKUP'; DROP TABLE jobs; --")


def test_allowed_operations_matches_jobtype_enum():
    """The inlined allow-list must not drift from the real JobType enum.

    ``_ALLOWED_OPERATIONS`` is hand-written (see the comment on it: importing
    shared.models would drag shared.config's JWT_SECRET validation into a pure
    SQL builder). This test is what keeps the copy honest — a new JobType
    member with no entry here would otherwise be rejected as "unknown".
    """
    import os
    import secrets

    os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
    os.environ.setdefault("JWT_SECRET", secrets.token_hex(32))

    from shared.models import JobType
    from shared.batch_rollup import _ALLOWED_OPERATIONS

    assert _ALLOWED_OPERATIONS == {m.value for m in JobType}


# ─── skipped_concurrent placeholders are not successes ────────────────
#
# backup-worker's cross-job concurrency guard writes a 0-item snapshot with
# status=COMPLETED when another job already holds a live lease on the
# resource. Counting those as done made a resource that captured NOTHING
# read green — and the state that produces them (a partition stranded
# IN_PROGRESS renewing its lease forever) means EVERY later cycle for that
# resource is a placeholder. Observed 2026-08-17 on `Chats — Corp_Esplanade`.
#
# retention_cleanup, dr-replication-worker, the restore-point filter and the
# scheduler all already reject them; the rollup was the last consumer that
# did not.

def test_deferred_only_batch_is_done_with_a_warning_not_failed():
    """All placeholders: nothing broke, nothing captured.

    Must NOT be Failed — excluding placeholders from snap_done drops
    straight through the "no successes" arm otherwise, which reports
    breakage where there is none.
    """
    r = _r(snap_total=2, snap_done=0, snap_skipped=2)
    status, warning = derive_batch_status(r)
    assert status == "Done"
    assert warning and "skipped_concurrent" in warning


def test_placeholders_do_not_mask_a_real_failure():
    r = _r(snap_total=3, snap_done=0, snap_skipped=2, snap_failed=1)
    assert derive_batch_status(r)[0] == "Failed"


def test_placeholders_alongside_real_successes_still_done():
    r = _r(snap_total=3, snap_done=2, snap_skipped=1)
    assert derive_batch_status(r)[0] == "Done"


def test_a_genuinely_empty_batch_is_unaffected():
    """No snapshots at all still follows the retention-pruned arm."""
    r = _r(snap_total=0, any_job_completed=True)
    assert derive_batch_status(r) == ("Done", None)
