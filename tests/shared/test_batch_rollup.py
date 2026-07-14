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
    assert "USER_ARCHIVE" not in _rollup_sql()


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
