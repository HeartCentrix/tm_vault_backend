"""STEP C orphan-job finalize must treat PARTIAL snapshots as success (2026-08-19).

Every other job finalizer agrees on the job-level rollup semantics:
COMPLETED if any child snapshot made progress (COMPLETED *or* PARTIAL),
FAILED only when every single child failed —
  * worker `_finalize_bulk_parent_if_complete`:
        WHEN c.completed = 0 AND c.partial = 0 THEN 'FAILED'
  * backup-scheduler stale-sweep bulk backstop: same CASE.

STEP C in sweep_orphans was the odd one out:

        WHEN EXISTS (... s.status = 'COMPLETED') THEN 'COMPLETED'
        ELSE 'FAILED'

A job whose only snapshot settled PARTIAL (an honesty-gated, accepted
restore point — e.g. every USER_ARCHIVE run while ARCHIVE_MIN_COVERAGE_PCT
rides at 75 during the MS autoexpanded-archive regression, ITO-10146) has
zero COMPLETED rows, so the sweep collapsed it to FAILED. Because the sweep
ticks every ~30s and fires the moment the last snapshot turns terminal —
while the worker is still inside complete_snapshot's storage rollup — it
won the one-shot terminal flip against the worker's (correct) finalizer on
every single run: raw-SQL FAILED, no error_message, progress frozen at 95.
Observed live 2026-08-19: all three Gold dispatches' archive jobs FAILED
despite accepted PARTIAL snapshots, so no batch "completed fully".

This pins STEP C's success branch to count PARTIAL alongside COMPLETED.
"""
from __future__ import annotations

import pathlib
import re

_SRC = (
    pathlib.Path(__file__).resolve().parents[2] / "shared" / "reconciler.py"
).read_text()


def _step_c() -> str:
    i = _SRC.find("STEP C")
    assert i != -1, "STEP C section not found"
    j = _SRC.find("UPDATE jobs j", i)
    assert j != -1, "STEP C UPDATE jobs statement not found"
    return _SRC[j: _SRC.find("STEP D", j)]


def test_partial_snapshot_counts_as_job_success():
    sql = _step_c()
    case = sql[sql.find("SET status = CASE"): sql.find("completed_at")]
    assert case, "status CASE not found in STEP C"
    # The COMPLETED branch must match snapshots in COMPLETED OR PARTIAL —
    # either an IN list or two explicit comparisons.
    has_in_list = re.search(
        r"s\.status\s+IN\s*\(\s*'COMPLETED'[^)]*'PARTIAL'", case
    ) or re.search(
        r"s\.status\s+IN\s*\(\s*'PARTIAL'[^)]*'COMPLETED'", case
    )
    has_both_eq = ("= 'COMPLETED'" in case or "'COMPLETED'::snapshotstatus" in case) and (
        "= 'PARTIAL'" in case or "'PARTIAL'::snapshotstatus" in case
    )
    assert has_in_list or has_both_eq, (
        "STEP C's COMPLETED branch only matches status='COMPLETED' — a job "
        "whose only snapshot is an accepted PARTIAL collapses to FAILED, "
        "contradicting the worker finalizer and the stale-sweep backstop "
        "(FAILED only when every child failed)"
    )


def test_failed_branch_still_exists():
    sql = _step_c()
    assert "'FAILED'::jobstatus" in sql, (
        "STEP C must still FAIL jobs whose every snapshot failed"
    )
