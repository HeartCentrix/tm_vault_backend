"""Regression pins for the 2026-08-25 reaper observability fixes.

Incident (8/24-8/25 pooler-starvation storms):

  * ``single_resource_job_reaper`` finalized archive jobs FAILED with
    ``error_message`` NULL (jobs fef2894a / 2d0da6b8) — a reaped failure was
    indistinguishable from "no error recorded".  Fix: on the FAILED arm the
    reaper copies the newest snapshot's ``extra_data->>'failure_reason'`` into
    ``jobs.error_message``.

  * ``missing_snapshot_reaper`` stamped the assertion "snapshot rows missing
    (deleted after dispatch)" — but batch b552bb16's rows were never created
    at all (its per-resource dispatch message dead-lettered to
    backup.urgent.dlq during a ConnectionDoesNotExistError storm before any
    worker touched it).  Fix: honest wording covering both shapes, plus a
    ``missing_resource_ids`` array in ``result`` so uncovered resources can be
    re-driven instead of silently skipped until the next scheduled window.

Source-inspection pins (the statements are inline text() SQL literals;
importing the module drags in the full scheduler runtime).
"""
from __future__ import annotations

import pathlib
import re

_MAIN = (
    pathlib.Path(__file__).resolve().parents[2]
    / "services" / "backup-scheduler" / "main.py"
).read_text()


def _block(anchor: str, span: int = 4000) -> str:
    idx = _MAIN.index(anchor)
    return _MAIN[max(0, idx - span): idx + span]


class TestSingleResourceJobReaperErrorStamp:
    def test_failed_arm_stamps_error_message(self):
        block = _block("single_resource_job_reaper")
        assert re.search(
            r"error_message\s*=\s*CASE\s+WHEN\s+a\.n_ok\s*>\s*0", block
        ), "FAILED finalizations must stamp error_message (COMPLETED arm keeps NULL)"

    def test_error_copies_snapshot_failure_reason(self):
        block = _block("single_resource_job_reaper")
        assert "failure_reason" in block, (
            "the stamped error must surface the snapshot's failure_reason, "
            "not a generic reaper string"
        )

    def test_never_overwrites_existing_error(self):
        block = _block("single_resource_job_reaper")
        assert re.search(r"COALESCE\(\s*j\.error_message", block), (
            "an error a worker already wrote must win over the reaper's copy"
        )


class TestMissingSnapshotReaperHonesty:
    def test_old_assertive_wording_gone(self):
        assert "snapshot rows missing (deleted after dispatch)" not in _MAIN, (
            "absence-of-row cannot prove deletion — the message must also "
            "cover dispatch messages dead-lettered before any worker ran"
        )

    def test_new_wording_covers_both_shapes(self):
        assert "lost/dead-lettered" in _MAIN
        assert "deleted after dispatch" in _MAIN  # still mentioned as a possibility

    def test_result_exposes_missing_resource_ids(self):
        block = _block("missing_snapshot_reaper")
        assert "'missing_resource_ids'" in block, (
            "result JSON must name the uncovered resources so they can be re-driven"
        )
        assert re.search(
            r"unnest\(j\.batch_resource_ids\)\s+AS\s+rid", block
        ), "missing set must be derived from batch_resource_ids minus existing rows"
