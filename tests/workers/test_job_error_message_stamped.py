"""ITO-10146 masking factor: ``jobs.error_message`` was ALWAYS NULL.

Every archive failure since 2026-08-11 recorded its reason only in
``snapshots.extra_data->>'error'``; the job row the operator actually looks
at said nothing (bulk parents aggregate via _finalize_bulk_parent_if_complete,
which never wrote it, and the single-resource path only stuffed
``result['error']``). The handler-failure path now stamps error_message in
BOTH shapes, appending per-resource-prefixed lines capped at 2000 chars so
one noisy child cannot erase a sibling's reason.

Source pin (repo convention where no DB fixture exists).
"""
from __future__ import annotations

import pathlib
import re

_SRC = (
    pathlib.Path(__file__).resolve().parents[2]
    / "workers" / "backup-worker" / "main.py"
).read_text()


def _failure_block() -> str:
    idx = _SRC.find("Bulk-parent awareness: under the fan-out path each per-")
    assert idx != -1, "handler-failure job-flip block not found"
    return _SRC[idx: idx + 4000]


def test_handler_failure_stamps_job_error_message():
    blk = _failure_block()
    assert "UPDATE jobs SET error_message" in blk, (
        "the handler-failure path must stamp jobs.error_message — "
        "it was always NULL and the real error hid in snapshot extra_data"
    )
    # Appended (prior reasons preserved) and capped.
    assert "COALESCE(NULLIF(error_message, '') || '; ', '')" in blk
    assert re.search(r"LEFT\(", blk)
    assert ", 2000)" in blk
    # Per-resource prefix so a bulk parent's message says WHICH child died.
    assert "resource.display_name or resource.id" in blk


def test_stamp_happens_before_both_terminal_flips():
    blk = _failure_block()
    stamp = blk.find("UPDATE jobs SET error_message")
    bulk = blk.find("_finalize_bulk_parent_if_complete")
    single = blk.find("update_job_status")
    assert stamp != -1 and bulk != -1 and single != -1
    assert stamp < bulk and stamp < single, (
        "error_message must land before the terminal flip so a FAILED row "
        "is never visible without its reason"
    )


def test_stamp_failure_cannot_cost_the_terminal_flip():
    """Review finding: the stamp is best-effort telemetry — a transient DB
    blip on that one extra round-trip must not skip the finalizer (for a
    bulk parent, a skipped finalizer strands the parent RUNNING forever)."""
    blk = _failure_block()
    stamp = blk.find("UPDATE jobs SET error_message")
    own_try = blk.rfind("try:", 0, stamp)
    own_except = blk.find("except Exception as _stamp_exc", stamp)
    bulk = blk.find("_finalize_bulk_parent_if_complete")
    assert own_try != -1 and own_except != -1
    assert own_except < bulk, (
        "the stamp's own except must close before the terminal flip runs"
    )
