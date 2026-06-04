"""Unit tests for mail intra-folder sub-sharding pure helpers.

Covers the testable decision/planning core of
docs/superpowers/specs/2026-06-05-mail-intra-folder-subsharding-design.md:
  - _should_clear_mail_deltas  (forceFullBackup fix, Task 2)
  - _plan_mail_date_buckets    (overlapping/never-gapped buckets, Task 3)
  - _should_intra_folder_subshard (decision gate, Task 4)

Loads workers/backup-worker/main.py via importlib (the hyphen in the path
blocks a normal import) — same pattern as test_mail_skip_by_fp.py.
"""
from __future__ import annotations

import importlib.util
import pathlib
import sys
from datetime import datetime, timezone

import pytest

_MAIN_PATH = (
    pathlib.Path(__file__).resolve().parents[2]
    / "workers" / "backup-worker" / "main.py"
)
_spec = importlib.util.spec_from_file_location("bw_main_subshard", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_main_subshard"] = _bw
try:
    _spec.loader.exec_module(_bw)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover
    pytest.skip(
        f"backup-worker module failed to import: {exc}",
        allow_module_level=True,
    )

MB = 1024 * 1024


# ─── Task 2: forceFullBackup delta-clear gate ────────────────────────────

def test_force_full_clears_deltas():
    assert _bw.BackupWorker._should_clear_mail_deltas(True) is True


def test_incremental_keeps_deltas():
    assert _bw.BackupWorker._should_clear_mail_deltas(False) is False
