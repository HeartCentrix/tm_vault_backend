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


# ─── Task 3: date-bucket planner (invariant #3) ──────────────────────────

def _plan(**k):
    return _bw.BackupWorker._plan_mail_date_buckets(**k)


T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
T1 = datetime(2026, 6, 1, tzinfo=timezone.utc)


def test_below_target_is_single_bucket():
    b = _plan(window_start=T0, window_end=T1, total_bytes=10 * MB,
              target_bytes=64 * MB, max_subshards=8, overlap_seconds=1)
    assert len(b) == 1 and b[0][0] == T0 and b[0][1] == T1


def test_count_scales_with_size_capped():
    # ceil(2GB/64MB)=32, capped at max_subshards=8
    b = _plan(window_start=T0, window_end=T1, total_bytes=2048 * MB,
              target_bytes=64 * MB, max_subshards=8, overlap_seconds=1)
    assert len(b) == 8


def test_buckets_cover_window_with_no_gap():
    b = _plan(window_start=T0, window_end=T1, total_bytes=512 * MB,
              target_bytes=64 * MB, max_subshards=8, overlap_seconds=1)
    assert b[0][0] == T0 and b[-1][1] == T1
    for i in range(1, len(b)):
        # next bucket starts at-or-before previous end → overlap, never gap
        assert b[i][0] <= b[i - 1][1]


def test_overlap_epsilon_applied():
    b = _plan(window_start=T0, window_end=T1, total_bytes=512 * MB,
              target_bytes=64 * MB, max_subshards=8, overlap_seconds=2)
    assert len(b) > 1
    assert (b[0][1] - b[1][0]).total_seconds() >= 2


def test_zero_or_negative_window_is_single_bucket():
    b = _plan(window_start=T1, window_end=T0, total_bytes=10 ** 12,
              target_bytes=64 * MB, max_subshards=8, overlap_seconds=1)
    assert len(b) == 1


def test_zero_bytes_is_single_bucket():
    b = _plan(window_start=T0, window_end=T1, total_bytes=0,
              target_bytes=64 * MB, max_subshards=8, overlap_seconds=1)
    assert len(b) == 1


# ─── Task 4: sub-shard decision gate ─────────────────────────────────────

def _dec(**k):
    return _bw.BackupWorker._should_intra_folder_subshard(**k)


def test_gate_disabled_never_subshards():
    assert _dec(enabled=False, size_bytes=999 * MB, prev_size_bytes=0,
                saved_token=None, min_bytes=128 * MB, jump_bytes=128 * MB) is False


def test_gate_full_big_folder_subshards():
    assert _dec(enabled=True, size_bytes=200 * MB, prev_size_bytes=0,
                saved_token=None, min_bytes=128 * MB, jump_bytes=128 * MB) is True


def test_gate_full_small_folder_no_subshard():
    assert _dec(enabled=True, size_bytes=50 * MB, prev_size_bytes=0,
                saved_token=None, min_bytes=128 * MB, jump_bytes=128 * MB) is False


def test_gate_incremental_small_jump_no_subshard():
    assert _dec(enabled=True, size_bytes=210 * MB, prev_size_bytes=200 * MB,
                saved_token="https://delta", min_bytes=128 * MB,
                jump_bytes=128 * MB) is False


def test_gate_incremental_big_jump_subshards():
    assert _dec(enabled=True, size_bytes=400 * MB, prev_size_bytes=200 * MB,
                saved_token="https://delta", min_bytes=128 * MB,
                jump_bytes=128 * MB) is True
