"""The detached interleave's per-message failure retention must be bounded.

Finding (preflight IMPORTANT 11, 2026-07-31): `_hc_failed_msgs` /
`_att_failed_msgs` were plain `List[Tuple[str, Dict]]` declared ONCE for the
entire USER_CHATS partition (workers/backup-worker/main.py:6617-6618),
appended per failed message and cleared only inside the
`if _hc_retry or _att_retry:` branch. Each entry pins the raw Graph
chatMessage dict, and the detached finalizer's closure keeps that list alive
after `_backup_one` has returned and released `items_data` — so on the
worst-case user (~713k messages) a tenant-wide hostedContent or attachment
failure retains ~713k message dicts (~1.4-2.1 GB) on the standard 8Gi
backup-worker tier that is already OOMKilling every ~90 minutes, and it
overlaps the NEXT user's drain.

Two properties are pinned here:

  1. Retention is capped (`CHAT_INTERLEAVE_MAX_RETAINED_FAILURES`), so peak
     memory is a constant, not a function of mailbox size.
  2. Dropping a failure must never buy a quieter status. Anything the cap
     refused to retain was never retried, so it is lost content and
     hc_drain_status must be FAILED — loudly, naming the knob.
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import sys

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "test_secret_for_unit_tests_only_x")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")
os.environ.setdefault("AUDIT_SERVICE_URL", "http://localhost:8000")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "workers" / "backup-worker" / "main.py"
_spec = importlib.util.spec_from_file_location("bw_failure_bound", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_failure_bound"] = _bw
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(_bw)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(
        f"backup-worker module failed to import: {exc}",
        allow_module_level=True,
    )

CHAT_A = "19:aaaaaaaa@thread.v2"


def _msg(i: int) -> dict:
    return {"id": f"m{i}", "body": {"content": "x" * 32}}


# ─── 1. the cap itself ─────────────────────────────────────────────────────

def test_retention_cap_default_is_bounded_and_overridable():
    """Default must be a finite, modest constant; env override wins; garbage
    falls back to the default rather than crashing the drain."""
    assert 0 < _bw.interleave_failure_retention_cap({}) <= 20000
    assert _bw.interleave_failure_retention_cap(
        {"CHAT_INTERLEAVE_MAX_RETAINED_FAILURES": "17"},
    ) == 17
    assert _bw.interleave_failure_retention_cap(
        {"CHAT_INTERLEAVE_MAX_RETAINED_FAILURES": "not-a-number"},
    ) == _bw.interleave_failure_retention_cap({})
    # A cap of 0 or negative would silently retain nothing and (worse) make
    # every failure invisible to the retry pass; clamp to at least 1.
    assert _bw.interleave_failure_retention_cap(
        {"CHAT_INTERLEAVE_MAX_RETAINED_FAILURES": "0"},
    ) >= 1


def test_failure_log_retains_up_to_cap_and_counts_the_rest():
    log = _bw.BoundedInterleaveFailureLog(5, label="hc")
    for i in range(12):
        log.append((CHAT_A, _msg(i)))

    assert len(log) == 5, "retention must stop at the cap"
    assert log.overflow == 7, "dropped failures must still be counted"
    assert [m["id"] for _cid, m in list(log)] == ["m0", "m1", "m2", "m3", "m4"]


def test_failure_log_clear_keeps_overflow_evidence():
    """The finalizer clears the lists before its one retry pass. Clearing
    must not erase the record of failures the cap refused to retain — those
    were never retried and are still lost content."""
    log = _bw.BoundedInterleaveFailureLog(2, label="att")
    for i in range(6):
        log.append((CHAT_A, _msg(i)))
    log.clear()

    assert len(log) == 0
    assert log.overflow == 4


def test_failure_log_is_drop_in_for_the_old_list_shape():
    """Call sites do `log.append((cid, msg))`, `list(log)` and `log.clear()`
    — the same shape the plain list had, so the wiring diff stays minimal."""
    log = _bw.BoundedInterleaveFailureLog(10)
    log.append((CHAT_A, _msg(1)))
    grouped = _bw.group_failed_interleaves(list(log))
    assert grouped == {CHAT_A: [_msg(1)]}


# ─── 2. dropping a failure must not buy a quieter status ───────────────────

def test_status_from_logs_is_complete_only_when_nothing_was_lost():
    status, reason = _bw.interleave_status_from_logs(
        gather_errors=0, hc_left={}, att_left={}, hc_overflow=0, att_overflow=0,
    )
    assert status == "COMPLETE"
    assert reason is None


def test_overflow_alone_forces_failed_and_names_the_knob():
    """Cap-dropped failures are content this snapshot does not have. A clean
    retry pass over the RETAINED entries must not report COMPLETE."""
    status, reason = _bw.interleave_status_from_logs(
        gather_errors=0, hc_left={}, att_left={}, hc_overflow=9, att_overflow=3,
    )
    assert status == "FAILED"
    assert "9" in reason and "3" in reason
    assert "CHAT_INTERLEAVE_MAX_RETAINED_FAILURES" in reason, (
        "the reason operators read must name the knob that dropped them"
    )


def test_status_from_logs_adds_overflow_to_the_retained_counts():
    status, reason = _bw.interleave_status_from_logs(
        gather_errors=0,
        hc_left={CHAT_A: [_msg(1), _msg(2)]},
        att_left={},
        hc_overflow=5,
        att_overflow=0,
    )
    assert status == "FAILED"
    assert "7 message(s) lost hostedContent" in reason
