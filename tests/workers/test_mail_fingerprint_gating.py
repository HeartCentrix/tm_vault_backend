"""USER_MAIL folder fingerprints may only advance for folders that both
drained AND persisted.

Bug (adversarial review, 2026-07-31): `_drained_fids` was computed as
"every folder this run planned to drain, minus the ones skipped by
fingerprint". Nothing removed folders whose

  * drain threw mid-pagination (the except arm swallows and keeps the
    partial page set),
  * page-persist failed (any_persist_failed=True), or
  * never ran at all because the 12h asyncio.wait_for fired.

Their fingerprints were advanced anyway, so skip-by-fingerprint suppressed
them for up to USER_MAIL_FULL_RESCAN_DAYS (default 3) while the snapshot
finalized COMPLETED — mail silently missing for days.
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import sys

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "workers" / "backup-worker" / "main.py"
_spec = importlib.util.spec_from_file_location("bw_mail_fp_gate", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_mail_fp_gate"] = _bw
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(_bw)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(
        f"backup-worker module failed to import: {exc}",
        allow_module_level=True,
    )


INBOX = "inbox-folder-id-aaaaaaaa"
SENT = "sent-folder-id-bbbbbbbb"
DRAFTS = "drafts-folder-id-cccccccc"
ARCHIVE = "archive-folder-id-dddddddd"


# ─── which fingerprints may advance ───────────────────────────────────────

def test_only_drained_and_persisted_folders_advance():
    advance, missing = _bw.evaluate_mail_folder_outcomes(
        planned_fids=[INBOX, SENT, DRAFTS],
        fp_skipped_fids=[],
        drained_ok_fids=[INBOX],
    )
    assert advance == {INBOX}
    assert missing == {SENT, DRAFTS}


def test_folder_whose_drain_threw_midpages_does_not_advance():
    """The except arm keeps the partial page set and returns normally —
    the folder must still be treated as not-drained."""
    advance, missing = _bw.evaluate_mail_folder_outcomes(
        planned_fids=[INBOX, SENT],
        fp_skipped_fids=[],
        drained_ok_fids=[SENT],
    )
    assert INBOX not in advance
    assert INBOX in missing


def test_folders_never_run_after_timeout_do_not_advance():
    """40 of 200 folders finished before the wait_for fired."""
    planned = [f"folder-{i:03d}" for i in range(200)]
    advance, missing = _bw.evaluate_mail_folder_outcomes(
        planned_fids=planned,
        fp_skipped_fids=[],
        drained_ok_fids=planned[:40],
    )
    assert advance == set(planned[:40])
    assert len(missing) == 160


def test_fp_skipped_folders_are_neither_advanced_nor_missing():
    """Skip-by-fingerprint means "unchanged since last time" — its
    fingerprint is already current and nothing is missing."""
    advance, missing = _bw.evaluate_mail_folder_outcomes(
        planned_fids=[INBOX, SENT, ARCHIVE],
        fp_skipped_fids=[ARCHIVE],
        drained_ok_fids=[INBOX, SENT],
    )
    assert advance == {INBOX, SENT}
    assert missing == set()


def test_clean_run_advances_everything_and_reports_nothing_missing():
    advance, missing = _bw.evaluate_mail_folder_outcomes(
        planned_fids=[INBOX, SENT, DRAFTS],
        fp_skipped_fids=[],
        drained_ok_fids=[INBOX, SENT, DRAFTS],
    )
    assert advance == {INBOX, SENT, DRAFTS}
    assert missing == set()


def test_root_placeholder_never_gets_a_fingerprint_but_counts_as_missing():
    """An empty folder tree drains via a single [None] placeholder
    ("/users/{id}/messages" root). There is no folder id to fingerprint,
    but a failed root drain still has to be visible."""
    advance, missing = _bw.evaluate_mail_folder_outcomes(
        planned_fids=[None],
        fp_skipped_fids=[],
        drained_ok_fids=[],
    )
    assert advance == set()
    assert missing == {"__all__"}


def test_root_placeholder_success_advances_no_fingerprint():
    advance, missing = _bw.evaluate_mail_folder_outcomes(
        planned_fids=[None],
        fp_skipped_fids=[],
        drained_ok_fids=[None],
    )
    assert advance == set()
    assert missing == set()


def test_no_folders_planned_is_clean():
    advance, missing = _bw.evaluate_mail_folder_outcomes(
        planned_fids=[], fp_skipped_fids=[], drained_ok_fids=[],
    )
    assert advance == set()
    assert missing == set()


# ─── the snapshot must say so ─────────────────────────────────────────────

def test_missing_folders_with_items_reports_partial():
    status, reason = _bw.evaluate_drain_honesty(
        planned=40, drained_ok=39, failed=1, items_persisted=925,
        unit="folder",
    )
    assert status is _bw.SnapshotStatus.PARTIAL
    assert reason and "folder" in reason


def test_every_folder_failed_with_zero_items_refuses_completed():
    status, reason = _bw.evaluate_drain_honesty(
        planned=40, drained_ok=0, failed=40, items_persisted=0,
        unit="folder",
    )
    assert status is _bw.SnapshotStatus.FAILED
    assert reason


def test_clean_mail_run_is_not_degraded():
    status, reason = _bw.evaluate_drain_honesty(
        planned=40, drained_ok=40, failed=0, items_persisted=925,
        unit="folder",
    )
    assert status is None
    assert reason is None
