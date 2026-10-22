"""Unit tests for `_compute_mail_skip_by_fp` — the USER_MAIL
skip-by-fingerprint decision function.

The function is the structural fix for the production incident on
2026-05-14 where Amit Mishra's mailbox (40 folders, partition fanout
into 2 shards) returned a snapshot with only Inbox (925 messages) and
zero rows from the other 39 folders. The second-finishing shard read
fingerprints written by the first shard and skipped its entire
allowlist. See
docs/superpowers/specs/2026-05-15-mail-folder-fingerprint-table-design.md.

These tests pin the decision behavior so any future refactor can't
silently re-introduce the race.
"""
from __future__ import annotations

import importlib.util
import pathlib
import sys
from datetime import datetime, timedelta

import pytest


_MAIN_PATH = (
    pathlib.Path(__file__).resolve().parents[2]
    / "workers" / "backup-worker" / "main.py"
)
_spec = importlib.util.spec_from_file_location("bw_main_under_test", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_main_under_test"] = _bw
try:
    _spec.loader.exec_module(_bw)  # type: ignore[union-attr]
except Exception as exc:
    pytest.skip(
        f"backup-worker module failed to import: {exc}",
        allow_module_level=True,
    )


_decide = _bw.BackupWorker._compute_mail_skip_by_fp
_RESCAN_SECS = 3 * 86400  # USER_MAIL_FULL_RESCAN_DAYS default


# ── Folder ids (long enough that fid[:8] in production logs disambiguates).
INBOX = "inbox-folder-id-aaaaaaaa"
SENT = "sent-folder-id-bbbbbbbb"
DRAFTS = "drafts-folder-id-cccccccc"
DELETED = "deleted-folder-id-dddddddd"


# ─── First run / no state ────────────────────────────────────────────────


def test_first_run_drains_every_folder():
    """No fp_table rows, no legacy dict, no baseline → everything drains."""
    drain, skipped, fallback = _decide(
        all_folder_ids=[INBOX, SENT, DRAFTS],
        fp_table={},
        legacy_fps={},
        legacy_baseline_dt=None,
        fingerprints_now={
            INBOX: "100|10|1000",
            SENT: "50|0|500",
            DRAFTS: "5|0|50",
        },
        rescan_seconds=_RESCAN_SECS,
    )
    assert set(drain) == {INBOX, SENT, DRAFTS}
    assert skipped == []
    assert fallback == 0


def test_leading_none_folder_id_passes_through():
    """When the folder tree is empty, drain path uses a single [None]
    placeholder to mean 'scan /users/{id}/messages root'. The decision
    must preserve it."""
    drain, skipped, _ = _decide(
        all_folder_ids=[None],
        fp_table={},
        legacy_fps={},
        legacy_baseline_dt=None,
        fingerprints_now={},
        rescan_seconds=_RESCAN_SECS,
    )
    assert drain == [None]
    assert skipped == []


# ─── Per-folder skip / drain via new table ───────────────────────────────


def test_unchanged_folder_with_fresh_baseline_is_skipped():
    now = datetime.utcnow()
    drain, skipped, _ = _decide(
        all_folder_ids=[INBOX],
        fp_table={
            INBOX: {"fp": "100|10|1000", "baseline_at": now - timedelta(hours=1)},
        },
        legacy_fps={},
        legacy_baseline_dt=None,
        fingerprints_now={INBOX: "100|10|1000"},
        rescan_seconds=_RESCAN_SECS,
        now_dt=now,
    )
    assert drain == []
    assert skipped == [INBOX]


def test_changed_fingerprint_drains_even_within_rescan_window():
    now = datetime.utcnow()
    drain, skipped, _ = _decide(
        all_folder_ids=[INBOX],
        fp_table={
            INBOX: {"fp": "100|10|1000", "baseline_at": now - timedelta(hours=1)},
        },
        legacy_fps={},
        legacy_baseline_dt=None,
        fingerprints_now={INBOX: "101|10|1100"},
        rescan_seconds=_RESCAN_SECS,
        now_dt=now,
    )
    assert drain == [INBOX]
    assert skipped == []


def test_stale_baseline_forces_drain_even_when_fingerprint_unchanged():
    """3-day full-rescan safety net catches silent Graph counter drift."""
    now = datetime.utcnow()
    drain, skipped, _ = _decide(
        all_folder_ids=[INBOX],
        fp_table={
            INBOX: {"fp": "100|10|1000", "baseline_at": now - timedelta(days=5)},
        },
        legacy_fps={},
        legacy_baseline_dt=None,
        fingerprints_now={INBOX: "100|10|1000"},
        rescan_seconds=_RESCAN_SECS,
        now_dt=now,
    )
    assert drain == [INBOX]
    assert skipped == []


# ─── Dual-read fallback (one-release transition) ─────────────────────────


def test_legacy_dict_fallback_when_table_row_missing():
    now = datetime.utcnow()
    drain, skipped, fallback = _decide(
        all_folder_ids=[INBOX],
        fp_table={},  # no row yet — pre-migration
        legacy_fps={INBOX: "100|10|1000"},
        legacy_baseline_dt=now - timedelta(hours=2),
        fingerprints_now={INBOX: "100|10|1000"},
        rescan_seconds=_RESCAN_SECS,
        now_dt=now,
    )
    # Skip honored via legacy fallback path.
    assert drain == []
    assert skipped == [INBOX]
    assert fallback == 1


def test_new_table_wins_over_legacy_dict_on_conflict():
    """When both the new row AND legacy entry exist, the row is authoritative."""
    now = datetime.utcnow()
    drain, skipped, fallback = _decide(
        all_folder_ids=[INBOX],
        fp_table={
            # Row says: this folder's fingerprint changed since last drain.
            INBOX: {"fp": "200|20|2000", "baseline_at": now - timedelta(hours=1)},
        },
        legacy_fps={INBOX: "100|10|1000"},  # stale value in legacy dict
        legacy_baseline_dt=now - timedelta(hours=1),
        fingerprints_now={INBOX: "100|10|1000"},  # matches LEGACY, not row
        rescan_seconds=_RESCAN_SECS,
        now_dt=now,
    )
    # If legacy were consulted, fp matches → skip. Row must win → drain.
    assert drain == [INBOX]
    assert skipped == []
    assert fallback == 0  # row hit, not legacy fallback


# ─── REGRESSION: cross-shard race that produced the Amit incident ────────


def test_regression_sibling_shard_writes_do_not_poison_skip():
    """Reproduces the 2026-05-14 incident.

    Shard 0 owns Inbox (1 folder), finishes first, persists ONLY
    Inbox's row to mail_folder_fingerprint. Shard 1 owns the other
    39 folders, starts ~2s later, reads `fp_table` — should see ONLY
    Inbox's row, NOT rows for its 39 folders.

    Pre-fix (whole-mailbox dict): shard 0's end-of-run wrote
    fingerprints for ALL 40 folders to `resources.metadata`, and the
    whole-mailbox `mail_folder_baseline_at` clock was fresh, so
    shard 1's skip-by-fp matched every fingerprint and skipped its
    allowlist — silently producing a snapshot with no non-Inbox rows.

    Post-fix (per-row table): shard 1's `fp_table` has no row for
    its 39 folders, so `force_full=True` for each, every folder
    enters `drain`. The race cannot recur.
    """
    now = datetime.utcnow()
    # Simulate shard 1 view: only Inbox got persisted by shard 0.
    shard1_fp_table = {
        INBOX: {"fp": "925|598|107732616", "baseline_at": now - timedelta(seconds=2)},
    }
    shard1_allowlist = [SENT, DRAFTS, DELETED]  # 3 of 39, all owned by shard 1
    fingerprints_now = {
        INBOX: "925|598|107732616",
        SENT: "131|71|7336993",
        DRAFTS: "58|0|14997527",
        DELETED: "5|4|540407",
    }
    drain, skipped, _ = _decide(
        all_folder_ids=shard1_allowlist,
        fp_table=shard1_fp_table,
        legacy_fps={},  # critical: legacy dict is NOT consulted when fp_table covers
        legacy_baseline_dt=None,
        fingerprints_now=fingerprints_now,
        rescan_seconds=_RESCAN_SECS,
        now_dt=now,
    )
    assert set(drain) == {SENT, DRAFTS, DELETED}, (
        "Shard 1 must drain every folder it owns — sibling shard 0's "
        "Inbox fingerprint write must not cause shard 1 to skip its allowlist."
    )
    assert skipped == []


def test_regression_legacy_dict_does_not_poison_shard_when_table_authoritative():
    """Belt-and-suspenders: even with a fully populated legacy dict
    matching today's fingerprints (left over from the broken
    whole-tree write), shard 1's folders must drain because the new
    table is authoritative and has no rows for them."""
    now = datetime.utcnow()
    fingerprints_now = {
        SENT: "131|71|7336993",
        DRAFTS: "58|0|14997527",
    }
    drain, skipped, fallback = _decide(
        all_folder_ids=[SENT, DRAFTS],
        # New table has rows for these folders, but with stale fingerprints
        # → must drain.
        fp_table={
            SENT: {"fp": "0|0|0", "baseline_at": now - timedelta(hours=1)},
            DRAFTS: {"fp": "0|0|0", "baseline_at": now - timedelta(hours=1)},
        },
        # Legacy dict has matching (poisonous) fingerprints — must be ignored.
        legacy_fps={
            SENT: "131|71|7336993",
            DRAFTS: "58|0|14997527",
        },
        legacy_baseline_dt=now - timedelta(seconds=2),
        fingerprints_now=fingerprints_now,
        rescan_seconds=_RESCAN_SECS,
        now_dt=now,
    )
    assert set(drain) == {SENT, DRAFTS}
    assert skipped == []
    assert fallback == 0


def test_folder_without_a_saved_cursor_is_never_fingerprint_skipped():
    """An evicted cursor (410, carry rewind, force-full) must force a
    re-walk even when the folder fingerprint is unchanged — otherwise the
    eviction is silently deferred a full rescan window."""
    now = datetime(2026, 9, 4, 12, 0, 0)
    fp_table = {
        INBOX: {"fp": "100|10|1000", "baseline_at": now - timedelta(minutes=5)},
        SENT: {"fp": "50|0|500", "baseline_at": now - timedelta(minutes=5)},
    }
    drain, skipped, _ = _decide(
        all_folder_ids=[INBOX, SENT],
        fp_table=fp_table,
        legacy_fps={},
        legacy_baseline_dt=None,
        fingerprints_now={INBOX: "100|10|1000", SENT: "50|0|500"},
        rescan_seconds=_RESCAN_SECS,
        now_dt=now,
        saved_tokens={SENT: "tok"},  # INBOX has no cursor
    )
    assert drain == [INBOX]
    assert skipped == [SENT]
