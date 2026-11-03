"""USER_CALENDAR / USER_CONTACTS cursors must not advance before persist.

Bug (adversarial review, 2026-07-31): this is the pre-31dc025 OneDrive
lost-baseline pattern, never ported to these two workloads.
``calendar_delta_tokens`` was merged onto the resource INSIDE _fetch_for —
before _backup_one's persist step and before any finalize — and
``contacts_deltas`` did the same. A first run that enumerates 25k events
and then dies strands those events permanently: the cursor says "you
already have everything up to here", so every later run is a tiny
incremental on top of a baseline that never landed.

Fix mirrors OneDrive: enumeration stashes into a *_pending slot; the
active map is only merged forward when the owning snapshot reaches
COMPLETED.
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
_spec = importlib.util.spec_from_file_location("bw_cal_cursor", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_cal_cursor"] = _bw
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(_bw)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(
        f"backup-worker module failed to import: {exc}",
        allow_module_level=True,
    )


CAL_A = "AAMkAGI2-calendar-default"
CAL_B = "AAMkAGI2-calendar-shared"
TOK_OLD = "https://graph.microsoft.com/v1.0/me/calendarView/delta?$deltatoken=OLD"
TOK_NEW = "https://graph.microsoft.com/v1.0/me/calendarView/delta?$deltatoken=NEW"

CONTACTS_FOLDER = "AAMkAGI2-contacts-default"


# ─── stash never touches the active map ───────────────────────────────────

def test_stash_leaves_active_calendar_map_untouched():
    ed = _bw.stash_pending_cursor_map(
        {"calendar_delta_tokens": {CAL_A: TOK_OLD}, "drive_id": "d"},
        pending_key="calendar_delta_tokens_pending",
        values={CAL_A: TOK_NEW},
    )
    assert ed["calendar_delta_tokens"] == {CAL_A: TOK_OLD}
    assert ed["calendar_delta_tokens_pending"] == {CAL_A: TOK_NEW}
    assert ed["drive_id"] == "d"


def test_stash_tolerates_missing_extra_data():
    ed = _bw.stash_pending_cursor_map(
        None,
        pending_key="contacts_deltas_pending",
        values={CONTACTS_FOLDER: TOK_NEW},
    )
    assert ed["contacts_deltas_pending"] == {CONTACTS_FOLDER: TOK_NEW}
    assert "contacts_deltas" not in ed


def test_stash_replaces_a_previous_runs_pending_slot():
    """A prior failed run's pending tokens must not survive into this
    run's promotion — they point past events that run never persisted."""
    ed = _bw.stash_pending_cursor_map(
        {"calendar_delta_tokens_pending": {CAL_B: TOK_OLD}},
        pending_key="calendar_delta_tokens_pending",
        values={CAL_A: TOK_NEW},
    )
    assert ed["calendar_delta_tokens_pending"] == {CAL_A: TOK_NEW}


def test_stash_of_nothing_is_a_noop():
    ed = _bw.stash_pending_cursor_map(
        {"calendar_delta_tokens": {CAL_A: TOK_OLD}},
        pending_key="calendar_delta_tokens_pending",
        values={},
    )
    assert "calendar_delta_tokens_pending" not in ed
    assert ed["calendar_delta_tokens"] == {CAL_A: TOK_OLD}


# ─── promotion merges forward, only on COMPLETED ──────────────────────────

def test_promote_merges_pending_into_active():
    ed, promoted = _bw.promote_pending_cursor_map(
        {
            "calendar_delta_tokens": {CAL_A: TOK_OLD, CAL_B: TOK_OLD},
            "calendar_delta_tokens_pending": {CAL_B: TOK_NEW},
        },
        pending_key="calendar_delta_tokens_pending",
        active_key="calendar_delta_tokens",
    )
    assert promoted is True
    assert ed["calendar_delta_tokens"] == {CAL_A: TOK_OLD, CAL_B: TOK_NEW}
    assert "calendar_delta_tokens_pending" not in ed


def test_promote_with_no_pending_is_a_noop():
    ed, promoted = _bw.promote_pending_cursor_map(
        {"calendar_delta_tokens": {CAL_A: TOK_OLD}},
        pending_key="calendar_delta_tokens_pending",
        active_key="calendar_delta_tokens",
    )
    assert promoted is False
    assert ed["calendar_delta_tokens"] == {CAL_A: TOK_OLD}


def test_failed_run_strands_nothing_the_next_run_reenumerates():
    """The whole point: enumeration stashed 25k events' token, the run
    died, no promotion happened → the active cursor is still the last
    SUCCESSFUL position, so the next run re-enumerates them."""
    ed = _bw.stash_pending_cursor_map(
        {"calendar_delta_tokens": {CAL_A: TOK_OLD}},
        pending_key="calendar_delta_tokens_pending",
        values={CAL_A: TOK_NEW},
    )
    # no promote — the snapshot went FAILED
    assert ed["calendar_delta_tokens"][CAL_A] == TOK_OLD


# ─── the finalize sites dispatch by resource type ─────────────────────────

def test_finalize_promotes_calendar_tokens():
    ed, promoted = _bw.promote_pending_cursors_for_type(
        {
            "calendar_delta_tokens": {CAL_A: TOK_OLD},
            "calendar_delta_tokens_pending": {CAL_A: TOK_NEW},
        },
        _bw.ResourceType.USER_CALENDAR,
    )
    assert promoted is True
    assert ed["calendar_delta_tokens"] == {CAL_A: TOK_NEW}


def test_finalize_promotes_contacts_deltas():
    ed, promoted = _bw.promote_pending_cursors_for_type(
        {
            "contacts_deltas": {CONTACTS_FOLDER: TOK_OLD},
            "contacts_deltas_pending": {CONTACTS_FOLDER: TOK_NEW},
        },
        _bw.ResourceType.USER_CONTACTS,
    )
    assert promoted is True
    assert ed["contacts_deltas"] == {CONTACTS_FOLDER: TOK_NEW}


def test_finalize_still_promotes_the_onedrive_link():
    ed, promoted = _bw.promote_pending_cursors_for_type(
        {
            "onedrive_delta_link": TOK_OLD,
            "onedrive_delta_link_pending": TOK_NEW,
        },
        _bw.ResourceType.USER_ONEDRIVE,
    )
    assert promoted is True
    assert ed["onedrive_delta_link"] == TOK_NEW
    assert "onedrive_delta_link_pending" not in ed


def test_finalize_is_a_noop_for_types_without_a_pending_cursor():
    ed, promoted = _bw.promote_pending_cursors_for_type(
        {"mail_delta_tokens_by_folder": {"inbox": TOK_OLD}},
        _bw.ResourceType.USER_MAIL,
    )
    assert promoted is False
    assert ed == {"mail_delta_tokens_by_folder": {"inbox": TOK_OLD}}


def test_finalize_tolerates_missing_extra_data():
    ed, promoted = _bw.promote_pending_cursors_for_type(
        None, _bw.ResourceType.USER_CALENDAR,
    )
    assert promoted is False
    assert ed == {}
