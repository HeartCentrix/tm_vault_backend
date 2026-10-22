"""A pending delta cursor may only be promoted by the snapshot that
stashed it.

Bug (adversarial review, 2026-07-31): the pending slot is a single
resource-level key and BOTH promotion sites popped whatever was in it with
no record of WHICH snapshot put it there. So a zombie worker finalizing an
abandoned run — or a second concurrent run on the same resource — promotes
the *other* run's cursor, advancing past files that were enumerated but
never uploaded. The inline finalize made it worse by setting
status=COMPLETED with no IN_PROGRESS guard, so a snapshot the stale-sweep
had already FAILED could be re-flipped to COMPLETED and promote on the way
through.
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
_spec = importlib.util.spec_from_file_location("bw_pending_key", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_pending_key"] = _bw
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(_bw)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(
        f"backup-worker module failed to import: {exc}",
        allow_module_level=True,
    )


LINK_OLD = "https://graph.microsoft.com/v1.0/drives/d/root/delta?token=OLD"
LINK_NEW = "https://graph.microsoft.com/v1.0/drives/d/root/delta?token=NEW"
SNAP_A = "aaaaaaaa-0000-0000-0000-000000000001"
SNAP_B = "bbbbbbbb-0000-0000-0000-000000000002"

CAL = "AAMkAGI2-calendar-default"
TOK_OLD = "cal-token-OLD"
TOK_NEW = "cal-token-NEW"


# ─── the single link ──────────────────────────────────────────────────────

def test_stash_records_the_owning_snapshot():
    ed = _bw.stash_pending_delta_link(
        {"onedrive_delta_link": LINK_OLD}, LINK_NEW, snapshot_id=SNAP_A,
    )
    assert ed["onedrive_delta_link_pending"] == LINK_NEW
    assert ed["onedrive_delta_link_pending_snapshot"] == SNAP_A


def test_owner_promotes_its_own_pending_link():
    ed = _bw.stash_pending_delta_link(
        {"onedrive_delta_link": LINK_OLD}, LINK_NEW, snapshot_id=SNAP_A,
    )
    ed, promoted = _bw.promote_pending_delta_link(ed, snapshot_id=SNAP_A)
    assert promoted is True
    assert ed["onedrive_delta_link"] == LINK_NEW
    assert "onedrive_delta_link_pending" not in ed
    assert "onedrive_delta_link_pending_snapshot" not in ed


def test_a_different_snapshot_may_not_promote_it():
    """The zombie / concurrent-run case: snapshot B finalizes while A's
    un-uploaded cursor is pending. B must leave it alone."""
    ed = _bw.stash_pending_delta_link(
        {"onedrive_delta_link": LINK_OLD}, LINK_NEW, snapshot_id=SNAP_A,
    )
    ed, promoted = _bw.promote_pending_delta_link(ed, snapshot_id=SNAP_B)
    assert promoted is False
    assert ed["onedrive_delta_link"] == LINK_OLD
    # A's pending link survives — A may still finalize legitimately.
    assert ed["onedrive_delta_link_pending"] == LINK_NEW
    assert ed["onedrive_delta_link_pending_snapshot"] == SNAP_A


def test_an_unidentified_caller_may_not_promote_an_owned_link():
    ed = _bw.stash_pending_delta_link(
        {"onedrive_delta_link": LINK_OLD}, LINK_NEW, snapshot_id=SNAP_A,
    )
    ed, promoted = _bw.promote_pending_delta_link(ed)
    assert promoted is False
    assert ed["onedrive_delta_link"] == LINK_OLD
    assert ed["onedrive_delta_link_pending"] == LINK_NEW


def test_legacy_unowned_pending_still_promotes():
    """Rows stashed before this fix carry no owner. Refusing them outright
    would stall the cursor; the first stash after deploy records an owner,
    so the window is one run."""
    ed, promoted = _bw.promote_pending_delta_link(
        {"onedrive_delta_link": LINK_OLD,
         "onedrive_delta_link_pending": LINK_NEW},
        snapshot_id=SNAP_A,
    )
    assert promoted is True
    assert ed["onedrive_delta_link"] == LINK_NEW


# ─── the cursor maps ──────────────────────────────────────────────────────

def test_map_stash_records_the_owning_snapshot():
    ed = _bw.stash_pending_cursor_map(
        {"calendar_delta_tokens": {CAL: TOK_OLD}},
        pending_key="calendar_delta_tokens_pending",
        values={CAL: TOK_NEW},
        snapshot_id=SNAP_A,
    )
    assert ed["calendar_delta_tokens_pending_snapshot"] == SNAP_A


def test_map_owner_promotes_its_own_pending():
    ed = _bw.stash_pending_cursor_map(
        {"calendar_delta_tokens": {CAL: TOK_OLD}},
        pending_key="calendar_delta_tokens_pending",
        values={CAL: TOK_NEW},
        snapshot_id=SNAP_A,
    )
    ed, promoted = _bw.promote_pending_cursor_map(
        ed,
        pending_key="calendar_delta_tokens_pending",
        active_key="calendar_delta_tokens",
        snapshot_id=SNAP_A,
    )
    assert promoted is True
    assert ed["calendar_delta_tokens"] == {CAL: TOK_NEW}
    assert "calendar_delta_tokens_pending_snapshot" not in ed


def test_map_other_snapshot_may_not_promote():
    ed = _bw.stash_pending_cursor_map(
        {"calendar_delta_tokens": {CAL: TOK_OLD}},
        pending_key="calendar_delta_tokens_pending",
        values={CAL: TOK_NEW},
        snapshot_id=SNAP_A,
    )
    ed, promoted = _bw.promote_pending_cursor_map(
        ed,
        pending_key="calendar_delta_tokens_pending",
        active_key="calendar_delta_tokens",
        snapshot_id=SNAP_B,
    )
    assert promoted is False
    assert ed["calendar_delta_tokens"] == {CAL: TOK_OLD}
    assert ed["calendar_delta_tokens_pending"] == {CAL: TOK_NEW}


def test_dispatcher_passes_the_snapshot_id_through():
    ed = _bw.stash_pending_cursor_map(
        {"contacts_deltas": {CAL: TOK_OLD}},
        pending_key="contacts_deltas_pending",
        values={CAL: TOK_NEW},
        snapshot_id=SNAP_A,
    )
    _, promoted_wrong = _bw.promote_pending_cursors_for_type(
        ed, _bw.ResourceType.USER_CONTACTS, snapshot_id=SNAP_B,
    )
    assert promoted_wrong is False
    ed_ok, promoted_ok = _bw.promote_pending_cursors_for_type(
        ed, _bw.ResourceType.USER_CONTACTS, snapshot_id=SNAP_A,
    )
    assert promoted_ok is True
    assert ed_ok["contacts_deltas"] == {CAL: TOK_NEW}


def test_dispatcher_routes_onedrive_snapshot_id():
    ed = _bw.stash_pending_delta_link(
        {"onedrive_delta_link": LINK_OLD}, LINK_NEW, snapshot_id=SNAP_A,
    )
    _, promoted_wrong = _bw.promote_pending_cursors_for_type(
        ed, _bw.ResourceType.USER_ONEDRIVE, snapshot_id=SNAP_B,
    )
    assert promoted_wrong is False
    _, promoted_ok = _bw.promote_pending_cursors_for_type(
        ed, _bw.ResourceType.USER_ONEDRIVE, snapshot_id=SNAP_A,
    )
    assert promoted_ok is True


# ─── the inline finalize's status transition ──────────────────────────────

def test_only_an_in_progress_snapshot_may_be_finalized():
    assert _bw.may_finalize_snapshot_status(_bw.SnapshotStatus.IN_PROGRESS) is True


def test_a_terminal_snapshot_may_not_be_reflipped():
    for st in (
        _bw.SnapshotStatus.COMPLETED,
        _bw.SnapshotStatus.FAILED,
        _bw.SnapshotStatus.PARTIAL,
        _bw.SnapshotStatus.PENDING_DELETION,
    ):
        assert _bw.may_finalize_snapshot_status(st) is False, st


def test_missing_status_is_not_finalizable():
    assert _bw.may_finalize_snapshot_status(None) is False
