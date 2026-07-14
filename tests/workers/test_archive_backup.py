"""Pure incremental-core helpers for the Exchange ARCHIVE backup handler
(workers/backup-worker/archive_backup.py). No DB / storage / Graph — just the
changeKey state-advance logic (the incremental-correctness heart) and the
SnapshotItem row shape. Orchestration is tested by integration elsewhere.

workers/backup-worker has a hyphen, so it's put on sys.path and imported
directly (same pattern as test_mail_item_attachment.py)."""
from __future__ import annotations

import pathlib
import sys

_ROOT = pathlib.Path(__file__).resolve().parents[2]
_WORKER = _ROOT / "workers" / "backup-worker"
for _p in (str(_ROOT), str(_WORKER)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from archive_backup import (  # noqa: E402
    ARCHIVE_ITEM_TYPE,
    advance_state,
    build_archive_item_row,
    is_skippable_error,
    item_kind,
    state_blob_path,
)


# ── is_skippable_error: permission/no-access => skip gracefully, don't FAIL ──

def test_skippable_errors_are_access_and_license_signals():
    # perms not consented / mailbox unlicensed / access denied => skip
    assert is_skippable_error("404 MailboxNotEnabledForRESTAPI")
    assert is_skippable_error("HTTP 403 Forbidden")
    assert is_skippable_error("401 Unauthorized")
    assert is_skippable_error("Authorization_RequestDenied: insufficient privileges")
    assert is_skippable_error("ErrorAccessDenied")
    assert is_skippable_error("Access is denied")


def test_transient_and_real_errors_are_NOT_skippable():
    # these should FAIL (and retry), not be silently skipped
    assert not is_skippable_error("503 Service Unavailable")
    assert not is_skippable_error("500 Internal Server Error")
    assert not is_skippable_error("ConnectTimeout")
    assert not is_skippable_error("KeyError: 'id'")
    assert not is_skippable_error("")
    assert not is_skippable_error(None)


# ── item_kind: MAPI class -> human kind (the only "what is it" we can show) ──

def test_item_kind_maps_common_mapi_classes():
    assert item_kind("IPM.Note") == "Email"
    assert item_kind("IPM.Note.SMIME") == "Email"
    assert item_kind("IPM.Contact") == "Contact"
    assert item_kind("IPM.Appointment") == "Calendar event"
    assert item_kind("IPM.Schedule.Meeting.Request") == "Calendar event"
    assert item_kind("IPM.Task") == "Task"
    assert item_kind("IPM.StickyNote") == "Note"
    # unknown / empty fall back to the raw class or a generic
    assert item_kind("IPM.Weird") == "IPM.Weird"
    assert item_kind("") == "Item"
    assert item_kind(None) == "Item"


# ── advance_state: the incremental engine ────────────────────────────────

def test_first_run_takes_all_stored():
    current = {"a": "k1", "b": "k2"}
    assert advance_state(None, current, {"a": "k1", "b": "k2"}) == {"a": "k1", "b": "k2"}


def test_unchanged_items_keep_prev_key_without_re_export():
    prev = {"a": "k1", "b": "k2"}
    current = {"a": "k1", "b": "k2"}  # nothing changed
    assert advance_state(prev, current, {}) == {"a": "k1", "b": "k2"}  # stored nothing


def test_changed_item_advances_only_when_stored():
    prev = {"a": "old"}
    current = {"a": "new"}  # changeKey moved
    # export FAILED (not in stored) -> keep OLD key so select_changed re-flags it
    assert advance_state(prev, current, {}) == {"a": "old"}
    # export OK -> advance to the new key
    assert advance_state(prev, current, {"a": "new"}) == {"a": "new"}


def test_deleted_item_dropped_from_state():
    prev = {"a": "k1", "gone": "k9"}
    current = {"a": "k1"}  # 'gone' no longer in the archive
    out = advance_state(prev, current, {})
    assert out == {"a": "k1"} and "gone" not in out


def test_new_item_that_failed_export_is_omitted_for_retry():
    prev: dict = {}
    current = {"new": "k1"}
    assert advance_state(prev, current, {}) == {}  # failed new -> retry next run
    assert advance_state(prev, current, {"new": "k1"}) == {"new": "k1"}  # stored -> tracked


# ── build_archive_item_row: SnapshotItem shape ───────────────────────────

def test_row_shape_uses_extra_data_key_not_metadata():
    row = build_archive_item_row(
        snapshot_id="S", tenant_id="T",
        folder={"id": "F", "displayName": "Archive/2019"},
        item={"id": "I", "changeKey": "CK", "type": "IPM.Note", "size": 1234,
              "lastModifiedDateTime": "2026-07-14T00:00:00Z"},
        blob_path="T/R/S/I", content_hash="H", blob_len=2048,
        mailbox_id="MBX", graph_version="beta",
    )
    assert row["external_id"] == "I"
    assert row["item_type"] == ARCHIVE_ITEM_TYPE == "ARCHIVE_ITEM"
    assert row["parent_external_id"] == "F"
    assert row["folder_path"] == "Archive/2019"
    assert row["blob_path"] == "T/R/S/I"
    assert row["content_size"] == 2048
    assert row["content_hash"] == row["content_checksum"] == "H"
    # name = friendly kind (no subject is available from the opaque endpoint)
    assert row["name"] == "Email"
    # mapped-class insert -> MUST be the ORM attribute 'extra_data', never the
    # column name 'metadata' (that key would silently vanish on insert).
    assert "extra_data" in row and "metadata" not in row
    assert row["extra_data"]["change_key"] == "CK"
    assert row["extra_data"]["opaque"] is True
    assert row["extra_data"]["mailbox_id"] == "MBX"
    # "what it is" — kind + raw MAPI class exposed for the browse UI
    assert row["extra_data"]["kind"] == "Email"
    assert row["extra_data"]["mapi_type"] == "IPM.Note"


def test_row_name_is_friendly_kind_not_raw_mapi_class():
    # archive item listing $selects id,changeKey,size,type — NO subject; the
    # name must be a human kind, never the raw "IPM.*" class.
    row = build_archive_item_row(
        snapshot_id="S", tenant_id="T", folder={"id": "F"},
        item={"id": "I", "changeKey": "CK", "type": "IPM.Contact"},
        blob_path="p", content_hash="H", blob_len=1, mailbox_id="M", graph_version="v1.0",
    )
    assert row["name"] == "Contact"
    assert row["folder_path"] == "F"  # falls back to folder id when no displayName


# ── state_blob_path: fixed & overwritable ────────────────────────────────

def test_state_blob_path_is_deterministic_and_overwritable():
    assert state_blob_path("T", "R") == state_blob_path("T", "R")  # no timestamp
    assert state_blob_path("T", "R") == "T/R/_archive_state/changekeys.json"
