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
    state_blob_path,
)


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
        item={"id": "I", "changeKey": "CK", "type": "message", "size": 1234,
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
    # mapped-class insert -> MUST be the ORM attribute 'extra_data', never the
    # column name 'metadata' (that key would silently vanish on insert).
    assert "extra_data" in row and "metadata" not in row
    assert row["extra_data"]["change_key"] == "CK"
    assert row["extra_data"]["opaque"] is True
    assert row["extra_data"]["mailbox_id"] == "MBX"


def test_row_name_falls_back_when_no_subject():
    # archive item listing $selects id,changeKey,size,type — no subject present
    row = build_archive_item_row(
        snapshot_id="S", tenant_id="T", folder={"id": "F"},
        item={"id": "I", "changeKey": "CK", "type": "message"},
        blob_path="p", content_hash="H", blob_len=1, mailbox_id="M", graph_version="v1.0",
    )
    assert row["name"] == "message"
    assert row["folder_path"] == "F"  # falls back to folder id when no displayName


# ── state_blob_path: fixed & overwritable ────────────────────────────────

def test_state_blob_path_is_deterministic_and_overwritable():
    assert state_blob_path("T", "R") == state_blob_path("T", "R")  # no timestamp
    assert state_blob_path("T", "R") == "T/R/_archive_state/changekeys.json"
