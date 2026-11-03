"""Regression: the OneDrive deltaLink must not advance past data that was
never uploaded.

Bug (found 2026-07-13, jrue/jlucas drives): the first full walk of a drive
OOM-crash-looped before the upload phase finished, but the enumeration
code persisted the fresh @odata.deltaLink to
resource.extra_data["onedrive_delta_link"] unconditionally. Every later
run resumed from that token as a tiny incremental, so the 130 GB / 348 GB
baselines were never captured (500 MB / 135 MB stored) — and nothing
upstream ever sends force-full for a resource with prior snapshots.

Fix: enumeration stashes the fresh link under
"onedrive_delta_link_pending"; it is promoted to the active
"onedrive_delta_link" key only when the snapshot reaches COMPLETED.
A PARTIAL/FAILED run leaves the active link untouched, so the next run
re-enumerates from the last *successful* position.
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import sys

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "workers" / "backup-worker" / "main.py"
_spec = importlib.util.spec_from_file_location("bw_delta_under_test", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_delta_under_test"] = _bw
assert _spec and _spec.loader
_spec.loader.exec_module(_bw)  # type: ignore[union-attr]


LINK_OLD = "https://graph.microsoft.com/v1.0/drives/d/root/delta?token=OLD"
LINK_NEW = "https://graph.microsoft.com/v1.0/drives/d/root/delta?token=NEW"


# ─── stash: enumeration must never touch the active link ──────────────────

def test_stash_sets_pending_and_preserves_active():
    ed = _bw.stash_pending_delta_link(
        {"onedrive_delta_link": LINK_OLD, "drive_id": "d"}, LINK_NEW,
    )
    assert ed["onedrive_delta_link_pending"] == LINK_NEW
    assert ed["onedrive_delta_link"] == LINK_OLD
    assert ed["drive_id"] == "d"


def test_stash_handles_missing_extra_data():
    ed = _bw.stash_pending_delta_link(None, LINK_NEW)
    assert ed["onedrive_delta_link_pending"] == LINK_NEW
    assert "onedrive_delta_link" not in ed


# ─── promote: only a COMPLETED run advances the active link ────────────────

def test_promote_moves_pending_to_active_and_clears_pending():
    ed, promoted = _bw.promote_pending_delta_link(
        {"onedrive_delta_link": LINK_OLD, "onedrive_delta_link_pending": LINK_NEW},
    )
    assert promoted is True
    assert ed["onedrive_delta_link"] == LINK_NEW
    assert "onedrive_delta_link_pending" not in ed


def test_promote_without_pending_is_a_noop():
    src = {"onedrive_delta_link": LINK_OLD}
    ed, promoted = _bw.promote_pending_delta_link(src)
    assert promoted is False
    assert ed["onedrive_delta_link"] == LINK_OLD


def test_promote_handles_missing_extra_data():
    ed, promoted = _bw.promote_pending_delta_link(None)
    assert promoted is False
    assert ed == {}


# ─── delta tombstone detection (2026-07-13) ────────────────────────────────
#
# Graph /delta expresses deletions two ways: the @removed annotation AND
# a `deleted` facet on the item. The walk only skipped @removed, so
# facet-style tombstones (unnamed, size 0, null quickXorHash) were
# persisted as ONEDRIVE_FILE rows — 121 blobless "(unnamed)" entries in
# jrue's My Drive root, rendered as "metadata only" junk in Recovery.

def test_removed_annotation_is_tombstone():
    assert _bw.is_delta_tombstone({"@removed": {"reason": "deleted"}, "id": "x"})


def test_deleted_facet_is_tombstone():
    assert _bw.is_delta_tombstone({
        "id": "01RRU7ZAHT4KUAJNFZV5AZXIRW72IQU3CB",
        "size": 0,
        "deleted": {"state": "deleted"},
        "parentReference": {"id": "p", "driveId": "d"},
    })


def test_live_file_is_not_tombstone():
    assert not _bw.is_delta_tombstone({
        "id": "abc", "name": "report.xlsx", "size": 123,
        "file": {"hashes": {"quickXorHash": "Zm9v"}},
        "parentReference": {"id": "p"},
    })
