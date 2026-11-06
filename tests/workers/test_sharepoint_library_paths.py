"""SharePoint library files must land where the Recovery Site view looks ({site}/lists/{library}/...),
and libraries Graph does not list as drives must still be backed up.

Railway 2026-11-04 (6d9c7ec): every library file was stored under Graph's raw '/drives/<id>/root:'
path, so Documents looked empty in the UI (Donor Desk's .docx); Site Assets (13 sites) and
Style Library were not backed up at all because /sites/{id}/drives omits them and the list
pass skipped every document library."""
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
_spec = importlib.util.spec_from_file_location("bw_sp_library_paths", _REPO_ROOT / "workers" / "backup-worker" / "main.py")
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_sp_library_paths"] = _bw
try:
    _spec.loader.exec_module(_bw)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(f"backup-worker module failed to import: {exc}", allow_module_level=True)

DRIVE = "/drives/b!z95-QaP4K0WjsljgdMK0fajgKPtmqFJNlz42VMFv4T25eXQ8yrrISpJzhadicsDD/root:"


def test_library_file_at_the_root_lands_in_its_library():
    item = {"_site_label": "Donor Desk", "_drive_name": "Documents", "parentReference": {"path": DRIVE}}
    assert _bw._drive_item_folder_path(item) == "Donor Desk/lists/Documents"


def test_library_subfolders_are_kept():
    item = {"_site_label": "QFION", "_drive_name": "Documents", "parentReference": {"path": DRIVE + "/General/2024"}}
    assert _bw._drive_item_folder_path(item) == "QFION/lists/Documents/General/2024"


def test_onedrive_files_keep_graphs_parent_path():
    item = {"parentReference": {"path": "/drive/root:/Work"}}
    assert _bw._drive_item_folder_path(item) == "/drive/root:/Work"


def test_a_library_graph_lists_as_a_drive_is_covered_by_the_drive_pass():
    drives = _bw._drive_web_urls([{"webUrl": "https://qfion.sharepoint.com/sites/DonorDesk/Shared%20Documents"}])
    assert _bw._library_has_drive({"webUrl": "https://qfion.sharepoint.com/sites/DonorDesk/Shared Documents/"}, drives)


def test_a_library_missing_from_the_drives_is_left_to_the_list_pass():
    drives = _bw._drive_web_urls([{"webUrl": "https://qfion.sharepoint.com/sites/DonorDesk/Shared%20Documents"}])
    assert not _bw._library_has_drive({"webUrl": "https://qfion.sharepoint.com/sites/DonorDesk/SiteAssets"}, drives)
    assert not _bw._library_has_drive({}, drives)


# Deletions: the Site view merges library files across restore points (Graph delta stores only changes),
# so a deleted file needs a tombstone or an older copy would show again.

def test_a_deleted_library_file_becomes_a_tombstone_under_its_library():
    from types import SimpleNamespace
    snap = SimpleNamespace(id="snap-1", backend_id="backend-1")
    item = {"id": "f2", "deleted": {"state": "deleted"}, "_site_label": "Donor Desk", "_drive_name": "Documents",
            "_drive_id": "drive-1", "parentReference": {"driveId": "drive-1"}}
    row = _bw._library_tombstone_row(snap, "tenant-1", item)
    assert row["item_type"] == "FILE" and row["is_deleted"] is True and row["blob_path"] is None
    assert row["external_id"] == "f2" and row["folder_path"] == "Donor Desk/lists/Documents"
    assert row["backend_id"] == "backend-1" and row["metadata"]["structured"]["drive_id"] == "drive-1"


def test_a_full_listing_tombstones_only_files_it_no_longer_sees():
    prior = [("a", "a.docx", "S/lists/Documents", False, "d1"), ("b", "b.docx", "S/lists/Documents", True, "d1"),
             ("x", "x.docx", "S/lists/Documents/Old", False, "d1")]
    unseen = _bw._unseen_library_files(prior, {"d1": {"a"}})
    assert [r[0] for r in unseen] == ["x"]


async def test_drive_items_carry_their_drive_id(monkeypatch):
    from shared.graph_client import GraphClient
    client = GraphClient("app", "secret", "tenant")

    async def drives(site_id):
        return [{"id": "drive-1", "name": "Documents"}]

    async def items(drive_id, token, holder):
        yield {"id": "f1"}

    monkeypatch.setattr(client, "list_sharepoint_site_drives", drives)
    monkeypatch.setattr(client, "iter_drive_items_by_id", items)
    got = [i async for i in client.iter_sharepoint_site_all_drive_items("site")]
    assert got == [{"id": "f1", "_drive_name": "Documents", "_drive_id": "drive-1"}]


# Unchanged list files: TM's REST pass re-downloaded 11,317 files / 214 MB every 8 h and retried
# 28,660 ghosted catalog files in 30 days. A list row's own version fields identify an unchanged file.

ROW = {"GUID": "g-1", "Modified": "2026-10-16T16:49:32Z", "OData__UIVersionString": "1.0",
       "FieldValuesAsText": {"owshiddenversion": "3"}}


def test_a_list_row_version_changes_whenever_the_item_changes():
    assert _bw._sp_row_version(ROW) == _bw._sp_row_version(dict(ROW))
    assert _bw._sp_row_version(ROW) != _bw._sp_row_version({**ROW, "FieldValuesAsText": {"owshiddenversion": "4"}})
    assert _bw._sp_row_version(ROW) != _bw._sp_row_version({**ROW, "Modified": "2026-10-17T00:00:00Z"})
    assert _bw._sp_row_version({"Modified": "x"}) is None          # no item id or version: never reuse


def test_only_a_stored_or_ghosted_copy_of_the_same_version_is_reused():
    version = _bw._sp_row_version(ROW)
    stored = {"row_version": version, "blob_path": "b/1", "ghosted": False}
    assert _bw._reusable_sp_copy(stored, version)
    assert _bw._reusable_sp_copy({**stored, "blob_path": None, "ghosted": True}, version)
    assert not _bw._reusable_sp_copy({**stored, "blob_path": None}, version)      # failed before: try again
    assert not _bw._reusable_sp_copy({**stored, "row_version": "old"}, version)
    assert not _bw._reusable_sp_copy(None, version) and not _bw._reusable_sp_copy(stored, None)


# Railway 2026-11-06: SharePoint re-stamped Site Assets' __siteIcon__ on 6 sites in one second (item
# version and Modified moved, bytes did not) and each was downloaded again. SP.File.ContentTag is
# "{UniqueId},<item version>,<content version>": only the third part moves with the bytes (TM's QFION
# __siteIcon__.jpg: item version 264, content version 2).
FILE = {"ContentTag": "{13E977B4-3A9F-4412-A520-4D7520AD2372},23,3", "Length": "10436",
        "UniqueId": "13e977b4-3a9f-4412-a520-4d7520ad2372"}


def test_a_restamped_file_with_the_same_content_reuses_the_stored_copy():
    stored = {"row_version": "old", "blob_path": "b/1", "ghosted": False, "file": FILE}
    restamped = {**FILE, "ContentTag": "{13E977B4-3A9F-4412-A520-4D7520AD2372},24,3"}
    assert _bw._reusable_sp_copy(stored, "new", restamped)
    assert not _bw._reusable_sp_copy(stored, "new", {**FILE, "ContentTag": "{13E977B4-3A9F-4412-A520-4D7520AD2372},24,4"})
    assert not _bw._reusable_sp_copy(stored, "new", {**restamped, "Length": "10437"})
    assert not _bw._reusable_sp_copy(stored, "new", {"Length": "10436"})               # no content tag: download
    assert not _bw._reusable_sp_copy({**stored, "file": {"Length": "10436"}}, "new", restamped)
    assert not _bw._reusable_sp_copy({**stored, "blob_path": None}, "new", restamped)   # failed before: try again
    assert not _bw._reusable_sp_copy(stored, "new", None)
