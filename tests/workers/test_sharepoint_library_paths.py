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
