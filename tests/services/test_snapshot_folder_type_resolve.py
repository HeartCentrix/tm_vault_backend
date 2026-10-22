"""Regression: the /snapshots/folders endpoint must auto-resolve
OneDrive item types to the USER_ONEDRIVE sibling.

Bug (2026-07-13): the Recovery OneDrive tab requests folders with the
ENTRA_USER parent's snapshot id. The endpoint's item_type→child map
only knew mail/chats/calendar/contacts, so OneDrive requests resolved
siblings of the PARENT resource and returned its junk folder rows —
the My Drive view rendered 17 root files and zero folder rows while
8,110 files sat reachable under 558 folders.
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

_MAIN_PATH = _REPO_ROOT / "services" / "snapshot-service" / "main.py"
_spec = importlib.util.spec_from_file_location("ss_main_under_test", _MAIN_PATH)
_ss = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["ss_main_under_test"] = _ss
assert _spec and _spec.loader
_spec.loader.exec_module(_ss)  # type: ignore[union-attr]

from shared.models import ResourceType  # noqa: E402


def test_onedrive_item_types_resolve_to_user_onedrive_child():
    m = _ss.FOLDER_ITEM_TYPE_TO_CHILD
    assert m.get("ONEDRIVE_FILE") == ResourceType.USER_ONEDRIVE
    assert m.get("FILE") == ResourceType.USER_ONEDRIVE


def test_existing_tab_mappings_survive():
    m = _ss.FOLDER_ITEM_TYPE_TO_CHILD
    assert m.get("EMAIL") == ResourceType.USER_MAIL
    assert m.get("TEAMS_CHAT_MESSAGE") == ResourceType.USER_CHATS
    assert m.get("CALENDAR_EVENT") == ResourceType.USER_CALENDAR
    assert m.get("USER_CONTACT") == ResourceType.USER_CONTACTS
