"""Disabled Entra accounts must stay backup-eligible while they still exist.

Discovery derives a staged row's status in ``_prepare_staging_rows``.  Until
2026-09-03 an ``accountEnabled=false`` user/room/shared mailbox was staged
INACCESSIBLE, which excluded it from every scheduler dispatch path even though
Graph app-only reads on the mailbox succeed.  Room and shared mailboxes are
account-disabled by design, and leavers' mailboxes are disabled by definition,
so "exists in the Graph listing" is the only gate discovery should apply.
Existence-loss is still signalled by the stale-mark (absent from a successful
scan) and by the worker's resource-level 404/423 marking.
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import sys
import uuid

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "test_secret_for_unit_tests_only_x")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")
os.environ.setdefault("AUDIT_SERVICE_URL", "http://localhost:8000")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_REPO_ROOT / "workers" / "discovery-worker"))

_spec = importlib.util.spec_from_file_location(
    "discovery_worker_main_disabled",
    _REPO_ROOT / "workers" / "discovery-worker" / "main.py",
)


def _load():
    mod = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(mod)
    return mod


def _stage_one(resource):
    mod = _load()
    rows = mod._prepare_staging_rows(uuid.uuid4(), [resource])
    assert len(rows) == 1
    return rows[0]


def test_disabled_entra_user_is_staged_discovered():
    row = _stage_one({
        "type": "ENTRA_USER",
        "external_id": "f58e7b73-c13a-42a5-84d7-735298495d95",
        "display_name": "Bonterra Conference Room",
        "email": "BonterraConferenceRoom@taylormorrison.com",
        "metadata": {"account_enabled": False, "user_type": "Member"},
        "_account_enabled": False,
    })
    assert row["resource_status"] == "DISCOVERED"


def test_disabled_room_mailbox_is_staged_discovered():
    row = _stage_one({
        "type": "ROOM_MAILBOX",
        "external_id": "f58e7b73-c13a-42a5-84d7-735298495d95",
        "display_name": "Bonterra Conference Room",
        "email": "BonterraConferenceRoom@taylormorrison.com",
        "metadata": {"account_enabled": False, "mailbox_purpose": "room"},
        "_account_enabled": False,
    })
    assert row["resource_status"] == "DISCOVERED"


def test_disabled_flag_is_kept_in_metadata_for_the_ui():
    row = _stage_one({
        "type": "SHARED_MAILBOX",
        "external_id": "11111111-2222-3333-4444-555555555555",
        "display_name": "dba_requests",
        "email": "dba_requests@taylormorrison.com",
        "metadata": {"account_enabled": False, "mailbox_purpose": "shared"},
        "_account_enabled": False,
    })
    assert row["resource_status"] == "DISCOVERED"
    assert row["metadata"]["account_enabled"] is False
