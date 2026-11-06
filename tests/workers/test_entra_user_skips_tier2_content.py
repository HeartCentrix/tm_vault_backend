"""ENTRA_USER re-read every contact and every calendar event (fresh blob each, attachments
re-downloaded) on every run, duplicating the user's Tier-2 USER_CONTACTS / USER_CALENDAR
backups. Recovery's contacts/calendar tabs read the Tier-2 child when it exists, so ENTRA_USER
skips that content when a protected Tier-2 child owns it."""
from __future__ import annotations

import asyncio
import importlib.util
import os
import pathlib
import sys
import uuid
from types import SimpleNamespace

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")
_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
_spec = importlib.util.spec_from_file_location("bw_entra_tier2", _REPO_ROOT / "workers" / "backup-worker" / "main.py")
_bw = importlib.util.module_from_spec(_spec)
sys.modules["bw_entra_tier2"] = _bw
try:
    _spec.loader.exec_module(_bw)
except Exception as exc:  # pragma: no cover
    pytest.skip(f"backup-worker module failed to import: {exc}", allow_module_level=True)


class _Graph:
    def __init__(self):
        self.calls = []

    async def get_user_profile(self, uid):
        self.calls.append("profile")
        return {"id": uid, "displayName": "U"}

    async def get_user_contacts(self, uid):
        self.calls.append("contacts")
        return {"value": [{"id": "c1"}]}

    async def get_calendar_events_delta(self, uid, token=None):
        self.calls.append("calendar")
        return {"value": [{"id": "e1"}], "@odata.deltaLink": "d"}

    async def get_user_manager(self, uid):
        return None

    async def get_user_direct_reports(self, uid):
        return {"value": []}

    async def get_user_group_memberships(self, uid):
        return {"value": []}


def _run(monkeypatch, owned):
    async def owned_elsewhere(tenant, rtype, ext, snapshot, what):
        return rtype in owned and ext == f"u1:{owned[rtype]}"

    async def none(*a, **k):
        return None

    async def upload(*a, **k):
        return {"success": True}

    persisted = []

    async def persist(session, items):
        persisted.extend(items)

    class _Sess:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *e):
            return False

        async def get(self, *a):
            return None

    monkeypatch.setattr(_bw, "async_session_factory", _Sess)
    monkeypatch.setattr(_bw, "upload_blob_with_retry", upload)
    monkeypatch.setattr(_bw, "_persist_entra_snapshot_items", persist)
    monkeypatch.setattr(_bw.azure_storage_manager, "get_shard_for_resource", lambda *a: None)
    monkeypatch.setattr(_bw.azure_storage_manager, "get_container_name", lambda *a: "c")
    monkeypatch.setattr(_bw.azure_storage_manager, "build_blob_path", lambda *a: "/".join(a))
    worker = SimpleNamespace(worker_id="t", get_sla_policy=none, _owned_elsewhere=owned_elsewhere,
                             update_resource_backup_info=none)
    resource = SimpleNamespace(id=uuid.uuid4(), external_id="u1", display_name="U",
                               type=SimpleNamespace(value="ENTRA_USER"), extra_data={})
    graph = _Graph()
    asyncio.run(_bw.BackupWorker._backup_entra_resource(
        worker, resource, SimpleNamespace(id=uuid.uuid4()), SimpleNamespace(id=uuid.uuid4(), backend_id=None),
        graph, uuid.uuid4()))
    return graph.calls, {i.item_type for i in persisted}


def test_protected_tier2_children_own_contacts_and_calendar(monkeypatch):
    calls, types = _run(monkeypatch, {"USER_CONTACTS": "contacts", "USER_CALENDAR": "calendar"})
    assert calls == ["profile"]
    assert types == {"USER_PROFILE"}


def test_without_tier2_owners_entra_user_still_backs_them_up(monkeypatch):
    calls, types = _run(monkeypatch, {})
    assert sorted(calls) == ["calendar", "contacts", "profile"]
    assert types == {"USER_PROFILE", "USER_CONTACT", "CALENDAR_EVENT"}
