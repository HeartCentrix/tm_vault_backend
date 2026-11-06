"""M365_GROUP channel backups re-read every channel's full history every run
(_backup_team_channels called get_channel_messages with no delta token). It now
resumes each channel from its saved delta token and saves the new one, like the
standalone TEAMS_CHANNEL handler. A stale token falls back to a full read inside
get_channel_messages (410/400)."""
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
_spec = importlib.util.spec_from_file_location("bw_group_ch_delta", _REPO_ROOT / "workers" / "backup-worker" / "main.py")
_bw = importlib.util.module_from_spec(_spec)
sys.modules["bw_group_ch_delta"] = _bw
try:
    _spec.loader.exec_module(_bw)
except Exception as exc:  # pragma: no cover
    pytest.skip(f"backup-worker module failed to import: {exc}", allow_module_level=True)


def test_group_channels_resume_and_save_per_channel_delta_tokens(monkeypatch):
    stored = SimpleNamespace(extra_data={"channel_delta_tokens": {"c1": "old-c1"}})
    seen = {}

    class _Graph:
        async def get_teams_channels(self, team_id):
            return {"value": [{"id": "c1", "displayName": "A"}, {"id": "c2", "displayName": "B"}]}

        async def get_channel_messages(self, team_id, ch_id, delta_token=None):
            seen[ch_id] = delta_token
            return {"value": [], "@odata.deltaLink": f"new-{ch_id}"}

    class _Sess:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *e):
            return False

        def add_all(self, rows):
            pass

        async def commit(self):
            pass

        async def get(self, model, rid):
            return stored

    monkeypatch.setattr(_bw, "async_session_factory", _Sess)
    monkeypatch.setattr(_bw.azure_storage_manager, "get_shard_for_resource", lambda *a: None)
    monkeypatch.setattr(_bw.azure_storage_manager, "get_container_name", lambda *a: "c")
    resource = SimpleNamespace(id=uuid.uuid4(), display_name="G",
                               extra_data={"channel_delta_tokens": {"c1": "old-c1"}})
    asyncio.run(_bw.BackupWorker._backup_team_channels(
        SimpleNamespace(worker_id="t"), resource, SimpleNamespace(id=uuid.uuid4()),
        SimpleNamespace(id=uuid.uuid4()), _Graph(), "team"))
    assert seen == {"c1": "old-c1", "c2": None}
    assert stored.extra_data["channel_delta_tokens"] == {"c1": "new-c1", "c2": "new-c2"}
