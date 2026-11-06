"""Markers recorded mid-run must also land on the in-memory snapshot. The terminal write
(_write_snapshot_terminal) persists the in-memory extra_data and keeps only an allowlist of keys
from the row, so a row-only patch is dropped at completion (Railway 2026-11-06 incremental #2:
every group/channel [DEDUP] skip logged, 0 of 22 backups kept covered_by_*)."""
from __future__ import annotations

import asyncio
import importlib.util
import os
import pathlib
import sys
from types import SimpleNamespace

import httpx
import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")
_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
_MAIN_PATH = _REPO_ROOT / "workers" / "backup-worker" / "main.py"
_spec = importlib.util.spec_from_file_location("bw_markers_in_memory", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)
sys.modules["bw_markers_in_memory"] = _bw
try:
    _spec.loader.exec_module(_bw)
except Exception as exc:  # pragma: no cover
    pytest.skip(f"backup-worker module failed to import: {exc}", allow_module_level=True)


async def _patch(snapshot_id, data):
    return True


def test_dedup_marker_is_kept_on_the_in_memory_snapshot(monkeypatch):
    import shared.sla_workloads as sla

    async def owner(*_a, **_k):
        return "owner-1"
    monkeypatch.setattr(sla, "protected_owner_id", owner)
    monkeypatch.setattr(_bw, "async_session_factory", _Null)
    worker = SimpleNamespace(worker_id="t", _patch_snapshot_extra_data=_patch)
    snap = SimpleNamespace(id="s1", extra_data={"provider_phase": "RUNNING"})
    tenant = SimpleNamespace(id="t1")
    assert asyncio.run(_bw.BackupWorker._owned_elsewhere(worker, tenant, "SHAREPOINT_SITE", "x", snap, "team_site"))
    assert snap.extra_data == {"provider_phase": "RUNNING", "covered_by_team_site": "owner-1"}


def test_no_mailbox_reason_is_kept_on_the_in_memory_snapshot():
    async def get(url, params=None, **_):
        req = httpx.Request("GET", url)
        resp = httpx.Response(404, json={"error": {"code": "MailboxNotEnabledForRESTAPI"}}, request=req)
        raise httpx.HTTPStatusError("err", request=req, response=resp)
    worker = SimpleNamespace(worker_id="t", _patch_snapshot_extra_data=_patch)
    graph = SimpleNamespace(GRAPH_URL="https://graph.microsoft.com/v1.0", _get=get)
    snap = SimpleNamespace(id="s1", extra_data={"provider_phase": "RUNNING"})
    assert asyncio.run(_bw.BackupWorker._note_no_mailbox(worker, graph, "u", snap, "USER_CALENDAR"))
    assert snap.extra_data["provider_phase"] == "RUNNING"
    assert "No Exchange Online mailbox" in snap.extra_data["no_mailbox"]


class _Null:
    async def __aenter__(self):
        return None

    async def __aexit__(self, *exc):
        return False
