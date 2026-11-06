"""Tenants past one Graph page (>999 users or groups, >50 group members/owners, >999 contacts)
must be captured in full. GraphClient._get follows @odata.nextLink, so the directory's users and
groups and ENTRA_USER contacts are complete. The group owners/members $batch with $top=50 was
always rejected by graph_batch's paginated-URL check (a $batch sub-response cannot follow
nextLink) and only then fell back to the paged per-group reads; those paged reads now run
directly."""
from __future__ import annotations

import asyncio
import importlib.util
import os
import pathlib
import sys
import uuid
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")
_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
_spec = importlib.util.spec_from_file_location("bw_entra_dir_pages", _REPO_ROOT / "workers" / "backup-worker" / "main.py")
_bw = importlib.util.module_from_spec(_spec)
sys.modules["bw_entra_dir_pages"] = _bw
try:
    _spec.loader.exec_module(_bw)
except Exception as exc:  # pragma: no cover
    pytest.skip(f"backup-worker module failed to import: {exc}", allow_module_level=True)

from shared.graph_client import GraphClient  # noqa: E402

BASE = "https://graph.microsoft.com/v1.0"


def _paged(prefix, total, per_page, path, request):
    page = int(request.url.params.get("page", "0"))
    start = page * per_page
    body = {"value": [{"id": f"{prefix}{i}", "displayName": f"{prefix}{i}"}
                      for i in range(start, min(start + per_page, total))]}
    if start + per_page < total:
        body["@odata.nextLink"] = f"{BASE}{path}?page={page + 1}"
    return httpx.Response(200, json=body)


def _respond(request):
    path = request.url.path.replace("/v1.0", "")
    sizes = {"/users": ("u", 1004, 999), "/groups": ("g", 1, 999),
             "/groups/g0/owners": ("o", 60, 50), "/groups/g0/members": ("m", 120, 50),
             "/users/u1/contacts": ("c", 1500, 999)}
    if path in sizes:
        return _paged(*sizes[path], path, request)
    return httpx.Response(404, json={"error": {"code": "NotFound"}})


@pytest.fixture
def client(monkeypatch):
    c = GraphClient("app", "secret", "tenant")
    monkeypatch.setattr(c, "_get_token", AsyncMock(return_value="token"))
    monkeypatch.setattr("shared.graph_rate_limiter.graph_rate_limiter.acquire", AsyncMock())
    http = httpx.AsyncClient(transport=httpx.MockTransport(_respond))

    @asynccontextmanager
    async def session(*a, **k):
        yield http
    monkeypatch.setattr(c, "_http_session", session)
    return c


@pytest.mark.parametrize("hardened", [True, False])
def test_directory_users_and_group_members_span_every_page(client, monkeypatch, hardened):
    if not hardened:
        monkeypatch.setattr(client, "_get", client._get_legacy)
    batched = []

    async def batch(reqs):
        batched.append(reqs)
        return {}
    monkeypatch.setattr(client, "batch", batch)
    rows = []

    class _Sess:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *e):
            return False

        def add_all(self, items):
            rows.extend(items)

        async def commit(self):
            pass

    monkeypatch.setattr(_bw, "async_session_factory", _Sess)
    worker = SimpleNamespace(worker_id="t", ENTRA_DIRECTORY_CATEGORIES=_bw.BackupWorker.ENTRA_DIRECTORY_CATEGORIES,
                             _should_partition_entra=lambda: False)
    resource = SimpleNamespace(id=uuid.uuid4(), display_name="Dir", external_id="tenant")
    snapshot = SimpleNamespace(id=uuid.uuid4(), job_id=uuid.uuid4())
    asyncio.run(_bw.BackupWorker.backup_entra_directory(
        worker, client, resource, snapshot, SimpleNamespace(id=uuid.uuid4()),
        {"partitionFilter": {"categoryIds": ["USERS", "GROUPS"]}}))
    users = [r for r in rows if r.item_type == "ENTRA_DIR_USER"]
    groups = [r for r in rows if r.item_type == "ENTRA_DIR_GROUP"]
    assert len(users) == 1004
    assert len(groups) == 1
    raw = groups[0].extra_data["raw"]
    assert len(raw["_owners"]) == 60 and len(raw["_members"]) == 120 and raw["_memberCount"] == 120
    assert batched == [], "owners/members are paged reads, never a $batch that cannot follow nextLink"


def test_entra_user_contacts_span_every_page(client):
    assert len(asyncio.run(client.get_user_contacts("u1"))["value"]) == 1500
