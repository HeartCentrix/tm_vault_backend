"""Regression: the export ZIP download must stream, never buffer.

Bug (found 2026-08-04 review): the ZIP branch of download_export_zip did
``content = await shard.download_blob(...)`` and returned
``StreamingResponse(iter([content]))`` — the whole artifact materialized
as one bytes object. Export routing explicitly supports whale exports
(>HEAVY_EXPORT_THRESHOLD_BYTES, 20 GB), so a multi-GB ZIP download blew
past the pod memory limit and OOM-killed job-service, dropping every
concurrent request; retries reproduced the kill loop.

Contract pinned here:
  - The handler never calls shard.download_blob for the ZIP branch — it
    probes with get_blob_properties and serves via download_blob_stream.
  - The streamed body is byte-identical to the blob, chunk boundaries
    preserved end-to-end.
  - Content-Length comes from the blob properties.
  - A blob missing from every candidate container is still a 404.
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import sys
import uuid

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")
# Point the fire-and-forget audit emit at a dead local port so tests
# don't wait on DNS for the compose hostname.
os.environ.setdefault("AUDIT_SERVICE_URL", "http://127.0.0.1:1")

import pytest
from fastapi.testclient import TestClient

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_SVC_DIR = _REPO_ROOT / "services" / "job-service"
if str(_SVC_DIR) not in sys.path:
    sys.path.insert(0, str(_SVC_DIR))

_spec = importlib.util.spec_from_file_location(
    "js_main_stream_under_test", _SVC_DIR / "main.py"
)
_js = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["js_main_stream_under_test"] = _js
assert _spec and _spec.loader
_spec.loader.exec_module(_js)  # type: ignore[union-attr]

from shared import azure_storage  # noqa: E402
from shared.database import get_db  # noqa: E402
from shared.models import Job, JobStatus, JobType  # noqa: E402
from shared.security import get_current_user_from_token  # noqa: E402

TENANT = uuid.uuid4()
CHUNKS = [b"PK\x03\x04 chunk-one ", b"chunk-two ", b"chunk-three"]
BLOB_PATH = "exports/j1/export_1.zip"


class _FakeShard:
    """Streams CHUNKS for BLOB_PATH in the 'exports' container. Explodes
    if anything tries the old buffering call."""

    def __init__(self, present=True):
        self.present = present
        self.stream_calls = []

    async def download_blob(self, container, blob_path):
        raise AssertionError(
            "download_blob buffers the whole ZIP in memory — the download "
            "endpoint must use get_blob_properties + download_blob_stream"
        )

    async def get_blob_properties(self, container, blob_path):
        if self.present and container == "exports" and blob_path == BLOB_PATH:
            return {"name": blob_path, "size": sum(len(c) for c in CHUNKS)}
        return None

    async def download_blob_stream(self, container, blob_path, chunk_size=4 * 1024 * 1024):
        self.stream_calls.append((container, blob_path))
        if self.present and container == "exports" and blob_path == BLOB_PATH:
            for c in CHUNKS:
                yield c


class _FakeSession:
    def __init__(self, job):
        self.job = job

    async def get(self, entity, ident):
        return self.job if ident == self.job.id else None


def _export_job():
    return Job(
        id=uuid.uuid4(),
        tenant_id=TENANT,
        resource_id=None,
        type=JobType.EXPORT,
        status=JobStatus.COMPLETED,
        spec={"restore_type": "EXPORT_ZIP", "snapshot_ids": [], "item_ids": []},
        result={"blob_path": BLOB_PATH, "container": "exports"},
    )


@pytest.fixture()
def client(monkeypatch):
    job = _export_job()
    sess = _FakeSession(job)
    shard = _FakeShard()

    async def _allow(db, user, jb):
        return None

    monkeypatch.setattr(_js, "ensure_job_access", _allow)
    monkeypatch.setattr(
        azure_storage.azure_storage_manager, "get_default_shard", lambda: shard
    )
    _js.app.dependency_overrides[get_db] = lambda: sess
    _js.app.dependency_overrides[get_current_user_from_token] = lambda: {
        "id": str(uuid.uuid4()),
        "email": "caller@taylormorrison.com",
        "roles": ["RESTORE_OPERATOR"],
        "tenant_ids": [str(TENANT)],
    }
    yield TestClient(_js.app), job, shard
    _js.app.dependency_overrides.clear()


def test_zip_download_streams_without_buffering(client):
    tc, job, shard = client
    r = tc.get(f"/api/v1/jobs/export/{job.id}/download")
    assert r.status_code == 200
    assert r.content == b"".join(CHUNKS)
    assert r.headers["content-length"] == str(sum(len(c) for c in CHUNKS))
    assert "attachment" in r.headers["content-disposition"]
    assert shard.stream_calls == [("exports", BLOB_PATH)]


def test_missing_blob_is_still_404(client, monkeypatch):
    tc, job, shard = client
    shard.present = False
    r = tc.get(f"/api/v1/jobs/export/{job.id}/download")
    assert r.status_code == 404
