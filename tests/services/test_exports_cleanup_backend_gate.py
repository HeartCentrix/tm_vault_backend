"""#31: the daily Azure export-lifecycle job must not touch Azure when the
active storage backend is something else (TM stores on on-prem S3)."""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from services import exports_cleanup
from shared.storage import router as router_mod


def _router(active, kinds):
    backends = [SimpleNamespace(backend_id=bid, kind=kind) for bid, kind in kinds.items()]
    return SimpleNamespace(active_backend_id=lambda: active, list_backends=lambda: backends)


@pytest.mark.parametrize("active,kinds,expected", [
    ("b1", {"b1": "seaweedfs"}, False),
    ("b2", {"b1": "seaweedfs", "b2": "azure_blob"}, True),
    (None, {}, True),  # router not loaded yet: keep the old behaviour
])
def test_azure_export_storage_active(monkeypatch, active, kinds, expected):
    monkeypatch.setattr(router_mod, "router", _router(active, kinds))
    assert exports_cleanup.azure_export_storage_active() is expected


def test_scheduler_job_and_startup_use_the_gate():
    from pathlib import Path
    src = (Path(__file__).resolve().parents[2] / "services/backup-scheduler/main.py").read_text()
    job = src[src.index("async def _exports_cleanup_daily"):src.index('id="exports_cleanup_daily"')]
    assert job.count("azure_export_storage_active()") == 2  # job early-return + startup info log
