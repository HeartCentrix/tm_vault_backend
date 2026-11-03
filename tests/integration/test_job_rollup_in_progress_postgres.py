"""Activity item counts must include live progress of IN_PROGRESS snapshots.

Chat captures publish ``snapshots.item_count`` while running, but the activity
rollup only summed terminal snapshots, so every user's chat count displayed 0
until the whole chat snapshot completed (hours, on the 2026-11-03 run).
"""
import importlib.util
import os
import pathlib
import sys
from uuid import uuid4

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")

import pytest
from sqlalchemy import text

from test_chat_catalogue_postgres import store
from shared import models

pytestmark = pytest.mark.integration

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
_SVC_DIR = _REPO_ROOT / "services" / "job-service"
for path in (_REPO_ROOT, _SVC_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))
_spec = importlib.util.spec_from_file_location("js_main_rollup_under_test", _SVC_DIR / "main.py")
_js = importlib.util.module_from_spec(_spec)
sys.modules["js_main_rollup_under_test"] = _js
_spec.loader.exec_module(_js)


async def test_rollup_items_include_in_progress_snapshot_progress(store):
    scope = await store.new_scope()
    async with store.factory.begin() as session:
        job_id = await session.scalar(text("SELECT job_id FROM snapshots WHERE id=:id"), {"id": scope.snapshot_id})
        await session.execute(text("UPDATE snapshots SET item_count=5 WHERE id=:id"), {"id": scope.snapshot_id})
        session.add(models.Snapshot(id=uuid4(), resource_id=scope.resource_id, job_id=job_id, backend_id=scope.backend_id,
                                    status=models.SnapshotStatus.COMPLETED, item_count=10, bytes_added=100))
    async with store.factory() as session:
        rollup = await _js._job_rollup_bulk(session, [job_id])
    assert rollup[str(job_id)]["items"] == 15
    assert rollup[str(job_id)]["snapshots_in_progress"] == 1
