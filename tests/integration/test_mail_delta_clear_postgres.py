"""A forced full mail backup must clear per-folder delta rows and the legacy token mirror.

Railway 2026-11-04: the clear's UPDATE named a non-existent ``resources.extra_data``
column (the ORM attribute maps to ``metadata``, a json column), so the shared
transaction rolled back and the DELETE of mail_folder_delta rows was undone too."""
import importlib.util
import os
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import func, select

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-mail-delta-clear-secret")
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from test_chat_catalogue_postgres import store  # noqa: E402,F401
from shared import models  # noqa: E402

spec = importlib.util.spec_from_file_location("mail_delta_clear_worker", ROOT / "workers/backup-worker/main.py")
worker = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = worker
spec.loader.exec_module(worker)
pytestmark = pytest.mark.integration


async def test_forced_full_clears_folder_deltas_and_legacy_mirror(store, monkeypatch):
    async with store.factory.begin() as db:
        resource = await db.get(models.Resource, store.resource)
        resource.extra_data = {"mail_delta_tokens_by_folder": {"inbox": "old"}, "keep": 1}
        db.add(models.MailFolderDelta(resource_id=resource.id, folder_id="inbox", delta_token="old"))
    monkeypatch.setattr(worker, "async_session_factory", store.factory)

    removed = await worker.BackupWorker._clear_mail_folder_deltas(SimpleNamespace(worker_id="t"), store.resource)

    assert removed == 1
    async with store.factory() as db:
        assert await db.scalar(select(func.count()).select_from(models.MailFolderDelta)) == 0
        assert (await db.get(models.Resource, store.resource)).extra_data == {"keep": 1}
