"""SharePoint lists whose items did not change are carried forward instead of re-read.

Railway 2026-11-05: every incremental re-read every list of 19 sites through SP REST (5,760 rows,
one commit each) although the list collection already returns each list's LastItemModifiedDate."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import os
from pathlib import Path
import sys
from uuid import uuid4

import pytest
from sqlalchemy import func, select, text

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
from test_chat_catalogue_postgres import store  # noqa: F401  (disposable schema fixture)
from shared import models

pytestmark = pytest.mark.integration
NOW = datetime(2026, 11, 6, 12, tzinfo=timezone.utc)
SIG = {"modified": "2026-11-01T10:00:00Z", "deleted": "2026-10-01T10:00:00Z", "count": 3, "title": "Tasks"}


def _worker():
    name = "bw_sp_list_reuse"
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(name, Path(__file__).parents[2] / "workers" / "backup-worker" / "main.py")
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
    return sys.modules[name]


def test_list_is_reused_only_when_its_items_and_title_are_unchanged_and_a_full_read_is_recent():
    reusable = _worker().sp_list_reusable
    walked = (NOW - timedelta(days=2)).isoformat()
    prior = {"rest_signature": SIG, "rest_walked_at": walked}
    assert reusable(SIG, prior, NOW) == walked
    assert reusable({**SIG, "deleted": "2026-11-05T00:00:00Z"}, prior, NOW) is None   # an item was deleted
    assert reusable({**SIG, "count": 4}, prior, NOW) is None
    assert reusable({**SIG, "title": "Renamed"}, prior, NOW) is None
    assert reusable(SIG, {**prior, "rest_walked_at": (NOW - timedelta(days=8)).isoformat()}, NOW) is None
    assert reusable(None, prior, NOW) is None and reusable(SIG, None, NOW) is None
    assert reusable({**SIG, "modified": None}, {**prior, "rest_signature": {**SIG, "modified": None}}, NOW) is None


async def test_reused_list_rows_are_copied_forward_in_one_statement(store, monkeypatch):
    worker_module = _worker()
    monkeypatch.setattr(worker_module, "async_session_factory", store.factory)
    worker = worker_module.BackupWorker.__new__(worker_module.BackupWorker)
    prior, current = await store.new_scope(), await store.new_scope()
    async with store.factory.begin() as db:
        for list_id, n in (("list-a", 3), ("list-b", 2)):
            for i in range(n):
                db.add(models.SnapshotItem(snapshot_id=prior.snapshot_id, tenant_id=store.tenant, backend_id=store.backend,
                    external_id=f"{list_id}:{i}", item_type="SHAREPOINT_LIST_ITEM", name=f"row {i}", folder_path="site/Tasks",
                    content_size=5, extra_data={"source": "sp_rest", "list_id": list_id, "site_id": "site-1"}))
    copied = await worker._copy_forward_sp_list_rows(current.snapshot_id, prior.snapshot_id, "site-1", "list-a")
    assert copied == 3
    async with store.factory() as db:
        rows = (await db.execute(select(models.SnapshotItem.external_id, models.SnapshotItem.content_size)
                .where(models.SnapshotItem.snapshot_id == current.snapshot_id).order_by(models.SnapshotItem.external_id))).all()
    assert rows == [("list-a:0", 5), ("list-a:1", 5), ("list-a:2", 5)]


def test_a_list_whose_only_change_is_its_modified_date_reads_rows_changed_since_then():
    # Railway 2026-11-06: SharePoint moved Site Assets' LastItemModifiedDate on 6 sites in one second
    # (count and deleted date unchanged) and each site re-read the whole library.
    changed_since = _worker().sp_list_changed_since
    walked = (NOW - timedelta(days=2)).isoformat()
    prior = {"rest_signature": SIG, "rest_walked_at": walked}
    touched = {**SIG, "modified": "2026-11-06T08:04:35Z"}
    assert changed_since(touched, prior, NOW) == (walked, "2026-11-01T09:59:00Z")   # 1 min margin
    assert changed_since(SIG, prior, NOW) is None                                   # unchanged: plain carry
    assert changed_since({**touched, "count": 4}, prior, NOW) is None               # added: full read
    assert changed_since({**touched, "deleted": "2026-11-05T00:00:00Z"}, prior, NOW) is None
    assert changed_since({**touched, "title": "Renamed"}, prior, NOW) is None
    assert changed_since(touched, {**prior, "rest_walked_at": (NOW - timedelta(days=8)).isoformat()}, NOW) is None
    assert changed_since(touched, {**prior, "rest_signature": {**SIG, "modified": None}}, NOW) is None
    assert changed_since(touched, None, NOW) is None and changed_since(None, prior, NOW) is None


async def test_rows_read_again_are_left_out_of_the_carry_forward(store, monkeypatch):
    worker_module = _worker()
    monkeypatch.setattr(worker_module, "async_session_factory", store.factory)
    worker = worker_module.BackupWorker.__new__(worker_module.BackupWorker)
    prior, current = await store.new_scope(), await store.new_scope()
    async with store.factory.begin() as db:
        for i in range(3):
            db.add(models.SnapshotItem(snapshot_id=prior.snapshot_id, tenant_id=store.tenant, backend_id=store.backend,
                external_id=f"list-a:{i}", item_type="SHAREPOINT_FILE", name=f"row {i}", folder_path="site/Assets",
                content_size=5, extra_data={"source": "sp_rest", "list_id": "list-a", "site_id": "site-1"}))
    copied = await worker._copy_forward_sp_list_rows(current.snapshot_id, prior.snapshot_id, "site-1", "list-a",
                                                     skip_external_ids=["list-a:1"])
    assert copied == 2
    async with store.factory() as db:
        rows = (await db.execute(select(models.SnapshotItem.external_id)
                .where(models.SnapshotItem.snapshot_id == current.snapshot_id).order_by(models.SnapshotItem.external_id))).scalars().all()
    assert rows == ["list-a:0", "list-a:2"]
