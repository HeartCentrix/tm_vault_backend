"""An unchanged mailbox's reuse snapshot owns no rows; its data is the full snapshot it names. DR must
not register it as a (metadata-only) replicated copy before that snapshot is replicated."""
from __future__ import annotations

import asyncio
import importlib.util
import os
import pathlib
import sys
import types

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")
_ROOT = pathlib.Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("drw_main_reuse", _ROOT / "services" / "dr-replication-worker" / "main.py")
_drw = importlib.util.module_from_spec(_spec)
sys.modules["drw_main_reuse"] = _drw
try:
    _spec.loader.exec_module(_drw)
except Exception as exc:  # pragma: no cover - import-env guard
    pytest.skip(f"dr-replication-worker module failed to import: {exc}", allow_module_level=True)


class _Session:
    def __init__(self, root):
        self.root = root

    async def get(self, model, key):
        return self.root if self.root is not None and key == self.root.id else None

    async def execute(self, *a, **k):  # pragma: no cover - must not be reached
        raise AssertionError("a reuse snapshot's own rows must not be listed")

    async def flush(self):
        pass


def _pointer():
    return types.SimpleNamespace(id="p", resource_id="r", reuse_chain_root_id="root", extra_data={},
                                 dr_replication_status="pending", dr_replication_attempts=0, dr_blob_path=None,
                                 dr_replicated_at=None, dr_error=None)


_TENANT = types.SimpleNamespace(id="t", dr_region_enabled=True, dr_storage_account_name="dr",
                                dr_storage_account_key_encrypted="enc", extra_data={})


def test_pointer_waits_for_the_snapshot_it_reuses():
    pointer = _pointer()
    root = types.SimpleNamespace(id="root", dr_replication_status="pending", dr_blob_path=None)
    asyncio.run(_drw.replicate_snapshot(pointer, _TENANT, _Session(root)))
    assert (pointer.dr_replication_status, pointer.dr_blob_path, pointer.dr_replication_attempts) == ("pending", None, 0)


def test_pointer_is_replicated_with_the_snapshot_it_reuses():
    pointer = _pointer()
    root = types.SimpleNamespace(id="root", dr_replication_status="replicated", dr_blob_path="dr/root")
    asyncio.run(_drw.replicate_snapshot(pointer, _TENANT, _Session(root)))
    assert (pointer.dr_replication_status, pointer.dr_blob_path) == ("replicated", "dr/root")


def test_chat_pointer_follows_the_backup_it_reuses_not_the_catalogue_copy():
    pointer = _pointer()
    pointer.extra_data = {"chat_inventory_format": 1}
    root = types.SimpleNamespace(id="root", dr_replication_status="replicated", dr_blob_path="dr/root")
    asyncio.run(_drw.replicate_snapshot(pointer, _TENANT, _Session(root)))
    assert (pointer.dr_replication_status, pointer.dr_blob_path) == ("replicated", "dr/root")
