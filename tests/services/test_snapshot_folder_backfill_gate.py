"""The snapshot-service folder_path backfill (three unbatched full-table
UPDATEs on snapshot_items) must NOT run on every start by default — at
customer scale (1.45 TB snapshot_items) it stalls boot. Opt in with
SNAPSHOT_FOLDER_PATH_BACKFILL_ON_START=true.
"""
from __future__ import annotations

import asyncio
import importlib.util
import os
import pathlib
import sys

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")

_MAIN_PATH = (
    pathlib.Path(__file__).resolve().parents[2]
    / "services" / "snapshot-service" / "main.py"
)
_spec = importlib.util.spec_from_file_location("snap_main_backfill_gate", _MAIN_PATH)
_m = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["snap_main_backfill_gate"] = _m
try:
    _spec.loader.exec_module(_m)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover
    pytest.skip(f"snapshot-service module failed to import: {exc}", allow_module_level=True)


class _Session:
    def __init__(self, log):
        self.log = log

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def execute(self, stmt):
        self.log.append(str(stmt))

    async def commit(self):
        pass


@pytest.fixture
def executed(monkeypatch):
    log: list[str] = []
    import shared.database as db
    monkeypatch.setattr(db, "async_session_factory", lambda: _Session(log))
    return log


def test_default_is_off():
    from shared.config import Settings
    os.environ.pop("SNAPSHOT_FOLDER_PATH_BACKFILL_ON_START", None)
    assert Settings().snapshot_folder_path_backfill_on_start is False


def test_default_skips_backfill(monkeypatch, executed):
    monkeypatch.setattr(_m.settings, "snapshot_folder_path_backfill_on_start", False)
    asyncio.run(_m._maybe_backfill_folder_paths())
    assert executed == []


def test_enabled_runs_backfill(monkeypatch, executed):
    monkeypatch.setattr(_m.settings, "snapshot_folder_path_backfill_on_start", True)
    asyncio.run(_m._maybe_backfill_folder_paths())
    assert len(executed) == 3
    assert all("UPDATE snapshot_items" in s for s in executed)
