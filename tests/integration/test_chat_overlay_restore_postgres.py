"""Restore-worker selections of a chat overlay snapshot equal those of the same backup written as a
full copy: inherited chats (the base's rows) restore, count and keep their children as the overlay's own."""
from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
from uuid import uuid4

import pytest
from sqlalchemy import text

from test_chat_catalogue_postgres import store  # noqa: F401  (fixture)
from test_chat_catalogue_entry_postgres import source_adapter  # noqa: F401  (fixture)
from test_chat_overlay_postgres import overlay_and_twin

pytestmark = pytest.mark.integration

_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def restore_api():
    path = _ROOT / "workers/restore-worker/main.py"
    sys.path.insert(0, str(path.parent))
    spec = importlib.util.spec_from_file_location("chat_overlay_restore", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def worker(store, restore_api, monkeypatch):
    monkeypatch.setattr(restore_api, "async_session_factory", store.factory)
    instance = restore_api.RestoreWorker.__new__(restore_api.RestoreWorker)
    instance.worker_id = "chat-overlay-restore-test"
    return instance


def norm(items, sid):
    assert {i.snapshot_id for i in items} == {sid}
    return sorted((i.item_type, i.external_id, i.chat_version_id) for i in items)


async def row_id(store, sid, external_id):
    async with store.factory() as session:
        return str((await session.execute(text(
            "SELECT id FROM snapshot_items_effective(:s) WHERE external_id = :e"),
            {"s": sid, "e": external_id})).scalar_one())


async def test_overlay_restore_fetch_matches_copy(store, source_adapter, monkeypatch, worker):
    _, overlay, twin = await overlay_and_twin(store, source_adapter, monkeypatch)
    seen = []
    for sid in (overlay, twin):
        async with store.factory() as session:
            seen.append(norm(await worker.fetch_snapshot_items(session, [str(sid)], []), sid))
    assert seen[0] == seen[1] and len(seen[0]) == 8   # alpha 1 + card, beta 3, gamma 3


async def test_overlay_restore_selected_inherited_message_keeps_its_children(store, source_adapter, monkeypatch, worker):
    _, overlay, twin = await overlay_and_twin(store, source_adapter, monkeypatch)
    async with store.factory() as session:
        alpha = (await session.execute(text(
            "SELECT external_id FROM snapshot_items_effective(:s) WHERE item_type = 'TEAMS_CHAT_MESSAGE' "
            "AND metadata->>'chat_id' = 'chat-alpha'"), {"s": overlay})).scalar_one()
    seen = []
    for sid in (overlay, twin):
        picked = await row_id(store, sid, alpha)
        # Plain id lookup, and the snapshot-scoped path an exclusion list routes through.
        for kwargs in ({}, {"excluded_item_ids": [str(uuid4())]}):
            async with store.factory() as session:
                seen.append(norm(await worker.fetch_snapshot_items(session, [str(sid)], [picked], **kwargs), sid))
    assert seen[0] == seen[2] and seen[1] == seen[3]
    assert len(seen[0]) == 2 and {t for t, _, _ in seen[0]} == {"TEAMS_CHAT_MESSAGE", "CHAT_ATTACHMENT"}


async def test_overlay_restore_count_matches_copy(store, source_adapter, monkeypatch, worker):
    _, overlay, twin = await overlay_and_twin(store, source_adapter, monkeypatch)
    counts = []
    for sid in (overlay, twin):
        async with store.factory() as session:
            counts.append(await worker.count_snapshot_items(session, [str(sid)], []))
    assert counts[0] == counts[1] == 8
