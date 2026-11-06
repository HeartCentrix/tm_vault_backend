"""Chat export over overlay snapshots: estimate and worker scope answer exactly as for the copy twin,
including for chats the overlay inherits from its base (rows owned by the base snapshot)."""
from __future__ import annotations

import importlib.util
from pathlib import Path
import sys

import pytest
from sqlalchemy import text

from test_chat_catalogue_postgres import store  # noqa: F401  (fixture)
from test_chat_catalogue_entry_postgres import source_adapter  # noqa: F401  (fixture)
from test_chat_overlay_postgres import overlay_and_twin

pytestmark = pytest.mark.integration
_ROOT = Path(__file__).resolve().parents[2]
CHATS = ("chat-alpha", "chat-beta", "chat-gamma")   # alpha + gamma inherited by the overlay, beta owned


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, _ROOT / path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def export_api():
    service_path = str(_ROOT / "services/job-service")
    if service_path not in sys.path:
        sys.path.insert(0, service_path)
    return load("overlay_export_api", "services/job-service/chat_export.py")


@pytest.fixture(scope="module")
def export_scope():
    return load("overlay_export_scope", "workers/chat-export-worker/scope.py")


async def selections(store, snapshot_id):
    """[(thread_path, row id of its lowest-keyed message)] per chat, read from the snapshot's effective rows."""
    async with store.factory() as session:
        rows = (await session.execute(text(
            "SELECT DISTINCT ON (folder_path) folder_path, id::text FROM snapshot_items_effective(:s) "
            "WHERE item_type IN ('TEAMS_CHAT_MESSAGE','TEAMS_MESSAGE','TEAMS_MESSAGE_REPLY') "
            "ORDER BY folder_path, external_id"), {"s": snapshot_id})).all()
    return [(path, item) for path, item in rows]


async def resource_of(store, snapshot_id):
    async with store.factory() as session:
        return await session.scalar(text("SELECT resource_id FROM snapshots WHERE id = :s"), {"s": snapshot_id})


def normalised(resolved):
    def descriptors(mapping):
        return sorted((parent, d["name"], d["size"], d.get("sha256")) for parent, ds in mapping.items() for d in ds)
    return (resolved.layout, resolved.thread_path,
            sorted((m.external_id, m.chat_version_id) for m in resolved.messages),
            descriptors(resolved.attachment_map), descriptors(resolved.hosted_map))


async def test_overlay_export_estimate_matches_copy(store, source_adapter, monkeypatch, export_api):
    _, overlay, twin = await overlay_and_twin(store, source_adapter, monkeypatch)
    picks = {sid: await selections(store, sid) for sid in (overlay, twin)}
    assert len(picks[overlay]) == 3 and [p for p, _ in picks[overlay]] == [p for p, _ in picks[twin]]
    resource = str(await resource_of(store, overlay))
    async with store.factory() as db:
        for (path, overlay_item), (_, twin_item) in zip(picks[overlay], picks[twin]):
            got = await export_api._resolve_counts(db, resource_id=resource, snapshot_ids=[str(overlay)],
                thread_path=path, item_ids=[])
            want = await export_api._resolve_counts(db, resource_id=resource, snapshot_ids=[str(twin)],
                thread_path=path, item_ids=[])
            assert got == want and got[0] > 0
            got = await export_api._resolve_counts(db, resource_id=resource, snapshot_ids=[str(overlay)],
                thread_path=None, item_ids=[overlay_item])
            want = await export_api._resolve_counts(db, resource_id=resource, snapshot_ids=[str(twin)],
                thread_path=None, item_ids=[twin_item])
            assert got == want and got[0] == 1
        for chat in CHATS:   # by chat id (chat_message_statement, overlay-aware)
            got = await export_api._resolve_counts(db, resource_id=resource, snapshot_ids=[str(overlay)],
                thread_path=None, item_ids=[], chat_id=chat)
            want = await export_api._resolve_counts(db, resource_id=resource, snapshot_ids=[str(twin)],
                thread_path=None, item_ids=[], chat_id=chat)
            assert got == want and got[0] > 0


async def test_overlay_export_scope_matches_copy(store, source_adapter, monkeypatch, export_scope):
    _, overlay, twin = await overlay_and_twin(store, source_adapter, monkeypatch)
    picks = {sid: await selections(store, sid) for sid in (overlay, twin)}
    resource = await resource_of(store, overlay)
    cards = 0
    async with store.factory() as db:
        for (path, overlay_item), (_, twin_item) in zip(picks[overlay], picks[twin]):
            got = await export_scope.resolve(db, resource_id=resource, snapshot_ids=[str(overlay)],
                thread_path=path, item_ids=[])
            want = await export_scope.resolve(db, resource_id=resource, snapshot_ids=[str(twin)],
                thread_path=path, item_ids=[])
            assert normalised(got) == normalised(want) and got.messages
            assert got.catalogue_snapshot_ids == (overlay,) and want.catalogue_snapshot_ids == (twin,)
            cards += len(got.attachment_map)
            got = await export_scope.resolve(db, resource_id=resource, snapshot_ids=[str(overlay)],
                thread_path=None, item_ids=[overlay_item])
            want = await export_scope.resolve(db, resource_id=resource, snapshot_ids=[str(twin)],
                thread_path=None, item_ids=[twin_item])
            assert normalised(got) == normalised(want) and len(got.messages) == 1
        for chat in CHATS:
            got = await export_scope.resolve(db, resource_id=resource, snapshot_ids=[str(overlay)],
                thread_path=None, item_ids=[], chat_id=chat)
            want = await export_scope.resolve(db, resource_id=resource, snapshot_ids=[str(twin)],
                thread_path=None, item_ids=[], chat_id=chat)
            assert normalised(got) == normalised(want) and got.messages
    assert cards == 1   # chat-alpha's adaptive card, inherited from the base
