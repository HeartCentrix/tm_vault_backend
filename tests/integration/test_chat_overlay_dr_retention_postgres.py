"""Chat overlays through DR archives, search reindex and retention heirs: each must see the
overlay's effective inventory (its own changed chats + the base's unchanged chats), exactly as
the copy-written twin of the same backup."""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import select, text, update

from test_chat_catalogue_postgres import store  # noqa: F401  (fixture)
from test_chat_catalogue_entry_postgres import source_adapter  # noqa: F401  (fixture)
from test_chat_overlay_postgres import overlay_and_twin
from shared import models

pytestmark = pytest.mark.integration

_dr = importlib.util.spec_from_file_location(
    "chat_overlay_dr_harness", Path(__file__).resolve().parents[1] / "services/test_chat_catalogue_dr.py")
dr_harness = importlib.util.module_from_spec(_dr)
_dr.loader.exec_module(dr_harness)


class NoSource:
    def get_store_by_id(self, backend):
        raise AssertionError("fixture chats hold no blob objects")


async def archive(store, snapshot_id, target):
    from shared.chat_catalogue_dr import replicate_catalogue_snapshot
    return await replicate_catalogue_snapshot(session_factory=store.factory, snapshot_id=snapshot_id,
        tenant_id=store.tenant, source_router=NoSource(), target=target, target_container="synthetic-dr",
        encryption=target.encryption)


def records(target, result):
    """Manifest records with snapshot/link identities and placement normalised away."""
    out = []
    for line in target.local(result.container, result.path).read_bytes().splitlines():
        record = json.loads(line)
        kind, data = record["type"], record.get("data")
        if kind in {"seal", "snapshot"}:
            continue
        data = {k: v for k, v in data.items() if not (kind == "link" and k in {"id", "snapshot_id"})
                and not (kind == "association" and k == "snapshot_id") and not (kind == "replica" and k == "path")}
        out.append(json.dumps([kind, data], sort_keys=True))
    return sorted(out)


async def test_dr_archive_of_an_overlay_equals_its_copy_twin_and_recovers(store, source_adapter, monkeypatch, tmp_path):
    from shared.chat_catalogue_dr import load_catalogue_dr_manifest
    base, overlay, twin = await overlay_and_twin(store, source_adapter, monkeypatch)
    target = dr_harness.DiskTarget(tmp_path)
    got, want = await archive(store, overlay, target), await archive(store, twin, target)
    assert got.item_count == want.item_count == 8   # alpha 1 + card, beta 3, gamma 3
    assert records(target, got) == records(target, want)
    async with load_catalogue_dr_manifest(target=target, container=got.container, path=got.path,
            expected_sha256=got.sha256, tenant_id=store.tenant, snapshot_id=overlay) as recovered:
        items = [item async for item in recovered.iter_items()]
    assert len(items) == 8 and {item.snapshot_id for item in items} == {overlay}


async def test_search_reindex_of_an_overlay_indexes_inherited_chats(store, source_adapter, monkeypatch):
    spec = importlib.util.spec_from_file_location(
        "chat_overlay_search_service", Path(__file__).resolve().parents[2] / "services/search-service/main.py")
    search = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(search)
    base, overlay, twin = await overlay_and_twin(store, source_adapter, monkeypatch)
    async with store.factory() as session:
        result = await search.reindex_snapshots(snapshot_ids=[str(overlay)], limit=1000, db=session)
    assert result["indexed"] == 8   # the test schema has no search_vector column; the selection is the point


async def state(store, snapshot_id):
    async with store.factory() as session:
        rows = sorted((await session.execute(text(
            "SELECT external_id, item_type, chat_version_id FROM snapshot_items_effective(:s)"), {"s": snapshot_id})).all())
        sci = sorted((await session.execute(select(models.SnapshotChatInventory.chat_id, models.SnapshotChatInventory.inventory_id)
            .where(models.SnapshotChatInventory.snapshot_id == snapshot_id))).all())
        return rows, sci


async def retire(store, doomed):
    from shared.retention_cleanup import _delete_snapshots
    async with store.factory.begin() as session:
        assert await _delete_snapshots(session, set(doomed)) == len(doomed)


async def counts(store, snapshot_id):
    async with store.factory() as session:
        snap = await session.get(models.Snapshot, snapshot_id)
        return snap.item_count, snap.bytes_total, snap.overlay_base_id, (snap.extra_data or {}).get("chat_inventory_format")


async def twin_counts(store, twin):
    from shared.chat_catalogue_entry import catalogue_snapshot_result
    async with store.factory() as session:
        result = await catalogue_snapshot_result(session, twin, force_recount=True)
    return result["item_count"], result["bytes_total"]


async def test_retiring_the_base_materialises_the_chat_overlay_with_its_inventories(store, source_adapter, monkeypatch):
    base, overlay, twin = await overlay_and_twin(store, source_adapter, monkeypatch)
    before = await state(store, overlay)
    assert len(before[1]) == 3
    await retire(store, [base])
    assert await state(store, overlay) == before
    item_count, bytes_total, base_id, fmt = await counts(store, overlay)
    assert base_id is None and fmt == 1
    assert (item_count, bytes_total) == await twin_counts(store, twin) and item_count == 7


async def test_retiring_a_chat_overlay_hands_it_to_its_pointer_with_inventories(store, source_adapter, monkeypatch):
    from shared.snapshot_reuse import point_at
    base, overlay, twin = await overlay_and_twin(store, source_adapter, monkeypatch)
    pointer, job = uuid4(), uuid4()
    async with store.factory.begin() as session:
        source = await session.get(models.Snapshot, overlay)
        session.add(models.Job(id=job, type=models.JobType.BACKUP, tenant_id=store.tenant,
                               resource_id=source.resource_id, status=models.JobStatus.COMPLETED))
        await session.flush()
        session.add(models.Snapshot(id=pointer, resource_id=source.resource_id, job_id=job, backend_id=source.backend_id,
                                    status=models.SnapshotStatus.COMPLETED, type=source.type,
                                    extra_data={"chat_inventory_format": 1}))
        await session.flush()
        assert await point_at(session, target_snapshot_id=pointer, prior_snapshot_id=overlay, marker="chat_reuse_of")
        await session.execute(text(
            "INSERT INTO snapshot_chat_inventories (snapshot_id, chat_id, tenant_id, inventory_id, capture_attempt_id, "
            "coverage_status, started_at, finished_at, failure_reason, uploaded_bytes) SELECT :p, chat_id, tenant_id, "
            "inventory_id, capture_attempt_id, coverage_status, started_at, finished_at, failure_reason, 0 "
            "FROM snapshot_chat_inventories WHERE snapshot_id = :o"), {"p": pointer, "o": overlay})
    before = await state(store, overlay)
    await retire(store, [overlay])
    assert await state(store, pointer) == before
    item_count, bytes_total, base_id, fmt = await counts(store, pointer)
    assert base_id == base and fmt == 1
    assert (item_count, bytes_total) == await twin_counts(store, twin) and item_count == 7
