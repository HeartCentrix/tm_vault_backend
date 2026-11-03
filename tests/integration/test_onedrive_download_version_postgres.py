"""Only the version observed around the transfer can become a resume receipt."""
from copy import deepcopy
import asyncio
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from test_chat_catalogue_postgres import store
from test_copy_worker_handoff_postgres import bw
from test_onedrive_resume_postgres import setup_worker, file_item
from shared import models

pytestmark = pytest.mark.integration


def install_metadata(env, source):
    batches = []

    async def urls(drive_id, ids, *, include_source_metadata=False):
        batches.append(list(ids))
        out = {}
        for key in ids:
            data = deepcopy(source[key])
            basic = ("https://download.invalid/" + key, data.get("size", 1024), "")
            out[key] = (*basic, data) if include_source_metadata else basic
        return out

    env.graph.get_download_urls_batch = urls
    return batches


@pytest.mark.parametrize("when", ["before_url", "during_stream"])
async def test_same_size_source_mutation_cannot_publish_download_receipt(store, bw, monkeypatch, when):
    item = file_item()
    env = await setup_worker(store, bw, monkeypatch, [item])
    source = {item["id"]: deepcopy(item)}
    install_metadata(env, source)
    streamed = []
    if when == "before_url":
        source[item["id"]]["eTag"] = "version-2"

    async def stream(**kwargs):
        streamed.append(True)
        source[item["id"]]["eTag"] = "version-2"
        return {"success": True, "content_sha256": "new-version-digest"}

    env.worker._stream_graph_to_backend = stream
    with pytest.raises(bw.EnumerationFailed, match="source.*changed"):
        await env.run()
    assert bool(streamed) == (when == "during_stream")
    async with store.factory() as session:
        row = await session.scalar(select(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == env.scope.snapshot_id))
        assert row.blob_path is None
        assert "onedrive_download_source" not in row.extra_data


@pytest.mark.parametrize("missing_at", ["before", "after"])
async def test_missing_version_captures_bytes_without_trusted_resume_marker(store, bw, monkeypatch, missing_at):
    item = file_item()
    env = await setup_worker(store, bw, monkeypatch, [item])
    source = {item["id"]: deepcopy(item)}
    batches = install_metadata(env, source)
    if missing_at == "before":
        source[item["id"]].pop("eTag")

    async def stream(**kwargs):
        if missing_at == "after":
            source[item["id"]].pop("eTag", None)
        return {"success": True, "content_sha256": "content-digest"}

    env.worker._stream_graph_to_backend = stream
    assert await env.run() == (1, 1024)
    async with store.factory() as session:
        row = await session.scalar(select(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == env.scope.snapshot_id))
        assert row.blob_path
        assert "onedrive_download_source" not in row.extra_data
    assert await env.run() == (1, 1024)
    assert len(batches) == 4


async def test_verification_is_bounded_and_proven_resume_makes_no_graph_reads(store, bw, monkeypatch):
    items = [file_item(str(i)) for i in range(45)]
    env = await setup_worker(store, bw, monkeypatch, items)
    batches = install_metadata(env, {i["id"]: i for i in items})
    assert await env.run() == (45, 45 * 1024)
    assert len(batches) == 6
    assert max(map(len, batches)) <= 20
    async with store.factory() as session:
        rows = (await session.scalars(select(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == env.scope.snapshot_id))).all()
        assert all(row.extra_data["onedrive_download_source"]["eTag"] == "version-1" for row in rows)
    assert await env.run() == (0, 0)
    assert len(batches) == 6


@pytest.mark.parametrize("stage", ["prefetch", "serial", "stream", "verification"])
@pytest.mark.parametrize("kind", ["deferred", "budget", "cancelled", "ownership"])
async def test_download_control_flow_is_never_downgraded_to_missing_proof(store, bw, monkeypatch, stage, kind):
    from shared.provider_admission import ProviderDeferred
    from shared.provider_runtime import ProviderBudgetExhausted
    from shared.provider_work import ProviderWorkConflict
    failure = {"deferred": ProviderDeferred(None, 60), "budget": ProviderBudgetExhausted("deadline"),
               "cancelled": asyncio.CancelledError(), "ownership": ProviderWorkConflict("moved")}[kind]
    item = file_item()
    env = await setup_worker(store, bw, monkeypatch, [item])
    calls = []

    async def urls(drive_id, ids, *, include_source_metadata=False):
        calls.append(list(ids))
        if stage == "prefetch" or (stage == "verification" and len(calls) > 1):
            raise failure
        if stage == "serial":
            return {}
        return {item["id"]: ("https://download.invalid/file", 1024, "", item)}

    env.graph.get_download_urls_batch = urls
    env.graph.get_download_url = AsyncMock(side_effect=failure)
    if stage == "stream":
        env.worker._stream_graph_to_backend = AsyncMock(side_effect=failure)
    expected = bw.ChatCatalogueWorkLeaseMoved if kind == "ownership" else type(failure)
    with pytest.raises(expected) as raised:
        await env.run()
    if kind == "ownership":
        assert raised.value.__cause__ is failure
    elif kind != "cancelled":  # gather represents a cancelled child with a fresh sentinel.
        assert raised.value is failure
    if stage != "serial":
        env.graph.get_download_url.assert_not_awaited()
    async with store.factory() as session:
        row = await session.scalar(select(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == env.scope.snapshot_id))
        assert row.blob_path is None


@pytest.mark.parametrize("failure", ["url_exhausted", "url_runtime", "url_transport", "stream_timeout", "stream_error"])
async def test_required_file_failure_cannot_be_certified_by_retry_enqueue(store, bw, monkeypatch, failure):
    env = await setup_worker(store, bw, monkeypatch, [file_item()])
    env.worker._enqueue_onedrive_retry = AsyncMock()
    if failure.startswith("url_"):
        env.graph.get_download_urls_batch = AsyncMock(return_value={})
        error = {"url_exhausted": bw.GraphRetryExhaustedError("exhausted"),
                 "url_runtime": RuntimeError("unrecognized source failure"),
                 "url_transport": OSError("transport failed")}[failure]
        env.graph.get_download_url = AsyncMock(side_effect=error)
    else:
        error = asyncio.TimeoutError() if failure == "stream_timeout" else OSError("stream failed")
        env.worker._stream_graph_to_backend = AsyncMock(side_effect=error)
    with pytest.raises(bw.EnumerationFailed, match="required file content"):
        await env.run()
    async with store.factory() as session:
        row = await session.scalar(select(models.SnapshotItem).where(models.SnapshotItem.snapshot_id == env.scope.snapshot_id))
        assert row.blob_path is None
