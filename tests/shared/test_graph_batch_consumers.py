"""Read-only batch consumers retain proven results and explicitly retry gaps."""
import ast
import asyncio
import os
from pathlib import Path
from typing import Any, Dict, List, Tuple

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")

import pytest

from shared.graph_batch import BatchReadError, BatchResponse
from shared.graph_client import GraphClient
from shared.provider_admission import ProviderDeferred
from shared.provider_runtime import ProviderBudgetExhausted, ProviderTurnDeferred


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(GraphClient, "_unreachable_urls", {})
    return GraphClient.__new__(GraphClient)


async def test_share_resolution_reuses_partial_reads_without_negative_caching_missing_urls(client):
    healthy, denied, missing = [f"https://tenant.sharepoint.com/{name}" for name in ("healthy", "denied", "missing")]
    item = {"id": "item", "size": 3, "@microsoft.graph.downloadUrl": "https://example.invalid/content"}

    async def batch(requests):
        raise BatchReadError("batch_transport", {
            requests[0].id: BatchResponse(requests[0].id, 200, {}, item),
            requests[1].id: BatchResponse(requests[1].id, 404, {}, {}),
        })

    client.batch = batch
    out = await client.resolve_shares_batch([healthy, denied, missing])

    assert out == {healthy: item, denied: None}
    assert client._is_url_unreachable(denied)
    assert not client._is_url_unreachable(healthy)
    assert not client._is_url_unreachable(missing)


@pytest.mark.parametrize("method", ["resolve_shares_batch", "list_file_versions_batch"])
@pytest.mark.parametrize("kind", ["defer", "deadline", "turn_defer", "cancel"])
async def test_partial_read_consumers_preserve_control_flow(client, method, kind):
    failure = {
        "defer": ProviderDeferred(None, 60),
        "deadline": ProviderBudgetExhausted("original deadline exhausted"),
        "turn_defer": ProviderTurnDeferred(ProviderDeferred(None, 60)),
        "cancel": asyncio.CancelledError(),
    }[kind]

    async def batch(requests):
        raise failure

    client.batch = batch
    args = (["https://tenant.sharepoint.com/a"],) if method == "resolve_shares_batch" else ("drive", ["a"])
    with pytest.raises(type(failure)) as raised:
        await getattr(client, method)(*args)
    assert raised.value is failure
    assert client._unreachable_urls == {}


async def test_version_lists_reuse_partial_results_and_mark_missing_for_fallback(client):
    versions = [{"id": "version-1", "size": 3}]

    async def batch(requests):
        raise BatchReadError("response_byte_limit", {
            "0": BatchResponse("0", 200, {}, {"value": versions}),
            "1": BatchResponse("1", 404, {}, {}),
        })

    client.batch = batch
    assert await client.list_file_versions_batch("drive", ["healthy", "deleted", "missing"]) == {
        "healthy": versions, "deleted": [], "missing": None,
    }


@pytest.mark.parametrize("body", [None, {}, {"value": None}, {"value": "invalid"},
    {"value": [{"id": "first"}], "@odata.nextLink": "next"}])
async def test_unproven_version_collection_requires_fallback(client, body):
    async def batch(requests):
        return {"0": BatchResponse("0", 200, {}, body)}

    client.batch = batch
    assert await client.list_file_versions_batch("drive", ["unproven", "missing"]) == {
        "unproven": None, "missing": None,
    }


async def test_confirmed_empty_version_collection_stays_empty(client):
    async def batch(requests):
        return {"0": BatchResponse("0", 200, {}, {"value": []})}

    client.batch = batch
    assert await client.list_file_versions_batch("drive", ["empty"]) == {"empty": []}


async def test_version_coordinator_refetches_only_unresolved_items(client):
    path = Path(__file__).resolve().parents[2] / "workers/backup-worker/main.py"
    tree = ast.parse(path.read_text())
    coordinator = next(node for node in tree.body if isinstance(node, ast.ClassDef)
                       and node.name == "VersionListCoordinator")
    namespace = dict(asyncio=asyncio, List=List, Dict=Dict, Tuple=Tuple, Any=Any)
    exec(compile(ast.Module(body=[coordinator], type_ignores=[]), str(path), "exec"), namespace)
    healthy_versions, fallback_versions = [{"id": "cached-version"}], [{"id": "fallback-version"}]
    fetched = []

    async def batch(requests):
        raise BatchReadError("batch_transport", {
            "0": BatchResponse("0", 200, {}, {"value": healthy_versions}),
            "2": BatchResponse("2", 404, {}, {}),
        })

    async def list_versions(drive_id, item_id):
        fetched.append(item_id)
        return fallback_versions

    client.batch, client.list_file_versions = batch, list_versions
    coord = namespace["VersionListCoordinator"](client, "drive")
    futures = [asyncio.get_running_loop().create_future() for _ in range(3)]
    await coord._flush(list(zip(["healthy", "missing", "deleted"], futures)))

    assert fetched == ["missing"]
    assert [future.result() for future in futures] == [healthy_versions, fallback_versions, []]
