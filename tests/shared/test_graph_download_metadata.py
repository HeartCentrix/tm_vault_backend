"""Download descriptors preserve observed version without changing tuple callers."""
from types import SimpleNamespace
from unittest.mock import AsyncMock
import os

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")

from shared.graph_client import GraphClient


@pytest.mark.parametrize("batched", [False, True])
async def test_download_metadata_is_opt_in_and_excludes_signed_url(batched):
    item = {"id": "file", "name": "file.txt", "size": 10, "eTag": "v1", "cTag": "c1",
            "parentReference": {"driveId": "drive"}, "file": {"hashes": {"quickXorHash": "hash"}},
            "@microsoft.graph.downloadUrl": "https://download.invalid/signed"}
    graph = GraphClient.__new__(GraphClient)
    graph._get = AsyncMock(return_value=item)
    graph.batch = AsyncMock(return_value={"file": SimpleNamespace(status=200, body=item)})

    async def call(**kwargs):
        if batched:
            return (await graph.get_download_urls_batch("drive", ["file"], **kwargs))["file"]
        return await graph.get_download_url("drive", "file", **kwargs)

    assert await call() == (item["@microsoft.graph.downloadUrl"], 10, "hash")
    descriptor = await call(include_source_metadata=True)
    assert descriptor[:3] == await call()
    assert descriptor[3] == {key: value for key, value in item.items() if not key.startswith("@")}


@pytest.mark.parametrize("kind", ["defer", "budget", "ownership"])
@pytest.mark.parametrize("batched", [False, True])
async def test_url_resolution_preserves_provider_control_flow(kind, batched):
    from shared.provider_admission import ProviderDeferred
    from shared.provider_runtime import ProviderBudgetExhausted
    from shared.provider_work import ProviderWorkConflict
    failure = {"defer": ProviderDeferred(None, 60), "budget": ProviderBudgetExhausted("deadline"),
               "ownership": ProviderWorkConflict("moved")}[kind]
    graph = GraphClient.__new__(GraphClient)
    graph._get = AsyncMock(side_effect=failure)
    graph.batch = AsyncMock(side_effect=failure)
    with pytest.raises(type(failure)) as raised:
        if batched:
            await graph.get_download_urls_batch("drive", ["file"], include_source_metadata=True)
        else:
            await graph.get_download_url("drive", "file", include_source_metadata=True)
    assert raised.value is failure
    assert (graph.batch if batched else graph._get).await_count == 1
