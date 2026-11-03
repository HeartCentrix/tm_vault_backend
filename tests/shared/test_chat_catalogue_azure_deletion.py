"""Catalogue retirement requires an honest Azure deletion acknowledgement."""
from types import SimpleNamespace

import pytest
from azure.core.exceptions import ResourceNotFoundError

from shared.storage.azure_blob import AzureBlobStore


class Shard:
    def __init__(self, error=None):
        self.error = error
        self.calls = []

    def get_async_client(self):
        return self

    def get_blob_client(self, *, container, blob):
        self.calls.append((container, blob))
        return SimpleNamespace(delete_blob=self.strict_delete)

    async def strict_delete(self):
        if self.error:
            raise self.error

    async def delete_blob(self, container, path):
        # Legacy shard helper intentionally swallows storage errors.
        return None


async def test_catalogue_delete_propagates_provider_failure_instead_of_false_success():
    store = AzureBlobStore("backend", "test", [Shard(PermissionError("retention prevents deletion"))])
    with pytest.raises(PermissionError, match="retention"):
        await store.delete("chat-catalogue-tenant", "chat-catalogue/v1/tenant/generation")


async def test_catalogue_delete_is_idempotent_on_actual_missing_object():
    shard = Shard(ResourceNotFoundError("missing"))
    store = AzureBlobStore("backend", "test", [shard])
    await store.delete("chat-catalogue-tenant", "chat-catalogue/v1/tenant/generation")
    assert shard.calls == [("chat-catalogue-tenant", "chat-catalogue/v1/tenant/generation")]
