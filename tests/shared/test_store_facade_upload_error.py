"""Upload failures must keep the exception class; ``str(ConnectTimeout(''))`` is empty."""
import os

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

import httpx

from shared.azure_storage import _StoreFacade


class _FailingStore:
    name, backend_id, kind = "seaweed", None, "s3"

    async def upload_stream(self, *args, **kwargs):
        raise httpx.ConnectTimeout("")


async def test_upload_blob_stream_error_names_the_exception_class():
    async def chunks():
        yield b"x"
    result = await _StoreFacade(_FailingStore()).upload_blob_stream("bucket", "path", chunks(), 1)
    assert result["success"] is False
    assert result["error"].startswith("ConnectTimeout")
