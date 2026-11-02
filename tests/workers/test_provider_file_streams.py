"""Execute production worker download methods against scripted HTTP streams."""
import ast
import asyncio
import hashlib
import logging
import os
os.environ.setdefault('ALLOW_DEV_JWT_SECRETS', 'true')
from pathlib import Path
import tempfile
from types import MethodType, SimpleNamespace
from typing import Any, Dict, Optional

import httpx
import pytest
from shared.provider_read import ProviderReadError


@pytest.fixture
def worker(monkeypatch):
    path = Path(__file__).resolve().parents[2] / 'workers/backup-worker/main.py'
    tree = ast.parse(path.read_text())
    names = {'_stream_graph_to_backend', '_parallel_range_stream_to_backend', '_download_to_temp_resumable'}
    methods = [n for n in ast.walk(tree) if isinstance(n, ast.AsyncFunctionDef) and n.name in names]
    namespace = dict(asyncio=asyncio, hashlib=hashlib, os=os, tempfile=tempfile, httpx=httpx,
        Dict=Dict, Any=Any, Optional=Optional, GraphClient=object)
    exec(compile(ast.Module(body=methods, type_ignores=[]), str(path), 'exec'), namespace)
    result = SimpleNamespace(worker_id='synthetic')
    for name in names:
        setattr(result, name, MethodType(namespace[name], result))
    from shared import provider_runtime
    monkeypatch.setattr(provider_runtime, '_budget', provider_runtime.ExecutionBudget(resident_bytes=16 * 1024 * 1024))
    monkeypatch.setenv('PROVIDER_SCOPED_ADMISSION_ENABLED', 'false')
    real = httpx.AsyncClient
    def transport(handler):
        monkeypatch.setattr(httpx, 'AsyncClient', lambda **kwargs: real(transport=httpx.MockTransport(handler), **kwargs))
    return result, transport


class Interrupted(httpx.AsyncByteStream):
    async def __aiter__(self):
        yield b'abc'
        raise httpx.ReadError('synthetic interruption')


@pytest.mark.parametrize('changed', [False, True])
async def test_resume_requires_exact_range_and_unchanged_http_version(worker, changed, monkeypatch):
    obj, transport = worker
    requests, written = [], []
    async def handler(request):
        requests.append(request)
        if len(requests) == 1:
            return httpx.Response(200, headers={'ETag': '"v1"'}, stream=Interrupted())
        assert request.headers['Range'] == 'bytes=3-'
        assert request.headers['If-Match'] == '"v1"'
        return httpx.Response(206, headers={'ETag': '"v2"' if changed else '"v1"', 'Content-Range': 'bytes 3-5/6'}, content=b'def')
    transport(handler)
    async def no_sleep(*args): pass
    monkeypatch.setattr(asyncio, 'sleep', no_sleep)
    class Storage:
        async def upload_blob_stream(self, container, path, stream, total, **kwargs):
            async for block in stream:
                written.append(block)
            return {'success': True, 'body': b''.join(written)}
    call = obj._stream_graph_to_backend('https://tenant.sharepoint.com/file', 6, 'container', 'path', Storage(), 'name', {}, chunk_size=1)
    if changed:
        with pytest.raises(ProviderReadError, match='version_changed'):
            await call
        assert b''.join(written) == b'abc'
    else:
        assert (await call)['body'] == b'abcdef'


async def test_file_spool_rejects_extra_bytes_and_cleans_on_cancel(worker, monkeypatch, tmp_path):
    obj, transport = worker
    monkeypatch.setattr(tempfile, 'tempdir', str(tmp_path))
    async def oversized(request):
        return httpx.Response(200, content=b'too many bytes')
    transport(oversized)
    with pytest.raises(ProviderReadError, match='source_length_changed|length_exceeded'):
        await obj._download_to_temp_resumable('https://tenant.sharepoint.com/file', 3, 'name')
    assert not list(tmp_path.iterdir())


async def test_unknown_length_interrupted_file_never_reports_success(worker, monkeypatch, tmp_path):
    obj, transport = worker
    monkeypatch.setattr(tempfile, 'tempdir', str(tmp_path))
    transport(lambda request: httpx.Response(200, stream=Interrupted()))
    async def no_sleep(*a): pass
    monkeypatch.setattr(asyncio, 'sleep', no_sleep)
    with pytest.raises(RuntimeError):
        await obj._download_to_temp_resumable('https://tenant.sharepoint.com/file', 0, 'name', max_attempts=1, chunk_size=1)
    assert not list(tmp_path.iterdir())


async def test_file_spool_accounts_bytes_and_preserves_disk_floor(worker, monkeypatch, tmp_path):
    import shutil
    from shared.provider_runtime import execution_budget
    obj, transport = worker
    monkeypatch.setattr(tempfile, 'tempdir', str(tmp_path))
    async def handler(request):
        assert execution_budget().used_bytes > 0
        return httpx.Response(200, content=b'abc')
    transport(handler)
    monkeypatch.setattr(shutil, 'disk_usage', lambda directory: SimpleNamespace(free=1024))
    with pytest.raises(ProviderReadError, match='spool_capacity'):
        await obj._download_to_temp_resumable('https://tenant.sharepoint.com/file', 3, 'name')
    assert execution_budget().used_bytes == 0
    assert not list(tmp_path.iterdir())


async def test_cancelled_file_spool_cleans_private_file(worker, monkeypatch, tmp_path):
    obj, transport = worker
    monkeypatch.setattr(tempfile, 'tempdir', str(tmp_path))
    class Cancelled(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b'abc'
            raise asyncio.CancelledError
    async def cancelled(request):
        return httpx.Response(200, stream=Cancelled())
    transport(cancelled)
    with pytest.raises(asyncio.CancelledError):
        await obj._download_to_temp_resumable('https://tenant.sharepoint.com/file', 6, 'name', chunk_size=1)
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize('kind', ['bad_range', 'wrong_length', 'interrupted_without_length'])
async def test_stale_metadata_or_bad_response_cannot_publish_a_truncated_file(worker, kind, monkeypatch):
    obj, transport = worker
    async def handler(request):
        if kind == 'bad_range':
            return httpx.Response(206, headers={'ETag': '"v1"', 'Content-Range': 'bytes 0-1/3'}, content=b'abc')
        headers = {'Content-Length': '6'} if kind == 'wrong_length' else {}
        return httpx.Response(200, headers=headers, stream=Interrupted())
    transport(handler)
    class Storage:
        async def upload_blob_stream(self, container, path, stream, total, **kwargs):
            async for block in stream: pass
            pytest.fail('a malformed or interrupted body must not finish publication')
    with pytest.raises(ProviderReadError):
        await obj._stream_graph_to_backend('https://tenant.sharepoint.com/file', 3,
            'container', 'path', Storage(), 'name', {}, chunk_size=1)
