"""File-backed parts preserve size limits without keeping whole parts on heap."""
import asyncio
import hashlib
import os
from contextlib import asynccontextmanager
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
import pytest
from shared import provider_runtime
from shared.provider_runtime import ExecutionBudget
from shared.storage.seaweedfs import SeaweedStore
from shared.azure_storage import AzureStorageShard

MIB = 1024 * 1024

async def drain(body):
    digest, total, peak = hashlib.sha256(), 0, 0
    while block := body.read(64 * 1024):
        digest.update(block)
        total += len(block)
        peak = max(peak, len(block))
        await asyncio.sleep(0)
    return total, digest.hexdigest(), peak

async def test_seekable_file_body_is_bounded_replayable_and_exact(tmp_path):
    from shared.storage.file_upload import FileBody
    path = tmp_path / 'body'
    path.write_bytes(b'a' * MIB + b'b' * MIB)
    with path.open('rb') as source:
        async with FileBody(source, MIB // 2, MIB) as body:
            assert body.seek(0, os.SEEK_END) == MIB
            assert body.seek(0) == 0
            total, digest, peak = await drain(body)
            assert total == MIB and digest == hashlib.sha256(b'a' * (MIB//2) + b'b' * (MIB//2)).hexdigest()
            assert peak <= MIB
            body.seek(0)
            assert len(body.read()) <= MIB

async def test_owned_file_parts_join_cancel_before_releasing_files(tmp_path):
    from shared.storage.file_upload import send_file_parts
    path = tmp_path / 'parts'
    path.write_bytes(b'a' * (8 * MIB))
    entered, gate = asyncio.Event(), asyncio.Event()
    active, peak, bodies = 0, 0, []
    async def send(number, body, length):
        nonlocal active, peak
        bodies.append(body)
        active += 1
        peak = max(peak, active)
        entered.set()
        try:
            await gate.wait()
            await drain(body)
            return number
        finally:
            active -= 1
    task = asyncio.create_task(send_file_parts(str(path), 8*MIB, part_size=MIB, concurrency=2, send=send))
    await entered.wait()
    await asyncio.sleep(.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert peak == 2 and active == 0 and all(body.closed for body in bodies)


def test_part_sizing_retains_large_object_support():
    from shared.storage.file_upload import part_size_for
    for size in (250*1024**3, 5*1024**4):
        part = part_size_for(size, max_parts=10000, minimum=8*MIB, maximum=5*1024**3)
        assert (size + part - 1) // part <= 10000 and part > 8*MIB

async def test_azure_file_upload_uses_owned_seekable_parts_and_budget(tmp_path, monkeypatch):
    path = tmp_path / 'azure'
    path.write_bytes(b'a' * (3*MIB))
    budget = ExecutionBudget(resident_bytes=16*MIB)
    monkeypatch.setattr(provider_runtime, '_budget', budget)
    calls = []
    async def stage(block_id, data, length):
        assert 0 < budget.used_bytes <= budget.resident_bytes
        size, digest, peak = await drain(data)
        assert size == length and peak <= MIB
        calls.append(block_id)
    blob = SimpleNamespace(url='https://local/blob', stage_block=stage, commit_block_list=AsyncMock())
    shard = AzureStorageShard('synthetic', 'synthetic')
    monkeypatch.setattr(shard, '_ensure_container', AsyncMock())
    monkeypatch.setattr(shard, 'get_async_client', lambda: SimpleNamespace(get_blob_client=lambda **kwargs: blob))
    result = await shard.upload_blob_from_file('c','p',str(path),file_size=path.stat().st_size, overwrite=False)
    assert result['success'] and budget.used_bytes == 0
    assert blob.commit_block_list.call_args.args[0] == calls
    assert blob.commit_block_list.call_args.kwargs['if_none_match'] == '*'

async def test_seaweed_file_upload_cancellation_aborts_after_children_join(tmp_path, monkeypatch):
    path = tmp_path / 's3'
    path.write_bytes(b'a' * (17*MIB))
    budget = ExecutionBudget(resident_bytes=16*MIB)
    monkeypatch.setattr(provider_runtime, '_budget', budget)
    from shared.crypto import runtime
    monkeypatch.setattr(runtime, 'is_write_enabled', lambda: False)
    started, gate = asyncio.Event(), asyncio.Event()
    active, aborted = 0, False
    async def send(**kwargs):
        nonlocal active
        assert budget.used_bytes > 0
        active += 1
        started.set()
        try:
            await gate.wait()
            await drain(kwargs['Body'])
            return {'ETag': 'part'}
        finally:
            active -= 1
    async def abort(**kwargs):
        nonlocal aborted
        assert active == 0
        aborted = True
    s3 = SimpleNamespace(create_multipart_upload=AsyncMock(return_value={'UploadId':'id'}),
        upload_part=send, abort_multipart_upload=abort, complete_multipart_upload=AsyncMock())
    @asynccontextmanager
    async def client(): yield s3
    store = SeaweedStore('id','test','http://local','key','secret',['bucket'])
    monkeypatch.setattr(store, '_client_ctx', client)
    task = asyncio.create_task(store.upload_from_file('c','p',str(path),path.stat().st_size))
    try:
        await asyncio.wait_for(started.wait(), 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError): await task
        assert aborted and budget.used_bytes == 0
        s3.complete_multipart_upload.assert_not_called()
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

@pytest.mark.parametrize('backend', ['s3', 'azure'])
async def test_installed_sdk_streams_seekable_parts_to_local_http_server(backend, tmp_path, monkeypatch):
    from aiohttp import web
    import base64
    from shared.crypto import runtime
    monkeypatch.setattr(runtime, 'is_write_enabled', lambda: False)
    path = tmp_path / 'sdk-file'
    with path.open('wb') as file:
        for _ in range(10): file.write(b'x' * MIB)
    bodies = []
    async def serve(request):
        size, digest = 0, hashlib.sha256()
        async for piece in request.content.iter_chunked(65536):
            size += len(piece)
            digest.update(piece)
        if request.method == 'HEAD':
            return web.Response(headers={'Content-Length': str(10*MIB), 'ETag': '"final"', 'Last-Modified': 'Mon, 02 Nov 2026 00:00:00 GMT'})
        if request.method == 'PUT' and ('partNumber' in request.query or request.query.get('comp') == 'block'):
            bodies.append((size, digest.hexdigest()))
        if request.method == 'POST' and 'uploads' in request.query:
            return web.Response(text='<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>key</Key><UploadId>id</UploadId></InitiateMultipartUploadResult>', content_type='application/xml')
        if request.method == 'POST':
            return web.Response(text='<CompleteMultipartUploadResult><Location>local</Location><Bucket>bucket</Bucket><Key>key</Key><ETag>"final"</ETag></CompleteMultipartUploadResult>', content_type='application/xml')
        return web.Response(status=201 if backend == 'azure' else 200, headers={'ETag':'"part"','Last-Modified':'Mon, 02 Nov 2026 00:00:00 GMT'})
    app = web.Application(client_max_size=32*MIB)
    app.router.add_route('*','/{tail:.*}',serve)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner,'127.0.0.1',0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    endpoint = f'http://127.0.0.1:{port}'
    try:
        if backend == 's3':
            import aioboto3
            store = SeaweedStore('id','test',endpoint,'key','secret',['bucket'])
            monkeypatch.setattr(store, '_client_ctx', lambda: aioboto3.Session().client('s3', endpoint_url=endpoint,
                aws_access_key_id='synthetic',aws_secret_access_key='synthetic',region_name='us-east-1'))
            result = await store.upload_from_file('container','key',str(path),10*MIB)
            assert result.size == 10*MIB
        else:
            from azure.storage.blob.aio import BlobServiceClient
            async with BlobServiceClient(account_url=endpoint+'/account',credential=base64.b64encode(b'synthetic').decode()) as client:
                shard = AzureStorageShard('synthetic','synthetic')
                monkeypatch.setattr(shard,'get_async_client',lambda: client)
                result = await shard.upload_blob_from_file('container','key',str(path),file_size=10*MIB)
                assert result['success']
        assert sum(size for size, _ in bodies) == 10*MIB
        for size, digest in bodies:
            assert digest == hashlib.sha256(b'x'*size).hexdigest()
    finally:
        await runner.cleanup()

async def test_stream_backends_share_one_budget_and_prepaid_stream_does_not_deadlock(monkeypatch):
    from shared.storage.azure_blob import AzureBlobStore
    from shared.crypto import runtime
    monkeypatch.setattr(runtime, 'is_write_enabled', lambda: False)
    budget = ExecutionBudget(resident_bytes=8192)
    monkeypatch.setattr(provider_runtime, '_budget', budget)
    gate, entered = asyncio.Event(), asyncio.Event()
    async def send(**kwargs):
        assert budget.used_bytes == 8192
        entered.set()
        await gate.wait()
        return {'ETag':'part'}
    s3 = SimpleNamespace(create_multipart_upload=AsyncMock(return_value={'UploadId':'id'}),upload_part=send,
        complete_multipart_upload=AsyncMock(),abort_multipart_upload=AsyncMock(),
        head_object=AsyncMock(return_value={'ContentLength':1024,'ETag':'"etag"','LastModified':datetime(2026,1,1)}))
    @asynccontextmanager
    async def client(): yield s3
    seaweed = SeaweedStore('id','test','http://local','key','secret',['bucket'])
    monkeypatch.setattr(seaweed,'_client_ctx',client)
    azure_shard = SimpleNamespace(_ensure_container=AsyncMock(),stage_block=AsyncMock(),
        commit_block_list_manual=AsyncMock(),get_blob_properties=AsyncMock(return_value={}))
    azure = AzureBlobStore('azure','test',[azure_shard])
    async def source(): yield b'x'*1024
    first = asyncio.create_task(seaweed.upload_stream('c','one',source(),1024,chunk_size=1024,max_parallel_parts=2))
    try:
        await asyncio.wait_for(entered.wait(),1)
        second = asyncio.create_task(azure.upload_stream('c','two',source(),1024,chunk_size=1024))
        await asyncio.sleep(.01)
        assert not second.done()
        azure_shard.stage_block.assert_not_called()
        gate.set()
        await asyncio.gather(first,second)
        assert budget.used_bytes == 0
        async with budget.bytes(8192,includes_storage=True), asyncio.timeout(1):
            await seaweed.upload_stream('c','prepaid',source(),1024,chunk_size=1024,max_parallel_parts=2)
            assert budget.used_bytes == 8192
    finally:
        gate.set()
        first.cancel()
        await asyncio.gather(first,return_exceptions=True)

async def test_spooled_part_cancellation_before_task_start_closes_disk_file(monkeypatch):
    import tempfile
    from shared.storage.file_upload import send_spooled_parts
    opened = []
    original = tempfile.TemporaryFile
    def tracked(*args,**kwargs):
        file = original(*args,**kwargs)
        opened.append(file)
        return file
    monkeypatch.setattr(tempfile,'TemporaryFile',tracked)
    async def source():
        yield b'abc'
        raise asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await send_spooled_parts(source(),part_size=3,concurrency=2,send=AsyncMock())
    assert opened and all(file.closed for file in opened)
