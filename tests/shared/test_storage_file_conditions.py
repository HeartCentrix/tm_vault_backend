"""Real SDK retries, conditional publication and owned I/O cancellation."""
import asyncio
import base64
import hashlib
from contextlib import asynccontextmanager
from collections import defaultdict
from xml.etree import ElementTree as ET

import pytest
from aiohttp import web
from shared.storage.seaweedfs import SeaweedStore
from shared.azure_storage import AzureStorageShard
from shared.crypto import runtime

MIB = 1024*1024

@asynccontextmanager
async def local_store(*, backend, fail_once=False, existing=None, tenant_id=None, head_missing=False, early_retry=None):
    state = {'attempts': defaultdict(list), 'stored': existing, 'condition': None, 'parts': {}, 'commits':0, 'aborted':[], 'signed':[]}
    async def serve(request):
        if early_retry is not None and request.method == 'PUT' and request.query.get('restype') != 'container' and request.query.get('comp') != 'blocklist':
            early_retry['requests'] += 1
            if early_retry['requests'] == 1:
                await asyncio.wait_for(early_retry['entered'].wait(), 2)
                return web.Response(status=500, text='<Error><Code>InternalError</Code></Error>', content_type='application/xml', headers={'Connection':'close'})
            early_retry['release'].set()
        try:
            body = await request.read()
        except ConnectionResetError:
            return web.Response(status=499)
        if request.method == 'HEAD':
            if state['stored'] is None or head_missing:
                return web.Response(status=404)
            return web.Response(headers={'Content-Length':str(len(state['stored'] or b'')), 'ETag':'"final"', 'Last-Modified':'Mon, 02 Nov 2026 00:00:00 GMT'})
        if request.method == 'PUT' and request.query.get('restype') == 'container':
            return web.Response(status=201)
        part = request.query.get('partNumber') or request.query.get('blockid')
        if part:
            state['attempts'][part].append(body)
            if fail_once and len(state['attempts'][part]) == 1:
                return web.Response(status=500, text='<Error><Code>InternalError</Code><Message>synthetic retry</Message></Error>', content_type='application/xml')
            state['parts'][part] = body
            return web.Response(status=201 if backend=='azure' else 200, headers={'ETag':'"part"'})
        if request.method == 'POST' and 'uploads' in request.query:
            return web.Response(text='<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>key</Key><UploadId>id</UploadId></InitiateMultipartUploadResult>', content_type='application/xml')
        if request.method == 'DELETE':
            state['aborted'].append(request.query.get('uploadId'))
            return web.Response(status=204)
        state['condition'] = request.headers.get('If-None-Match')
        if state['condition'] is not None:
            state['signed'].append('if-none-match' in request.headers.get('Authorization',''))
        if state['stored'] is not None and state['condition']=='*':
            return web.Response(status=412, text='<Error><Code>ConditionNotMet</Code><Message>exists</Message></Error>', content_type='application/xml')
        if request.query.get('comp') == 'blocklist':
            state['stored'] = b''.join(state['parts'][elem.text] for elem in ET.fromstring(body))
            state['commits'] += 1
            return web.Response(status=201)
        if request.method == 'POST':
            state['stored'] = b''.join(state['parts'][k] for k in sorted(state['parts'],key=int))
            state['commits'] += 1
            return web.Response(text='<CompleteMultipartUploadResult><Location>local</Location><Bucket>bucket</Bucket><Key>key</Key><ETag>"final"</ETag></CompleteMultipartUploadResult>', content_type='application/xml')
        state['stored'] = body
        state['commits'] += 1
        return web.Response(status=201 if backend=='azure' else 200, headers={'ETag':'"put"'})
    app=web.Application(client_max_size=24*MIB)
    app.router.add_route('*','/{tail:.*}',serve)
    runner=web.AppRunner(app); await runner.setup()
    site=web.TCPSite(runner,'127.0.0.1',0); await site.start()
    endpoint=f'http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}'
    try:
        if backend=='azure':
            from azure.storage.blob.aio import BlobServiceClient
            from azure.storage.blob.aio import ExponentialRetry
            async with BlobServiceClient(account_url=endpoint+'/account', credential=base64.b64encode(b'synthetic').decode(), retry_policy=ExponentialRetry(initial_backoff=0,increment_base=0,random_jitter_range=0,retry_total=1)) as client:
                shard=AzureStorageShard('synthetic','synthetic')
                shard.get_async_client=lambda: client
                async def upload(path,size,overwrite=True):
                    return await shard.upload_blob_from_file('container','key',str(path),file_size=size,overwrite=overwrite)
                yield upload,state
        else:
            import aioboto3
            from botocore.config import Config
            store=SeaweedStore('id','test',endpoint,'key','secret',['bucket'])
            store._tenant_id=tenant_id
            store._client_ctx=lambda: aioboto3.Session().client('s3', endpoint_url=endpoint,aws_access_key_id='synthetic',aws_secret_access_key='synthetic',region_name='us-east-1',config=Config(retries={'max_attempts':1}))
            async def upload(path,size,overwrite=True):
                return await store.upload_from_file('container','key',str(path),size,overwrite=overwrite)
            yield upload,state
    finally:
        await runner.cleanup()

@pytest.mark.parametrize('backend',['azure','s3'])
async def test_actual_sdk_retries_replay_identical_file_parts(backend,tmp_path,monkeypatch):
    monkeypatch.setattr(runtime,'is_write_enabled',lambda: False)
    path=tmp_path/'retry'; path.write_bytes(b'a'*(5*MIB)+b'b'*(4*MIB))
    async with local_store(backend=backend,fail_once=True) as (upload,state):
        result=await upload(path,path.stat().st_size)
        assert state['stored']==path.read_bytes()
        assert all(len(v)==2 and v[0]==v[1] for v in state['attempts'].values())


@pytest.mark.parametrize('backend', ['azure', 's3'])
async def test_early_retry_joins_borrowed_read_before_rewinding_and_signing(backend, tmp_path, monkeypatch):
    import threading
    from shared.storage.file_upload import FileBody
    monkeypatch.setattr(runtime, 'is_write_enabled', lambda: False)
    entered, release, finished = asyncio.Event(), threading.Event(), threading.Event()
    owner, loop = threading.get_ident(), asyncio.get_running_loop()
    original = FileBody.__init__
    class Source:
        def __init__(self, source): self.source = source
        def seek(self, *args): return self.source.seek(*args)
        def read(self, size):
            data = self.source.read(size)
            if threading.get_ident() != owner and data and not entered.is_set():
                loop.call_soon_threadsafe(entered.set)
                release.wait(4)
                finished.set()
            return data
    def init(body, source, start, length):
        original(body, source, start, length)
        body.source = Source(source)
    monkeypatch.setattr(FileBody, '__init__', init)
    path = tmp_path / 'early-response'
    path.write_bytes(b'a' * MIB + b'b' * MIB + b'c' * MIB)
    retry = {'entered': entered, 'release': release, 'requests': 0}
    async with local_store(backend=backend, early_retry=retry) as (upload, state):
        try:
            async with asyncio.timeout(10):
                await upload(path, path.stat().st_size)
            assert retry['requests'] == 2
            assert state['stored'] == path.read_bytes()
        finally:
            release.set()
            while entered.is_set() and not finished.is_set():
                await asyncio.sleep(.001)

@pytest.mark.parametrize('backend',['azure','s3'])
async def test_actual_sdk_zero_byte_file(backend,tmp_path,monkeypatch):
    monkeypatch.setattr(runtime,'is_write_enabled',lambda: False)
    path=tmp_path/'zero';path.write_bytes(b'')
    async with local_store(backend=backend) as (upload,state):
        result=await upload(path,0)
        assert state['stored']==b''

@pytest.mark.parametrize('backend',['azure','s3'])
@pytest.mark.parametrize('size',[0,3,9*MIB])
async def test_actual_sdk_create_only_preserves_previous_object(backend,size,tmp_path,monkeypatch):
    monkeypatch.setattr(runtime,'is_write_enabled',lambda: False)
    path=tmp_path/'new';path.write_bytes(b'n'*size)
    async with local_store(backend=backend,existing=b'PREVIOUS',head_missing=True) as (upload,state):
        try:
            result=await upload(path,size,overwrite=False)
        except Exception as exc:
            result=repr(exc)
        assert state['condition']=='*'
        assert state['stored']==b'PREVIOUS'
        if backend=='s3':
            assert state['signed']==[True]
            assert state['aborted']==(['id'] if size>=8*MIB else [])

@pytest.mark.parametrize('backend',['azure','s3'])
async def test_actual_sdk_cancel_waits_for_borrowed_file_read(backend,tmp_path,monkeypatch):
    import threading
    from shared.storage.file_upload import FileBody
    from shared import provider_runtime
    monkeypatch.setattr(runtime,'is_write_enabled',lambda:False)
    budget=provider_runtime.ExecutionBudget(resident_bytes=16*MIB)
    monkeypatch.setattr(provider_runtime,'_budget',budget)
    path=tmp_path/'cancel';path.write_bytes(b'x'*(3*MIB))
    entered=asyncio.Event(); release=threading.Event(); finished=threading.Event()
    loop=asyncio.get_running_loop(); owner=threading.get_ident(); borrowed=[]
    original=FileBody.read
    def slow_read(body,size=-1):
        data=original(body,size)
        if threading.get_ident()!=owner and data and not entered.is_set():
            borrowed.append((body,len(data)))
            loop.call_soon_threadsafe(entered.set)
            release.wait(5)
            finished.set()
        return data
    monkeypatch.setattr(FileBody,'read',slow_read)
    async with local_store(backend=backend) as (upload,state):
        task=asyncio.create_task(upload(path,path.stat().st_size))
        try:
            await asyncio.wait_for(entered.wait(),2)
            task.cancel()
            await asyncio.sleep(.1)
            observed=(task.done(),budget.used_bytes,finished.is_set(),[(b.closed,n) for b,n in borrowed])
            assert not task.done(), 'caller/budget returned while SDK read thread retained payload'
            assert budget.used_bytes>0 and all(not body.closed for body,_ in borrowed)
        finally:
            release.set()
            await asyncio.gather(task,return_exceptions=True)
            while not finished.is_set():
                await asyncio.sleep(.001)
        assert budget.used_bytes==0 and all(body.closed for body,_ in borrowed)

async def test_repeated_cancel_waits_for_owned_abort(tmp_path,monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from shared import provider_runtime
    monkeypatch.setattr(runtime,'is_write_enabled',lambda:False)
    budget=provider_runtime.ExecutionBudget(resident_bytes=16*MIB)
    monkeypatch.setattr(provider_runtime,'_budget',budget)
    path=tmp_path/'abort';path.write_bytes(b'x'*(9*MIB))
    sending=asyncio.Event(); aborting=asyncio.Event(); release=asyncio.Event(); aborted=[]
    async def send(**kwargs):
        sending.set()
        await asyncio.Event().wait()
    async def abort(**kwargs):
        aborting.set()
        await release.wait()
        aborted.append(kwargs['UploadId'])
    s3=SimpleNamespace(create_multipart_upload=AsyncMock(return_value={'UploadId':'owned'}),upload_part=send,abort_multipart_upload=abort)
    @asynccontextmanager
    async def client(): yield s3
    store=SeaweedStore('id','test','http://local','key','secret',['bucket']);store._client_ctx=client
    task=asyncio.create_task(store.upload_from_file('container','key',str(path),path.stat().st_size))
    try:
        await asyncio.wait_for(sending.wait(),2)
        task.cancel()
        await asyncio.wait_for(aborting.wait(),2)
        task.cancel()
        await asyncio.sleep(.05)
        assert not task.done(), 'second cancellation abandoned owned multipart abort'
    finally:
        release.set()
        await asyncio.gather(task,return_exceptions=True)
    assert aborted==['owned'] and budget.used_bytes==0

@pytest.mark.parametrize('size',[0,9*MIB+37])
async def test_encrypted_actual_sdk_retries_preserve_plaintext_and_cipher_parts(size,tmp_path):
    from shared.crypto import kek, envelope
    from shared.crypto.dek_manager import DekManager, DekStore
    class Memory(DekStore):
        def __init__(self):self.by_kid={};self.active={}
        async def get_active(self,t):return self.by_kid.get(self.active.get(t))
        async def get_by_key_id(self,k):return self.by_kid.get(k)
        async def insert(self,rec):self.by_kid[rec.key_id]=rec;self.active[rec.tenant_id]=rec.key_id
    manager=DekManager(kek.EnvKek(b'k'*32,kek_id='test'),Memory())
    runtime.configure(manager,write_enabled=True,verify_readback=False)
    path=tmp_path/'encrypted'; path.write_bytes(b'p'*size)
    try:
        async with local_store(backend='s3',fail_once=True,tenant_id='t1') as (upload,state):
            result=await upload(path,size)
            _,dek=await manager.active_dek('t1')
            assert envelope.decrypt_blob(state['stored'],dek)==path.read_bytes()
            assert result.content_md5==hashlib.sha256(path.read_bytes()).hexdigest()
            assert all(len(v)==2 and v[0]==v[1] for v in state['attempts'].values())
    finally:
        runtime.configure(None)


@pytest.mark.parametrize('size',[0,3,9*MIB])
async def test_encrypted_create_only_preserves_previous_object(size,tmp_path):
    from shared.crypto import kek
    from shared.crypto.dek_manager import DekManager, DekStore
    class Memory(DekStore):
        def __init__(self): self.by_kid={}; self.active={}
        async def get_active(self,t): return self.by_kid.get(self.active.get(t))
        async def get_by_key_id(self,k): return self.by_kid.get(k)
        async def insert(self,rec): self.by_kid[rec.key_id]=rec; self.active[rec.tenant_id]=rec.key_id
    manager=DekManager(kek.EnvKek(b'k'*32,kek_id='test'),Memory())
    runtime.configure(manager,write_enabled=True,verify_readback=False)
    path=tmp_path/'encrypted'; path.write_bytes(b'p'*size)
    try:
        async with local_store(backend='s3',existing=b'PREVIOUS',tenant_id='t1',head_missing=True) as (upload,state):
            with pytest.raises(Exception):
                await upload(path,size,overwrite=False)
            assert state['stored']==b'PREVIOUS' and state['commits']==0
            assert state['condition']=='*' and state['signed']==[True]
            assert state['aborted']==['id']
    finally:
        runtime.configure(None)


async def test_conditional_helper_uses_native_supported_model():
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from shared.storage.seaweedfs import _s3_create_only
    native=AsyncMock(return_value={'ok':True})
    client=SimpleNamespace(put_object=native,meta=SimpleNamespace(service_model=SimpleNamespace(
        operation_model=lambda name: SimpleNamespace(input_shape=SimpleNamespace(members={'IfNoneMatch':object()})))))
    assert await _s3_create_only(client,'put_object',overwrite=False,Bucket='b',Key='k',Body=b'')=={'ok':True}
    assert native.call_args.kwargs['IfNoneMatch']=='*'


async def test_legacy_conditional_hook_is_removed_after_rejected_write(tmp_path):
    from shared.storage.seaweedfs import _s3_create_only
    import aioboto3
    from aiohttp import web
    calls=[]
    async def serve(request):
        await request.read()
        condition=request.headers.get('If-None-Match')
        calls.append(condition)
        if condition:
            assert 'if-none-match' in request.headers.get('Authorization','')
            return web.Response(status=412,text='<Error><Code>PreconditionFailed</Code></Error>',content_type='application/xml')
        return web.Response(status=200)
    app=web.Application(); app.router.add_put('/{tail:.*}',serve)
    runner=web.AppRunner(app); await runner.setup()
    site=web.TCPSite(runner,'127.0.0.1',0); await site.start()
    endpoint=f'http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}'
    try:
        async with aioboto3.Session().client('s3',endpoint_url=endpoint,
                aws_access_key_id='synthetic',aws_secret_access_key='synthetic',region_name='us-east-1') as client:
            with pytest.raises(Exception):
                await _s3_create_only(client,'put_object',overwrite=False,Bucket='b',Key='k',Body=b'x')
            await _s3_create_only(client,'put_object',overwrite=True,Bucket='b',Key='k',Body=b'y')
        assert calls==['*',None]
    finally:
        await runner.cleanup()

async def test_retry_seek_waits_for_owned_read_without_racing_file_cursor():
    import io
    import threading
    from shared.storage.file_upload import FileBody
    entered, release = threading.Event(), threading.Event()
    class SlowSource(io.BytesIO):
        active = False
        def read(self,size=-1):
            self.active=True
            entered.set()
            assert release.wait(3)
            try: return super().read(size)
            finally: self.active=False
        def seek(self,offset,whence=0):
            assert not self.active, 'retry seek raced an active file read'
            return super().seek(offset,whence)
    source=SlowSource(b'payload')
    async with FileBody(source,0,7) as body:
        first=asyncio.create_task(anext(body.__aiter__()))
        second=None
        try:
            assert await asyncio.to_thread(entered.wait,1)
            assert body.seek(0)==0
            second=asyncio.create_task(anext(body.__aiter__()))
            await asyncio.sleep(.02)
            assert not second.done()
            release.set()
            with pytest.raises(StopAsyncIteration): await first
            assert await second==b'payload'
        finally:
            release.set()
            await asyncio.gather(*(task for task in (first,second) if task),return_exceptions=True)


@pytest.mark.parametrize('size',[0,3,9*MIB])
async def test_known_existing_object_is_refused_before_upload_on_legacy_server(size,tmp_path,monkeypatch):
    from shared.storage.errors import ImmutableBlobError
    monkeypatch.setattr(runtime,'is_write_enabled',lambda:False)
    path=tmp_path/'existing';path.write_bytes(b'n'*size)
    async with local_store(backend='s3',existing=b'PREVIOUS') as (upload,state):
        with pytest.raises(ImmutableBlobError):
            await upload(path,size,overwrite=False)
        assert state['stored']==b'PREVIOUS' and state['commits']==0 and not state['attempts']
