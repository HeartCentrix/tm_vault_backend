"""The production archive writer publishes exact file-upload receipts."""
import hashlib
import importlib.util
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4
import os
os.environ.setdefault('ALLOW_DEV_JWT_SECRETS', 'true')
import pytest
from shared.archive_spool import SpooledArchiveData

path = Path(__file__).resolve().parents[2] / 'workers/backup-worker/archive_backup.py'
spec = importlib.util.spec_from_file_location('archive_spooled_publication', path)
archive = importlib.util.module_from_spec(spec)
spec.loader.exec_module(archive)


@pytest.mark.parametrize('upload_success', [True, False])
async def test_spooled_content_uses_existing_storage_and_commits_only_after_upload(tmp_path, monkeypatch, upload_success):
    from shared import azure_storage as storage
    payload = bytes(range(256)) * 4096
    path = tmp_path / 'opaque'
    path.write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    data = SpooledArchiveData(str(path), len(payload), digest)
    uploads, rows, failures = [], [], []
    class Shard:
        backend_id = uuid4()
        async def upload_blob_from_file(self, container, blob_path, file_path, **kwargs):
            uploads.append((container, blob_path, hashlib.sha256(Path(file_path).read_bytes()).hexdigest()))
            return {'success': upload_success, 'blob_path': blob_path, 'error': 'simulated upload failure'}
    shard = Shard()
    monkeypatch.setattr(storage.azure_storage_manager, 'get_shard_for_resource', lambda *a: shard)
    monkeypatch.setattr(storage.azure_storage_manager, 'get_container_name', lambda *a: 'archive')
    monkeypatch.setattr(storage.azure_storage_manager, 'build_blob_path', lambda *a: 'snapshot/opaque')
    async def no_dedup(*args): return None
    monkeypatch.setattr(storage, '_readable_dedup_result', no_dedup)
    async def no_sleep(*args): pass
    monkeypatch.setattr(storage.asyncio, 'sleep', no_sleep)
    class Ledger:
        async def fail_items(self, ids, reason): failures.append((ids, reason))
        async def commit_rows(self, batch):
            assert uploads
            rows.extend(batch)
            return ['receipt']
    resource = SimpleNamespace(id=uuid4())
    snapshot = SimpleNamespace(id=uuid4(), backend_id=shard.backend_id)
    tenant = SimpleNamespace(id=uuid4())
    persist = archive._make_durable_persist(resource, snapshot, tenant, Ledger(), SimpleNamespace(ARCHIVE_GRAPH_VERSION='beta'))
    receipts = await persist([({'id': 'item', 'changeKey': 'v1', '_folder_id': 'folder',
        '_mailbox_id': 'mailbox', '_folder_path': 'Archive'}, {'changeKey': 'v1', 'data': data})])
    if upload_success:
        assert receipts == ['receipt'] and not failures
        assert rows[0]['content_size'] == len(payload)
        assert rows[0]['content_hash'] == digest and rows[0]['content_checksum'] == digest
        assert rows[0]['backend_id'] == shard.backend_id
        assert uploads[0][2] == digest
    else:
        assert not receipts and not rows
        assert failures == [(['item'], 'upload_failed')]
