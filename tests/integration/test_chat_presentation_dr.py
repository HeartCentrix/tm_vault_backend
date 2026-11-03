"""DR keeps captured naming metadata alongside the existing database backup."""
import hashlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import delete
from azure.core.exceptions import ResourceExistsError

from test_chat_catalogue_postgres import store
from test_chat_presentation import legacy, label_task, drain_labels
from test_chat_catalogue_reads import load
from shared import models

pytestmark = pytest.mark.integration


async def captured(store, *, empty=False):
    scope = await legacy(store, [] if empty else [('chat-a', 'chats/Stored', {})])
    labels = [] if empty else [{'chat_id': 'chat-a', 'folder_path': 'chats/Planning / 東京',
        'quality': 3, 'provenance': 'current_topic'}]
    task, publish = await label_task(store, scope, labels, publication=True)
    await drain_labels(store, task, terminal=False)
    await drain_labels(store, publish)
    async with store.factory.begin() as db:
        (await db.get(models.Snapshot, scope.snapshot_id)).status = models.SnapshotStatus.COMPLETED
    return scope


class Replica:
    def __init__(self, corrupt=False):
        self.data, self.uploads, self.closed = None, 0, False
        self.corrupt = corrupt

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        self.closed = True

    async def upload_blob(self, data, **kwargs):
        if self.data is not None:
            raise ResourceExistsError('already uploaded')
        self.data, self.uploads = data, self.uploads + 1

    async def download_blob(self):
        return SimpleNamespace(readall=AsyncMock(return_value=b'corrupt' if self.corrupt else self.data))


async def test_dr_round_trip_restores_names_after_database_records_are_recovered(store):
    from shared.chat_presentation_dr import export_presentation_metadata, restore_presentation_metadata, replicate_presentation_metadata
    from shared.chat_presentation import captured_chat_path
    scope = await captured(store)
    replica = Replica()
    async with store.factory.begin() as db:
        data = await export_presentation_metadata(db, snapshot_id=scope.snapshot_id, tenant_id=scope.tenant_id)
        receipt = await replicate_presentation_metadata(db, snapshot_id=scope.snapshot_id,
            tenant_id=scope.tenant_id, client=replica)
        assert receipt['sha256'] == hashlib.sha256(data).hexdigest()
        assert await replicate_presentation_metadata(db, snapshot_id=scope.snapshot_id,
            tenant_id=scope.tenant_id, client=replica) == receipt
        assert replica.uploads == 1
        await db.execute(delete(models.SnapshotChatPresentation).where(models.SnapshotChatPresentation.snapshot_id == scope.snapshot_id))
        await db.execute(delete(models.ChatPresentationPlan).where(models.ChatPresentationPlan.snapshot_id == scope.snapshot_id))
    async with store.factory.begin() as db:
        await restore_presentation_metadata(db, snapshot_id=scope.snapshot_id, tenant_id=scope.tenant_id,
            data=replica.data, expected_sha256=receipt['sha256'])
        assert await captured_chat_path(db, scope.snapshot_id, 'chat-a') == 'chats/Planning / 東京'
        await restore_presentation_metadata(db, snapshot_id=scope.snapshot_id, tenant_id=scope.tenant_id,
            data=replica.data, expected_sha256=receipt['sha256'])


async def test_dr_import_rejects_corruption_and_wrong_ownership(store):
    from shared.chat_presentation_dr import export_presentation_metadata, restore_presentation_metadata
    scope = await captured(store)
    async with store.factory.begin() as db:
        data = await export_presentation_metadata(db, snapshot_id=scope.snapshot_id, tenant_id=scope.tenant_id)
        with pytest.raises(ValueError, match='checksum'):
            await restore_presentation_metadata(db, snapshot_id=scope.snapshot_id, tenant_id=scope.tenant_id,
                data=data + b' ', expected_sha256=hashlib.sha256(data).hexdigest())
        tampered = json.loads(data)
        tampered['tenant_id'] = 'other'
        changed = json.dumps(tampered).encode()
        with pytest.raises(ValueError, match='scope'):
            await restore_presentation_metadata(db, snapshot_id=scope.snapshot_id, tenant_id=scope.tenant_id,
                data=changed, expected_sha256=hashlib.sha256(changed).hexdigest())


@pytest.mark.parametrize('corrupt', [False, True])
async def test_replication_worker_writes_verified_label_manifest_before_metadata_only_success(store, monkeypatch, corrupt):
    module = load('presentation_dr_worker_' + str(corrupt), 'services/dr-replication-worker/main.py')
    scope = await captured(store, empty=True)
    replica = Replica(corrupt)
    monkeypatch.setattr(module.azure_storage_manager, 'get_default_shard', lambda: SimpleNamespace(account_name='primary', account_key='synthetic'))
    monkeypatch.setattr(module.azure_storage_manager, 'get_container_name', lambda *args: 'synthetic-chats')
    monkeypatch.setattr(module, 'decrypt_secret', lambda value: 'synthetic')
    monkeypatch.setattr(module, 'BlobClient', SimpleNamespace(from_blob_url=lambda *args, **kwargs: replica))
    async with store.factory.begin() as db:
        snapshot = await db.get(models.Snapshot, scope.snapshot_id)
        tenant = await db.get(models.Tenant, scope.tenant_id)
        tenant.dr_storage_account_name = 'replica'
        tenant.dr_storage_account_key_encrypted = b'synthetic'
        await module.replicate_snapshot(snapshot, tenant, db)
        assert snapshot.dr_replication_status == ('failed' if corrupt else 'replicated')
        assert replica.uploads == 1 and replica.closed
        if not corrupt:
            assert snapshot.extra_data['chat_presentation_dr']['sha256'] == hashlib.sha256(replica.data).hexdigest()
