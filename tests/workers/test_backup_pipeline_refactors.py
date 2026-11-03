"""Preserve file-version policy, mailbox source coverage and archive row ownership."""
import asyncio
import importlib.util
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest


@pytest.fixture(scope="module")
def backup_module():
    path = Path(__file__).resolve().parents[2] / "workers/backup-worker/main.py"
    with pytest.MonkeyPatch.context() as patch:
        patch.syspath_prepend(str(path.parent))
        spec = importlib.util.spec_from_file_location("sonar_backup_pipeline", path)
        module = importlib.util.module_from_spec(spec)
        patch.setitem(sys.modules, spec.name, module)
        spec.loader.exec_module(module)
        yield module


@pytest.fixture
def backup(backup_module):
    worker = backup_module.BackupWorker.__new__(backup_module.BackupWorker)
    worker.worker_id = "test"
    worker.backup_semaphore = asyncio.Semaphore(1)
    return worker


@pytest.mark.parametrize("carry", ["copied", "deferred", "failed", "empty"])
async def test_file_versions_keep_carry_forward_and_fallback(backup_module, backup, monkeypatch, carry):
    from shared.copy_workflow import DeferredCopy

    snapshot = SimpleNamespace(id=uuid4(), resource_id=uuid4())
    prior = str(uuid4())
    monkeypatch.setattr(backup_module, "_fetch_prior_file_etag_map", AsyncMock(
        return_value=(prior, {"file": "etag"}, {"file": {"v1"}}),
    ))
    result = {"copied": 1, "deferred": DeferredCopy(uuid4(), uuid4()), "empty": 0}
    copy = AsyncMock(side_effect=RuntimeError("copy failed")) if carry == "failed" else AsyncMock(return_value=result[carry])
    monkeypatch.setattr(backup_module, "_carry_forward_prior_file_versions", copy)
    monkeypatch.setattr(backup, "VERSION_CARRY_FORWARD_ENABLED", True)
    backup._backup_file_versions = AsyncMock(return_value=(1, 7))
    await backup._capture_file_version_history(
        object(), object(), snapshot, "drive", "file", "report.docx",
        {"createdDateTime": "before", "lastModifiedDateTime": "after"},
        "container", object(), "etag",
    )
    copy.assert_awaited_once_with(target_snapshot_id=str(snapshot.id), prior_snap_id=prior, file_ids=["file"])
    if carry in ("copied", "deferred"):
        backup._backup_file_versions.assert_not_awaited()
    else:
        assert backup._backup_file_versions.await_args.kwargs["skip_version_ids"] == {"v1"}


@pytest.mark.parametrize("name,created,modified", [
    ("report.pdf", "before", "after"), ("report.docx", "same", "same"),
])
async def test_ineligible_files_do_not_fetch_version_history(backup, name, created, modified):
    backup._load_prior_file_version_state = AsyncMock()
    backup._backup_file_versions = AsyncMock()
    await backup._capture_file_version_history(
        object(), object(), object(), "drive", "file", name,
        {"createdDateTime": created, "lastModifiedDateTime": modified},
        "container", object(), "etag",
    )
    backup._load_prior_file_version_state.assert_not_awaited()
    backup._backup_file_versions.assert_not_awaited()


@pytest.mark.parametrize("uploaded", [True, False])
async def test_file_stream_publishes_only_after_upload_and_removes_spool(backup_module, backup, monkeypatch, tmp_path, uploaded):
    path = tmp_path / "captured.part"
    path.write_bytes(b"abc")
    backup._download_to_temp_resumable = AsyncMock(return_value=(str(path), "sha256"))
    backup._create_file_snapshot_item = AsyncMock()
    backup._capture_file_version_history = AsyncMock()
    monkeypatch.setattr(backup_module, "upload_blob_with_retry_from_file", AsyncMock(return_value={"success": uploaded}))
    graph = SimpleNamespace(get_download_url=AsyncMock(return_value=("https://graph.invalid/file", 3, "quickxor")))
    result = await backup._stream_file_snapshot(
        object(), object(), {"size": 3}, graph, "drive", "file", "report.docx",
        "container", "blob", object(), "etag",
    )
    assert result["success"] is uploaded
    assert not path.exists()
    assert backup._create_file_snapshot_item.await_count == int(uploaded)
    assert backup._capture_file_version_history.await_count == int(uploaded)


async def test_fullwalk_keeps_folder_sources_and_applies_exclusions_before_persist(backup):
    backup.get_sla_policy = AsyncMock(return_value=SimpleNamespace(
        id=uuid4(), backup_exchange_archive=True, backup_exchange_recoverable=True,
    ))
    backup.get_policy_exclusions = AsyncMock(return_value=["exclude"])
    backup._compile_exclusions = lambda rules: rules
    backup._item_is_excluded_compiled = lambda kind, item, rules: item["id"] == "excluded"
    backup._persist_tier1_mail_items = AsyncMock(return_value=(3, 12, False))

    async def folder_tree(user, well_known_root=None):
        return {"primary": "/Inbox"} if well_known_root is None else {well_known_root: "/" + well_known_root}

    async def folder_items(user, folder):
        return [{"id": folder + "-item"}]

    graph = SimpleNamespace(
        get_mail_folder_tree=AsyncMock(side_effect=folder_tree),
        get_messages_delta=AsyncMock(return_value={"value": [
            {"id": "primary-item", "parentFolderId": "primary"},
            {"id": "excluded", "parentFolderId": "primary"},
        ], "@odata.deltaLink": "next"}),
        list_messages_in_folder=AsyncMock(side_effect=folder_items),
    )
    resource = SimpleNamespace(external_id="user", display_name="Mailbox", extra_data={"mail_delta_token": "old"})
    result = await backup._backup_mailbox_fullwalk(graph, resource, object(), object(), {})
    graph.get_messages_delta.assert_awaited_once_with("user", "old")
    items = backup._persist_tier1_mail_items.await_args.args[-1]
    assert [(item["id"], item["_source_mailbox"], item["_full_folder_path"]) for item in items] == [
        ("primary-item", "primary", "/Inbox"),
        ("archive-item", "archive", "/archive"),
        ("recoverableitemsroot-item", "recoverable", "/recoverableitemsroot"),
    ]
    assert result == {"item_count": 3, "bytes_added": 12, "new_delta_token": "next"}


async def test_archive_exports_keep_only_stored_change_keys_and_folder_paths(backup_module, backup, monkeypatch):
    from shared import archive_graph, azure_storage

    async def exports(graph, mailbox, ids):
        for item_id in ids:
            if item_id == "bad":
                yield item_id, None, {"code": "denied"}
            else:
                yield item_id, b"opaque", None

    monkeypatch.setattr(archive_graph, "export_items", exports)
    monkeypatch.setattr(azure_storage, "upload_blob_with_dedup", AsyncMock(return_value={"blob_path": "shared/content"}))
    monkeypatch.setattr(backup_module.azure_storage_manager, "get_container_name", lambda *args: "archive")
    monkeypatch.setattr(backup_module.azure_storage_manager, "get_shard_for_resource", lambda *args: object())
    monkeypatch.setattr(backup_module.azure_storage_manager, "build_blob_path", lambda *args: "candidate")
    tenant, resource, snapshot = (SimpleNamespace(id=uuid4()) for _ in range(3))
    folder = {"id": "folder", "displayName": "Leaf"}
    pairs = {key: (folder, {"id": key, "changeKey": key + "-version"}) for key in ("good", "bad")}
    rows, stored_keys, size = await backup._export_archive_changes(
        object(), tenant, resource, snapshot, "mailbox", ["good", "bad"], pairs,
        {"folder": "Archive/Parent/Leaf"},
    )
    assert stored_keys == {"good": "good-version"}
    assert size == len(b"opaque")
    assert len(rows) == 1
    assert rows[0]["snapshot_id"] == snapshot.id
    assert rows[0]["blob_path"] == "shared/content"
    assert rows[0]["folder_path"] == "Archive/Parent/Leaf"
    assert rows[0]["extra_data"]["change_key"] == "good-version"
