"""Exercise the actual row worker with storage and database seams controlled."""
from __future__ import annotations

import ast
import asyncio
from contextlib import asynccontextmanager
import hashlib
import logging
import os
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, Optional
import uuid

import httpx
import pytest
import secrets
os.environ.setdefault("JWT_SECRET", secrets.token_hex(32))
from shared.models import SnapshotItem


@pytest.fixture
def run_row():
    path = Path(__file__).parents[2] / "workers/backup-worker/main.py"
    tree = ast.parse(path.read_text())
    fn = next(n for n in ast.walk(tree) if isinstance(n, ast.AsyncFunctionDef) and n.name == "_backup_sp_rest_row")
    helpers = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in ("_sp_row_version", "_sp_content_key", "_reusable_sp_copy")]
    state = {"uploads": [], "rows": [], "db_fail": False, "metadata_calls": 0}
    stats = {k: 0 for k in ("rest_rows", "rest_fail", "rest_files_ok", "rest_bytes")}

    class Session:
        async def execute(self, stmt):
            if state["db_fail"]:
                raise RuntimeError("database unavailable")
            state["rows"].append(stmt.compile().params)
        async def commit(self):
            pass

    @asynccontextmanager
    async def session():
        yield Session()

    async def upload(**kwargs):
        state["uploads"].append(Path(kwargs["file_path"]).read_bytes())
        return {"success": True}

    ns = {"asyncio": asyncio, "hashlib": hashlib, "os": os, "uuid": uuid,
          "Any": Any, "Dict": Dict, "Optional": Optional,
          "Resource": object, "Tenant": object, "Snapshot": object, "GraphClient": object,
          "SnapshotItem": SnapshotItem, "async_session_factory": session,
          "upload_blob_with_retry_from_file": upload, "logger": logging.getLogger(__name__),
          "azure_storage_manager": SimpleNamespace(build_blob_path=lambda *args: "blob"),
          "record_sp_rest_failure": lambda stats, *args, **kwargs: (stats.update(rest_fail=stats['rest_fail'] + 1) or "failed")}
    # Execute the production method unchanged; replace only its external seams.
    exec(compile(ast.Module(body=[*helpers, fn], type_ignores=[]), str(path), "exec"), ns)

    async def run(*, metadata=None, chunks=(b"abc",), metadata_error=None, after=None, listed=None, previous=None):
        if metadata is None:
            metadata = {"Length": "3", "ETag": '"v1"', "UniqueId": "file", "UIVersionLabel": "1.0"}
        async def get_meta(*args):
            state["metadata_calls"] += 1
            if metadata_error:
                raise metadata_error
            return after if after is not None and state["metadata_calls"] > 1 else metadata
        async def stream(*args, **kwargs):
            for chunk in chunks:
                if isinstance(chunk, Exception):
                    raise chunk
                yield chunk
        worker = SimpleNamespace(worker_id="test", backup_semaphore=asyncio.Semaphore(1))
        await ns[fn.name](worker, resource=SimpleNamespace(id=uuid.uuid4()), tenant=SimpleNamespace(id=uuid.uuid4()),
            snapshot=SimpleNamespace(id=uuid.uuid4()), graph_client=SimpleNamespace(
                get_sharepoint_file_metadata_via_rest=get_meta, stream_sharepoint_file_via_rest=stream),
            shard=None, container="container", stats=stats, work={
                "row": {"Id": 1, "FileRef": "/sites/a/x", "FileLeafRef": "x", "FileSystemObjectType": 0,
                        **({"File": listed} if listed is not None else {})},
                "previous": previous, "lst_id": "list", "list_display": "List", "site_id": "site", "site_label": "Site",
                "sp_hostname": "tenant.sharepoint.com", "sp_site_web_url": "https://tenant.sharepoint.com/sites/a"})
        return state, stats
    return run, state, stats


async def test_metadata_error_is_failure_not_successful_metadata_only(run_row):
    run, state, stats = run_row
    await run(metadata_error=RuntimeError("denied"))
    assert stats["rest_fail"] == 1
    assert not state["uploads"]


async def test_truncated_download_never_uploaded(run_row):
    run, state, stats = run_row
    await run(chunks=(b"ab",))
    assert not state["uploads"]
    assert stats["rest_fail"] == 1


async def test_zero_byte_file_has_durable_empty_content(run_row):
    run, state, stats = run_row
    await run(metadata={"Length": "0", "ETag": '"v1"'}, chunks=())
    assert state["uploads"] == [b""]
    assert stats["rest_files_ok"] == 1
    assert state["rows"][0]["blob_path"] == "blob"


async def test_metadata_change_rejects_download(run_row):
    run, state, stats = run_row
    await run(after={"Length": "3", "ETag": '"v2"'})
    assert not state["uploads"]
    assert stats["rest_fail"] == 1


async def test_persist_failure_is_reported(run_row):
    run, state, stats = run_row
    state["db_fail"] = True
    await run()
    assert stats["rest_fail"] == 1
    assert stats["rest_files_ok"] == 0


async def test_valid_content_and_metadata_published(run_row):
    run, state, stats = run_row
    await run()
    assert stats["rest_fail"] == 0
    assert stats["rest_files_ok"] == 1
    assert state["rows"][0]["content_hash"] == hashlib.sha256(b"abc").hexdigest()


async def test_listed_file_identity_skips_the_first_metadata_request(run_row):
    # The list page's $expand=File carries the same identity (live: 181/181 files), so only
    # the post-download recheck reads metadata; the stored file metadata is that full recheck.
    run, state, stats = run_row
    await run(listed={"Length": "3", "ETag": '"v1"', "UniqueId": "file", "UIVersionLabel": "1.0"})
    assert state["metadata_calls"] == 1
    assert stats["rest_files_ok"] == 1 and len(state["uploads"]) == 1


async def test_listed_identity_that_changed_before_download_is_rejected(run_row):
    run, state, stats = run_row
    await run(listed={"Length": "3", "ETag": '"v0"', "UniqueId": "file", "UIVersionLabel": "0.9"})
    assert not state["uploads"] and stats["rest_fail"] == 1


async def test_row_without_a_usable_listed_identity_reads_metadata_first(run_row):
    run, state, stats = run_row
    await run(listed={"ETag": '"v1"'})  # no Length
    assert state["metadata_calls"] == 2 and stats["rest_files_ok"] == 1


async def test_restamped_file_with_unchanged_content_reuses_the_stored_copy(run_row):
    # Railway 2026-11-06: SharePoint re-stamped __siteIcon__ (item version moved, ContentTag's
    # content version did not) and the row worker downloaded it again. Reuse the stored blob.
    run, state, stats = run_row
    listed = {"Length": "3", "ETag": '"{F},24"', "ContentTag": "{F},24,3", "UniqueId": "f", "UIVersionLabel": "18.0"}
    previous = {"row_version": "f|23", "blob_path": "prior/blob", "content_hash": "h", "content_size": 3,
                "ghosted": False, "file": {**listed, "ETag": '"{F},23"', "ContentTag": "{F},23,3"}}
    await run(listed=listed, previous=previous)
    assert not state["uploads"] and state["metadata_calls"] == 0
    row = state["rows"][0]
    assert (row["blob_path"], row["content_hash"], row["content_size"]) == ("prior/blob", "h", 3)
    assert row["metadata"]["file"] == listed          # the new version is what this restore point records
    assert stats["rest_reused"] == 1 and stats["rest_fail"] == 0


async def test_file_whose_content_version_moved_is_downloaded(run_row):
    run, state, stats = run_row
    listed = {"Length": "3", "ETag": '"v1"', "ContentTag": "{F},24,4", "UniqueId": "file", "UIVersionLabel": "1.0"}
    previous = {"row_version": "f|23", "blob_path": "prior/blob", "content_hash": "h", "content_size": 3,
                "ghosted": False, "file": {**listed, "ContentTag": "{F},23,3"}}
    await run(listed=listed, previous=previous)
    assert len(state["uploads"]) == 1 and state["rows"][0]["blob_path"] == "blob"
