"""Execute archive dispatch methods without importing the worker startup stack."""
import ast
import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


_WORKER_CLASS = next(
    node for node in ast.parse(
        (Path(__file__).resolve().parents[2] / "workers/backup-worker/main.py").read_text()
    ).body if isinstance(node, ast.ClassDef) and node.name == "BackupWorker"
)


def _worker_methods():
    methods = {"_backup_resource_group", "_backup_generic_parallel", "backup_archive"}
    body = [node for node in _WORKER_CLASS.body
            if isinstance(node, ast.AsyncFunctionDef) and node.name in methods]
    table = next(node for node in _WORKER_CLASS.body
                 if isinstance(node, ast.AnnAssign)
                 and isinstance(node.target, ast.Name)
                 and node.target.id == "_HANDLER_TABLE")
    body.append(table)
    module = ast.Module(body=[
        ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0),
        ast.ClassDef(name="Worker", bases=[], keywords=[], body=body, decorator_list=[]),
    ], type_ignores=[])

    @asynccontextmanager
    async def session():
        yield object()

    namespace = {
        "asyncio": asyncio, "settings": SimpleNamespace(BACKUP_CONCURRENCY=2),
        "_raise_if_job_cancelled": AsyncMock(), "async_session_factory": session,
    }
    exec(compile(ast.fix_missing_locations(module), "archive_dispatch", "exec"), namespace)
    return namespace["Worker"]()


@pytest.mark.asyncio
@pytest.mark.parametrize("message,force_full", [({}, False), ({"forceFullBackup": True}, True), ({"type": "FULL"}, True)])
async def test_bulk_and_single_archive_routes_share_pipeline_and_snapshot(monkeypatch, message, force_full):
    worker = _worker_methods()
    worker.worker_id = "archive-test"
    graph, tenant, snapshot = object(), object(), SimpleNamespace(id="snapshot")
    resource = SimpleNamespace(id="resource", type=SimpleNamespace(value="USER_ARCHIVE"))
    result = {"item_count": 12, "bytes_added": 42}
    run = AsyncMock(return_value=result)
    monkeypatch.setitem(sys.modules, "archive_backup", SimpleNamespace(run_archive_backup=run))
    worker.get_graph_client = AsyncMock(return_value=graph)
    worker.create_snapshot = AsyncMock(return_value=snapshot)
    worker.complete_snapshot = AsyncMock()
    worker._stamp_resource_from_snapshot_row = AsyncMock()
    worker.fail_snapshot = AsyncMock()

    assert worker._HANDLER_TABLE["USER_ARCHIVE"] == "backup_archive"
    assert await worker._backup_resource_group([resource], tenant, message, "job") == result
    worker.create_snapshot.assert_awaited_once_with(resource, message, "job")
    run.assert_awaited_once_with(
        graph, resource, snapshot, tenant, worker_id="archive-test", force_full=force_full,
    )
    worker.complete_snapshot.assert_awaited_once()
    worker.fail_snapshot.assert_not_awaited()
