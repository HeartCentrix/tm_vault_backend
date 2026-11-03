"""Run legacy worker entry points against the durable admission boundary."""
from __future__ import annotations

import importlib.util
from contextlib import nullcontext
import pathlib
import sys
import types
import uuid
from dataclasses import dataclass
from datetime import datetime
from unittest.mock import AsyncMock

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]


def _load(name, relative):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def worker_module():
    return _load("bw_durable_copy_handoff", "workers/backup-worker/main.py")


@pytest.fixture
def workflow(monkeypatch):
    module = types.ModuleType("shared.copy_workflow")

    @dataclass(frozen=True)
    class DeferredCopy:
        task_id: str
        workflow_id: str

    module.DeferredCopy = DeferredCopy
    module.defer_current_copy = AsyncMock(return_value=DeferredCopy("task", "workflow"))
    module.has_pending_copy = AsyncMock(return_value=True)
    module.copy_work_guards_enabled = lambda: False
    module.verify_copy_resume = AsyncMock(return_value=True)
    module.pending_copy_sql = lambda **kw: "NOT EXISTS (SELECT 1 FROM copy_workflows)"
    monkeypatch.setitem(sys.modules, "shared.copy_workflow", module)
    return module


@pytest.mark.parametrize("name,kind,lane,params", [
    ("_carry_forward_prior_chat_snapshot_items", "CHAT_CARRY", "giant",
     {"tenant_id": "tenant", "resource_id": "resource", "target_snapshot_id": "target"}),
    ("_carry_forward_prior_mail_snapshot_items", "MAIL_CARRY", "giant",
     {"tenant_id": "tenant", "resource_id": "resource", "target_snapshot_id": "target",
      "excluded_paths": ["/Deleted"], "tombstoned_ids": ["removed"]}),
    ("_carry_forward_prior_file_versions", "FILE_VERSIONS", "micro",
     {"target_snapshot_id": "target", "prior_snap_id": "prior", "file_ids": ["file"]}),
    ("_copy_chat_attachment_rows_to_snapshot", "LEGACY_CHAT_ATTACHMENTS", "micro",
     {"tenant_id": "tenant", "target_snapshot_id": "target", "chat_id": "chat"}),
])
async def test_required_copy_delegates_without_opening_inline_slot(
    monkeypatch, worker_module, workflow, name, kind, lane, params,
):
    def inline_slot(*args, **kwargs):
        pytest.fail("delegated copy must release its handler instead of polling a slot")

    monkeypatch.setattr(worker_module, "carry_forward_slot_session", inline_slot)
    result = await getattr(worker_module, name)(**params)
    assert result is workflow.defer_current_copy.return_value
    call = workflow.defer_current_copy.await_args
    assert call.args[0] == kind
    assert call.kwargs.get("lane", "giant") == lane
    assert call.args[1]["target_snapshot_id"] == "target"
    assert not isinstance(result, int), "delegation must never masquerade as zero copied rows"


async def test_archive_required_copy_delegates_before_inline_slot(monkeypatch, workflow):
    archive = _load("archive_durable_copy_handoff", "workers/backup-worker/archive_backup.py")
    import shared.database as database

    monkeypatch.setattr(database, "carry_forward_slot_session", lambda *a, **k: pytest.fail("inline slot"))
    result = await archive._default_carry_forward("target", "prior", ["unchanged"])
    assert result is workflow.defer_current_copy.return_value
    call = workflow.defer_current_copy.await_args
    assert call.args == ("ARCHIVE_CARRY", {
        "target_snapshot_id": "target", "prior_snapshot_id": "prior", "external_ids": ["unchanged"],
    })


async def test_pending_copy_blocks_inline_status_and_cursor_promotion(worker_module, workflow):
    worker = worker_module.BackupWorker.__new__(worker_module.BackupWorker)
    worker.worker_id = "test-worker"
    snapshot = types.SimpleNamespace(
        id=uuid.uuid4(), job_id=uuid.uuid4(), status=worker_module.SnapshotStatus.IN_PROGRESS,
        extra_data={}, started_at=datetime.utcnow(), completed_at=None,
    )
    resource = types.SimpleNamespace(
        id=uuid.uuid4(), type=worker_module.ResourceType.USER_ONEDRIVE,
        display_name="test", extra_data={},
    )
    session = types.SimpleNamespace(get=AsyncMock(side_effect=AssertionError("no cursor promotion")),
        no_autoflush=nullcontext(), scalar=AsyncMock(return_value=uuid.uuid4()), refresh=AsyncMock())

    outcome = await worker._settle_inline_snapshot_finalize(session, snapshot, resource)
    assert outcome == "deferred_copy"
    assert snapshot.status == worker_module.SnapshotStatus.IN_PROGRESS
    assert snapshot.completed_at is None


async def test_pending_copy_blocks_complete_snapshot_before_any_terminal_work(worker_module, workflow):
    worker = worker_module.BackupWorker.__new__(worker_module.BackupWorker)
    worker.worker_id = "test-worker"
    snapshot = types.SimpleNamespace(id=uuid.uuid4())
    outcome = await worker.complete_snapshot(object(), snapshot, {"item_count": 1})
    assert outcome is False


async def test_pending_copy_blocks_partition_finalize_before_terminal_sql(monkeypatch, worker_module, workflow):
    worker = worker_module.BackupWorker.__new__(worker_module.BackupWorker)
    worker.worker_id = "test-worker"

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def execute(self, *args, **kwargs):
            pytest.fail("pending required copy must gate the first terminal UPDATE")

    monkeypatch.setattr(worker_module, "async_session_factory", Session)
    assert await worker._finalize_partitioned_snapshot(uuid.uuid4()) is None


async def test_user_leaf_seals_after_buffers_drain_and_returns_waiting(
    monkeypatch, worker_module, workflow,
):
    from contextlib import contextmanager

    trace = []
    marker = {"copy_waiting": True, "copy_workflow_id": "workflow"}

    @contextmanager
    def collection(snapshot_id, partition_id=None, **expected):
        trace.append("collect")
        yield
        trace.append("released")

    async def seal(continuation):
        assert trace == ["collect", "versions"]
        assert continuation["resume_kind"] == "user_content"
        assert "items_data" not in continuation
        trace.append("sealed")
        return marker

    workflow.copy_collection = collection
    workflow.seal_current_copy = seal
    workflow.has_pending_copy.return_value = False
    snapshot = types.SimpleNamespace(id=uuid.uuid4(), extra_data={})
    resource = types.SimpleNamespace(
        id=uuid.uuid4(), type=worker_module.ResourceType.USER_CHATS,
        display_name="test", extra_data={}, external_id="test",
    )
    tenant = types.SimpleNamespace(id=uuid.uuid4())

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def get(self, model, key):
            return snapshot

        async def execute(self, *args, **kwargs):
            pytest.fail("leaf must hand off before ordinary counters/finalization")

    worker = worker_module.BackupWorker.__new__(worker_module.BackupWorker)
    worker.worker_id = "test-worker"
    worker.create_snapshot = AsyncMock(return_value=snapshot)
    worker._maybe_backup_chat_catalogue = AsyncMock(return_value=None)
    import shared.snapshot_item_writes as chat_writes
    monkeypatch.setattr(chat_writes, "capture_chat_write_scope", AsyncMock(return_value=None))
    worker.get_sla_policy = AsyncMock(return_value=None)

    async def flush(**kwargs):
        trace.append("versions")
        return 0

    worker.flush_version_rows = flush
    monkeypatch.setattr(worker_module, "async_session_factory", Session)
    monkeypatch.setattr(worker_module, "_raise_if_job_cancelled", AsyncMock())
    result = await worker._backup_user_content_parallel(
        [resource], object(), tenant, {}, uuid.uuid4(),
    )
    assert result == marker
    assert trace == ["collect", "versions", "sealed", "released"]


async def test_required_copy_failure_cannot_promote_pending_cursor(worker_module, workflow):
    workflow.has_pending_copy.return_value = False
    worker = worker_module.BackupWorker.__new__(worker_module.BackupWorker)
    worker.worker_id = "test-worker"
    snapshot = types.SimpleNamespace(
        id=uuid.uuid4(), job_id=uuid.uuid4(), status=worker_module.SnapshotStatus.IN_PROGRESS,
        extra_data={"copy_required_failed": True}, started_at=datetime.utcnow(),
        completed_at=None, item_count=3,
    )
    resource = types.SimpleNamespace(
        id=uuid.uuid4(), type=worker_module.ResourceType.USER_MAIL, display_name="test",
    )
    session = types.SimpleNamespace(get=AsyncMock(side_effect=AssertionError("no cursor promotion")),
        no_autoflush=nullcontext(), scalar=AsyncMock(return_value=uuid.uuid4()), refresh=AsyncMock())
    # Avoid unrelated cancellation DB lookup.
    from unittest.mock import patch
    with patch.object(worker_module, "_is_job_cancelled", AsyncMock(return_value=False)):
        outcome = await worker._settle_inline_snapshot_finalize(session, snapshot, resource)
    assert outcome == "PARTIAL"
    assert snapshot.status == worker_module.SnapshotStatus.PARTIAL


async def test_archive_handoff_waits_for_export_gather_and_persists_only_scalar_continuation(
    monkeypatch, workflow,
):
    archive = _load("archive_durable_copy_seal", "workers/backup-worker/archive_backup.py")
    helpers = _load("archive_handoff_test_helpers", "tests/workers/test_archive_backup.py")
    graph = helpers.FakeGraph({"F": {"items": [
        {"id": "old", "changeKey": "old-ck"}, {"id": "fresh", "changeKey": "new-ck"},
    ]}}, ["F"])
    trace = []
    marker = {"copy_waiting": True, "copy_workflow_id": "workflow"}

    async def persist(pairs):
        trace.append("export_committed")
        return 1, 20, 0

    async def seal(continuation):
        assert trace == ["export_committed"]
        assert continuation["resume_kind"] == "archive"
        assert continuation["result"]["item_count"] == 1
        assert all(value is None or isinstance(value, (str, int, float, bool))
                   for value in continuation["result"].values())
        assert "walker" not in continuation and "unchanged_ids" not in continuation
        trace.append("sealed")
        return marker

    workflow.seal_current_copy = seal
    result = await archive.run_archive_backup(
        graph, helpers._resource(), helpers._snapshot(), helpers._tenant(),
        settings=helpers._settings(),
        load_prev=AsyncMock(return_value=("prior", {"old": "old-ck"})),
        persist_batch=persist, carry_forward=AsyncMock(return_value=workflow.defer_current_copy.return_value),
    )
    assert result == marker
    assert trace == ["export_committed", "sealed"]


async def test_resume_uses_persisted_continuation_and_never_restarts_graph(
    monkeypatch, worker_module, workflow,
):
    from contextlib import nullcontext

    sid, rid, tid, jid, wid, owner = (uuid.uuid4() for _ in range(6))
    snapshot = types.SimpleNamespace(id=sid, resource_id=rid, job_id=jid,
        extra_data={}, status=worker_module.SnapshotStatus.IN_PROGRESS)
    resource = types.SimpleNamespace(id=rid, tenant_id=tid,
        type=worker_module.ResourceType.USER_ONEDRIVE, extra_data={})
    tenant = types.SimpleNamespace(id=tid)
    job = types.SimpleNamespace(id=jid, batch_resource_ids=None)
    persisted = types.SimpleNamespace(target_snapshot_id=sid, partition_id=None,
        job_id=jid, resource_id=rid, tenant_id=tid,
        continuation={"resume_kind": "user_content", "result": {"item_count": 0, "bytes_added": 0}})
    workflow.begin_resume = AsyncMock(return_value=persisted)
    workflow.finish_resume = AsyncMock(return_value=True)
    workflow.resuming_copy_workflow = lambda workflow_id: nullcontext()
    workflow.has_pending_copy.return_value = False

    class Session:
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
        async def get(self, model, key, **kwargs):
            return {"Snapshot": snapshot, "Resource": resource, "Tenant": tenant, "Job": job}.get(model.__name__)
        async def execute(self, *args, **kwargs):
            return types.SimpleNamespace(one=lambda: (7, 128))
        async def commit(self): pass

    worker = worker_module.BackupWorker.__new__(worker_module.BackupWorker)
    worker.worker_id = "test-worker"
    worker._heartbeat = types.SimpleNamespace(worker_uuid=owner)
    worker.get_graph_client = AsyncMock(side_effect=AssertionError("resume must not request Graph"))
    worker._publish_copy_mail_state = AsyncMock()
    worker._settle_copy_parent = AsyncMock(return_value=True)
    worker.complete_snapshot = AsyncMock()
    worker._stamp_resource_from_snapshot_row = AsyncMock()
    worker.update_job_status = AsyncMock()
    worker.audit_logger = types.SimpleNamespace(log=AsyncMock())
    monkeypatch.setattr(worker_module, "async_session_factory", Session)

    await worker.resume_copy_workflow(wid)
    result = worker.complete_snapshot.await_args.args[2]
    assert result["item_count"] == 7
    assert result["bytes_total"] == 128
    workflow.finish_resume.assert_awaited_once_with(Session, wid, owner, failed=False)


@pytest.mark.parametrize("environment,expected", [
    ({}, {"concurrency": 2, "batch_size": 512, "statement_timeout_ms": 60000,
          "lock_timeout_ms": 5000, "poll_s": 1, "recovery_s": 10}),
    ({"COPY_WORK_CONCURRENCY": "4", "COPY_WORK_BATCH_ROWS": "128",
      "COPY_WORK_STATEMENT_TIMEOUT_MS": "45000", "COPY_WORK_LOCK_TIMEOUT_MS": "2000",
      "COPY_WORK_POLL_SECONDS": "0.25", "COPY_WORK_RECOVERY_SECONDS": "3.5"},
     {"concurrency": 4, "batch_size": 128, "statement_timeout_ms": 45000,
      "lock_timeout_ms": 2000, "poll_s": 0.25, "recovery_s": 3.5}),
])
async def test_existing_worker_starts_and_closes_copy_recovery_runner_when_guards_active(
    monkeypatch, worker_module, workflow, environment, expected,
):
    trace = []
    runtime = types.ModuleType("shared.copy_runtime")

    class Runner:
        def __init__(self, factory, resume, **kwargs):
            assert resume.__name__ == "resume_copy_workflow"
            assert kwargs == expected
            trace.append("constructed")

        async def start(self): trace.append("started")
        async def close(self): trace.append("closed")

    for name in ("COPY_WORK_CONCURRENCY", "COPY_WORK_BATCH_ROWS", "COPY_WORK_STATEMENT_TIMEOUT_MS",
                 "COPY_WORK_LOCK_TIMEOUT_MS", "COPY_WORK_POLL_SECONDS", "COPY_WORK_RECOVERY_SECONDS"):
        monkeypatch.delenv(name, raising=False)
    for name, value in environment.items():
        monkeypatch.setenv(name, value)
    runtime.CopyWorkRunner = Runner
    workflow.copy_work_guards_enabled = lambda: True
    monkeypatch.setitem(sys.modules, "shared.copy_runtime", runtime)
    monkeypatch.setenv("BACKUP_WORKER_DEDICATED_ONLY", "true")
    monkeypatch.setenv("ONEDRIVE_RETRY_QUEUE_ENABLED", "false")
    monkeypatch.setattr(worker_module, "settings", types.SimpleNamespace(ONEDRIVE_PARTITION_ENABLED=False))
    worker = worker_module.BackupWorker.__new__(worker_module.BackupWorker)
    worker.worker_id = "test-worker"
    worker._backup_queue_name = "backup.normal"
    worker.initialize = AsyncMock()
    monkeypatch.setattr(worker_module.message_bus, "connect", AsyncMock())
    worker._supervised_consume = AsyncMock()
    await worker.start()
    assert trace == ["constructed", "started", "closed"]


def test_collection_binds_pre_capture_tokens_and_task_claim_before_source_work(worker_module, workflow):
    from contextlib import contextmanager

    snapshot_id, partition_id, owner = (uuid.uuid4() for _ in range(3))
    observed = []

    @contextmanager
    def collection(snapshot, partition, **expected):
        observed.append((snapshot, partition, expected))
        yield object()

    workflow.copy_collection = collection
    worker_module._bind_partition_lease(partition_id, {
        "snapshot_id": snapshot_id, "lease_owner_id": owner, "lease_token": 3,
    })
    try:
        with worker_module._worker_copy_collection(
            snapshot_id, partition_id, expected_snapshot_token=7, expected_job_token=11,
        ):
            # A later claim must never change the already captured expected generation.
            worker_module._bind_partition_lease(partition_id, {
                "snapshot_id": snapshot_id, "lease_owner_id": owner, "lease_token": 4,
            })
        assert observed == [(str(snapshot_id), partition_id, {
            "expected_snapshot_token": 7, "expected_partition_token": 3, "expected_job_token": 11,
        })]
    finally:
        worker_module._bind_partition_lease(partition_id, None)


@pytest.mark.parametrize("name,value", [
    ("COPY_WORK_CONCURRENCY", "0"), ("COPY_WORK_BATCH_ROWS", "0"),
    ("COPY_WORK_STATEMENT_TIMEOUT_MS", "0"), ("COPY_WORK_LOCK_TIMEOUT_MS", "0"),
    ("COPY_WORK_POLL_SECONDS", "0"), ("COPY_WORK_RECOVERY_SECONDS", "nan"),
])
async def test_worker_rejects_invalid_copy_limits_through_runtime_validation(
    monkeypatch, worker_module, workflow, name, value,
):
    runtime = _load("copy_runtime_env_validation", "shared/copy_runtime.py")
    monkeypatch.setitem(sys.modules, "shared.copy_runtime", runtime)
    workflow.copy_work_guards_enabled = lambda: True
    monkeypatch.setenv(name, value)
    monkeypatch.setattr(runtime.CopyWorkRunner, "start", AsyncMock(
        side_effect=AssertionError("invalid limits must fail before runner startup")))
    worker = worker_module.BackupWorker.__new__(worker_module.BackupWorker)
    worker.worker_id = "test-worker"
    worker.initialize = AsyncMock()
    with pytest.raises(ValueError, match="copy "):
        await worker.start()


async def test_actual_start_runs_sql_recovery_before_unavailable_broker(monkeypatch, worker_module, workflow):
    import asyncio
    import shared.heartbeat as heartbeat
    import shared.lease as lease
    import shared.reclaim as reclaim

    started, entered = asyncio.Event(), asyncio.Event()
    runtime = types.ModuleType('shared.copy_runtime')
    class Runner:
        def __init__(self, *args, **kwargs): pass
        async def start(self): started.set()
        async def close(self): pass
    class Heartbeat:
        def __init__(self, **kwargs):
            self.worker_uuid, self.replica_id = uuid.uuid4(), 'test-replica'
        async def start(self): pass
    class Extender:
        def __init__(self, **kwargs): pass
        async def start(self): pass
    async def unavailable():
        entered.set()
        await asyncio.Event().wait()
    runtime.CopyWorkRunner = Runner
    workflow.initialize_copy_guards = AsyncMock()
    workflow.copy_work_guards_enabled = lambda: True
    monkeypatch.setitem(sys.modules, 'shared.copy_runtime', runtime)
    monkeypatch.setattr(heartbeat, 'HeartbeatThread', Heartbeat)
    monkeypatch.setattr(lease, 'LeaseExtender', Extender)
    monkeypatch.setattr(reclaim, 'reclaim_for_replica', AsyncMock(return_value=0))
    monkeypatch.setattr(worker_module.message_bus, 'connect', unavailable)
    worker = worker_module.BackupWorker.__new__(worker_module.BackupWorker)
    worker.worker_id, worker._backup_queue_name = 'test-worker', 'backup.normal'
    task = asyncio.create_task(worker.start())
    try:
        await asyncio.wait_for(entered.wait(), 1)
        assert started.is_set(), 'broker outage must not prevent durable SQL recovery startup'
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize('source_result', [
    {'files_failed': 2, 'enumeration_incomplete': True},
    {'files_failed': 0, 'enumeration_incomplete': True},
])
async def test_partition_resume_preserves_source_loss_after_successful_copy(
    monkeypatch, worker_module, workflow, source_result,
):
    from contextlib import nullcontext
    sid, rid, tid, jid, wid, pid, owner = (uuid.uuid4() for _ in range(7))
    snapshot = types.SimpleNamespace(id=sid, extra_data={}, status=worker_module.SnapshotStatus.IN_PROGRESS)
    resource = types.SimpleNamespace(id=rid, type=worker_module.ResourceType.SHAREPOINT_SITE)
    partition = types.SimpleNamespace(id=pid, lease_owner_id=owner, lease_token=2, partition_type='SHAREPOINT_DRIVES')
    persisted = types.SimpleNamespace(target_snapshot_id=sid, partition_id=pid, job_id=jid,
        resource_id=rid, tenant_id=tid, continuation={'resume_kind':'partition', 'result':source_result})
    workflow.begin_resume = AsyncMock(return_value=persisted)
    workflow.finish_resume = AsyncMock(return_value=True)
    workflow.resuming_copy_workflow = lambda _: nullcontext()
    class Session:
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
        async def get(self, model, key, **kwargs):
            return {'Snapshot':snapshot,'Resource':resource,'Tenant':types.SimpleNamespace(id=tid),
                    'Job':types.SimpleNamespace(id=jid),'SnapshotPartition':partition}.get(model.__name__)
        async def execute(self, *args, **kwargs): return types.SimpleNamespace(one=lambda:(1,7))
    monkeypatch.setattr(worker_module, 'async_session_factory', Session)
    worker = worker_module.BackupWorker.__new__(worker_module.BackupWorker)
    worker.worker_id = 'test-worker'
    worker._heartbeat = types.SimpleNamespace(worker_uuid=owner)
    worker._mark_partition_terminal = AsyncMock()
    worker._emit_partition_audit = AsyncMock()
    worker._finalize_partitioned_snapshot = AsyncMock()
    worker._settle_copy_parent = AsyncMock(return_value=True)
    await worker.resume_copy_workflow(wid)
    fs = worker._mark_partition_terminal.await_args.kwargs['failure_state']
    assert fs and fs['enumeration_incomplete'] is True
    assert fs['files_failed'] >= source_result['files_failed']
    assert worker._emit_partition_audit.await_args.args[1] == 'PARTIAL'
