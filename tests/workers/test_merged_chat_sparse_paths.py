"""Exercise the real nested drain branches with isolated Graph/DB boundaries."""
import ast
from contextlib import asynccontextmanager
import importlib.util
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
import sys

import pytest


@pytest.fixture(scope="module")
def drain_code():
    path = Path(__file__).resolve().parents[2] / "workers/backup-worker/main.py"
    spec = importlib.util.spec_from_file_location("bw_sparse_merge", path)
    bw = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = bw
    spec.loader.exec_module(bw)
    tree = ast.parse(path.read_text())
    fn = next(n for n in ast.walk(tree) if isinstance(n, ast.AsyncFunctionDef) and n.name == "_drain_one_chat")
    # Rebind captured counters into this test namespace; the body stays intact.
    fn.body = [ast.Global(names=n.names) if isinstance(n, ast.Nonlocal) else n for n in fn.body]
    module = ast.fix_missing_locations(ast.Module(body=[fn], type_ignores=[]))
    return bw, compile(module, str(path), "exec")


@pytest.mark.parametrize("claimed", [False, True], ids=["reuse", "activity-skip"])
@pytest.mark.parametrize("sparse,prior,copy_fails", [(True, True, False), (False, True, False), (True, False, False), (False, True, True)])
async def test_sparse_paths_skip_copy_only_with_prior_inventory(drain_code, claimed, sparse, prior, copy_fails):
    bw, code = drain_code
    worker = bw.BackupWorker.__new__(bw.BackupWorker)
    worker.worker_id = "test"
    cursor = "2026-10-01T10:00:00Z"
    claim = AsyncMock(return_value={"thread_id": "thread", "claimed": claimed, "drain_cursor": cursor, "drain_failure_state": {}})
    load = AsyncMock(return_value=[{"external_id": "message"}])
    copy = AsyncMock(side_effect=RuntimeError("copy failed") if copy_fails else None, return_value=1)
    mark = AsyncMock()
    db = SimpleNamespace()

    @asynccontextmanager
    async def gate():
        yield db

    ns = dict(bw.__dict__)
    ns.update({
        "self": worker, "settings": SimpleNamespace(CHAT_SPARSE_INCREMENTAL_ENABLED=sparse, CHAT_INCREMENTAL_FILTER_ENABLED=True),
        "_chat_fanout_cancelled": False, "_persisted_total": 0, "_persisted_bytes": 0,
        "_chat_has_prior_snap": prior, "chat_meta": {"chat": {"topic": "test"}},
        "tenant": SimpleNamespace(id="tenant"), "snapshot": SimpleNamespace(id="snapshot"),
        "chat_last_updated_map": {"chat": cursor}, "_claim_or_load_chat_thread": claim,
        "_load_chat_thread_messages_for_pointer": load, "_chat_pointer_db_gate": gate,
        "_build_chat_pointer_rows": Mock(return_value=([{"external_id": "message"}], 5)),
        "async_session_factory": gate, "_bulk_upsert_snapshot_items": AsyncMock(),
        "_copy_chat_attachment_rows_to_snapshot": copy, "_mark_chat_thread_drained": mark,
        "_progress": {"chats_done": 0, "msgs": 0}, "_chat_outcomes": {"ok": 0, "failed": 0, "suppressed": 0},
        "_chat_att_copy": {"failed": 0}, "cancel_check_every": 100,
        "_pick_chat_client": lambda cid: SimpleNamespace(client_id="app"), "_shard_scope_fps": {},
    })
    exec(code, ns)
    assert await ns["_drain_one_chat"]("chat") == ("chat", [], cursor)
    skipped = sparse and prior
    assert load.await_count == (0 if skipped else 1)
    assert copy.await_count == (0 if skipped else 1)
    assert ns["_chat_outcomes"]["ok"] == 1
    assert ns["_chat_att_copy"]["failed"] == int(copy_fails and not skipped)
    assert ns["_progress"]["chats_done"] == 1
    if claimed:
        mark.assert_awaited_once()
