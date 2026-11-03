"""Required streaming pointer failures cannot certify a successful chat drain."""
import asyncio
import os
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tests.workers.test_merged_chat_sparse_paths import drain_code

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")


def streaming_drain(drain_code, *, cursor, failure=None):
    bw, code = drain_code
    worker = bw.BackupWorker.__new__(bw.BackupWorker)
    worker.worker_id = "test-stream-honesty"
    page_calls = []
    message = {"id": "new-message", "lastModifiedDateTime": "2026-11-02T10:00:00Z"}

    async def pages(url, **kwargs):
        page_calls.append((url, kwargs))
        yield {"value": [message]}

    checkpoint = AsyncMock()
    persist = AsyncMock(side_effect=failure, return_value=(1, 5))
    prior_failure = {"count": 2, "error_class": "TRANSIENT", "first_failed_at": "2026-10-31T10:00:00Z"}
    namespace = dict(bw.__dict__)
    namespace.update({
        "self": worker,
        "user_label": "Synthetic chats",
        "settings": SimpleNamespace(CHAT_SPARSE_INCREMENTAL_ENABLED=False, CHAT_INCREMENTAL_FILTER_ENABLED=True),
        "_chat_fanout_cancelled": False, "_persisted_total": 0, "_persisted_bytes": 0,
        "_chat_has_prior_snap": cursor is not None, "chat_meta": {"chat": {"topic": "test"}},
        "tenant": SimpleNamespace(id="tenant"), "snapshot": SimpleNamespace(id="snapshot"),
        "chat_last_updated_map": {"chat": "2026-11-02T10:00:00Z"},
        "_claim_or_load_chat_thread": AsyncMock(return_value={"thread_id": "thread", "claimed": True,
            "drain_cursor": cursor, "drain_failure_state": prior_failure, "last_drained_msg_count": 1}),
        "_mark_chat_thread_drained": checkpoint,
        "_pick_chat_client": lambda cid: SimpleNamespace(client_id="app", _iter_pages=pages),
        "_shard_scope_fps": {"app": "original-scopes"},
        "graph_client": SimpleNamespace(GRAPH_URL="https://graph.invalid"),
        "per_chat_sem": asyncio.Semaphore(1), "_streaming_enabled": True,
        "_stream_persist_chat_items": persist, "new_failures": {},
        "_interleave_hc": False, "_interleave_att": False,
        "_progress": {"chats_done": 0, "msgs": 0},
        "_chat_outcomes": {"ok": 0, "failed": 0, "suppressed": 0},
        "cancel_check_every": 100,
    })
    exec(code, namespace)
    return namespace, checkpoint, persist, page_calls


@pytest.mark.parametrize("cursor", [None, "2026-11-01T10:00:00Z"], ids=["initial", "incremental"])
async def test_exhausted_stream_pointer_write_holds_cursor_and_completeness(drain_code, cursor):
    failure = RuntimeError("required pointer transaction retry exhausted")
    namespace, checkpoint, persist, page_calls = streaming_drain(drain_code, cursor=cursor, failure=failure)

    cid, messages, next_cursor = await namespace["_drain_one_chat"]("chat")

    assert (cid, len(messages), next_cursor) == ("chat", 1, None)
    state = checkpoint.call_args.kwargs
    assert state["drain_succeeded"] is False
    assert state["drain_cursor"] is None and state["msg_count"] is None
    assert state["failure_state"]["count"] == 3
    assert state["failure_state"]["first_failed_at"] == "2026-10-31T10:00:00Z"
    assert "retry exhausted" in state["failure_state"]["last_error"]
    assert state["failure_state"]["error_class"] == "TRANSIENT"
    assert namespace["_chat_outcomes"] == {"ok": 0, "failed": 1, "suppressed": 0}
    assert (namespace["_persisted_total"], namespace["_persisted_bytes"]) == (0, 0)
    assert len(page_calls) == 1 and persist.await_count == 1


@pytest.mark.parametrize("kind", ["vanished", "lease_moved"])
async def test_lost_stream_target_stops_fanout_before_success_checkpoint(drain_code, kind):
    bw, _ = drain_code
    failure_type = bw.SnapshotVanishedError if kind == "vanished" else bw.ChatCatalogueWorkLeaseMoved
    namespace, checkpoint, _, _ = streaming_drain(drain_code, cursor=None, failure=failure_type("target lost"))

    with pytest.raises(failure_type):
        await namespace["_drain_one_chat"]("chat")

    assert namespace["_chat_fanout_cancelled"] is True
    assert checkpoint.await_count == 0
    assert namespace["_chat_outcomes"]["ok"] == 0
