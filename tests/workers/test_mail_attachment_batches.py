"""Exercise mail attachment batching and its real failure-settlement call site."""
import ast
import asyncio
import base64
import copy
import hashlib
import json
import os
from datetime import datetime
from pathlib import Path
import time
from types import MethodType, SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple
import uuid

import httpx
import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")

from shared.graph_batch import BatchReadError, BatchResponse
from shared.models import SnapshotItem, SnapshotStatus
from shared.provider_admission import ProviderDeferred
from shared.provider_runtime import ProviderBudgetExhausted, ProviderTurnDeferred


@pytest.fixture
def capture(monkeypatch):
    path = Path(__file__).resolve().parents[2] / "workers/backup-worker/main.py"
    tree = ast.parse(path.read_text())
    names = {"_usermail_backup_attachments", "_record_enumeration_failure",
             "EnumerationFailed", "enumeration_failure_extra_data", "reconcile_enumeration_failure_status",
             "mailbox_batch_concurrency"}
    nodes = [n for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
             and n.name in names]
    snapshot = SimpleNamespace(id=uuid.uuid4(), status=SnapshotStatus.IN_PROGRESS,
                               extra_data={}, item_count=0, completed_at=None)
    resource = SimpleNamespace(id=uuid.uuid4(), type=SimpleNamespace(value="USER_MAIL"),
                               extra_data={"user_id": "user"}, display_name="Mail")
    tenant = SimpleNamespace(id=uuid.uuid4())

    class Session:
        async def __aenter__(self): return self
        async def __aexit__(self, *args): return False
        async def get(self, model, id_): return snapshot
        async def commit(self): pass

    namespace = dict(asyncio=asyncio, os=os, hashlib=hashlib, time=time, json=json, uuid=uuid,
        datetime=datetime, Dict=Dict, List=List, Optional=Optional, Tuple=Tuple, Any=Any,
        GraphClient=object, Resource=object, Snapshot=object, Tenant=object,
        SnapshotItem=SnapshotItem, SnapshotStatus=SnapshotStatus,
        SNAPSHOT_DEGRADED_REASON_KEY="degraded_reason",
        RESOURCE_LAST_BACKUP_TERMINAL_STATUSES=("COMPLETED", "PARTIAL", "FAILED"),
        async_session_factory=Session,
        azure_storage_manager=SimpleNamespace(get_shard_for_resource=lambda *args: object(),
                                             get_container_name=lambda *args: "email"),
        _maybe_inline_attachment=lambda content: base64.b64encode(content).decode(),
        _extract_cid_refs_from_body=lambda body: {"inline"},
        ProviderDeferred=ProviderDeferred, ProviderBudgetExhausted=ProviderBudgetExhausted,
        ProviderTurnDeferred=ProviderTurnDeferred,
        JobCancelledMidFlight=type("JobCancelledMidFlight", (Exception,), {}),
        ChatCatalogueWorkLeaseMoved=type("ChatCatalogueWorkLeaseMoved", (Exception,), {}),
        RunAbandoned=type("RunAbandoned", (Exception,), {}))
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), "exec"), namespace)
    worker = SimpleNamespace(worker_id="test-mail", ledger={})
    worker._shard_failure_ledger = lambda: worker.ledger
    async def patch_snapshot(id_, extra): snapshot.extra_data.update(extra); return True
    worker._patch_snapshot_extra_data = patch_snapshot
    for name in ("_usermail_backup_attachments", "_record_enumeration_failure"):
        setattr(worker, name, MethodType(namespace[name], worker))

    # Execute the production USER_MAIL branch, including its return before finalize.
    branch = next(n for n in ast.walk(tree) if isinstance(n, ast.If)
        and isinstance(n.test, ast.Compare)
        and any(isinstance(c, ast.Constant) and c.value == "USER_MAIL" for c in n.test.comparators)
        and any(isinstance(c, ast.Attribute) and c.attr == "_usermail_backup_attachments" for c in ast.walk(n)))
    branch = copy.deepcopy(branch)
    branch.orelse = []
    entry = ast.parse("async def capture_call(self, graph_client, resource, snapshot, tenant, items_data, message):\n    pass").body[0]
    entry.body = [branch, ast.Return(value=ast.Constant("finalize_reached"))]
    exec(compile(ast.fix_missing_locations(ast.Module(body=[entry], type_ignores=[])), str(path), "exec"), namespace)

    outer = next(n for n in ast.walk(tree) if isinstance(n, ast.For)
        and isinstance(n.target, ast.Name) and n.target.id == "_r"
        and isinstance(n.iter, ast.Name) and n.iter.id == "results"
        and any(isinstance(c, ast.Name) and c.id == "ChatCatalogueWorkLeaseMoved" for c in ast.walk(n)))
    check = ast.parse("def check_results(results):\n    pass").body[0]
    check.body = [copy.deepcopy(outer)]
    exec(compile(ast.fix_missing_locations(ast.Module(body=[check], type_ignores=[])), str(path), "exec"), namespace)
    return worker, resource, snapshot, tenant, namespace


def attachment(id_):
    return {"id": id_, "@odata.type": "#microsoft.graph.fileAttachment", "name": id_,
            "contentType": "text/plain", "size": 3, "contentBytes": "YWJj"}


def response(id_, value=None, **extra):
    return BatchResponse(id_, 200, {}, {"value": [] if value is None else value, **extra})


class Graph:
    def __init__(self, batches, fallback=None):
        self.batches, self.fallback = list(batches), fallback or {}
        self.batch_calls, self.list_calls = [], []

    async def batch(self, requests):
        self.batch_calls.append([r.id for r in requests])
        result = self.batches.pop(0)
        if isinstance(result, BaseException): raise result
        return result

    async def list_message_attachments(self, user_id, message_id):
        self.list_calls.append(message_id)
        result = self.fallback.get(message_id, [])
        if isinstance(result, BaseException): raise result
        return result


async def run_capture(capture, graph, ids):
    worker, resource, snapshot, tenant, _ = capture
    return await worker._usermail_backup_attachments(
        graph, resource, snapshot, tenant, "user", [{"id": id_} for id_ in ids])


async def test_paginated_batch_response_uses_complete_message_listing(capture):
    graph = Graph([{"m": response("m", [attachment("first")], **{"@odata.nextLink": "next"})}],
                  {"m": [attachment("first"), attachment("second")]})
    items, size = await run_capture(capture, graph, ["m"])
    assert {item.external_id for item in items} == {"m::first", "m::second"}
    assert size == 6 and graph.list_calls == ["m"]


async def test_partial_batch_reuses_confirmed_lists_and_fetches_only_missing(capture):
    graph = Graph([BatchReadError("response_byte_limit", {"a": response("a", [attachment("one")])})],
                  {"b": [attachment("two")]})
    items, size = await run_capture(capture, graph, ["a", "b"])
    assert {item.external_id for item in items} == {"a::one", "b::two"}
    assert size == 6 and graph.list_calls == ["b"]


async def test_configured_attachment_batch_size_bounds_each_request(capture, monkeypatch):
    monkeypatch.setenv("USER_MAIL_ATT_BATCH", "1")
    graph = Graph([{id_: response(id_)} for id_ in ("a", "b", "c")])
    assert await run_capture(capture, graph, ["a", "b", "c"]) == ([], 0)
    assert graph.batch_calls == [["a"], ["b"], ["c"]]
    assert graph.list_calls == []


@pytest.mark.parametrize("body", [{}, {"value": {"not": "a collection"}}])
async def test_malformed_batch_collection_is_not_cached_as_complete(capture, body):
    graph = Graph([{"m": BatchResponse("m", 200, {}, body)}], {"m": [attachment("full")]})
    items, size = await run_capture(capture, graph, ["m"])
    assert [item.external_id for item in items] == ["m::full"]
    assert graph.list_calls == ["m"]


@pytest.mark.parametrize("landed,initial,partition,expected", [
    (0, SnapshotStatus.IN_PROGRESS, None, SnapshotStatus.FAILED),
    (1, SnapshotStatus.IN_PROGRESS, None, SnapshotStatus.FAILED),
    (1, SnapshotStatus.COMPLETED, None, SnapshotStatus.PARTIAL),
    (1, SnapshotStatus.IN_PROGRESS, "partition", SnapshotStatus.IN_PROGRESS),
])
async def test_required_listing_failure_cannot_reach_completed_finalize(capture, landed, initial, partition, expected):
    worker, resource, snapshot, tenant, ns = capture
    snapshot.item_count = landed
    snapshot.status = initial
    graph = Graph([{}], {"m": httpx.ReadError("provider details must not reach the reason")})
    result = await ns["capture_call"](worker, graph, resource, snapshot, tenant,
        [("EMAIL", "subject", "m", {"raw": {"id": "m", "hasAttachments": True}})],
        {"partitionId": partition})
    assert result == {"item_count": 0, "bytes_added": 0, "failed": True}
    assert snapshot.status == expected and snapshot.status != SnapshotStatus.COMPLETED
    assert "attachment" in snapshot.extra_data["degraded_reason"]
    assert "provider details" not in snapshot.extra_data["degraded_reason"]
    if partition:
        assert worker.ledger[partition]["reason"] == "EnumerationFailed"


@pytest.mark.parametrize("stage", ["batch", "list", "content", "mime", "mime_result"])
@pytest.mark.parametrize("kind", ["defer", "deadline", "turn_defer", "cancel"])
async def test_mail_capture_propagates_control_flow(capture, stage, kind):
    error = {"defer": ProviderDeferred(None, 60), "deadline": ProviderBudgetExhausted("deadline"),
             "turn_defer": ProviderTurnDeferred(ProviderDeferred(None, 60)), "cancel": asyncio.CancelledError()}[kind]
    graph = Graph([error if stage == "batch" else {}], {"m": error})
    messages = [{"id": "m"}]
    async def fail(*args, **kwargs): raise error
    if stage == "content":
        att = attachment("binary")
        del att["contentBytes"]
        graph.batches = [{"m": response("m", [att])}]
        graph.get_message_attachment_content = fail
    elif stage in ("mime", "mime_result"):
        graph.batches = [{"m": response("m")}]
        async def results(*args, **kwargs): return {"m": error}
        graph.get_messages_mime_concurrent = fail if stage == "mime" else results
        messages[0]["body"] = {"content": "<img src='cid:inline'>"}
    worker, resource, snapshot, tenant, _ = capture
    with pytest.raises(type(error)):
        await worker._usermail_backup_attachments(graph, resource, snapshot, tenant, "user", messages)


@pytest.mark.parametrize("malformed", [{"not": "a list"}, ["not an attachment"]])
async def test_malformed_fallback_listing_is_an_enumeration_failure(capture, malformed):
    graph = Graph([{}], {"m": malformed})
    with pytest.raises(capture[-1]["EnumerationFailed"]):
        await run_capture(capture, graph, ["m"])


@pytest.mark.parametrize("kind", ["defer", "deadline", "turn_defer", "cancel"])
def test_outer_resource_collector_propagates_control_flow(capture, kind):
    error = {"defer": ProviderDeferred(None, 60), "deadline": ProviderBudgetExhausted("deadline"),
             "turn_defer": ProviderTurnDeferred(ProviderDeferred(None, 60)), "cancel": asyncio.CancelledError()}[kind]
    with pytest.raises(type(error)):
        capture[-1]["check_results"]([{"item_count": 1}, error])
