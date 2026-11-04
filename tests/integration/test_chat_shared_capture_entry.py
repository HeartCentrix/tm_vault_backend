"""Two members of one chat: the second derives the first's capture instead of re-reading it."""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from uuid import uuid4

import pytest
from sqlalchemy import select

from shared import chat_catalogue_entry, chat_capture_graph, models
from test_chat_catalogue_postgres import store

pytestmark = pytest.mark.integration

ALPHA, BETA = "principal-alpha", "principal-beta"


async def test_member_postpones_a_chat_being_captured_and_derives_it_afterwards(store, monkeypatch):
    gates = {(ALPHA, "chat-shared"): asyncio.Event(), (BETA, "chat-beta"): asyncio.Event()}
    started = {key: asyncio.Event() for key in gates}
    reads = []

    class Source(chat_capture_graph.StrictChatGraphSource):
        permission_scope_hash = "a" * 64

        def __init__(self, principal):
            self.principal_id = principal
            self.chat_ids = ["chat-shared", "chat-alpha" if principal == ALPHA else "chat-beta"]

        async def iter_chats(self):
            for cid in self.chat_ids:
                yield {"id": cid, "topic": cid, "chatType": "group",
                       "members": [{"userId": ALPHA}, {"userId": BETA}]}

        async def iter_message_pages(self, chat_id):
            reads.append((self.principal_id, chat_id))
            key = (self.principal_id, chat_id)
            if key in gates:
                started[key].set()
                await gates[key].wait()
            yield [{"id": "message-" + chat_id, "createdDateTime": "2026-06-01T00:00:00Z",
                    "body": {"content": chat_id}, "attachments": []}]

        async def iter_hosted_contents(self, chat_id, message_id):
            if False:
                yield {}

    @asynccontextmanager
    async def pinned(_client, *, principal_id):
        yield Source(principal_id)

    monkeypatch.setattr(chat_capture_graph, "pinned_chat_source", pinned)

    class Storage:
        def active_backend_id(self):
            return str(store.backend)

        def get_store_by_id(self, _backend):
            raise AssertionError("unexpected binary storage access")

    async def member(principal):
        resource, job, snapshot = uuid4(), uuid4(), uuid4()
        async with store.factory.begin() as session:
            session.add(models.Resource(id=resource, tenant_id=store.tenant, type=models.ResourceType.USER_CHATS,
                external_id=principal, display_name=principal,
                extra_data={"user_id": principal, "chat_inventory_format": 1}))
            await session.flush()
            session.add(models.Job(id=job, type=models.JobType.BACKUP, tenant_id=store.tenant,
                resource_id=resource, status=models.JobStatus.RUNNING))
            await session.flush()
            session.add(models.Snapshot(id=snapshot, resource_id=resource, job_id=job,
                backend_id=store.backend, extra_data={"chat_inventory_format": 1}))
        return resource, snapshot

    async def run(resource, snapshot):
        return await chat_catalogue_entry.run_chat_catalogue_resource(
            resource_id=resource, snapshot_id=snapshot, writer_enabled=True, graph_client=object(),
            session_factory=store.factory, storage=Storage(), concurrency=1)

    alpha, beta = await member(ALPHA), await member(BETA)
    alpha_run = asyncio.create_task(run(*alpha))
    beta_run = None
    try:
        await asyncio.wait_for(started[(ALPHA, "chat-shared")].wait(), timeout=10)
        beta_run = asyncio.create_task(run(*beta))
        await asyncio.wait_for(started[(BETA, "chat-beta")].wait(), timeout=10)
        gates[(ALPHA, "chat-shared")].set()       # alpha publishes the shared chat
        alpha_result = await asyncio.wait_for(alpha_run, timeout=30)
        gates[(BETA, "chat-beta")].set()          # beta finishes its own chat, then the postponed one
        beta_result = await asyncio.wait_for(beta_run, timeout=30)
    finally:
        for gate in gates.values():
            gate.set()
        await asyncio.gather(alpha_run, *([beta_run] if beta_run else []), return_exceptions=True)
    assert alpha_result["outcome"] == beta_result["outcome"] == "complete"
    assert (BETA, "chat-shared") not in reads
    assert reads.count((ALPHA, "chat-shared")) >= 1
    async with store.factory() as session:
        attempt = await session.scalar(select(models.ChatCaptureAttempt).where(
            models.ChatCaptureAttempt.snapshot_id == beta[1], models.ChatCaptureAttempt.chat_id == "chat-shared"))
    assert attempt.state == "PUBLISHED"
    assert any(entry.get("derived_from_principal") == ALPHA for entry in attempt.evidence_history)
