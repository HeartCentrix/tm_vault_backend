"""Parent job validation locks must not serialize sibling progress writes.

Live Railway samples (2026-11-03) showed 5-28 queries continuously blocked on
one user's ``jobs`` row: every sibling workload validated its parent with
``FOR SHARE`` while progress/lease UPDATEs needed ``FOR NO KEY UPDATE``.
Validation only needs ``FOR KEY SHARE``: ordinary UPDATEs flow, and an
explicit ``FOR UPDATE`` (cancellation) still waits for in-flight writers.
"""
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from test_chat_catalogue_postgres import store

pytestmark = pytest.mark.integration


async def _job_id(factory, snapshot_id):
    async with factory() as session:
        return await session.scalar(text("SELECT job_id FROM snapshots WHERE id=:id"), {"id": snapshot_id})


async def _lock_timed_out(factory, statement, params):
    async with factory() as other:
        await other.execute(text("SET LOCAL lock_timeout='300ms'"))
        try:
            await other.execute(text(statement), params)
        except DBAPIError as exc:
            assert "lock timeout" in str(exc).lower()
            return True
        await other.rollback()
        return False


async def hold_item_write_parents(session, snapshot_id, job_id):
    from shared.snapshot_item_writes import _locked_parents
    await _locked_parents(session, snapshot_id, job_id, None)


async def hold_provider_parents_shared(session, snapshot_id, job_id):
    from shared.provider_work import _parents
    await _parents(session, snapshot_id, share_job=True)


async def hold_provider_parents_exclusive(session, snapshot_id, job_id):
    from shared.provider_work import _parents
    await _parents(session, snapshot_id, share_job=False)


async def hold_catalogue_settlement(session, snapshot_id, job_id):
    from shared.chat_catalogue_entry import lock_chat_catalogue_snapshot
    await lock_chat_catalogue_snapshot(session, snapshot_id=snapshot_id, snapshot_lease_token=0, job_lease_token=0)


async def hold_copy_settlement(session, snapshot_id, job_id):
    from shared.copy_workflow import _lock_parents
    await _lock_parents(session, SimpleNamespace(job_id=job_id, target_snapshot_id=snapshot_id, partition_id=None))


HOLDERS = [hold_item_write_parents, hold_provider_parents_shared, hold_provider_parents_exclusive,
           hold_catalogue_settlement, hold_copy_settlement]


@pytest.mark.parametrize("hold", HOLDERS, ids=lambda f: f.__name__)
async def test_parent_validation_lock_does_not_block_job_progress_update(store, hold):
    scope = await store.new_scope()
    job_id = await _job_id(store.factory, scope.snapshot_id)
    async with store.factory() as holder:
        await hold(holder, scope.snapshot_id, job_id)
        blocked = await _lock_timed_out(store.factory,
            "UPDATE jobs SET progress_pct=42, lease_expires_at=NOW() WHERE id=:id", {"id": job_id})
        await holder.rollback()
    assert not blocked, f"{hold.__name__} still serializes sibling progress writes"


@pytest.mark.parametrize("hold", HOLDERS, ids=lambda f: f.__name__)
async def test_cancellation_fence_still_waits_for_in_flight_writer(store, hold):
    scope = await store.new_scope()
    job_id = await _job_id(store.factory, scope.snapshot_id)
    async with store.factory() as holder:
        await hold(holder, scope.snapshot_id, job_id)
        blocked = await _lock_timed_out(store.factory, "SELECT id FROM jobs WHERE id=:id FOR UPDATE", {"id": job_id})
        await holder.rollback()
    assert blocked, f"{hold.__name__} no longer fences cancellation"


async def hold_catalogue_scope(session, snapshot_id, job_id):
    from shared.chat_catalogue import _lock_scope
    from shared.chat_catalogue_identity import ChatCaptureScope
    resource_id, tenant_id = (await session.execute(text(
        "SELECT s.resource_id, j.tenant_id FROM snapshots s JOIN jobs j ON j.id = s.job_id WHERE s.id=:id"),
        {"id": snapshot_id})).one()
    await _lock_scope(session, ChatCaptureScope(snapshot_id, resource_id, tenant_id, "chat-alpha", "principal-alpha", "a" * 64, uuid4()))


async def hold_provider_owned_parents(session, snapshot_id, job_id):
    from shared.provider_work import _parents
    await _parents(session, snapshot_id, share_job=True)


# Settlement (lock_chat_catalogue_snapshot) writes snapshot columns, so it is a writer, not a validator.
SNAPSHOT_HOLDERS = [hold_item_write_parents, hold_catalogue_scope, hold_provider_owned_parents]


@pytest.mark.parametrize("hold", SNAPSHOT_HOLDERS, ids=lambda f: f.__name__)
async def test_snapshot_validation_lock_does_not_block_snapshot_progress_update(store, hold):
    """Live 2026-11-04 sample: chat reserve/make_live transactions waited ~2 s each on the snapshot row."""
    scope = await store.new_scope()
    job_id = await _job_id(store.factory, scope.snapshot_id)
    async with store.factory() as holder:
        await hold(holder, scope.snapshot_id, job_id)
        blocked = await _lock_timed_out(store.factory,
            "UPDATE snapshots SET item_count=7, lease_expires_at=NOW() WHERE id=:id", {"id": scope.snapshot_id})
        await holder.rollback()
    assert not blocked, f"{hold.__name__} still serializes snapshot progress writes"
