"""TM issue #10: a shard abandoned by pod kills is replayed automatically, bounded, before dead-lettering."""
from uuid import uuid4

import pytest
from sqlalchemy import text

from test_chat_catalogue_postgres import store
from shared import models

pytestmark = pytest.mark.integration


async def _abandoned_shard(store, **values):
    scope = await store.new_scope()
    async with store.factory.begin() as db:
        await db.execute(text("CREATE TABLE IF NOT EXISTS worker_heartbeats (worker_id uuid, last_seen_at timestamp)"))
        await db.execute(text("CREATE TABLE IF NOT EXISTS work_dead_letter (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), "
            "work_kind text, work_id uuid, reason text, last_payload json, created_at timestamptz DEFAULT NOW())"))
        snapshot = await db.get(models.Snapshot, scope.snapshot_id)
        partition = models.SnapshotPartition(id=uuid4(), snapshot_id=snapshot.id, tenant_id=scope.tenant_id,
            resource_id=scope.resource_id, job_id=snapshot.job_id, partition_type="CHATS", partition_index=0,
            payload={"chat_ids": ["19:a"]})
        db.add(partition)
        await db.flush()
        # Claimed, then its worker died RECONCILER_MAX_REQUEUE times.
        await db.execute(text("UPDATE snapshot_partitions SET status='IN_PROGRESS', worker_id=:w, requeue_count=:rq, "
            "lease_owner_id=NULL, lease_expires_at=NULL, enqueued_at=NOW()-INTERVAL '1 day' WHERE id=:p"),
            {"w": str(uuid4()), "rq": values.get("requeue_count", 3), "p": partition.id})
    return scope, partition.id


async def _state(store, pid):
    async with store.factory() as db:
        return (await db.execute(text("SELECT status, requeue_count, failure_state FROM snapshot_partitions WHERE id=:p"),
            {"p": pid})).one()


async def _sweep(store):
    from shared.reconciler import sweep_orphans
    async with store.factory() as db:
        stats = await sweep_orphans(db)
        await db.commit()
    return stats


async def _dead_letters(store, pid):
    async with store.factory() as db:
        return (await db.execute(text("SELECT reason FROM work_dead_letter WHERE work_id=:p"), {"p": pid})).scalars().all()


async def test_poison_shard_is_replayed_after_delay_then_dead_lettered_once_budget_is_spent(store, capsys):
    scope, pid = await _abandoned_shard(store)

    stats = await _sweep(store)  # requeue cap hit: replay scheduled, not FAILED
    status, requeue_count, failure_state = await _state(store, pid)
    assert (status, requeue_count, failure_state["dlq_replay_count"]) == ("QUEUED", 0, 1)
    assert stats.partitions_dlq == 0 and await _dead_letters(store, pid) == []
    assert str(pid) not in {p["id"] for p in stats.requeue_payloads}  # not before the delay
    assert f"[RECONCILER][DLQ_REPLAY] partition={pid} replay=1/1" in capsys.readouterr().out
    async with store.factory() as db:
        assert (await db.get(models.Snapshot, scope.snapshot_id)).status == models.SnapshotStatus.IN_PROGRESS

    stats = await _sweep(store)  # still inside the delay: nothing happens
    assert str(pid) not in {p["id"] for p in stats.requeue_payloads}

    async with store.factory.begin() as db:
        await db.execute(text("UPDATE snapshot_partitions SET enqueued_at=NOW()-INTERVAL '16 minutes' WHERE id=:p"), {"p": pid})
    stats = await _sweep(store)  # delay elapsed: republished through the normal requeue path
    assert str(pid) in {p["id"] for p in stats.requeue_payloads}

    async with store.factory.begin() as db:  # the replay died too
        await db.execute(text("UPDATE snapshot_partitions SET status='IN_PROGRESS', requeue_count=3, "
            "enqueued_at=NOW()-INTERVAL '1 day' WHERE id=:p"), {"p": pid})
    stats = await _sweep(store)
    assert (await _state(store, pid))[0] == "FAILED"
    assert stats.partitions_dlq == 1 and await _dead_letters(store, pid) == ["poison_pill"]


async def test_replay_never_touches_a_shard_owned_by_durable_copy_work(store, monkeypatch):
    import shared.copy_workflow as copy_workflow
    import shared.provider_work as provider_work
    monkeypatch.setattr(copy_workflow, "copy_work_guards_enabled", lambda: True)
    monkeypatch.setattr(provider_work, "pending_provider_sql", lambda *a, **k: "TRUE")
    scope, pid = await _abandoned_shard(store)
    async with store.factory.begin() as db:
        snapshot = await db.get(models.Snapshot, scope.snapshot_id)
        await db.execute(text("INSERT INTO copy_workflows (id, tenant_id, resource_id, job_id, target_snapshot_id, "
            "partition_id, leaf_key, state, capture_attempt, generation, parent_lease_token, snapshot_lease_token, "
            "job_lease_token, continuation, parent_settle_pending) VALUES (:w, :t, :r, :j, :s, :p, :k, 'WAITING', "
            "0, 0, 0, 0, 0, '{}', false)"), {"w": uuid4(), "t": scope.tenant_id, "r": scope.resource_id,
            "j": snapshot.job_id, "s": scope.snapshot_id, "p": pid, "k": str(uuid4())})
    await _sweep(store)
    status, requeue_count, failure_state = await _state(store, pid)
    assert (status, requeue_count) == ("IN_PROGRESS", 3) and "dlq_replay_count" not in (failure_state or {})
