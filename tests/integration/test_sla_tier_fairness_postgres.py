"""Gold / Silver / Bronze cycles overlapping on one tenant, real PostgreSQL.

Proves the scheduler side cannot let one SLA tier starve another: every
policy owns its own lock, cap and in-flight count, so a backlog (or a
stuck worker) on one tier never stops the others' waves.
"""
import asyncio
import importlib.util
import os
import pathlib
import sys
from datetime import datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import select, update

from test_chat_catalogue_postgres import store
from shared import models

pytestmark = pytest.mark.integration

ROOT = pathlib.Path(__file__).resolve().parents[2]
CAP, PER_TIER = 3, 8
TIERS = (("Gold", "THREE_DAILY"), ("Silver", "DAILY"), ("Bronze", "DAILY"))


def _load_scheduler():
    os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")
    spec = importlib.util.spec_from_file_location(
        "backup_scheduler_main_tier_fairness", ROOT / "services/backup-scheduler/main.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
async def tiers(store, monkeypatch):
    sched = _load_scheduler()
    published = []

    async def _publish(queue, message, priority=None):
        published.append((queue, message))

    async def _noop(*a, **kw):
        return None

    monkeypatch.setattr(sched, "async_session_factory", store.factory)
    monkeypatch.setattr(sched.message_bus, "publish", _publish, raising=False)
    for name in ("publish_cycle_audit_event", "emit_backup_triggered",
                 "publish_sla_skip_audit_events", "publish_license_skip_audit_events"):
        monkeypatch.setattr(sched, name, _noop, raising=False)
    monkeypatch.setattr(sched.asyncio, "sleep", _noop)
    monkeypatch.delenv("BACKUP_CYCLE_DRAIN", raising=False)
    sched._cycle_exhausted_until.clear()
    sched._cycle_incomplete_emitted.clear()

    policies = {}
    yesterday = datetime.utcnow() - timedelta(days=1)
    async with store.factory.begin() as db:
        for name, freq in TIERS:
            policy = models.SlaPolicy(id=uuid4(), tenant_id=store.tenant, name=name,
                                      frequency=freq, enabled=True, max_concurrent_backups=CAP)
            db.add(policy)
            await db.flush()
            for i in range(PER_TIER):
                db.add(models.Resource(id=uuid4(), tenant_id=store.tenant, sla_policy_id=policy.id,
                                       type=models.ResourceType.MAILBOX, external_id=f"{name}-{i}",
                                       display_name=f"{name}-{i}", status=models.ResourceStatus.ACTIVE,
                                       last_backup_at=yesterday - timedelta(minutes=i)))
            policies[name] = policy
    return sched, store, policies, published


async def _inflight(sched, store, policy):
    async with store.factory() as db:
        return (await db.execute(
            sched.dispatch_policy.build_inflight_resource_count_query(policy.id))).scalar()


async def _finish(store, policy):
    """Workers finish every job this tier has in flight."""
    async with store.factory.begin() as db:
        await db.execute(update(models.Job).where(
            models.Job.spec["sla_policy_id"].as_string() == str(policy.id),
            models.Job.status.in_([models.JobStatus.QUEUED, models.JobStatus.RUNNING]),
        ).values(status=models.JobStatus.COMPLETED))


def _drain_now(sched, policy):
    trigger = sched._policy_cycle_trigger(policy)
    fire = trigger.get_next_fire_time(None, sched.dispatch_policy._aware_utc(datetime.utcnow()))
    window_start = fire.replace(tzinfo=None) - (timedelta(hours=8) if policy.frequency == "THREE_DAILY"
                                                 else timedelta(days=1))
    return window_start + timedelta(minutes=30)


async def _tick(sched, policies):
    return {name: await sched.drain_policy_cycle(str(p.id), now=_drain_now(sched, p))
            for name, p in policies.items()}


async def test_overlapping_cycles_all_progress_within_their_own_cap(tiers):
    sched, store, policies, published = tiers
    # All three cron slots fire together: no shared lock or cap between them.
    await asyncio.gather(*(sched.dispatch_policy_backups(str(p.id)) for p in policies.values()))
    for p in policies.values():
        assert await _inflight(sched, store, p) == CAP

    dispatched = {name: CAP for name in policies}
    while any(n < PER_TIER for n in dispatched.values()):
        for p in policies.values():
            await _finish(store, p)
        wave = await _tick(sched, policies)
        for name, n in wave.items():
            if dispatched[name] < PER_TIER:  # every tier with a backlog moves every tick
                assert n == min(CAP, PER_TIER - dispatched[name]), (name, wave)
            dispatched[name] += n
        for p in policies.values():
            assert await _inflight(sched, store, p) <= CAP
    assert dispatched == {name: PER_TIER for name in policies}

    by_queue = {q for q, _ in published}
    assert by_queue == {"backup.high", "backup.normal"}
    # Every resource has a prior backup: every scheduled job is incremental.
    assert all(m["type"] == "INCREMENTAL" and not m["forceFullBackup"] for _, m in published)
    async with store.factory() as db:
        specs = (await db.execute(select(models.Job.spec))).scalars().all()
    assert specs and not any(s["fullBackup"] for s in specs)


@pytest.mark.parametrize("stuck", ["Gold", "Silver"])
async def test_a_stuck_tier_never_starves_the_others(tiers, stuck):
    sched, store, policies, _ = tiers
    await asyncio.gather(*(sched.dispatch_policy_backups(str(p.id)) for p in policies.values()))
    dispatched = {name: CAP for name in policies}
    for _ in range(2):
        for name, p in policies.items():
            if name != stuck:  # the stuck tier's workers never finish
                await _finish(store, p)
        wave = await _tick(sched, policies)
        assert wave[stuck] == 0
        for name in policies:
            if name != stuck:
                assert wave[name] == min(CAP, PER_TIER - dispatched[name]) > 0, (name, wave)
                dispatched[name] += wave[name]
        assert await _inflight(sched, store, policies[stuck]) == CAP
