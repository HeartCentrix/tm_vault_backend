"""The scheduled dispatch must COMMIT Job rows before PUBLISHING them.

Verified on prod 2026-09-03: every scheduled batch was published before
its Job row existed, the worker ack-dropped the unknown jobId, and the
3-minute outbox reconciler republished it ~5 min later on backup.urgent.
This suite drives the real dispatch_policy_backups with a faked session /
bus and records the ORDER of commit vs publish. It also pins the new
spec.queue / spec.wave stamps and the per-policy lock.
"""
from __future__ import annotations

import asyncio
import importlib.util
import os
import pathlib
import sys
import types
import uuid

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret-value")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")

_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _load_scheduler():
    path = _ROOT / "services" / "backup-scheduler" / "main.py"
    spec = importlib.util.spec_from_file_location(
        "backup_scheduler_main_dispatch_helper", path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


sched = _load_scheduler()

_Q_INFLIGHT_JOBS = "Q:inflight_jobs"
_Q_INFLIGHT_IDS = "Q:inflight_ids"
_Q_INFLIGHT_COUNT = "Q:inflight_count"
_TENANT = uuid.uuid4()


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def one(self):
        return self._rows[0]

    def all(self):
        return self._rows

    def scalar(self):
        return self._rows[0][0] if self._rows else 0

    def scalars(self):
        return types.SimpleNamespace(all=lambda: [r[0] for r in self._rows])


class _Session:
    """Records 'add' / 'commit' into a shared event list."""

    def __init__(self, policy, resources, events, inflight_count=0):
        self.policy = policy
        self.resources = resources
        self.events = events
        self.inflight_count = inflight_count
        self.added = []
        self.committed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def get(self, model, ident):
        return self.policy

    async def execute(self, q):
        if q is _Q_INFLIGHT_JOBS:
            return _Result([(0, None)])
        if q is _Q_INFLIGHT_IDS:
            return _Result([])
        if q is _Q_INFLIGHT_COUNT:
            return _Result([(self.inflight_count,)])
        return _Result([(r,) for r in self.resources])

    def add(self, obj):
        self.added.append(obj)
        self.events.append("add")

    async def commit(self):
        self.committed = True
        self.events.append("commit")


_FLAGS = dict(
    backup_exchange=True, backup_onedrive=True, backup_sharepoint=True,
    backup_teams=True, backup_teams_chats=True, contacts=True,
    calendars=True, backup_exchange_archive=True, backup_entra_id=True,
    backup_power_platform=True, backup_copilot=True, planner=True,
    tasks=True, backup_azure_vm=True, backup_azure_sql=True,
    backup_azure_postgresql=True,
)


def _policy(max_concurrent=0):
    return types.SimpleNamespace(
        id=uuid.uuid4(), name="Silver", enabled=True,
        frequency="DAILY", max_concurrent_backups=max_concurrent,
        backup_days=["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"],
        backup_window_start=None, backup_window_end=None, **_FLAGS,
    )


def _resource(rtype="USER_MAIL", last_backup_at=None):
    return types.SimpleNamespace(
        id=uuid.uuid4(), tenant_id=_TENANT,
        type=types.SimpleNamespace(value=rtype), status="ACTIVE",
        display_name=f"{rtype}-res", last_backup_at=last_backup_at,
        extra_data={}, external_id=None,
    )


@pytest.fixture
def wired(monkeypatch):
    dp = sched.dispatch_policy
    monkeypatch.setattr(dp, "build_scheduled_inflight_query", lambda pid: _Q_INFLIGHT_JOBS)
    monkeypatch.setattr(dp, "build_inflight_resource_ids_query", lambda: _Q_INFLIGHT_IDS)
    monkeypatch.setattr(dp, "build_inflight_resource_count_query", lambda pid: _Q_INFLIGHT_COUNT)

    events: list = []
    published: list = []

    async def _publish(queue, message, priority=None):
        events.append("publish")
        published.append((queue, message))

    async def _noop(*a, **kw):
        return None

    monkeypatch.setattr(sched, "publish_cycle_audit_event", _noop)
    monkeypatch.setattr(sched, "publish_sla_skip_audit_events", _noop)
    monkeypatch.setattr(sched, "emit_backup_triggered", _noop)
    monkeypatch.setattr(sched.message_bus, "publish", _publish, raising=False)
    monkeypatch.setattr(sched.asyncio, "sleep", _noop)

    async def run(*, resources, policy=None, inflight_count=0):
        policy = policy or _policy()
        session = _Session(policy, resources, events, inflight_count)
        monkeypatch.setattr(sched, "async_session_factory", lambda: session)
        await sched.dispatch_policy_backups(str(policy.id))
        return session

    return types.SimpleNamespace(run=run, events=events, published=published)


async def test_cron_path_commits_before_the_first_publish(wired):
    session = await wired.run(resources=[_resource(), _resource("USER_CALENDAR")])
    assert session.committed
    assert "publish" in wired.events
    assert wired.events.index("commit") < wired.events.index("publish")
    # every add happens before the commit
    assert max(i for i, e in enumerate(wired.events) if e == "add") < wired.events.index("commit")


async def test_cron_jobs_are_wave_one_and_record_their_queue(wired):
    session = await wired.run(resources=[_resource(), _resource("USER_ONEDRIVE")])
    queues = {q for q, _ in wired.published}
    for job in session.added:
        assert job.spec["wave"] == 1
        assert job.spec["queue"] in queues
    # heavy routing still applies: USER_ONEDRIVE goes to the heavy lane
    by_type = {j.spec["resource_type"]: j.spec["queue"] for j in session.added}
    assert by_type["USER_ONEDRIVE"] == sched.settings.BACKUP_HEAVY_QUEUE
    assert by_type["USER_MAIL"] == "backup.normal"


async def test_publish_failure_does_not_abort_the_batch_or_the_commit(wired, monkeypatch):
    calls = {"n": 0}

    async def _flaky(queue, message, priority=None):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("bus hiccup")
        wired.events.append("publish")

    monkeypatch.setattr(sched.message_bus, "publish", _flaky, raising=False)
    session = await wired.run(resources=[_resource(), _resource("USER_CALENDAR")])
    assert session.committed and len(session.added) == 2
    assert calls["n"] == 2, "second job must still be published"


async def test_emit_backup_triggered_runs_even_when_publish_fails(wired, monkeypatch):
    """The Job row is committed regardless of publish outcome, and the
    outbox reconciler republishes a failed-publish row WITHOUT ever
    emitting BACKUP_TRIGGERED — so the emit must depend only on the
    commit, not on the publish succeeding, or that job never gets a
    trigger audit row anywhere."""
    calls = {"n": 0}

    async def _flaky(queue, message, priority=None):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("bus hiccup")

    emitted: list = []

    async def _record_emit(**kw):
        emitted.append(kw)

    monkeypatch.setattr(sched.message_bus, "publish", _flaky, raising=False)
    monkeypatch.setattr(sched, "emit_backup_triggered", _record_emit)
    session = await wired.run(resources=[_resource(), _resource("USER_CALENDAR")])
    assert len(session.added) == 2
    assert len(emitted) == 2, (
        "every committed job must get a BACKUP_TRIGGERED emit, "
        "including the one whose first publish attempt failed"
    )


async def test_helper_returns_totals_and_queues(monkeypatch):
    events: list = []
    policy = _policy()
    session = _Session(policy, [], events)
    resources = [_resource(), _resource(), _resource("USER_CALENDAR")]
    published: list = []

    async def _publish(queue, message, priority=None):
        published.append(queue)

    async def _noop(*a, **kw):
        return None

    monkeypatch.setattr(sched.message_bus, "publish", _publish, raising=False)
    monkeypatch.setattr(sched, "emit_backup_triggered", _noop)
    monkeypatch.setattr(sched.asyncio, "sleep", _noop)
    total, queues = await sched._dispatch_resources(
        session, policy, resources, dispatch_batch_id="b1",
        deferred_resource_ids=["d1"], overlap_held_resource_ids=[], wave=4,
    )
    assert total == 3 and queues == {"backup.normal"}
    assert {j.spec["batch_id"] for j in session.added} == {"b1"}
    assert all(j.spec["deferred_resource_ids"] == ["d1"] for j in session.added)
    assert all(j.spec["wave"] == 4 for j in session.added)
    assert events[-1] == "commit" and events.count("add") == 2  # all adds precede the single commit
    assert len(published) == 2  # one job per type group


async def test_policy_lock_serializes_concurrent_dispatch(monkeypatch):
    """Cron tick and drain tick for the same policy must never overlap."""
    active = {"n": 0, "max": 0}
    pid = str(uuid.uuid4())

    async def _slow_impl(policy_id, respect_overlap_guard=True):
        active["n"] += 1
        active["max"] = max(active["max"], active["n"])
        await asyncio.sleep(0.01)
        active["n"] -= 1

    monkeypatch.setattr(sched, "_dispatch_policy_backups_impl", _slow_impl)
    await asyncio.gather(
        sched.dispatch_policy_backups(pid),
        sched.dispatch_policy_backups(pid),
        sched.dispatch_policy_backups(pid),
    )
    assert active["max"] == 1
    assert sched._policy_lock(pid) is sched._policy_lock(pid)


async def test_policy_lock_key_is_canonical(monkeypatch):
    """A differently-cased or brace-wrapped id from the manual
    /scheduler/policy/{id}/trigger path must serialize against the SAME
    lock the cron/drain path uses, and a garbage id must never insert a
    lock entry at all."""
    pid = uuid.uuid4()

    async def _noop_impl(policy_id, respect_overlap_guard=True):
        return None

    monkeypatch.setattr(sched, "_dispatch_policy_backups_impl", _noop_impl)
    sched._policy_locks.clear()

    await sched.dispatch_policy_backups(str(pid).upper())
    await sched.dispatch_policy_backups(str(pid))

    assert len(sched._policy_locks) == 1
    assert set(sched._policy_locks) == {str(pid)}

    before = dict(sched._policy_locks)
    with pytest.raises(ValueError):
        await sched.dispatch_policy_backups("not-a-uuid")
    assert sched._policy_locks == before


async def test_cycle_audit_event_accepts_outcome(monkeypatch):
    captured = {}

    def _mk(**kw):
        captured.update(kw)
        return {"ok": True}

    async def _pub(queue, message, priority=None):
        captured["queue"] = queue
        captured["priority"] = priority

    monkeypatch.setattr(sched, "create_audit_event_message", _mk)
    monkeypatch.setattr(sched.message_bus, "publish", _pub, raising=False)
    policy = types.SimpleNamespace(id=uuid.uuid4(), name="Silver", tenant_id=uuid.uuid4())
    await sched.publish_cycle_audit_event(policy, "BACKUP_CYCLE_WAVE_DISPATCHED", {"a": 1},
                                          outcome="SUCCESS", priority=2)
    assert captured["outcome"] == "SUCCESS" and captured["priority"] == 2
    await sched.publish_cycle_audit_event(policy, "BACKUP_DEFERRED_CONCURRENCY_CAP", {"a": 1})
    assert captured["outcome"] == "PARTIAL" and captured["priority"] == 3
