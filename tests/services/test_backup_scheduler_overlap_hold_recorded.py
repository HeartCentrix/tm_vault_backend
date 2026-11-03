"""The overlap guard must RECORD what it withheld, in the job spec.

2026-08-11, prod batch 667734c3 (Gold, 10:07 UTC). All 8 jobs COMPLETED,
0 pending snapshots, 0 pending partitions — and the Activity row read
"BACKUP · In Progress · 15 %" and could never clear, because
``missing_t2`` was 7: the seven scope users' USER_CHATS children, which
``partition_overlap_holds`` had withheld from the tick ("Policy 'Gold':
previous scheduled cycle still draining after 8.0h (2 job(s) in flight)
— held 10 busy resource(s) and dispatched 40"). Those chats went out
under a DIFFERENT batch_id at the next tick, so the 10:07 batch could
never observe them.

To the rollup a withheld resource is indistinguishable from a Tier-2
child whose fan-out was LOST — unless the dispatcher writes down which
ones it withheld. This suite drives the real
``dispatch_policy_backups`` with a faked session / message bus and
asserts on the Job rows it builds. It does NOT grep main.py: an earlier
attempt at this task was rejected because its pins were ``SRC.index()``
searches that survived a logic-inverting mutation.
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import sys
import types
import uuid
from datetime import datetime, timedelta

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
        "backup_scheduler_main_overlap_record", path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


sched = _load_scheduler()


# ── fakes ──────────────────────────────────────────────────────────────
#
# The query BUILDERS are replaced with sentinels so the fake session can
# tell the three lookups apart without parsing SQLAlchemy Select objects.
# Every DECISION helper (decide_overlap_action, partition_overlap_holds,
# plan_concurrency_budget) stays the real implementation — that is the
# logic under test.

_Q_INFLIGHT_JOBS = "Q:inflight_jobs"
_Q_INFLIGHT_IDS = "Q:inflight_ids"
_Q_INFLIGHT_COUNT = "Q:inflight_count"
_Q_RESOURCES = "Q:resources"


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
    def __init__(self, policy, resources, inflight_ids, inflight_jobs,
                 inflight_oldest):
        self.policy = policy
        self.resources = resources
        self.inflight_ids = inflight_ids
        self.inflight_jobs = inflight_jobs
        self.inflight_oldest = inflight_oldest
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
            return _Result([(self.inflight_jobs, self.inflight_oldest)])
        if q is _Q_INFLIGHT_IDS:
            return _Result([(rid,) for rid in self.inflight_ids])
        if q is _Q_INFLIGHT_COUNT:
            return _Result([(len(self.inflight_ids),)])
        # the only remaining execute() is the Resource select
        return _Result([(r,) for r in self.resources])

    def add(self, obj):
        self.added.append(obj)

    async def commit(self):
        self.committed = True


def _resource(rtype, *, last_backup_at=None, tenant_id=None):
    return types.SimpleNamespace(
        id=uuid.uuid4(),
        tenant_id=tenant_id or _TENANT,
        type=types.SimpleNamespace(value=rtype),
        status="ACTIVE",
        display_name=f"{rtype}-res",
        last_backup_at=last_backup_at or datetime.utcnow() - timedelta(days=1),
        extra_data={},
        external_id=None,
    )


_TENANT = uuid.uuid4()

# Every backup_* / feature flag the Gold shape needs, all on.
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
        id=uuid.uuid4(), name="Gold", enabled=True,
        frequency=types.SimpleNamespace(value="THREE_DAILY"),
        max_concurrent_backups=max_concurrent,
        **_FLAGS,
    )


@pytest.fixture
def wired(monkeypatch):
    """Drive dispatch_policy_backups with fakes; return a runner."""
    dp = sched.dispatch_policy
    monkeypatch.setattr(dp, "build_scheduled_inflight_query",
                        lambda pid: _Q_INFLIGHT_JOBS)
    monkeypatch.setattr(dp, "build_inflight_resource_ids_query",
                        lambda: _Q_INFLIGHT_IDS)
    monkeypatch.setattr(dp, "build_inflight_resource_count_query",
                        lambda pid: _Q_INFLIGHT_COUNT)

    async def _noop(*a, **kw):
        return None

    monkeypatch.setattr(sched, "publish_cycle_audit_event", _noop)
    monkeypatch.setattr(sched, "publish_sla_skip_audit_events", _noop)
    monkeypatch.setattr(sched, "emit_backup_triggered", _noop)
    monkeypatch.setattr(sched.message_bus, "publish", _noop, raising=False)
    monkeypatch.setattr(sched.asyncio, "sleep", _noop)

    async def run(*, resources, inflight_ids, policy=None,
                  inflight_jobs=1, age_hours=1.0):
        policy = policy or _policy()
        session = _Session(
            policy, resources, inflight_ids, inflight_jobs,
            datetime.utcnow() - timedelta(hours=age_hours),
        )
        monkeypatch.setattr(sched, "async_session_factory", lambda: session)
        await sched.dispatch_policy_backups(str(policy.id))
        return session

    return run


def _specs(session):
    return [j.spec for j in session.added]


# ── the pins ───────────────────────────────────────────────────────────

async def test_overlap_held_ids_are_written_into_every_job_spec(wired):
    """The Gold 10:07 shape: a previous cycle is 1 h into its drain and
    still holds the USER_CHATS resources. Those must be withheld from
    dispatch (existing behaviour) AND recorded on every job of the batch,
    which is the only durable trace the Activity rollup can read."""
    users = [_resource("ENTRA_USER") for _ in range(7)]
    mail = [_resource("USER_MAIL") for _ in range(8)]
    chats = [_resource("USER_CHATS") for _ in range(8)]
    sites = [_resource("SHAREPOINT_SITE") for _ in range(2)]
    busy = chats + sites                       # "held 10 busy resource(s)"

    session = await wired(resources=users + mail + chats + sites,
                          inflight_ids={r.id for r in busy})

    assert session.added, "expected jobs to be dispatched, not a skipped tick"
    held_expected = sorted(str(r.id) for r in busy)
    dispatched = {str(rid) for j in session.added
                  for rid in j.batch_resource_ids}
    assert dispatched == {str(r.id) for r in users + mail}, (
        "the busy resources must still be withheld from dispatch"
    )
    for spec in _specs(session):
        assert sorted(spec["overlap_held_resource_ids"]) == held_expected, (
            "every job in the batch must carry the withheld ids — the "
            "rollup unions them per batch_id and any job missing them "
            "would leave the batch wedged"
        )


async def test_quiet_tick_records_an_empty_hold_not_a_missing_key(wired):
    """Absent key means "batch predates this writer" to the rollup, which
    deliberately keeps the old (conservative) behaviour. A tick that held
    nothing must therefore say so explicitly rather than omit the key."""
    users = [_resource("ENTRA_USER") for _ in range(3)]
    session = await wired(resources=users, inflight_ids=set(),
                          inflight_jobs=0)
    assert session.added
    for spec in _specs(session):
        assert "overlap_held_resource_ids" in spec
        assert spec["overlap_held_resource_ids"] == []


async def test_hold_and_cap_are_recorded_under_separate_keys(wired):
    """Two different reasons a resource sat out a cycle: the concurrency
    cap (capacity) and the overlap guard (duration). Merging them into
    one list would destroy the answer to "why did this not run?", so the
    keys stay distinct and each must carry only its own resources."""
    users = [_resource("ENTRA_USER") for _ in range(6)]
    chats = [_resource("USER_CHATS") for _ in range(2)]
    # cap of 4 with 2 in flight leaves a budget of 2 of the 6 users.
    policy = _policy(max_concurrent=4)
    session = await wired(resources=users + chats,
                          inflight_ids={r.id for r in chats},
                          policy=policy)

    assert session.added
    specs = _specs(session)
    held = {i for s in specs for i in s["overlap_held_resource_ids"]}
    deferred = {i for s in specs for i in s["deferred_resource_ids"]}
    assert held == {str(r.id) for r in chats}
    assert deferred, "the cap must still defer the over-budget users"
    assert not (held & deferred), (
        "a resource must be attributed to exactly one withhold reason"
    )
    assert deferred <= {str(r.id) for r in users}


async def test_held_resources_are_not_dropped_forever(wired):
    """Requirement (c): recording the hold must not make the resource
    invisible to the mechanism that eventually backs it up. Nothing here
    consumes the recorded ids — the next tick simply finds the resource
    idle and dispatches it under a new batch_id. Simulate that tick."""
    users = [_resource("ENTRA_USER") for _ in range(3)]
    chats = [_resource("USER_CHATS") for _ in range(2)]

    busy = await wired(resources=users + chats,
                       inflight_ids={r.id for r in chats})
    dispatched_now = {str(rid) for j in busy.added
                      for rid in j.batch_resource_ids}
    assert not (dispatched_now & {str(r.id) for r in chats})

    # next tick: the previous cycle has drained, nothing is in flight
    later = await wired(resources=users + chats, inflight_ids=set(),
                        inflight_jobs=0)
    dispatched_later = {str(rid) for j in later.added
                        for rid in j.batch_resource_ids}
    assert {str(r.id) for r in chats} <= dispatched_later, (
        "a withheld resource must be picked up by the following tick"
    )
    later_batches = {j.spec["batch_id"] for j in later.added}
    busy_batches = {j.spec["batch_id"] for j in busy.added}
    assert not (later_batches & busy_batches), (
        "the retry lands under a NEW batch_id — which is exactly why the "
        "first batch can never observe it and needs the recorded hold"
    )
