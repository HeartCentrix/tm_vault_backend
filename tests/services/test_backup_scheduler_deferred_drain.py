"""Deferred-resource drain: waves within a cycle.

Drives the real drain_policy_cycle with a faked session / bus. Query
BUILDERS are replaced by sentinels (the fake session tells them apart by
identity); every DECISION helper stays real. The cycle window comes from a
real CronTrigger for a DAILY 02:39 UTC policy.
"""
from __future__ import annotations

import asyncio
import importlib.util
import os
import pathlib
import sys
import types
import uuid
from datetime import datetime, timedelta

import pytest
from apscheduler.triggers.cron import CronTrigger

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret-value")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")

_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _load_scheduler():
    path = _ROOT / "services" / "backup-scheduler" / "main.py"
    spec = importlib.util.spec_from_file_location(
        "backup_scheduler_main_deferred_drain", path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


sched = _load_scheduler()

_Q_INFLIGHT_COUNT = "Q:inflight_count"
_Q_INFLIGHT_IDS = "Q:inflight_ids"
_Q_CANDIDATES = "Q:candidates"
_Q_BATCH_ID = "Q:batch_id"
_Q_MAX_WAVE = "Q:max_wave"
_Q_POLICIES = "Q:policies"

_TENANT = uuid.uuid4()
SILVER_TRIGGER = CronTrigger(hour=2, minute=39, timezone="UTC")
# 03:30 UTC: 51 min into the 02:39 cycle, past the 5-min grace.
NOW = datetime(2026, 9, 4, 3, 30)
WINDOW_END = datetime(2026, 9, 5, 2, 39)


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows

    def scalar(self):
        if not self._rows:
            return None
        row = self._rows[0]
        return row[0] if isinstance(row, tuple) else row


class _Session:
    def __init__(self, policy, *, candidates, inflight_ids=(), inflight_count=0,
                 batch_id="cycle-batch", max_wave=1, events=None):
        self.policy = policy
        self.candidates = list(candidates)
        self.inflight_ids = list(inflight_ids)
        self.inflight_count = inflight_count
        self.batch_id = batch_id
        self.max_wave = max_wave
        self.events = events if events is not None else []
        self.added = []
        self.committed = False
        self.seen = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def get(self, model, ident):
        return self.policy

    async def execute(self, q):
        self.seen.append(q)
        if q is _Q_INFLIGHT_COUNT:
            return _Result([(self.inflight_count,)])
        if q is _Q_INFLIGHT_IDS:
            return _Result([(rid,) for rid in self.inflight_ids])
        if q is _Q_CANDIDATES:
            return _Result(self.candidates)          # Row-like objects
        if q is _Q_BATCH_ID:
            return _Result([(self.batch_id,)] if self.batch_id else [])
        if q is _Q_MAX_WAVE:
            return _Result([(self.max_wave,)])
        if q is _Q_POLICIES:
            return _Result([(self.policy.id,)])
        raise AssertionError(f"unexpected query {q!r}")

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


def _policy(max_concurrent=50):
    return types.SimpleNamespace(
        id=uuid.uuid4(), name="Silver", enabled=True, frequency="DAILY",
        max_concurrent_backups=max_concurrent,
        backup_days=["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"],
        backup_window_start=None, backup_window_end=None, **_FLAGS,
    )


def _row(rtype="USER_MAIL", last_backup_at=None):
    """Light projection row as build_drain_candidates_query returns it."""
    return types.SimpleNamespace(
        id=uuid.uuid4(), type=types.SimpleNamespace(value=rtype),
        tenant_id=_TENANT, last_backup_at=last_backup_at, display_name=f"{rtype}-res",
    )


@pytest.fixture
def wired(monkeypatch):
    dp = sched.dispatch_policy
    monkeypatch.setattr(dp, "build_inflight_resource_count_query", lambda pid: _Q_INFLIGHT_COUNT)
    monkeypatch.setattr(dp, "build_inflight_resource_ids_query", lambda: _Q_INFLIGHT_IDS)
    monkeypatch.setattr(dp, "build_drain_candidates_query", lambda pid, since, types_: _Q_CANDIDATES)
    monkeypatch.setattr(dp, "build_cycle_batch_id_query", lambda pid, since: _Q_BATCH_ID)
    monkeypatch.setattr(dp, "build_cycle_max_wave_query", lambda pid, since: _Q_MAX_WAVE)
    monkeypatch.setattr(dp, "build_drainable_policies_query", lambda: _Q_POLICIES)
    monkeypatch.setattr(sched, "_policy_cycle_trigger", lambda policy: SILVER_TRIGGER)
    monkeypatch.delenv("BACKUP_CYCLE_DRAIN", raising=False)
    sched._cycle_exhausted_until.clear()
    sched._cycle_incomplete_emitted.clear()

    events: list = []
    audits: list = []

    async def _publish(queue, message, priority=None):
        events.append("publish")

    async def _audit(policy, action, details, tenant_id=None, **kw):
        audits.append((action, details, kw.get("outcome", "PARTIAL")))

    async def _noop(*a, **kw):
        return None

    monkeypatch.setattr(sched, "publish_cycle_audit_event", _audit)
    monkeypatch.setattr(sched, "emit_backup_triggered", _noop)
    monkeypatch.setattr(sched.message_bus, "publish", _publish, raising=False)
    monkeypatch.setattr(sched.asyncio, "sleep", _noop)

    def make(policy=None, **kw):
        policy = policy or _policy()
        session = _Session(policy, events=events, **kw)
        monkeypatch.setattr(sched, "async_session_factory", lambda: session)
        return policy, session

    return types.SimpleNamespace(make=make, events=events, audits=audits)


def _ids(session):
    out = []
    for j in session.added:
        out.extend(str(x) for x in j.batch_resource_ids)
    return out


async def test_wave_dispatches_the_next_budget_and_records_the_remainder(wired):
    policy, session = wired.make(candidates=[_row() for _ in range(306)])
    n = await sched.drain_policy_cycle(str(policy.id), now=NOW)
    assert n == 50 and len(_ids(session)) == 50
    spec = session.added[0].spec
    assert spec["batch_id"] == "cycle-batch" and spec["wave"] == 2
    assert spec["triggered_by"] == "SCHEDULED" and spec["sla_policy_id"] == str(policy.id)
    assert len(spec["deferred_resource_ids"]) == 256
    assert spec["overlap_held_resource_ids"] == []
    assert session.committed and wired.events.index("commit") < wired.events.index("publish")
    actions = [a for a, _, _ in wired.audits]
    assert actions == ["BACKUP_CYCLE_WAVE_DISPATCHED"]
    _, details, outcome = wired.audits[0]
    assert outcome == "SUCCESS" and details["wave"] == 2
    assert "resource_ids" not in details and "resources" not in details


async def test_cycle_anchor_is_the_window_start(wired, monkeypatch):
    """``since`` must be the window start itself — cron Job rows are
    created by this same process seconds AFTER the fire time, so
    created_at >= window.start already carries them. A -60s anchor
    overlapped the previous cycle's closing band and mis-carried its
    last wave into the new cycle under the OLD batch_id/wave numbering."""
    dp = sched.dispatch_policy
    recorded: list = []

    def _candidates(pid, since, types_):
        recorded.append(since)
        return _Q_CANDIDATES

    def _batch_id(pid, since):
        recorded.append(since)
        return _Q_BATCH_ID

    def _max_wave(pid, since):
        recorded.append(since)
        return _Q_MAX_WAVE

    monkeypatch.setattr(dp, "build_drain_candidates_query", _candidates)
    monkeypatch.setattr(dp, "build_cycle_batch_id_query", _batch_id)
    monkeypatch.setattr(dp, "build_cycle_max_wave_query", _max_wave)

    policy, session = wired.make(candidates=[_row() for _ in range(10)])
    n = await sched.drain_policy_cycle(str(policy.id), now=NOW)
    assert n == 10
    assert recorded, "none of the since-taking builders were invoked"
    assert all(since == datetime(2026, 9, 4, 2, 39) for since in recorded)


async def test_exhausted_cycle_adds_nothing_and_memoizes(wired):
    policy, session = wired.make(candidates=[])
    assert await sched.drain_policy_cycle(str(policy.id), now=NOW) == 0
    assert session.added == [] and wired.audits == []
    assert sched._cycle_exhausted_until[str(policy.id)] == WINDOW_END
    # second tick: nothing touches resources
    session.seen.clear()
    assert await sched.drain_policy_cycle(str(policy.id), now=NOW + timedelta(minutes=2)) == 0
    assert _Q_CANDIDATES not in session.seen


async def test_cron_tick_clears_the_exhausted_memo(wired, monkeypatch):
    policy, session = wired.make(candidates=[])
    await sched.drain_policy_cycle(str(policy.id), now=NOW)
    assert str(policy.id) in sched._cycle_exhausted_until

    async def _impl(policy_id, respect_overlap_guard=True):
        return None

    monkeypatch.setattr(sched, "_dispatch_policy_backups_impl", _impl)
    await sched.dispatch_policy_backups(str(policy.id))
    assert str(policy.id) not in sched._cycle_exhausted_until


async def test_no_budget_never_loads_candidates(wired):
    policy, session = wired.make(candidates=[_row() for _ in range(10)], inflight_count=50)
    assert await sched.drain_policy_cycle(str(policy.id), now=NOW) == 0
    assert _Q_CANDIDATES not in session.seen and session.added == []


async def test_in_flight_resources_are_held_not_dispatched(wired):
    rows = [_row() for _ in range(60)]
    busy = [str(r.id) for r in rows[:5]]
    policy, session = wired.make(candidates=rows, inflight_ids=busy, inflight_count=5)
    n = await sched.drain_policy_cycle(str(policy.id), now=NOW)
    assert n == 45  # budget = 50 - 5
    dispatched = set(_ids(session))
    assert dispatched.isdisjoint(busy)
    assert set(session.added[0].spec["overlap_held_resource_ids"]) == set(busy)


async def test_tiny_budget_waits_for_the_wave_floor(wired):
    policy, session = wired.make(candidates=[_row() for _ in range(100)], inflight_count=47)
    assert await sched.drain_policy_cycle(str(policy.id), now=NOW) == 0
    assert session.added == [] and wired.audits == []


async def test_tail_that_fits_a_tiny_budget_is_dispatched(wired):
    policy, session = wired.make(candidates=[_row() for _ in range(3)], inflight_count=47)
    assert await sched.drain_policy_cycle(str(policy.id), now=NOW) == 3


async def test_outside_the_window_is_a_noop(wired):
    policy, session = wired.make(candidates=[_row() for _ in range(10)])
    # 02:41 — inside the 5-min grace after the 02:39 tick
    assert await sched.drain_policy_cycle(str(policy.id), now=datetime(2026, 9, 4, 2, 41)) == 0
    assert session.seen == []


async def test_backup_window_end_is_honoured(wired):
    policy = _policy()
    policy.backup_window_start = "02:00"
    policy.backup_window_end = "03:00"
    policy, session = wired.make(policy=policy, candidates=[_row() for _ in range(10)])
    assert await sched.drain_policy_cycle(str(policy.id), now=NOW) == 0   # 03:30 is past 03:00


async def test_kill_switch(wired, monkeypatch):
    monkeypatch.setenv("BACKUP_CYCLE_DRAIN", "off")
    policy, session = wired.make(candidates=[_row() for _ in range(10)])
    assert await sched.drain_policy_cycle(str(policy.id), now=NOW) == 0
    assert session.seen == []


async def test_uncapped_policy_is_never_drained(wired):
    policy, session = wired.make(policy=_policy(max_concurrent=0), candidates=[_row()])
    assert await sched.drain_policy_cycle(str(policy.id), now=NOW) == 0
    assert session.seen == []


async def test_drain_opens_the_cycle_when_the_cron_left_no_batch(wired):
    policy, session = wired.make(candidates=[_row() for _ in range(10)], batch_id=None, max_wave=0)
    n = await sched.drain_policy_cycle(str(policy.id), now=NOW)
    assert n == 10
    spec = session.added[0].spec
    uuid.UUID(spec["batch_id"])  # fresh, valid
    assert spec["wave"] == 1
    assert wired.audits[0][1]["started_cycle"] is True


async def test_never_backed_up_go_first_in_the_wave(wired):
    fresh = [_row(last_backup_at=datetime(2026, 9, 1)) for _ in range(30)]
    never = [_row() for _ in range(30)]
    policy, session = wired.make(candidates=fresh + never)
    await sched.drain_policy_cycle(str(policy.id), now=NOW)
    dispatched = set(_ids(session))
    assert {str(r.id) for r in never} <= dispatched
    assert len(dispatched) == 50


async def test_cycle_incomplete_is_emitted_once_when_the_window_closes_with_work_left(wired):
    closing = WINDOW_END - timedelta(seconds=60)
    policy, session = wired.make(candidates=[_row() for _ in range(10)], inflight_count=50)
    assert await sched.drain_policy_cycle(str(policy.id), now=closing) == 0
    actions = [a for a, _, _ in wired.audits]
    assert actions == ["BACKUP_CYCLE_INCOMPLETE"]
    assert wired.audits[0][1]["remaining_count"] == 10
    # not repeated on the next tick of the same cycle
    await sched.drain_policy_cycle(str(policy.id), now=closing + timedelta(seconds=30))
    assert [a for a, _, _ in wired.audits] == ["BACKUP_CYCLE_INCOMPLETE"]


async def test_cycle_incomplete_is_not_emitted_when_nothing_is_left(wired):
    closing = WINDOW_END - timedelta(seconds=60)
    policy, session = wired.make(candidates=[], inflight_count=50)
    await sched.drain_policy_cycle(str(policy.id), now=closing)
    assert wired.audits == []


async def test_last_tick_dispatches_the_tail_below_the_floor_then_reports_the_rest(wired):
    """On the closing tick the wave floor would block a 3-slot budget
    against 100 candidates forever — bypass it so free capacity is used,
    then report the true remainder as BACKUP_CYCLE_INCOMPLETE."""
    closing = WINDOW_END - timedelta(seconds=60)
    policy, session = wired.make(candidates=[_row() for _ in range(100)], inflight_count=47)
    n = await sched.drain_policy_cycle(str(policy.id), now=closing)
    assert n == 3
    actions = [a for a, _, _ in wired.audits]
    assert actions == ["BACKUP_CYCLE_WAVE_DISPATCHED", "BACKUP_CYCLE_INCOMPLETE"]
    assert wired.audits[1][1]["remaining_count"] == 97
    assert wired.audits[1][1]["batch_id"] == "cycle-batch"


async def test_a_tick_before_the_last_one_keeps_the_floor_and_stays_quiet(wired):
    """Same budget/backlog, but not yet the last tick before close — the
    wave floor still applies and nothing is reported."""
    not_yet_closing = WINDOW_END - timedelta(seconds=200)
    policy, session = wired.make(candidates=[_row() for _ in range(100)], inflight_count=47)
    n = await sched.drain_policy_cycle(str(policy.id), now=not_yet_closing)
    assert n == 0
    assert wired.audits == []


async def test_incomplete_on_a_drain_opened_cycle_carries_the_fresh_batch_id(wired):
    """When the drain itself opens the cycle on the closing tick, the
    INCOMPLETE event must reference the SAME fresh batch_id the wave was
    dispatched under, not the (nonexistent) looked-up one."""
    closing = WINDOW_END - timedelta(seconds=60)
    policy, session = wired.make(
        candidates=[_row() for _ in range(100)], inflight_count=47,
        batch_id=None, max_wave=0,
    )
    n = await sched.drain_policy_cycle(str(policy.id), now=closing)
    assert n == 3
    actions = [a for a, _, _ in wired.audits]
    assert actions == ["BACKUP_CYCLE_WAVE_DISPATCHED", "BACKUP_CYCLE_INCOMPLETE"]
    wave_batch_id = wired.audits[0][1]["batch_id"]
    incomplete_batch_id = wired.audits[1][1]["batch_id"]
    uuid.UUID(wave_batch_id)  # fresh, valid
    assert wave_batch_id == incomplete_batch_id


async def test_drain_policy_cycles_runs_every_drainable_policy_and_swallows_errors(wired, monkeypatch):
    policy, session = wired.make(candidates=[])
    calls = []

    async def _one(pid, **kw):
        calls.append(pid)
        raise RuntimeError("boom")

    monkeypatch.setattr(sched, "drain_policy_cycle", _one)
    await sched.drain_policy_cycles()   # must not raise
    assert calls == [str(policy.id)]


def test_policy_cycle_trigger_falls_back_when_the_job_is_not_registered(monkeypatch):
    """No cron job registered for the policy (unit test, or a startup
    race) — build the trigger from frequency_to_cron_params directly.
    That dict is shaped for scheduler.add_job (it carries a "trigger"
    key), which apscheduler.triggers.cron.CronTrigger.__init__ does not
    accept — the fallback must strip it before constructing the trigger."""
    monkeypatch.setattr(sched.scheduler, "get_job", lambda job_id: None)
    policy = _policy()
    trigger = sched._policy_cycle_trigger(policy)
    assert isinstance(trigger, CronTrigger)
    window = sched.dispatch_policy.cycle_window_from_trigger(
        trigger, datetime(2026, 9, 4, 3, 30))
    assert window.start.year == 2026 and window.start.month == 9 and window.start.day == 4
    assert window.start.hour == 2
    assert window.end - window.start == timedelta(hours=24)


def test_drain_tick_is_registered_with_safe_apscheduler_options():
    src = (_ROOT / "services" / "backup-scheduler" / "main.py").read_text()
    block = src[src.index("scheduler.add_job(\n        drain_policy_cycles"):]
    block = block[:block.index(")\n", block.index("misfire_grace_time"))]
    for needle in ('id="policy_cycle_drain"', "replace_existing=True",
                   "max_instances=1", "coalesce=True", "misfire_grace_time=30",
                   "dispatch_policy.drain_tick_seconds()"):
        assert needle in block, needle
