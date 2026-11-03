"""Pure cycle-window / wave helpers for the deferred-resource drain.

The drain must agree with the cron about when a cycle starts and ends, so
the window is derived from a real APScheduler CronTrigger — the same object
the cron job is registered with — never from hand-rolled cron math.
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import sys
import types
import uuid
from datetime import datetime, timezone

import pytest
from apscheduler.triggers.cron import CronTrigger

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret-value")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")

_HELPERS = (
    pathlib.Path(__file__).resolve().parents[2]
    / "services" / "backup-scheduler" / "dispatch_policy.py"
)


def _load():
    spec = importlib.util.spec_from_file_location(
        "backup_scheduler_dispatch_policy_cycle", _HELPERS
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


dp = _load()

SILVER = CronTrigger(hour=2, minute=39, timezone="UTC")            # DAILY
GOLD = CronTrigger(hour="2,10,18", minute=7, timezone="UTC")        # THREE_DAILY
WEEKDAYS = CronTrigger(hour=2, minute=39, day_of_week="mon-fri", timezone="UTC")


def _res(last_backup_at=None, rid=None):
    return types.SimpleNamespace(id=rid or uuid.uuid4(), last_backup_at=last_backup_at)


# ── cycle window from the real trigger ─────────────────────────────────

def test_daily_window_after_the_tick_starts_today_and_ends_tomorrow():
    w = dp.cycle_window_from_trigger(SILVER, datetime(2026, 9, 3, 3, 0))
    assert w.start == datetime(2026, 9, 3, 2, 39)
    assert w.end == datetime(2026, 9, 4, 2, 39)


def test_daily_window_before_the_tick_belongs_to_yesterday():
    w = dp.cycle_window_from_trigger(SILVER, datetime(2026, 9, 3, 2, 38))
    assert w.start == datetime(2026, 9, 2, 2, 39)
    assert w.end == datetime(2026, 9, 3, 2, 39)


def test_exactly_at_the_tick_opens_the_new_window():
    w = dp.cycle_window_from_trigger(SILVER, datetime(2026, 9, 3, 2, 39))
    assert w.start == datetime(2026, 9, 3, 2, 39)


def test_three_daily_window_is_eight_hours_wide():
    w = dp.cycle_window_from_trigger(GOLD, datetime(2026, 9, 3, 11, 0))
    assert w.start == datetime(2026, 9, 3, 10, 7)
    assert w.end == datetime(2026, 9, 3, 18, 7)


def test_weekday_policy_on_saturday_keeps_fridays_cycle_open_until_monday():
    # 2026-09-05 is a Saturday.
    w = dp.cycle_window_from_trigger(WEEKDAYS, datetime(2026, 9, 5, 12, 0))
    assert w.start == datetime(2026, 9, 4, 2, 39)
    assert w.end == datetime(2026, 9, 7, 2, 39)


def test_aware_now_is_accepted_and_result_is_naive_utc():
    w = dp.cycle_window_from_trigger(
        SILVER, datetime(2026, 9, 3, 3, 0, tzinfo=timezone.utc))
    assert w.start.tzinfo is None and w.start == datetime(2026, 9, 3, 2, 39)


# ── drainable? ─────────────────────────────────────────────────────────

def test_not_drainable_inside_the_grace_period_after_the_tick():
    w = dp.CycleWindow(datetime(2026, 9, 3, 2, 39), datetime(2026, 9, 4, 2, 39))
    assert not dp.cycle_is_drainable(w, now=datetime(2026, 9, 3, 2, 43, 59))
    assert dp.cycle_is_drainable(w, now=datetime(2026, 9, 3, 2, 44, 0))


def test_not_drainable_once_the_window_has_closed():
    w = dp.CycleWindow(datetime(2026, 9, 3, 2, 39), datetime(2026, 9, 4, 2, 39))
    assert dp.cycle_is_drainable(w, now=datetime(2026, 9, 4, 2, 38, 59))
    assert not dp.cycle_is_drainable(w, now=datetime(2026, 9, 4, 2, 39))


# ── backup window (HH:MM strings on the policy) ────────────────────────

def test_no_window_end_means_always_inside():
    assert dp.within_backup_window(
        now=datetime(2026, 9, 3, 15, 0), window_start="02:00", window_end=None)


def test_window_that_wraps_midnight():
    assert dp.within_backup_window(
        now=datetime(2026, 9, 3, 1, 0), window_start="22:00", window_end="04:00")
    assert not dp.within_backup_window(
        now=datetime(2026, 9, 3, 5, 0), window_start="22:00", window_end="04:00")


def test_unparseable_window_never_blocks_the_drain():
    assert dp.within_backup_window(
        now=datetime(2026, 9, 3, 5, 0), window_start="soon", window_end="later")


# ── env knobs ──────────────────────────────────────────────────────────

@pytest.mark.parametrize("raw", ["off", "0", "false", "no", " OFF "])
def test_drain_kill_switch(monkeypatch, raw):
    monkeypatch.setenv("BACKUP_CYCLE_DRAIN", raw)
    assert dp.drain_enabled() is False


def test_drain_is_on_by_default(monkeypatch):
    monkeypatch.delenv("BACKUP_CYCLE_DRAIN", raising=False)
    assert dp.drain_enabled() is True


def test_drain_tick_default_and_floor(monkeypatch):
    monkeypatch.delenv("BACKUP_DRAIN_TICK_S", raising=False)
    assert dp.drain_tick_seconds() == 120
    monkeypatch.setenv("BACKUP_DRAIN_TICK_S", "5")
    assert dp.drain_tick_seconds() == 30
    monkeypatch.setenv("BACKUP_DRAIN_TICK_S", "garbage")
    assert dp.drain_tick_seconds() == 120


# ── wave planning ──────────────────────────────────────────────────────

def test_wave_takes_the_first_budget_items():
    items = [_res() for _ in range(10)]
    assert dp.plan_wave(items, budget=4, max_concurrent=50) == items[:4]


def test_tiny_budget_waits_unless_the_tail_fits():
    items = [_res() for _ in range(100)]
    assert dp.plan_wave(items, budget=3, max_concurrent=50) == []      # floor is 10
    tail = items[:3]
    assert dp.plan_wave(tail, budget=3, max_concurrent=50) == tail


def test_zero_budget_or_nothing_remaining_is_an_empty_wave():
    assert dp.plan_wave([_res()], budget=0, max_concurrent=50) == []
    assert dp.plan_wave([], budget=50, max_concurrent=50) == []


def test_never_backed_up_ties_break_deterministically_by_id():
    a = _res(rid=uuid.UUID("00000000-0000-0000-0000-000000000001"))
    b = _res(rid=uuid.UUID("00000000-0000-0000-0000-000000000002"))
    assert sorted([b, a], key=dp.backup_priority_key) == [a, b]
    assert dp.backup_priority_key(a)[2] == str(a.id)
