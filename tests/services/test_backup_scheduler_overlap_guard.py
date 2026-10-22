"""Run-overlap guard + concurrency-cap unit correctness for backup-scheduler.

Root cause (2026-08-05 RCA, Gold/THREE_DAILY):

  1. NO run-overlap guard existed. ``dispatch_policy_backups`` only
     publishes to RabbitMQ and returns in seconds, so APScheduler's
     default ``max_instances=1`` never engages — cycle N+1 always fired
     on schedule even with cycle N still draining a 62 GB SharePoint
     partition 12 h in.

  2. ``max_concurrent_backups`` had a UNIT MISMATCH: ``inflight`` counted
     *Jobs* (~10 per cycle) while ``budget`` was spent on *resources*
     (cap 50). The ``budget == 0`` defer path needed 50 in-flight jobs —
     five fully-stalled cycles — so it was unreachable.

  3. Gold has exactly 50 enabled resources and a cap of 50, so the
     guard's only observable effect was an off-by-N trim: one RUNNING
     job ⇒ ``budget = 49`` ⇒ one resource SILENTLY dropped from the
     cycle, with no log line and no audit event. Verified live: ticks
     with a prior job in flight dispatched 49, all others 50.

  4. The dropped resource then read as lost fan-out to the Activity
     rollup (``missing_t2``), wedging the row at "In Progress · 15 %"
     for the full 48 h ``batch_recent`` horizon even though every job
     and snapshot was terminal. That is the "previous run still going"
     ghost the operator reported.

These tests pin the decision logic, which is why it lives in pure
helpers rather than inline in the async dispatch body.
"""
from __future__ import annotations

import importlib.util
import pathlib
import sys
import types
import uuid
from datetime import datetime, timedelta, timezone

import pytest


# ─── module loading ────────────────────────────────────────────────────
# services/backup-scheduler/ is not an importable package name (hyphen),
# and importing main.py executes FastAPI/APScheduler wiring. The pure
# helpers live in a sibling module with no such side effects.
_HELPERS = (
    pathlib.Path(__file__).resolve().parents[2]
    / "services" / "backup-scheduler" / "dispatch_policy.py"
)


def _load():
    spec = importlib.util.spec_from_file_location(
        "backup_scheduler_dispatch_policy", _HELPERS
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


dp = _load()


def _res(last_backup_at=None, rid=None, rtype="USER_MAIL", name=None):
    """Minimal Resource stand-in — the helpers only read these fields."""
    return types.SimpleNamespace(
        id=rid or uuid.uuid4(),
        tenant_id=uuid.uuid4(),
        last_backup_at=last_backup_at,
        type=types.SimpleNamespace(value=rtype),
        display_name=name or "res",
        status=types.SimpleNamespace(value="ACTIVE"),
    )


# ─── cadence → interval ────────────────────────────────────────────────

def test_three_daily_interval_is_eight_hours():
    assert dp.frequency_interval_hours("THREE_DAILY") == 8


def test_unknown_frequency_falls_back_to_daily():
    # Must never return 0 — a 0 interval makes every cycle look stale and
    # would disable the overlap guard outright.
    assert dp.frequency_interval_hours("SOMETHING_NEW") == 24
    assert dp.frequency_interval_hours(None) == 24


def test_stale_horizon_never_below_a_full_day():
    """THREE_DAILY x3 = 24 h. A legitimate Gold cycle has been observed
    at 13 h 52 m, so a horizon tighter than a day would override the
    guard on healthy runs and re-create the pile-up."""
    assert dp.overlap_stale_after_hours("THREE_DAILY", multiplier=3) == 24
    # A tiny multiplier must still floor at 24 h.
    assert dp.overlap_stale_after_hours("THREE_DAILY", multiplier=0.1) == 24
    # Long cadences scale up rather than clamping down.
    assert dp.overlap_stale_after_hours("WEEKLY", multiplier=3) == 504


# ─── the overlap decision ──────────────────────────────────────────────

def test_no_inflight_work_dispatches():
    assert dp.decide_overlap_action(
        inflight_jobs=0, oldest_created_at=None,
        now=datetime(2026, 8, 5, 2, 7), stale_after_hours=24,
    ) == "dispatch"


def test_inflight_scheduled_cycle_skips_the_tick():
    """THE headline fix: the Aug 5 02:07 UTC tick must not fire while the
    Aug 4 18:07 cycle is still draining."""
    now = datetime(2026, 8, 5, 2, 7)
    assert dp.decide_overlap_action(
        inflight_jobs=1, oldest_created_at=now - timedelta(hours=8),
        stale_after_hours=24, now=now,
    ) == "skip"


def test_wedged_cycle_past_horizon_overrides_rather_than_halting_backups():
    """A permanently stuck job must NOT silently stop every future backup
    — that would turn one wedged shard into total data-protection loss.
    Past the horizon we dispatch anyway and say so loudly."""
    now = datetime(2026, 8, 6, 2, 7)
    assert dp.decide_overlap_action(
        inflight_jobs=1, oldest_created_at=now - timedelta(hours=25),
        stale_after_hours=24, now=now,
    ) == "override"


def test_horizon_boundary_is_exclusive_so_a_healthy_run_still_skips():
    now = datetime(2026, 8, 5, 12, 0)
    assert dp.decide_overlap_action(
        inflight_jobs=1, oldest_created_at=now - timedelta(hours=24),
        stale_after_hours=24, now=now,
    ) == "skip"


def test_guard_can_be_disabled_for_a_rollback():
    now = datetime(2026, 8, 5, 2, 7)
    assert dp.decide_overlap_action(
        inflight_jobs=3, oldest_created_at=now - timedelta(hours=1),
        stale_after_hours=24, now=now, guard_enabled=False,
    ) == "dispatch"


def test_missing_timestamp_does_not_crash_and_prefers_skipping():
    """oldest_created_at can be NULL if a job row was written without
    created_at (legacy rows). Unknown age must not be treated as stale —
    that would silently reinstate the overlap."""
    assert dp.decide_overlap_action(
        inflight_jobs=1, oldest_created_at=None,
        stale_after_hours=24, now=datetime(2026, 8, 5, 2, 7),
    ) == "skip"


def test_timezone_aware_job_timestamp_is_comparable():
    """jobs.created_at is `timestamp without time zone` but lease columns
    are `with time zone`; a mixed-awareness comparison raises TypeError
    and would crash every dispatch."""
    now = datetime(2026, 8, 6, 2, 7)
    got = dp.decide_overlap_action(
        inflight_jobs=1,
        oldest_created_at=(now - timedelta(hours=25)).replace(tzinfo=timezone.utc),
        stale_after_hours=24, now=now,
    )
    assert got == "override"


# ─── concurrency-cap budget (the unit-mismatch fix) ────────────────────

def test_cap_of_zero_means_unlimited_and_defers_nothing():
    resources = [_res() for _ in range(5)]
    dispatch, deferred = dp.plan_concurrency_budget(
        resources, max_concurrent=0, inflight_resources=99,
    )
    assert len(dispatch) == 5 and deferred == []


def test_budget_is_spent_in_resources_not_jobs():
    """The bug: 10 in-flight JOBS carrying 50 resources used to read as
    'inflight = 10', leaving budget 40 when zero capacity remained."""
    resources = [_res() for _ in range(50)]
    dispatch, deferred = dp.plan_concurrency_budget(
        resources, max_concurrent=50, inflight_resources=50,
    )
    assert dispatch == [] and len(deferred) == 50


def test_full_capacity_dispatches_everything_and_defers_nothing():
    """Gold's steady state: 50 enabled resources, cap 50, nothing in
    flight. Must dispatch all 50 — the off-by-N trim is what created
    missing_t2 and the 48 h 'In Progress' ghost."""
    resources = [_res() for _ in range(50)]
    dispatch, deferred = dp.plan_concurrency_budget(
        resources, max_concurrent=50, inflight_resources=0,
    )
    assert len(dispatch) == 50 and deferred == []


def test_partial_budget_defers_the_freshest_backups_first():
    """Oldest last_backup_at is the most SLA-behind, so it must win the
    budget; the resource backed up most recently is the one deferred."""
    old = _res(last_backup_at=datetime(2026, 8, 1), name="oldest")
    mid = _res(last_backup_at=datetime(2026, 8, 3), name="mid")
    fresh = _res(last_backup_at=datetime(2026, 8, 5), name="freshest")
    dispatch, deferred = dp.plan_concurrency_budget(
        [fresh, old, mid], max_concurrent=3, inflight_resources=1,
    )
    assert [r.display_name for r in dispatch] == ["oldest", "mid"]
    assert [r.display_name for r in deferred] == ["freshest"]


def test_never_backed_up_resources_get_absolute_priority():
    never = _res(last_backup_at=None, name="never")
    backed = _res(last_backup_at=datetime(2020, 1, 1), name="backed")
    dispatch, deferred = dp.plan_concurrency_budget(
        [backed, never], max_concurrent=1, inflight_resources=0,
    )
    assert [r.display_name for r in dispatch] == ["never"]
    assert [r.display_name for r in deferred] == ["backed"]


def test_mixed_timezone_awareness_does_not_crash_the_sort():
    """`sort(key=lambda r: r.last_backup_at or datetime.min)` raises
    TypeError the moment one row is tz-aware. That aborts the whole
    dispatch, so every policy resource silently misses its window."""
    naive = _res(last_backup_at=datetime(2026, 8, 1), name="naive")
    aware = _res(
        last_backup_at=datetime(2026, 8, 2, tzinfo=timezone.utc), name="aware",
    )
    dispatch, deferred = dp.plan_concurrency_budget(
        [aware, naive], max_concurrent=1, inflight_resources=0,
    )
    assert [r.display_name for r in dispatch] == ["naive"]
    assert [r.display_name for r in deferred] == ["aware"]


def test_inflight_above_cap_never_yields_a_negative_budget():
    resources = [_res() for _ in range(3)]
    dispatch, deferred = dp.plan_concurrency_budget(
        resources, max_concurrent=10, inflight_resources=999,
    )
    assert dispatch == [] and len(deferred) == 3


# ─── SQL contracts ─────────────────────────────────────────────────────

def test_scheduled_inflight_query_counts_resources_not_job_rows():
    from sqlalchemy.dialects import postgresql
    sql = str(
        dp.build_inflight_resource_count_query(str(uuid.uuid4())).compile(
            dialect=postgresql.dialect()
        )
    )
    low = sql.lower()
    assert "unnest(jobs.batch_resource_ids)" in low and "count(distinct" in low, (
        "budget is spent in resources — the in-flight side must count "
        "distinct batch members, not job rows"
    )
    assert "array_length" not in low, (
        "array_length(batch_resource_ids) is job-granular: it kept a bulk "
        "job's COMPLETED members charged against the cap until its slowest "
        "member finished (2026-10-14, Gold USER_CHATS) — see "
        "test_backup_scheduler_inflight_granularity.py"
    )
    assert "->>" in sql  # Job.spec is plain JSON, not JSONB


def test_overlap_query_ignores_manual_and_preemptive_work():
    """A manual 'Backup now' click or a preemptive anomaly backup must
    never suppress a scheduled cycle. Those paths stamp
    triggered_by=MANUAL / PREEMPTIVE and carry no sla_policy_id."""
    from sqlalchemy.dialects import postgresql
    # JSON key names are bound params, so they only surface with
    # literal_binds — assert on the fully-rendered statement.
    sql = str(
        dp.build_scheduled_inflight_query(str(uuid.uuid4())).compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )
    assert "triggered_by" in sql and "'SCHEDULED'" in sql, (
        "overlap guard must restrict itself to SCHEDULED jobs"
    )
    assert "sla_policy_id" in sql


# ─── wiring pins (main.py can't be imported: no fastapi in unit env) ───

def _scheduler_src() -> str:
    return (
        pathlib.Path(__file__).resolve().parents[2]
        / "services" / "backup-scheduler" / "main.py"
    ).read_text()


def test_cron_path_keeps_the_guard_but_manual_trigger_bypasses_it():
    """An explicit operator trigger must not be silently swallowed
    because a cycle is in flight — that reads as a broken button. The
    cron path keeps the default (guard on)."""
    src = _scheduler_src()
    trigger = src[src.index("async def trigger_policy_backup"):]
    trigger = trigger[:trigger.index("@app.post", 1)]
    assert "dispatch_policy_backups, policy_id, False" in trigger, (
        "manual /trigger must pass respect_overlap_guard=False"
    )
    # The APScheduler registration must NOT pass False.
    cron = src[src.index("scheduler.add_job(\n        dispatch_policy_backups"):]
    cron = cron[:cron.index(")\n", cron.index("**cron_params"))]
    assert "False" not in cron, "cron path must keep the overlap guard on"


def test_concurrency_cap_runs_after_the_power_bi_prefilter():
    """Ordering matters: the cap used to spend budget on resources the
    Power BI modified-workspace prefilter then dropped, so a deferral was
    recorded while capacity went unused."""
    src = _scheduler_src()
    assert src.index("power_bi_resources = [resource") < src.index(
        "# R2.2 — honor max_concurrent_backups"
    ), "the concurrency cap must run after all resource filtering"


def test_overlap_guard_runs_before_the_resource_fetch():
    """A skipped tick should cost one COUNT, not a full resource load
    plus a Power BI Graph walk."""
    src = _scheduler_src()
    assert src.index("# R2.3 — run-overlap guard") < src.index(
        "# Fetch all active resources assigned to this policy"
    )


def test_deferred_ids_are_stamped_into_every_job_spec():
    """The Activity rollup reads spec.deferred_resource_ids to tell a
    DELIBERATE deferral from a LOST fan-out. Without the stamp the row
    wedges at 'In Progress' for the full 48h horizon."""
    src = _scheduler_src()
    assert '"deferred_resource_ids": deferred_resource_ids,' in src


def test_deferrals_are_audited_not_just_logged():
    src = _scheduler_src()
    assert '"BACKUP_DEFERRED_CONCURRENCY_CAP"' in src
    assert '"BACKUP_CYCLE_SKIPPED_OVERLAP"' in src
    assert '"BACKUP_CYCLE_OVERLAP_OVERRIDE"' in src


def test_new_audit_actions_are_registered_as_activity_warnings():
    """An unregistered action never reaches the Activity feed, so the
    coverage gap would stay as invisible as it was before the fix."""
    audit_src = (
        pathlib.Path(__file__).resolve().parents[2]
        / "services" / "audit-service" / "main.py"
    ).read_text()
    warn_block = audit_src[
        audit_src.index("WARNING_ACTIVITY_ACTIONS = {"):
        audit_src.index("# RANSOMWARE_SIGNAL deliberately excluded")
    ]
    for action in (
        "BACKUP_CYCLE_SKIPPED_OVERLAP",
        "BACKUP_CYCLE_OVERLAP_OVERRIDE",
        "BACKUP_DEFERRED_CONCURRENCY_CAP",
    ):
        assert action in warn_block, f"{action} missing from WARNING_ACTIVITY_ACTIONS"
        assert f'"{action}":' in audit_src, f"{action} missing from ACTIONS"


def test_wave_and_incomplete_actions_are_registered_and_classified():
    """A wave is normal progress (must NOT render as a warning); a cycle
    that closes with work never dispatched IS a coverage gap (must)."""
    audit_src = (
        pathlib.Path(__file__).resolve().parents[2]
        / "services" / "audit-service" / "main.py"
    ).read_text()
    warn_block = audit_src[
        audit_src.index("WARNING_ACTIVITY_ACTIONS = {"):
        audit_src.index("# RANSOMWARE_SIGNAL deliberately excluded")
    ]
    assert '"BACKUP_CYCLE_WAVE_DISPATCHED":' in audit_src
    assert '"BACKUP_CYCLE_INCOMPLETE":' in audit_src
    assert "BACKUP_CYCLE_INCOMPLETE" in warn_block
    assert "BACKUP_CYCLE_WAVE_DISPATCHED" not in warn_block


def test_scheduler_emits_the_new_actions():
    src = _scheduler_src()
    assert '"BACKUP_CYCLE_WAVE_DISPATCHED"' in src
    assert '"BACKUP_CYCLE_INCOMPLETE"' in src


# ─── SLA violation classification (round-3 findings) ───────────────────

def test_manual_policies_have_no_sla_cadence():
    """`Manual` is enabled with sla_violation_alert=True in prod but is
    NEVER scheduled (frequency_to_cron_params returns None for MANUAL).
    Measuring it against a fabricated 24h cadence makes every resource on
    it a permanent, unfixable violation."""
    assert dp.policy_has_sla_cadence("THREE_DAILY") is True
    assert dp.policy_has_sla_cadence("DAILY") is True
    assert dp.policy_has_sla_cadence("MANUAL") is False
    assert dp.policy_has_sla_cadence("manual") is False
    assert dp.policy_has_sla_cadence(None) is False


def test_monthly_cadence_is_not_treated_as_daily():
    """check_sla_violations' inline map had no MONTHLY entry, so it fell
    back to 24h and reported a violation every single day."""
    assert dp.frequency_interval_hours("MONTHLY") == 720


def test_never_backed_up_resource_is_a_critical_violation():
    """`if resource.last_backup_at:` silently skipped resources that have
    NEVER been captured — the highest-risk case on a backup product was
    the one case that produced no alert. Confirmed live: one ROOM_MAILBOX
    on an alerting policy with last_backup_at IS NULL."""
    assert dp.classify_sla_violation(
        hours_since_backup=None, frequency_hours=8, never_backed_up=True,
    ) == "CRITICAL"


def test_never_backed_up_but_first_capture_running_is_only_a_warning():
    assert dp.classify_sla_violation(
        hours_since_backup=None, frequency_hours=8,
        never_backed_up=True, backup_in_progress=True,
    ) == "WARNING"


def test_within_sla_is_not_a_violation():
    assert dp.classify_sla_violation(
        hours_since_backup=3, frequency_hours=8,
    ) is None
    # Exactly at the boundary is still compliant.
    assert dp.classify_sla_violation(
        hours_since_backup=8, frequency_hours=8,
    ) is None


def test_overdue_escalates_to_critical_past_double_the_window():
    assert dp.classify_sla_violation(
        hours_since_backup=9, frequency_hours=8,
    ) == "WARNING"
    assert dp.classify_sla_violation(
        hours_since_backup=17, frequency_hours=8,
    ) == "CRITICAL"


def test_in_flight_backup_never_escalates_to_critical():
    """The live noise source: IT OPS NOTES (62GB SharePoint partition,
    12h into an actively-progressing run) fired
    '[SLA VIOLATION] CRITICAL ... 16.1h ago' every 30 minutes. Data
    protection IS happening — surface it, don't page on it. The overlap
    guard makes this worse (skipped cycles age last_backup_at further),
    so the two fixes ship together."""
    assert dp.classify_sla_violation(
        hours_since_backup=16.1, frequency_hours=8, backup_in_progress=True,
    ) == "WARNING"


def test_inflight_resource_ids_query_covers_batch_and_single_job_shapes():
    from sqlalchemy.dialects import postgresql
    sql = str(
        dp.build_inflight_resource_ids_query().compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    ).lower()
    assert "unnest" in sql, "batch-shape jobs carry batch_resource_ids"
    assert "resource_id" in sql, "single-resource jobs carry resource_id"
    assert "union" in sql


# ─── SLA alert payload (round-4: a bug the round-3 fix would introduce) ─

def test_never_backed_up_alert_message_does_not_crash_on_none_overdue():
    """`f"{violation.get('hours_overdue', 0):.1f}"` returns None — not 0 —
    because the KEY EXISTS with value None. f-string formatting a None
    raises TypeError, and send_violations_to_alert_service wraps the whole
    loop in one try/except, so a single never-backed-up resource silently
    dropped EVERY remaining SLA alert in the batch. Live-triggering:
    Corp_Esplanade (ROOM_MAILBOX, never captured)."""
    msg = dp.sla_alert_message({
        "resource_type": "ROOM_MAILBOX", "hours_overdue": None,
        "never_backed_up": True,
    })
    assert "never" in msg.lower()
    assert "None" not in msg


def test_overdue_alert_message_keeps_the_hours():
    msg = dp.sla_alert_message({
        "resource_type": "SHAREPOINT_SITE", "hours_overdue": 8.7,
        "never_backed_up": False,
    })
    assert "8.7" in msg and "SHAREPOINT_SITE" in msg


def test_in_flight_alert_message_says_so():
    """Otherwise an on-call engineer pages on a backup that is running."""
    msg = dp.sla_alert_message({
        "resource_type": "SHAREPOINT_SITE", "hours_overdue": 8.7,
        "never_backed_up": False, "backup_in_progress": True,
    })
    assert "in progress" in msg.lower() or "in flight" in msg.lower()


def test_alert_message_survives_a_missing_key_entirely():
    assert dp.sla_alert_message({"resource_type": "MAILBOX"})


def test_alert_send_isolates_per_violation_failures():
    """One failing POST must not drop the remaining violations."""
    src = _scheduler_src()
    fn = src[src.index("async def send_violations_to_alert_service"):]
    fn = fn[:fn.index("\nasync def ", 1)]
    body = fn[fn.index("for violation in violations:"):]
    assert "try:" in body, (
        "the try/except must be INSIDE the loop so one bad violation "
        "doesn't abort every later alert"
    )
