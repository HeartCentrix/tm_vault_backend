"""Pure decision helpers for scheduled backup dispatch.

Split out of ``main.py`` so the policy decisions are unit-testable
without importing the FastAPI app, APScheduler, or the message bus.

Two independent controls live here — they answer different questions and
must not be conflated (the 2026-08-05 RCA came from doing exactly that):

  ``decide_overlap_action``  — "is the previous SCHEDULED CYCLE for this
      policy still draining?" Cycle-level. Skips the whole tick.

  ``plan_concurrency_budget`` — "how much work for this policy is already
      in flight?" Resource-level. Trims which resources go out this tick.

Before this module existed there was no cycle-level control at all:
``dispatch_policy_backups`` publishes to RabbitMQ and returns in
seconds, so APScheduler's default ``max_instances=1`` never engaged and
cycle N+1 always fired on schedule even with cycle N 12 h deep in a
62 GB SharePoint partition. The only in-flight check was
``max_concurrent_backups``, which compared a count of *Jobs* (~10 per
cycle) against a budget spent in *resources* (cap 50) — so its
``budget == 0`` defer path needed five fully-stalled cycles and was
unreachable. Its one observable effect was an off-by-N trim that
silently dropped a resource per tick and wedged the Activity row at
"In Progress · 15 %" for 48 h.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Collection, Dict, List, Optional, Sequence, Tuple

from sqlalchemy import and_, distinct, exists, func, literal_column, or_, select

from shared.models import (
    Job, JobStatus, JobType, Resource, ResourceStatus, ResourceType, SlaPolicy,
    Snapshot, SnapshotStatus,
)


# ─── cadence ───────────────────────────────────────────────────────────

# Mirrors frequency_to_cron_params in main.py: THREE_DAILY fires at
# window_start +0/+8/+16h, DAILY once a day. Keep the two in step — this
# map is what sizes the overlap staleness horizon.
FREQUENCY_INTERVAL_HOURS: Dict[str, float] = {
    "THREE_DAILY": 8.0,
    "DAILY": 24.0,
    "WEEKLY": 168.0,
    "MONTHLY": 720.0,
}

# Unknown / MANUAL / legacy frequency values fall back to a day. Never
# return 0: a 0-hour interval makes every in-flight cycle look stale and
# silently disables the overlap guard.
DEFAULT_INTERVAL_HOURS = 24.0

# Absolute floor for the staleness horizon. A legitimate Gold cycle has
# been observed at 13 h 52 m, so anything tighter than a day would fire
# the override on healthy runs and re-create the pile-up.
MIN_STALE_AFTER_HOURS = 24.0

_DEFAULT_STALE_MULTIPLIER = 3.0


def frequency_interval_hours(frequency: Any) -> float:
    """Nominal hours between two ticks of ``frequency``."""
    if not frequency:
        return DEFAULT_INTERVAL_HOURS
    key = frequency.value if hasattr(frequency, "value") else str(frequency)
    return FREQUENCY_INTERVAL_HOURS.get(key.upper(), DEFAULT_INTERVAL_HOURS)


def overlap_stale_after_hours(
    frequency: Any, multiplier: Optional[float] = None,
) -> float:
    """How long an in-flight cycle may run before the guard gives up on
    waiting and dispatches anyway.

    A cycle that never terminates must not silently halt every future
    backup — on a data-protection product that converts one wedged shard
    into total coverage loss. Past this horizon we dispatch and shout.
    """
    if multiplier is None:
        multiplier = _stale_multiplier_from_env()
    return max(frequency_interval_hours(frequency) * multiplier,
               MIN_STALE_AFTER_HOURS)


def _stale_multiplier_from_env() -> float:
    raw = os.environ.get("BACKUP_OVERLAP_STALE_MULT", "")
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return _DEFAULT_STALE_MULTIPLIER
    # A non-positive multiplier would collapse the horizon to the 24 h
    # floor rather than doing something surprising.
    return value if value > 0 else _DEFAULT_STALE_MULTIPLIER


def overlap_guard_enabled() -> bool:
    """``BACKUP_OVERLAP_GUARD=off`` is the 30-second rollback, matching
    the ``ACTIVITY_GROUP_DEFAULT`` escape hatch on audit-service."""
    return os.environ.get("BACKUP_OVERLAP_GUARD", "on").strip().lower() not in (
        "off", "0", "false", "no",
    )


# ─── deferred-resource drain (waves) ───────────────────────────────────
#
# The cron tick trims to max_concurrent_backups and, until 2026-09-03,
# simply dropped the remainder until the NEXT tick. For a DAILY policy
# with 356 eligible resources and cap 50 that meant 50 backups a night
# and a structurally unmeetable SLA (Silver: 219 never backed up).
# The drain re-dispatches the remainder in waves as in-flight work
# finishes, so the cap limits CONCURRENCY, not per-day volume.

DRAIN_TICK_DEFAULT_S = 120
DRAIN_TICK_MIN_S = 30
# The cron tick itself is wave 1; give it this long to land its rows
# before the drain may add a wave, so the two never race on the same
# candidate set even if the lock were lost across a restart.
DRAIN_CYCLE_GRACE_S = 300
# Slots free at whole-job granularity, so a budget of 1-3 freed slots
# is common. Wait for max(1, cap*fraction) unless the tail fits.
DRAIN_MIN_WAVE_FRACTION = 0.2


def drain_enabled() -> bool:
    """``BACKUP_CYCLE_DRAIN=off`` turns every drain tick into a no-op —
    the 30-second rollback, same shape as ``overlap_guard_enabled``."""
    return os.environ.get("BACKUP_CYCLE_DRAIN", "on").strip().lower() not in (
        "off", "0", "false", "no",
    )


def drain_tick_seconds() -> int:
    raw = os.environ.get("BACKUP_DRAIN_TICK_S", "")
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return DRAIN_TICK_DEFAULT_S
    return max(DRAIN_TICK_MIN_S, value)


@dataclass(frozen=True)
class CycleWindow:
    """One policy cycle: ``start`` = latest cron fire <= now, ``end`` =
    the fire after it. Both naive UTC."""
    start: datetime
    end: datetime


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def cycle_window_from_trigger(
    trigger: Any, now: datetime, *, lookback_hours: float = 192.0,
) -> Optional[CycleWindow]:
    """Derive the current cycle from the policy's own APScheduler trigger.

    Using the registered ``CronTrigger`` (not a re-implementation of
    ``frequency_to_cron_params``) means DAILY / THREE_DAILY / backup_days
    are honoured exactly as the cron honours them, and a future cadence
    change cannot leave the drain computing a different window than the
    tick it is draining.
    """
    now_aware = _aware_utc(now)
    probe = now_aware - timedelta(hours=lookback_hours)
    start = None
    for _ in range(512):  # THREE_DAILY over 8 days = 24 fires; bounded anyway
        nxt = trigger.get_next_fire_time(None, probe)
        if nxt is None or nxt > now_aware:
            break
        start = nxt
        probe = nxt + timedelta(seconds=1)
    if start is None:
        return None
    end = trigger.get_next_fire_time(None, start + timedelta(seconds=1))
    if end is None or end <= start:
        return None
    return CycleWindow(start=_naive_utc(start), end=_naive_utc(end))


def cycle_is_drainable(
    window: CycleWindow, *, now: datetime, grace_seconds: int = DRAIN_CYCLE_GRACE_S,
) -> bool:
    n = _naive_utc(now)
    return window.start + timedelta(seconds=grace_seconds) <= n < window.end


def within_backup_window(
    *, now: datetime, window_start: Optional[str], window_end: Optional[str],
) -> bool:
    """Policies may pin a HH:MM window; a missing/unparseable end never
    blocks the drain (the cron does not honour it either)."""
    if not window_end:
        return True

    def _minutes(raw: str) -> int:
        hh, mm = raw.strip().split(":")[:2]
        return int(hh) * 60 + int(mm)

    try:
        end_m = _minutes(window_end)
        start_m = _minutes(window_start) if window_start else 0
    except (ValueError, AttributeError):
        return True
    n = _naive_utc(now)
    cur = n.hour * 60 + n.minute
    if start_m <= end_m:
        return start_m <= cur < end_m
    return cur >= start_m or cur < end_m  # wraps past midnight


def plan_wave(
    remaining: Sequence[Any], budget: int, max_concurrent: int, *,
    min_fraction: float = DRAIN_MIN_WAVE_FRACTION,
) -> List[Any]:
    """The next wave: the first ``budget`` of ``remaining`` (already in
    priority order), or nothing if the budget is below the wave floor and
    the tail does not fit.

    The floor is capped at the size of ``remaining`` itself — waiting for
    ``cap * min_fraction`` free slots makes no sense once the whole
    backlog is already smaller than that, so a shrinking tail is never
    stuck waiting for a floor it can no longer reach.
    """
    items = list(remaining)
    if budget <= 0 or not items:
        return []
    if len(items) <= budget:
        return items
    floor = max(1, int(min(len(items), max_concurrent) * min_fraction))
    if budget < floor:
        return []
    return items[:budget]


# ─── timestamp normalisation ───────────────────────────────────────────

def _naive_utc(value: Optional[datetime]) -> Optional[datetime]:
    """Collapse to naive UTC.

    ``jobs.created_at`` is ``timestamp without time zone`` while the
    lease columns are ``with time zone``; comparing across the two
    raises TypeError and aborts the whole dispatch, so every resource on
    the policy silently misses its window.
    """
    if value is None:
        return None
    if value.tzinfo is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


# ─── cycle-level overlap decision ──────────────────────────────────────

DISPATCH = "dispatch"
SKIP = "skip"
OVERRIDE = "override"
# Previous cycle still draining, but only SOME resources are actually busy:
# hold those, dispatch the rest. See partition_overlap_holds.
PARTIAL = "dispatch_partial"


def decide_overlap_action(
    *,
    inflight_jobs: int,
    oldest_created_at: Optional[datetime],
    now: datetime,
    stale_after_hours: float,
    guard_enabled: bool = True,
) -> str:
    """Return ``"dispatch"``, ``"skip"`` or ``"override"``.

    ``skip``     — a previous scheduled cycle is still draining.
    ``override`` — it has been draining past ``stale_after_hours``; go
                   anyway, loudly, so a wedge can't stop all backups.
    """
    if not guard_enabled:
        return DISPATCH
    if inflight_jobs <= 0:
        return DISPATCH

    oldest = _naive_utc(oldest_created_at)
    if oldest is None:
        # Unknown age must not be read as stale — that would silently
        # reinstate the overlap this guard exists to prevent.
        return SKIP

    age_hours = (_naive_utc(now) - oldest).total_seconds() / 3600.0
    return OVERRIDE if age_hours > stale_after_hours else SKIP


# ─── resource-level budget ─────────────────────────────────────────────

def backup_priority_key(resource: Any) -> Tuple[int, float, str]:
    """Sort key: never-backed-up first, then oldest ``last_backup_at``,
    then id — so ties among never-backed-up rows are deterministic
    instead of falling back to heap order (which is what made wave
    membership unpredictable before the drain existed).

    Replaces ``key=lambda r: r.last_backup_at or datetime.min``, which
    raises TypeError as soon as one row is tz-aware.
    """
    rid = str(getattr(resource, "id", ""))
    ts = _naive_utc(getattr(resource, "last_backup_at", None))
    if ts is None:
        return (0, 0.0, rid)
    return (1, ts.timestamp(), rid)


def partition_overlap_holds(
    resources: Sequence[Any], inflight_resource_ids: Any,
) -> Tuple[List[Any], List[Any]]:
    """Split ``resources`` into ``(dispatch_now, held)`` for an overlapping tick.

    ``decide_overlap_action`` is CYCLE-level, so a single long resource used to
    suppress every other resource on the policy. Gold, 2026-08-06 02:07Z: one
    job carrying 2 SharePoint sites (IT OPS NOTES, 62 GB partitioned) ran
    12.84h — longer than Gold's own 8h cadence — and the tick dispatched
    NOTHING for all 50 resources. The 3×-cadence/24h staleness override is far
    too coarse to catch it, so every over-cadence walk costs a whole tick.

    Holding per RESOURCE keeps the guard's actual purpose (never run two
    backups of the same resource concurrently, and don't pile Graph load on a
    still-draining cycle) while preserving coverage for everything idle.
    Data safety does not rest on this: the worker enforces its own
    per-resource ``skipped_concurrent`` guard.

    Ids are compared as strings — the in-flight set comes from a SQL union
    whose UUIDs may arrive as ``uuid.UUID`` or text depending on the driver,
    and a type mismatch here would silently dispatch a resource that is
    already running.
    """
    held_ids = {str(i) for i in (inflight_resource_ids or set())}
    dispatch_now: List[Any] = []
    held: List[Any] = []
    for r in resources:
        (held if str(getattr(r, "id", "")) in held_ids else dispatch_now).append(r)
    return dispatch_now, held


def plan_concurrency_budget(
    resources: Sequence[Any],
    max_concurrent: int,
    inflight_resources: int,
) -> Tuple[List[Any], List[Any]]:
    """Split ``resources`` into ``(dispatch_now, deferred)``.

    ``max_concurrent`` and ``inflight_resources`` are both counts of
    RESOURCES. Passing a count of Jobs for either is the original bug.
    ``max_concurrent <= 0`` means unlimited.
    """
    all_resources = list(resources)
    if max_concurrent <= 0:
        return all_resources, []

    budget = max(0, max_concurrent - max(0, inflight_resources))
    if budget >= len(all_resources):
        return all_resources, []

    ordered = sorted(all_resources, key=backup_priority_key)
    return ordered[:budget], ordered[budget:]


# ─── SQL contracts ─────────────────────────────────────────────────────

def build_scheduled_inflight_query(policy_id: str):
    """(job_count, oldest_created_at) for this policy's in-flight
    SCHEDULED cycle work.

    Restricted to ``triggered_by = 'SCHEDULED'`` on purpose: a manual
    "Backup now" click or a PREEMPTIVE anomaly backup must never
    suppress a scheduled cycle. Those paths stamp MANUAL / PREEMPTIVE
    and carry no ``sla_policy_id``, but pinning the trigger keeps the
    guard correct if that ever changes.

    ``.as_string()`` not ``.astext``: Job.spec is a plain JSON column
    (see test_backup_scheduler_inflight_query.py).
    """
    return select(
        func.count(Job.id),
        func.min(Job.created_at),
    ).where(and_(
        Job.type == JobType.BACKUP,
        Job.spec["sla_policy_id"].as_string() == str(policy_id),
        Job.spec["triggered_by"].as_string() == "SCHEDULED",
        Job.status.in_([JobStatus.QUEUED, JobStatus.RUNNING]),
    ))


# ─── in-flight accounting (resource-granular) ──────────────────────────
#
# "In flight" is decided per RESOURCE, not per job (2026-10-14). A Gold
# USER_CHATS wave is ONE 8-member bulk job: while one member's chat walk
# ran 12-38 h the other 7 — already COMPLETED — stayed "in flight" for
# every later tick: held by the cron overlap hold and the drain hold,
# charged against the cap budget, and classified in_progress by
# check_sla_violations. Measured: 72 of 164 held resource-ticks in 7 days
# were resources whose snapshot had already finished.
#
#   QUEUED job   → every member (nothing has started; all of it is owed).
#   RUNNING job  → only members whose own snapshot FOR THAT JOB is not
#                  terminal: no snapshot row yet, or a live IN_PROGRESS one.
#
# Both builders below derive from the same member rows and the same
# predicate so the id set and the count can never disagree.

# Snapshot statuses that mean a member's capture under a job is over.
# IN_PROGRESS is the only live state. PENDING_DELETION is reached via
# retention / the cancelled-sweep, never while the owning job is still
# RUNNING, so it is left out on purpose (spec: COMPLETED/PARTIAL/FAILED).
SNAPSHOT_TERMINAL_STATUSES = (
    SnapshotStatus.COMPLETED, SnapshotStatus.PARTIAL, SnapshotStatus.FAILED,
)


def _snapshot_is_placeholder():
    """SQL predicate: this snapshot row is backup-worker's cross-job
    concurrency placeholder (``extra_data.skipped_concurrent = true``).

    It captured nothing and holds no worker, so it is never evidence of
    work in flight — and for the member it belongs to it counts as
    settled. Same NULL-safe text comparison as
    main.py::_not_skipped_concurrent: json_extract_path_text works on the
    plain-json column, COALESCE keeps rows that predate the flag, and a
    text compare can't raise the way an ``->>::boolean`` cast would.
    """
    return func.coalesce(
        func.json_extract_path_text(Snapshot.extra_data, "skipped_concurrent"),
        "false",
    ).in_(("true", "t", "1"))


def _inflight_members(policy_id: Any = None):
    """``(job_id, job_status, resource_id)`` — one row per member of every
    QUEUED/RUNNING BACKUP job, optionally only jobs stamped
    ``spec.sla_policy_id = policy_id``.

    Both job shapes must be covered or the in-flight suppression misses
    half the fleet: bulk/scheduled jobs carry ``batch_resource_ids``
    (unnested here), per-resource jobs (manual "Backup now", AUTO_RETRY,
    PREEMPTIVE) carry ``resource_id``. UNION ALL on purpose: a resource
    carried by two jobs keeps both rows so each is judged against its own
    job's snapshot; the callers collapse duplicates with DISTINCT.
    """
    conds = [
        Job.type == JobType.BACKUP,
        Job.status.in_([JobStatus.QUEUED, JobStatus.RUNNING]),
    ]
    if policy_id is not None:
        conds.append(Job.spec["sla_policy_id"].as_string() == str(policy_id))
    batch_shape = select(
        Job.id.label("job_id"),
        Job.status.label("job_status"),
        func.unnest(Job.batch_resource_ids).label("resource_id"),
    ).where(and_(*conds, Job.batch_resource_ids.isnot(None)))
    single_shape = select(
        Job.id.label("job_id"),
        Job.status.label("job_status"),
        Job.resource_id.label("resource_id"),
    ).where(and_(*conds, Job.resource_id.isnot(None)))
    return batch_shape.union_all(single_shape).subquery("inflight_members")


def _member_in_flight(members):
    """SQL predicate over one ``_inflight_members`` row: is this member's
    backup still in flight?

    * QUEUED job — always.
    * RUNNING job — while the member's own snapshot for THAT job is not
      terminal: no snapshot row yet, or a live IN_PROGRESS one. A member
      retried inside the same job carries FAILED + IN_PROGRESS (20 such
      pairs in prod), so a live IN_PROGRESS row wins over an older
      terminal sibling. ``skipped_concurrent`` placeholders are never in
      flight and count as settled for their member.

    EXISTS / NOT EXISTS, never NOT IN: one NULL inside a NOT IN subquery
    makes PostgreSQL return zero rows and would silently release every
    hold.
    """
    this_member = and_(
        Snapshot.job_id == members.c.job_id,
        Snapshot.resource_id == members.c.resource_id,
    )
    live_snapshot = exists(
        select(literal_column("1")).where(and_(
            this_member,
            Snapshot.status == SnapshotStatus.IN_PROGRESS,
            ~_snapshot_is_placeholder(),
        ))
    )
    settled_snapshot = exists(
        select(literal_column("1")).where(and_(
            this_member,
            or_(
                Snapshot.status.in_(list(SNAPSHOT_TERMINAL_STATUSES)),
                _snapshot_is_placeholder(),
            ),
        ))
    )
    return or_(
        members.c.job_status == JobStatus.QUEUED,
        live_snapshot,
        ~settled_snapshot,
    )


def build_inflight_resource_count_query(policy_id: str):
    """Number of distinct RESOURCES in flight for this policy.

    Resource-granular: the same member rows and predicate as
    ``build_inflight_resource_ids_query`` (``_inflight_members`` /
    ``_member_in_flight``), restricted to jobs stamped
    ``spec.sla_policy_id = policy_id``. Until 2026-10-14 this summed
    ``array_length(batch_resource_ids, 1)`` over every QUEUED/RUNNING
    job, so a bulk job's COMPLETED members stayed charged against the cap
    for as long as its slowest member ran.

    Still counts resources, never job rows — one job can carry up to
    BATCH_SIZE resources, so a row count understated in-flight load by
    orders of magnitude and made the ``budget == 0`` defer path
    unreachable.

    Deliberately NOT restricted by trigger: every in-flight backup for
    the policy consumes the same Graph throughput the cap is protecting.
    """
    members = _inflight_members(policy_id=policy_id)
    return select(
        func.count(distinct(members.c.resource_id))
    ).where(_member_in_flight(members))


# ─── drain: what has this cycle already carried? ───────────────────────

def _cycle_scheduled_jobs_filter(policy_id: Any, since: datetime):
    """Every SCHEDULED BACKUP job of this policy created since the cycle
    opened — ANY status. A FAILED wave job still counts as carried: the
    retry loop owns that resource for the rest of the cycle, and
    re-picking it here would double-dispatch and never terminate."""
    return and_(
        Job.type == JobType.BACKUP,
        Job.spec["sla_policy_id"].as_string() == str(policy_id),
        Job.spec["triggered_by"].as_string() == "SCHEDULED",
        Job.created_at >= since,
    )


def build_cycle_dispatched_ids_query(policy_id: Any, since: datetime):
    """Distinct resource ids carried by this cycle's SCHEDULED jobs."""
    return select(
        func.unnest(Job.batch_resource_ids).label("resource_id")
    ).where(and_(
        _cycle_scheduled_jobs_filter(policy_id, since),
        Job.batch_resource_ids.isnot(None),
    )).distinct()


def build_cycle_batch_id_query(policy_id: Any, since: datetime):
    """``spec.batch_id`` of the cycle's earliest SCHEDULED job — waves
    reuse it so the Activity rollup keeps ONE row per policy-cycle."""
    return (
        select(Job.spec["batch_id"].as_string())
        .where(_cycle_scheduled_jobs_filter(policy_id, since))
        .order_by(Job.created_at.asc())
        .limit(1)
    )


def build_cycle_max_wave_query(policy_id: Any, since: datetime):
    """Highest ``spec.wave`` stamped so far this cycle (0 if none)."""
    return select(
        func.coalesce(func.max(Job.spec["wave"].as_integer()), 0)
    ).where(_cycle_scheduled_jobs_filter(policy_id, since))


def build_drainable_policies_query():
    """Policies the drain tick should look at: enabled, on a cadence, and
    actually capped (an uncapped policy dispatches everything at the tick)."""
    return select(SlaPolicy.id).where(and_(
        SlaPolicy.enabled.is_(True),
        SlaPolicy.frequency != "MANUAL",
        SlaPolicy.max_concurrent_backups > 0,
    ))


def build_drain_candidates_query(
    policy_id: Any, since: datetime, enabled_types: Collection[str],
):
    """Eligible resources this cycle has NOT carried yet, in dispatch
    priority order (never-backed-up first, then oldest, then id).

    NOT EXISTS, never NOT IN: one NULL inside a NOT IN subquery makes
    PostgreSQL return zero rows and would silently switch the drain off.
    Light projection on purpose — the cron loads full ORM rows with the
    tenant relationship; the drain runs every couple of minutes on a
    512 Mi pod and needs only the fields the dispatcher reads.
    POWER_BI is cron-only: its modified-workspace Graph prefilter
    (main.py, "power_bi_resources") is not replicated here.
    """
    carried = build_cycle_dispatched_ids_query(policy_id, since).subquery("carried")
    type_values = [ResourceType(t) for t in enabled_types]
    return select(
        Resource.id, Resource.type, Resource.tenant_id,
        Resource.last_backup_at, Resource.display_name,
    ).where(and_(
        Resource.sla_policy_id == policy_id,
        Resource.status.in_([ResourceStatus.DISCOVERED, ResourceStatus.ACTIVE]),
        Resource.type.in_(type_values),
        Resource.type != ResourceType.POWER_BI,
        # Match cron's license prefilter; otherwise the drain re-enqueues
        # workloads intentionally skipped before the first wave.
        func.coalesce(func.json_extract_path_text(Resource.extra_data, "license_missing"), "false") != "true",
        ~exists(
            select(literal_column("1"))
            .select_from(carried)
            .where(carried.c.resource_id == Resource.id)
        ),
    )).order_by(Resource.last_backup_at.asc().nulls_first(), Resource.id.asc())


# ─── SLA violation classification ──────────────────────────────────────

# Frequencies that are never put on the cron (see
# frequency_to_cron_params, which returns None for MANUAL). A policy with
# no cadence has no SLA window, so measuring it against a fabricated 24h
# default makes every resource on it a permanent, unfixable violation.
# Prod has `Manual` enabled with sla_violation_alert=True — exactly that.
SLA_UNSCHEDULED_FREQUENCIES = {"MANUAL"}


def policy_has_sla_cadence(frequency: Any) -> bool:
    """True when ``frequency`` puts the policy on a real schedule."""
    if not frequency:
        return False
    key = frequency.value if hasattr(frequency, "value") else str(frequency)
    return key.upper() not in SLA_UNSCHEDULED_FREQUENCIES


def classify_sla_violation(
    *,
    hours_since_backup: Optional[float],
    frequency_hours: float,
    never_backed_up: bool = False,
    backup_in_progress: bool = False,
) -> Optional[str]:
    """``"CRITICAL"`` / ``"WARNING"`` / ``None`` (compliant).

    Three behaviours the original inline check got wrong:

    * ``if resource.last_backup_at:`` skipped resources that have NEVER
      been captured — the highest-risk case produced no alert at all.
      Confirmed live: a ROOM_MAILBOX on an alerting policy with
      last_backup_at IS NULL.
    * A resource with a backup actively in flight was escalated to
      CRITICAL every 30 min. IT OPS NOTES (62 GB SharePoint partition,
      12 h into a progressing run) paged continuously while data
      protection was in fact happening. Behind-SLA-but-running is a
      WARNING, never a CRITICAL — and the overlap guard makes this more
      common, since a skipped cycle ages last_backup_at further.
    * The boundary was ``>`` on a value that had already been rounded
      into the window; exactly-at-window is compliant.
    """
    if never_backed_up:
        # Nothing has ever been captured. If the first capture is running
        # right now that is the good case, so don't page.
        return "WARNING" if backup_in_progress else "CRITICAL"

    if hours_since_backup is None or hours_since_backup <= frequency_hours:
        return None

    if backup_in_progress:
        return "WARNING"

    return (
        "CRITICAL" if hours_since_backup > frequency_hours * 2 else "WARNING"
    )


def sla_alert_message(violation: Dict[str, Any]) -> str:
    """Human-readable alert text for one SLA violation.

    Must tolerate ``hours_overdue`` being None. The old inline f-string
    did ``violation.get('hours_overdue', 0):.1f``, which returns None
    rather than 0 whenever the KEY EXISTS with a None value — and
    formatting None raises TypeError. Because the caller wrapped its
    whole loop in one try/except, a single never-backed-up resource
    silently dropped every remaining alert in the batch.
    """
    rtype = violation.get("resource_type") or "resource"
    suffix = (
        " (backup currently in progress)"
        if violation.get("backup_in_progress") else ""
    )
    if violation.get("never_backed_up"):
        return f"SLA violation: {rtype} resource has never been backed up{suffix}"
    overdue = violation.get("hours_overdue")
    if overdue is None:
        return f"SLA violation: {rtype} resource is overdue{suffix}"
    return (
        f"SLA violation: {rtype} resource overdue by {overdue:.1f} hours{suffix}"
    )


def build_inflight_resource_ids_query():
    """Distinct resource ids whose backup is genuinely in flight.

    Resource-granular (see ``_member_in_flight``): a QUEUED job holds
    every member; a RUNNING job holds only the members whose own
    snapshot for that job is not yet terminal. Until 2026-10-14 every
    member of every QUEUED/RUNNING job was returned, so one slow member
    of a bulk job held its COMPLETED siblings for the rest of the run.

    Both job shapes are covered (``_inflight_members``): bulk/scheduled
    jobs carry ``batch_resource_ids``, per-resource jobs (manual "Backup
    now", AUTO_RETRY, PREEMPTIVE) carry ``resource_id``. Deliberately NOT
    policy-scoped: a resource busy under ANY job must be held by the
    overlap/drain holds and read as in-progress by check_sla_violations.
    One column per row (``resource_id``), never NULL.
    """
    members = _inflight_members()
    return (
        select(members.c.resource_id)
        .where(_member_in_flight(members))
        .distinct()
    )


# ─── audit payloads ────────────────────────────────────────────────────

def deferred_audit_details(
    policy: Any, deferred: Sequence[Any], *,
    max_concurrent: int, inflight_resources: int, dispatched: int,
) -> Dict[str, Any]:
    """Details for one cycle-level BACKUP_DEFERRED_CONCURRENCY_CAP event.

    One event per tick, not per resource: a 33-resource trim would
    otherwise flood the Activity feed with 33 warning rows. The per
    resource detail rides along in ``resource_ids`` / ``resources``.
    """
    return {
        "message": (
            f"{len(deferred)} resource(s) deferred to a later wave of this cycle — "
            f"policy '{getattr(policy, 'name', '?')}' concurrency cap "
            f"{max_concurrent} with {inflight_resources} already in flight "
            f"({dispatched} dispatched this cycle)."
        ),
        "skip_reason": "concurrency_cap",
        "policy_id": str(getattr(policy, "id", "")),
        "policy_name": getattr(policy, "name", None),
        "max_concurrent_backups": max_concurrent,
        "inflight_resources": inflight_resources,
        "dispatched_count": dispatched,
        "deferred_count": len(deferred),
        "resource_ids": [str(r.id) for r in deferred],
        "resources": [
            {
                "id": str(r.id),
                "type": (
                    r.type.value if hasattr(r.type, "value") else str(r.type)
                ),
                "name": getattr(r, "display_name", None),
            }
            for r in deferred
        ],
        "source": "backup_scheduler",
    }


def overlap_partial_audit_details(
    policy: Any, held: Sequence[Any], *, inflight_jobs: int,
    oldest_created_at: Optional[datetime], now: datetime, dispatched: int,
) -> Dict[str, Any]:
    """Details for BACKUP_CYCLE_PARTIAL_OVERLAP.

    One event per tick (same reasoning as ``deferred_audit_details``), with the
    held resources enumerated in ``resources`` so the operator can see WHICH
    resource is running long without log archaeology — the thing that made the
    02:07Z loss invisible.
    """
    oldest = _naive_utc(oldest_created_at)
    age_hours = (
        (_naive_utc(now) - oldest).total_seconds() / 3600.0
        if oldest is not None else None
    )
    name = getattr(policy, "name", "?")
    age_txt = f" after {age_hours:.1f}h" if age_hours is not None else ""
    return {
        "message": (
            f"Policy '{name}': previous scheduled cycle still draining"
            f"{age_txt} ({inflight_jobs} job(s) in flight) — held "
            f"{len(held)} busy resource(s) and dispatched {dispatched}."
        ),
        "skip_reason": "cycle_overlap",
        "action": PARTIAL,
        "policy_id": str(getattr(policy, "id", "")),
        "policy_name": name,
        "inflight_jobs": inflight_jobs,
        "inflight_oldest_at": oldest.isoformat() if oldest else None,
        "inflight_age_hours": (
            round(age_hours, 2) if age_hours is not None else None
        ),
        "held_count": len(held),
        "dispatched_count": dispatched,
        "resource_ids": [str(r.id) for r in held],
        "resources": [
            {
                "id": str(r.id),
                "type": (
                    r.type.value if hasattr(r.type, "value") else str(r.type)
                ),
                "name": getattr(r, "display_name", None),
            }
            for r in held
        ],
        "source": "backup_scheduler",
    }


def overlap_audit_details(
    policy: Any, *, action: str, inflight_jobs: int,
    oldest_created_at: Optional[datetime], now: datetime,
    stale_after_hours: float,
) -> Dict[str, Any]:
    """Details for BACKUP_CYCLE_SKIPPED_OVERLAP / ..._OVERLAP_OVERRIDE."""
    oldest = _naive_utc(oldest_created_at)
    age_hours = (
        (_naive_utc(now) - oldest).total_seconds() / 3600.0
        if oldest is not None else None
    )
    name = getattr(policy, "name", "?")
    if action == OVERRIDE:
        message = (
            f"Policy '{name}': previous scheduled cycle still in flight "
            f"after {age_hours:.1f}h (limit {stale_after_hours:.0f}h) — "
            f"dispatching anyway. Investigate the stuck cycle."
            if age_hours is not None else
            f"Policy '{name}': previous cycle stale — dispatching anyway."
        )
    else:
        message = (
            f"Policy '{name}': scheduled cycle skipped — the previous "
            f"cycle is still running ({inflight_jobs} job(s) in flight"
            + (f", {age_hours:.1f}h elapsed" if age_hours is not None else "")
            + ")."
        )
    return {
        "message": message,
        "skip_reason": "cycle_overlap",
        "action": action,
        "policy_id": str(getattr(policy, "id", "")),
        "policy_name": name,
        "inflight_jobs": inflight_jobs,
        "inflight_oldest_at": oldest.isoformat() if oldest else None,
        "inflight_age_hours": (
            round(age_hours, 2) if age_hours is not None else None
        ),
        "stale_after_hours": stale_after_hours,
        "source": "backup_scheduler",
    }


def wave_audit_details(
    policy: Any, *, batch_id: str, wave: int, dispatched: int, remaining: int,
    held: int, inflight_resources: int, max_concurrent: int, started_cycle: bool,
) -> Dict[str, Any]:
    """Details for one BACKUP_CYCLE_WAVE_DISPATCHED event.

    Counts only. The nightly deferral event enumerates every deferred
    resource (46 KB for Silver); a wave every few minutes must not.
    """
    name = getattr(policy, "name", "?")
    return {
        "message": (
            f"Policy '{name}': wave {wave} dispatched {dispatched} resource(s); "
            f"{remaining} remaining, {held} held in flight; cap {max_concurrent} "
            f"with {inflight_resources} in flight before this wave."
            + (" Drain opened the cycle — no cron batch found." if started_cycle else "")
        ),
        "policy_id": str(getattr(policy, "id", "")),
        "policy_name": name,
        "batch_id": batch_id,
        "wave": wave,
        "dispatched_count": dispatched,
        "remaining_count": remaining,
        "held_count": held,
        "inflight_resources": inflight_resources,
        "max_concurrent_backups": max_concurrent,
        "started_cycle": started_cycle,
        "source": "backup_scheduler",
    }


def cycle_incomplete_audit_details(
    policy: Any, remaining: Sequence[Any], *, batch_id: Optional[str],
    window_end: datetime, max_ids: int = 200, held: Sequence[Any] = (),
) -> Dict[str, Any]:
    """Details for BACKUP_CYCLE_INCOMPLETE — the cycle is closing and these
    eligible resources were never dispatched. This is the real coverage-gap
    signal now that deferral itself is routine.

    ``held`` is counted, never merged into ``remaining``/``resource_ids``:
    a resource in flight under another job is a duration problem, not a
    capacity one, and the wave event keeps that distinction — this one
    must too, or "the cap was never free long enough" gets blamed for a
    slow overlapping run.
    """
    name = getattr(policy, "name", "?")
    shown = list(remaining)[:max_ids]
    message = (
        f"Policy '{name}': cycle closing at {window_end.isoformat()} with "
        f"{len(remaining)} eligible resource(s) never dispatched — the "
        f"concurrency cap was never free long enough. Raise "
        f"max_concurrent_backups or shorten the cadence."
    )
    if held:
        message += f" ({len(held)} more held in flight)"
    return {
        "message": message,
        "skip_reason": "cycle_incomplete",
        "policy_id": str(getattr(policy, "id", "")),
        "policy_name": name,
        "batch_id": batch_id,
        "window_end": window_end.isoformat(),
        "remaining_count": len(remaining),
        "held_count": len(held),
        "resource_ids": [str(r.id) for r in shown],
        "resources": [
            {
                "id": str(r.id),
                "type": (r.type.value if hasattr(r.type, "value") else str(r.type)),
                "name": getattr(r, "display_name", None),
            }
            for r in shown
        ],
        "source": "backup_scheduler",
    }
