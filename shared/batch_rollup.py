"""Batch-rollup logic for the Activity Manager.

Pure functions + one SQL builder, isolated from any FastAPI handler so
the state machine is unit-testable without a database. Imported by
``services/audit-service/main.py`` (the only consumer today).

The rollup considers THREE state sources:

1. Jobs       — the 3 fan-out Jobs of a "Backup all" click share
                ``spec.batch_id`` (Tier-1 ENTRA_USER bulk, Tier-2-urgent
                for mail/calendar/contacts, Tier-2-heavy for OneDrive/
                chats).
2. Snapshots  — per-resource state under each Job.
3. snapshot_partitions — per-shard state under partitioned snapshots
                (OneDrive / Chats / Mail / SharePoint).

A batch is "Done" only when ALL three are terminal AND no expected
Tier-2 child Job is still missing from the fan-out (the
``fanout_incomplete`` gate fixes the 3-5 s Tier-1 → Tier-2 handoff
flicker that the operator sees today).

See docs/superpowers/specs/2026-05-15-activity-batch-rollup-design.md.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from sqlalchemy import text

# Operations that may be inlined into the SQL by ``build_batch_rollup_query``.
#
# Deliberately a literal set rather than ``frozenset(m.value for m in JobType)``:
# importing shared.models pulls in shared.config, which validates JWT_SECRET at
# import time and raises without it. This module is a pure SQL builder with no
# other heavy imports, and coupling it to config validation would make it
# unimportable in exactly the lightweight contexts that use it.
#
# Drift against JobType is caught by
# tests/shared/test_batch_rollup.py::test_allowed_operations_matches_jobtype_enum.
_ALLOWED_OPERATIONS = frozenset(
    {"BACKUP", "RESTORE", "EXPORT", "DISCOVERY", "DELETE"}
)


@dataclass
class RollupCounts:
    """Pre-computed rollup counts for one batch.

    Built by ``build_batch_rollup_query``; consumed by
    ``derive_batch_status`` and ``shape_batch_row``. Fields map 1:1 to
    the CTE output columns so the SQL → Python boundary is mechanical.
    """
    all_jobs_terminal: bool
    any_cancelled: bool
    any_job_failed: bool
    snap_total: int
    snap_done: int
    snap_partial: int
    snap_failed: int
    snap_pending: int
    parts_pending: int
    missing_t2: int
    expected_total: int = 0
    # Concurrency placeholders: COMPLETED rows that captured nothing
    # because another job held the resource's lease. Tracked separately
    # so they neither inflate snap_done nor read as a failure.
    snap_skipped: int = 0
    # Tier-1 vs Tier-2 split for the weighted progress bar. The data
    # volume between the two tiers is wildly skewed: a Tier-1
    # ENTRA_USER snapshot is ~1 KB of metadata, while one Tier-2
    # workload (Mail / OneDrive / Chats / Calendar / Contacts) routinely
    # carries multiple GB of mailbox or drive content. Weighting Tier-1
    # at 10 % and Tier-2 at 90 % of the bar keeps the displayed
    # percentage proportional to actual work done (2026-05-16 user
    # report: "user entra id info is just 1 % data but as soon as that
    # happens it shows 99 % complete which is wrong"). Without this
    # split the bar jumps to 99 % after Tier-1 finishes, then collapses
    # back to ~30 % once Tier-2 discovery spawns its real children —
    # the exact non-monotone behaviour we're fixing.
    #
    # UNITS (2026-08-11): all four counters are DISTINCT RESOURCES, and
    # numerator/denominator of each tier count the SAME population.
    #   tier1_* — ENTRA_USER members of the click-time scope.
    #   tier2_* — every other resource of the batch's expected work
    #             (non-ENTRA_USER scope members ∪ expected Tier-2
    #             children).
    # They used to disagree: tier1_total was the whole click-time SCOPE
    # RESOURCE count while tier1_terminal counted only ENTRA_USER
    # snapshots, and the tier2 denominator was floored at
    # scope_resources × 5. On Gold (50 scope resources, 7 users, ~34
    # real children) that produced 10*(7/50) + 90*(38/250) = 15 — a bar
    # frozen at 15 % while 44 of 50 snapshots were already done.
    tier1_total: int = 0
    tier1_terminal: int = 0
    tier2_total: int = 0
    tier2_terminal: int = 0
    # True when at least one Job in the batch reached COMPLETED. Only
    # consulted by the retention-pruned fallback below -- when every
    # snapshot row has been deleted by retention, job statuses are the
    # only durable record of the outcome.
    any_job_completed: bool = False
    # True while *any* user in this batch is still WAITING_DISCOVERY in
    # ``batch_pending_users``. Without this gate, a batch whose Tier-1
    # jobs finished before Tier-2 gap-fill discovery completes briefly
    # reports "Done 100%" — then flips back to "In Progress" once the
    # newly-discovered Tier-2 resources spawn snapshots. The user-visible
    # Done→InProgress→Done flicker confused operators (2026-05-16
    # incident: 9-user backup briefly showed Done before Tier-2 work
    # had even been published).
    discovery_pending: bool = False
    # Count of Tier-2 child workloads (Mail / OneDrive / Calendar / …) in
    # this batch's scope that were SKIPPED because the user has no M365
    # license for them (``resources.metadata.license_missing``). These are
    # deliberately EXCLUDED from ``missing_t2`` and ``expected_total``
    # upstream in the SQL — an unlicensed workload can never produce a
    # snapshot, so counting it as "missing" pinned the batch at
    # "In Progress" forever and inflated the progress denominator (the
    # Amit-Kumar/Lyly "14% so far, never finishes" report). We keep the
    # count only to render a neutral, informational "N skipped (no
    # license)" chip on an otherwise-Done row. Discovery auto-clears
    # license_missing when a license later appears, so the workload
    # rejoins the expected set on the next fire automatically.
    skipped_no_license: int = 0
    # Did at least one Tier-2 job spawn in this batch? Arbitrates
    # past-horizon missing_t2 (see derive_batch_status): missing
    # children WITH Tier-2 jobs = expectation drift; missing children
    # WITHOUT any Tier-2 job = the fan-out trigger was lost.
    any_tier2_jobs: bool = False
    # False once the batch is older than the missing_t2 freshness
    # horizon (48h). Defaults True (conservative: unknown age behaves
    # like a fresh batch and keeps the anti-flicker wedge).
    batch_recent: bool = True


def derive_batch_status(
    r: RollupCounts,
) -> Tuple[str, Optional[Dict[str, int]]]:
    """Map RollupCounts → (status, warnings).

    Returns ``(status_label, warnings_dict_or_None)``. Status labels
    match the existing frontend enum:

        "In Progress" | "Done" | "Failed" | "Canceled" | "Expired"

    "Expired" = the backup completed but every snapshot has since been
    pruned by the SLA retention policy — a succeeded backup whose restore
    point aged out, NOT a failure.

    ``warnings`` is non-None only when status == "Done" and at least
    one child snapshot landed in PARTIAL or FAILED. Shape:

        {"partial": N, "failed": M}
    """
    # 1. Anything still moving → In Progress.
    #
    # Four gates must ALL pass before we can declare the batch terminal:
    #   (a) all_jobs_terminal      — every Job row in the batch finalized
    #   (b) snap_pending == 0      — no snapshot row still IN_PROGRESS
    #   (c) parts_pending == 0     — no snapshot_partition still in-flight
    #   (d) missing_t2 == 0        — every expected Tier-2 child resource
    #                                that has been discovered has either
    #                                been snapshotted or has a backup job
    #                                in batch_resource_ids
    #   (e) discovery_pending == False — Tier-2 gap-fill discovery itself
    #                                is complete for every user in the
    #                                batch (no batch_pending_users row
    #                                still in WAITING_DISCOVERY). This
    #                                gate is what prevents the user-
    #                                visible "Done → In Progress → Done"
    #                                flicker: at T+30s after a click,
    #                                Tier-1 backups finish but Tier-2
    #                                discovery is still walking the Graph;
    #                                without (e), missing_t2 would equal
    #                                zero (Tier-2 resources don't exist
    #                                in the DB yet) and the batch would
    #                                falsely report Done.
    # missing_t2 is computed against the CURRENT resources table, so it
    # may only WEDGE a batch while the batch is recent — the gate exists
    # for the minutes-scale post-click flicker, and letting it judge
    # settled history means a later resource-status flip (2026-08-03: a
    # discovery reset revived long-INACCESSIBLE children) retroactively
    # turns every old batch "In Progress". Past the horizon it is
    # arbitrated by any_tier2_jobs below instead: drift is ignored, a
    # genuinely lost fan-out surfaces as a warning — never as a wedge,
    # and never as a silent clean Done.
    if (
        not r.all_jobs_terminal
        or r.snap_pending > 0
        or r.parts_pending > 0
        or (r.missing_t2 > 0 and r.batch_recent)
        or r.discovery_pending
    ):
        return ("In Progress", None)

    # Past-horizon fan-out audit: expected children exist but NO Tier-2
    # job ever spawned in this batch — the fan-out trigger was lost
    # (e.g. the fire-and-forget trigger-bulk POST failed after
    # batch_pending_users was already committed BACKUP_ENQUEUED). Only
    # Tier-1 metadata was captured; a clean "Done" here would be a
    # false success on a backup product. Surfaced via the warnings chip
    # in the mixed-outcome branch below.
    missing_unspawned = (
        r.missing_t2 if (r.missing_t2 > 0 and not r.any_tier2_jobs) else 0
    )

    # 1.5 Retention-pruned history: every Job settled but not one
    # snapshot row remains -- retention deleted them (their blobs may
    # still be referenced by newer snapshots). Judging success by
    # snap_done would flip aged-out Done batches to "Failed" as
    # retention advances (2026-07-14: every pre-7/13 scheduled
    # dispatch). With zero snapshot rows, the terminal Job statuses
    # are the durable record -- trust them. A batch that genuinely
    # never wrote anything still fails below: its jobs are FAILED /
    # CANCELLED, never COMPLETED.
    if r.snap_total == 0:
        if r.skipped_no_license > 0 and not r.any_job_failed and not missing_unspawned:
            return ("Done", {"skipped_no_license": r.skipped_no_license})
        if r.any_job_completed:
            return ("Done", None)
        if r.any_cancelled:
            return ("Canceled", None)
        return ("Failed", None) if r.any_job_failed else ("Expired", None)

    # 2. Cancellation: if operator cancelled and NOTHING got persisted,
    # show Canceled. If some snapshots completed before the cancel
    # landed, treat those as a partial-success backup so the operator
    # can still see (and recover) what we managed to capture.
    # 1.6 Deferred-only: every snapshot in this batch is a
    # `skipped_concurrent` placeholder. Nothing broke and nothing was
    # captured — another run still held the resource's lease. Without
    # this arm the (correct) exclusion of placeholders from snap_done
    # would drop straight through to "no successes -> Failed", which
    # is a worse lie than the one we are fixing: it reports breakage
    # where there is none. Report Done WITH a warning so the operator
    # can see the run captured nothing and why.
    if (
        r.snap_skipped > 0
        and r.snap_done == 0
        and r.snap_partial == 0
        and r.snap_failed == 0
    ):
        return (
            "Done",
            "no data captured — another run held the resource lease "
            "(skipped_concurrent)",
        )

    has_successes = r.snap_done > 0 or r.snap_partial > 0
    if r.any_cancelled and not has_successes:
        return ("Canceled", None)

    # 3. No successes at all. Distinguish a genuine failure from a batch whose
    #    snapshots were later pruned by the SLA retention policy:
    #      * no snapshot rows remain (snap_total == 0) AND no job failed →
    #        the backup COMPLETED and its restore point has since aged out
    #        under retention. That is "Expired", NOT "Failed". Mislabeling a
    #        policy-pruned backup as a failure is what surfaced as "2 of 3
    #        daily backups Failed": GFS keeps only the newest snapshot per
    #        day, so every earlier same-day fire had all its snapshots deleted
    #        and the Activity feed screamed Failed on a backup that succeeded.
    #      * anything else (a job actually FAILED, or snapshots still exist but
    #        every one of them failed) → a real Failed.
    if not has_successes:
        if r.snap_total == 0 and not r.any_job_failed:
            # A fire that stored nothing itself and did not fail. Normally
            # this is an Expired (retention-pruned) restore point. But if
            # the ONLY thing left to do was license-skipped workloads, the
            # backup ran correctly and simply had nothing to capture — that
            # is a successful, fully-skipped run, NOT an expiry/failure.
            if r.skipped_no_license > 0:
                return ("Done", {"skipped_no_license": r.skipped_no_license})
            return ("Expired", None)
        return ("Failed", None)

    # 4. Mixed outcome → Done with warnings chip. Cancellation that
    # landed AFTER some snapshots completed is also a "not-clean" Done
    # — we surface a warning so the operator sees the truncation even
    # if every committed snapshot itself was clean. Missing fan-out counts
    # as failed; license skips remain a separate informational count.
    warnings: Dict[str, int] = {}
    if r.snap_partial > 0 or r.snap_failed > 0 or r.any_cancelled or missing_unspawned > 0:
        warnings["partial"] = r.snap_partial
        warnings["failed"] = r.snap_failed + missing_unspawned
    if r.skipped_no_license > 0:
        warnings["skipped_no_license"] = r.skipped_no_license
    return ("Done", warnings or None)


def archive_backup_enabled() -> bool:
    """Whether Online Archive backup is on (default ON — mirrors
    archive_graph). Gates USER_ARCHIVE in both the batch DISPLAY rollup and the
    FINALIZER leaf set so the flag is safely reversible: off → archive is
    dropped from what a batch waits on, so no batch pins to In Progress on
    archive snapshots that were never dispatched."""
    import os as _os
    return _os.getenv("ARCHIVE_BACKUP_ENABLED", "true").strip().lower() in (
        "1", "true", "yes", "on",
    )


def _withheld_children_cte(cte_name: str, spec_key: str, comment: str) -> str:
    """SQL for one "the dispatcher DECLARED these resources withheld" CTE.

    The rollup has to tell two indistinguishable things apart:

      * a Tier-2 child whose fan-out was LOST — real data was never
        captured, the operator must see it; and
      * a child the dispatcher deliberately held back this cycle — the
        next tick picks it up, nothing is missing.

    The only safe discriminator is a RECORDED FACT written by the
    dispatcher at dispatch time. Inferring the hold from job coverage +
    timing was tried and rejected on 2026-08-11: measured over full prod
    history, 597 (batch, resource) pairs matched a "some other job
    covered this resource around then" heuristic, but 452 of them (76 %)
    had NO snapshot from the covering job and 331 of those covering jobs
    had FAILED. Erasing those children from missing_t2 would have turned
    batches that never captured the data into a clean "Done" — strictly
    worse than the cosmetic phantom it set out to fix.

    Two defences so a bad row can never take down the rollup for EVERY
    other batch (same posture as the stale-snapshot sweep's
    failure_state handling):
      * jsonb_typeof — j.spec is plain JSON; a scalar or object value
        makes jsonb_array_elements raise.
      * UUID shape filter before the cast — a non-UUID string would
        raise on ::uuid.

    Absent key ⇒ zero rows ⇒ the LEFT JOIN in ``fanout`` leaves the child
    counted as missing, i.e. EXACTLY today's behaviour for every batch
    dispatched before the writer shipped. Never "expect nothing".
    """
    return f"""
    {cte_name} AS (
{comment}
        SELECT DISTINCT
            COALESCE(j.spec->>'batch_id', j.id::text) AS batch_id,
            (d.value #>> '{{}}')::uuid                AS child_resource_id
        FROM filtered_jobs j
        CROSS JOIN LATERAL jsonb_array_elements(
            CASE
                WHEN jsonb_typeof(
                        COALESCE((j.spec->'{spec_key}')::jsonb,
                                 '[]'::jsonb)
                     ) = 'array'
                THEN COALESCE((j.spec->'{spec_key}')::jsonb,
                              '[]'::jsonb)
                ELSE '[]'::jsonb
            END
        ) AS d(value)
        WHERE (d.value #>> '{{}}') ~*
              '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$'
    )"""


# The two withhold REASONS are stamped under separate spec keys and read
# by separate CTEs on purpose. Conflating them into one list would lose
# the information an operator needs to answer "why did this resource not
# run this cycle?" — a concurrency-cap deferral is a capacity signal
# (raise max_concurrent_backups), an overlap hold is a duration signal
# (the previous cycle is still walking that resource).
_DEFERRED_COMMENT = """\
        -- Children the backup-scheduler's max_concurrent_backups cap
        -- DELIBERATELY held back this cycle (spec.deferred_resource_ids,
        -- stamped on every job in the batch).
        --
        -- Without this, a deferral is indistinguishable from a LOST
        -- fan-out: the child exists and is dispatchable, so expected_t2
        -- wants it, but no job ever carried it — missing_t2 > 0 wedges
        -- the row at "In Progress" for the whole 48h batch_recent
        -- horizon even though every job and snapshot already settled.
        -- That produced the ghost "previous run still going" row the
        -- operator hit on 2026-08-05 (Gold, Aug 4 02:07 UTC: the cap
        -- trimmed exactly one resource — John Lucas USER_MAIL — because
        -- Gold has 50 resources and a cap of 50, and one in-flight job
        -- dropped the budget to 49)."""

_OVERLAP_HELD_COMMENT = """\
        -- Children the backup-scheduler's run-overlap guard DELIBERATELY
        -- withheld this cycle (spec.overlap_held_resource_ids, stamped on
        -- every job in the batch by dispatch_backups_for_policy).
        --
        -- Same failure mode as the concurrency cap above, different
        -- cause: partition_overlap_holds drops any resource that still
        -- has a backup in flight from the PREVIOUS cycle. Measured on
        -- prod 2026-08-11, batch 667734c3 (Gold, 10:07 UTC): all 8 jobs
        -- COMPLETED, 0 pending snapshots, 0 pending partitions — and
        -- missing_t2 = 7. The 7 were the scope users' USER_CHATS
        -- children, withheld because the 02:07 cycle still held them
        -- ("held 10 busy resource(s) and dispatched 40"). Those chats
        -- were dispatched under a DIFFERENT batch_id at the 18:07 tick
        -- (batch 2123e84f, 8 USER_CHATS snapshots), so batch 667734c3
        -- could never go terminal and its Activity row read
        -- "BACKUP · In Progress · 15 %" forever."""


def build_batch_rollup_query(
    *,
    tenant_id: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
    operation: Optional[str],
    size: int,
    offset: int,
) -> Any:
    """Build the single CTE-driven query that returns one row per batch.

    Returns a SQLAlchemy ``text()`` clause with named bind params:
        :tid, :start_date, :end_date, :op, :size, :off

    Output columns (one row per batch):
        batch_id, tenant_id, started_at, jobs_max_completed_at,
        snaps_max_completed_at, parts_max_completed_at,
        job_ids (uuid[]), entra_user_count, total_resource_count,
        bytes_added, items_added, all_jobs_terminal, any_cancelled,
        any_job_failed, snap_total, snap_done, snap_partial,
        snap_failed, snap_pending, parts_pending, missing_t2

    Performance: relies on existing indexes ``ix_jobs_tenant_started``,
    ``ix_snapshots_job_id``, ``ix_snapshot_partitions_snapshot``.
    Measured at ~80 ms on a 10 k-job tenant in similar audit-service
    aggregate queries.
    """
    # Online Archive (USER_ARCHIVE) counts toward batch completion only WHILE
    # archive backup is enabled. Gating the EXPECTED / terminal type-list on the
    # flag at query-BUILD time is what makes the flag safely reversible: turn it
    # off and archive silently drops out of "expected", so an in-flight batch
    # never pins to In Progress waiting on archive snapshots that will never come
    # (archive resources persist once discovered, so an unconditional add would
    # hang the batch forever). Default ON — matches archive_graph's default and
    # the operator's intent to always back up archives.
    _t2_types = ["USER_MAIL", "USER_ONEDRIVE", "USER_CHATS", "USER_CALENDAR", "USER_CONTACTS"]
    if archive_backup_enabled():
        _t2_types.append("USER_ARCHIVE")
    t2_types_sql = ", ".join(f"'{t}'" for t in _t2_types)

    # Compare the enum column directly against an enum literal.
    #
    # This used to be ``AND j.type::text = :op``. Casting the COLUMN to text
    # is non-sargable against idx_jobs_tenant_type_status, and it also throws
    # away the planner's selectivity estimate for filtered_jobs — the ~10
    # materialized CTEs below then plan on rows=1 and nest-loop, which is
    # exactly the O(N^2) rescan the page-cap comment further down describes.
    # Measured on prod 2026-08-13, same query, only this predicate differing:
    #   operation=None   cap=200 -> 0.25s
    #   operation=BACKUP cap=20  -> 2.68s
    #   operation=BACKUP cap=200 -> >60s (statement timeout)
    # /api/v1/activity held a connection per request for 70+ min until the
    # 40+40 pool exhausted and every audit endpoint 500'd.
    #
    # The value is inlined rather than bound because a bound parameter would
    # need an explicit ``CAST(:op AS jobtype)``, hardcoding the Postgres enum
    # type name into shared SQL. Inlining is safe ONLY because the value is
    # allow-listed against JobType first — never relax that check.
    op_filter = ""
    if operation:
        op = str(operation).strip().upper()
        if op not in _ALLOWED_OPERATIONS:
            raise ValueError(
                f"unknown operation {operation!r}; "
                f"expected one of {sorted(_ALLOWED_OPERATIONS)}"
            )
        op_filter = f"AND j.type = '{op}'"
    # One generator for both withhold reasons: the CTEs are structurally
    # identical and a hand-copied second one is exactly how the jsonb_typeof
    # guard would silently go missing from the newer of the two.
    withheld_ctes = ",".join((
        _withheld_children_cte(
            "deferred_t2", "deferred_resource_ids", _DEFERRED_COMMENT,
        ),
        _withheld_children_cte(
            "overlap_held_t2", "overlap_held_resource_ids",
            _OVERLAP_HELD_COMMENT,
        ),
    ))
    sql = f"""
    WITH filtered_jobs AS (
        SELECT *
        FROM jobs j
        WHERE (CAST(:tid AS UUID) IS NULL OR j.tenant_id = CAST(:tid AS UUID))
          AND (CAST(:start_date AS TIMESTAMP) IS NULL OR j.created_at >= CAST(:start_date AS TIMESTAMP))
          AND (CAST(:end_date   AS TIMESTAMP) IS NULL OR j.created_at <= CAST(:end_date AS TIMESTAMP))
          {op_filter}
          AND j.type = 'BACKUP'
    ),
    batches_all AS (
        SELECT
            COALESCE(j.spec->>'batch_id', j.id::text)              AS batch_id,
            j.tenant_id                                            AS tenant_id,
            MIN(j.created_at)                                      AS started_at,
            MAX(j.completed_at)                                    AS jobs_max_completed_at,
            ARRAY_AGG(j.id ORDER BY j.created_at)                  AS job_ids,
            BOOL_OR(j.status::text = 'CANCELLED')                  AS any_cancelled,
            BOOL_OR(j.status::text = 'FAILED')                     AS any_job_failed,
            BOOL_OR(j.status::text = 'COMPLETED')                  AS any_job_completed,
            BOOL_AND(j.status::text IN ('COMPLETED','FAILED','CANCELLED'))
                                                                    AS all_jobs_terminal,
            -- For non-bulk single-Job batches (no batch_resource_ids),
            -- carry through job.resource_id so we can resolve a real
            -- display name instead of falling back to "Bulk Operation".
            -- COUNT()=1 + MIN()=MAX() guarantees single Job.
            COUNT(*)                                               AS job_count,
            -- PG has no MIN/MAX for uuid; array_agg + [1] pulls the
            -- single resource_id when job_count=1.
            (ARRAY_AGG(j.resource_id))[1]                          AS single_resource_id,
            -- Any Job in this batch fired by the anomaly-detector
            -- "preemptive" sweep? Used to relabel the Activity row so
            -- the operator sees "Preemptive — <resource>" instead of
            -- a generic "1 resource Done" that looks like a phantom
            -- duplicate of their manual click.
            BOOL_OR(j.spec->>'triggered_by' = 'PREEMPTIVE')        AS any_preemptive,
            -- Did Tier-2 fan-out actually spawn for this batch? Feeds
            -- the derive's past-horizon missing_t2 arbitration: absent
            -- Tier-2 jobs + expected children = the fan-out trigger was
            -- lost (visible warning), whereas present Tier-2 jobs +
            -- missing children = expectation drift from later resource
            -- status changes (ignore).
            BOOL_OR(COALESCE(j.spec->>'tier2', 'false') = 'true')  AS any_tier2_jobs
        FROM filtered_jobs j
        GROUP BY 1, 2
    ),
    batches AS (
        -- Page-scoped cap (2026-07-13 perf fix). The final SELECT joins
        -- ~10 materialized CTEs on batch_id; CTE stats are opaque to the
        -- planner (rows=1 estimates) so it nests loops — O(N^2) CTE
        -- rescans across ALL history: 79s at 475 batches, growing with
        -- every scheduled job. Downstream CTEs only ever contribute to
        -- rows the final LIMIT/OFFSET keeps, so restricting to the
        -- newest size+offset batches here is semantics-preserving and
        -- makes the whole rollup O(page).
        SELECT * FROM batches_all
        ORDER BY started_at DESC NULLS LAST
        LIMIT :batch_cap
    ),
    batch_res AS (
        -- Flatten batch_resource_ids per batch via lateral unnest.
        -- Avoids ARRAY_AGG(uuid[]) which fails on jagged arrays and
        -- the unnest(uuid[][]) → scalar gotcha. NULLs coalesced to
        -- empty array so the LATERAL stays safe.
        SELECT
            COALESCE(j.spec->>'batch_id', j.id::text) AS batch_id,
            COALESCE(
                ARRAY_AGG(DISTINCT bid) FILTER (WHERE bid IS NOT NULL),
                ARRAY[]::uuid[]
            ) AS all_res_ids
        FROM filtered_jobs j
        LEFT JOIN LATERAL unnest(COALESCE(j.batch_resource_ids, ARRAY[]::uuid[])) AS bid ON TRUE
        GROUP BY 1
    ),
    single_res AS (
        -- A batch is "single-resource" if either (a) the only Job in
        -- it carries a non-NULL j.resource_id, OR (b) the only Job is
        -- a BATCH-shape row whose batch_resource_ids has exactly one
        -- entry (the preemptive / per-resource trigger-bulk shape).
        -- Without the (b) branch, single-element batch rows displayed
        -- "1 resource" instead of the real resource name (the 2026-
        -- 05-15 PREEMPTIVE phantom-duplicate symptom).
        SELECT
            b.batch_id,
            r.display_name AS single_resource_name,
            r.type::text   AS single_resource_type
        FROM batches b
        LEFT JOIN batch_res br ON br.batch_id = b.batch_id
        LEFT JOIN resources r ON r.id = COALESCE(
            b.single_resource_id,
            CASE
                WHEN b.job_count = 1
                 AND COALESCE(array_length(br.all_res_ids, 1), 0) = 1
                THEN br.all_res_ids[1]
            END
        )
        WHERE b.job_count = 1
    ),
    snap_roll AS (
        -- `skipped_concurrent` placeholders are NOT successes.
        --
        -- backup-worker's cross-job concurrency guard writes a 0-item
        -- snapshot with status=COMPLETED when another job already holds
        -- a live lease on the resource. Counting those as done makes a
        -- resource that captured NOTHING read green — and the failure
        -- mode that produces them is exactly the one where you most need
        -- the truth: a partition stranded IN_PROGRESS renews its lease
        -- forever, so every subsequent cycle for that resource is a
        -- placeholder (2026-08-17, `Chats — Corp_Esplanade`).
        --
        -- retention_cleanup.py:202, dr-replication-worker/main.py:300,
        -- the restore-point filter and the scheduler all already reject
        -- them; this rollup was the last consumer that did not.
        --
        -- Deliberately counted as a THIRD category rather than folded
        -- into failed: nothing is broken, the work was legitimately
        -- deferred. But it is not progress, so it must not inflate
        -- snap_done. NB `extra_data` is json, not jsonb — use
        -- json_extract_path_text, matching dr-replication-worker.
        SELECT
            b.batch_id,
            COUNT(s.id)                                             AS snap_total,
            COUNT(*) FILTER (
                WHERE s.status::text = 'COMPLETED'
                  AND COALESCE(
                        json_extract_path_text(s.extra_data, 'skipped_concurrent'),
                        'false'
                      ) <> 'true'
            )                                                       AS snap_done,
            COUNT(*) FILTER (
                WHERE COALESCE(
                        json_extract_path_text(s.extra_data, 'skipped_concurrent'),
                        'false'
                      ) = 'true'
            )                                                       AS snap_skipped,
            COUNT(*) FILTER (WHERE s.status::text = 'PARTIAL')      AS snap_partial,
            COUNT(*) FILTER (WHERE s.status::text = 'FAILED')       AS snap_failed,
            COUNT(*) FILTER (WHERE s.status::text IN ('QUEUED','IN_PROGRESS'))
                                                                    AS snap_pending,
            MAX(s.completed_at)                                     AS snaps_max_completed_at,
            COALESCE(SUM(s.bytes_added), 0)                         AS bytes_added,
            COALESCE(SUM(s.item_count), 0)                          AS items_added
        FROM batches b
        LEFT JOIN snapshots s ON s.job_id = ANY(b.job_ids)
        GROUP BY 1
    ),
    parts_roll AS (
        SELECT
            b.batch_id,
            COUNT(*) FILTER (WHERE sp.status::text NOT IN ('COMPLETED','FAILED'))
                                                                    AS parts_pending,
            MAX(sp.completed_at)                                    AS parts_max_completed_at
        FROM batches b
        LEFT JOIN snapshot_partitions sp ON sp.job_id = ANY(b.job_ids)
        GROUP BY 1
    ),
    tier1_scope AS (
        -- The CLICK-TIME scope: batch_resource_ids of the Tier-1 jobs
        -- in this batch (spec.tier2 missing or 'false'). Stable across
        -- polls because Tier-1 jobs aren't recreated and their
        -- batch_resource_ids never mutate after insert. This is what
        -- ``progress denominator`` must be derived from — using the
        -- growing union of all jobs' resource_ids gives a denominator
        -- that drops when Tier-2 jobs land, producing the visible
        -- progress-bar bounce (2026-05-15 incident: "16 % → 90 % →
        -- 30 % → 99 %"). For a Tier-2-only batch (no Tier-1 job —
        -- e.g. PREEMPTIVE single-resource), scope_ids is empty; the
        -- ``total_expected`` CTE below falls back to all_res_ids.
        SELECT
            COALESCE(j.spec->>'batch_id', j.id::text) AS batch_id,
            COALESCE(
                ARRAY_AGG(DISTINCT bid) FILTER (WHERE bid IS NOT NULL),
                ARRAY[]::uuid[]
            ) AS scope_ids
        FROM filtered_jobs j
        LEFT JOIN LATERAL unnest(COALESCE(j.batch_resource_ids, ARRAY[]::uuid[])) AS bid ON TRUE
        WHERE COALESCE(j.spec->>'tier2', 'false') = 'false'
        GROUP BY 1
    ),
    expected_t2 AS (
        -- Per batch, the Cartesian product of (ENTRA_USERs in the
        -- Tier-1 scope) × (their Tier-2 child resources from
        -- ``resources.parent_resource_id``). One row per (batch,
        -- child) tells us which Tier-2 children we EXPECT to see
        -- regardless of whether the Tier-2 Jobs have spawned yet.
        -- Joining against ``tier1_scope`` (not ``batch_res``) keeps
        -- this count stable as Tier-2 jobs land — those add USER_*
        -- ids to ``batch_res.all_res_ids`` but never to the Tier-1
        -- scope, so the expected-child set doesn't drift.
        SELECT
            ts.batch_id,
            r2.id AS child_resource_id
        FROM tier1_scope ts
        JOIN batches b ON b.batch_id = ts.batch_id
        CROSS JOIN LATERAL unnest(ts.scope_ids) AS bid
        JOIN resources r1 ON r1.id = bid AND r1.type::text = 'ENTRA_USER'
        JOIN resources r2 ON r2.parent_resource_id = r1.id
                         AND r2.type::text IN ({t2_types_sql})
                         AND COALESCE(
                             (r2.metadata->>'license_missing')::boolean,
                             false
                         ) = false
                         -- Only children the scheduler will actually
                         -- dispatch (mirrors backup-scheduler's
                         -- ResourceStatus.in_([DISCOVERED, ACTIVE]) and
                         -- its INACCESSIBLE/SUSPENDED/PENDING_DELETION
                         -- refusal). Unlicensed identity accounts get
                         -- INACCESSIBLE USER_CHATS/USER_ONEDRIVE children
                         -- from discovery; expecting those wedges every
                         -- batch containing the parent at "In Progress"
                         -- forever — including retroactively, because
                         -- expectations are computed against the current
                         -- resources table.
                         AND r2.status::text IN ('DISCOVERED', 'ACTIVE')
                         -- ...and only children that EXISTED when the
                         -- batch was dispatched. Expectations are
                         -- evaluated against the current resources
                         -- table, so a later discovery run that adds
                         -- children under a user retroactively wedges
                         -- every finished batch containing that user
                         -- (2026-07-14 18:07 batch vs children created
                         -- 23:38). Children discovered mid-run by
                         -- Tier-2 gap-fill get Jobs immediately (they
                         -- land in the observed set below) and the
                         -- discovery_pending gate covers the insert→job
                         -- window, so excluding them here is safe.
                         AND r2.created_at <= b.started_at + INTERVAL '5 minutes'
    ),
    license_skip AS (
        SELECT ts.batch_id, COUNT(*) AS skipped_no_license
        FROM tier1_scope ts
        JOIN batches b ON b.batch_id = ts.batch_id
        CROSS JOIN LATERAL unnest(ts.scope_ids) AS bid
        JOIN resources r1 ON r1.id = bid AND r1.type::text = 'ENTRA_USER'
        JOIN resources r2 ON r2.parent_resource_id = r1.id
                         AND r2.type::text IN (
                             'USER_MAIL', 'USER_ONEDRIVE', 'USER_CHATS',
                             'USER_CALENDAR', 'USER_CONTACTS')
                         AND COALESCE(
                             (r2.metadata->>'license_missing')::boolean,
                             false
                         ) = true
                         AND r2.created_at <= b.started_at + INTERVAL '5 minutes'
        GROUP BY 1
    ),
    {withheld_ctes},
    observed_t2 AS (
        SELECT DISTINCT
            br.batch_id,
            bid AS child_resource_id
        FROM batch_res br
        CROSS JOIN LATERAL unnest(br.all_res_ids) AS bid
    ),
    fanout AS (
        -- A child counts as MISSING only when no job carried it AND no
        -- dispatcher recorded withholding it. Both withhold reasons are
        -- recorded facts (see _withheld_children_cte); neither is
        -- inferred from coverage or timing.
        SELECT
            e.batch_id,
            COUNT(*) FILTER (
                WHERE o.child_resource_id IS NULL
                  AND df.child_resource_id IS NULL
                  AND oh.child_resource_id IS NULL
            ) AS missing_t2
        FROM expected_t2 e
        LEFT JOIN observed_t2 o
               ON o.batch_id = e.batch_id
              AND o.child_resource_id = e.child_resource_id
        LEFT JOIN deferred_t2 df
               ON df.batch_id = e.batch_id
              AND df.child_resource_id = e.child_resource_id
        LEFT JOIN overlap_held_t2 oh
               ON oh.batch_id = e.batch_id
              AND oh.child_resource_id = e.child_resource_id
        GROUP BY 1
    ),
    entra_count AS (
        SELECT
            br.batch_id,
            COUNT(*) FILTER (WHERE r.type::text = 'ENTRA_USER') AS entra_user_count,
            COUNT(*)                                            AS total_resource_count
        FROM batch_res br
        CROSS JOIN LATERAL unnest(br.all_res_ids) AS bid
        LEFT JOIN resources r ON r.id = bid
        GROUP BY 1
    ),
    -- When a batch targets exactly one ENTRA_USER, pull that user's
    -- display_name so the Activity row shows "Hemant Singh" instead of
    -- "1 user". Multi-user batches fall through to the "N users" label.
    entra_one AS (
        SELECT
            br.batch_id,
            (ARRAY_AGG(r.display_name))[1] AS entra_user_name
        FROM batch_res br
        CROSS JOIN LATERAL unnest(br.all_res_ids) AS bid
        JOIN resources r ON r.id = bid AND r.type::text = 'ENTRA_USER'
        GROUP BY 1
        HAVING COUNT(*) = 1
    ),
    tier1_users AS (
        -- The ENTRA_USER members of the click-time scope — i.e. the
        -- Tier-1 progress DENOMINATOR, in the same unit as the
        -- numerator.
        --
        -- 2026-08-11: tier1_total used to be
        -- ``COALESCE(array_length(ts.scope_ids,1),0)`` — the click-time
        -- SCOPE RESOURCE count — while tier1_terminal counted only
        -- ENTRA_USER snapshots. Different units. On the scheduled path
        -- the scope is the whole policy (Gold: 50 resources = 7
        -- ENTRA_USER + 8 USER_MAIL + 8 USER_CALENDAR + 8 USER_CONTACTS
        -- + 8 USER_CHATS + 6 USER_ONEDRIVE + 2 SHAREPOINT_SITE + 1
        -- USER_ARCHIVE + 1 ROOM_MAILBOX + 1 ENTRA_GROUP), so the
        -- fraction was 7/50 forever — it could not reach 1.0 even with
        -- every user done.
        SELECT ts.batch_id, COUNT(*) AS users_total
        FROM tier1_scope ts
        JOIN batches b ON b.batch_id = ts.batch_id
        CROSS JOIN LATERAL unnest(ts.scope_ids) AS bid
        JOIN resources r ON r.id = bid AND r.type::text = 'ENTRA_USER'
        GROUP BY 1
    ),
    tier2_content AS (
        -- The CONTENT expectation: every NON-ENTRA_USER resource in the
        -- click-time scope, UNIONed with every expected Tier-2 child.
        --
        -- UNION, not "+": the two overlap. On the UI click path the
        -- scope is users only and the children are disjoint from it; on
        -- the SCHEDULED path the children are policy-assigned resources
        -- and are dispatched IN the scope, so adding the two double-
        -- counts (that is why ``total_expected`` reads 84 for a Gold
        -- batch that really has 50 distinct resources).
        --
        -- Non-user scope members (SHAREPOINT_SITE, ROOM_MAILBOX,
        -- ENTRA_GROUP, POWER_BI …) belong here too: they are real
        -- content work, and the old USER_*-only type list left them out
        -- of BOTH tiers, so a SharePoint-heavy batch could never move
        -- the bar at all.
        SELECT batch_id, COUNT(*) AS content_total
        FROM (
            SELECT ts.batch_id, bid AS rid
              FROM tier1_scope ts
              JOIN batches b ON b.batch_id = ts.batch_id
              CROSS JOIN LATERAL unnest(ts.scope_ids) AS bid
              JOIN resources r ON r.id = bid
                              AND r.type::text <> 'ENTRA_USER'
            UNION
            SELECT e.batch_id, e.child_resource_id FROM expected_t2 e
        ) u
        GROUP BY 1
    ),
    tier_split AS (
        -- Split per-batch progress by tier so ``shape_batch_row`` can
        -- weight the bar (Tier-1 = 10 %, Tier-2 = 90 %). Tier-1 =
        -- ENTRA_USER identity metadata (~1 KB); Tier-2 = the heavy
        -- content workloads.
        --
        -- Both tiers are counted in DISTINCT RESOURCES, matching their
        -- denominators. Counting snapshot ROWS overcounts a resource the
        -- worker retried, which is how the numerator used to be able to
        -- outrun the denominator (2026-05-16: denom 45, terminal 68).
        --
        -- Monotonicity (the 2026-05-15 "16 % → 90 % → 30 % → 99 %"
        -- incident) holds because every input only grows during a
        -- healthy run: scope_ids never mutates after insert, expected_t2
        -- is pinned to children that existed at dispatch, and a resource
        -- that has a terminal snapshot keeps it.
        SELECT
            b.batch_id,
            COALESCE(u.users_total, 0)                     AS tier1_total,
            COUNT(DISTINCT s.resource_id) FILTER (
                WHERE r.type::text = 'ENTRA_USER'
                  AND s.status::text IN ('COMPLETED','PARTIAL','FAILED')
            )                                              AS tier1_terminal,
            -- Tier-2 denominator: must always be ≥ the numerator so the
            -- fraction can't exceed 1.0.
            --   * content_total        — the real expectation.
            --   * users_total * 5      — pre-discovery floor. Between
            --     Tier-1 completion and Tier-2 discovery the real
            --     expectation is 0; without the floor tier2_total would
            --     be 0, shape_batch_row would take the Tier-1-only
            --     branch, the bar would read 100 % and then collapse
            --     when the children land. 5 = the canonical per-user
            --     workload count (mail/onedrive/chats/calendar/
            --     contacts). NOTE the multiplier is per USER now — it
            --     used to be per SCOPE RESOURCE, which on Gold made the
            --     floor 50×5 = 250 against ~39 real children and pinned
            --     the bar at 15 % (2026-08-11 RCA).
            --   * observed distinct non-ENTRA_USER resources — the
            --     worker can spawn more Tier-2 work than expected
            --     (extra OneDrives, shared mailboxes, mid-run gap-fill
            --     children). Counting resources that have ANY snapshot
            --     (not just terminal ones) keeps the bar from reading
            --     100 % while such a resource is still in flight.
            GREATEST(
                COALESCE(c.content_total, 0),
                GREATEST(
                    COALESCE(u.users_total, 0) * 5
                    - COALESCE(ls.skipped_no_license, 0),
                    0
                ),
                COUNT(DISTINCT s.resource_id) FILTER (
                    WHERE r.type IS NOT NULL
                      AND r.type::text <> 'ENTRA_USER'
                )
            )                                              AS tier2_total,
            -- Every non-ENTRA_USER resource with a terminal snapshot in
            -- this batch. Type-list-free on purpose: the old explicit
            -- list had to be edited every time a workload was added
            -- (USER_ARCHIVE was bolted on exactly that way) and silently
            -- under-counted anything not on it.
            COUNT(DISTINCT s.resource_id) FILTER (
                WHERE r.type IS NOT NULL
                  AND r.type::text <> 'ENTRA_USER'
                  AND s.status::text IN ('COMPLETED','PARTIAL','FAILED')
            )                                              AS tier2_terminal
        FROM batches b
        LEFT JOIN tier1_users   u ON u.batch_id = b.batch_id
        LEFT JOIN tier2_content c ON c.batch_id = b.batch_id
        LEFT JOIN license_skip ls ON ls.batch_id = b.batch_id
        LEFT JOIN snapshots s    ON s.job_id = ANY(b.job_ids)
        LEFT JOIN resources r    ON r.id = s.resource_id
        GROUP BY b.batch_id, u.users_total, c.content_total, ls.skipped_no_license
    ),
    total_expected AS (
        -- Stable progress denominator. Computed from click-time scope
        -- so it doesn't drift as Tier-2 jobs land:
        --   |tier1_scope.scope_ids|  (the Tier-1 ENTRA_USER snapshots
        --                             we expect to produce)
        -- + COUNT(expected_t2)      (the Tier-2 child snapshots we
        --                             expect per ENTRA_USER × workload)
        -- For Tier-2-only batches (no Tier-1 job — e.g. PREEMPTIVE
        -- single-resource), tier1_scope is empty and the fallback in
        -- ``shape_batch_row`` uses snap_total instead. See
        -- docs/superpowers/specs/2026-05-15-activity-batch-rollup-design.md.
        SELECT
            b.batch_id,
            COALESCE(array_length(ts.scope_ids, 1), 0)
            + COALESCE((SELECT COUNT(*) FROM expected_t2 e WHERE e.batch_id = b.batch_id), 0)
              AS expected_count
        FROM batches b
        LEFT JOIN tier1_scope ts ON ts.batch_id = b.batch_id
    ),
    discovery_pending_cte AS (
        -- Tier-2 gap-fill discovery state. ``batch_pending_users`` is
        -- populated at click time by job_service for every ENTRA_USER
        -- in the batch; each row's ``state`` tracks the per-user
        -- progress through Tier-2 discovery:
        --   WAITING_DISCOVERY → discovery still running
        --   NO_CONTENT         → user has no Tier-2 children
        --   DISCOVERY_FAILED   → discovery errored (treated terminal)
        --   BACKUP_ENQUEUED    → discovery complete, Tier-2 jobs spawned
        -- If *any* user is still WAITING_DISCOVERY, the batch cannot
        -- yet declare its child-set "known" — so the rollup must say
        -- "In Progress" regardless of whatever current children show
        -- up in the resources table. This is the fix for the 2026-05-16
        -- "Done → In Progress → Done" flicker.
        --
        -- Legacy/single-resource batches (no batch_pending_users rows)
        -- correctly evaluate to FALSE because EXISTS returns false on
        -- an empty set — they keep the pre-existing missing_t2-only
        -- behaviour.
        --
        -- Cast b.batch_id (text) → uuid for the JOIN. Wrapped in a
        -- subquery so a malformed batch_id (single-Job legacy job id
        -- that didn't actually originate from a backup_batches row)
        -- doesn't crash the rollup — invalid uuids resolve to FALSE.
        SELECT
            b.batch_id,
            CASE
                WHEN b.batch_id IS NULL OR length(b.batch_id) < 32 THEN FALSE
                ELSE COALESCE(
                    (SELECT EXISTS (
                        SELECT 1
                          FROM batch_pending_users bpu
                         WHERE bpu.batch_id = cast(b.batch_id AS uuid)
                           AND bpu.state = 'WAITING_DISCOVERY'
                    )),
                    FALSE
                )
            END AS pending
        FROM batches b
    )
    SELECT
        b.batch_id,
        b.tenant_id,
        b.started_at,
        b.jobs_max_completed_at,
        sr.snaps_max_completed_at,
        pr.parts_max_completed_at,
        b.job_ids,
        COALESCE(ec.entra_user_count, 0)    AS entra_user_count,
        COALESCE(ec.total_resource_count, 0) AS total_resource_count,
        COALESCE(sr.bytes_added, 0)         AS bytes_added,
        COALESCE(sr.items_added, 0)         AS items_added,
        b.all_jobs_terminal,
        b.any_cancelled,
        b.any_job_failed,
        b.any_job_completed,
        b.any_tier2_jobs,
        -- Freshness horizon for the missing_t2 gate (see
        -- derive_batch_status): expectations are computed against the
        -- CURRENT resources table, so they may only WEDGE a batch at
        -- "In Progress" while it is recent — a later resource-status
        -- flip must not rewrite settled history (2026-08-03: a per-user
        -- discovery reset flipped long-INACCESSIBLE children back to
        -- DISCOVERED and every Gold batch since 7/27 turned
        -- "In Progress" retroactively).
        (b.started_at > NOW() - INTERVAL '48 hours') AS batch_recent,
        COALESCE(sr.snap_total, 0)          AS snap_total,
        COALESCE(sr.snap_done, 0)           AS snap_done,
        COALESCE(sr.snap_skipped, 0)        AS snap_skipped,
        COALESCE(sr.snap_partial, 0)        AS snap_partial,
        COALESCE(sr.snap_failed, 0)         AS snap_failed,
        COALESCE(sr.snap_pending, 0)        AS snap_pending,
        COALESCE(pr.parts_pending, 0)       AS parts_pending,
        COALESCE(f.missing_t2, 0)           AS missing_t2,
        COALESCE(ls.skipped_no_license, 0)  AS skipped_no_license,
        COALESCE(te.expected_count, 0)      AS expected_count,
        COALESCE(dp.pending, FALSE)         AS discovery_pending,
        COALESCE(tsp.tier1_total, 0)        AS tier1_total,
        COALESCE(tsp.tier1_terminal, 0)     AS tier1_terminal,
        COALESCE(tsp.tier2_total, 0)        AS tier2_total,
        COALESCE(tsp.tier2_terminal, 0)     AS tier2_terminal,
        sres.single_resource_name           AS single_resource_name,
        sres.single_resource_type           AS single_resource_type,
        eo.entra_user_name                  AS entra_user_name,
        COALESCE(b.any_preemptive, false)   AS any_preemptive
    FROM batches b
    LEFT JOIN snap_roll   sr ON sr.batch_id = b.batch_id
    LEFT JOIN parts_roll  pr ON pr.batch_id = b.batch_id
    LEFT JOIN fanout       f ON f.batch_id  = b.batch_id
    LEFT JOIN license_skip ls ON ls.batch_id = b.batch_id
    LEFT JOIN entra_count ec ON ec.batch_id = b.batch_id
    LEFT JOIN single_res  sres ON sres.batch_id = b.batch_id
    LEFT JOIN entra_one   eo ON eo.batch_id = b.batch_id
    LEFT JOIN total_expected te ON te.batch_id = b.batch_id
    LEFT JOIN discovery_pending_cte dp ON dp.batch_id = b.batch_id
    LEFT JOIN tier_split tsp ON tsp.batch_id = b.batch_id
    ORDER BY b.started_at DESC NULLS LAST
    LIMIT :size OFFSET :off
    """
    binds: Dict[str, Any] = dict(
        tid=tenant_id,
        start_date=start_date,
        end_date=end_date,
        size=size,
        off=offset,
        batch_cap=size + offset,
    )
    # No :op bind — the operation is allow-listed and inlined as an enum
    # literal above, so binding it here would raise ArgumentError on an
    # unused parameter.
    return text(sql).bindparams(**binds)


def shape_batch_row(row: Any) -> Dict[str, Any]:
    """Convert one CTE row → the ActivityItem JSON the frontend renders.

    Maps the SQL columns into the shape documented in §6.1 of the spec.
    The state-machine call lives here so callers don't have to re-derive.
    """
    counts = RollupCounts(
        all_jobs_terminal=bool(row.all_jobs_terminal),
        any_cancelled=bool(row.any_cancelled),
        any_job_failed=bool(row.any_job_failed),
        any_job_completed=bool(getattr(row, "any_job_completed", False)),
        snap_total=int(row.snap_total or 0),
        snap_done=int(row.snap_done or 0),
        snap_skipped=int(getattr(row, "snap_skipped", 0) or 0),
        snap_partial=int(row.snap_partial or 0),
        snap_failed=int(row.snap_failed or 0),
        snap_pending=int(row.snap_pending or 0),
        parts_pending=int(row.parts_pending or 0),
        missing_t2=int(row.missing_t2 or 0),
        expected_total=int(getattr(row, "expected_count", 0) or 0),
        discovery_pending=bool(getattr(row, "discovery_pending", False)),
        any_tier2_jobs=bool(getattr(row, "any_tier2_jobs", False)),
        batch_recent=bool(getattr(row, "batch_recent", True)),
        tier1_total=int(getattr(row, "tier1_total", 0) or 0),
        tier1_terminal=int(getattr(row, "tier1_terminal", 0) or 0),
        tier2_total=int(getattr(row, "tier2_total", 0) or 0),
        tier2_terminal=int(getattr(row, "tier2_terminal", 0) or 0),
        skipped_no_license=int(getattr(row, "skipped_no_license", 0) or 0),
    )
    status, warnings = derive_batch_status(counts)

    # finish_time is set only when truly terminal (invariant I3). Max
    # across Jobs / Snapshots / Partitions terminal timestamps — the
    # LAST thing that finished. For a Failed batch with no work
    # produced, falls back to jobs_max_completed_at.
    finish_iso = ""
    if status in ("Done", "Failed", "Canceled", "Expired"):
        # "Expired" is terminal too (backup completed, snapshots later pruned by
        # retention). Its snapshot timestamps are gone, so this falls back to
        # jobs_max_completed_at — the fire's actual completion time.
        candidates = [
            row.parts_max_completed_at,
            row.snaps_max_completed_at,
            row.jobs_max_completed_at,
        ]
        ts = max([c for c in candidates if c is not None], default=None)
        finish_iso = ts.isoformat() if ts else ""

    # Resource-count display: "9 users" if the batch targets
    # ENTRA_USERs (the M365 click scope), else the total. Never the
    # post-fan-out leaf count — that's noise from the user's POV.
    # For non-bulk single-Job backups (per-resource "Backup now"),
    # prefer the resource display name to match legacy UX.
    #
    # PREEMPTIVE (anomaly-driven) Jobs get a "Preemptive — <name>"
    # label so the operator can tell them apart from manual clicks at
    # a glance. Before this, a preemptive backup of e.g. Hemant's mail
    # rendered as "BACKUP · 1 resource · Done" which looked like a
    # phantom duplicate of the parent bulk row.
    entra = int(row.entra_user_count or 0)
    total = int(row.total_resource_count or 0)
    single_name = getattr(row, "single_resource_name", None)
    single_type = getattr(row, "single_resource_type", None)
    entra_name = getattr(row, "entra_user_name", None)
    any_preemptive = bool(getattr(row, "any_preemptive", False))
    if any_preemptive:
        # Preemptive sweeps are always per-resource (one Job, one
        # batch_resource_id). Fall back gracefully if name resolution
        # failed (e.g. resource hard-deleted).
        if single_name and single_type:
            obj_label = f"Preemptive — {single_type} · {single_name}"
        elif single_name:
            obj_label = f"Preemptive — {single_name}"
        else:
            obj_label = "Preemptive backup"
    elif entra == 1 and entra_name:
        obj_label = entra_name
    elif entra > 1:
        obj_label = f"{entra} users"
    elif total > 0:
        obj_label = single_name or (f"{total} resource" if total == 1 else f"{total} resources")
    elif single_name:
        obj_label = single_name
    else:
        obj_label = "Bulk Operation"

    # progress_pct: terminal / expected. The denominator is the
    # CLICK-TIME scope (``expected_total`` — Tier-1 ENTRA_USER count
    # + their Tier-2 child resource count) so it does NOT drift as
    # Tier-2 Jobs spawn and snapshots land. This keeps the percentage
    # monotonically non-decreasing during a healthy run — the
    # 2026-05-15 incident "16% → 90% → 30% → 99%" was caused by the
    # old denominator ``snap_total + missing_t2`` shrinking each time
    # a Tier-2 Job moved a child from "missing" to "observed".
    #
    # Fallback: for Tier-2-only batches (e.g. PREEMPTIVE single-resource
    # — no Tier-1 job, so ``tier1_scope`` is empty) ``expected_total``
    # is 0; we fall back to ``snap_total`` so the bar still progresses
    # 0→100 % rather than sticking at 0.
    #
    # No 99 % ceiling needed: with a stable denominator the only way
    # to reach 100 % is via the terminal-status branch below, which
    # also handles the Tier-1→Tier-2 handoff (``missing_t2`` keeps
    # status="In Progress" until Tier-2 Jobs spawn).
    bytes_added = int(row.bytes_added or 0)
    # Weighted progress: Tier-1 = 10 % of bar, Tier-2 = 90 %. See the
    # RollupCounts comment for rationale (data volume between the two
    # tiers is ~1 : 100, so a per-snapshot count overweights Tier-1).
    #
    # Three batch shapes to handle:
    #   (1) Tier-1 + Tier-2 (the M365 user click) — both terms apply.
    #   (2) Tier-1-only (e.g. ENTRA discovery without thenBackup) —
    #       no Tier-2 expected, scale Tier-1 to 100 %.
    #   (3) Tier-2-only (PREEMPTIVE per-resource trigger) — no Tier-1,
    #       scale Tier-2 to 100 %.
    # Fall back to ``expected_total`` / ``snap_total`` only when both
    # tier counters are zero (legacy / edge-case batches).
    if status in ("Done", "Failed", "Canceled"):
        progress_pct = 100
    elif counts.tier1_total > 0 and counts.tier2_total > 0:
        t1_frac = min(1.0, counts.tier1_terminal / counts.tier1_total)
        t2_frac = min(1.0, counts.tier2_terminal / counts.tier2_total)
        progress_pct = int(10 * t1_frac + 90 * t2_frac)
    elif counts.tier1_total > 0:
        t1_frac = min(1.0, counts.tier1_terminal / counts.tier1_total)
        progress_pct = int(100 * t1_frac)
    elif counts.tier2_total > 0:
        t2_frac = min(1.0, counts.tier2_terminal / counts.tier2_total)
        progress_pct = int(100 * t2_frac)
    else:
        # Legacy fallback: pre-tier-split batches OR truly empty
        # batch (no snapshots yet). Mirrors the old denominator so
        # behaviour is unchanged for callers that never populate the
        # tier_split CTE (e.g. unit tests).
        denom = counts.expected_total if counts.expected_total > 0 else counts.snap_total
        if denom > 0:
            terminal = counts.snap_done + counts.snap_partial + counts.snap_failed
            progress_pct = min(100, int(100 * terminal / denom))
        else:
            progress_pct = 0
    # Clamp below 100 while still "In Progress" so status and percent
    # agree. The Tier-1=10/Tier-2=90 split makes the natural ceiling
    # 99 (e.g. Tier-1 done + 98 / 99 Tier-2 children done = 10 + 89 = 99),
    # but on rounding the int can hit 100 before the terminal status
    # latches.
    if status == "In Progress" and progress_pct >= 100:
        progress_pct = 99

    # Phase chip — v1 collapses to in_progress / done. Refining to
    # discovering / urgent / heavy needs Tier-1 vs Tier-2 Job-type
    # discrimination which we can add as a follow-up; for now the
    # operator gets the binary signal.
    phase = "done" if status in ("Done", "Failed", "Canceled") else "in_progress"

    # Cancel button shows only while at least one Job can still be
    # cancelled.
    cancellable = not (counts.all_jobs_terminal or counts.any_cancelled)

    job_ids = [str(j) for j in (row.job_ids or [])]

    return {
        "id":             row.batch_id,
        "batchId":        row.batch_id,
        "jobIds":         job_ids,
        "start_time":     row.started_at.isoformat() if row.started_at else "",
        "operation":      "BACKUP",
        "object":         obj_label,
        "status":         status,
        "finish_time":    finish_iso,
        "details":        _format_details(status, bytes_added, counts),
        "data_backed_up": bytes_added,
        # No true bytes_expected today — emit 0 instead of echoing
        # bytes_added (which forced any "X of Y bytes" UI to a fake
        # 100 %). Clients already treat 0 as "unknown total".
        "total_data":     0,
        "phase":          phase,
        "counts": {
            "total":       counts.snap_total,
            "done":        counts.snap_done,
            "partial":     counts.snap_partial,
            "failed":      counts.snap_failed,
            "in_progress": counts.snap_pending,
            "queued":      0,  # combined into snap_pending today
        },
        "warnings":     warnings,
        "progress_pct": progress_pct,
        "cancellable":  cancellable,
    }


def _format_details(status: str, bytes_added: int, counts: RollupCounts) -> str:
    """Human-readable summary line.

    Percent is intentionally NOT included for "In Progress" rows: the
    UI already renders the percent in the dedicated progress bar (row)
    and the detail-panel header, both driven by the ``progress_pct``
    field. Carrying a second percent here produced visible
    mismatches across polls (row text said 78 % while the bar showed
    83 % because the client clamps the bar monotonically). Single
    source of truth = no mismatch possible.
    """
    if status == "Done":
        if bytes_added > 0:
            return f"{_fmt_bytes(bytes_added)} backed up"
        return "Completed"
    if status == "Failed":
        return "Failed"
    if status == "Canceled":
        return "Cancelled"
    # In Progress — bytes-only progress hint, no percent.
    if bytes_added > 0:
        return f"{_fmt_bytes(bytes_added)} so far"
    return "In progress"


def _fmt_bytes(n: int) -> str:
    """1024-base formatter; matches the legacy helper in audit-service."""
    if n < 1024:
        return f"{n} B"
    units = ["KiB", "MiB", "GiB", "TiB", "PiB"]
    v = float(n) / 1024.0
    for u in units:
        if v < 1024.0:
            return f"{v:.1f} {u}"
        v /= 1024.0
    return f"{v:.1f} EiB"


async def _finalize_batch_if_complete(batch_id, session) -> Optional[str]:
    """Strict 4-condition completion gate for a backup_batches row.

    Returns the new terminal status ('COMPLETED' / 'PARTIAL' / 'FAILED')
    when the gate passed AND the row was actually flipped from
    IN_PROGRESS. Returns None when the gate did not pass OR another
    worker already flipped the row (idempotent via ``WHERE status =
    'IN_PROGRESS'``).

    Gate conditions (all must pass; see design spec
    ``docs/superpowers/specs/2026-05-15-backup-batch-row-redesign-design.md``):

      1. Every ENTRA_USER in ``scope_user_ids`` has at least one Tier-2
         child in ``resources`` (parent_resource_id = user.id,
         archived_at IS NULL). Non-ENTRA_USER scope entries skip this
         check (treated as direct leaves).
      2. Every Tier-2 child has a snapshot with ``created_at >
         batch.created_at`` AND ``status::text IN
         ('COMPLETED','PARTIAL','FAILED')``.
      3. Every ENTRA_USER itself has a snapshot with ``created_at >
         batch.created_at`` AND ``status::text IN
         ('COMPLETED','PARTIAL','FAILED')``. Non-ENTRA scope entries
         use this rule directly.
      4. No ``snapshot_partitions`` row for any of those snapshots has
         ``status::text IN ('QUEUED','IN_PROGRESS')``.

    SQL safety: ``cast(:x AS uuid)`` (not ``:x::uuid``);
    ``COALESCE(spec::jsonb, ...)`` (spec column is JSON, not JSONB);
    enums compared with ``::text``; writes wrapped in
    ``SELECT FOR UPDATE`` for race protection.
    """
    locked = (await session.execute(text("""
        SELECT id, created_at, scope_user_ids
          FROM backup_batches
         WHERE id = cast(:bid AS uuid)
           AND status = 'IN_PROGRESS'
         FOR UPDATE
    """), {"bid": str(batch_id)})).first()
    if not locked:
        return None

    batch_created_at = locked.created_at
    scope = list(locked.scope_user_ids or [])
    if not scope:
        await session.execute(text("""
            UPDATE backup_batches
               SET status = 'COMPLETED', completed_at = NOW()
             WHERE id = cast(:bid AS uuid) AND status = 'IN_PROGRESS'
        """), {"bid": str(batch_id)})
        await session.commit()
        return "COMPLETED"

    scope_str = [str(u) for u in scope]

    # Classify scope: ENTRA_USER rows expand to (user + tier-2 children);
    # non-ENTRA scope rows are treated as direct leaves.
    entra_rows = (await session.execute(text("""
        SELECT id FROM resources
         WHERE id = ANY(cast(:ids AS uuid[]))
           AND type = 'ENTRA_USER'
           AND archived_at IS NULL
    """), {"ids": scope_str})).all()
    entra_ids = [str(r.id) for r in entra_rows]
    non_entra = [s for s in scope_str if s not in set(entra_ids)]

    # Gate 1: every ENTRA_USER in scope must be reachable from EITHER
    #   (a) at least one non-archived Tier-2 child resource, OR
    #   (b) a batch_pending_users row in a terminal state
    #       (NO_CONTENT / DISCOVERY_FAILED / BACKUP_ENQUEUED).
    # BACKUP_ENQUEUED still requires the resulting backup snapshots
    # to terminalize, which gate 2 (snapshots present) handles
    # downstream — so flipping a row to BACKUP_ENQUEUED simultaneously
    # creates new Tier-2 children, and (a) becomes true on the next
    # finalizer tick. NO_CONTENT / DISCOVERY_FAILED users contribute
    # no work and (b) is what lets the batch finalize as PARTIAL.
    #
    # Backward-compat: batches predating the pending-rows fix have
    # no batch_pending_users entries; the LEFT JOIN returns NULL and
    # we fall back to the original "must have Tier-2 children" rule —
    # zero regression for in-flight pre-deploy batches.
    # See docs/superpowers/specs/2026-05-15-backup-batch-race-fix-design.md.
    if entra_ids:
        missing_t2 = (await session.execute(text("""
            SELECT s.user_id
              FROM unnest(cast(:ids AS uuid[])) s(user_id)
              LEFT JOIN resources r
                ON r.parent_resource_id = s.user_id
               AND r.archived_at IS NULL
              LEFT JOIN batch_pending_users bpu
                ON bpu.user_id  = s.user_id
               AND bpu.batch_id = cast(:bid AS uuid)
             GROUP BY s.user_id, bpu.state
            HAVING COUNT(r.id) = 0
               AND (bpu.state IS NULL OR bpu.state = 'WAITING_DISCOVERY')
        """), {"ids": entra_ids, "bid": str(batch_id)})).all()
        if missing_t2:
            return None

    # Resolve full leaf set: entra-users + their tier-2 children + non-entra scope.
    if entra_ids:
        leaves_rows = (await session.execute(text("""
            WITH scope AS (
                SELECT cast(unnest(cast(:ids AS uuid[])) AS uuid) AS user_id
            ),
            targets AS (
                SELECT user_id AS resource_id FROM scope
                UNION ALL
                SELECT r.id AS resource_id
                  FROM resources r
                  JOIN scope s ON r.parent_resource_id = s.user_id
                 WHERE r.archived_at IS NULL
                   -- Exclude workloads the user has NO M365 license for.
                   -- They can never produce a snapshot, so requiring one in
                   -- gates 2/3 below would pin the batch at IN_PROGRESS
                   -- forever (the Amit-Kumar / Lyly "never completes" bug).
                   -- Discovery clears metadata.license_missing when a license
                   -- appears, so the workload rejoins the leaf set on the
                   -- next fire automatically.
                   AND COALESCE(
                       (r.metadata->>'license_missing')::boolean, false
                   ) = false
                   -- Same safety valve for Online Archive: when archive backup
                   -- is disabled no archive snapshot is dispatched, so keeping
                   -- USER_ARCHIVE in the leaf set would pin the batch forever.
                   -- Gated to match the DISPLAY rollup (archive_backup_enabled).
                   AND (:archive_on OR r.type::text <> 'USER_ARCHIVE')
            )
            SELECT DISTINCT resource_id FROM targets
        """), {"ids": entra_ids, "archive_on": archive_backup_enabled()})).all()
        leaf_ids = [str(r.resource_id) for r in leaves_rows]
    else:
        leaf_ids = []
    leaf_ids.extend(non_entra)
    leaf_ids = list(dict.fromkeys(leaf_ids))  # dedupe, preserve order

    if not leaf_ids:
        return None

    # Gates 2 + 3: every leaf must have a terminal snapshot newer than batch.created_at.
    pending = (await session.execute(text("""
        WITH leaves AS (
            SELECT cast(unnest(cast(:lids AS uuid[])) AS uuid) AS rid
        ),
        latest AS (
            SELECT DISTINCT ON (s.resource_id)
                   s.resource_id, s.status::text AS status
              FROM snapshots s
              JOIN leaves l ON s.resource_id = l.rid
             WHERE s.created_at > cast(:created AS timestamp)
             ORDER BY s.resource_id, s.created_at DESC
        )
        SELECT l.rid
          FROM leaves l
          LEFT JOIN latest la ON la.resource_id = l.rid
         WHERE la.resource_id IS NULL
            OR la.status NOT IN ('COMPLETED','PARTIAL','FAILED')
    """), {"lids": leaf_ids, "created": batch_created_at})).all()
    if pending:
        return None

    # Gate 4: every snapshot_partition for those snapshots must be terminal.
    inflight = (await session.execute(text("""
        SELECT 1
          FROM snapshot_partitions sp
          JOIN snapshots s ON s.id = sp.snapshot_id
         WHERE s.resource_id = ANY(cast(:lids AS uuid[]))
           AND s.created_at > cast(:created AS timestamp)
           AND sp.status::text IN ('QUEUED','IN_PROGRESS')
         LIMIT 1
    """), {"lids": leaf_ids, "created": batch_created_at})).first()
    if inflight:
        return None

    # Resolve target status — PARTIAL if any leaf snapshot is PARTIAL/FAILED.
    any_partial = (await session.execute(text("""
        SELECT 1
          FROM snapshots s
         WHERE s.resource_id = ANY(cast(:lids AS uuid[]))
           AND s.created_at > cast(:created AS timestamp)
           AND s.status::text IN ('PARTIAL','FAILED')
         LIMIT 1
    """), {"lids": leaf_ids, "created": batch_created_at})).first()
    new_status = "PARTIAL" if any_partial else "COMPLETED"

    await session.execute(text("""
        UPDATE backup_batches
           SET status = :ns, completed_at = NOW()
         WHERE id = cast(:bid AS uuid)
           AND status = 'IN_PROGRESS'
    """), {"ns": new_status, "bid": str(batch_id)})
    await session.commit()
    return new_status
