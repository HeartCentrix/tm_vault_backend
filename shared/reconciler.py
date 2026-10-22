"""Reconciler sweep — bottom-up orphan finalization.

Runs once per tick (default every 60 s) in Backup_scheduler. Walks the
work-row tree (partitions → snapshots → jobs) and finalizes any row
that is "orphaned": stuck in a non-terminal status, with no live
worker holding the lease, and whose children are all terminal.

Layer ordering matters. We sweep bottom-up so a parent row never
finalizes before its children — otherwise the parent might pick
COMPLETED while a still-pending child would later flip to FAILED, and
we'd have to re-open the parent (which is out of scope).

Design ref: docs/superpowers/specs/2026-05-16-distributed-reconciliation-design.md §7.
"""
from __future__ import annotations

import json
import uuid
import os
from dataclasses import dataclass, field
from typing import List, Optional

from sqlalchemy import text


RECONCILER_BATCH = int(os.getenv("RECONCILER_BATCH", "200"))
RECONCILER_MAX_REQUEUE = int(os.getenv("RECONCILER_MAX_REQUEUE", "3"))
HEARTBEAT_STALE_S = int(os.getenv("HEARTBEAT_STALE_S", "60"))
# Minimum age before a partition is eligible for requeue. Was hard-coded
# to 5 minutes — too aggressive at 5k-user prod scale where a single
# CHATS shard can sit in RMQ behind the running batch for 6+ minutes
# (workers limited to ~12 concurrent drains across 4+2 replicas, and
# partitioned snapshots commonly have 4 shards each). 5 min meant the
# reconciler bumped requeue_count on partitions that simply hadn't been
# popped yet, and after 3 bumps (= 15 min) they were poisoned to FAILED
# without any worker ever having touched them. 15 min lets a slow drain
# queue clear naturally before the recovery path kicks in. Real worker
# deaths are detected separately via lease_expires_at expiry (10 min
# TTL), so this knob only governs the rare "message lost in RMQ" case.
RECONCILER_PART_STALE_MIN = int(
    os.getenv("RECONCILER_PART_STALE_MIN", "15"),
)
# Separate, much longer horizon for partitions NO worker has ever
# claimed. Those rows have lease_owner_id/lease_expires_at NULL, so the
# lease guard above protects nothing and enqueued_at age was the only
# gate — a shard merely waiting in a saturated queue (routine during
# worker OOMKill churn and large fan-outs) got requeue_count bumped
# every 15 min and was poisoned to FAILED after 3 bumps (~45 min)
# without anything being wrong with it. Never-claimed rows are now
# re-published on this horizon instead (see STEP A), and are never
# treated as poison pills: re-publishing every 15 min only amplifies the
# queue that is already the problem, so 60 min.
RECONCILER_PART_UNCLAIMED_STALE_MIN = int(
    os.getenv("RECONCILER_PART_UNCLAIMED_STALE_MIN", "60"),
)
# ...but the never-claimed republish still has to be BOUNDED, for two
# reasons. (a) The original AMQP message is normally still in RabbitMQ,
# so every republish leaves one more copy behind — unbounded means one
# surplus copy per shard per hour forever on the queue that is already
# the bottleneck. (b) Some partition types have no republish route at
# all (see _PARTITION_QUEUE_BY_TYPE): without a terminal state those
# rows would sit QUEUED forever, holding their snapshot IN_PROGRESS and
# their job RUNNING, with no work_dead_letter row and nothing to alert
# on. After this many republish attempts the shard is marked FAILED and
# dead-lettered with reason 'unclaimed_expired' — a loud "nobody is
# draining this queue" signal instead of a silent forever-hang.
# 5 attempts x 60 min = the queue had ~5 h to produce one consumer.
RECONCILER_PART_UNCLAIMED_MAX_REPUBLISH = int(
    os.getenv("RECONCILER_PART_UNCLAIMED_MAX_REPUBLISH", "5"),
)
# The never-claimed republish counter lives in the existing
# ``failure_state`` JSON column (the backup-scheduler stale-sweep
# already writes that column the same way), so this needs no migration.
# It is DELIBERATELY not ``requeue_count``: that counter is the
# circuit breaker for shards a worker claimed and then died on, and
# spending it on time spent waiting in a queue means the first genuine
# worker death after a long queue wait finds the budget empty and
# poisons the shard to FAILED with zero recovery requeues.
# The regex guard makes a hand-edited / garbage value read as 0 rather
# than raising, which would kill the whole sweep tick (and with it every
# snapshot/job finalization, not just this row).
_UNCLAIMED_RQ = (
    "COALESCE(CASE WHEN {c}->>'unclaimed_republish_count' ~ '^[0-9]+$' "
    "THEN ({c}->>'unclaimed_republish_count')::int END, 0)"
)
_UNCLAIMED_RQ_SP = _UNCLAIMED_RQ.format(c="sp.failure_state")
# Shared "no worker has ever touched this row" predicate. Kept in one
# place so the republish branch and the expiry branch can never drift
# into overlapping with the claimed-then-abandoned paths.
_NEVER_CLAIMED = """\
{p}status = 'QUEUED'
                   AND {p}worker_id IS NULL
                   AND {p}started_at IS NULL
                   AND {p}lease_owner_id IS NULL
                   AND {p}retry_count = 0"""
# Age fallback for legacy rows that don't have leases stamped yet.
# Once every writer is lease-aware this becomes belt-and-braces, but
# it's the primary path on the day this PR lands (current in-flight
# data has lease_owner_id IS NULL on rows already RUNNING).
JOB_LEGACY_AGE_S = int(os.getenv("RECONCILER_JOB_LEGACY_AGE_S", "900"))   # 15 min
SNAP_LEGACY_AGE_S = int(os.getenv("RECONCILER_SNAP_LEGACY_AGE_S", "900"))  # 15 min
DRY_RUN = os.getenv("RECONCILER_DRY_RUN", "false").lower() in ("true", "1", "yes")

# STEP B slow-path corroboration (2026-08-25). During the 8/24-8/25
# pooler-starvation storms the reconciler settled LIVE walks FAILED:
# the worker was alive and progressing, but its lease-renew and
# heartbeat WRITES were the very connections query_wait_timeout was
# killing, so "lease expired + no live heartbeat" was true while the
# walk itself kept going (snapshot 1386f54c was flipped terminal
# mid-carry-forward; 29c425d5 finished 856k items and was orphaned).
# A dead lease alone is one severed write; require it to STAY dead
# for a grace window, and treat recently-committed snapshot_items as
# proof of life regardless of what the lease says.
SNAP_REAP_LEASE_GRACE_S = int(os.getenv("RECONCILER_SNAP_REAP_LEASE_GRACE_S", "900"))
# 9000 matches ARCHIVE_STALL_ABORT_S: the archive walk's legitimate
# zero-progress quiet phases (retry ladder ~3600s, carry-forward copy
# ~2880s and growing — its INSERT..SELECT rows are invisible until the
# single end-of-copy commit) all exceed a shorter window, which would
# make the proof-of-life veto inert for exactly the runs it protects.
# The worker's own stall guard owns the genuinely-hung case.
SNAP_REAP_PROGRESS_GRACE_S = int(os.getenv("RECONCILER_SNAP_REAP_PROGRESS_GRACE_S", "9000"))


@dataclass
class SweepStats:
    partitions_requeued: int = 0
    partitions_dlq: int = 0
    snapshots_finalized: int = 0
    jobs_finalized: int = 0
    requeue_payloads: List[dict] = field(default_factory=list)
    # Subset of partitions_requeued that had never been claimed by any
    # worker (message still in RabbitMQ), and the subset of
    # partitions_dlq that hit the never-claimed republish bound. Broken
    # out for the log line: "unclaimed_expired > 0" means a queue has no
    # consumer draining it, which is a very different page from a
    # poison-pill shard.
    partitions_unclaimed_requeued: int = 0
    partitions_unclaimed_expired: int = 0

    def total(self) -> int:
        return (
            self.partitions_requeued
            + self.partitions_dlq
            + self.snapshots_finalized
            + self.jobs_finalized
        )


def _partition_envelope(row) -> dict:
    """The one partition-message shape ``republish_partition_messages``
    consumes.

    Extracted so the requeue paths and the dead-letter paths cannot drift:
    a work_dead_letter row is only replayable if it stores exactly what a
    republish would have published. Both callers must therefore RETURNING
    the same column set (id, snapshot_id, partition_type, partition_index,
    drive_id, file_ids, payload).
    """
    return {
        "kind": "partition",
        "id": str(row.id),
        "snapshot_id": str(row.snapshot_id),
        "partition_type": row.partition_type,
        "partition_index": row.partition_index,
        "drive_id": row.drive_id,
        "file_ids": row.file_ids,
        "payload": row.payload,
    }


async def _dead_letter_partition(session, row, reason: str) -> None:
    """Record an abandoned shard so it can be triaged AND replayed.

    Before 2026-08-11 this was ``INSERT INTO work_dead_letter (work_kind,
    work_id, reason)`` with no payload, and nothing anywhere read the table.
    Consequences, both measured on prod: all 12 rows (2026-06-17/18,
    reason=poison_pill) have ``last_payload IS NULL`` and name a partition
    UUID and nothing else, so none can be replayed or even attributed — and
    once retention drops the partition row they become permanently
    uninterpretable. Meanwhile the `alerts` table had 0 rows in its entire
    history, so an operator was never told a backup shard had been given up
    on.

    Now stores the full republish envelope (replay == read last_payload,
    hand it to republish_partition_messages), emits one alert per abandoned
    shard, and prints one greppable structured line.

    Failure here must never abort the sweep: this runs inside the 60s tick's
    transaction, so an exception would roll back the whole sweep every
    minute. The alert INSERT resolves tenant_id through a subselect against
    `tenants` so a since-deleted tenant yields NULL rather than tripping the
    FK.
    """
    envelope = _partition_envelope(row)
    await session.execute(
        text(
            """
            INSERT INTO work_dead_letter
                        (work_kind, work_id, reason, last_payload)
            VALUES ('partition', cast(:wid AS uuid), :reason,
                    cast(:body AS json))
            """
        ),
        {
            "wid": envelope["id"],
            "reason": reason,
            # `reason` rides along INSIDE last_payload as well as in its own
            # column: whoever replays this row is reading the JSON, and needs
            # to know what it was abandoned for without a second lookup.
            # republish_partition_messages picks named keys out of the dict,
            # so the extra key is inert on replay.
            "body": json.dumps({**envelope, "reason": reason}, default=str),
        },
    )
    try:
        # NOTE: `alerts` has NO `title` column — the shape is
        # (type, severity, message, resource_*, triggered_by, details).
        # tenant_id and resource_id are FKs, so both are resolved through
        # subselects that yield NULL when the row is gone; a raising INSERT
        # here would roll back the whole 60s sweep.
        await session.execute(
            text(
                """
                INSERT INTO alerts
                            (id, tenant_id, type, severity, message,
                             resource_id, resource_type, triggered_by,
                             details, created_at, updated_at)
                SELECT cast(:aid AS uuid),
                       (SELECT id FROM tenants
                         WHERE id = cast(:tenant AS uuid)),
                       'WORK_DEAD_LETTER', 'HIGH', :message,
                       cast(:resource AS uuid),
                       'SNAPSHOT_PARTITION', 'reconciler',
                       cast(:details AS json), NOW(), NOW()
                """
            ),
            {
                "aid": str(uuid.uuid4()),
                "tenant": str(getattr(row, "tenant_id", None) or "") or None,
                "resource": str(getattr(row, "resource_id", None) or "") or None,
                # Name the recovery action in the alert itself. An operator
                # reading this at 3am should not have to know that
                # work_dead_letter.last_payload is replayable.
                "message": (
                    f"Backup shard abandoned ({reason}): "
                    f"{envelope['partition_type']} shard "
                    f"{envelope['partition_index']} of snapshot "
                    f"{envelope['snapshot_id']} was dead-lettered and will "
                    f"not be retried automatically. To recover: replay "
                    f"work_dead_letter.last_payload for work_id "
                    f"{envelope['id']} through "
                    f"republish_partition_messages, or re-run the resource's "
                    f"next scheduled backup."
                ),
                "details": json.dumps(
                    {
                        "work_kind": "partition",
                        "work_id": envelope["id"],
                        "reason": reason,
                        "snapshot_id": envelope["snapshot_id"],
                        "partition_type": envelope["partition_type"],
                        "partition_index": envelope["partition_index"],
                        "replayable": True,
                    },
                    default=str,
                ),
            },
        )
    except Exception as exc:  # pragma: no cover - defence in depth
        # The dead_letter row itself is the durable record; losing the alert
        # is bad but losing the sweep every 60s is worse.
        print(
            f"[RECONCILER][DLQ] alert insert failed for "
            f"work_id={envelope['id']}: {type(exc).__name__}: {exc}"
        )
    print(
        f"[RECONCILER][DLQ] work_kind=partition work_id={envelope['id']} "
        f"reason={reason} snapshot_id={envelope['snapshot_id']} "
        f"partition_type={envelope['partition_type']} "
        f"partition_index={envelope['partition_index']} replayable=true"
    )


async def sweep_orphans(session) -> SweepStats:
    """Single sweep tick. Caller owns the session + commit timing."""
    stats = SweepStats()

    # ------------------------------------------------------------------
    # STEP A — orphan partitions
    # ------------------------------------------------------------------
    # Re-publish partition messages whose owning worker is dead. Cap
    # at RECONCILER_MAX_REQUEUE → after that, mark FAILED + DLQ.
    requeue_rows = (
        await session.execute(
            text(
                """
                -- enqueued_at is reset to NOW() on each requeue (see the
                -- UPDATE below) so the staleness window starts over per
                -- requeue cycle. Without that reset the criterion stayed
                -- TRUE forever once it first matched, and every reconciler
                -- tick (60 s) would bump requeue_count + 1 — three ticks
                -- = poison-pill regardless of whether the message had
                -- actually been picked up. Observed 2026-05-16 prod: 22
                -- partitions hit rq>=1 within ~7 min of being enqueued
                -- (workers still busy on prior partitions), and 2 hit
                -- rq=3 = FAILED without any worker ever touching them.
                WITH stale AS (
                    SELECT sp.id, sp.snapshot_id, sp.partition_type,
                           sp.partition_index, sp.drive_id, sp.file_ids,
                           sp.payload, sp.requeue_count
                      FROM snapshot_partitions sp
                      LEFT JOIN worker_heartbeats wh
                             ON wh.worker_id = sp.lease_owner_id
                            AND wh.last_seen_at > NOW() - (:stale_s * INTERVAL '1 second')
                     WHERE sp.status IN ('QUEUED','IN_PROGRESS')
                       AND wh.worker_id IS NULL
                       AND (
                            sp.lease_expires_at IS NULL
                         OR sp.lease_expires_at < NOW()
                       )
                       -- Claimed then abandoned: a worker really took
                       -- this shard. Evidence survives a requeue — the
                       -- reconciler leaves worker_id/started_at alone,
                       -- and the legacy stale-sweep (backup-scheduler)
                       -- which does NULL them bumps retry_count.
                       -- Original 15-min horizon + requeue cap. Rows no
                       -- worker ever claimed are handled by their own
                       -- statement below, on a longer horizon and with
                       -- their own (separate) attempt budget.
                       AND (
                            sp.status = 'IN_PROGRESS'
                         OR sp.worker_id IS NOT NULL
                         OR sp.started_at IS NOT NULL
                         OR sp.lease_owner_id IS NOT NULL
                         OR sp.retry_count > 0
                       )
                       AND sp.enqueued_at <
                           NOW() - (:part_stale * INTERVAL '1 minute')
                       AND sp.requeue_count < :max_rq
                     ORDER BY sp.enqueued_at
                     LIMIT :batch
                     FOR UPDATE OF sp SKIP LOCKED
                )
                UPDATE snapshot_partitions sp
                   SET status         = 'QUEUED',
                       lease_owner_id = NULL,
                       lease_expires_at = NULL,
                       lease_token    = sp.lease_token + 1,
                       requeue_count  = sp.requeue_count + 1,
                       enqueued_at    = NOW()
                  FROM stale
                 WHERE sp.id = stale.id
                RETURNING sp.id, sp.snapshot_id, sp.partition_type,
                          sp.partition_index, sp.drive_id, sp.file_ids,
                          sp.payload, sp.requeue_count
                """
            ),
            {
                "stale_s": HEARTBEAT_STALE_S,
                "max_rq": RECONCILER_MAX_REQUEUE,
                "batch": RECONCILER_BATCH,
                "part_stale": RECONCILER_PART_STALE_MIN,
            },
        )
    ).all()
    for r in requeue_rows:
        stats.requeue_payloads.append(_partition_envelope(r))
    stats.partitions_requeued = len(requeue_rows)

    # ---- never-claimed shards: bounded re-publish on a longer horizon --
    # No worker has ever touched these rows, so the lease guard above
    # matches vacuously and enqueued_at age is the only signal. They are
    # healthy work whose AMQP message was lost (or whose queue has no
    # consumer), so they are re-published rather than failed — but the
    # attempts are counted in failure_state, NOT in requeue_count, so a
    # long queue wait can never spend the circuit-breaker budget that
    # exists for shards a worker claimed and died on.
    unclaimed_rows = (
        await session.execute(
            text(
                f"""
                WITH unclaimed AS (
                    SELECT sp.id, sp.snapshot_id, sp.partition_type,
                           sp.partition_index, sp.drive_id, sp.file_ids,
                           sp.payload, sp.requeue_count
                      FROM snapshot_partitions sp
                      LEFT JOIN worker_heartbeats wh
                             ON wh.worker_id = sp.lease_owner_id
                            AND wh.last_seen_at > NOW() - (:stale_s * INTERVAL '1 second')
                     WHERE {_NEVER_CLAIMED.format(p="sp.")}
                       AND wh.worker_id IS NULL
                       AND sp.lease_expires_at IS NULL
                       AND sp.enqueued_at <
                           NOW() - (:unclaimed_stale * INTERVAL '1 minute')
                       AND {_UNCLAIMED_RQ_SP} < :max_unclaimed_rq
                     ORDER BY sp.enqueued_at
                     LIMIT :batch
                     FOR UPDATE OF sp SKIP LOCKED
                )
                UPDATE snapshot_partitions sp
                   SET lease_token = sp.lease_token + 1,
                       enqueued_at = NOW(),
                       failure_state = COALESCE(
                               sp.failure_state, '{{}}'::json
                           )::jsonb || jsonb_build_object(
                               'unclaimed_republish_count',
                               {_UNCLAIMED_RQ_SP} + 1,
                               'unclaimed_last_republish_at', NOW()
                           )
                  FROM unclaimed u
                 WHERE sp.id = u.id
                RETURNING sp.id, sp.snapshot_id, sp.partition_type,
                          sp.partition_index, sp.drive_id, sp.file_ids,
                          sp.payload, sp.requeue_count
                """
            ),
            {
                "stale_s": HEARTBEAT_STALE_S,
                "batch": RECONCILER_BATCH,
                "unclaimed_stale": RECONCILER_PART_UNCLAIMED_STALE_MIN,
                "max_unclaimed_rq": RECONCILER_PART_UNCLAIMED_MAX_REPUBLISH,
            },
        )
    ).all()
    for r in unclaimed_rows:
        stats.requeue_payloads.append(_partition_envelope(r))
    stats.partitions_unclaimed_requeued = len(unclaimed_rows)
    stats.partitions_requeued += len(unclaimed_rows)

    # Partitions that exceeded the requeue cap → mark FAILED + DLQ.
    poison_rows = (
        await session.execute(
            text(
                """
                UPDATE snapshot_partitions
                   SET status = 'FAILED',
                       lease_owner_id = NULL,
                       lease_expires_at = NULL,
                       lease_token = lease_token + 1
                 WHERE status IN ('QUEUED','IN_PROGRESS')
                   AND requeue_count >= :max_rq
                   AND (lease_expires_at IS NULL OR lease_expires_at < NOW())
                   -- Only a shard some worker actually claimed can be a
                   -- poison pill. A never-claimed QUEUED row is healthy
                   -- work still waiting in RabbitMQ; failing + DLQ-ing
                   -- it loses a backup that never started. Same
                   -- claim-evidence predicate as the CTE above.
                   AND (
                        status = 'IN_PROGRESS'
                     OR worker_id IS NOT NULL
                     OR started_at IS NOT NULL
                     OR lease_owner_id IS NOT NULL
                     OR retry_count > 0
                   )
                -- Same column set as the requeue CTEs above: a dead-letter
                -- row is only replayable if it stores exactly what a
                -- republish would have published (_partition_envelope).
                RETURNING id, snapshot_id, tenant_id, resource_id,
                          partition_type, partition_index,
                          drive_id, file_ids, payload
                """
            ),
            {"max_rq": RECONCILER_MAX_REQUEUE},
        )
    ).all()
    for r in poison_rows:
        await _dead_letter_partition(session, r, "poison_pill")
    stats.partitions_dlq = len(poison_rows)

    # ---- never-claimed shards that exhausted their republish budget ----
    # Terminal, deliberately loud. Reaching here means either the queue
    # has had no consumer for RECONCILER_PART_UNCLAIMED_MAX_REPUBLISH
    # horizons, or the partition_type has no route in
    # _PARTITION_QUEUE_BY_TYPE so the republish was a no-op. Both are
    # operator problems, and both are far worse handled by leaving the
    # row QUEUED forever: that pins the snapshot at IN_PROGRESS and the
    # job at RUNNING (STEP B/C require no non-terminal children), blocks
    # backup_batches finalization, and writes no work_dead_letter row —
    # i.e. an invisible, permanently stuck backup. FAILED + DLQ is
    # visible and the next scheduled run retries the resource.
    unclaimed_expired_rows = (
        await session.execute(
            text(
                f"""
                UPDATE snapshot_partitions sp
                   SET status = 'FAILED',
                       lease_owner_id = NULL,
                       lease_expires_at = NULL,
                       lease_token = sp.lease_token + 1,
                       failure_state = COALESCE(
                               sp.failure_state, '{{}}'::json
                           )::jsonb || jsonb_build_object(
                               'unclaimed_expired', true,
                               'unclaimed_expired_at', NOW()
                           )
                 WHERE {_NEVER_CLAIMED.format(p="sp.")}
                   AND sp.lease_expires_at IS NULL
                   AND sp.enqueued_at <
                       NOW() - (:unclaimed_stale * INTERVAL '1 minute')
                   AND {_UNCLAIMED_RQ_SP} >= :max_unclaimed_rq
                RETURNING sp.id, sp.snapshot_id, sp.tenant_id,
                          sp.resource_id, sp.partition_type,
                          sp.partition_index, sp.drive_id, sp.file_ids,
                          sp.payload
                """
            ),
            {
                "unclaimed_stale": RECONCILER_PART_UNCLAIMED_STALE_MIN,
                "max_unclaimed_rq": RECONCILER_PART_UNCLAIMED_MAX_REPUBLISH,
            },
        )
    ).all()
    for r in unclaimed_expired_rows:
        await _dead_letter_partition(session, r, "unclaimed_expired")
    stats.partitions_unclaimed_expired = len(unclaimed_expired_rows)
    stats.partitions_dlq += len(unclaimed_expired_rows)
    if unclaimed_expired_rows:
        print(
            f"[RECONCILER] {len(unclaimed_expired_rows)} partition(s) were "
            f"never claimed after {RECONCILER_PART_UNCLAIMED_MAX_REPUBLISH} "
            f"republish attempts — marked FAILED + dead-lettered "
            f"(reason=unclaimed_expired). Either no worker is draining "
            f"their queue, or their partition_type has no route in "
            f"_PARTITION_QUEUE_BY_TYPE."
        )

    # ------------------------------------------------------------------
    # STEP B — orphan snapshots
    # ------------------------------------------------------------------
    # A snapshot is orphan when:
    #   - status = IN_PROGRESS
    #   - EITHER no live worker owns the lease (lease expired / legacy
    #     age exceeded / no live heartbeat), OR all of its partitions
    #     have already reached terminal — in which case no worker
    #     handler is going to advance it (in particular, partitions
    #     marked FAILED by STEP A's poison-pill branch never get a
    #     finalize call). The fast-path on all-terminal-partitions
    #     prevents 5k-user runs from waiting out the full lease TTL
    #     after the reconciler itself poisons the last partition.
    #   - no in-flight partitions remain
    snap_rows = (
        await session.execute(
            text(
                """
                -- snapshot_partitions.status is plain VARCHAR (not an
                -- enum) — the publisher writes 'QUEUED', workers flip
                -- to 'IN_PROGRESS', then 'COMPLETED'/'FAILED'. No
                -- 'CANCELLED' value is produced today, so we don't
                -- match on it. (See shared/models.py SnapshotPartition.)
                -- snapshotstatus enum: IN_PROGRESS, COMPLETED, FAILED,
                -- PARTIAL, PENDING_DELETION. The CASE only produces
                -- values that exist on snapshotstatus.
                UPDATE snapshots s
                   SET status = CASE
                           -- Non-partitioned orphan: the owning worker
                           -- died mid-run (dead lease is a WHERE
                           -- precondition on this path). Nothing can
                           -- vouch for its contents — deriving
                           -- COMPLETED here manufactured false
                           -- successes: a crashed multi-hour archive
                           -- walk became a COMPLETED 0-item snapshot
                           -- that read as "successful empty backup",
                           -- fed retention deletion, and (before the
                           -- baseline fix) poisoned the next run's
                           -- changeKey diff. FAILED is the honest
                           -- state; the retry paths own recovery.
                           WHEN NOT EXISTS (
                               SELECT 1 FROM snapshot_partitions sp
                                WHERE sp.snapshot_id = s.id
                           ) THEN 'FAILED'::snapshotstatus
                           -- Lossy-but-finished shards: a shard that
                           -- completed while LOSING files records the
                           -- count in failure_state (partition status
                           -- deliberately stays a two-value terminal
                           -- domain — eight consumers across both deploy
                           -- lineages read any other value as "in
                           -- flight"). Checked BEFORE the all-COMPLETED
                           -- branch: otherwise an orphaned snapshot whose
                           -- shards all finished would be derived
                           -- COMPLETED and pose as a complete restore
                           -- point that retention may promote over a
                           -- genuinely-complete predecessor.
                           WHEN (
                               SELECT bool_or((jsonb_typeof(sp.failure_state::jsonb -> 'files_failed') = 'number'
                               AND (sp.failure_state::jsonb ->> 'files_failed')::bigint > 0))
                                 FROM snapshot_partitions sp
                                WHERE sp.snapshot_id = s.id
                           ) THEN 'PARTIAL'::snapshotstatus
                           -- Handler-declared degradation, same rule as the
                           -- integrity sweep in STEP D. A worker that died
                           -- after its honesty gate wrote degraded_reason
                           -- but before it could settle the row must not be
                           -- finalized COMPLETED here just because every
                           -- shard reached COMPLETED — the loss happened
                           -- INSIDE the shards. Checked BEFORE the
                           -- all-COMPLETED branch for the same reason
                           -- any_lossy is.
                           WHEN coalesce(s.extra_data ->> 'degraded_reason', '') <> ''
                                THEN 'PARTIAL'::snapshotstatus
                           WHEN (
                               SELECT bool_and(sp.status = 'COMPLETED')
                                 FROM snapshot_partitions sp
                                WHERE sp.snapshot_id = s.id
                           ) THEN 'COMPLETED'::snapshotstatus
                           WHEN (
                               SELECT bool_and(sp.status = 'FAILED')
                                 FROM snapshot_partitions sp
                                WHERE sp.snapshot_id = s.id
                           ) THEN 'FAILED'::snapshotstatus
                           WHEN (
                               SELECT bool_or(sp.status = 'FAILED')
                                 FROM snapshot_partitions sp
                                WHERE sp.snapshot_id = s.id
                           ) THEN 'PARTIAL'::snapshotstatus
                           ELSE 'FAILED'::snapshotstatus
                       END,
                       completed_at     = NOW(),
                       -- Say WHY. This settle used to land a bare {} , so a
                       -- FAILED snapshot was indistinguishable from a
                       -- handler crash or a Graph failure without grepping
                       -- every worker replica (2026-08-05: Corp_Esplanade's
                       -- ROOM_MAILBOX had never once succeeded and every
                       -- post-mortem was log archaeology). On a backup
                       -- product the reason IS the product.
                       --
                       -- The reconciler's object is on the LEFT of || so
                       -- existing keys win: `a || b` lets b overwrite a, and
                       -- a handler's more specific failure_reason (or a
                       -- shard's enumeration_failure / suppressed_reason)
                       -- must survive a 60s re-run.
                       --
                       -- degraded_reason is deliberately NOT written:
                       -- terminal_status_for_extra_data turns that key into
                       -- PARTIAL, which would silently change the derived
                       -- status of snapshots this same statement settles
                       -- COMPLETED.
                       extra_data = jsonb_build_object(
                               'failure_reason', CASE
                                   WHEN NOT EXISTS (
                                       SELECT 1 FROM snapshot_partitions sp
                                        WHERE sp.snapshot_id = s.id
                                   ) THEN 'reconciler: orphaned non-partitioned '
                                        || 'snapshot — lease expired with no live '
                                        || 'worker heartbeat and no handler ever '
                                        || 'finalized it'
                                   ELSE 'reconciler: status derived from partition '
                                        || 'outcomes (no shard left in flight)'
                               END,
                               'reconciler_settled', true
                           ) || COALESCE(s.extra_data::jsonb, '{}'::jsonb),
                       -- Was NULL on the live row, which is what proved
                       -- fail_snapshot / the inline finalize never ran. Fill
                       -- it so that signal stays meaningful.
                       duration_secs    = CASE
                               WHEN s.started_at IS NOT NULL
                               THEN EXTRACT(EPOCH FROM (NOW() - s.started_at))::int
                               ELSE s.duration_secs
                           END,
                       -- Restate the counters from the truth as we settle.
                       -- item_count / bytes_total are written MID-walk as a
                       -- per-process running total and are only restated by
                       -- a real COUNT(*) on the SUCCESS path — so a snapshot
                       -- whose owning process died keeps item_count = 0
                       -- while holding thousands of committed
                       -- snapshot_items. That lie is load-bearing twice
                       -- over: retention's _holds_persisted_content treats
                       -- the counters as the only proof of content (it
                       -- deleted real data for this reason), and the
                       -- Recovery version dropdown filters `item_count > 0`,
                       -- so a lying zero ALSO hides the restore point from
                       -- the UI. 25 snapshots on prod 2026-08-11 held
                       -- 270,885 real rows while reading 0.
                       --
                       -- GREATEST keeps the 60s re-run idempotent and can
                       -- never LOWER a counter a handler already wrote.
                       -- Postgres evaluates a SET expression only for rows
                       -- that pass WHERE, so this costs one index range scan
                       -- per settled orphan (idx_snapshot_items_snapshot_type
                       -- leads on snapshot_id) — 0 rows on a quiet tick.
                       item_count       = GREATEST(
                               COALESCE(s.item_count, 0),
                               (SELECT COUNT(*) FROM snapshot_items si
                                 WHERE si.snapshot_id = s.id)
                           ),
                       bytes_total      = GREATEST(
                               COALESCE(s.bytes_total, 0),
                               (SELECT COALESCE(SUM(si.content_size), 0)
                                  FROM snapshot_items si
                                 WHERE si.snapshot_id = s.id)
                           ),
                       lease_owner_id   = NULL,
                       lease_expires_at = NULL,
                       lease_token      = s.lease_token + 1
                 WHERE s.status = 'IN_PROGRESS'::snapshotstatus
                   AND NOT EXISTS (
                       SELECT 1 FROM snapshot_partitions sp
                        WHERE sp.snapshot_id = s.id
                          AND sp.status IN ('QUEUED','IN_PROGRESS')
                   )
                   AND (
                       -- Fast path: snapshot is partitioned AND all
                       -- partitions terminal. Skip lease check — no
                       -- worker handler will call finalize on a
                       -- partition that the reconciler itself moved
                       -- to FAILED via poison-pill (STEP A above).
                       EXISTS (
                           SELECT 1 FROM snapshot_partitions sp
                            WHERE sp.snapshot_id = s.id
                       )
                       OR (
                           -- Slow path (non-partitioned snapshot):
                           -- require lease/age based staleness so we
                           -- don't race a live worker.
                           --
                           -- 2026-08-25 hardening: a lease that is
                           -- merely PAST its TTL proves only that a
                           -- couple of renew writes were severed —
                           -- during a pooler outage that is true of
                           -- perfectly live walks (1386f54c was
                           -- flipped terminal mid-run this way). So
                           -- the lease must have been dead for a
                           -- grace window on top of the TTL, and a
                           -- snapshot that committed items recently
                           -- is alive no matter what the lease says.
                           -- Cost on a true worker death: settle
                           -- lands ~15 min later — this sweep is a
                           -- safety net, not a hot path.
                           (
                               (s.lease_expires_at IS NOT NULL
                                AND s.lease_expires_at <
                                    NOW() - (:reap_grace_s * INTERVAL '1 second'))
                               OR (s.lease_owner_id IS NULL
                                   AND s.started_at <
                                       NOW() - (:legacy_s * INTERVAL '1 second'))
                           )
                           AND NOT EXISTS (
                               SELECT 1 FROM worker_heartbeats wh
                                WHERE wh.worker_id = s.lease_owner_id
                                  AND wh.last_seen_at >
                                      NOW() - (:stale_s * INTERVAL '1 second')
                           )
                           AND NOT EXISTS (
                               -- proof-of-life: rows committed within
                               -- the progress grace window. Uses the
                               -- snapshot_id-leading index; only rows
                               -- already passing every predicate
                               -- above pay this scan, ~0 per tick.
                               SELECT 1 FROM snapshot_items si
                                WHERE si.snapshot_id = s.id
                                  AND si.created_at >
                                      NOW() - (:progress_grace_s * INTERVAL '1 second')
                           )
                       )
                   )
                RETURNING s.id, s.status::text
                """
            ),
            {
                "stale_s": HEARTBEAT_STALE_S,
                "legacy_s": SNAP_LEGACY_AGE_S,
                "reap_grace_s": SNAP_REAP_LEASE_GRACE_S,
                "progress_grace_s": SNAP_REAP_PROGRESS_GRACE_S,
            },
        )
    ).all()
    stats.snapshots_finalized = len(snap_rows)

    # ------------------------------------------------------------------
    # STEP C — orphan jobs
    # ------------------------------------------------------------------
    # Resolve status from leaf snapshots. Use bool_or for PARTIAL/FAILED
    # detection so a single bad child taints the parent appropriately.
    job_rows = (
        await session.execute(
            text(
                """
                -- jobstatus enum values: QUEUED, PENDING, RUNNING,
                -- COMPLETED, FAILED, CANCELLED, CANCELLING, RETRYING.
                -- *No* PARTIAL — that lives on snapshotstatus only.
                -- Mixed-outcome roll-up is the batch row's job (see
                -- shared.batch_rollup.derive_batch_status), so at the
                -- job level we collapse to:
                --   any snapshot COMPLETED or PARTIAL     → COMPLETED
                --   zero COMPLETED/PARTIAL (all FAILED)   → FAILED
                -- PARTIAL is an accepted, restorable outcome (the worker
                -- honesty gates settle degraded-but-usable runs PARTIAL),
                -- so it counts as job-level success — matching the worker's
                -- _finalize_bulk_parent_if_complete and the stale-sweep
                -- backstop (both: FAILED only when EVERY child failed).
                -- 2026-08-19: the old zero-COMPLETED→FAILED mapping raced
                -- (and beat) the worker finalizer on every accepted-PARTIAL
                -- archive run, locking the job FAILED with no error_message.
                -- snapshotstatus enum: IN_PROGRESS, COMPLETED, FAILED,
                -- PARTIAL, PENDING_DELETION. No QUEUED, no CANCELLED.
                -- Lease check intentionally dropped: the outer WHERE
                -- already requires every child snapshot to be terminal,
                -- so there is no live work for the lease holder to do
                -- on this job. The worker that completed the last
                -- snapshot normally calls _finalize_bulk_parent_if_complete;
                -- this branch is the safety net for when STEP B above
                -- flipped a partitioned snapshot terminal on the
                -- reconciler's behalf and no worker was left to bubble
                -- up to the job. (Without this, a poisoned-partition
                -- batch parent would wait out the full lease TTL at
                -- 5k-user scale.)
                UPDATE jobs j
                   SET status = CASE
                           WHEN EXISTS (
                               SELECT 1 FROM snapshots s
                                WHERE s.job_id = j.id
                                  AND s.status IN (
                                      'COMPLETED'::snapshotstatus,
                                      'PARTIAL'::snapshotstatus
                                  )
                           ) THEN 'COMPLETED'::jobstatus
                           ELSE 'FAILED'::jobstatus
                       END,
                       completed_at     = NOW(),
                       lease_owner_id   = NULL,
                       lease_expires_at = NULL,
                       lease_token      = j.lease_token + 1
                 WHERE j.status = 'RUNNING'::jobstatus
                   AND NOT EXISTS (
                       SELECT 1 FROM snapshots s
                        WHERE s.job_id = j.id
                          AND s.status = 'IN_PROGRESS'::snapshotstatus
                   )
                   AND EXISTS (
                       SELECT 1 FROM snapshots s WHERE s.job_id = j.id
                   )
                RETURNING j.id, j.status::text
                """
            ),
            {
                "stale_s": HEARTBEAT_STALE_S,
                "legacy_s": JOB_LEGACY_AGE_S,
            },
        )
    ).all()
    stats.jobs_finalized = len(job_rows)

    # ------------------------------------------------------------------
    # STEP D — terminal-snapshot integrity sweep
    # ------------------------------------------------------------------
    # Catches snapshots that were flipped COMPLETED via a path that
    # observed all-COMPLETED partitions at the moment of the CTE — but
    # whose partitions later regressed (e.g. via a race between the
    # finalize CTE read and a concurrent _mark_partition_terminal
    # commit; or via STEP A's poison-pill which marks late partitions
    # FAILED after the parent snapshot has already terminalised).
    # Observed in prod 2026-05-16: Vinay/Ranjeet CHATS had snap.status =
    # COMPLETED with all 4 partitions FAILED, item_count populated. The
    # CTE in _finalize_partitioned_snapshot guards against forward race
    # (IN_PROGRESS only) but cannot retroactively correct a terminal
    # row. This step closes that gap.
    #
    # Idempotent: only writes when the derived status differs from the
    # stored status, and only for snapshots that have partition rows.
    # Single GROUP BY pass — cheaper than 4 correlated subqueries per
    # candidate at 5k-user prod scale (~30k snapshots/batch). Filters
    # out partitions that are still pending and snapshots already in
    # the right state inside the same statement.
    snap_integrity_rows = (
        await session.execute(
            text(
                """
                WITH part_agg AS (
                    SELECT sp.snapshot_id,
                           COUNT(*)                                      AS n,
                           bool_or(sp.status IN ('QUEUED','IN_PROGRESS')) AS any_pending,
                           bool_and(sp.status = 'COMPLETED')             AS all_completed,
                           bool_and(sp.status = 'FAILED')                AS all_failed,
                           bool_or(sp.status = 'FAILED')                 AS any_failed,
                           -- Shards that finished but LOST files report
                           -- COMPLETED and record the count in
                           -- failure_state (partition status is a
                           -- two-value terminal domain). Without this
                           -- term the integrity sweep below re-derived
                           -- COMPLETED for a lossy set every 60s and
                           -- overwrote the PARTIAL that the finalizer and
                           -- STEP B had correctly derived — turning a
                           -- known-incomplete snapshot back into a
                           -- "complete" restore point that retention may
                           -- promote over a genuinely-complete one.
                           bool_or((jsonb_typeof(sp.failure_state::jsonb -> 'files_failed') = 'number'
                               AND (sp.failure_state::jsonb ->> 'files_failed')::bigint > 0))                                        AS any_lossy
                      FROM snapshot_partitions sp
                     GROUP BY sp.snapshot_id
                )
                UPDATE snapshots s
                   SET status = CASE
                           WHEN pa.all_failed    THEN 'FAILED'::snapshotstatus
                           -- Handler-declared degradation. `degraded_reason`
                           -- is written by the honesty gates in the worker
                           -- (evaluate_drain_honesty for chats, the folder
                           -- gate for mail) and lives on the SNAPSHOT, not
                           -- on any partition row — a chats snapshot whose
                           -- 6 shards each finished cleanly can still have
                           -- lost 93% of its chats inside those shards.
                           -- Without this term the sweep re-derived
                           -- COMPLETED from the partitions alone and erased
                           -- the gate's verdict ~40s later, every 60s,
                           -- forever: 31 snapshots fleet-wide read
                           -- COMPLETED while carrying degraded_reason
                           -- (e.g. "769 of 806 chat drain(s) failed"),
                           -- against only 3 that kept an honest PARTIAL.
                           -- Same failure class as any_lossy above.
                           --
                           -- This CASE is duplicated verbatim in the
                           -- `s.status != (...)` guard below. The two MUST
                           -- stay byte-identical: if the SET derives PARTIAL
                           -- but the guard still derives COMPLETED, the row
                           -- matches forever and every 60s tick rewrites it
                           -- and bumps lease_token.
                           WHEN pa.any_failed OR pa.any_lossy
                                OR coalesce(s.extra_data ->> 'degraded_reason', '') <> ''
                                                 THEN 'PARTIAL'::snapshotstatus
                           WHEN pa.all_completed THEN 'COMPLETED'::snapshotstatus
                           ELSE s.status
                       END,
                       lease_owner_id   = NULL,
                       lease_expires_at = NULL,
                       lease_token      = s.lease_token + 1,
                       -- Restate the counters while we are correcting the
                       -- status. STEP B covers rows this sweep settles, but
                       -- the 25 lying rows measured on prod 2026-08-11
                       -- (270,885 real snapshot_items at item_count = 0) are
                       -- already TERMINAL, and this integrity sweep is the
                       -- only pass that revisits them. A zero item_count is
                       -- not cosmetic: retention treats the counters as the
                       -- only proof of content, and the Recovery version
                       -- dropdown filters `item_count > 0`, so the lie both
                       -- risks deletion and hides the restore point.
                       -- GREATEST makes the 60s re-run idempotent and can
                       -- never lower a handler-written counter, so this
                       -- cannot join the status CASE in causing a rewrite
                       -- loop.
                       --
                       -- These assignments MUST live here in the SET list:
                       -- commit 3ddd0fc appended them after the WHERE
                       -- clause's guard CASE instead, which is a syntax
                       -- error — and because sweep_orphans runs STEP D on
                       -- the same session/tick as STEPS A-C, that one PARSE
                       -- failure aborted and rolled back EVERY sweep tick
                       -- (shard requeue, orphan snapshot/job finalization)
                       -- fleet-wide from the moment it deployed until
                       -- 2026-08-18.
                       item_count = GREATEST(
                               COALESCE(s.item_count, 0),
                               (SELECT COUNT(*) FROM snapshot_items si
                                 WHERE si.snapshot_id = s.id)
                           ),
                       bytes_total = GREATEST(
                               COALESCE(s.bytes_total, 0),
                               (SELECT COALESCE(SUM(si.content_size), 0)
                                  FROM snapshot_items si
                                 WHERE si.snapshot_id = s.id)
                           )
                  FROM part_agg pa
                 WHERE s.id = pa.snapshot_id
                   AND pa.n > 0
                   AND NOT pa.any_pending
                   AND s.status IN ('COMPLETED'::snapshotstatus,
                                    'PARTIAL'::snapshotstatus,
                                    'FAILED'::snapshotstatus)
                   AND s.status != (CASE
                           WHEN pa.all_failed    THEN 'FAILED'::snapshotstatus
                           -- Handler-declared degradation. `degraded_reason`
                           -- is written by the honesty gates in the worker
                           -- (evaluate_drain_honesty for chats, the folder
                           -- gate for mail) and lives on the SNAPSHOT, not
                           -- on any partition row — a chats snapshot whose
                           -- 6 shards each finished cleanly can still have
                           -- lost 93% of its chats inside those shards.
                           -- Without this term the sweep re-derived
                           -- COMPLETED from the partitions alone and erased
                           -- the gate's verdict ~40s later, every 60s,
                           -- forever: 31 snapshots fleet-wide read
                           -- COMPLETED while carrying degraded_reason
                           -- (e.g. "769 of 806 chat drain(s) failed"),
                           -- against only 3 that kept an honest PARTIAL.
                           -- Same failure class as any_lossy above.
                           --
                           -- This CASE is duplicated verbatim in the
                           -- `s.status != (...)` guard below. The two MUST
                           -- stay byte-identical: if the SET derives PARTIAL
                           -- but the guard still derives COMPLETED, the row
                           -- matches forever and every 60s tick rewrites it
                           -- and bumps lease_token.
                           WHEN pa.any_failed OR pa.any_lossy
                                OR coalesce(s.extra_data ->> 'degraded_reason', '') <> ''
                                                 THEN 'PARTIAL'::snapshotstatus
                           WHEN pa.all_completed THEN 'COMPLETED'::snapshotstatus
                           ELSE s.status
                       END)
                RETURNING s.id, s.status::text AS new_status
                """
            ),
        )
    ).all()
    snaps_corrected = len(snap_integrity_rows)
    for r in snap_integrity_rows:
        print(
            f"[RECONCILER][INTEGRITY] snapshot={r.id} status "
            f"corrected → {r.new_status}"
        )

    if DRY_RUN:
        await session.rollback()
        print(
            f"[RECONCILER][DRY] parts_requeue={stats.partitions_requeued} "
            f"parts_dlq={stats.partitions_dlq} "
            f"snaps_final={stats.snapshots_finalized} "
            f"snaps_corrected={snaps_corrected} "
            f"jobs_final={stats.jobs_finalized}"
        )
        return stats

    await session.commit()
    if stats.total() or snaps_corrected:
        print(
            f"[RECONCILER] parts_requeue={stats.partitions_requeued} "
            f"(unclaimed={stats.partitions_unclaimed_requeued}) "
            f"parts_dlq={stats.partitions_dlq} "
            f"(unclaimed_expired={stats.partitions_unclaimed_expired}) "
            f"snaps_final={stats.snapshots_finalized} "
            f"snaps_corrected={snaps_corrected} "
            f"jobs_final={stats.jobs_finalized}"
        )
    return stats


# Mirror of the publish-side routing in
# shared/message_bus.py / shared/config.py. Centralised here so the
# reconciler doesn't drift out of sync — if a new partition type ever
# lands, add its queue here AND extend the test in
# tests/shared/test_reconciler.py.
_PARTITION_QUEUE_BY_TYPE = {
    "ONEDRIVE_FILES":     "backup.onedrive_partition",
    "CHATS":              "backup.chats_partition",
    "MAIL_FOLDERS":       "backup.mail_partition",
    "SHAREPOINT_DRIVES":  "backup.sharepoint_partition",
    # Added 2026-07-31: both types persist real snapshot_partitions rows
    # (workers/backup-worker/main.py _fanout_groups_partitions /
    # _fanout_entra_partitions) and have their own declared queues, but
    # were missing here — so a never-claimed shard of either type could
    # not be re-published, and (since STEP A stopped poisoning
    # never-claimed rows) could not be failed either. Safe to route: the
    # consumers read only ``partitionId`` off the envelope and take
    # everything else from the claimed row.
    "GROUPS_CHANNELS":    "backup.groups_partition",
    "ENTRA_CATEGORIES":   "backup.entra_partition",
}


# messageType the backup-worker's `process_backup_message` dispatcher
# expects for each partition type. Without this key the worker can't
# route to the matching ``_process_*_partition_message`` handler and
# the message falls through to the generic backup-message path which
# requires ``jobId`` — none of our partition payloads carry one, so
# the message KeyError-crashes and (in classic queues) loops because
# requeue doesn't bump x-delivery-count (2026-05-16 incident:
# reconciler-requeued CHATS partitions poisoned worker-9f90b1b2).
_MESSAGE_TYPE_BY_PARTITION_TYPE = {
    "ONEDRIVE_FILES":     "BACKUP_ONEDRIVE_PARTITION",
    "CHATS":              "BACKUP_CHATS_PARTITION",
    "MAIL_FOLDERS":       "BACKUP_MAIL_PARTITION",
    "SHAREPOINT_DRIVES":  "BACKUP_SHAREPOINT_PARTITION",
    # See _PARTITION_QUEUE_BY_TYPE. Dispatcher branches:
    # workers/backup-worker/main.py process_backup_message →
    # _process_groups_partition_message / _process_entra_partition_message.
    "GROUPS_CHANNELS":    "BACKUP_GROUPS_PARTITION",
    "ENTRA_CATEGORIES":   "BACKUP_ENTRA_PARTITION",
}


def _queue_for_partition_type(ptype: Optional[str]) -> Optional[str]:
    if not ptype:
        return None
    return _PARTITION_QUEUE_BY_TYPE.get(ptype)


def _message_type_for_partition_type(ptype: Optional[str]) -> Optional[str]:
    if not ptype:
        return None
    return _MESSAGE_TYPE_BY_PARTITION_TYPE.get(ptype)


async def republish_partition_messages(
    stats: SweepStats,
    *,
    message_bus,
) -> int:
    """Publish AMQP messages for partitions the sweeper re-queued.

    Caller passes the global ``message_bus`` so this module stays
    free of AMQP imports — keeps the unit tests pure.
    """
    published = 0
    for p in stats.requeue_payloads:
        queue = _queue_for_partition_type(p.get("partition_type"))
        msg_type = _message_type_for_partition_type(p.get("partition_type"))
        if queue is None or msg_type is None:
            print(
                f"[RECONCILER] no queue/messageType mapping for "
                f"partition_type={p.get('partition_type')!r}; "
                f"skipping {p['id']} — this shard CANNOT be recovered; "
                f"STEP A will mark it FAILED + dead-letter it "
                f"(reason=unclaimed_expired) once its republish budget "
                f"runs out. Add the type to _PARTITION_QUEUE_BY_TYPE."
            )
            continue
        try:
            await message_bus.publish(
                queue,
                {
                    # messageType lets the backup-worker's
                    # process_backup_message dispatcher route to the
                    # right ``_process_*_partition_message`` handler.
                    # Without it the worker falls through to the
                    # generic path that requires jobId (which a bare
                    # partition envelope lacks) and crash-loops on
                    # the same message in classic queues.
                    "messageType":    msg_type,
                    "snapshotId":     p["snapshot_id"],
                    "partitionId":    p["id"],
                    "partitionType":  p["partition_type"],
                    "partitionIndex": p["partition_index"],
                    "driveId":        p.get("drive_id"),
                    "fileIds":        p.get("file_ids"),
                    "payload":        p.get("payload"),
                    "_reconciler_requeue": True,
                },
            )
            published += 1
        except Exception as exc:
            print(f"[RECONCILER] republish failed for partition {p['id']}: {exc}")
    return published
