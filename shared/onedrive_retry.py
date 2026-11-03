"""Bounded ownership for the legacy OneDrive retry queue (no source I/O)."""
from sqlalchemy import select, text

from shared.models import Job, OneDriveFileRetry, Snapshot


class RetryOwnershipLost(RuntimeError):
    pass


def _eligible(alias="r"):
    # Correlated EXISTS use snapshot/job identities and the existing child
    # indexes. A terminal snapshot alone does not prove its writers stopped.
    return f"""EXISTS (
        SELECT 1 FROM snapshots s JOIN jobs j ON j.id = s.job_id
        WHERE s.id = {alias}.snapshot_id AND s.resource_id = {alias}.resource_id
          AND j.tenant_id = {alias}.tenant_id
          AND s.status IN ('COMPLETED', 'PARTIAL', 'FAILED')
          AND j.status NOT IN ('CANCELLED', 'CANCELLING')
          AND s.extra_data->>'cancelled_at' IS NULL
          AND NOT EXISTS (SELECT 1 FROM snapshot_partitions p
              WHERE p.snapshot_id = s.id AND p.status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED'))
          AND NOT EXISTS (SELECT 1 FROM provider_work w
              WHERE w.snapshot_id = s.id AND w.state IN ('DRAINING', 'WAITING', 'RESUMING'))
    )"""


async def claim_retries(session, *, limit, max_attempts, lease_seconds):
    # An attempt is consumed once, on claim. Reaping it must not add another
    # attempt, reset the budget, or restore an exhausted row to PENDING.
    reaped = (await session.execute(text(f"""
        UPDATE onedrive_file_retries r
        SET status = CASE WHEN attempt_count >= :maximum OR NOT {_eligible()}
                          THEN 'FAILED_PERMANENT' ELSE 'PENDING' END,
            updated_at = clock_timestamp(), next_retry_at = clock_timestamp(),
            last_error = 'retry owner expired or attempt budget exhausted'
        WHERE (status = 'IN_PROGRESS'
               AND updated_at <= clock_timestamp() - (:lease * INTERVAL '1 second'))
           OR (status = 'PENDING' AND attempt_count >= :maximum)
        RETURNING id
    """), {"maximum": max_attempts, "lease": lease_seconds})).all()
    claimed = (await session.execute(text(f"""
        WITH ready AS (
            SELECT r.id FROM onedrive_file_retries r
            WHERE r.status = 'PENDING' AND r.attempt_count < :maximum
              AND r.next_retry_at <= clock_timestamp() AND {_eligible()}
            ORDER BY r.next_retry_at, r.id LIMIT :limit
            FOR UPDATE OF r SKIP LOCKED
        )
        UPDATE onedrive_file_retries r
        SET status = 'IN_PROGRESS', attempt_count = attempt_count + 1,
            updated_at = clock_timestamp()
        FROM ready WHERE r.id = ready.id
        RETURNING r.id, r.attempt_count
    """), {"maximum": max_attempts, "limit": limit})).all()
    return claimed, len(reaped)


async def owned_retry(session, *, retry_id, attempt, lease_seconds):
    # Parent cancellation/deletion and retry replacement cannot cross a
    # publication transaction. Locks are short and never cover source I/O.
    parents = (await session.execute(select(Snapshot, Job)
        .join(Job, Job.id == Snapshot.job_id)
        .join(OneDriveFileRetry, OneDriveFileRetry.snapshot_id == Snapshot.id)
        .where(OneDriveFileRetry.id == retry_id)
        .with_for_update(read=True, key_share=True, of=(Job, Snapshot)))).first()
    if parents is None:
        raise RetryOwnershipLost("OneDrive retry parent disappeared")
    found = await session.scalar(text(f"""
        SELECT r.id FROM onedrive_file_retries r
        WHERE r.id = :id AND r.attempt_count = :attempt AND r.status = 'IN_PROGRESS'
          AND r.updated_at > clock_timestamp() - (:lease * INTERVAL '1 second')
          AND {_eligible()}
        FOR UPDATE OF r
    """), {"id": retry_id, "attempt": attempt, "lease": lease_seconds})
    if found is None:
        raise RetryOwnershipLost("OneDrive retry ownership expired or moved")
    row = await session.get(OneDriveFileRetry, retry_id, populate_existing=True)
    return row, parents[0], parents[1]


async def renew_retry(session, *, retry_id, attempt, lease_seconds):
    state = await session.scalar(text(f"""
        UPDATE onedrive_file_retries r SET updated_at = clock_timestamp()
        WHERE r.id = :id AND r.attempt_count = :attempt AND r.status = 'IN_PROGRESS'
          AND r.updated_at > clock_timestamp() - (:lease * INTERVAL '1 second')
          AND {_eligible()}
        RETURNING status
    """), {"id": retry_id, "attempt": attempt, "lease": lease_seconds})
    if state is None:
        # Receipt and RESCUED are committed together. A heartbeat racing that
        # successful commit must not cancel the completed owner on its way out.
        state = await session.scalar(select(OneDriveFileRetry.status).where(
            OneDriveFileRetry.id == retry_id, OneDriveFileRetry.attempt_count == attempt,
            OneDriveFileRetry.status == "RESCUED"))
    return state


async def release_retry(session, *, retry_id, attempt, lease_seconds,
                        max_attempts, error, permanent=False, delay_seconds=60):
    # Failure/cancellation never adopts a replacement generation. Expired
    # attempts are left for the reaper, which has the same finite budget.
    return await session.scalar(text(f"""
        UPDATE onedrive_file_retries
        SET status = CASE WHEN :permanent OR attempt_count >= :maximum
                              OR NOT {_eligible("onedrive_file_retries")}
                          THEN 'FAILED_PERMANENT' ELSE 'PENDING' END,
            last_error = :error, updated_at = clock_timestamp(),
            next_retry_at = clock_timestamp() + (:delay * INTERVAL '1 second')
        WHERE id = :id AND attempt_count = :attempt AND status = 'IN_PROGRESS'
          AND updated_at > clock_timestamp() - (:lease * INTERVAL '1 second')
        RETURNING status
    """), {"id": retry_id, "attempt": attempt, "lease": lease_seconds,
           "maximum": max_attempts, "error": error[:1000], "permanent": permanent,
           "delay": delay_seconds})
