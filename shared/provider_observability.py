"""Content-free progress for an already authorized Activity/job selection."""
from uuid import UUID
from sqlalchemy import text


async def provider_work_summaries(session, *, job_ids):
    if not job_ids:
        return {}
    rows = (await session.execute(text('''
        WITH selected AS MATERIALIZED (
            SELECT id,job_id,state,next_eligible_at,created_at,parent_settle_pending,lease_expires_at
            FROM provider_work WHERE job_id=ANY(CAST(:ids AS uuid[]))
        ), units AS (
            SELECT w.job_id,
                count(*) FILTER (WHERE u.state IN ('PENDING','RUNNING')) AS pending_units,
                count(*) FILTER (WHERE u.state='COMPLETED') AS completed_units,
                count(*) FILTER (WHERE u.state IN ('FAILED','CANCELLED')) AS failed_units,
                sum(u.attempts) AS reserved_provider_attempts,
                sum(u.waited_s) AS retry_wait_s
            FROM selected w JOIN provider_work_units u ON u.work_id=w.id GROUP BY w.job_id
        ), leaves AS (
            SELECT job_id,
                CASE WHEN bool_or(state IN ('RESUMING','DRAINING') AND lease_expires_at > statement_timestamp()) THEN 'RUNNING'
                     WHEN bool_or(state IN ('WAITING','RESUMING','DRAINING')) THEN 'WAITING'
                     WHEN bool_or(parent_settle_pending) THEN 'SETTLING'
                     WHEN bool_or(state IN ('FAILED','PARTIAL','CANCELLED')) THEN 'INCOMPLETE'
                     ELSE 'COMPLETE' END AS state,
                count(*) FILTER (WHERE state IN ('DRAINING','WAITING','RESUMING')) AS pending_leaves,
                count(*) FILTER (WHERE state='WAITING' AND next_eligible_at > statement_timestamp()) AS cooldown_leaves,
                count(*) FILTER (WHERE state='WAITING' AND next_eligible_at <= statement_timestamp()) AS ready_leaves,
                count(*) FILTER (WHERE state IN ('DRAINING','RESUMING') AND lease_expires_at > statement_timestamp()) AS running_leaves,
                count(*) FILTER (WHERE state IN ('DRAINING','RESUMING') AND
                    (lease_expires_at IS NULL OR lease_expires_at <= statement_timestamp())) AS expired_lease_leaves,
                min(next_eligible_at) FILTER (WHERE state='WAITING') AS next_eligible_at,
                max(EXTRACT(EPOCH FROM clock_timestamp()-created_at)) FILTER
                    (WHERE state IN ('DRAINING','WAITING','RESUMING')) AS oldest_queue_age_s
            FROM selected GROUP BY job_id
        )
        SELECT l.*,u.pending_units,u.completed_units,u.failed_units,u.reserved_provider_attempts,u.retry_wait_s
        FROM leaves l LEFT JOIN units u USING(job_id)
    '''), {'ids': [str(UUID(str(value))) for value in job_ids]})).mappings()
    # ready means due, not proof of a capacity bottleneck; age includes active time.
    # retry_wait_s is scheduled delay, not measured elapsed waiting.
    result = {}
    for row in rows:
        result[str(row['job_id'])] = {key: int(row[key] or 0) for key in (
            'cooldown_leaves', 'ready_leaves', 'running_leaves', 'expired_lease_leaves',
            'pending_leaves', 'pending_units', 'completed_units', 'failed_units', 'reserved_provider_attempts')}
        result[str(row['job_id'])].update(state=row['state'],
            next_eligible_at=row['next_eligible_at'].isoformat() if row['next_eligible_at'] else None,
            retry_wait_s=float(row['retry_wait_s'] or 0), oldest_queue_age_s=float(row['oldest_queue_age_s'] or 0))
    return result
