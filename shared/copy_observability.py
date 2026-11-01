"""Bounded, content-free progress for durable required database copies."""
from uuid import UUID

from sqlalchemy import text

from shared.copy_workflow import copy_work_guards_enabled


_POLICY_LIMIT = 32
_GAP_CODES = (
    'copy_admission_timeout', 'copy_parent_cancelled', 'copy_parent_terminal',
    'copy_cancelled', 'copy_statement_timeout', 'copy_deadlock', 'copy_execution_failed',
)
_WAIT_CODES = (
    'copy_resource_busy', 'copy_target_busy', 'copy_tenant_busy', 'copy_lane_busy',
    'copy_aggregate_busy', 'copy_protocol_disabled', 'copy_lock_timeout', 'copy_admission_wait',
)
_COUNTS = """
    COUNT(*) FILTER (WHERE state IN ('READY','RUNNING','CAPTURING')) AS pending_tasks,
    COUNT(*) FILTER (WHERE state='READY') AS queued_tasks,
    COUNT(*) FILTER (WHERE state='RUNNING') AS running_tasks,
    COUNT(*) FILTER (WHERE state IN ('FAILED','CANCELLED')) AS failed_tasks,
    COALESCE(SUM(row_count),0) AS committed_rows,
    COALESCE(SUM(bytes_count),0) AS committed_bytes,
    COALESCE(SUM(wait_s),0) AS admission_wait_s,
    COALESCE(SUM(execution_s),0) AS sql_runtime_s,
    COALESCE(MAX(queue_age_s),0) AS oldest_queue_age_s
"""


def _code_counts(codes, column):
    return "jsonb_build_object(" + ",".join(
        f"'{code}',COUNT(*) FILTER (WHERE {column}='{code}')" for code in codes
    ) + ")"


async def copy_work_summaries(session, *, job_ids):
    """One aggregate query per existing job page; no task/manifest payloads."""
    if not copy_work_guards_enabled() or not job_ids:
        return {}
    gap_codes = ','.join(f"'{code}'" for code in _GAP_CODES)
    wait_codes = ','.join(f"'{code}'" for code in _WAIT_CODES)
    rows = (await session.execute(text(f"""
        WITH workflows AS MATERIALIZED (
            SELECT id,job_id,state,capture_attempt,parent_settle_pending
              FROM copy_workflows WHERE job_id=ANY(CAST(:ids AS uuid[]))
        ), workflow_counts AS (
            SELECT job_id,
                bool_or(state IN ('WAITING','RESUMING') OR parent_settle_pending) AS awaiting_resume,
                bool_or(state='DRAINING') AS draining,
                bool_or(state IN ('PARTIAL','FAILED','CANCELLED')) AS failed
              FROM workflows GROUP BY job_id
        ), tasks AS MATERIALIZED (
            SELECT w.job_id,t.sla_policy_id,CASE WHEN w.state='DRAINING' AND t.state='READY'
                    THEN 'CAPTURING' ELSE t.state END AS state,t.row_count,t.bytes_count,t.execution_s,
                COALESCE(t.cumulative_wait_s,0) + CASE WHEN t.state='READY' AND w.state<>'DRAINING'
                    THEN GREATEST(0,EXTRACT(EPOCH FROM
                        clock_timestamp()-COALESCE(t.queue_entered_at,t.created_at))) ELSE 0 END AS wait_s,
                CASE WHEN t.state='READY' AND w.state<>'DRAINING' THEN GREATEST(0,EXTRACT(EPOCH FROM
                    clock_timestamp()-COALESCE(t.queue_entered_at,t.created_at))) ELSE 0 END AS queue_age_s,
                CASE WHEN t.state IN ('FAILED','CANCELLED') THEN
                    CASE WHEN t.error->>'code' IN ({gap_codes}) THEN t.error->>'code'
                         WHEN t.state='CANCELLED' THEN 'copy_cancelled'
                         ELSE 'copy_execution_failed' END END AS gap_code,
                CASE WHEN t.state='READY' AND w.state<>'DRAINING' THEN
                    CASE WHEN t.error->>'code' IN ({wait_codes}) THEN t.error->>'code'
                         ELSE 'copy_admission_wait' END END AS wait_code
              FROM copy_work_tasks t JOIN workflows w ON w.id=t.workflow_id
             WHERE t.capture_attempt=w.capture_attempt
        ), task_counts AS (
            SELECT job_id,{_COUNTS},
                {_code_counts(_GAP_CODES, 'gap_code')} AS gap_counts,
                {_code_counts(_WAIT_CODES, 'wait_code')} AS wait_reasons
              FROM tasks GROUP BY job_id
        ), policy_counts AS (
            SELECT job_id,sla_policy_id,{_COUNTS}
              FROM tasks GROUP BY job_id,sla_policy_id
        ), policy_ranked AS (
            SELECT *,row_number() OVER (PARTITION BY job_id ORDER BY
                pending_tasks DESC,oldest_queue_age_s DESC,sla_policy_id NULLS FIRST) AS rank
              FROM policy_counts
        )
        SELECT w.*,t.pending_tasks,t.queued_tasks,t.running_tasks,t.failed_tasks,
            t.committed_rows,t.committed_bytes,t.admission_wait_s,t.sql_runtime_s,t.oldest_queue_age_s,
            t.gap_counts,t.wait_reasons,
            (SELECT COUNT(*) FROM policy_counts p WHERE p.job_id=w.job_id) AS policy_count,
            (SELECT jsonb_agg(to_jsonb(p)-'job_id'-'rank' ORDER BY rank)
               FROM policy_ranked p WHERE p.job_id=w.job_id AND rank<=:policy_limit) AS policies,
            (SELECT jsonb_build_object(
                'policy_count',COUNT(*),'pending_tasks',COALESCE(SUM(pending_tasks),0),
                'committed_rows',COALESCE(SUM(committed_rows),0),
                'committed_bytes',COALESCE(SUM(committed_bytes),0),
                'admission_wait_s',COALESCE(SUM(admission_wait_s),0),
                'oldest_queue_age_s',COALESCE(MAX(oldest_queue_age_s),0))
               FROM policy_ranked p WHERE p.job_id=w.job_id AND rank>:policy_limit) AS other_policies
          FROM workflow_counts w LEFT JOIN task_counts t ON t.job_id=w.job_id
    """), {'ids': [str(UUID(str(job_id))) for job_id in job_ids],
            'policy_limit': _POLICY_LIMIT})).mappings().all()
    summaries = {}
    for row in rows:
        counts = {key: int(row[key] or 0) for key in (
            'pending_tasks', 'queued_tasks', 'running_tasks', 'failed_tasks',
            'committed_rows', 'committed_bytes', 'policy_count',
        )}
        if counts['running_tasks']:
            state = 'RUNNING'
        elif counts['queued_tasks']:
            state = 'QUEUED'
        elif row['awaiting_resume']:
            state = 'WAITING_RESUME'
        elif row['draining']:
            state = 'CAPTURING'
        elif row['failed'] or counts['failed_tasks']:
            state = 'FAILED'
        else:
            state = 'COMPLETE'
        summaries[str(row['job_id'])] = {
            'state': state, **counts,
            'admission_wait_s': float(row['admission_wait_s'] or 0),
            'sql_runtime_s': float(row['sql_runtime_s'] or 0),
            'oldest_queue_age_s': float(row['oldest_queue_age_s'] or 0),
            'gap_counts': {code: count for code, count in (row['gap_counts'] or {}).items() if count},
            'wait_reasons': {code: count for code, count in (row['wait_reasons'] or {}).items() if count},
            'policies': row['policies'] or [],
            'other_policies': row['other_policies'],
        }
    return summaries


async def copy_work_summary(session, *, job_id):
    """Safe optional addition to an already authorized job response."""
    return (await copy_work_summaries(session, job_ids=[job_id])).get(str(job_id))
