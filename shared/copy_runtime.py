"""Bounded copy executors and recovery hosted by an existing backup worker.

PostgreSQL is authoritative. wake() is an optional hint; polling and recovery
also run with producers disabled while the database protocol remains enabled.
The resume callback must perform only the persisted database continuation.
"""
import asyncio
import logging
import math
from uuid import uuid4

from sqlalchemy import and_, case, exists, func, or_, select, text, update
from sqlalchemy.exc import SQLAlchemyError

from shared import copy_admission, copy_work, copy_workflow
from shared.db_retry import _chain
from shared.lease import LEASE_TTL_S
from shared.models import CopyWorkControl, CopyWorkflow, CopyWorkTask, Job, Snapshot, SnapshotPartition
from shared.reconciler import HEARTBEAT_STALE_S

logger = logging.getLogger(__name__)


class CopyWorkRunner:
    def __init__(self, factory, resume_callback, *, concurrency=2, batch_size=512,
                 statement_timeout_ms=60000, lock_timeout_ms=5000, poll_s=1, recovery_s=10):
        if type(concurrency) is not int or not 1 <= concurrency <= 32:
            raise ValueError('copy concurrency must be between 1 and 32')
        if any(type(value) is not int or value < 1 for value in
               (batch_size, statement_timeout_ms, lock_timeout_ms)):
            raise ValueError('copy batch size and statement/lock timeouts must be positive integers')
        if any(not isinstance(value, (int, float)) or not math.isfinite(value) or not 0 < value <= 60
               for value in (poll_s, recovery_s)):
            raise ValueError('copy polling and recovery intervals must be in (0,60] seconds')
        self.factory, self.resume_callback = factory, resume_callback
        self.concurrency, self.batch_size = concurrency, batch_size
        self.statement_timeout_ms, self.lock_timeout_ms = statement_timeout_ms, lock_timeout_ms
        self.poll_s, self.recovery_s = poll_s, recovery_s
        self.owner_id = uuid4()
        self._slots = asyncio.Semaphore(concurrency)
        self._resume_slots = asyncio.Semaphore(concurrency)
        self._initialization = asyncio.Lock()
        self._recovery = asyncio.Lock()
        self._initialized = False
        self._closing = False
        self._wake = asyncio.Event()
        self._stop = asyncio.Event()
        self._tasks = set()
        self._inflight = set()
        self._resuming = set()
        self._resume_cursor = None
        self._orphan_cursor = None

    async def _initialize(self):
        if not self._initialized:
            async with self._initialization:
                if not self._initialized:
                    async with asyncio.timeout(10):
                        await copy_workflow.initialize_copy_guards(self.factory)
                    self._initialized = True

    @staticmethod
    async def _short_settings(session):
        await session.execute(text("SELECT set_config('statement_timeout','5000ms',true),"
                                   "set_config('lock_timeout','500ms',true)"))

    async def _enabled(self):
        try:
            async with self.factory() as session:
                await self._short_settings(session)
                return bool(await session.scalar(select(CopyWorkControl.enabled)
                    .where(CopyWorkControl.id == 'global')))
        except SQLAlchemyError as exc:
            if not copy_workflow.copy_work_enabled() and any(
                    getattr(error, 'sqlstate', None) == '42P01' for error in _chain(exc)):
                return False
            raise

    def wake(self):
        if not self._closing:
            self._wake.set()

    async def start(self):
        if self._closing:
            raise RuntimeError('copy runner is closed')
        if self._tasks:
            return
        await self._initialize()
        if self._closing:
            raise RuntimeError('copy runner is closed')
        if self._tasks:
            return
        for index in range(self.concurrency):
            self._tasks.add(asyncio.create_task(self._worker(), name=f'copy-executor-{index}'))
        self._tasks.add(asyncio.create_task(self._recover(), name='copy-recovery'))

    async def close(self):
        self._closing = True
        self._stop.set()
        self._wake.set()
        current = asyncio.current_task()
        tasks = (self._tasks | self._inflight) - {current}
        for task in tasks:
            task.cancel()
        # Await transaction cancellation/rollback. Never abandon a SQL coroutine
        # and pretend its server-held physical capacity has been released.
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks.clear()

    async def _release_cancelled_claim(self, claim):
        try:
            async with self.factory.begin() as session:
                await self._short_settings(session)
                await session.execute(update(CopyWorkTask).where(
                    CopyWorkTask.id == claim.task_id, CopyWorkTask.state == 'RUNNING',
                    CopyWorkTask.lease_owner_id == claim.owner_id,
                    CopyWorkTask.generation == claim.generation).values(
                        state='READY', lease_owner_id=None, lease_expires_at=None,
                        ready_at=func.clock_timestamp(), queue_entered_at=func.clock_timestamp(),
                        # Cancellation cannot prove that SQL never executed.
                        # Keep v1 reservations until a committed checkpoint or
                        # explicit nonexecution/capacity settlement clears them.
                        attempts=case((CopyWorkTask.params.op('->>')('_copy_execution_budget_v1') == 'true',
                            CopyWorkTask.attempts), else_=func.greatest(CopyWorkTask.attempts - 1, 0))))
        except Exception as exc:
            logger.warning('copy_shutdown_release_failed task_id=%s error_type=%s', claim.task_id, type(exc).__name__)

    async def _resume(self, workflow_id):
        if self._closing or workflow_id in self._resuming:
            return False
        self._resuming.add(workflow_id)
        try:
            async with self._resume_slots:
                if self._closing:
                    return False
                await self.resume_callback(workflow_id)
                return True
        except Exception as exc:
            logger.warning('copy_resume_failed workflow_id=%s error_type=%s', workflow_id, type(exc).__name__)
            return False
        finally:
            self._resuming.discard(workflow_id)

    async def _ready_workflows(self, *, task_id=None):
        async with self.factory() as session:
            await self._short_settings(session)
            now = await session.scalar(select(func.clock_timestamp()))
            eligible = ((CopyWorkflow.state == 'WAITING') |
                ((CopyWorkflow.state == 'RESUMING') &
                 ((CopyWorkflow.lease_expires_at.is_(None)) | (CopyWorkflow.lease_expires_at <= now))) |
                CopyWorkflow.parent_settle_pending)
            query = select(CopyWorkflow.id, CopyWorkflow.created_at).where(
                eligible,
                ~exists(select(CopyWorkTask.id).where(CopyWorkTask.workflow_id == CopyWorkflow.id,
                    CopyWorkTask.capture_attempt == CopyWorkflow.capture_attempt,
                    CopyWorkTask.state.notin_(copy_work.TERMINAL_TASKS))))
            if task_id is not None:
                query = query.where(CopyWorkflow.id.in_(select(CopyWorkTask.workflow_id)
                    .where(CopyWorkTask.id == task_id)))
            base = query.order_by(CopyWorkflow.created_at, CopyWorkflow.id).limit(max(16, self.concurrency * 4))
            cursor = self._resume_cursor if task_id is None else None
            if cursor is not None:
                query = base.where(or_(CopyWorkflow.created_at > cursor[0],
                    and_(CopyWorkflow.created_at == cursor[0], CopyWorkflow.id > cursor[1])))
            else:
                query = base
            rows = (await session.execute(query)).all()
            if not rows and cursor is not None:
                rows = (await session.execute(base)).all()
                self._resume_cursor = None
            if rows and task_id is None:
                self._resume_cursor = (rows[-1].created_at, rows[-1].id)
            return [row.id for row in rows]

    async def run_once(self):
        if self._closing:
            return None
        operation, claim = asyncio.current_task(), None
        self._inflight.add(operation)
        try:
            await self._initialize()
            async with self._slots:
                if self._closing or not await self._enabled():
                    return None
                async with asyncio.timeout(10):
                    async with self.factory.begin() as session:
                        await self._short_settings(session)
                        if self._closing:
                            return None
                        claim = await copy_work.claim(session, owner_id=self.owner_id)
                if claim is None:
                    return None
                result = await copy_admission.run_batch(self.factory, claim,
                    batch_size=self.batch_size, statement_timeout_ms=self.statement_timeout_ms,
                    lock_timeout_ms=self.lock_timeout_ms)
                logger.info('copy_batch_result task_id=%s state=%s committed_rows=%d committed_bytes=%d execution_s=%.6f',
                    result.task_id,result.status,result.rows,result.bytes,result.query_seconds)
            for workflow_id in await self._ready_workflows(task_id=claim.task_id):
                await self._resume(workflow_id)
            if result.status == 'READY':
                self.wake()
            return result
        except asyncio.CancelledError:
            if claim is not None:
                await self._release_cancelled_claim(claim)
            raise
        except Exception as exc:
            logger.warning('copy_execution_loop_failed task_id=%s error_type=%s',
                           claim.task_id if claim else None, type(exc).__name__)
            return None
        finally:
            self._inflight.discard(operation)

    async def _orphan_candidates(self):
        async with self.factory() as session:
            await self._short_settings(session)
            from shared.provider_work import pending_provider_sql, settling_parent
            with settling_parent():
                provider_guard = pending_provider_sql(snapshot_expr='cw.target_snapshot_id', partition_expr=None)
            query = text(f"""
                SELECT cw.id,cw.created_at FROM copy_workflows cw
                JOIN jobs j ON j.id=cw.job_id JOIN snapshots s ON s.id=cw.target_snapshot_id
                LEFT JOIN snapshot_partitions p ON p.id=cw.partition_id
                WHERE {provider_guard} AND cw.state IN ('DRAINING','WAITING','RESUMING') AND (
                    j.status::text NOT IN ('RUNNING','QUEUED','PENDING','RETRYING')
                    OR s.status::text <> 'IN_PROGRESS'
                    OR (p.id IS NOT NULL AND p.status IN ('COMPLETED','FAILED'))
                    OR (cw.state='DRAINING'
                        AND CASE WHEN cw.partition_id IS NULL THEN s.lease_token ELSE p.lease_token END<>cw.parent_lease_token)
                    OR (cw.state='DRAINING'
                        AND s.lease_token=cw.snapshot_lease_token AND j.lease_token=cw.job_lease_token
                        AND CASE WHEN cw.partition_id IS NULL THEN s.lease_token ELSE p.lease_token END=cw.parent_lease_token
                        AND (CASE WHEN cw.partition_id IS NULL THEN s.lease_expires_at ELSE p.lease_expires_at END <= NOW()
                            OR (CASE WHEN cw.partition_id IS NULL THEN s.lease_expires_at ELSE p.lease_expires_at END IS NULL
                                AND cw.created_at <= NOW()-(:ttl*INTERVAL '1 second')))
                        AND NOT EXISTS (SELECT 1 FROM worker_heartbeats wh
                            WHERE wh.worker_id=CASE WHEN cw.partition_id IS NULL THEN s.lease_owner_id ELSE p.lease_owner_id END
                              AND wh.last_seen_at>NOW()-(:heartbeat*INTERVAL '1 second'))))
                AND NOT (cw.state='RESUMING' AND j.status::text<>'CANCELLED'
                    AND CASE WHEN cw.partition_id IS NULL THEN s.status::text ELSE p.status END
                        IN ('COMPLETED','PARTIAL','FAILED'))
                AND (CAST(:after_created AS timestamptz) IS NULL
                    OR (cw.created_at,cw.id)>(CAST(:after_created AS timestamptz),CAST(:after_id AS uuid)))
                ORDER BY cw.created_at,cw.id LIMIT :limit
            """)
            cursor = self._orphan_cursor
            params = {'ttl': LEASE_TTL_S, 'heartbeat': HEARTBEAT_STALE_S,
                      'limit': max(16, self.concurrency * 4),
                      'after_created': cursor[0] if cursor else None, 'after_id': cursor[1] if cursor else None}
            rows = (await session.execute(query, params)).all()
            if not rows and cursor is not None:
                rows = (await session.execute(query, dict(params, after_created=None, after_id=None))).all()
                self._orphan_cursor = None
            if rows:
                self._orphan_cursor = (rows[-1].created_at, rows[-1].id)
            return [row.id for row in rows]

    async def _abandon(self, workflow_id):
        async with self.factory.begin() as session:
            await self._short_settings(session)
            workflow = await session.get(CopyWorkflow, workflow_id)
            if workflow is None:
                return False
            job, snapshot, partition = await copy_workflow._lock_parents(session, workflow)
            workflow = await session.scalar(select(CopyWorkflow).where(CopyWorkflow.id == workflow_id)
                                           .with_for_update().execution_options(populate_existing=True))
            if workflow.state not in copy_work.ACTIVE_WORKFLOWS:
                return False
            leaf = partition or snapshot
            if (workflow.state == 'RESUMING' and leaf is not None and job is not None
                and getattr(job.status, 'value', job.status) != 'CANCELLED'
                and getattr(leaf.status, 'value', leaf.status) in {'COMPLETED','PARTIAL','FAILED'}):
                # The callback commits its leaf before finishing the workflow.
                # Its live/expired resume lease, not orphan cancellation, owns
                # that interval. Expired claims replay parent settlement.
                return False
            now = await session.scalar(select(func.clock_timestamp()))
            cancelled = job is not None and getattr(job.status, 'value', job.status) == 'CANCELLED'
            terminal = (job is None or snapshot is None or
                getattr(job.status, 'value', job.status) not in {'RUNNING','QUEUED','PENDING','RETRYING'} or
                getattr(snapshot.status, 'value', snapshot.status) != 'IN_PROGRESS' or
                (partition is not None and partition.status in {'COMPLETED', 'FAILED'}))
            superseded = False
            if not terminal:
                leaf = partition or snapshot
                if workflow.state != 'DRAINING' or leaf is None:
                    return False
                if leaf.lease_token < workflow.parent_lease_token:
                    logger.warning('copy_unsealed_future_fence workflow_id=%s expected=%s actual=%s',
                                   workflow_id, workflow.parent_lease_token, leaf.lease_token)
                    return False
                superseded = leaf.lease_token > workflow.parent_lease_token
                if not superseded:
                    if (snapshot.lease_token != workflow.snapshot_lease_token
                        or job.lease_token != workflow.job_lease_token):
                        return False
                    expires = leaf.lease_expires_at
                    if expires is not None and expires > now:
                        return False
                    if expires is None and (now - workflow.created_at).total_seconds() < LEASE_TTL_S:
                        return False
                    if leaf.lease_owner_id is not None and await session.scalar(text("""
                        SELECT EXISTS(SELECT 1 FROM worker_heartbeats WHERE worker_id=:owner
                            AND last_seen_at>NOW()-(:stale*INTERVAL '1 second'))
                    """), {'owner': leaf.lease_owner_id, 'stale': HEARTBEAT_STALE_S}):
                        return False
            await copy_workflow.cancel_copy_work(session, workflow_id=workflow_id,
                reason=('copy_parent_cancelled' if cancelled else
                        'copy_unsealed_superseded' if superseded else 'copy_unsealed_abandoned'),
                terminal_state='CANCELLED' if cancelled else 'FAILED')
            return True

    async def recover_once(self):
        if self._closing:
            return 0
        operation = asyncio.current_task()
        self._inflight.add(operation)
        handled = 0
        try:
            await self._initialize()
            async with self._recovery:
                if self._closing or not await self._enabled():
                    return 0
                # Order matters: fully settled terminal parents first belong to
                # begin_resume's idempotent finish path, not abandonment.
                for workflow_id in await self._ready_workflows():
                    handled += bool(await self._resume(workflow_id))
                for workflow_id in await self._orphan_candidates():
                    if self._closing:
                        break
                    try:
                        handled += bool(await self._abandon(workflow_id))
                    except Exception as exc:
                        logger.warning('copy_abandon_failed workflow_id=%s error_type=%s', workflow_id, type(exc).__name__)
                self.wake()
            return handled
        except Exception as exc:
            logger.warning('copy_recovery_failed error_type=%s', type(exc).__name__)
            return handled
        finally:
            self._inflight.discard(operation)

    async def _worker(self):
        while not self._closing:
            result = await self.run_once()
            if self._closing:
                break
            if result is not None and result.status in {'READY', 'COMPLETED', 'FAILED', 'CANCELLED'}:
                continue
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self.poll_s)
            except asyncio.TimeoutError:
                pass
            self._wake.clear()

    async def _recover(self):
        while not self._closing:
            await self.recover_once()
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.recovery_s)
            except asyncio.TimeoutError:
                pass
