"""Bounded provider turns, durable recovery and cooperative deferral."""
from __future__ import annotations
import asyncio
from contextlib import asynccontextmanager, suppress
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import logging
import os
from uuid import uuid4
from sqlalchemy import and_, exists, func, or_, select
from shared import provider_work as work
from shared.models import ProviderWork, ProviderWorkUnit, Snapshot, SnapshotPartition
from shared.provider_admission import ProviderDeferred

logger = logging.getLogger(__name__)
_EXECUTION = ContextVar('provider_execution', default=None)
_UNIT = ContextVar('provider_unit', default=None)


class ProviderTurnDeferred(BaseException):
    """Control flow across legacy error collectors; runner commits suspension."""
    def __init__(self, error):
        self.error = error
        self.next_eligible_at = error.next_eligible_at
        super().__init__(getattr(error, 'stage', 'provider_wait'))


class ProviderBudgetExhausted(RuntimeError):
    pass


class ExecutionBudget:
    """One process budget shared by content lanes; control has its own reserve."""
    def __init__(self, *, content=8, control=2, resident_bytes=128 * 1024 * 1024):
        if min(content, control, resident_bytes) < 1:
            raise ValueError('execution capacity must be positive')
        self.content = asyncio.Semaphore(content)
        self.control = asyncio.Semaphore(control)
        self.resident_bytes, self.used_bytes = resident_bytes, 0
        self._bytes_changed = asyncio.Condition()

    @asynccontextmanager
    async def permit(self, *, control=False):
        async with (self.control if control else self.content):
            yield

    async def acquire_bytes(self, amount):
        if type(amount) is not int or not 0 <= amount <= self.resident_bytes:
            raise ValueError('byte reservation must fit the process budget')
        async with self._bytes_changed:
            await self._bytes_changed.wait_for(lambda: self.used_bytes + amount <= self.resident_bytes)
            self.used_bytes += amount

    async def release_bytes(self, amount):
        async with self._bytes_changed:
            self.used_bytes -= amount
            self._bytes_changed.notify_all()

    @asynccontextmanager
    async def bytes(self, amount):
        await self.acquire_bytes(amount)
        try:
            yield
        finally:
            await asyncio.shield(self.release_bytes(amount))


_budget = None

def execution_budget():
    global _budget
    if _budget is None:
        _budget = ExecutionBudget(content=int(os.getenv('PROVIDER_EXECUTION_CAPACITY', '8')),
            control=int(os.getenv('PROVIDER_CONTROL_RESERVE', '2')),
            resident_bytes=int(os.getenv('PROVIDER_RESIDENT_BYTES', str(128 * 1024 * 1024))))
    return _budget


@dataclass
class ProviderExecution:
    factory: object
    claim: work.ProviderClaim
    deferred: ProviderDeferred | None = None
    attempt_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    planning_unit: object = None
    lease_lost: bool = False
    owner_task: object = field(default_factory=asyncio.current_task)
    provider_tasks: set = field(default_factory=set)

    def defer(self, error):
        if self.deferred is None or error.next_eligible_at > self.deferred.next_eligible_at:
            self.deferred = error
        return ProviderTurnDeferred(self.deferred)


@dataclass
class UnitExecution:
    key: str
    kind: str
    deadline_at: datetime
    created_at: datetime
    reconciliations: int
    remaining: int = 0


def current_execution():
    return _EXECUTION.get()


def current_unit():
    return _UNIT.get()


def check_provider_turn():
    execution = _EXECUTION.get()
    if execution and execution.deferred:
        raise ProviderTurnDeferred(execution.deferred)


def defer_provider(error):
    execution = _EXECUTION.get()
    if execution is None:
        raise error
    raise execution.defer(error)


@asynccontextmanager
async def provider_request_scope():
    check_provider_turn()
    execution, task = current_execution(), asyncio.current_task()
    if execution and task is not execution.owner_task and task not in execution.provider_tasks:
        execution.provider_tasks.add(task)
        task.add_done_callback(execution.provider_tasks.discard)
    try:
        yield
    except ProviderDeferred as error:
        defer_provider(error)


@asynccontextmanager
async def unit_scope(unit_key, kind, *, checkpoint=None, deadline_at=None):
    execution = _EXECUTION.get()
    if execution is None:
        yield None
        return
    check_provider_turn()
    async with execution.factory.begin() as session:
        unit = await work.record_unit_checkpoint(session, execution.claim, unit_key=unit_key, kind=kind,
            checkpoint=checkpoint, deadline_at=deadline_at)
    state = UnitExecution(unit.unit_key, unit.kind, unit.deadline_at, unit.created_at, unit.reconciliations)
    token = _UNIT.set(state)
    try:
        yield state
    finally:
        _UNIT.reset(token)


async def account_provider_attempt(context=None):
    """Reserve up to16 transmissions before I/O; crash loses unused allowance.

    Counts are conservative consumed reservations, never reset by redelivery.
    Each grant uses a short independent transaction, not a pinned HTTP session.
    """
    check_provider_turn()
    execution, unit = _EXECUTION.get(), _UNIT.get()
    if execution is None:
        return
    if unit is None:
        # Enumeration outside a more precise stable unit is independently durable.
        if execution.planning_unit is None:
            async with unit_scope('planning', 'planning') as planning:
                execution.planning_unit = planning
        token = _UNIT.set(execution.planning_unit)
        try:
            await account_provider_attempt(context)
        finally:
            _UNIT.reset(token)
        return
    if datetime.now(timezone.utc) >= unit.deadline_at:
        raise ProviderBudgetExhausted('provider unit original deadline exhausted')
    async with execution.attempt_lock:
        if unit.remaining == 0:
            async with execution.factory.begin() as session:
                await work.record_unit_checkpoint(session, execution.claim, unit_key=unit.key,
                    kind=unit.kind, attempts=16)
            unit.remaining = 16
        unit.remaining -= 1


async def checkpoint_unit(*, receipt_id=None, state='COMPLETED', reconciliations=0):
    execution, unit = _EXECUTION.get(), _UNIT.get()
    if execution is None or unit is None:
        return
    async with execution.factory.begin() as session:
        row = await work.record_unit_checkpoint(session, execution.claim, unit_key=unit.key,
            kind=unit.kind, state=state, receipt_id=receipt_id, reconciliations=reconciliations)
        unit.reconciliations = row.reconciliations


async def other_provider_work_waiting():
    execution = current_execution()
    if execution is None:
        return False
    async with execution.factory() as session:
        return bool(await session.scalar(select(exists(select(ProviderWork.id).where(
            ProviderWork.id != execution.claim.work_id, ProviderWork.state == 'WAITING',
            ProviderWork.next_eligible_at <= func.clock_timestamp())))))


def yield_provider_turn():
    defer_provider(ProviderDeferred(None, 0, stage='fair_checkpoint'))


async def _join_provider_tasks(execution):
    while True:
        tasks = [task for task in execution.provider_tasks if not task.done()]
        if not tasks:
            return
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


class ProviderWorkRunner:
    def __init__(self, factory, resume_callback, settle_callback, *, concurrency=8, poll_s=1,
                 owner_id=None, budget=None, renew_s=30):
        self.factory, self.resume_callback, self.settle_callback = factory, resume_callback, settle_callback
        self.concurrency, self.poll_s = max(1, concurrency), poll_s
        self.renew_s = max(.01, renew_s)
        self.owner_id, self.budget = owner_id or uuid4(), budget or execution_budget()
        self._tasks, self._closing = set(), False
        self._wake = asyncio.Event()
        self._settlement_cursor = None

    def wake(self):
        self._wake.set()

    async def start(self):
        await work.initialize_guards(self.factory)
        if not self._tasks:
            for index in range(self.concurrency):
                self._tasks.add(asyncio.create_task(self._worker(), name=f'provider-work-{index}'))
            self._tasks.add(asyncio.create_task(self._settle(), name='provider-settlement'))

    async def close(self):
        self._closing = True
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    async def _renew(self, execution, owner_task):
        try:
            while True:
                await asyncio.sleep(self.renew_s)
                async with self.budget.permit(control=True):
                    async with self.factory.begin() as session:
                        await work.renew_work(session, execution.claim)
        except asyncio.CancelledError:
            raise
        except Exception:
            execution.lease_lost = True
            owner_task.cancel()

    async def run_once(self):
        async with self.budget.permit():
            async with self.factory.begin() as session:
                claim = await work.claim_due_work(session, owner_id=self.owner_id)
            if claim is None:
                return None
            execution = ProviderExecution(self.factory, claim)
            token = _EXECUTION.set(execution)
            renewer = asyncio.create_task(self._renew(execution, asyncio.current_task()))
            try:
                with work.work_scope(claim):
                    async with unit_scope('planning', 'planning'):
                        result = await self.resume_callback(claim)
                        check_provider_turn()
                        async with self.factory() as session:
                            handed = await session.get(ProviderWork, claim.work_id)
                            if handed.state in work.TERMINAL and handed.last_reason == 'copy_handoff':
                                return {'copy_waiting': True}
                        await checkpoint_unit()
                    async with self.factory.begin() as session:
                        await work.finish_work(session, claim)
                renewer.cancel()
                await asyncio.gather(renewer, return_exceptions=True)
                await self.settle_callback(claim.work_id)
                return result
            except (ProviderDeferred, ProviderTurnDeferred) as deferred:
                error = deferred.error if isinstance(deferred, ProviderTurnDeferred) else deferred
                execution.defer(error)
                await _join_provider_tasks(execution)
                async with self.factory.begin() as session:
                    await work.suspend_current_work(session, claim, next_eligible_at=error.next_eligible_at,
                        reason=getattr(error, 'stage', 'provider_wait'))
                return {'provider_waiting': True, 'provider_work_id': str(claim.work_id)}
            except asyncio.CancelledError:
                await _join_provider_tasks(execution)
                # Only an owned nonterminal leaf can be released; user cancellation
                # wins under the same parent locks. A hard crash is lease-recovered.
                with suppress(work.ProviderWorkConflict):
                    async with self.factory.begin() as session:
                        await work.suspend_current_work(session, claim,
                            next_eligible_at=datetime.now(timezone.utc), reason='worker_interrupted')
                if execution.lease_lost and not self._closing:
                    return {'provider_lease_lost': True}
                raise
            finally:
                renewer.cancel()
                await asyncio.gather(renewer, return_exceptions=True)
                _EXECUTION.reset(token)

    async def _worker(self):
        while not self._closing:
            try:
                if await self.run_once() is not None:
                    continue
            except asyncio.CancelledError:
                raise
            except Exception as error:
                logger.warning('provider_turn_failed error_type=%s', type(error).__name__)
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._wake.wait(), self.poll_s)
            self._wake.clear()

    async def _settle(self):
        while not self._closing:
            try:
                async with self.budget.permit(control=True):
                    async with self.factory() as session:
                        query = select(ProviderWork.id, ProviderWork.created_at).where(ProviderWork.parent_settle_pending)
                        if self._settlement_cursor is not None:
                            created, key = self._settlement_cursor
                            query = query.where(or_(ProviderWork.created_at > created,
                                and_(ProviderWork.created_at == created, ProviderWork.id > key)))
                        rows = (await session.execute(query.order_by(ProviderWork.created_at, ProviderWork.id).limit(32))).all()
                        self._settlement_cursor = (rows[-1].created_at, rows[-1].id) if rows else None
                    for work_id, _ in rows:
                        await self.settle_callback(work_id)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                logger.warning('provider_settlement_failed error_type=%s', type(error).__name__)
            await asyncio.sleep(max(1, self.poll_s))
