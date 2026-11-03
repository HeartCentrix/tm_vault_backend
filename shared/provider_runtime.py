"""Bounded provider turns, durable recovery and cooperative deferral."""
from __future__ import annotations
import asyncio
from contextlib import asynccontextmanager, contextmanager, suppress
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import logging
import os
import time
from uuid import uuid4
from sqlalchemy import exists, func, select
from shared import provider_work as work
from shared.db_retry import run_db_transaction
from shared.models import ProviderWork, ProviderWorkUnit, Snapshot, SnapshotPartition
from shared.provider_admission import ProviderDeferred

logger = logging.getLogger(__name__)
_EXECUTION = ContextVar('provider_execution', default=None)
_UNIT = ContextVar('provider_unit', default=None)
_PREPAID_STORAGE = ContextVar('prepaid_storage_bytes', default=None)
_TRANSFER = ContextVar('provider_logical_transfer', default=None)
_RECEIPT = ContextVar('provider_receipt_drain', default=None)
_LIVE_WAIT = ContextVar('provider_live_wait', default=None)
_DRAIN_EXPIRED = 'provider_concurrency_drain_expired'
CONCURRENCY_DRAIN_SECONDS = 300


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
    async def bytes(self, amount, *, includes_storage=False):
        allowance = amount if includes_storage is True else includes_storage
        if allowance is not False and (type(allowance) is not int or not 0 <= allowance <= amount):
            raise ValueError('invalid prepaid storage allowance')
        await self.acquire_bytes(amount)
        token = _PREPAID_STORAGE.set((self, asyncio.current_task(), allowance)) if allowance is not False else None
        try:
            yield
        finally:
            if token is not None:
                _PREPAID_STORAGE.reset(token)
            await asyncio.shield(self.release_bytes(amount))


_budget = None

def execution_budget():
    global _budget
    if _budget is None:
        _budget = ExecutionBudget(content=int(os.getenv('PROVIDER_EXECUTION_CAPACITY', '8')),
            control=int(os.getenv('PROVIDER_CONTROL_RESERVE', '2')),
            resident_bytes=int(os.getenv('PROVIDER_RESIDENT_BYTES', str(128 * 1024 * 1024))))
    return _budget


@asynccontextmanager
async def storage_buffer_budget(amount):
    """Consume an explicit same-task prepaid scope, or reserve standalone buffers."""
    budget = execution_budget()
    prepaid = _PREPAID_STORAGE.get()
    if prepaid and prepaid[0] is budget and prepaid[1] is asyncio.current_task():
        if amount > prepaid[2]:
            raise ValueError('storage buffers exceed prepaid reservation')
        yield
    else:
        async with budget.bytes(amount):
            yield


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
    congestion: ProviderDeferred | None = None
    drain_deadline: float | None = None
    drain_task: object = None
    transfers: set = field(default_factory=set)
    transfers_changed: asyncio.Event = field(default_factory=asyncio.Event)
    drain_callbacks: set = field(default_factory=set)

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


@dataclass(eq=False)
class ProviderTransfer:
    execution: ProviderExecution
    owner_task: object
    deadline: float | None
    implicit: bool = False
    active: bool = False


def current_execution():
    return _EXECUTION.get()


def current_unit():
    return _UNIT.get()


@asynccontextmanager
async def provider_live_wait_scope():
    """Keep bounded chat observations alive through local admission contention."""
    execution, unit = current_execution(), current_unit()
    remaining = ((unit.deadline_at - datetime.now(timezone.utc)).total_seconds()
                 if unit else 1800)
    marker = _LIVE_WAIT.set((execution, time.monotonic() + max(0, remaining)))
    try:
        check_provider_turn()
        yield
    finally:
        _LIVE_WAIT.reset(marker)


@asynccontextmanager
async def provider_transfer_scope(*, timeout=None, implicit=False):
    """Keep one admitted file alive across range gaps, storage and buffered receipts."""
    execution, task = current_execution(), asyncio.current_task()
    inherited = _TRANSFER.get()
    if execution is None or (inherited and inherited.execution is execution
            and (not inherited.implicit or inherited.owner_task is task)):
        yield inherited
        return
    deadlines = [time.monotonic() + timeout] if timeout is not None and timeout > 0 else []
    unit = current_unit()
    if unit is not None:
        deadlines.append(time.monotonic() + (unit.deadline_at - datetime.now(timezone.utc)).total_seconds())
    transfer = ProviderTransfer(execution, task, min(deadlines) if deadlines else None, implicit)
    marker = _TRANSFER.set(transfer)
    try:
        await before_provider_admission()
        yield transfer
    finally:
        _TRANSFER.reset(marker)
        execution.transfers.discard(transfer)
        execution.transfers_changed.set()


@contextmanager
def provider_drain_callback(callback):
    execution = current_execution()
    registered = execution is not None and callback not in execution.drain_callbacks
    if registered:
        execution.drain_callbacks.add(callback)
    try:
        yield
    finally:
        if registered:
            execution.drain_callbacks.discard(callback)


@contextmanager
def provider_receipt_scope():
    """Allow bounded proof/receipt work through local congestion, never provider limits."""
    marker = _RECEIPT.set(current_execution())
    try:
        yield
    finally:
        _RECEIPT.reset(marker)


def _can_drain(execution):
    transfer = _TRANSFER.get()
    return (_RECEIPT.get() is execution or
        bool(transfer and transfer.execution is execution and transfer.active))


async def before_provider_admission(*, withdraw=None):
    check_provider_turn()
    execution = current_execution()
    if execution is None or execution.congestion is None:
        return
    if _can_drain(execution):
        if time.monotonic() >= execution.drain_deadline:
            raise ProviderTurnDeferred(execution.congestion)
        return
    if withdraw is not None:
        await withdraw()
    await asyncio.shield(execution.drain_task)
    raise ProviderTurnDeferred(execution.deferred or execution.congestion)


def provider_admission_granted():
    """A Redis grant activates a logical transfer; racing new grants must be released."""
    check_provider_turn()
    execution, transfer = current_execution(), _TRANSFER.get()
    if execution is None:
        return True
    if execution.congestion is not None and not _can_drain(execution):
        return False
    if transfer and transfer.execution is execution and not transfer.active:
        transfer.active = True
        if _RECEIPT.get() is not execution:
            execution.transfers.add(transfer)
    return True


async def _drain_congestion(execution):
    marker = _TRANSFER.set(None)
    deadline = asyncio.timeout_at(execution.drain_deadline)
    try:
        try:
            async with deadline:
                while execution.transfers:
                    execution.transfers_changed.clear()
                    await execution.transfers_changed.wait()
                with provider_receipt_scope():
                    for callback in tuple(execution.drain_callbacks):
                        await callback()
        except TimeoutError:
            if not deadline.expired():
                raise
            execution.defer(execution.congestion)
            for transfer in tuple(execution.transfers):
                transfer.owner_task.cancel(_DRAIN_EXPIRED)
        else:
            execution.defer(execution.congestion)
    finally:
        _TRANSFER.reset(marker)


async def wait_provider_concurrency(error, *, wait=None, withdraw=None):
    """Continue the same Redis grant attempt while already-admitted transfers drain."""
    execution = current_execution()
    live = _LIVE_WAIT.get()
    if error.reason != 'concurrency':
        return False
    if live is not None and live[0] is execution:
        check_provider_turn()
        await (wait or asyncio.sleep)(min(error.retry_after, max(0, live[1] - time.monotonic())))
        check_provider_turn()
        return True
    if execution is None:
        return False
    if not _can_drain(execution) and withdraw is not None:
        # This task now waits for sibling receipts, not for a network grant.
        # Leaving it queued would block the receipt requests that release the turn.
        await withdraw()
    if execution.congestion is None:
        execution.congestion = error
        deadlines = [time.monotonic() + CONCURRENCY_DRAIN_SECONDS]
        deadlines.extend(transfer.deadline for transfer in execution.transfers if transfer.deadline is not None)
        unit = current_unit()
        if unit is not None:
            deadlines.append(time.monotonic() + (unit.deadline_at - datetime.now(timezone.utc)).total_seconds())
        execution.drain_deadline = min(deadlines)
        execution.drain_task = asyncio.create_task(_drain_congestion(execution))
    await before_provider_admission()
    await (wait or asyncio.sleep)(min(error.retry_after, max(0, execution.drain_deadline - time.monotonic())))
    return True


def check_provider_turn():
    execution = _EXECUTION.get()
    if execution and execution.deferred:
        raise ProviderTurnDeferred(execution.deferred)
    live = _LIVE_WAIT.get()
    if live is not None and live[0] is execution and time.monotonic() >= live[1]:
        raise ProviderBudgetExhausted('provider unit original deadline exhausted')


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
    unit = await run_db_transaction(execution.factory, lambda session:
        work.record_unit_checkpoint(session, execution.claim, unit_key=unit_key, kind=kind,
            checkpoint=checkpoint, deadline_at=deadline_at))
    state = UnitExecution(unit.unit_key, unit.kind, unit.deadline_at, unit.created_at, unit.reconciliations)
    if datetime.now(timezone.utc) >= state.deadline_at:
        raise ProviderBudgetExhausted('provider unit original deadline exhausted')
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
            await run_db_transaction(execution.factory, lambda session:
                work.record_unit_checkpoint(session, execution.claim, unit_key=unit.key,
                    kind=unit.kind, attempts=16))
            unit.remaining = 16
        unit.remaining -= 1


async def checkpoint_unit(*, receipt_id=None, state='COMPLETED', reconciliations=0):
    execution, unit = _EXECUTION.get(), _UNIT.get()
    if execution is None or unit is None:
        return
    row = await run_db_transaction(execution.factory, lambda session:
        work.record_unit_checkpoint(session, execution.claim, unit_key=unit.key,
            kind=unit.kind, state=state, receipt_id=receipt_id, reconciliations=reconciliations))
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
    if execution.drain_task is not None:
        execution.drain_task.cancel()
        await asyncio.gather(execution.drain_task, return_exceptions=True)
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
                    await run_db_transaction(self.factory, lambda session:
                        work.renew_work(session, execution.claim))
        except asyncio.CancelledError:
            raise
        except Exception as error:
            from shared.provider_observability import workflow_error_details
            reason, locations = workflow_error_details(error)
            logger.warning('provider_lease_renewal_failed work_id=%s snapshot_id=%s generation=%s error_type=%s reason=%s locations=%s',
                execution.claim.work_id, execution.claim.snapshot_id, execution.claim.generation,
                type(error).__name__, reason, locations)
            execution.lease_lost = True
            owner_task.cancel()

    async def run_once(self):
        entered = time.monotonic()
        async with self.budget.permit():
            admitted = time.monotonic()
            claim = await run_db_transaction(self.factory, lambda session:
                work.claim_due_work(session, owner_id=self.owner_id))
            if claim is None:
                return None
            started = time.monotonic()
            logger.info('provider_turn_started work_id=%s snapshot_id=%s job_id=%s generation=%s local_slot_wait_s=%.3f claim_transaction_s=%.3f',
                claim.work_id, claim.snapshot_id, claim.job_id, claim.generation, admitted-entered, started-admitted)
            execution = ProviderExecution(self.factory, claim)
            token = _EXECUTION.set(execution)
            renewer = asyncio.create_task(self._renew(execution, asyncio.current_task()))
            try:
                with work.work_scope(claim):
                    async with unit_scope('planning', 'planning'):
                        result = await self.resume_callback(claim)
                        check_provider_turn()
                        if execution.drain_task is not None:
                            await asyncio.shield(execution.drain_task)
                            check_provider_turn()
                        async with self.factory() as session:
                            handed = await session.get(ProviderWork, claim.work_id)
                            if handed.state in work.TERMINAL and handed.last_reason == 'copy_handoff':
                                return {'copy_waiting': True}
                        await checkpoint_unit()
                    await run_db_transaction(self.factory, lambda session:
                        work.finish_work(session, claim))
                renewer.cancel()
                await asyncio.gather(renewer, return_exceptions=True)
                await self.settle_once(claim.work_id)
                return result
            except (ProviderDeferred, ProviderTurnDeferred) as deferred:
                error = deferred.error if isinstance(deferred, ProviderTurnDeferred) else deferred
                return await self._commit_deferral(execution, error)
            except ProviderBudgetExhausted:
                await _join_provider_tasks(execution)
                with work.work_scope(claim):
                    await run_db_transaction(self.factory, lambda session:
                        work.fail_exhausted_work(session, claim))
                renewer.cancel()
                await asyncio.gather(renewer, return_exceptions=True)
                await self.settle_once(claim.work_id)
                return {'provider_exhausted': True, 'provider_work_id': str(claim.work_id)}
            except asyncio.CancelledError as cancelled:
                if (cancelled.args == (_DRAIN_EXPIRED,) and execution.congestion is not None
                        and not execution.lease_lost and not self._closing):
                    return await self._commit_deferral(execution, execution.congestion)
                await _join_provider_tasks(execution)
                # Only an owned nonterminal leaf can be released; user cancellation
                # wins under the same parent locks. A hard crash is lease-recovered.
                with suppress(work.ProviderWorkConflict):
                    await run_db_transaction(self.factory, lambda session:
                        work.suspend_current_work(session, claim,
                            next_eligible_at=datetime.now(timezone.utc), reason='worker_interrupted'))
                if execution.lease_lost and not self._closing:
                    return {'provider_lease_lost': True}
                raise
            except Exception as error:
                from shared.provider_observability import workflow_error_details
                reason, locations = workflow_error_details(error)
                logger.warning('provider_turn_error work_id=%s snapshot_id=%s job_id=%s generation=%s error_type=%s reason=%s locations=%s',
                    claim.work_id, claim.snapshot_id, claim.job_id, claim.generation,
                    type(error).__name__, reason, locations)
                raise
            finally:
                logger.info('provider_turn_ended work_id=%s snapshot_id=%s elapsed_s=%.3f',
                    claim.work_id, claim.snapshot_id, time.monotonic()-started)
                await _join_provider_tasks(execution)
                renewer.cancel()
                await asyncio.gather(renewer, return_exceptions=True)
                _EXECUTION.reset(token)

    async def _commit_deferral(self, execution, error):
        execution.defer(error)
        await _join_provider_tasks(execution)
        error, claim = execution.deferred, execution.claim
        await run_db_transaction(self.factory, lambda session:
            work.suspend_current_work(session, claim, next_eligible_at=error.next_eligible_at,
                reason=getattr(error, 'stage', 'provider_wait')))
        logger.info('provider_turn_deferred work_id=%s snapshot_id=%s stage=%s reason=%s scope=%s retry_after_s=%s next_eligible_at=%s',
            claim.work_id, claim.snapshot_id, getattr(error, 'stage', 'provider_wait'),
            getattr(error, 'reason', None), getattr(error, 'scope', None),
            error.retry_after, error.next_eligible_at)
        return {'provider_waiting': True, 'provider_work_id': str(claim.work_id)}

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

    async def settle_once(self, work_id=None):
        async with self.budget.permit(control=True):
            claim = await run_db_transaction(self.factory, lambda session:
                work.claim_parent_settlement(session, owner_id=self.owner_id, work_id=work_id))
            if claim is None:
                return False
            try:
                with work.settling_parent(claim):
                    # Leave recovery time inside the 120-second claim if a callback stalls.
                    async with asyncio.timeout(60):
                        await self.settle_callback(claim.work_id)
            finally:
                await run_db_transaction(self.factory, lambda session:
                    work.release_parent_settlement(session, claim))
            return True

    async def _settle(self):
        while not self._closing:
            try:
                for _ in range(32):
                    if self._closing or not await self.settle_once():
                        break
            except asyncio.CancelledError:
                raise
            except Exception as error:
                logger.warning('provider_settlement_failed error_type=%s', type(error).__name__)
            await asyncio.sleep(max(1, self.poll_s))
