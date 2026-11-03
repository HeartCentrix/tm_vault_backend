import asyncio
import pytest
from shared.provider_runtime import ExecutionBudget

async def test_all_lanes_share_data_cap_while_control_remains_available():
    budget = ExecutionBudget(content=2, control=1, resident_bytes=100)
    active = peak = 0
    entered, release = asyncio.Event(), asyncio.Event()
    async def consume(lane):
        nonlocal active, peak
        async with budget.permit():
            active += 1
            peak = max(peak, active)
            if active == 2:
                entered.set()
            await release.wait()
            active -= 1
    tasks = [asyncio.create_task(consume(lane)) for lane in ['light', 'heavy', 'partition', 'resume']]
    await entered.wait()
    async with asyncio.timeout(1):
        async with budget.permit(control=True):
            assert active == 2
    release.set()
    await asyncio.gather(*tasks)
    assert peak == 2

async def test_byte_reservation_cancellation_does_not_leak_capacity():
    budget = ExecutionBudget(content=2, control=1, resident_bytes=100)
    async with budget.bytes(90):
        task = asyncio.create_task(budget.acquire_bytes(20))
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert budget.used_bytes == 90
    assert budget.used_bytes == 0
    async with budget.bytes(100):
        assert budget.used_bytes == 100

async def test_prepaid_storage_scope_reuses_only_own_matching_budget(monkeypatch):
    from shared import provider_runtime as runtime
    from shared.provider_runtime import storage_buffer_budget
    budget = ExecutionBudget(content=2, control=1, resident_bytes=100)
    monkeypatch.setattr(runtime, '_budget', budget)
    async with budget.bytes(100, includes_storage=True):
        async with asyncio.timeout(1):
            async with storage_buffer_budget(80):
                assert budget.used_bytes == 100
        with pytest.raises(ValueError, match='prepaid'):
            async with storage_buffer_budget(101):
                pass
        waiting = asyncio.create_task(storage_buffer_budget(1).__aenter__())
        await asyncio.sleep(0)
        assert not waiting.done()
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
    assert budget.used_bytes == 0

async def test_standalone_storage_operations_share_budget_and_cancel_waiter(monkeypatch):
    from shared import provider_runtime as runtime
    from shared.provider_runtime import storage_buffer_budget
    budget = ExecutionBudget(resident_bytes=100)
    monkeypatch.setattr(runtime, '_budget', budget)
    async with storage_buffer_budget(90):
        task = asyncio.create_task(storage_buffer_budget(20).__aenter__())
        await asyncio.sleep(0)
        assert not task.done()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert budget.used_bytes == 0

async def test_prepaid_storage_cannot_spend_retained_metadata_allowance(monkeypatch):
    from shared import provider_runtime as runtime
    budget = ExecutionBudget(resident_bytes=100)
    monkeypatch.setattr(runtime, '_budget', budget)
    async with budget.bytes(100, includes_storage=40):
        async with runtime.storage_buffer_budget(40):
            assert budget.used_bytes == 100
        with pytest.raises(ValueError, match='prepaid'):
            async with runtime.storage_buffer_budget(41): pass
    for invalid in (-1, 101, '40'):
        with pytest.raises(ValueError):
            async with budget.bytes(100, includes_storage=invalid): pass
    assert budget.used_bytes == 0


@pytest.mark.parametrize('live, reason, expected', [
    (True, 'rate', True), (True, 'concurrency', True),
    (True, 'cooldown', False), (False, 'rate', False),
])
async def test_live_provider_wait_keeps_internal_rate_wait_but_not_cooldown(live, reason, expected):
    from shared import provider_runtime as runtime
    from shared.provider_admission import ProviderDeferred
    execution = runtime.ProviderExecution(None, None)
    marker = runtime._EXECUTION.set(execution)
    try:
        error = ProviderDeferred(None, .001, reason=reason)
        if live:
            async with runtime.provider_live_wait_scope():
                result = await runtime.wait_provider_concurrency(error)
        else:
            result = await runtime.wait_provider_concurrency(error)
        assert result is expected
        assert execution.congestion is None
        assert execution.deferred is None
    finally:
        runtime._EXECUTION.reset(marker)


async def test_child_task_cannot_consume_its_parents_prepared_attempt():
    from datetime import datetime, timedelta, timezone
    from shared import provider_runtime as runtime
    now = datetime.now(timezone.utc)
    unit = runtime.UnitExecution('chat', 'chat', now + timedelta(minutes=1), now, 0, remaining=3)
    execution_marker = runtime._EXECUTION.set(runtime.ProviderExecution(None, None))
    unit_marker = runtime._UNIT.set(unit)
    try:
        async with runtime.provider_attempt_scope():
            await asyncio.create_task(runtime.account_provider_attempt())
            await runtime.account_provider_attempt()
        assert unit.remaining == 1
    finally:
        runtime._UNIT.reset(unit_marker)
        runtime._EXECUTION.reset(execution_marker)
