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
