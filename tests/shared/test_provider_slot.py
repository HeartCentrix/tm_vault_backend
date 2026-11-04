"""A claimed provider leaf never idles on its lease behind a busy worker slot.

Railway 2026-11-04: one worker held four OneDrive leaves with two OneDrive slots; Rohit's
partitions sat leased for 8+ minutes behind Amit's 127 GiB partition while other replicas
had free slots.
"""
import asyncio

import pytest


async def test_full_slot_under_provider_work_hands_the_lease_back():
    from shared import provider_runtime as runtime
    slots = asyncio.Semaphore(1)
    await slots.acquire()
    marker = runtime._EXECUTION.set(runtime.ProviderExecution(None, None))
    try:
        with pytest.raises(runtime.ProviderTurnDeferred) as raised:
            async with runtime.provider_slot(slots, stage="onedrive_slot_busy"):
                pytest.fail("a busy slot must not be entered")
    finally:
        runtime._EXECUTION.reset(marker)
    assert raised.value.error.stage == "onedrive_slot_busy"
    assert slots.locked()


async def test_free_slot_under_provider_work_is_entered_and_released():
    from shared import provider_runtime as runtime
    slots = asyncio.Semaphore(1)
    marker = runtime._EXECUTION.set(runtime.ProviderExecution(None, None))
    try:
        async with runtime.provider_slot(slots, stage="onedrive_slot_busy"):
            assert slots.locked()
    finally:
        runtime._EXECUTION.reset(marker)
    assert not slots.locked()


async def test_legacy_consumer_still_waits_for_the_slot():
    from shared import provider_runtime as runtime
    slots = asyncio.Semaphore(1)
    await slots.acquire()
    entered = asyncio.Event()

    async def legacy():
        async with runtime.provider_slot(slots, stage="onedrive_slot_busy"):
            entered.set()

    task = asyncio.create_task(legacy())
    await asyncio.sleep(0.01)
    assert not entered.is_set()
    slots.release()
    await asyncio.wait_for(task, 1)
    assert entered.is_set()
