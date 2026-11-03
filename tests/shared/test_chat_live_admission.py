"""Local slot contention preserves live chat progress without bypassing limits."""
import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from shared import provider_runtime as runtime
from shared.provider_admission import ProviderDeferred, ProviderRequestContext


def busy(reason='concurrency'):
    return ProviderDeferred(ProviderRequestContext('tenant', 'app', 'teams'), .001,
                            reason=reason, scope='tenant')


async def test_live_chat_wait_keeps_execution_and_progress_without_drain():
    execution = runtime.ProviderExecution(None, None)
    marker = runtime._EXECUTION.set(execution)
    try:
        async with runtime.provider_live_wait_scope():
            progress = ['page-one']
            assert await runtime.wait_provider_concurrency(busy())
            await runtime.before_provider_admission()
            progress.append('page-two')
            assert progress == ['page-one', 'page-two']
            assert execution.congestion is None and execution.drain_task is None
            assert runtime.provider_admission_granted()
        assert runtime._LIVE_WAIT.get() is None
    finally:
        runtime._EXECUTION.reset(marker)


async def test_live_chat_wait_honors_original_deadline_and_real_cooldown():
    execution = runtime.ProviderExecution(None, None)
    marker = runtime._EXECUTION.set(execution)
    unit = runtime._UNIT.set(SimpleNamespace(deadline_at=datetime.now(timezone.utc)+timedelta(seconds=.02)))
    try:
        async with runtime.provider_live_wait_scope():
            assert not await runtime.wait_provider_concurrency(busy('cooldown'))
            await asyncio.sleep(.03)
            with pytest.raises(runtime.ProviderBudgetExhausted):
                await runtime.wait_provider_concurrency(busy())
        assert execution.congestion is None
    finally:
        runtime._UNIT.reset(unit)
        runtime._EXECUTION.reset(marker)


async def test_live_chat_wait_cancellation_cleans_context():
    started = asyncio.Event()
    async def queued():
        token = runtime._EXECUTION.set(runtime.ProviderExecution(None, None))
        try:
            async with runtime.provider_live_wait_scope():
                started.set()
                await runtime.wait_provider_concurrency(ProviderDeferred(
                    ProviderRequestContext('tenant','app','teams'),30,reason='concurrency'))
        finally:
            runtime._EXECUTION.reset(token)
    task=asyncio.create_task(queued())
    await asyncio.wait_for(started.wait(), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert runtime._LIVE_WAIT.get() is None


async def test_capture_concurrency_is_bounded_before_opening_source(monkeypatch):
    from shared import chat_capture as capture
    monkeypatch.setenv('CHAT_CAPTURE_CONCURRENCY', '2')
    active = peak = 0
    ready, release = asyncio.Event(), asyncio.Event()
    async def observe(**kwargs):
        nonlocal active, peak
        active += 1
        peak = max(active, peak)
        if active == 2: ready.set()
        try: await release.wait()
        finally: active -= 1
    monkeypatch.setattr(capture, '_capture_chat_inventory', observe)
    tasks = [asyncio.create_task(capture.capture_chat_inventory(
        scope=None, graph_client=None, storage=None, session_factory=None)) for _ in range(6)]
    try:
        await asyncio.wait_for(ready.wait(), 1)
        await asyncio.sleep(.01)
        assert peak == 2
        release.set()
        await asyncio.gather(*tasks)
    finally:
        release.set()
        await asyncio.gather(*tasks, return_exceptions=True)
    assert active == 0 and peak == 2
