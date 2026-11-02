"""Shared pacing must survive operations without sharing retry exhaustion."""
import asyncio
import os
os.environ.setdefault('ALLOW_DEV_JWT_SECRETS', 'true')
import pytest
from shared.graph_client import GraphClient
from shared.graph_ratelimit import AsyncTokenBucket


def test_cached_pacing_and_fresh_retry_budgets():
    gc = GraphClient('app', 'secret', 'tenant')
    assert gc._policy is gc._policy
    first = gc._policy.for_operation()
    second = gc._policy.for_operation()
    first._throttle_walker._cumulative = 123456
    assert second._throttle_walker.cumulative_wait() == 0
    assert first.app_bucket is second.app_bucket is gc._policy.app_bucket
    assert first.stream_bucket is second.stream_bucket


async def test_low_priority_is_not_starved_by_continuous_high_priority():
    bucket = AsyncTokenBucket(100, capacity=1)
    await bucket.acquire()
    order = []
    async def take(label, priority):
        await bucket.acquire(priority=priority)
        order.append(label)
    # Urgent gets the first turn, but a normal waiter has a positive share.
    tasks = [asyncio.create_task(take('normal', 0))]
    tasks += [asyncio.create_task(take('urgent', 2)) for _ in range(50)]
    await asyncio.gather(*tasks)
    assert order[0] == 'urgent'
    assert order.index('normal') < 10


async def test_invalid_cost_fails_instead_of_waiting_forever():
    bucket = AsyncTokenBucket(10, capacity=1)
    with pytest.raises(ValueError):
        await asyncio.wait_for(bucket.acquire(cost=2), .02)
