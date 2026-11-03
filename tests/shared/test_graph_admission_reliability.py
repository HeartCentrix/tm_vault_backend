"""Admission is a real atomic grant, never a timer treated as a reservation."""
import asyncio
from types import SimpleNamespace

import pytest

from shared.graph_rate_limiter import _LocalTokenBucket, _RedisTokenBucket


async def test_redis_denial_redraws_before_grant(monkeypatch):
    calls = []
    async def script(**kwargs):
        calls.append(kwargs)
        return ('0.1', '0') if len(calls) == 1 else ('0', '0')
    async def sleep(delay):
        assert delay == .1
    monkeypatch.setattr('shared.graph_rate_limiter.asyncio.sleep', sleep)
    bucket = _RedisTokenBucket(SimpleNamespace(register_script=lambda lua: script), 'key', 10, 1, _LocalTokenBucket(10, 1))
    assert await bucket.acquire() == .1
    assert len(calls) == 2


async def test_redis_outage_never_falls_back_to_full_local_quota():
    async def script(**kwargs):
        raise OSError('coordinator offline')
    bucket = _RedisTokenBucket(SimpleNamespace(register_script=lambda lua: script), 'key', 10, 1, _LocalTokenBucket(10, 1))
    with pytest.raises(Exception, match='coordinator'):
        await bucket.acquire()


@pytest.mark.parametrize('cost', [0, -1, 2, float('nan'), float('inf')])
async def test_impossible_cost_fails_without_sleeping(cost):
    bucket = _LocalTokenBucket(10, 1)
    with pytest.raises(ValueError, match='cost'):
        await asyncio.wait_for(bucket.acquire(cost), timeout=.02)


async def test_cancelled_denial_does_not_grant_or_fallback(monkeypatch):
    calls = []
    async def script(**kwargs):
        calls.append(kwargs)
        return ('0.1', '0')
    async def sleep(delay):
        raise asyncio.CancelledError
    monkeypatch.setattr('shared.graph_rate_limiter.asyncio.sleep', sleep)
    bucket = _RedisTokenBucket(SimpleNamespace(register_script=lambda lua: script), 'key', 10, 1, _LocalTokenBucket(10, 1))
    with pytest.raises(asyncio.CancelledError):
        await bucket.acquire()
    assert len(calls) == 1


@pytest.mark.parametrize('enabled', [True, False])
async def test_coordinator_uses_enabled_individual_redis_settings(monkeypatch, enabled):
    from unittest.mock import AsyncMock
    from shared.config import settings
    from shared.graph_rate_limiter import GraphRateLimiter
    monkeypatch.delenv('REDIS_URL', raising=False)
    monkeypatch.setattr(settings, 'REDIS_ENABLED', enabled)
    url = 'redis://:escaped%2Fpassword@coordinator:6379/4'
    monkeypatch.setattr(settings, 'REDIS_URL_FULL', url)
    seen = []
    client = SimpleNamespace(ping=AsyncMock())
    def connect(actual, **kwargs):
        seen.append(actual)
        return client
    monkeypatch.setattr('redis.asyncio.Redis.from_url', connect)
    limiter = GraphRateLimiter()
    await limiter.maybe_init_redis()
    assert seen == ([url] if enabled else [])
    assert isinstance(limiter._bucket, _RedisTokenBucket) is enabled
