"""One newest-change check per chat per run, shared by every member and worker through Redis:
Teams allows 1 request/s per chat, so a group chat with hundreds of members must cost one check."""
import asyncio
from datetime import datetime, timezone

import pytest

from shared import chat_capture
from shared.chat_capture_graph import ChatGraphError


class FakeRedis:
    def __init__(self):
        self.data = {}

    async def mget(self, keys):
        return [self.data.get(k) for k in keys]

    def pipeline(self, transaction=False):
        redis = self

        class Pipe:
            def __init__(self):
                self.ops = []
            async def __aenter__(self):
                return self
            async def __aexit__(self, *exc):
                return False
            def set(self, key, value, ex=None, nx=False):
                self.ops.append((key, value, nx))
            async def execute(self):
                for key, value, nx in self.ops:
                    if not (nx and key in redis.data):
                        redis.data[key] = value
        return Pipe()


@pytest.fixture
def redis(monkeypatch):
    from shared.graph_rate_limiter import graph_rate_limiter
    fake = FakeRedis()
    async def shared():
        return fake
    monkeypatch.setattr(graph_rate_limiter, "redis", shared)
    return fake


def probe(calls, *, fail=()):
    async def latest(chat_id):
        calls.append(chat_id)
        if chat_id in fail:
            raise ChatGraphError("Graph capture metadata failed with HTTP 429", status_code=429)
        return None if chat_id == "empty" else datetime(2026, 11, 5, 14, 38, 32, tzinfo=timezone.utc)
    return latest


def test_every_member_of_a_run_shares_one_check_per_chat(redis):
    calls = []
    first = asyncio.run(chat_capture._shared_latest_changes("job-1", "tenant", ["a", "empty"], probe(calls)))
    second = asyncio.run(chat_capture._shared_latest_changes("job-1", "tenant", ["a", "empty"], probe(calls)))
    assert calls == ["a", "empty"] and first == second
    assert first == {"a": datetime(2026, 11, 5, 14, 38, 32, tzinfo=timezone.utc), "empty": None}
    asyncio.run(chat_capture._shared_latest_changes("job-2", "tenant", ["a"], probe(calls)))
    assert calls == ["a", "empty", "a"]   # a new run checks again: an edit between backups is never hidden


def test_a_failed_check_is_never_shared(redis):
    calls = []
    assert asyncio.run(chat_capture._shared_latest_changes("job-1", "tenant", ["a"], probe(calls, fail={"a"}))) == {"a": False}
    asyncio.run(chat_capture._shared_latest_changes("job-1", "tenant", ["a"], probe(calls)))
    assert calls == ["a", "a"]


def test_without_redis_each_member_checks(monkeypatch):
    from shared.graph_rate_limiter import graph_rate_limiter
    async def none():
        return None
    monkeypatch.setattr(graph_rate_limiter, "redis", none)
    calls = []
    for _ in range(2):
        asyncio.run(chat_capture._shared_latest_changes("job-1", "tenant", ["a"], probe(calls)))
    assert calls == ["a", "a"]
