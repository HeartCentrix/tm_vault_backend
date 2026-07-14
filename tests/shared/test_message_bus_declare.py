"""Guard: _declare_queue must be able to create a BRAND-NEW queue.

This broker rejects `x-consumer-timeout` at queue *creation* (pre-existing
queues are grandfathered). Adding a new queue WITH that arg failed the whole
connect for every service (P0). `apply_consumer_timeout=False` must omit it so a
new queue like enrich.priority always creates; the default must keep it for the
long-running drain queues."""
from __future__ import annotations

import pathlib
import sys

import pytest

_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from shared.message_bus import MessageBus  # noqa: E402


class _FakeQueue:
    async def bind(self, exchange, routing_key):
        return None


class _FakeChannel:
    def __init__(self):
        self.declares = []  # (name, arguments)

    async def declare_queue(self, name, durable=False, arguments=None):
        self.declares.append((name, dict(arguments or {})))
        return _FakeQueue()


def _args_for(ch, name):
    return next(a for n, a in ch.declares if n == name)


@pytest.mark.asyncio
async def test_new_queue_omits_consumer_timeout_when_opted_out():
    mb = MessageBus()
    mb.channel = _FakeChannel()
    mb.exchange = object()
    await mb._declare_queue("enrich.priority", "enrich.priority", apply_consumer_timeout=False)
    args = _args_for(mb.channel, "enrich.priority")
    assert "x-consumer-timeout" not in args          # the arg that broke creation
    assert args["x-dead-letter-exchange"] == "tm.exchange"  # DLX still wired


@pytest.mark.asyncio
async def test_default_keeps_consumer_timeout_for_long_drains():
    mb = MessageBus()
    mb.channel = _FakeChannel()
    mb.exchange = object()
    await mb._declare_queue("restore.low", "restore.low")
    assert "x-consumer-timeout" in _args_for(mb.channel, "restore.low")
