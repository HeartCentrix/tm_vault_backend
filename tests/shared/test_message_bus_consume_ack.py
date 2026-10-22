"""`consume()` must never ACK a message whose handler raised.

The callback exception was caught INSIDE `async with message.process():`,
so the context manager saw a clean exit and aio_pika ACKed a message
whose handler had failed — the "send to DLQ" was only a comment. The one
production consumer is audit-service's `consume_audit_events`, whose
callback persists audit events, so any handler error silently destroyed
the event.

Queues are declared in `_declare_queue` with
`x-dead-letter-exchange: tm.exchange` +
`x-dead-letter-routing-key: <routing_key>.dlq`, so a reject/nack WITHOUT
requeue lands in `<queue>.dlq`. The chosen semantics are therefore:
requeue once, then dead-letter on the redelivery — bounded at two
attempts so a permanently-poisonous message cannot hot-loop.

The fake messages below are driven through aio_pika's REAL
`ProcessContext`, so ack/reject behaviour is the library's, not a
re-implementation of it.
"""
from __future__ import annotations

import os

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "test_secret_for_unit_tests_only_x")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")
os.environ.setdefault("AUDIT_SERVICE_URL", "http://localhost:8000")
os.environ.setdefault("RABBITMQ_USERNAME", "test")
os.environ.setdefault("RABBITMQ_PASSWORD", "test")

import asyncio
import json
from typing import Any, Dict, List, Optional

import pytest

from aio_pika.message import ProcessContext

from shared.message_bus import MessageBus


def _run(coro):
    """Run on a live loop, installing one if the suite left none behind.

    Other modules in this suite use asyncio.run(), which uninstalls the
    current event loop; get_event_loop() then raises depending on test
    order. Leave the loop installed (not closed) so later modules find a
    usable one too.
    """
    try:
        loop = asyncio.get_event_loop_policy().get_event_loop()
        if loop.is_closed():
            raise RuntimeError("closed")
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)


class _FakeChannelState:
    is_closed = False


class _FakeMessage:
    """Minimal IncomingMessage stand-in driven by the real ProcessContext."""

    def __init__(self, body: Dict[str, Any] | bytes, redelivered: bool = False):
        self.body = body if isinstance(body, bytes) else json.dumps(body).encode()
        self.redelivered = redelivered
        self.processed = False
        self.channel = _FakeChannelState()
        self.acked = False
        self.rejects: List[Optional[bool]] = []   # requeue flag per reject

    def process(self, **kwargs):
        return ProcessContext(self, **{
            "requeue": kwargs.get("requeue", False),
            "reject_on_redelivered": kwargs.get("reject_on_redelivered", False),
            "ignore_processed": kwargs.get("ignore_processed", False),
        })

    async def ack(self):
        self.processed = True
        self.acked = True

    async def reject(self, requeue: bool = False):
        self.processed = True
        self.rejects.append(requeue)

    async def nack(self, requeue: bool = False):
        self.processed = True
        self.rejects.append(requeue)


class _FakeQueue:
    def __init__(self, messages: List[_FakeMessage]):
        self._messages = messages

    def iterator(self):
        messages = self._messages

        class _Iter:
            async def __aenter__(self):
                async def _gen():
                    for m in messages:
                        yield m
                return _gen()

            async def __aexit__(self, *exc):
                return False

        return _Iter()


def _bus_with(messages: List[_FakeMessage], monkeypatch) -> MessageBus:
    monkeypatch.setattr("shared.message_bus.settings.RABBITMQ_ENABLED", True)
    bus = MessageBus()

    class _Chan:
        async def get_queue(self, name):
            return _FakeQueue(messages)

    bus.channel = _Chan()
    return bus


# ─── Tests ────────────────────────────────────────────────────────────


def test_failed_handler_is_not_acked(monkeypatch):
    """A raising callback must not result in an ACK (silent data loss)."""
    msg = _FakeMessage({"action": "BACKUP_COMPLETED"})
    bus = _bus_with([msg], monkeypatch)

    async def _boom(body):
        raise RuntimeError("db down")

    _run(bus.consume("audit.events", _boom))

    assert msg.acked is False, (
        "message whose handler RAISED was ACKed — the event is gone; the "
        "queue's dead-letter-exchange never sees it"
    )
    assert msg.rejects, (
        "message whose handler raised was neither requeued nor rejected"
    )


def test_failed_handler_requeues_once_then_dead_letters(monkeypatch):
    """Bounded redelivery: requeue on first failure, DLQ on the retry."""
    first = _FakeMessage({"action": "A"}, redelivered=False)
    retry = _FakeMessage({"action": "A"}, redelivered=True)
    bus = _bus_with([first, retry], monkeypatch)

    async def _boom(body):
        raise RuntimeError("db down")

    _run(bus.consume("audit.events", _boom))

    assert first.rejects == [True], (
        f"first failure should be requeued once, got {first.rejects}"
    )
    assert retry.rejects == [False], (
        "a redelivered message that fails again must be rejected WITHOUT "
        "requeue so the x-dead-letter-exchange routes it to <queue>.dlq "
        f"instead of hot-looping, got {retry.rejects}"
    )


def test_undecodable_body_is_not_silently_dropped(monkeypatch):
    """Malformed JSON belongs in the DLQ, not in the bit bucket."""
    msg = _FakeMessage(b"{not json", redelivered=True)
    bus = _bus_with([msg], monkeypatch)

    seen: List[Any] = []

    async def _cb(body):
        seen.append(body)

    _run(bus.consume("audit.events", _cb))

    assert seen == [], "callback should not have been reached"
    assert msg.acked is False, "undecodable message was ACKed and lost"
    assert msg.rejects == [False], (
        f"undecodable message should be dead-lettered, got {msg.rejects}"
    )


def test_consumer_survives_a_failing_message_and_keeps_draining(monkeypatch):
    """One bad message must not tear down the consumer loop."""
    bad = _FakeMessage({"action": "BAD"})
    good = _FakeMessage({"action": "GOOD"})
    bus = _bus_with([bad, good], monkeypatch)

    handled: List[str] = []

    async def _cb(body):
        if body["action"] == "BAD":
            raise RuntimeError("transient")
        handled.append(body["action"])

    _run(bus.consume("audit.events", _cb))

    assert handled == ["GOOD"], (
        f"consumer stopped after the failing message: handled={handled}"
    )
    assert good.acked is True, "the good message should still be ACKed"


def test_successful_handler_is_acked(monkeypatch):
    msg = _FakeMessage({"action": "OK"})
    bus = _bus_with([msg], monkeypatch)

    async def _cb(body):
        return None

    _run(bus.consume("audit.events", _cb))

    assert msg.acked is True
    assert msg.rejects == []
