"""Regression test for the scheduler RabbitMQ connection leak.

`message_bus.connect()` is a singleton-level call invoked from several
APScheduler jobs (e.g. `_reconcile_stuck_queued_jobs`, `_sweep_tier2_gaps`)
on every interval tick. The original code opened a *new* `connect_robust`
connection on each call without closing the previous one — and aio_pika
keeps abandoned robust connections alive, so every tick leaked one
connection. Over hours the backup-scheduler pod accumulated ~150 open
connections to RabbitMQ.

`connect()` must be idempotent: when a live connection already exists,
re-calling it must NOT open a second one.
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
from unittest.mock import AsyncMock, MagicMock

import pytest

from shared.message_bus import MessageBus


def _run(coro):
    """Run on a live loop, installing one if the suite left none behind.

    Other modules in this suite use asyncio.run(), which uninstalls the
    current event loop; get_event_loop() then raises depending on test
    order. Leave the loop installed (not closed) so later modules find a
    usable one too.

    Bare `asyncio.get_event_loop()` here is why both tests in this file
    failed in a full-suite run while passing in isolation
    ("RuntimeError: There is no current event loop in thread 'MainThread'").
    Identical helper to the sibling test_message_bus_consume_ack.py, which
    covers the same module and already had the fix.
    """
    try:
        loop = asyncio.get_event_loop_policy().get_event_loop()
        if loop.is_closed():
            raise RuntimeError("closed")
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)


def _patch_bus_settings(monkeypatch):
    """Make `settings` usable regardless of import order.

    The module-level `os.environ.setdefault(...)` calls above only take
    effect if THIS module is imported before `shared.config` builds its
    `settings` singleton. In a full-suite run something imports it first,
    so RABBITMQ_USER/PASSWORD stay empty and the computed RABBITMQ_URL
    property raises "RabbitMQ credentials not set".

    That surfaced as a HANG rather than a failure: connect()'s default
    max_retries=0 means "retry forever with backoff", so the RuntimeError
    was swallowed and re-tried every few seconds until the suite was
    killed. Patch the fields the property reads instead of trusting env
    ordering.
    """
    monkeypatch.setattr("shared.message_bus.settings.RABBITMQ_ENABLED", True)
    monkeypatch.setattr("shared.message_bus.settings.RABBITMQ_USER", "test")
    monkeypatch.setattr("shared.message_bus.settings.RABBITMQ_PASSWORD", "test")


def _make_fake_connect_robust(call_counter):
    """Build a fake aio_pika.connect_robust that records call count and
    returns a fully-mocked, *open* connection."""

    async def _fake(*args, **kwargs):
        call_counter["n"] += 1

        queue = MagicMock()
        queue.bind = AsyncMock()

        channel = MagicMock()
        channel.declare_exchange = AsyncMock(return_value=MagicMock())
        channel.declare_queue = AsyncMock(return_value=queue)
        channel.set_qos = AsyncMock()

        conn = MagicMock()
        conn.is_closed = False
        conn.channel = AsyncMock(return_value=channel)
        conn.close = AsyncMock()
        return conn

    return _fake


def test_connect_is_idempotent_when_already_connected(monkeypatch):
    """Two connect() calls on a live bus open exactly ONE connection."""
    counter = {"n": 0}
    _patch_bus_settings(monkeypatch)
    monkeypatch.setattr(
        "shared.message_bus.aio_pika.connect_robust",
        _make_fake_connect_robust(counter),
    )

    bus = MessageBus()
    _run(bus.connect())
    assert counter["n"] == 1, "first connect should open a connection"

    # Simulate a later scheduler tick re-calling connect() unconditionally.
    _run(bus.connect())
    assert counter["n"] == 1, (
        f"connect() leaked a new connection on the second call "
        f"(connect_robust called {counter['n']}x, expected 1)"
    )


def test_connect_reconnects_after_connection_closed(monkeypatch):
    """If the existing connection is closed, connect() must reopen one."""
    counter = {"n": 0}
    _patch_bus_settings(monkeypatch)
    monkeypatch.setattr(
        "shared.message_bus.aio_pika.connect_robust",
        _make_fake_connect_robust(counter),
    )

    bus = MessageBus()
    _run(bus.connect())
    assert counter["n"] == 1

    bus.connection.is_closed = True  # broker dropped us, robust gave up
    _run(bus.connect())
    assert counter["n"] == 2, "a closed connection must be replaced"
