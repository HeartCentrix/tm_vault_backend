"""A dropped DB connection must be recognisable as transient.

2026-08-11 02:20:50: the post-drain bookkeeping SELECT of CHATS shard 4 on
snapshot 6807df97 ("Chats — Carlos Romero") died with asyncpg
ConnectionDoesNotExistError ("connection was closed in the middle of
operation") — collateral from our own pool saturation ("QueuePool limit of
size 40 overflow 40 reached", 3x on that snapshot minutes earlier). The
shard's catch-all read that as "the shard failed" and marked the partition
terminally FAILED with files_uploaded=0, while its 19,554 messages across
176 of 184 chats were already committed. No sweep recovers FAILED, so the
shard was lost to a bookkeeping query.

Nothing on that path could tell a dropped connection from a real failure.
These tests pin the classifier that now can — including the direction it
errs in: anything unrecognised is PERMANENT, because a false "transient"
would retry a poison query forever.
"""
from __future__ import annotations

import asyncio
import os
import pathlib
import sys

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from shared.db_retry import (  # noqa: E402
    db_error_label,
    is_transient_db_error,
    run_with_db_retry,
)


def _run(coro):
    try:
        loop = asyncio.get_event_loop_policy().get_event_loop()
        if loop.is_closed():
            raise RuntimeError("closed")
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)


# Stand-ins named exactly like the real driver classes — the classifier
# matches on class NAME on purpose (see shared/db_retry docstring), so
# these are faithful stubs, not approximations.
class ConnectionDoesNotExistError(Exception):
    pass


class InterfaceError(Exception):
    pass


class OperationalError(Exception):
    pass


class IntegrityError(Exception):
    pass


class ProgrammingError(Exception):
    pass


class DBAPIError(Exception):
    """SQLAlchemy's wrapper: the driver error hangs off `.orig`."""

    def __init__(self, msg, orig=None, connection_invalidated=False):
        super().__init__(msg)
        self.orig = orig
        self.connection_invalidated = connection_invalidated


# ─── the exact prod exception ─────────────────────────────────────────────

def test_the_2026_08_11_connection_drop_is_transient():
    exc = ConnectionDoesNotExistError(
        "connection was closed in the middle of operation"
    )
    assert is_transient_db_error(exc) is True


def test_pool_exhaustion_is_transient():
    """The root trigger. sqlalchemy.exc.TimeoutError's class name is the
    builtin's, so it is matched by its unmistakable message instead."""
    exc = TimeoutError(
        "QueuePool limit of size 40 overflow 40 reached, connection timed "
        "out, timeout 30.00"
    )
    assert is_transient_db_error(exc) is True


def test_sqlalchemy_wrapper_around_the_driver_error_is_transient():
    """SQLAlchemy hands callers a DBAPIError; the identifying class name
    is one link down on `.orig`."""
    wrapped = DBAPIError(
        "(asyncpg.exceptions.ConnectionDoesNotExistError) ...",
        orig=ConnectionDoesNotExistError("connection was closed"),
    )
    assert is_transient_db_error(wrapped) is True


def test_connection_invalidated_flag_is_trusted():
    exc = DBAPIError("something the name set has never seen",
                     connection_invalidated=True)
    assert is_transient_db_error(exc) is True


def test_cause_chain_is_walked():
    outer = RuntimeError("shard bookkeeping failed")
    outer.__cause__ = InterfaceError("connection is closed")
    assert is_transient_db_error(outer) is True


# ─── things that must NOT be retried ──────────────────────────────────────

def test_integrity_error_is_permanent():
    assert is_transient_db_error(IntegrityError("duplicate key value")) is False


def test_permanent_wins_over_a_transient_looking_wrapper():
    """A UniqueViolation nested inside an OperationalError-shaped wrapper
    is still a UniqueViolation — retrying it fails identically forever."""
    wrapped = DBAPIError("(asyncpg) ...", orig=IntegrityError("dup key"))
    assert is_transient_db_error(wrapped) is False


def test_programming_error_is_permanent():
    assert is_transient_db_error(ProgrammingError("column x does not exist")) is False


def test_unrecognised_exception_is_permanent():
    """The safe direction: an unknown error keeps today's behaviour."""
    assert is_transient_db_error(ValueError("nope")) is False
    assert is_transient_db_error(None) is False


def test_graph_side_timeouts_are_not_claimed_as_db_transients():
    """`_partition_retry_or_fail` consults this predicate on a path where a
    Graph exception can also arrive. A bare socket timeout must keep its
    existing handling, or every Graph read timeout silently becomes a
    shard requeue."""
    assert is_transient_db_error(TimeoutError("Timeout on reading data from socket")) is False
    assert is_transient_db_error(ConnectionResetError("Connection reset by peer")) is False


def test_self_referential_context_does_not_hang():
    a = InterfaceError("boom")
    b = RuntimeError("outer")
    b.__context__ = a
    a.__context__ = b
    assert is_transient_db_error(b) is True


# ─── the retry wrapper ────────────────────────────────────────────────────

def test_retry_succeeds_on_the_second_attempt():
    calls = {"n": 0}

    async def _op():
        calls["n"] += 1
        if calls["n"] == 1:
            raise ConnectionDoesNotExistError(
                "connection was closed in the middle of operation"
            )
        return (19554, 123456)

    out = _run(run_with_db_retry(_op, attempts=3, base_delay=0.0))
    assert out == (19554, 123456)
    assert calls["n"] == 2


def test_retry_does_not_retry_a_permanent_error():
    calls = {"n": 0}

    async def _op():
        calls["n"] += 1
        raise IntegrityError("dup key")

    with pytest.raises(IntegrityError):
        _run(run_with_db_retry(_op, attempts=5, base_delay=0.0))
    assert calls["n"] == 1, "a permanent error must be raised on the first try"


def test_retry_reraises_after_the_last_attempt():
    calls = {"n": 0}

    async def _op():
        calls["n"] += 1
        raise InterfaceError("connection is closed")

    with pytest.raises(InterfaceError):
        _run(run_with_db_retry(_op, attempts=3, base_delay=0.0))
    assert calls["n"] == 3, "attempts must be bounded, and the error must surface"


def test_retry_takes_a_factory_not_a_coroutine():
    """A coroutine object can only be awaited once, and the whole point is
    a SECOND attempt on a FRESH session."""
    seen = []

    async def _op():
        seen.append(object())
        raise InterfaceError("connection is closed")

    with pytest.raises(InterfaceError):
        _run(run_with_db_retry(_op, attempts=2, base_delay=0.0))
    assert len(seen) == 2, "each attempt must call the factory again"


def test_on_retry_callback_failure_cannot_break_the_retry():
    calls = {"n": 0}

    async def _op():
        calls["n"] += 1
        if calls["n"] < 3:
            raise InterfaceError("connection is closed")
        return "ok"

    def _boom(_attempt, _exc):
        raise RuntimeError("logging blew up")

    assert _run(
        run_with_db_retry(_op, attempts=3, base_delay=0.0, on_retry=_boom)
    ) == "ok"


def test_db_error_label_is_bounded_and_names_the_class():
    label = db_error_label(ConnectionDoesNotExistError("x" * 900))
    assert label.startswith("ConnectionDoesNotExistError: ")
    assert len(label) < 400
