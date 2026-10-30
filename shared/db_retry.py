"""Transient-vs-permanent classification for database errors, plus a
bounded retry wrapper for short DB round trips.

Why this module exists (2026-08-11 02:20:50 prod): partition CHATS/4 of
snapshot 6807df97 ("Chats — Carlos Romero") was marked terminally FAILED
with files_uploaded=0 while all 19,554 of its messages across 176 of its
184 chats were already COMMITTED. Nothing was lost and nothing was even
still running: `_backup_user_content_parallel` had already returned. What
died was the shard's post-drain BOOKKEEPING query —

    SELECT COUNT(*), SUM(content_size) FROM snapshot_items WHERE ...

— with asyncpg ConnectionDoesNotExistError ("connection was closed in
the middle of operation"), a self-inflicted casualty of pool saturation
(`QueuePool limit of size 40 overflow 40 reached` was logged 3x against
the same snapshot minutes earlier). The handler's catch-all treated
"I could not COUNT what I already wrote" as "the shard failed".

That would have been a cosmetic ledger error if anything retried it. It
does not: `_mark_partition_terminal` makes FAILED a hard terminal state,
`_claim_partition` only claims status='QUEUED' or a stale 'IN_PROGRESS',
shared/reconciler.py STEP A requeues only ('QUEUED','IN_PROGRESS') and
its never-claimed / poison-pill statements exclude FAILED too, and the
backup-scheduler stale-sweep is `WHERE status = 'IN_PROGRESS'` in both
of its arms. One dropped TCP connection therefore cost a shard
permanently.

The worker had no way to tell a dropped connection from a real failure:
its only classifier (`_classify_chat_drain_error`) is Graph-level and is
not consulted on this path. This module is that missing classifier.

Deliberately dependency-free — asyncpg / SQLAlchemy are matched by
exception CLASS NAME and message text rather than by isinstance, so:
  * any service can import this without dragging a DB driver into its
    import graph (the api-gateway and the schedulers do not all install
    the same extras), and
  * a driver upgrade that reshuffles its exception hierarchy (asyncpg
    has done this twice) degrades to "unrecognised → permanent", which
    is the safe direction: a permanent classification keeps today's
    behaviour, a false "transient" would retry a poison query forever.

Pinned by tests/shared/test_db_retry_transient_classification.py.
"""
from __future__ import annotations

import asyncio
from typing import Any, Optional

# Connection-layer / resource-layer failures. Every one of these means
# "this particular connection or pool slot is unusable right now", never
# "this statement is wrong".
#
#   asyncpg:    ConnectionDoesNotExistError, InterfaceError,
#               ConnectionFailureError, PostgresConnectionError,
#               TooManyConnectionsError, CannotConnectNowError,
#               ConnectionRejectionError (all raised as themselves when
#               the worker talks to asyncpg directly)
#   SQLAlchemy: OperationalError / InterfaceError are the DBAPIError
#               wrappers the asyncpg dialect raises for exactly this
#               category (class NAMES collide with asyncpg's on purpose
#               — matching by name covers both layers with one entry),
#
# NOT listed on purpose: TimeoutError / ConnectionResetError. Both are
# builtins that any socket in the process can raise (Graph, blob
# storage), and this predicate is also consulted on paths where a
# non-DB exception can arrive — a Graph read timeout must keep its
# existing "permanent for this attempt" handling. QueuePool's
# sqlalchemy.exc.TimeoutError is still caught, by its unmistakable
# "QueuePool limit of size N overflow N reached" message below; pool
# exhaustion is transient by definition, the fix being to come back
# once a sibling shard has let its slot go.
_TRANSIENT_DB_EXC_NAMES = frozenset({
    "ConnectionDoesNotExistError",
    "ConnectionFailureError",
    "ConnectionRejectionError",
    "CannotConnectNowError",
    "TooManyConnectionsError",
    "PostgresConnectionError",
    "InterfaceError",
    "OperationalError",
    "DisconnectionError",
    "InvalidCachedStatementError",
})

# Messages that identify the same category even when the class is
# generic (asyncpg raises a bare InterfaceError for several of these,
# and SQLAlchemy re-wraps a couple as plain DBAPIError).
_TRANSIENT_DB_MSG_PATTERNS = (
    "connection was closed in the middle of operation",
    "connection is closed",
    "the connection is closed",
    "connection already closed",
    "server closed the connection unexpectedly",
    "terminating connection due to administrator command",
    "terminating connection due to unexpected postmaster exit",
    "queuepool limit of size",
    "connection pool is closed",
    "ssl connection has been closed unexpectedly",
    "cannot perform operation: another operation is in progress",
    "canceling statement due to statement timeout",
    "too many connections for role",
    "the database system is starting up",
    "the database system is shutting down",
)

# Never retry these, whatever else the chain says: the statement, the
# schema or the data is wrong and a second attempt fails identically.
# Checked FIRST, because SQLAlchemy nests an IntegrityError inside an
# OperationalError-shaped chain on some driver paths.
_PERMANENT_DB_EXC_NAMES = frozenset({
    "IntegrityError",
    "UniqueViolationError",
    "ForeignKeyViolationError",
    "NotNullViolationError",
    "CheckViolationError",
    "ProgrammingError",
    "UndefinedColumnError",
    "UndefinedTableError",
    "UndefinedFunctionError",
    "SyntaxOrAccessError",
    "PostgresSyntaxError",
    "DataError",
    "InvalidTextRepresentationError",
    "NumericValueOutOfRangeError",
    "StringDataRightTruncationError",
    "InsufficientPrivilegeError",
    "InvalidPasswordError",
    "InvalidCatalogNameError",
    "InvalidAuthorizationSpecificationError",
})

_MAX_CAUSE_DEPTH = 8


def _chain(exc: Optional[BaseException]) -> list:
    """Flatten an exception and everything it wraps.

    SQLAlchemy wraps the driver error twice on the async path
    (DBAPIError.orig → asyncpg error, plus __cause__), so the class name
    that actually identifies the failure can be two or three links down.
    Bounded + visited-set so a self-referential __context__ can't spin.
    """
    out: list = []
    seen: set = set()
    queue = [exc]
    while queue and len(out) < _MAX_CAUSE_DEPTH:
        cur = queue.pop(0)
        if cur is None or id(cur) in seen:
            continue
        seen.add(id(cur))
        out.append(cur)
        for nxt in (
            getattr(cur, "orig", None),      # SQLAlchemy DBAPIError
            cur.__cause__,
            cur.__context__,
        ):
            if isinstance(nxt, BaseException) and id(nxt) not in seen:
                queue.append(nxt)
    return out


def is_transient_db_error(exc: Optional[BaseException]) -> bool:
    """True when ``exc`` is a database failure that a retry can fix.

    Transient = connection dropped / pool exhausted / server cycling.
    Everything unrecognised is reported as PERMANENT on purpose (see the
    module docstring): the caller's existing behaviour is preserved when
    we do not understand the error.
    """
    if exc is None:
        return False
    links = _chain(exc)
    names = {link.__class__.__name__ for link in links}
    # Permanent wins over transient — an IntegrityError nested under an
    # OperationalError-shaped wrapper is still an IntegrityError.
    if names & _PERMANENT_DB_EXC_NAMES:
        return False
    if names & _TRANSIENT_DB_EXC_NAMES:
        return True
    # SQLAlchemy marks a connection it had to throw away. Only trust it
    # when it is actually True (the attribute is False on every
    # statement-level error).
    for link in links:
        if getattr(link, "connection_invalidated", False) is True:
            return True
    haystack = " ".join(str(link) for link in links).lower()
    return any(p in haystack for p in _TRANSIENT_DB_MSG_PATTERNS)


def db_error_label(exc: Optional[BaseException]) -> str:
    """Short ``ClassName: message`` label for logs / failure_state.

    Truncated because it gets written into a JSONB column that ops read
    in a terminal, and asyncpg's messages can carry a whole query.
    """
    if exc is None:
        return ""
    return f"{exc.__class__.__name__}: {str(exc)[:300]}"


async def run_with_db_retry(
    op: Any,
    *,
    attempts: int = 3,
    base_delay: float = 0.25,
    max_delay: float = 4.0,
    label: str = "db",
    on_retry: Any = None,
) -> Any:
    """Await ``op()`` again while it keeps failing TRANSIENTLY.

    ``op`` is a zero-arg callable returning an awaitable — a factory, not
    a coroutine object, because a coroutine can only be awaited once and
    the whole point here is a second attempt. Callers must therefore
    pass ``lambda: session_scoped_query()``, and the factory must open
    its OWN session: the connection the first attempt used is exactly
    what is broken.

    Non-transient errors and the final attempt re-raise unchanged, so
    this never converts a real failure into silence. ``on_retry(attempt,
    exc)`` (sync) is called before each sleep for logging.

    Backoff is deliberately sub-second-first: the failure mode this was
    written for (pool exhaustion while 40 sibling shards drain) clears
    as soon as one sibling commits, and these are single-row COUNT
    queries — nothing here is worth a 30 s ladder.
    """
    if attempts < 1:
        attempts = 1
    last: Optional[BaseException] = None
    for attempt in range(1, attempts + 1):
        try:
            return await op()
        except Exception as exc:  # noqa: BLE001 - re-raised below
            last = exc
            if attempt >= attempts or not is_transient_db_error(exc):
                raise
            delay = min(max_delay, base_delay * (2 ** (attempt - 1)))
            if on_retry is not None:
                try:
                    on_retry(attempt, exc)
                except Exception:
                    pass
            await asyncio.sleep(delay)
    # Unreachable: the loop either returns or raises.
    raise last  # type: ignore[misc]


def is_retryable_catalogue_transaction_error(exc: Optional[BaseException]) -> bool:
    """Only deadlock/serialization SQLSTATEs permit a full catalogue retry.

    The first database code wins; an already-aborted transaction cannot inherit
    retry eligibility from an earlier error left in its exception context.
    """
    for link in _chain(exc):
        code = getattr(link, "sqlstate", None) or getattr(link, "pgcode", None)
        if code:
            return code in {"40P01", "40001"}
    return False


async def run_catalogue_transaction(session_factory, operation, attempts=3):
    """Run a DB-only operation in a fresh session/transaction on every attempt.

    Never pass Graph, storage or other externally side-effecting operations.
    Existing generic retry classification and callers remain unchanged.
    """
    if type(attempts) is not int or attempts < 1:
        raise ValueError("attempts must be a positive integer")
    for number in range(attempts):
        try:
            async with session_factory() as session:
                async with session.begin():
                    return await operation(session)
        except Exception as exc:
            if number + 1 == attempts or not is_retryable_catalogue_transaction_error(exc):
                raise
            await asyncio.sleep(min(0.5, 0.05 * 2 ** min(number, 4)))
