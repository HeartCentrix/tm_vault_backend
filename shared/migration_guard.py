"""Guards for migrations that touch hot tables during an online upgrade (sync alembic connection).

A DDL statement waiting for its table lock queues every later reader/writer behind it, so one
idle-in-transaction session can stall the whole fleet. lock_timeout() bounds that wait
(MIGRATION_LOCK_TIMEOUT, default 10s) and fails with what the operator should do.
"""
import os
from contextlib import contextmanager

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError


@contextmanager
def lock_timeout(conn, what):
    timeout = os.environ.get("MIGRATION_LOCK_TIMEOUT", "10s")
    previous = conn.scalar(text("SELECT current_setting('lock_timeout')"))
    conn.execute(text("SELECT set_config('lock_timeout', :t, true)"), {"t": timeout})
    try:
        yield
    except DBAPIError as exc:
        if "lock timeout" not in str(exc):
            raise
        raise RuntimeError(
            f"{what}: table lock not granted within MIGRATION_LOCK_TIMEOUT={timeout}. Nothing was "
            "changed by this step; re-run the migration. Before retrying, look for sessions holding "
            "locks, usually idle in transaction: SELECT pid, state, xact_start, left(query, 80) FROM "
            "pg_stat_activity WHERE state LIKE 'idle in transaction%' OR wait_event_type = 'Lock'; "
            "end them (pg_terminate_backend(pid)) or retry in a quieter window / raise MIGRATION_LOCK_TIMEOUT."
        ) from exc
    conn.execute(text("SELECT set_config('lock_timeout', :t, true)"), {"t": previous})


def require_valid_index(conn, name, revision):
    """CREATE INDEX CONCURRENTLY IF NOT EXISTS keeps an INVALID index an interrupted build left."""
    if conn.scalar(text("SELECT NOT indisvalid FROM pg_index WHERE indexrelid = to_regclass(:n)"), {"n": name}):
        raise RuntimeError(
            f"Index {name} exists but is INVALID (an earlier concurrent build was interrupted). "
            f"Run: DROP INDEX CONCURRENTLY {name}; then re-run migration {revision}."
        )
