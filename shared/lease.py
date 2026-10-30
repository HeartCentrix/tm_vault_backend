"""Atomic lease claim + renewal with fence tokens.

A *lease* is a (lease_owner_id, lease_expires_at, lease_token) triple
stored on every work row (``jobs``, ``snapshots``, ``snapshot_partitions``).
The fence token is a monotonic counter — bumped on every claim and on
every reconciler sweep — that defends against a resurrected stale
worker writing over fresh state. Any state UPDATE the worker issues
carries ``WHERE lease_token = :my_token``, so a worker whose lease was
re-taken writes 0 rows and aborts cleanly.

Design ref: docs/superpowers/specs/2026-05-16-distributed-reconciliation-design.md §6.

This module is the SQL primitives only — wiring into the per-handler
message path is done by the worker (see ``with_lease`` context manager).
"""
from __future__ import annotations

import asyncio
import contextlib
import os
import threading
import uuid
from dataclasses import dataclass
from typing import Optional

from sqlalchemy import text


# 5k-user prod scale: CHATS partitions with 403 backoffs and heavy
# OneDrive shards routinely run 5+ minutes per partition. Renew at
# TTL/5 so a single missed tick (network blip, DB stall, pool wait)
# does not orphan a live worker's lease to the sweeper.
LEASE_TTL_S = int(os.getenv("LEASE_TTL_S", "600"))
LEASE_RENEW_S = int(os.getenv("LEASE_RENEW_S", "120"))

# Liveness of the renewal / heartbeat loops themselves. 2026-10-13 CNPG
# failover: 6 of 20 backup-worker pods sat on a DB connect / UPDATE that
# never returned (black-holed socket to the old primary) with no timeout
# around it, so their heartbeat task — and in 3 of them the LeaseExtender
# loop too — hung for good. Every round-trip is now bounded, and after
# this many consecutive failures the shared engine is disposed so a
# wedged pool is rebuilt (every Nth failure, so a pool that wedges again
# is rebuilt again). 0 disables the reset.
LEASE_EXTENDER_RESET_AFTER = int(os.getenv("LEASE_EXTENDER_RESET_AFTER", "6"))
ENGINE_RESET_TIMEOUT_S = float(os.getenv("ENGINE_RESET_TIMEOUT_S", "30"))


def bounded_timeout_s(interval_s: float) -> float:
    """Per-call bound for a periodic DB round-trip: twice the period and
    never under 10 s (a 10 s heartbeat gets 20 s, a 120 s renew 240 s)."""
    return max(2.0 * float(interval_s), 10.0)


def _retrieve_task_result(task) -> None:
    # An abandoned task that eventually fails must not be reported by the
    # loop as "Task exception was never retrieved".
    if not task.cancelled():
        task.exception()


async def run_bounded(coro, timeout_s: float):
    """Await ``coro`` for at most ``timeout_s`` seconds.

    ``asyncio.wait_for`` with one deliberate difference: on timeout the
    inner task is cancelled but NOT awaited. ``wait_for`` waits for the
    cancelled task to finish unwinding, and when the hung call is a DB
    round-trip on a black-holed socket the unwinding (session close →
    ROLLBACK on that same socket) is the very hang being defended
    against. The abandoned task finishes on its own or leaks with its
    connection; ``reset_db_engine`` after repeated failures rebuilds the
    pool underneath it. Raises ``asyncio.TimeoutError`` on timeout and
    re-raises whatever ``coro`` raised otherwise.
    """
    task = asyncio.ensure_future(coro)
    try:
        return await asyncio.wait_for(asyncio.shield(task), timeout=timeout_s)
    except asyncio.TimeoutError:
        task.cancel()
        task.add_done_callback(_retrieve_task_result)
        raise
    except asyncio.CancelledError:
        task.cancel()
        task.add_done_callback(_retrieve_task_result)
        raise


async def reset_db_engine(tag: str, timeout_s: float = ENGINE_RESET_TIMEOUT_S) -> bool:
    """Dispose the shared SQLAlchemy engine so its pool is rebuilt on the
    next checkout. ``AsyncEngine.dispose()`` closes checked-in connections
    and recreates the pool; connections still checked out (the hung
    calls) stay with their abandoned tasks. Closing a black-holed
    connection can itself block, so the close is bounded and falls back
    to dereferencing the old pool without closing it (``close=False``,
    no I/O). Returns True when a fresh pool is in place."""
    try:
        from shared.database import engine
    except Exception as exc:
        print(f"[{tag}] engine reset unavailable: {type(exc).__name__}: {exc}")
        return False
    try:
        await run_bounded(engine.dispose(), timeout_s)
        print(f"[{tag}] engine disposed — connection pool rebuilt")
        return True
    except Exception as exc:
        print(
            f"[{tag}] engine dispose {type(exc).__name__}: {exc} — "
            f"dereferencing the pool without closing it"
        )
    try:
        await run_bounded(engine.dispose(close=False), timeout_s)
        print(f"[{tag}] engine pool dereferenced — fresh pool on next checkout")
        return True
    except Exception as exc:
        print(f"[{tag}] engine reset failed: {type(exc).__name__}: {exc}")
        return False


# Tables that carry lease columns. Hard-coded set guards against
# accidental call against an unrelated table.
_LEASED_TABLES = frozenset({"jobs", "snapshots", "snapshot_partitions"})


@dataclass(frozen=True)
class Lease:
    """A held lease. ``token`` is the fence value to be carried in
    every subsequent write."""
    table: str
    work_id: str
    owner_id: str
    token: int

    def fence_clause(self) -> str:
        """SQL fragment for state writes:
            ``AND lease_token = :__lease_token``
        Bind ``__lease_token`` separately in the params dict."""
        return "AND lease_token = :__lease_token"

    def fence_params(self) -> dict:
        return {"__lease_token": self.token}


def _check_table(table: str) -> None:
    if table not in _LEASED_TABLES:
        raise ValueError(f"lease: unsupported table {table!r}")


async def claim(
    session,
    *,
    table: str,
    work_id,
    owner_id: str,
    in_progress_status: str,
    ttl_seconds: int = LEASE_TTL_S,
) -> Optional[Lease]:
    """Atomically claim work row ``work_id`` in ``table``.

    Returns a ``Lease`` on success, ``None`` if another live worker
    already holds it.

    ``in_progress_status`` differs per table:
       - jobs: 'RUNNING'
       - snapshots: 'IN_PROGRESS'
       - snapshot_partitions: 'IN_PROGRESS'

    Race semantics: the UPDATE filters on ``(lease_expires_at IS NULL OR
    lease_expires_at < NOW())`` so a held-and-fresh lease cannot be
    stolen. The token is incremented in the same UPDATE so the loser
    of the race never sees a usable token.
    """
    _check_table(table)
    sql = text(
        f"""
        UPDATE {table}
           SET lease_owner_id   = cast(:owner AS uuid),
               lease_expires_at = NOW() + (:ttl * INTERVAL '1 second'),
               lease_token      = lease_token + 1
         WHERE id = cast(:wid AS uuid)
           AND status::text = :in_prog
           AND (lease_expires_at IS NULL OR lease_expires_at < NOW())
         RETURNING lease_token
        """
    )
    row = (
        await session.execute(
            sql,
            {
                "owner": str(owner_id),
                "ttl": ttl_seconds,
                "wid": str(work_id),
                "in_prog": in_progress_status,
            },
        )
    ).first()
    if not row:
        return None
    await session.commit()
    return Lease(
        table=table,
        work_id=str(work_id),
        owner_id=str(owner_id),
        token=int(row.lease_token),
    )


async def renew(
    session,
    lease: Lease,
    *,
    ttl_seconds: int = LEASE_TTL_S,
) -> bool:
    """Extend the lease. Returns True if still ours; False if stolen
    (fence mismatch or row was already finalized by the reconciler)."""
    _check_table(lease.table)
    row = (
        await session.execute(
            text(
                f"""
                UPDATE {lease.table}
                   SET lease_expires_at = NOW() + (:ttl * INTERVAL '1 second')
                 WHERE id = cast(:wid AS uuid)
                   AND lease_owner_id = cast(:owner AS uuid)
                   AND lease_token = :tok
                 RETURNING id
                """
            ),
            {
                "wid": lease.work_id,
                "owner": lease.owner_id,
                "tok": lease.token,
                "ttl": ttl_seconds,
            },
        )
    ).first()
    if row:
        await session.commit()
        return True
    return False


async def release(session, lease: Lease) -> None:
    """Clear the lease without changing status. Called after a clean
    finalize; the status write already advanced the row out of the
    IN_PROGRESS branch."""
    _check_table(lease.table)
    await session.execute(
        text(
            f"""
            UPDATE {lease.table}
               SET lease_owner_id   = NULL,
                   lease_expires_at = NULL
             WHERE id = cast(:wid AS uuid)
               AND lease_token = :tok
            """
        ),
        {"wid": lease.work_id, "tok": lease.token},
    )
    await session.commit()


class LeaseRenewer:
    """Async context manager: spawns a background task that calls
    ``renew(lease)`` every ``LEASE_RENEW_S`` seconds. If the renewer
    detects fence-loss it sets ``stolen`` and stops — handler should
    check ``renewer.stolen`` before issuing terminal writes.

    Usage::

        lease = await claim(session, table="snapshots", work_id=sid, ...)
        if lease is None:
            return  # another worker has it
        async with LeaseRenewer(lease, session_factory=async_session_factory):
            ...do work...
            await session.execute(
                text("UPDATE snapshots SET status='COMPLETED' WHERE id=:sid "
                     + lease.fence_clause()),
                {"sid": sid, **lease.fence_params()},
            )
    """

    def __init__(
        self,
        lease: Lease,
        *,
        session_factory,
        interval_s: int = LEASE_RENEW_S,
    ):
        self.lease = lease
        self._session_factory = session_factory
        self.interval_s = interval_s
        self.stolen = False
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

    async def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.interval_s)
                return
            except asyncio.TimeoutError:
                pass
            try:
                async with self._session_factory() as session:
                    ok = await renew(session, self.lease)
                if not ok:
                    self.stolen = True
                    print(
                        f"[LEASE] stolen on {self.lease.table}/"
                        f"{self.lease.work_id} (token={self.lease.token})"
                    )
                    return
            except Exception as exc:
                # Renewal failure is non-fatal — TTL is generous enough
                # to survive a single skipped tick. The sweeper will
                # take over if we lose enough cycles to expire.
                print(f"[LEASE] renew failed: {exc}")

    async def __aenter__(self) -> "LeaseRenewer":
        self._task = asyncio.create_task(self._loop(), name="lease-renew")
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self._stop.set()
        if self._task is not None:
            with contextlib.suppress(asyncio.CancelledError, asyncio.TimeoutError):
                await asyncio.wait_for(self._task, timeout=2.0)


def new_owner_uuid() -> uuid.UUID:
    """Convenience for callers that need a fresh owner id (typically
    derived once per worker at boot, mirroring the heartbeat row)."""
    return uuid.uuid4()


# ── Handler-scoped renewal registry ──────────────────────────────────
# Which rows a LIVE handler in this process is working on, per table.
# Claim sites register; terminal writes, releases and exception exits
# unregister. Once anything has registered in this process the
# LeaseExtender renews ONLY registered rows — until then it keeps the
# owner-wide renewal, so other LeaseExtender users are unaffected.
#
# Why: 2026-10-14 DB outage — handlers aborted with their terminal write
# AND their release lost ("row will strand until this pod exits"), but
# the owner-wide renewal kept every one of those rows leased for as long
# as the pod lived, so the reconciler (which needs an expired lease)
# could never recover them: 5 RUNNING jobs / 9 IN_PROGRESS snapshots
# were zombies for 12 h. A renewal that only covers rows a live handler
# is still working on lets a dead handler's row lapse within LEASE_TTL_S.
_registry_lock = threading.Lock()
_registry: dict = {t: set() for t in _LEASED_TABLES}
_registry_used = False


def _row_key(row_id) -> str:
    # Canonical form so a UUID object, its str() and a JSON-carried
    # string all name the same row.
    try:
        return str(uuid.UUID(str(row_id)))
    except (ValueError, TypeError, AttributeError):
        return str(row_id)


def register(table: str, row_id) -> None:
    """Record that a handler in this process holds the lease on ``row_id``
    in ``table`` — the LeaseExtender keeps it alive from here on."""
    global _registry_used
    _check_table(table)
    with _registry_lock:
        _registry[table].add(_row_key(row_id))
        _registry_used = True


def unregister(table: str, row_id) -> None:
    """Forget ``row_id`` (idempotent). Call on the terminal write, on
    release, and on exception exit — a row nobody is working on must
    lapse to the reconciler, never be kept alive by this process."""
    _check_table(table)
    with _registry_lock:
        _registry[table].discard(_row_key(row_id))


def registered_ids(table: str) -> list:
    """Snapshot of the rows registered for ``table`` (sorted)."""
    _check_table(table)
    with _registry_lock:
        return sorted(_registry[table])


def registry_active() -> bool:
    """True once anything has registered in this process; the
    owner-wide fallback renewal is only used while this is False."""
    with _registry_lock:
        return _registry_used


def _reset_registry() -> None:
    """Test hook: back to the never-populated state."""
    global _registry_used
    with _registry_lock:
        for ids in _registry.values():
            ids.clear()
        _registry_used = False


class LeaseExtender:
    """Background task: bump ``lease_expires_at`` every ``interval_s``
    seconds on every row that this worker currently owns across
    ``jobs``, ``snapshots``, and ``snapshot_partitions``.

    Cheaper than a per-handler ``LeaseRenewer`` because one query per
    table covers however many rows the worker holds. Loses fence-token
    precision — if the reconciler bumps a token mid-renew the renewer
    can't tell which specific row was stolen — but the eventual write
    from the actual handler still carries the lease_token it stamped
    at claim time, so a stolen row's terminal write fails as expected.

    Started once per worker after heartbeat init. Stopped on shutdown.
    """

    def __init__(
        self,
        *,
        owner_id: uuid.UUID,
        session_factory,
        interval_s: int = LEASE_RENEW_S,
        ttl_seconds: int = LEASE_TTL_S,
        bump_timeout_s: Optional[float] = None,
        reset_after: int = LEASE_EXTENDER_RESET_AFTER,
        engine_reset=None,
    ):
        self.owner_id = str(owner_id)
        self._session_factory = session_factory
        self.interval_s = interval_s
        self.ttl_seconds = ttl_seconds
        # Bound on one _bump_once round-trip (see run_bounded).
        self.bump_timeout_s = (
            float(bump_timeout_s) if bump_timeout_s is not None
            else bounded_timeout_s(interval_s)
        )
        self.reset_after = int(reset_after)
        self._engine_reset = engine_reset or reset_db_engine
        self.consecutive_failures = 0
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

    async def _bump_once(self) -> None:
        # Handler-scoped once anything has registered in this process
        # (see the registry above); owner-wide until then.
        scoped = registry_active()
        async with self._session_factory() as session:
            for table, in_prog in (
                ("snapshots", "IN_PROGRESS"),
                ("snapshot_partitions", "IN_PROGRESS"),
                ("jobs", "RUNNING"),
            ):
                if not scoped:
                    await session.execute(
                        text(
                            f"""
                            UPDATE {table}
                               SET lease_expires_at = NOW() + (:ttl * INTERVAL '1 second')
                             WHERE lease_owner_id = cast(:owner AS uuid)
                               AND status::text = :sts
                            """
                        ),
                        {"owner": self.owner_id, "ttl": self.ttl_seconds, "sts": in_prog},
                    )
                    continue
                ids = registered_ids(table)
                if not ids:
                    continue
                renewed = (await session.execute(
                    text(
                        f"""
                        UPDATE {table}
                           SET lease_expires_at = NOW() + (:ttl * INTERVAL '1 second')
                         WHERE lease_owner_id = cast(:owner AS uuid)
                           AND status::text = :sts
                           AND id = ANY(cast(:ids AS uuid[]))
                     RETURNING id
                        """
                    ),
                    {
                        "owner": self.owner_id,
                        "ttl": self.ttl_seconds,
                        "sts": in_prog,
                        "ids": ids,
                    },
                )).all()
                kept = {str(r[0]) for r in renewed}
                for rid in ids:
                    if rid not in kept:
                        # Terminal, released, or re-leased by someone
                        # else: nothing left for this process to keep
                        # alive, and a forgotten unregister must not
                        # grow the set forever.
                        unregister(table, rid)
            await session.commit()

    async def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.interval_s)
                return
            except asyncio.TimeoutError:
                pass
            try:
                await run_bounded(self._bump_once(), self.bump_timeout_s)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.consecutive_failures += 1
                n = self.consecutive_failures
                kind = "timed out" if isinstance(exc, asyncio.TimeoutError) else "failed"
                print(
                    f"[LEASE_EXTENDER] bump {kind} ({n} consecutive): "
                    f"{type(exc).__name__}: {exc}"
                )
                if self.reset_after > 0 and n % self.reset_after == 0:
                    print(
                        f"[LEASE_EXTENDER] {n} consecutive failed bumps — "
                        f"resetting the DB engine"
                    )
                    try:
                        await self._engine_reset("LEASE_EXTENDER")
                    except Exception as rexc:
                        print(
                            f"[LEASE_EXTENDER] engine reset raised: "
                            f"{type(rexc).__name__}: {rexc}"
                        )
                continue
            self.consecutive_failures = 0

    async def start(self) -> None:
        if self._task is not None:
            return
        self._task = asyncio.create_task(self._loop(), name="lease-extender")
        print(
            f"[LEASE_EXTENDER] started owner={self.owner_id} "
            f"interval={self.interval_s}s ttl={self.ttl_seconds}s"
        )

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            with contextlib.suppress(asyncio.CancelledError, asyncio.TimeoutError):
                await asyncio.wait_for(self._task, timeout=2.0)
