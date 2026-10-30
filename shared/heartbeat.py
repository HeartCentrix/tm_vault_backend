"""Worker heartbeat — liveness signal for the reconciliation sweeper.

Each worker spawns a single ``HeartbeatThread`` after AMQP connect and
before it starts consuming. The thread UPSERTs a row into
``worker_heartbeats`` every ``HEARTBEAT_INTERVAL_S`` seconds with the
current timestamp. The reconciler in ``shared/reconciler.py`` reads
those rows to decide whether a held lease belongs to a live worker
(safe to leave alone) or a dead one (sweep-eligible).

Stop-on-SIGTERM tombstones the row by writing ``last_seen_at = NOW() -
INTERVAL '1 hour'`` so the sweeper picks up orphans immediately instead
of waiting the full 60 s staleness window.

Liveness of the loop itself (2026-10-13 CNPG failover: 6 of 20
backup-worker pods had this task hung on a DB connect / UPDATE that
never returned): every beat is bounded, consecutive failures are
counted and logged, after ``HEARTBEAT_RESET_AFTER`` of them the shared
engine is disposed so a wedged pool is rebuilt, and after
``HEARTBEAT_FATAL_S`` seconds without a SUCCESSFUL beat the process
exits non-zero so the orchestrator restarts it — a worker whose own row
is stale is already being treated as dead by every sweep.

Design ref: docs/superpowers/specs/2026-05-16-distributed-reconciliation-design.md §5.
"""
from __future__ import annotations

import asyncio
import os
import sys
import time
import uuid
from typing import Iterable, Optional

from sqlalchemy import text

from shared.database import async_session_factory
from shared.lease import bounded_timeout_s, reset_db_engine, run_bounded


HEARTBEAT_INTERVAL_S = int(os.getenv("HEARTBEAT_INTERVAL_S", "10"))
# Consecutive failed / timed-out beats before the shared DB engine is
# disposed so a wedged pool is rebuilt (every Nth failure, so a pool
# that wedges again is rebuilt again). 0 disables the reset.
HEARTBEAT_RESET_AFTER = int(os.getenv("HEARTBEAT_RESET_AFTER", "6"))
# No SUCCESSFUL beat for this long ⇒ exit non-zero so Kubernetes
# restarts the pod. The row is stale by then, so the reconciler already
# treats every lease this process holds as dead; a process that cannot
# reach the database for this long is not doing useful work either.
# 0 disables the guard.
HEARTBEAT_FATAL_S = int(os.getenv("HEARTBEAT_FATAL_S", "300"))


def _fatal_exit(code: int = 1) -> None:
    """Terminate the process NOW.

    ``os._exit`` on purpose: the worker runs under ``asyncio.run`` and a
    SystemExit raised inside a task is routed through the worker's
    shutdown sequence (heartbeat tombstone, router shutdown) against the
    very database that is wedged, so the exit could itself hang.

    Unacked AMQP deliveries are returned to their queues by the broker
    when the connection drops and are redelivered to another replica;
    the stale heartbeat row and lapsing leases hand everything else to
    the reconciler. stdout/stderr are flushed first so the FATAL line
    reaches the pod log.
    """
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.flush()
        except Exception:
            pass
    os._exit(code)


def _resolve_replica_id() -> str:
    """Stable id across worker process restarts inside the same container.

    Railway exposes ``RAILWAY_REPLICA_ID``; the Compose / k8s case sets
    ``HOSTNAME``. Fallback is a fresh UUID — that breaks startup
    reclaim (we won't recognise our own old lease) but keeps the
    process bootable in dev / CI.
    """
    for k in ("RAILWAY_REPLICA_ID", "HOSTNAME", "POD_NAME"):
        v = os.getenv(k)
        if v:
            return v
    return str(uuid.uuid4())


class HeartbeatThread:
    def __init__(
        self,
        *,
        worker_id: str,
        service_name: str,
        queues: Iterable[str] = (),
        version: Optional[str] = None,
        interval_s: int = HEARTBEAT_INTERVAL_S,
        beat_timeout_s: Optional[float] = None,
        reset_after: int = HEARTBEAT_RESET_AFTER,
        fatal_s: float = HEARTBEAT_FATAL_S,
        engine_reset=None,
        exit_fn=None,
    ):
        # worker_id must be a UUID for the PK column; if the caller
        # passed a free-form string (e.g. "worker-3889f71f") we derive
        # a stable UUIDv5 from it so re-runs hit the same row.
        try:
            self.worker_uuid = uuid.UUID(worker_id)
        except (ValueError, TypeError):
            self.worker_uuid = uuid.uuid5(
                uuid.NAMESPACE_OID, f"tmvault-worker:{worker_id}"
            )
        self.worker_label = str(worker_id)
        self.replica_id = _resolve_replica_id()
        self.service_name = service_name
        self.queues = list(queues)
        self.pid = os.getpid()
        self.version = version or os.getenv("RAILWAY_GIT_COMMIT_SHA")
        self.interval_s = interval_s
        # Bound on one _beat_once round-trip (see shared.lease.run_bounded).
        self.beat_timeout_s = (
            float(beat_timeout_s) if beat_timeout_s is not None
            else bounded_timeout_s(interval_s)
        )
        self.reset_after = int(reset_after)
        self.fatal_s = float(fatal_s)
        self._engine_reset = engine_reset or reset_db_engine
        self._exit = exit_fn or _fatal_exit
        self.consecutive_failures = 0
        # time.monotonic() of the last beat that reached the DB; None until
        # the first one lands (the fatal clock then runs from loop start).
        self.last_success_at: Optional[float] = None
        self._loop_started_at: Optional[float] = None
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

    async def _beat_once(self) -> None:
        async with async_session_factory() as session:
            await session.execute(
                text(
                    """
                    INSERT INTO worker_heartbeats
                        (worker_id, replica_id, service_name, pid,
                         queues, last_seen_at, version)
                    VALUES
                        (cast(:wid AS uuid), :rep, :svc, :pid,
                         :queues, NOW(), :ver)
                    ON CONFLICT (worker_id) DO UPDATE
                       SET last_seen_at = NOW(),
                           replica_id   = EXCLUDED.replica_id,
                           service_name = EXCLUDED.service_name,
                           pid          = EXCLUDED.pid,
                           queues       = EXCLUDED.queues,
                           version      = EXCLUDED.version
                    """
                ),
                {
                    "wid": str(self.worker_uuid),
                    "rep": self.replica_id,
                    "svc": self.service_name,
                    "pid": self.pid,
                    "queues": self.queues,
                    "ver": self.version,
                },
            )
            await session.commit()

    async def _beat_bounded(self) -> bool:
        """One beat, bounded by ``beat_timeout_s``. Returns True when it
        reached the DB. Never raises (CancelledError excepted): a failed
        or hung beat is logged with its consecutive count, every
        ``reset_after``-th one disposes the engine, and the fatal guard
        decides whether the process is still worth keeping."""
        try:
            await run_bounded(self._beat_once(), self.beat_timeout_s)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.consecutive_failures += 1
            n = self.consecutive_failures
            kind = "timed out" if isinstance(exc, asyncio.TimeoutError) else "failed"
            print(
                f"[HEARTBEAT] beat {kind} ({n} consecutive): "
                f"{type(exc).__name__}: {exc}"
            )
            if self.reset_after > 0 and n % self.reset_after == 0:
                print(
                    f"[HEARTBEAT] {n} consecutive failed beats — "
                    f"resetting the DB engine"
                )
                try:
                    await self._engine_reset("HEARTBEAT")
                except Exception as rexc:
                    print(
                        f"[HEARTBEAT] engine reset raised: "
                        f"{type(rexc).__name__}: {rexc}"
                    )
            self._check_fatal()
            return False
        self.consecutive_failures = 0
        self.last_success_at = time.monotonic()
        return True

    def _check_fatal(self) -> None:
        """Exit the process once no beat has succeeded for ``fatal_s``
        seconds (``HEARTBEAT_FATAL_S=0`` opts out)."""
        if self.fatal_s <= 0:
            return
        anchor = (
            self.last_success_at if self.last_success_at is not None
            else self._loop_started_at
        )
        if anchor is None:
            return
        silent = time.monotonic() - anchor
        if silent < self.fatal_s:
            return
        print(
            f"[HEARTBEAT] FATAL: no successful beat for {int(silent)}s, "
            f"exiting (limit={int(self.fatal_s)}s, "
            f"consecutive_failures={self.consecutive_failures}) so the "
            f"orchestrator restarts this worker; unacked deliveries are "
            f"redelivered",
            flush=True,
        )
        self._exit(1)

    async def _loop(self) -> None:
        self._loop_started_at = time.monotonic()
        # First beat immediately so the row exists before any work is
        # claimed under this worker_id.
        await self._beat_bounded()
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.interval_s)
                break  # _stop was set
            except asyncio.TimeoutError:
                pass
            # Best-effort. A missed beat just shortens our liveness
            # window; the next tick recovers. Only the fatal guard
            # (no success for HEARTBEAT_FATAL_S) ends the process.
            await self._beat_bounded()

    async def start(self) -> None:
        if self._task is not None:
            return
        self._task = asyncio.create_task(self._loop(), name="heartbeat")
        print(
            f"[HEARTBEAT] started worker={self.worker_label} "
            f"replica={self.replica_id} svc={self.service_name} "
            f"every={self.interval_s}s queues={self.queues}"
        )

    async def stop(self) -> None:
        """Tombstone our row + cancel the loop. Best-effort."""
        self._stop.set()
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=2.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._task.cancel()
        try:
            # Bounded like a beat: a tombstone stuck on a dead socket must
            # not hold up the rest of the shutdown sequence.
            await run_bounded(self._tombstone_once(), self.beat_timeout_s)
        except Exception as exc:
            # Tombstone is best-effort — the staleness window
            # (HEARTBEAT_STALE_S) will catch us anyway.
            print(f"[HEARTBEAT] tombstone failed: {type(exc).__name__}: {exc}")

    async def _tombstone_once(self) -> None:
        async with async_session_factory() as session:
            await session.execute(
                text(
                    """
                    UPDATE worker_heartbeats
                       SET last_seen_at = NOW() - INTERVAL '1 hour'
                     WHERE worker_id = cast(:wid AS uuid)
                    """
                ),
                {"wid": str(self.worker_uuid)},
            )
            await session.commit()
