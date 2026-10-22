"""Helpers services call from their FastAPI lifespan (or worker main())
to load the StorageRouter at startup and close it at shutdown.

Safe to call before Phase 2 migrations have applied — router.load()
logs a warning and no-ops when storage_backends/system_config tables
don't exist yet.

Boot-race resilience: in compose / k8s startups the DB hostname can
fail DNS for the first few seconds while postgres' init scripts run.
We retry with exponential backoff so workers don't end up in a silent
"no-router" zombie state where OneDrive metadata is written but blobs
are never uploaded.

2026-10-13 incident: nine backup-worker pods booted while the CNPG
primary was failing over (node reboots), exhausted the 12 bounded
attempts, logged "gave up" and then consumed backup work for an hour
with NO active backend. Blob uploads raised "No storage shards
configured" (visible FAILED jobs) and the OneDrive handler completed
snapshots whose ONEDRIVE_FILE rows never got a blob (invisible data
loss). Two guarantees now hold:

  * exhaustion is not terminal — a background task keeps retrying with
    capped backoff until the router loads; ``router_loaded()`` and
    ``wait_for_router()`` expose the state;
  * ``startup_router(required=True)`` (or STORAGE_ROUTER_REQUIRED=true)
    blocks until the router is loaded, so a worker can never start
    consuming work without a storage backend.
"""
from __future__ import annotations

import asyncio
import logging
import os
from typing import Optional

from shared.storage.router import router

log = logging.getLogger("tmvault.storage.startup")

# Module state. The event is created lazily so it binds to the running
# loop; the background task is a singleton so repeated startup calls
# (tests, re-entrant lifespans) never stack retry loops.
_loaded_event: Optional[asyncio.Event] = None
_bg_task: Optional[asyncio.Task] = None


def _build_dsn() -> str:
    """Build the DSN the router uses. Prefers DATABASE_URL; otherwise
    assembles from DB_HOST/DB_PORT/DB_USERNAME/DB_PASSWORD/DB_NAME the
    same way shared/database.py does."""
    dsn = os.getenv("DATABASE_URL")
    if dsn:
        return dsn
    # ITO-9652: the router holds a LISTEN connection, which needs a stable
    # session-scoped backend. Prefer DB_DIRECT_HOST (the CNPG primary/RW
    # service) so LISTEN bypasses the pgbouncer transaction-mode pooler
    # entirely; fall back to DB_HOST when unset (compose/dev, or any
    # deployment that hasn't set up the pooler split).
    host = os.getenv("DB_DIRECT_HOST") or os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    user = os.getenv("DB_USERNAME", os.getenv("DB_USER", "postgres"))
    pw = os.getenv("DB_PASSWORD", "")
    db = os.getenv("DB_NAME", "postgres")
    return f"postgresql://{user}:{pw}@{host}:{port}/{db}"


def _event() -> asyncio.Event:
    global _loaded_event
    if _loaded_event is None:
        _loaded_event = asyncio.Event()
    return _loaded_event


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def router_loaded() -> bool:
    """True once the router has an active backend (i.e. load() succeeded)."""
    return bool(router.active_backend_id())


async def _attempt_load() -> None:
    try:
        await router.load(db_dsn=_build_dsn())
    except BaseException:
        # load() can fail AFTER asyncpg.connect() succeeded (catalogue
        # query against a failing-over primary). close() drops the
        # half-open connection so a long retry loop can't leak one
        # server connection per attempt.
        try:
            await router.close()
        except Exception:
            pass
        raise


def _describe(router_obj) -> str:
    try:
        return (
            f"{len(router_obj.list_backends())} backend(s), "
            f"active={router_obj.active_backend_id()}, "
            f"state={router_obj.transition_state()}"
        )
    except Exception:
        return "state unavailable"


async def _background_retry_loop() -> None:
    """Keep trying to load the router until it succeeds. Never gives up;
    cancelled by shutdown_router()."""
    base = float(os.getenv("STORAGE_ROUTER_BG_RETRY_S", "5.0"))
    cap = float(os.getenv("STORAGE_ROUTER_BG_RETRY_CAP_S", "30.0"))
    delay = max(0.0, base)
    attempt = 0
    while True:
        await asyncio.sleep(delay)
        attempt += 1
        try:
            await _attempt_load()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            # First failure and then every 10th, so an hour-long outage
            # is visible without flooding the log.
            if attempt == 1 or attempt % 10 == 0:
                log.warning(
                    "storage router background load attempt %d failed "
                    "(%s: %s); next attempt in %.0fs",
                    attempt, type(e).__name__, e, min(cap, delay * 2) if delay else base,
                )
            delay = min(cap, delay * 2) if delay else min(cap, base)
            continue
        # WARNING on purpose: workers run with INFO hidden and operators
        # must be able to see that a degraded boot has self-healed.
        log.warning(
            "storage router loaded by background retry on attempt %d: %s",
            attempt, _describe(router),
        )
        _event().set()
        return


def _ensure_background_retry() -> None:
    global _bg_task
    if _bg_task is not None and not _bg_task.done():
        return
    _bg_task = asyncio.create_task(_background_retry_loop())


async def wait_for_router(timeout: Optional[float] = None) -> bool:
    """Block until the router is loaded. Returns False only on timeout."""
    if router_loaded():
        _event().set()
        return True
    try:
        await asyncio.wait_for(_event().wait(), timeout)
    except asyncio.TimeoutError:
        return False
    return True


async def startup_router(required: Optional[bool] = None) -> bool:
    """Load the storage router, retrying on transient connection errors.

    Boot order is not guaranteed between containers in compose — postgres
    may take 10-30s to be ready. Without retries we silently degrade to
    "no router" and the worker's fallback path uploads to a stale Azure
    account / raw shard that doesn't have the `upload_blob_stream` facade.
    That manifests as OneDrive files persisted with metadata only and
    zero blob bytes — data loss disguised as a healthy snapshot.

    Returns True when the router is loaded on return, False when the
    bounded attempts were exhausted and loading continues in the
    background (only possible with required=False).

    ``required`` — block until the router is loaded instead of returning
    False. Workers pass True: they must never consume work without a
    backend. API services keep the default so their HTTP health/readiness
    endpoints come up and the background retry heals them. Defaults to
    the STORAGE_ROUTER_REQUIRED env flag (false).

    Knobs:
      STORAGE_ROUTER_MAX_RETRIES (default 12) — bounded attempts
      STORAGE_ROUTER_BACKOFF_BASE_S (default 1.0)
      STORAGE_ROUTER_BACKOFF_CAP_S (default 10.0)
      STORAGE_ROUTER_BG_RETRY_S / _CAP_S (default 5 / 30) — background
        retry cadence after the bounded attempts are exhausted
      STORAGE_ROUTER_REQUIRED (default false) — gate startup on load
    Worst case before the background loop takes over: ~110s with defaults.
    """
    if required is None:
        required = _env_flag("STORAGE_ROUTER_REQUIRED", False)
    max_retries = int(os.getenv("STORAGE_ROUTER_MAX_RETRIES", "12"))
    base = float(os.getenv("STORAGE_ROUTER_BACKOFF_BASE_S", "1.0"))
    cap = float(os.getenv("STORAGE_ROUTER_BACKOFF_CAP_S", "10.0"))
    last_err: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            await _attempt_load()
        except Exception as e:
            last_err = e
            if attempt >= max_retries:
                break
            sleep_s = min(cap, base * (2 ** (attempt - 1)))
            log.warning(
                "storage router load attempt %d/%d failed (%s); retrying in %.1fs",
                attempt, max_retries, type(e).__name__, sleep_s,
            )
            await asyncio.sleep(sleep_s)
            continue
        log.info(
            "storage router loaded on attempt %d: %s", attempt, _describe(router),
        )
        _event().set()
        return True

    log.error(
        "storage router load gave up after %d attempts (last error: %s) — "
        "retrying in the background until it loads; %s",
        max_retries, last_err,
        "startup is BLOCKED until then (required)" if required
        else "any work that needs a storage backend will fail until then",
    )
    _ensure_background_retry()
    if required:
        await wait_for_router(timeout=None)
        return True
    return False


async def shutdown_router() -> None:
    global _bg_task
    task = _bg_task
    _bg_task = None
    if task is not None and not task.done():
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass
    try:
        await router.close()
    except Exception as e:
        log.warning("storage router close error: %s", e)
