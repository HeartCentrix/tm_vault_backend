"""Storage backend + system_config bootstrap.

Runs from init_db() on every service startup. Idempotent — seeds the minimum
set of backends (azure-primary + seaweedfs-local) and the system_config
singleton when missing, installs the NOTIFY trigger, and asserts the end
state is usable. A fresh DB (schema drop/re-create) becomes self-healing.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import uuid
from typing import Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

log = logging.getLogger(__name__)


# asyncpg / SQLAlchemy text() runs one statement per execute(), so each
# CREATE FUNCTION / TRIGGER is its own entry.
_NOTIFY_TRIGGER_STATEMENTS = (
    """CREATE OR REPLACE FUNCTION notify_system_config_changed()
    RETURNS trigger AS $$
    BEGIN
      PERFORM pg_notify('system_config_changed', NEW.id::text);
      RETURN NEW;
    END;
    $$ LANGUAGE plpgsql""",
    "DROP TRIGGER IF EXISTS trg_system_config_notify ON system_config",
    """CREATE TRIGGER trg_system_config_notify
      AFTER UPDATE OR INSERT ON system_config
      FOR EACH ROW
      EXECUTE FUNCTION notify_system_config_changed()""",
    """CREATE OR REPLACE FUNCTION notify_storage_backends_changed()
    RETURNS trigger AS $$
    BEGIN
      PERFORM pg_notify('storage_backends_changed',
                        COALESCE(NEW.id::text, OLD.id::text));
      RETURN COALESCE(NEW, OLD);
    END;
    $$ LANGUAGE plpgsql""",
    "DROP TRIGGER IF EXISTS trg_storage_backends_notify ON storage_backends",
    """CREATE TRIGGER trg_storage_backends_notify
      AFTER INSERT OR UPDATE OR DELETE ON storage_backends
      FOR EACH ROW
      EXECUTE FUNCTION notify_storage_backends_changed()""",
    # Legacy worker paths (any pre-router code still using
    # azure_storage_manager directly) INSERT into snapshots/snapshot_items
    # without setting backend_id. This trigger fills in the active backend
    # from system_config so the NOT NULL constraint doesn't kill the job.
    """CREATE OR REPLACE FUNCTION default_snapshot_backend_id()
    RETURNS trigger AS $$
    BEGIN
      IF NEW.backend_id IS NULL THEN
        SELECT active_backend_id INTO NEW.backend_id
        FROM system_config WHERE id = 1;
      END IF;
      RETURN NEW;
    END;
    $$ LANGUAGE plpgsql""",
    "DROP TRIGGER IF EXISTS trg_snapshots_default_backend ON snapshots",
    """CREATE TRIGGER trg_snapshots_default_backend
      BEFORE INSERT ON snapshots
      FOR EACH ROW EXECUTE FUNCTION default_snapshot_backend_id()""",
    "DROP TRIGGER IF EXISTS trg_snapshot_items_default_backend ON snapshot_items",
    """CREATE TRIGGER trg_snapshot_items_default_backend
      BEFORE INSERT ON snapshot_items
      FOR EACH ROW EXECUTE FUNCTION default_snapshot_backend_id()""",
)


def _azure_config() -> dict:
    account = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "").strip()
    return {
        "shards": [
            {
                "account": account or "PLACEHOLDER",
                "key_ref": "env://AZURE_STORAGE_ACCOUNT_KEY",
            }
        ]
    }


def _azure_endpoint() -> str:
    ep = os.getenv("AZURE_STORAGE_BLOB_ENDPOINT", "").strip()
    if ep:
        return ep
    account = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "").strip()
    if account:
        return f"https://{account}.blob.core.windows.net/"
    return "https://PLACEHOLDER.blob.core.windows.net/"


def _seaweed_endpoint() -> str:
    # Match ONPREM_S3_ENDPOINT (the var the SeaweedStore expects via
    # secret_ref resolution) so operator config flows through one place.
    return os.getenv("ONPREM_S3_ENDPOINT", "http://seaweedfs:8333").strip()


def _seaweed_buckets() -> list[str]:
    raw = os.getenv("ONPREM_S3_BUCKETS", "").strip()
    if not raw:
        return ["tmvault-shard-0"]
    return [b.strip() for b in raw.split(",") if b.strip()]


def _seaweed_config() -> dict:
    """Config blob that SeaweedStore.from_config() consumes.

    - access_key_env: env var name to read the access key from
    - buckets: list of pre-created buckets (object-lock enabled)
    - verify_tls: bool — false for local Azurite/SeaweedFS over plain HTTP
    """
    return {
        "access_key_env": "ONPREM_S3_ACCESS_KEY",
        "buckets": _seaweed_buckets(),
        "verify_tls": os.getenv("ONPREM_S3_VERIFY_TLS", "true").lower() == "true",
        "region": os.getenv("ONPREM_S3_REGION", "us-east-1"),
    }


async def _try_acquire_bootstrap_lock(
    conn,
    lock_key: int,
    deadline_s: float,
) -> bool:
    """Try to acquire the bootstrap advisory lock with bounded backoff.

    Uses ``pg_try_advisory_lock`` (non-blocking) so a stuck holder can never
    wedge the caller indefinitely. Returns True if acquired, False if we
    gave up — in which case the caller proceeds *without* the lock,
    relying on the per-statement SAVEPOINT recovery to handle the rare
    CREATE FUNCTION race (idempotent DDL is safe to run concurrently;
    SAVEPOINTs ensure we don't poison the transaction if it loses).
    """
    delay = 0.25
    elapsed = 0.0
    attempts = 0
    while True:
        attempts += 1
        got = (await conn.execute(
            text(f"SELECT pg_try_advisory_lock({lock_key})"),
        )).scalar()
        if got:
            if attempts > 1:
                log.info(
                    "[storage-bootstrap] acquired lock after %d attempt(s) (%.1fs)",
                    attempts, elapsed,
                )
            return True
        if elapsed >= deadline_s:
            log.warning(
                "[storage-bootstrap] could not acquire lock %d after %.1fs / %d attempts — "
                "proceeding without serialization (DDL is idempotent + SAVEPOINT-wrapped)",
                lock_key, elapsed, attempts,
            )
            return False
        await asyncio.sleep(delay)
        elapsed += delay
        delay = min(delay * 2, 2.0)


async def ensure_storage_bootstrap(engine: AsyncEngine) -> None:
    """Seed azure-primary + seaweedfs-local + system_config, install triggers,
    assert sanity. Idempotent and safe to call from every service.

    Lock semantics:
      * Uses key 9_042_043 (NOT 9_042_042). 9_042_042 is reserved by
        storage_toggle_worker for its singleton-leader election, which
        holds the lock for the worker's entire lifetime. Sharing that
        key would wedge every service's init_db until the toggle worker
        restarts.
      * Acquires via pg_try_advisory_lock with bounded backoff (max 5s),
        so a stuck holder can never block startup forever. If we can't
        get the lock, we proceed anyway — the DDL is all idempotent
        (CREATE OR REPLACE, DO $$ EXCEPTION duplicate_object NULL,
        SAVEPOINT-wrapped) so concurrent runs are safe.
    """
    BOOTSTRAP_LOCK_KEY = 9_042_043
    # 5s is plenty: even a heavy bootstrap finishes in <2s; if we can't get
    # the lock in 5s, something is wedged and we're better off running the
    # idempotent DDL ourselves than waiting forever.
    LOCK_DEADLINE_S = float(os.getenv("STORAGE_BOOTSTRAP_LOCK_DEADLINE_S", "5"))
    async with engine.begin() as conn:
        have_lock = await _try_acquire_bootstrap_lock(
            conn, BOOTSTRAP_LOCK_KEY, LOCK_DEADLINE_S,
        )

        # Bound how long any bootstrap DDL will wait for a table lock.
        # The DROP/CREATE TRIGGER on snapshot_items needs ACCESS EXCLUSIVE;
        # without a cap it can queue behind a long UPDATE and, per PG
        # lock-queue fairness, block every live INSERT behind it (prod
        # froze ~5 min on 2026-07-30). On timeout the per-statement
        # SAVEPOINT below swallows the failure and we continue — the
        # triggers are idempotent and already present from a prior boot.
        # Validate against a strict Postgres-duration pattern so a malformed
        # env value (e.g. "3 seconds", "abc") can't raise here and abort
        # startup on every boot of all 4 services — fall back to '3s'.
        #
        # SET LOCAL is TRANSACTION-scoped, so it is released again right
        # after the DDL loop below. Only the DDL loop is SAVEPOINT-wrapped;
        # the seed UPSERTs and the invariant check are bare, so leaving the
        # timeout in force for them would turn a row-lock conflict (a
        # concurrent bootstrap or storage_toggle_worker holding the
        # azure-primary row — reachable because _try_acquire_bootstrap_lock
        # gives up after STORAGE_BOOTSTRAP_LOCK_DEADLINE_S and proceeds
        # unserialized) from "wait briefly, then succeed" into a raise that
        # aborts the whole bootstrap and silently skips the operator-facing
        # endpoint/config upsert. Those statements take row locks only —
        # they cannot cause the ACCESS EXCLUSIVE pile-up this guard exists
        # for — so they keep the server default.
        _raw_lt = os.getenv("STORAGE_BOOTSTRAP_LOCK_TIMEOUT", "3s").strip()
        _lock_timeout = _raw_lt if re.fullmatch(r"\d+(us|ms|s|min|h|d)?", _raw_lt) else "3s"
        await conn.execute(text(f"SET LOCAL lock_timeout = '{_lock_timeout}'"))

        # Wrap each DDL in its own SAVEPOINT so a stmt-level failure (the
        # CREATE FUNCTION pg_proc_proname_args_nsp_index race we're
        # guarding against, plus any IF NOT EXISTS edge cases) doesn't
        # abort the whole transaction and bring down the downstream seed.
        # This makes the bootstrap correct even when we couldn't acquire
        # the advisory lock — concurrent racers just both succeed via
        # CREATE OR REPLACE, or one loses the unique-index race and gets
        # rolled back to its savepoint.
        for stmt in _NOTIFY_TRIGGER_STATEMENTS:
            sp_name = f"sp_bootstrap_{abs(hash(stmt)) % (10**8)}"
            try:
                await conn.execute(text(f"SAVEPOINT {sp_name}"))
                await conn.execute(text(stmt))
                await conn.execute(text(f"RELEASE SAVEPOINT {sp_name}"))
            except Exception as exc:
                # Roll back to savepoint so the outer transaction stays alive
                # for the seed rows + invariant check below.
                try:
                    await conn.execute(text(f"ROLLBACK TO SAVEPOINT {sp_name}"))
                except Exception:
                    pass
                log.warning(
                    "[storage-bootstrap] trigger stmt failed: %s (%s...)",
                    exc, stmt[:60],
                )

        # DDL done — hand the rest of the transaction back to the server
        # default so the un-SAVEPOINTed seed/invariant statements below
        # keep their pre-guard "wait for the row lock, then succeed"
        # behaviour. DEFAULT (not a literal) so an operator-configured
        # server/role setting is honoured, and it stays transaction-local.
        await conn.execute(text("SET LOCAL lock_timeout = DEFAULT"))

        azure_id = str(uuid.uuid4())
        # Upsert on re-seed so operator-driven .env changes (e.g.
        # AZURE_STORAGE_ACCOUNT_NAME swap to a new account) actually
        # land in the DB. Previously this used DO NOTHING, which froze
        # the endpoint at whatever .env held the first time bootstrap
        # ran — toggles to "azure-primary" then preflighted against a
        # stale account and timed out.
        await conn.execute(
            text(
                "INSERT INTO storage_backends "
                "(id, kind, name, endpoint, secret_ref, config, is_enabled, "
                " created_at, updated_at) "
                "VALUES (:id, 'azure_blob', 'azure-primary', :endpoint, "
                " 'env://AZURE_STORAGE_ACCOUNT_KEY', CAST(:config AS JSONB), "
                " true, NOW(), NOW()) "
                "ON CONFLICT (name) DO UPDATE SET "
                "  endpoint = EXCLUDED.endpoint, "
                "  secret_ref = EXCLUDED.secret_ref, "
                "  config = EXCLUDED.config, "
                "  updated_at = NOW()"
            ),
            {
                "id": azure_id,
                "endpoint": _azure_endpoint(),
                "config": json.dumps(_azure_config()),
            },
        )

        seaweed_id = str(uuid.uuid4())
        # Upsert on re-seed: SeaweedStore.from_config() reads credentials
        # from env vars referenced by secret_ref / access_key_env, so if the
        # operator later changes ONPREM_S3_* vars, the DB row needs to stay
        # in sync with the expected env names + bucket list.
        await conn.execute(
            text(
                "INSERT INTO storage_backends "
                "(id, kind, name, endpoint, secret_ref, config, is_enabled, "
                " created_at, updated_at) "
                "VALUES (:id, 'seaweedfs', 'seaweedfs-local', :endpoint, "
                " 'env://ONPREM_S3_SECRET_KEY', CAST(:config AS JSONB), "
                " true, NOW(), NOW()) "
                "ON CONFLICT (name) DO UPDATE SET "
                "  endpoint = EXCLUDED.endpoint, "
                "  secret_ref = EXCLUDED.secret_ref, "
                "  config = EXCLUDED.config, "
                "  updated_at = NOW()"
            ),
            {
                "id": seaweed_id,
                "endpoint": _seaweed_endpoint(),
                "config": json.dumps(_seaweed_config()),
            },
        )

        # system_config singleton — point at whichever backend is named by
        # STORAGE_DEFAULT_BACKEND (azure-primary by default).
        default_name = os.getenv("STORAGE_DEFAULT_BACKEND", "azure-primary")
        await conn.execute(
            text(
                "INSERT INTO system_config "
                "(id, active_backend_id, transition_state, updated_at) "
                "SELECT 1, b.id, 'stable', NOW() "
                "FROM storage_backends b WHERE b.name = :name "
                "ON CONFLICT (id) DO NOTHING"
            ),
            {"name": default_name},
        )

        # Startup assertion — fail loud if we still don't have a usable
        # singleton after the seeder ran.
        row = await conn.execute(
            text(
                "SELECT sc.active_backend_id, "
                "       (SELECT count(*) FROM storage_backends "
                "        WHERE is_enabled = true) AS enabled_count "
                "FROM system_config sc WHERE sc.id = 1"
            )
        )
        r = row.first()
        if not r or r.active_backend_id is None or r.enabled_count == 0:
            if have_lock:
                try:
                    await conn.execute(
                        text(f"SELECT pg_advisory_unlock({BOOTSTRAP_LOCK_KEY})"),
                    )
                except Exception:
                    pass
            raise RuntimeError(
                "[storage-bootstrap] invariant failed: "
                "system_config singleton or enabled backends missing after seed"
            )

        # Release the advisory lock before the txn commits, so the next
        # racing waiter unblocks immediately rather than waiting for the
        # session/connection to close.
        if have_lock:
            await conn.execute(
                text(f"SELECT pg_advisory_unlock({BOOTSTRAP_LOCK_KEY})"),
            )

    log.info("[storage-bootstrap] seed + triggers + invariants OK")

    # Bucket auto-provisioning — SeaweedStore expects buckets to pre-exist,
    # and preflight probes write to _buckets[0]. A fresh SeaweedFS has none,
    # so every toggle would fail with NoSuchBucket / SignatureDoesNotMatch.
    try:
        await _ensure_seaweed_buckets()
    except Exception as exc:
        log.warning("[storage-bootstrap] seaweed bucket ensure failed: %s", exc)


async def _ensure_seaweed_buckets() -> None:
    """Create any buckets listed in ONPREM_S3_BUCKETS that don't yet exist.
    Idempotent. Skipped when aioboto3 isn't available in this runtime."""
    buckets = _seaweed_buckets()
    if not buckets:
        return
    try:
        import aioboto3  # type: ignore
    except ImportError:
        log.info("[storage-bootstrap] aioboto3 not available; skipping bucket create")
        return

    endpoint = _seaweed_endpoint()
    access = os.getenv("ONPREM_S3_ACCESS_KEY", "")
    secret = os.getenv("ONPREM_S3_SECRET_KEY", "")
    if not access or not secret:
        log.info("[storage-bootstrap] ONPREM_S3 creds not set; skipping bucket create")
        return

    # Retry the endpoint-reachability probe with backoff so a cold
    # docker-compose up doesn't leave the bucket uncreated when the
    # toggle-worker beats seaweedfs to readiness (seaweedfs has no
    # healthcheck so we can't depends_on it). 5 retries × 4s = ~20s
    # cold-boot tolerance.
    import asyncio as _asyncio
    session = aioboto3.Session()
    attempts = int(os.getenv("STORAGE_BUCKET_ENSURE_ATTEMPTS", "5"))
    backoff_s = float(os.getenv("STORAGE_BUCKET_ENSURE_BACKOFF_S", "4"))
    # Two-phase: for each bucket, first verify it exists; if not,
    # try to create it. Track which buckets are still pending AFTER
    # each attempt and ONLY retry the ones that haven't landed yet.
    # This fixes a subtle bug where the previous code swallowed the
    # create_bucket exception inside the inner try/except so the
    # outer retry never fired — cold-start still left buckets
    # uncreated even though the loop "completed".
    pending = set(buckets)
    last_err: Optional[Exception] = None
    for attempt in range(1, attempts + 1):
        if not pending:
            break
        try:
            async with session.client(
                "s3", endpoint_url=endpoint,
                aws_access_key_id=access, aws_secret_access_key=secret,
                region_name=os.getenv("ONPREM_S3_REGION", "us-east-1"),
                verify=os.getenv(
                    "ONPREM_S3_VERIFY_TLS", "true",
                ).lower() == "true",
            ) as s3:
                for bucket in list(pending):
                    try:
                        await s3.head_bucket(Bucket=bucket)
                        pending.discard(bucket)
                        continue
                    except Exception as he:
                        last_err = he  # record but keep trying to create
                    try:
                        await s3.create_bucket(Bucket=bucket)
                        pending.discard(bucket)
                        log.info(
                            "[storage-bootstrap] created seaweed bucket %s",
                            bucket,
                        )
                    except Exception as ce:
                        # Keep bucket in pending, retry on next round.
                        last_err = ce
        except Exception as exc:
            last_err = exc

        if pending and attempt < attempts:
            log.info(
                "[storage-bootstrap] bucket ensure (attempt %d/%d) "
                "still pending %s — retrying in %.1fs",
                attempt, attempts, sorted(pending), backoff_s,
            )
            await _asyncio.sleep(backoff_s)

    if pending:
        log.warning(
            "[storage-bootstrap] gave up after %d attempts — "
            "buckets still missing: %s (last error: %s)",
            attempts, sorted(pending), last_err,
        )
