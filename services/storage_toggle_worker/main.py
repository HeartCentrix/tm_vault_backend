"""storage-toggle-worker — orchestrates azure↔onprem toggles.

Consumes `storage.toggle` RabbitMQ queue. Acquires a Postgres advisory
lock so only one instance actually runs the orchestration at a time;
the other is a hot standby.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os

import aio_pika
import asyncpg

from services.storage_toggle_worker.orchestrator import run_toggle
from shared.storage.router import router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("storage-toggle-worker")

ADVISORY_LOCK_ID = 9_042_042
QUEUE_NAME = "storage.toggle"


def _dsn() -> str:
    dsn = os.getenv("DATABASE_URL")
    if dsn:
        return dsn
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    user = os.getenv("DB_USERNAME", "postgres")
    pw = os.getenv("DB_PASSWORD")
    db = os.getenv("DB_NAME", "postgres")
    # Fail closed (B-L1). An empty DB_PASSWORD silently produces a DSN
    # like `postgresql://postgres:@host:5432/postgres`; depending on the
    # server's pg_hba.conf this can succeed unauthenticated. This worker
    # toggles the storage backend for the entire deployment — refuse to
    # start without an explicit password. Set DATABASE_URL or DB_PASSWORD.
    if not pw:
        raise RuntimeError(
            "DB_PASSWORD must be set for storage-toggle-worker (or pass a "
            "full DATABASE_URL with credentials baked in). Refusing to "
            "build a passwordless Postgres DSN."
        )
    return f"postgresql://{user}:{pw}@{host}:{port}/{db}"


def _rmq_url() -> str:
    url = os.getenv("RABBITMQ_URL")
    if url:
        return url
    # Fail closed when credentials aren't supplied. The storage.toggle queue
    # drives storage-backend switching for the whole deployment; anyone who
    # can publish to it can trigger unauthorized azure↔onprem migrations.
    # Defaulting to RabbitMQ's well-known guest:guest would let any process
    # with network access to the broker push toggle messages, so we refuse
    # to start instead of silently using a default that ships in every
    # RabbitMQ image. Set RABBITMQ_URL or RABBITMQ_USERNAME/PASSWORD/HOST.
    u = os.getenv("RABBITMQ_USERNAME") or os.getenv("RABBITMQ_USER")
    p = os.getenv("RABBITMQ_PASSWORD")
    h = os.getenv("RABBITMQ_HOST")
    port = os.getenv("RABBITMQ_PORT", "5672")
    missing = [name for name, val in (
        ("RABBITMQ_USERNAME (or RABBITMQ_USER)", u),
        ("RABBITMQ_PASSWORD", p),
        ("RABBITMQ_HOST", h),
    ) if not val]
    if missing:
        raise RuntimeError(
            "Missing required RabbitMQ env vars: "
            + ", ".join(missing)
            + ". Set RABBITMQ_URL, or all of "
            "RABBITMQ_USERNAME/RABBITMQ_PASSWORD/RABBITMQ_HOST."
        )
    if u == "guest" and p == "guest":
        raise RuntimeError(
            "Refusing to connect with the default guest:guest RabbitMQ "
            "credential. Provision a dedicated user for storage-toggle-worker."
        )
    return f"amqp://{u}:{p}@{h}:{port}/"


async def _acquire_advisory_lock_forever(dsn: str) -> asyncpg.Connection:
    while True:
        conn = await asyncpg.connect(dsn)
        locked = await conn.fetchval(
            "SELECT pg_try_advisory_lock($1)", ADVISORY_LOCK_ID,
        )
        if locked:
            log.info("acquired advisory lock %s", ADVISORY_LOCK_ID)
            return conn
        await conn.close()
        log.info("lock held by another instance; retrying in 15s")
        await asyncio.sleep(15)


async def consume() -> None:
    lock_conn = await _acquire_advisory_lock_forever(_dsn())

    # Self-heal: make sure the storage seed + NOTIFY triggers + seaweedfs
    # buckets exist before we start consuming. Toggle-worker is the right
    # place because it has aioboto3 available (the seeder's bucket create
    # step needs it) and because this is the process that actually toggles.
    try:
        from shared.database import engine
        from shared.storage_bootstrap import ensure_storage_bootstrap
        await ensure_storage_bootstrap(engine)
    except Exception as exc:
        log.warning("storage bootstrap at worker startup failed: %s", exc)

    await router.load(db_dsn=_dsn())

    connection = await aio_pika.connect_robust(_rmq_url())
    try:
        channel = await connection.channel()
        queue = await channel.declare_queue(QUEUE_NAME, durable=True)
        log.info("toggle worker ready, consuming %s", QUEUE_NAME)
        async with queue.iterator() as it:
            async for message in it:
                async with message.process():
                    payload = json.loads(message.body)
                    log.info("toggle message: %s", payload)
                    try:
                        await run_toggle(payload)
                    except Exception as e:
                        log.exception("orchestrator crashed: %s", e)
    finally:
        await lock_conn.close()
        await connection.close()
        await router.close()


if __name__ == "__main__":
    asyncio.run(consume())
