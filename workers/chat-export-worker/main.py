"""chat-export-worker entrypoint.

Consumes q.export.chat.thread, renders HTML/JSON/PDF, streams ZIP to blob.
v1: one job = one ZIP. v2 will add parent/merge consumers alongside.
"""
import asyncio
import logging
import signal

from aiohttp import web
from prometheus_client import start_http_server
import aio_pika

from shared.config import settings

from workers.chat_export_worker.consumers.thread import consume_thread

log = logging.getLogger("chat-export-worker")
Q_THREAD = "q.export.chat.thread"


async def _reclaim_orphan_jobs() -> None:
    """Mark chat-export jobs left in RUNNING/PENDING as FAILED on startup.

    If the worker was OOM-killed or crashed mid-job, the row stays in RUNNING
    and the UI polls forever. There is exactly one chat-export-worker per
    deployment, so on startup any RUNNING chat-export job has no live owner
    and should be failed so the client sees a terminal state.

    B-M2: the SELECT and UPDATE used to be unsynchronised, leaving a TOCTOU
    window during rolling deploys where a sibling worker could have advanced
    a job between the two queries — we'd then incorrectly mark a job
    FAILED while another replica was actively processing it. Closing the
    window with two compounding guards:

      1. `SELECT … FOR UPDATE SKIP LOCKED` so any row another worker is
         already operating on (i.e. holding a row lock inside its own
         consumer transaction) is invisible to this reclaim scan.
      2. Re-check the status set in the UPDATE's WHERE clause so a row
         that legitimately transitioned out of RUNNING/PENDING/QUEUED
         between SELECT and UPDATE — possibly via a code path that
         doesn't take row locks — is also skipped.
    """
    from sqlalchemy import and_, select, update as sa_update
    from shared.database import async_session_factory
    from shared.models import Job, JobStatus, JobType
    in_flight_states = [JobStatus.RUNNING, JobStatus.PENDING, JobStatus.QUEUED]
    async with async_session_factory() as sess:
        q = (
            select(Job.id, Job.spec)
            .where(
                and_(
                    Job.type == JobType.EXPORT,
                    Job.status.in_(in_flight_states),
                )
            )
            .with_for_update(skip_locked=True)
        )
        rows = (await sess.execute(q)).all()
        ids_to_fail: list = []
        for jid, spec in rows:
            if isinstance(spec, dict) and spec.get("kind") == "chat_export_thread":
                ids_to_fail.append(jid)
        if not ids_to_fail:
            await sess.commit()  # release the locks SKIP LOCKED grabbed
            return
        result = await sess.execute(
            sa_update(Job)
            .where(
                and_(
                    Job.id.in_(ids_to_fail),
                    # Re-assert the in-flight states. Belt-and-suspenders
                    # alongside the SELECT FOR UPDATE — if a non-locking
                    # write path (e.g. an admin SQL fix) advanced the row
                    # since the SELECT, don't clobber it.
                    Job.status.in_(in_flight_states),
                )
            )
            .values(
                status=JobStatus.FAILED,
                result={"error": {"code": "worker_restart",
                                   "message": "chat-export-worker restarted before the job finished"}},
            )
        )
        await sess.commit()
        log.info(
            "reclaimed %d orphan chat-export job(s) (rows touched: %d)",
            len(ids_to_fail), result.rowcount or 0,
        )


async def health_server() -> None:
    async def ok(_r):
        return web.Response(text="ok")

    app = web.Application()
    app.router.add_get("/health", ok)
    app.router.add_get("/ready", ok)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8081)
    await site.start()


async def main() -> None:
    from shared.storage.startup import startup_router
    logging.basicConfig(
        level=logging.INFO,
        format='{"ts":"%(asctime)s","lvl":"%(levelname)s","msg":"%(message)s","svc":"chat-export-worker"}',
    )
    start_http_server(9102)
    await health_server()
    await startup_router()
    try:
        await _reclaim_orphan_jobs()
    except Exception:
        log.exception("orphan-job reclaim failed; continuing")

    conn = await aio_pika.connect_robust(
        settings.RABBITMQ_URL,
        heartbeat=settings.RABBITMQ_CONSUMER_HEARTBEAT_SECONDS,
    )
    channel = await conn.channel()
    await channel.set_qos(prefetch_count=1)
    # Bind to the shared tm.exchange (DIRECT) used by message_bus.publish so
    # messages routed with key=Q_THREAD land in our queue.
    exchange = await channel.declare_exchange(
        "tm.exchange", aio_pika.ExchangeType.DIRECT, durable=True,
    )
    dlq_rk = f"dlq.{Q_THREAD.split('.', 1)[1]}"
    queue = await channel.declare_queue(
        Q_THREAD,
        durable=True,
        arguments={
            "x-dead-letter-exchange": "tm.exchange",
            "x-dead-letter-routing-key": dlq_rk,
        },
    )
    await queue.bind(exchange, routing_key=Q_THREAD)
    dlq = await channel.declare_queue(dlq_rk, durable=True)
    await dlq.bind(exchange, routing_key=dlq_rk)
    parent_q = await channel.declare_queue("q.export.chat.parent", durable=True)
    await parent_q.bind(exchange, routing_key="q.export.chat.parent")
    merge_q = await channel.declare_queue("q.export.chat.merge", durable=True)
    await merge_q.bind(exchange, routing_key="q.export.chat.merge")

    log.info("worker started queue=%s", Q_THREAD)
    stop = asyncio.Event()

    def _sigterm(*_):
        stop.set()

    for s in (signal.SIGTERM, signal.SIGINT):
        asyncio.get_event_loop().add_signal_handler(s, _sigterm)

    async with queue.iterator() as it:
        async for message in it:
            if stop.is_set():
                break
            await consume_thread(message)

    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
