"""Aggregated application-health primitives.

Single source of truth for "is the whole app healthy?" — consumed by:
  * dashboard-service  → GET /health/deep   (served through the gateway's
    GET /api/v1/health aggregator)
  * scripts/health_check.py → post-deploy verification

Each check is defensive (its own try/except) so one failing subsystem
degrades that component rather than 500-ing the whole report. Status
vocabulary is uniform: "healthy" | "degraded" | "unhealthy", and the
overall status is the worst of the components.

Reuses existing primitives rather than reinventing them:
  * shared.database.engine + _has_required_tables  (Postgres)
  * RabbitMQ management API                         (queue depth + consumers)
  * worker_heartbeats table                         (worker liveness)
  * system_config / storage_backends tables         (active storage backend)
  * snapshot_partitions lease columns               (orphaned work)
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, List

import httpx
from sqlalchemy import text

from shared.config import settings
from shared.storage.labels import backend_summary, describe_backend

HEALTHY = "healthy"
DEGRADED = "degraded"
UNHEALTHY = "unhealthy"

_RANK = {HEALTHY: 0, DEGRADED: 1, UNHEALTHY: 2}

# Worker queues that must always have at least one consumer attached. A
# consumers=0 here is exactly the failure mode that left backup.heavy with
# 51 stuck jobs (no heavy worker pool) — surface it loudly. Non-worker
# queues (notification, report.normal, delete.low, sla.monitor, export.normal)
# are intentionally excluded: they're drained on-demand and idle at 0 by design.
CRITICAL_QUEUES = (
    "backup.urgent", "backup.high", "backup.normal", "backup.low", "backup.heavy",
    "backup.onedrive_partition", "backup.chats_partition", "backup.mail_partition",
    "backup.sharepoint_partition", "backup.groups_partition", "backup.entra_partition",
    "restore.urgent", "restore.normal", "restore.low",
    "discovery.m365", "discovery.azure", "discovery.tier2",
    "azure.vm", "azure.sql", "azure.postgres",
    "azure.restore.vm", "azure.restore.sql", "azure.restore.postgres",
    "audit.events",
)

# HEARTBEAT_STALE_S mirrors shared.reconciler — a worker not seen within this
# window is considered dead.
HEARTBEAT_STALE_S = int(os.getenv("HEARTBEAT_STALE_S", "60"))
# How long a partition may sit IN_PROGRESS past its lease before we treat it
# as an orphan worth flagging.
LEASE_GRACE_S = int(os.getenv("HEALTH_LEASE_GRACE_S", "60"))


def worst(statuses: List[str]) -> str:
    """Return the worst (most severe) status in the list, healthy if empty."""
    if not statuses:
        return HEALTHY
    return max(statuses, key=lambda s: _RANK.get(s, 1))


def _schema() -> str:
    return os.getenv("DB_SCHEMA", "tm")


async def check_database() -> Dict[str, Any]:
    try:
        from shared.database import engine, _has_required_tables, REQUIRED_TABLES
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
            tables_ok = await _has_required_tables(conn)
        if tables_ok:
            return {"status": HEALTHY, "detail": f"select1 ok; {len(REQUIRED_TABLES)} required tables present"}
        return {"status": UNHEALTHY, "detail": "connected but required tables missing"}
    except Exception as e:
        return {"status": UNHEALTHY, "detail": f"{type(e).__name__}: {e}"}


async def _fetch_queues() -> List[Dict[str, Any]]:
    """Query the RabbitMQ management API for all queues (name, messages,
    consumers). Raises on failure so the caller can mark rabbitmq unhealthy."""
    host = settings.RABBITMQ_HOST
    port = int(os.getenv("RABBITMQ_MGMT_PORT", "15672"))
    user = settings.RABBITMQ_USER
    pw = settings.RABBITMQ_PASSWORD
    url = f"http://{host}:{port}/api/queues"
    async with httpx.AsyncClient(timeout=httpx.Timeout(8.0)) as client:
        r = await client.get(url, auth=(user, pw), params={"columns": "name,messages,consumers"})
        r.raise_for_status()
        return r.json()


async def check_rabbitmq_and_queues() -> Dict[str, Dict[str, Any]]:
    """Returns two components: 'rabbitmq' (connectivity) and 'queues'
    (consumers=0 + non-empty DLQ analysis)."""
    if not settings.RABBITMQ_ENABLED:
        info = {"status": DEGRADED, "detail": "RabbitMQ disabled by config"}
        return {"rabbitmq": info, "queues": dict(info)}
    try:
        queues = await _fetch_queues()
    except Exception as e:
        info = {"status": UNHEALTHY, "detail": f"management API unreachable: {type(e).__name__}: {e}"}
        return {"rabbitmq": info, "queues": dict(info)}

    by_name = {q["name"]: q for q in queues}
    no_consumer = [
        name for name in CRITICAL_QUEUES
        if name in by_name and (by_name[name].get("consumers") or 0) == 0
    ]
    dlq_nonempty = [
        q["name"] for q in queues
        if q["name"].endswith(".dlq") and (q.get("messages") or 0) > 0
    ]

    rabbitmq = {"status": HEALTHY, "detail": f"connected; {len(queues)} queues"}

    q_status = HEALTHY
    bits = []
    if no_consumer:
        q_status = UNHEALTHY
        bits.append(f"{len(no_consumer)} critical queue(s) with no consumer: {', '.join(no_consumer)}")
    if dlq_nonempty:
        q_status = worst([q_status, DEGRADED])
        bits.append(f"{len(dlq_nonempty)} dead-letter queue(s) with messages: {', '.join(dlq_nonempty)}")
    queues_comp = {
        "status": q_status,
        "detail": "; ".join(bits) if bits else "all critical queues have consumers; DLQs empty",
        "no_consumer": no_consumer,
        "dlq_nonempty": dlq_nonempty,
    }
    return {"rabbitmq": rabbitmq, "queues": queues_comp}


async def check_workers() -> Dict[str, Any]:
    try:
        from shared.database import engine
        async with engine.connect() as conn:
            rows = (await conn.execute(text(
                "SELECT service_name, count(*) AS n, "
                "       bool_or('backup.heavy' = ANY(queues)) AS has_heavy "
                "FROM worker_heartbeats "
                "WHERE last_seen_at > NOW() - make_interval(secs => :stale) "
                "GROUP BY service_name"
            ), {"stale": HEARTBEAT_STALE_S})).mappings().all()
        if not rows:
            return {"status": UNHEALTHY, "detail": "no live worker heartbeats"}
        counts = {r["service_name"]: r["n"] for r in rows}
        has_heavy = any(r["has_heavy"] for r in rows)
        backup_live = counts.get("backup_worker", 0)
        status = HEALTHY
        bits = [f"{k}:{v}" for k, v in sorted(counts.items())]
        if backup_live == 0:
            status = UNHEALTHY
            bits.append("NO live backup_worker")
        if not has_heavy:
            status = worst([status, DEGRADED])
            bits.append("no heavy pool (backup.heavy consumer)")
        return {"status": status, "detail": ", ".join(bits), "live": counts, "heavy_pool": has_heavy}
    except Exception as e:
        return {"status": UNHEALTHY, "detail": f"{type(e).__name__}: {e}"}


async def check_storage() -> Dict[str, Any]:
    try:
        from shared.database import engine
        schema = _schema()
        async with engine.connect() as conn:
            exists = await conn.execute(text(
                f"SELECT to_regclass('{schema}.system_config') IS NOT NULL "
                f"   AND to_regclass('{schema}.storage_backends') IS NOT NULL"
            ))
            if not exists.scalar():
                return {"status": DEGRADED, "detail": "storage tables not present yet"}
            sc = (await conn.execute(text(
                "SELECT active_backend_id::text AS aid, transition_state AS ts "
                "FROM system_config WHERE id = 1"
            ))).mappings().first()
            if not sc or not sc["aid"]:
                return {"status": UNHEALTHY, "detail": "no active storage backend configured"}
            row = (await conn.execute(text(
                "SELECT kind, name, endpoint, is_enabled "
                "FROM storage_backends WHERE id = :id"
            ), {"id": sc["aid"]})).mappings().first()
        if not row or not row["is_enabled"]:
            return {"status": UNHEALTHY, "detail": "active backend missing or disabled"}
        ts = (sc["ts"] or "stable")
        status = HEALTHY if ts in ("stable", "STABLE", None) else DEGRADED
        # 2026-08-11 QA-11: this line used to read
        #   f"active={row['kind']}/{row['name']}, state={ts}"
        # which printed "active=seaweedfs/seaweedfs-local" while System >
        # Storage — reading the SAME system_config row — showed "Cloud
        # Storage / WASABI / US-WEST-2". Neither was stale: `seaweedfs` is
        # the S3-compatible SLOT and this deployment points its endpoint at
        # https://s3.us-west-2.wasabisys.com (ONPREM_S3_ENDPOINT), keeping
        # the seeded slot name "seaweedfs-local". Naming a SeaweedFS host
        # that receives nothing, on a backup product's health page, is the
        # bug. Emit the endpoint-derived provider label, the raw slot
        # identifiers AND the endpoint, so the two surfaces are recognisably
        # the same backend; ship the same values structured so machine
        # consumers never have to parse the string.
        # Pinned by tests/shared/test_health_storage_backend_identity.py.
        desc = describe_backend(row["kind"], row["name"], row["endpoint"])
        return {
            "status": status,
            "detail": (
                f"active={backend_summary(row['kind'], row['name'], row['endpoint'])}"
                f", state={ts}"
            ),
            "active_backend": {
                "id": str(sc["aid"]),
                "kind": row["kind"],
                "name": row["name"],
                "endpoint": row["endpoint"],
                **desc,
            },
            "transition_state": ts,
        }
    except Exception as e:
        return {"status": UNHEALTHY, "detail": f"{type(e).__name__}: {e}"}


async def check_leases() -> Dict[str, Any]:
    """Count IN_PROGRESS partitions whose lease has expired (orphaned work).
    Near-zero is healthy; a backlog signals a dead worker not yet reconciled
    (the orphaned-partition stall we hit this session)."""
    try:
        from shared.database import engine
        async with engine.connect() as conn:
            n = (await conn.execute(text(
                "SELECT count(*) FROM snapshot_partitions "
                "WHERE status = 'IN_PROGRESS' "
                "  AND lease_expires_at IS NOT NULL "
                "  AND lease_expires_at < NOW() - make_interval(secs => :grace)"
            ), {"grace": LEASE_GRACE_S})).scalar() or 0
        if n == 0:
            return {"status": HEALTHY, "detail": "0 orphaned partitions"}
        status = DEGRADED if n < 20 else UNHEALTHY
        return {"status": status, "detail": f"{n} partition(s) with expired lease (orphaned work)"}
    except Exception as e:
        # Table may not exist on a fresh deploy — treat as informational.
        return {"status": HEALTHY, "detail": f"lease check skipped: {type(e).__name__}"}


async def build_health(detailed: bool = True) -> Dict[str, Any]:
    """Run all subsystem checks and assemble the aggregate report.

    detailed=False strips the verbose per-component extras (queue lists,
    worker counts) for the unauthenticated public summary.
    """
    db = await check_database()
    mq = await check_rabbitmq_and_queues()
    workers = await check_workers()
    storage = await check_storage()
    leases = await check_leases()

    components: Dict[str, Any] = {
        "database": db,
        "rabbitmq": mq["rabbitmq"],
        "queues": mq["queues"],
        "workers": workers,
        "storage": storage,
        "leases": leases,
    }
    overall = worst([c["status"] for c in components.values()])

    if not detailed:
        components = {
            name: {"status": c["status"]} for name, c in components.items()
        }

    return {
        "status": overall,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "components": components,
    }
