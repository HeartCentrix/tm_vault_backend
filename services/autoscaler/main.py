"""TMvault autoscaler — queue-depth-driven replica scaling for Railway.

Railway doesn't natively support custom-metric autoscaling. This worker
polls RabbitMQ management API for queue depth, computes a desired
replica count per workload using a simple formula, and calls the
Railway GraphQL API to apply it. Loops every ``SCALE_INTERVAL_S``
(default 60s).

Scaling decision (per service)
------------------------------
  desired = clamp(
      ceil(queue_depth / TARGET_DEPTH_PER_REPLICA),
      min_replicas,
      max_replicas,
  )

If `desired` differs from current by more than `SCALE_HYSTERESIS`
replicas, we apply the change. Otherwise we hold.

Hysteresis prevents flapping when the queue oscillates around the
threshold. Combined with the 60-second loop it produces ~smooth
ramp-up/down without thrashing.

Environment
-----------
  RABBITMQ_MGMT_URL          # e.g. http://rabbitmq:15672 (used to read
                             # queue length via /api/queues/{vhost}/{name})
  RABBITMQ_MGMT_USER         # default 'guest'
  RABBITMQ_MGMT_PASSWORD     # default 'guest'

  RAILWAY_API_TOKEN          # required for replica writes; without this
                             # the autoscaler is read-only and logs the
                             # decisions it *would* make.
  RAILWAY_PROJECT_ID         # the tm_vault project ID
  RAILWAY_ENVIRONMENT_ID     # 'production' env ID

  AUTOSCALER_CONFIG          # JSON describing each service to scale.
                             # See `_DEFAULT_CONFIG` below for shape.
  SCALE_INTERVAL_S           # default 60
  SCALE_HYSTERESIS           # default 1
  CORE_METRICS_PORT          # default 9103 (see shared.core_metrics)

Failure modes
-------------
- RabbitMQ unreachable → log + skip this tick.
- Railway API unreachable → log + skip this tick.
- Partial failure (one service scaled, another failed) → log per-service
  outcome; no rollback (the next tick is the rollback).
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import os
from typing import Any, Dict, List, Optional

import httpx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("autoscaler")


# Default per-service scaling targets. Override via AUTOSCALER_CONFIG.
_DEFAULT_CONFIG: List[Dict[str, Any]] = [
    {
        "service_name": "backup_worker",
        "queues": ["backup.jobs", "backup.partitions"],
        "target_depth_per_replica": 200,
        "min_replicas": 2,
        "max_replicas": 50,
    },
    {
        "service_name": "backup_worker_heavy",
        "queues": ["backup.heavy"],
        "target_depth_per_replica": 50,
        "min_replicas": 1,
        "max_replicas": 8,
    },
    {
        "service_name": "discovery_worker",
        "queues": ["discovery.runs"],
        "target_depth_per_replica": 25,
        "min_replicas": 1,
        "max_replicas": 4,
    },
    {
        "service_name": "restore_worker",
        "queues": ["restore.jobs", "restore.heavy"],
        "target_depth_per_replica": 10,
        "min_replicas": 1,
        "max_replicas": 8,
    },
]


def _load_config() -> List[Dict[str, Any]]:
    raw = os.environ.get("AUTOSCALER_CONFIG")
    if not raw:
        return _DEFAULT_CONFIG
    try:
        cfg = json.loads(raw)
        if isinstance(cfg, list):
            return cfg
        log.warning("[autoscaler] AUTOSCALER_CONFIG is not a list — using defaults")
    except Exception as exc:
        log.warning("[autoscaler] AUTOSCALER_CONFIG parse error %s — using defaults", exc)
    return _DEFAULT_CONFIG


# ─── RabbitMQ queue depth ────────────────────────────────────────────


async def _get_queue_depth(
    http: httpx.AsyncClient, mgmt_url: str, auth: tuple, queue: str,
) -> Optional[int]:
    """Return the `messages` field for a queue, or None on error."""
    url = f"{mgmt_url.rstrip('/')}/api/queues/%2F/{queue}"
    try:
        r = await http.get(url, auth=auth, timeout=10.0)
        if r.status_code == 404:
            # Queue doesn't exist yet — treat as zero depth.
            return 0
        r.raise_for_status()
        body = r.json()
        return int(body.get("messages") or 0)
    except Exception as exc:
        log.warning("[autoscaler] queue %s depth probe failed: %s", queue, exc)
        return None


# ─── Railway GraphQL: set replicas ───────────────────────────────────


_RAILWAY_GRAPHQL = "https://backboard.railway.app/graphql/v2"


async def _set_replicas(
    http: httpx.AsyncClient, token: str, project_id: str,
    environment_id: str, service_name: str, desired: int,
) -> bool:
    """Call Railway's serviceInstanceUpdate to set numReplicas.

    Returns True on success, False on API error. The call is
    declarative — Railway converges replicas asynchronously.
    """
    mutation = (
        "mutation($projectId:String!,$envId:String!,$serviceName:String!,$n:Int!){\n"
        "  serviceInstanceUpdate(\n"
        "    projectId:$projectId, environmentId:$envId, serviceName:$serviceName,\n"
        "    input:{numReplicas:$n}\n"
        "  ) { id }\n"
        "}"
    )
    body = {
        "query": mutation,
        "variables": {
            "projectId": project_id, "envId": environment_id,
            "serviceName": service_name, "n": int(desired),
        },
    }
    try:
        r = await http.post(
            _RAILWAY_GRAPHQL,
            json=body,
            headers={"Authorization": f"Bearer {token}"},
            timeout=20.0,
        )
        r.raise_for_status()
        data = r.json()
        if data.get("errors"):
            log.warning("[autoscaler] Railway API error scaling %s: %s",
                        service_name, data["errors"])
            return False
        return True
    except Exception as exc:
        log.warning("[autoscaler] Railway API request failed for %s: %s", service_name, exc)
        return False


# ─── Loop body ──────────────────────────────────────────────────────


async def _tick(
    http: httpx.AsyncClient, mgmt_url: str, mgmt_auth: tuple,
    railway_token: Optional[str], project_id: Optional[str],
    env_id: Optional[str], hysteresis: int,
    config: List[Dict[str, Any]],
    current_replicas: Dict[str, int],
) -> None:
    """One scan of all configured services."""
    for svc in config:
        name = svc["service_name"]
        queues = svc["queues"]
        target = int(svc["target_depth_per_replica"])
        mn = int(svc["min_replicas"])
        mx = int(svc["max_replicas"])

        total_depth = 0
        any_failed = False
        for q in queues:
            depth = await _get_queue_depth(http, mgmt_url, mgmt_auth, q)
            if depth is None:
                any_failed = True
                continue
            total_depth += depth

        if any_failed and total_depth == 0:
            # All probes failed — don't change replicas based on bogus data.
            continue

        desired = max(mn, min(mx, math.ceil(total_depth / max(1, target))))
        current = current_replicas.get(name, mn)

        if abs(desired - current) < hysteresis:
            log.info(
                "[autoscaler] %s hold: depth=%d desired=%d current=%d (hysteresis=%d)",
                name, total_depth, desired, current, hysteresis,
            )
            continue

        log.info(
            "[autoscaler] %s scale: depth=%d current=%d → desired=%d",
            name, total_depth, current, desired,
        )

        # Emit observability for the autoscale decision.
        try:
            from shared import core_metrics
            core_metrics.set_queue_depth("|".join(queues), total_depth)
            core_metrics.set_worker_active_jobs(name, current)
        except Exception:
            pass

        if not (railway_token and project_id and env_id):
            log.info("[autoscaler] dry-run: RAILWAY_API_TOKEN unset, not applying scale change")
            continue
        ok = await _set_replicas(http, railway_token, project_id, env_id, name, desired)
        if ok:
            current_replicas[name] = desired


async def main() -> None:
    from shared import core_metrics
    core_metrics.init()

    interval = float(os.environ.get("SCALE_INTERVAL_S", "60"))
    hysteresis = int(os.environ.get("SCALE_HYSTERESIS", "1"))
    mgmt_url = os.environ.get(
        "RABBITMQ_MGMT_URL", "http://rabbitmq:15672"
    )
    mgmt_auth = (
        os.environ.get("RABBITMQ_MGMT_USER", "guest"),
        os.environ.get("RABBITMQ_MGMT_PASSWORD", "guest"),
    )
    railway_token = os.environ.get("RAILWAY_API_TOKEN")
    project_id = os.environ.get("RAILWAY_PROJECT_ID")
    env_id = os.environ.get("RAILWAY_ENVIRONMENT_ID")
    config = _load_config()

    current_replicas: Dict[str, int] = {
        svc["service_name"]: int(svc["min_replicas"]) for svc in config
    }

    if not railway_token:
        log.warning(
            "[autoscaler] RAILWAY_API_TOKEN unset — running in DRY-RUN mode "
            "(decisions logged, replicas not actually changed)"
        )
    log.info(
        "[autoscaler] starting: interval=%ss hysteresis=%d services=%s",
        interval, hysteresis, [s["service_name"] for s in config],
    )

    async with httpx.AsyncClient() as http:
        while True:
            try:
                await _tick(
                    http, mgmt_url, mgmt_auth, railway_token,
                    project_id, env_id, hysteresis, config, current_replicas,
                )
            except Exception as exc:
                log.exception("[autoscaler] tick failed: %s", exc)
            await asyncio.sleep(interval)


if __name__ == "__main__":
    asyncio.run(main())
