"""Prometheus metrics for backup/restore/discovery hot paths and operational
cost telemetry.

Single source of truth for metrics emitted by workers + services in TMvault.
Mirrors the lazy-init / no-op fallback pattern from `shared/pst_metrics.py`
and `shared/sla_metrics.py` so a single container can stack multiple metrics
modules without conflict (each binds its own port).

Two concerns combined here:

1. **Operational observability** — what the system is doing right now.
   Counters for jobs/snapshots, histograms for Graph API latency, gauges
   for queue depth + PG pool usage. Drives Grafana dashboards and the
   autoscaler's scale-out signal.

2. **Operational cost telemetry** — what the system consumed.
   Bytes ingested, bytes egressed, compute seconds, Graph API quota
   burn. *Not* a billing/subscription model — purely operational
   visibility ("how much storage did we provision this week, where did
   it go?"). Per-tenant labels are present so the same counters keep
   working when we go multi-tenant; today they're all the single tenant.

Scrape port: $CORE_METRICS_PORT (default 9103). Distinct from
PST_METRICS_PORT (9100), SLA_METRICS_PORT (9101), and the chat-export-worker's
own scrape on 9102 — so a container that wires several of them does not
fight for the same port.

Wire from each entry point:

    from shared.core_metrics import init as core_metrics_init
    core_metrics_init()

Then call helpers from hot paths:

    from shared import core_metrics as cm
    cm.inc_job("USER_MAIL", "completed")
    cm.observe_graph_call("messages.delta", 0.42, "success")
    cm.add_backup_bytes("USER_ONEDRIVE", "seaweedfs-local", 1024*1024*371)
"""
from __future__ import annotations

import logging
import os
import threading
import time
from contextlib import contextmanager
from typing import Optional

logger = logging.getLogger(__name__)

_ENABLED = False
_INIT_DONE = False
_LOCK = threading.Lock()

# ─── Observability metrics ────────────────────────────────────────────
jobs_total = None                  # Counter[type, status]
snapshots_total = None             # Counter[type, status]
backup_duration_seconds = None     # Histogram[type, status]
graph_api_calls_total = None       # Counter[endpoint, status]
graph_api_duration_seconds = None  # Histogram[endpoint]
graph_throttles_total = None       # Counter[app_id, reason]
graph_rate_limited_waits_total = None  # Counter[reason]  # global limiter waits
graph_rate_limit_wait_seconds = None   # Histogram[reason]
discovery_504_total = None         # Counter[outcome]  # success|permanent_pending
storage_write_bytes_total = None   # Counter[backend, kind]  # seaweedfs/azure × {mail,chat,onedrive,sp}
storage_write_seconds = None       # Histogram[backend]
queue_depth_gauge = None           # Gauge[queue]
worker_active_jobs_gauge = None    # Gauge[worker_type]
pg_pool_in_use_gauge = None        # Gauge[service]
pg_pool_size_gauge = None          # Gauge[service]
worker_rss_mb_gauge = None         # Gauge[worker_type]

# ─── Cost telemetry counters (operational, not subscription) ─────────
cost_storage_bytes_total = None    # Counter[tenant, backend, kind]
cost_egress_bytes_total = None     # Counter[tenant, direction]   # to_graph / from_graph / to_seaweed / restore_out
cost_compute_seconds_total = None  # Counter[tenant, worker_type]
cost_graph_calls_total = None      # Counter[tenant, endpoint]
cost_seaweed_write_bytes_total = None  # Counter[tenant, shard]
cost_seaweed_read_bytes_total = None   # Counter[tenant, shard]


def init(metrics_port: Optional[int] = None) -> bool:
    """Bring up the /metrics HTTP server and register all counters/gauges.

    Idempotent. Safe to call from every worker entry point.

    Returns True when prometheus_client is wired and HTTP is bound.
    Returns False when prometheus_client is missing OR the port is
    already bound (e.g. another metrics module grabbed it); in both
    cases the inc_*/observe_*/set_* helpers are no-ops so callers
    don't need to guard.
    """
    global _ENABLED, _INIT_DONE
    global jobs_total, snapshots_total, backup_duration_seconds
    global graph_api_calls_total, graph_api_duration_seconds
    global graph_throttles_total, graph_rate_limited_waits_total
    global graph_rate_limit_wait_seconds, discovery_504_total
    global storage_write_bytes_total, storage_write_seconds
    global queue_depth_gauge, worker_active_jobs_gauge
    global pg_pool_in_use_gauge, pg_pool_size_gauge, worker_rss_mb_gauge
    global cost_storage_bytes_total, cost_egress_bytes_total
    global cost_compute_seconds_total, cost_graph_calls_total
    global cost_seaweed_write_bytes_total, cost_seaweed_read_bytes_total

    with _LOCK:
        if _INIT_DONE:
            return _ENABLED
        _INIT_DONE = True
        try:
            from prometheus_client import Counter, Histogram, Gauge, start_http_server
        except Exception as exc:
            logger.warning("[core_metrics] prometheus_client unavailable (%s) — metrics disabled", exc)
            return False

        # Observability
        jobs_total = Counter(
            "tmvault_jobs_total",
            "Backup/restore jobs by type and terminal status",
            ["type", "status"],
        )
        snapshots_total = Counter(
            "tmvault_snapshots_total",
            "Snapshots by resource type and terminal status",
            ["type", "status"],
        )
        backup_duration_seconds = Histogram(
            "tmvault_backup_duration_seconds",
            "Wall-clock duration of a backup job from start to terminal status",
            ["type", "status"],
            buckets=(1, 5, 10, 30, 60, 300, 900, 1800, 3600, 7200, 14400, 28800),
        )
        graph_api_calls_total = Counter(
            "tmvault_graph_api_calls_total",
            "Microsoft Graph API calls by endpoint and outcome",
            ["endpoint", "status"],
        )
        graph_api_duration_seconds = Histogram(
            "tmvault_graph_api_duration_seconds",
            "Graph API call latency (request to response, includes retries)",
            ["endpoint"],
            buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60),
        )
        graph_throttles_total = Counter(
            "tmvault_graph_throttles_total",
            "Per-app Graph throttles (429s) by application registration",
            ["app_id", "reason"],
        )
        graph_rate_limited_waits_total = Counter(
            "tmvault_graph_rate_limited_waits_total",
            "Times the global Graph rate limiter forced the caller to wait",
            ["reason"],
        )
        graph_rate_limit_wait_seconds = Histogram(
            "tmvault_graph_rate_limit_wait_seconds",
            "Seconds spent waiting on the global Graph rate limiter",
            ["reason"],
            buckets=(0.001, 0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30),
        )
        discovery_504_total = Counter(
            "tmvault_discovery_504_total",
            "Discovery 504 outcomes (retry success vs permanently flagged pending)",
            ["outcome"],
        )
        storage_write_bytes_total = Counter(
            "tmvault_storage_write_bytes_total",
            "Bytes written to a storage backend by resource kind",
            ["backend", "kind"],
        )
        storage_write_seconds = Histogram(
            "tmvault_storage_write_seconds",
            "Storage backend write latency per request",
            ["backend"],
            buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30),
        )
        queue_depth_gauge = Gauge(
            "tmvault_queue_depth",
            "Pending message count per queue (drives autoscaler)",
            ["queue"],
        )
        worker_active_jobs_gauge = Gauge(
            "tmvault_worker_active_jobs",
            "In-flight job count per worker type",
            ["worker_type"],
        )
        pg_pool_in_use_gauge = Gauge(
            "tmvault_pg_pool_in_use",
            "Postgres connection-pool connections currently checked out",
            ["service"],
        )
        pg_pool_size_gauge = Gauge(
            "tmvault_pg_pool_size",
            "Postgres connection-pool maximum size",
            ["service"],
        )
        worker_rss_mb_gauge = Gauge(
            "tmvault_worker_rss_mb",
            "Worker process resident-set-size (sampled)",
            ["worker_type"],
        )

        # Cost telemetry
        cost_storage_bytes_total = Counter(
            "tmvault_cost_storage_bytes_total",
            "Bytes resident in long-term storage by tenant / backend / resource kind",
            ["tenant", "backend", "kind"],
        )
        cost_egress_bytes_total = Counter(
            "tmvault_cost_egress_bytes_total",
            "Bytes egressed by tenant and direction (to_graph, from_graph, to_seaweed, restore_out)",
            ["tenant", "direction"],
        )
        cost_compute_seconds_total = Counter(
            "tmvault_cost_compute_seconds_total",
            "Worker compute seconds spent on tenant work",
            ["tenant", "worker_type"],
        )
        cost_graph_calls_total = Counter(
            "tmvault_cost_graph_calls_total",
            "Graph API calls attributed to tenant (drives quota / cost accounting)",
            ["tenant", "endpoint"],
        )
        cost_seaweed_write_bytes_total = Counter(
            "tmvault_cost_seaweed_write_bytes_total",
            "Bytes written to a specific SeaweedFS shard per tenant",
            ["tenant", "shard"],
        )
        cost_seaweed_read_bytes_total = Counter(
            "tmvault_cost_seaweed_read_bytes_total",
            "Bytes read from a specific SeaweedFS shard per tenant",
            ["tenant", "shard"],
        )

        port = metrics_port if metrics_port is not None else int(
            os.environ.get("CORE_METRICS_PORT", "9103")
        )
        try:
            start_http_server(port)
            logger.info("[core_metrics] HTTP server listening on :%d", port)
            _ENABLED = True
        except OSError as exc:
            logger.warning(
                "[core_metrics] could not bind :%d (%s) — counters live, HTTP scrape disabled",
                port, exc,
            )
            _ENABLED = False
        return _ENABLED


# ─── Safe helpers (no-op when prometheus_client missing) ─────────────

def _safe_inc(metric, labels: tuple, amount: float = 1.0) -> None:
    if metric is None:
        return
    try:
        (metric.labels(*labels) if labels else metric).inc(amount)
    except Exception as exc:
        logger.debug("[core_metrics] inc failed: %s", exc)


def _safe_observe(metric, value: float, labels: tuple = ()) -> None:
    if metric is None:
        return
    try:
        (metric.labels(*labels) if labels else metric).observe(value)
    except Exception as exc:
        logger.debug("[core_metrics] observe failed: %s", exc)


def _safe_set(metric, value: float, labels: tuple = ()) -> None:
    if metric is None:
        return
    try:
        (metric.labels(*labels) if labels else metric).set(value)
    except Exception as exc:
        logger.debug("[core_metrics] set failed: %s", exc)


# ─── Observability emitters ──────────────────────────────────────────

def inc_job(type_: str, status: str) -> None:
    _safe_inc(jobs_total, (type_, status))

def inc_snapshot(type_: str, status: str) -> None:
    _safe_inc(snapshots_total, (type_, status))

def observe_backup_duration(type_: str, status: str, seconds: float) -> None:
    _safe_observe(backup_duration_seconds, seconds, (type_, status))

def inc_graph_call(endpoint: str, status: str) -> None:
    _safe_inc(graph_api_calls_total, (endpoint, status))

def observe_graph_call(endpoint: str, seconds: float, status: str = "success") -> None:
    _safe_inc(graph_api_calls_total, (endpoint, status))
    _safe_observe(graph_api_duration_seconds, seconds, (endpoint,))

def inc_graph_throttle(app_id: str, reason: str) -> None:
    _safe_inc(graph_throttles_total, (app_id, reason))

def inc_graph_rate_limit_wait(reason: str, wait_seconds: float) -> None:
    _safe_inc(graph_rate_limited_waits_total, (reason,))
    _safe_observe(graph_rate_limit_wait_seconds, wait_seconds, (reason,))

def inc_discovery_504(outcome: str) -> None:
    _safe_inc(discovery_504_total, (outcome,))

def add_backup_bytes(kind: str, backend: str, n_bytes: int) -> None:
    _safe_inc(storage_write_bytes_total, (backend, kind), amount=float(n_bytes))

def observe_storage_write(backend: str, seconds: float) -> None:
    _safe_observe(storage_write_seconds, seconds, (backend,))

def set_queue_depth(queue: str, depth: int) -> None:
    _safe_set(queue_depth_gauge, float(depth), (queue,))

def set_worker_active_jobs(worker_type: str, n: int) -> None:
    _safe_set(worker_active_jobs_gauge, float(n), (worker_type,))

def set_pg_pool(service: str, in_use: int, size: int) -> None:
    _safe_set(pg_pool_in_use_gauge, float(in_use), (service,))
    _safe_set(pg_pool_size_gauge, float(size), (service,))

def set_worker_rss_mb(worker_type: str, rss_mb: float) -> None:
    _safe_set(worker_rss_mb_gauge, float(rss_mb), (worker_type,))


# ─── Cost telemetry emitters ─────────────────────────────────────────

def add_cost_storage(tenant: str, backend: str, kind: str, n_bytes: int) -> None:
    _safe_inc(cost_storage_bytes_total, (tenant, backend, kind), amount=float(n_bytes))

def add_cost_egress(tenant: str, direction: str, n_bytes: int) -> None:
    _safe_inc(cost_egress_bytes_total, (tenant, direction), amount=float(n_bytes))

def add_cost_compute(tenant: str, worker_type: str, seconds: float) -> None:
    _safe_inc(cost_compute_seconds_total, (tenant, worker_type), amount=float(seconds))

def inc_cost_graph_call(tenant: str, endpoint: str) -> None:
    _safe_inc(cost_graph_calls_total, (tenant, endpoint))

def add_cost_seaweed_write(tenant: str, shard: str, n_bytes: int) -> None:
    _safe_inc(cost_seaweed_write_bytes_total, (tenant, shard), amount=float(n_bytes))

def add_cost_seaweed_read(tenant: str, shard: str, n_bytes: int) -> None:
    _safe_inc(cost_seaweed_read_bytes_total, (tenant, shard), amount=float(n_bytes))


# ─── Context manager: time + count Graph call in one block ───────────

@contextmanager
def time_graph_call(endpoint: str, tenant: Optional[str] = None):
    """Use around a Graph API request to time + count it in one shot.

        with cm.time_graph_call("messages.mime", tenant=tenant_id):
            data = await graph_client.get_message_mime_source(...)
    """
    t0 = time.monotonic()
    status = "success"
    try:
        yield
    except Exception:
        status = "error"
        raise
    finally:
        elapsed = time.monotonic() - t0
        observe_graph_call(endpoint, elapsed, status)
        if tenant:
            inc_cost_graph_call(tenant, endpoint)


@contextmanager
def time_storage_write(backend: str):
    """Use around a SeaweedFS / Azure write call to time it."""
    t0 = time.monotonic()
    try:
        yield
    finally:
        observe_storage_write(backend, time.monotonic() - t0)
