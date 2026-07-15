"""Prometheus metrics for SLA-policy reconcile / sweeper / CMK paths.

Mirrors `shared/pst_metrics.py` so the same scrape pattern works:
    - lazy init (no cost in pods that never reconcile)
    - all metrics are no-ops if prometheus_client is unavailable
    - exposes /metrics over HTTP on SLA_METRICS_PORT (default 9101)

Wire from backup-scheduler entry point via:
    from shared.sla_metrics import init as sla_metrics_init
    sla_metrics_init()
and then call helpers like `inc_reconcile(status='success')` from the body.

Scrape with:
    curl -s http://backup-scheduler:9101/metrics
"""
from __future__ import annotations

import logging
import os
import threading
from typing import Optional

logger = logging.getLogger(__name__)

_ENABLED = False
_INIT_DONE = False
_HTTP_STARTED = False
_LOCK = threading.Lock()

# Counters
reconcile_total = None              # status (success|failure|transient)
worm_locks_applied_total = None     # mode (Locked|Unlocked|None)
worm_loosen_refused_total = None    # ()
cmk_drift_detected_total = None     # ()
cmk_rotated_total = None            # ()
encryption_status_transitions_total = None  # from→to
attempt_cap_reached_total = None    # ()
audit_publish_failed_total = None   # ()
blob_writes_total = None            # at-rest encryption: labelled encrypted=true|false

# Gauges
dirty_policies_gauge = None         # current count of lifecycle_dirty=true rows
encryption_write_enabled_gauge = None  # 1 when this process is encrypting new writes


def init(metrics_port: Optional[int] = None) -> bool:
    """Lazy-initialize metrics + bring up the /metrics HTTP server.
    Idempotent — safe to call multiple times.
    Returns True if Prometheus is wired and listening, False otherwise.

    Port resolution: explicit arg > $SLA_METRICS_PORT > 9101.

    Failure modes are non-fatal: if prometheus_client is missing or the
    port is already bound (e.g. multiple workers in one container, the
    pst_metrics one already grabbed it) we log a warning and return
    False — the inc_*/set_* helpers are no-ops in that case so callers
    don't need to guard.
    """
    global _ENABLED, _INIT_DONE, _HTTP_STARTED
    global reconcile_total, worm_locks_applied_total, worm_loosen_refused_total
    global cmk_drift_detected_total, cmk_rotated_total, encryption_status_transitions_total
    global attempt_cap_reached_total, audit_publish_failed_total, dirty_policies_gauge
    global blob_writes_total, encryption_write_enabled_gauge

    with _LOCK:
        if _INIT_DONE:
            return _ENABLED
        _INIT_DONE = True
        try:
            from prometheus_client import Counter, Gauge, start_http_server
        except Exception as exc:
            logger.warning("[sla_metrics] prometheus_client unavailable (%s)", exc)
            return False

        reconcile_total = Counter(
            "sla_lifecycle_reconcile_total",
            "SLA lifecycle reconcile attempts by terminal status",
            ["status"],  # 'success' | 'failure' | 'transient'
        )
        worm_locks_applied_total = Counter(
            "sla_worm_locks_applied_total",
            "Container WORM lock transitions applied",
            ["mode"],  # 'Locked' | 'Unlocked' | 'None'
        )
        worm_loosen_refused_total = Counter(
            "sla_worm_loosen_refused_total",
            "Refusals to loosen a Locked container immutability policy",
        )
        cmk_drift_detected_total = Counter(
            "sla_cmk_drift_detected_total",
            "CMK encryption-scope drift detected (live keyUri != policy intent)",
        )
        cmk_rotated_total = Counter(
            "sla_cmk_rotated_total",
            "CMK key rotations applied (latest-version resolution stamped a new version)",
        )
        encryption_status_transitions_total = Counter(
            "sla_encryption_status_transitions_total",
            "Transitions of policy.encryption_status",
            ["from_status", "to_status"],
        )
        attempt_cap_reached_total = Counter(
            "sla_reconcile_attempt_cap_reached_total",
            "Policies that hit the reconcile attempt cap and stopped retrying",
        )
        audit_publish_failed_total = Counter(
            "sla_audit_publish_failed_total",
            "Audit-event publishes that failed (observability, not blocking)",
        )
        dirty_policies_gauge = Gauge(
            "sla_lifecycle_dirty_policies",
            "Current count of policies awaiting lifecycle reconcile",
        )
        blob_writes_total = Counter(
            "backup_blob_writes_total",
            "Backup blob writes by at-rest encryption outcome",
            ["encrypted"],  # 'true' | 'false'
        )
        encryption_write_enabled_gauge = Gauge(
            "backup_encryption_write_enabled",
            "1 if this process encrypts new backup writes, else 0",
        )

        # Bring up /metrics on a separate port from pst_metrics (9100)
        # so a single container running both worker types can scrape both.
        port = metrics_port if metrics_port is not None else int(
            os.environ.get("SLA_METRICS_PORT", "9101")
        )
        try:
            start_http_server(port)
            logger.info("[sla_metrics] HTTP server listening on :%d", port)
            _ENABLED = True
            _HTTP_STARTED = True
        except OSError as exc:
            # Port collisions (e.g. another worker in this container is
            # already serving on the same port) are non-fatal — counters
            # still register on the default registry, but no HTTP scrape.
            logger.warning(
                "[sla_metrics] could not bind :%d (%s) — counters live, HTTP scrape disabled",
                port, exc,
            )
            _ENABLED = False
        return _ENABLED


# Convenience helpers — silent no-ops when metrics unavailable.

def inc_reconcile(status: str) -> None:
    if reconcile_total is not None:
        reconcile_total.labels(status=status).inc()

def inc_worm(mode: str) -> None:
    if worm_locks_applied_total is not None:
        worm_locks_applied_total.labels(mode=mode).inc()

def inc_worm_loosen_refused() -> None:
    if worm_loosen_refused_total is not None:
        worm_loosen_refused_total.inc()

def inc_cmk_drift() -> None:
    if cmk_drift_detected_total is not None:
        cmk_drift_detected_total.inc()

def inc_cmk_rotated() -> None:
    if cmk_rotated_total is not None:
        cmk_rotated_total.inc()

def inc_encryption_transition(from_s: str, to_s: str) -> None:
    if encryption_status_transitions_total is not None:
        encryption_status_transitions_total.labels(from_status=from_s or "_none_", to_status=to_s or "_none_").inc()

def inc_attempt_cap() -> None:
    if attempt_cap_reached_total is not None:
        attempt_cap_reached_total.inc()

def inc_audit_publish_failed() -> None:
    if audit_publish_failed_total is not None:
        audit_publish_failed_total.inc()

def set_dirty_count(n: int) -> None:
    if dirty_policies_gauge is not None:
        dirty_policies_gauge.set(n)

def record_blob_write(encrypted: bool) -> None:
    """Count one backup blob write by whether it was sealed at rest. Lets an
    operator alert when plaintext writes are non-zero while encryption is meant
    to be on (i.e. the 'encrypted at rest' claim is silently false)."""
    if blob_writes_total is not None:
        blob_writes_total.labels(encrypted=("true" if encrypted else "false")).inc()

def set_encryption_write_enabled(enabled: bool) -> None:
    if encryption_write_enabled_gauge is not None:
        encryption_write_enabled_gauge.set(1 if enabled else 0)
