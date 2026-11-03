"""Pins for the discovery Activity-row pairing fixes (2026-07-15).

One logical discovery run must produce exactly one STARTED event stream
entry, or the Activity feed's greedy STARTED↔RUN pairing wedges: the
orphan STARTED steals the NEXT run's terminal event and every discovery
row after it renders "In Progress" forever (manual trigger of
2026-07-14 17:34 emitted STARTED from both tenant-service and
discovery-worker, 1s apart).

Two guarantees, pinned by source inspection (the pairing is inline in
audit-service's /activity route; importing the service mains pulls the
whole FastAPI dependency graph — same trade-off as
test_scheduler_sweep_lease_aware.py):

1. tenant-service's manual /discover trigger does NOT pre-emit
   DISCOVERY_STARTED — the discovery-worker's emit is canonical.
2. audit-service collapses duplicate STARTEDs for the same
   (tenant, resource_type) within a short window before pairing, so
   historical duplicates already in audit_events also render correctly.
"""
from __future__ import annotations

import pathlib

_ROOT = pathlib.Path(__file__).resolve().parents[2]


def _src(rel: str) -> str:
    return (_ROOT / rel).read_text()


def test_tenant_service_does_not_pre_emit_discovery_started():
    src = _src("services/tenant-service/main.py")
    assert '"DISCOVERY_STARTED"' not in src, (
        "tenant-service emits DISCOVERY_STARTED again — manual discovery "
        "triggers will double-emit and wedge the Activity feed pairing"
    )


def test_discovery_worker_still_emits_started():
    # The canonical emitter must survive; deleting BOTH emitters would
    # leave discovery rows permanently unpaired in the other direction.
    src = _src("workers/discovery-worker/main.py")
    assert '"DISCOVERY_STARTED"' in src


def test_audit_service_dedupes_duplicate_started_events():
    src = _src("services/audit-service/main.py")
    assert "_DUP_STARTED_WINDOW_S" in src, (
        "audit-service lost the duplicate-STARTED collapse — historical "
        "double-emits in audit_events will wedge discovery rows again"
    )
    # BOTH pairing sites must dedupe: _list_activities_batch (the default
    # group=batch path the UI hits) AND list_activities' legacy branch.
    # 2026-07-15: the first fix only patched the legacy branch and the
    # feed stayed wedged.
    assert src.count("run_between") >= 2, (
        "a discovery-pairing site lost its duplicate-STARTED collapse — "
        "the default group=batch path and the legacy branch both need it"
    )
