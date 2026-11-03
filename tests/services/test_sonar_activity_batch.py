"""Behavioral coverage for the batch Activity feed's extracted query and shaping phases."""
import importlib.util
from datetime import datetime, timedelta
from pathlib import Path
import sys
from types import SimpleNamespace
from uuid import UUID

import pytest


@pytest.fixture(scope="module")
def audit():
    path = Path(__file__).resolve().parents[2] / "services" / "audit-service" / "main.py"
    spec = importlib.util.spec_from_file_location("sonar_activity_batch", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


_TENANT = UUID("00000000-0000-0000-0000-000000000001")
_START = datetime(2026, 11, 1)


def event(name, action, second, **overrides):
    values = {
        "id": name, "action": action, "occurred_at": _START + timedelta(seconds=second),
        "tenant_id": _TENANT, "resource_type": "M365", "resource_name": None,
        "details": {}, "outcome": "SUCCESS",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_discovery_pairing_keeps_tenant_boundaries_and_runs_between_starts(audit):
    other = UUID("00000000-0000-0000-0000-000000000002")
    events = [
        event("orphan", "DISCOVERY_RUN", 0, resource_type="AZURE", outcome="FAILURE", details={"error": "denied"}),
        event("start", "DISCOVERY_STARTED", 10, resource_name="Tenant discovery"),
        event("duplicate", "DISCOVERY_STARTED", 11),
        event("other", "DISCOVERY_STARTED", 12, tenant_id=other),
        event("run", "DISCOVERY_RUN", 20, details={"resourcesFound": 0}),
        event("next", "DISCOVERY_STARTED", 30),
        event("next-run", "DISCOVERY_RUN", 40, details={"resourcesFound": 3}),
        event("pending", "DISCOVERY_STARTED", 200),
    ]
    rows = audit._discovery_activity_rows(list(reversed(events)), None)
    by_id = {row["id"]: row for row in rows}
    assert len(rows) == 5
    assert by_id["discovery-run"]["object"] == "Tenant discovery"
    assert by_id["discovery-run"]["start_time"] == events[1].occurred_at.isoformat()
    assert by_id["discovery-run"]["details"] == "M365 discovery completed — 0 resources"
    assert by_id["discovery-next-run"]["start_time"] == events[5].occurred_at.isoformat()
    assert by_id["discovery-other"]["status"] == "In Progress"
    assert by_id["discovery-pending"]["finish_time"] == ""
    assert by_id["discovery-orphan"]["details"] == "AZURE discovery failed: denied"
    filtered = audit._discovery_activity_rows(events, "Done")
    assert {row["id"] for row in filtered} == {"discovery-run", "discovery-next-run"}


@pytest.mark.parametrize("details, expected", [
    ({"message": "Saved warning"}, "Saved warning"),
    ({"anomaly_type": "ITEM_MASS_DELETION", "deleted_item_count": 9, "avg_prior_item_count": 10, "drop_pct": 90},
     "Ransomware signal: 9 items deleted (90% of live inventory)."),
    ({"anomaly_type": "ITEM_COUNT_DROP", "avg_prior_item_count": 10, "current_item_count": 0},
     "Ransomware signal: item count dropped from avg 10 to 0."),
    ({"anomaly_type": "ITEM_MASS_DELETION", "deleted_item_count": 9},
     "Ransomware signal detected (ITEM_MASS_DELETION)."),
])
def test_warning_messages_preserve_saved_evidence_and_zero_counts(audit, details, expected):
    row = audit._warning_activity_row(event("warning", "RANSOMWARE_SIGNAL", 0, details=details))
    assert row["details"] == expected
    assert row["status"] == "Warning"
    assert row["operation"] == "BACKUP"


@pytest.mark.parametrize("operation,status,sources", [
    (None, None, ["batch", "warning", "discovery"]),
    ("backup", "Warning", ["warning"]),
    ("discovery", None, ["discovery"]),
    (None, "Done", ["batch", "discovery"]),
    ("RESTORE", None, []),
])
@pytest.mark.asyncio
async def test_batch_feed_keeps_source_filters_and_fixed_scan_scope(audit, monkeypatch, operation, status, sources):
    from shared import batch_rollup

    queries = []
    batch_stmt = object()

    def build_query(**kwargs):
        assert kwargs == {
            "tenant_id": str(_TENANT), "start_date": "2026-11-01", "end_date": "2026-11-02",
            "operation": "BACKUP", "size": audit.ACTIVITY_SCAN_CAP, "offset": 0,
        }
        return batch_stmt

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def execute(self, stmt):
            if stmt is batch_stmt:
                queries.append("batch")
                return SimpleNamespace(all=lambda: [
                    {"id": "done", "status": "Done", "start_time": "2026-11-01T00:01:00"},
                    {"id": "failed", "status": "Failed", "start_time": "2026-11-01T00:00:00"},
                ])
            compiled = stmt.compile()
            params = compiled.params
            assert params["tenant_id_1"] == _TENANT
            assert params["occurred_at_1"] == _START
            assert params["occurred_at_2"] == _START + timedelta(days=1)
            assert params["param_1"] == audit.ACTIVITY_SCAN_CAP
            if set(params["action_1"]) == set(audit.WARNING_ACTIVITY_ACTIONS):
                queries.append("warning")
                assert set(params["resource_type_1"]) == {kind.value for kind in audit.M365_RESOURCE_TYPES}
                rows = [event("warning", "RANSOMWARE_SIGNAL", 30)]
            else:
                queries.append("discovery")
                assert set(params["action_1"]) == set(audit.DISCOVERY_ACTIVITY_ACTIONS)
                assert params["resource_type_1"] == "M365"
                rows = [event("discovery", "DISCOVERY_RUN", 15)]
            return SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: rows))

    monkeypatch.setattr(batch_rollup, "build_batch_rollup_query", build_query)
    monkeypatch.setattr(batch_rollup, "shape_batch_row", dict)
    monkeypatch.setattr(audit, "async_session_factory", Session)
    params = {
        "tenantId": str(_TENANT), "serviceType": "m365", "start_date": "2026-11-01",
        "end_date": "2026-11-02", "operation": operation, "status": status,
    }
    first = await audit._list_activities_batch(**params, page=1, size=1)
    assert queries == sources
    queries.clear()
    full = await audit._list_activities_batch(**params, page=1, size=50)
    second = await audit._list_activities_batch(**params, page=2, size=1)
    assert first["total"] == full["total"]
    assert second["total"] == full["total"]
    assert first["items"] == full["items"][:1]
    assert second["items"] == full["items"][1:2]
    assert first["has_more"] == (first["total"] > 1)
    if status:
        assert all(row["status"] == status for row in full["items"])
