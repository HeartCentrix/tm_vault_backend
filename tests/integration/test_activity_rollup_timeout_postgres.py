"""A slow Activity rollup must stop at its own statement timeout, not run on after the gateway.

api-gateway answers 504 after 60 s, but the audit-service handler kept its rollup query
running. On 10-21 TM's frontend polled every ~15 s, and every poll from 18:09 to 18:30 timed
out (catalogue #20). The rollup now has a statement timeout. When it fires, the feed still
returns its other sources and says the backup rows timed out. The CSV export refuses rather
than ship a file without its backups.
"""
import importlib.util
import os
import pathlib
import sys
import time
from datetime import datetime

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")

import pytest
from fastapi import HTTPException
from sqlalchemy import text

import shared.batch_rollup as batch_rollup
import shared.models as models
from test_activity_rollup_cost_postgres import db  # noqa: F401  (fixture)

pytestmark = pytest.mark.integration

_SVC_DIR = pathlib.Path(__file__).resolve().parents[2] / "services" / "audit-service"
if str(_SVC_DIR) not in sys.path:
    sys.path.insert(0, str(_SVC_DIR))
_spec = importlib.util.spec_from_file_location("audit_main_rollup_timeout", _SVC_DIR / "main.py")
_as = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = _as
_spec.loader.exec_module(_as)


@pytest.fixture
async def slow_rollup(db, monkeypatch):
    async with db.factory.begin() as s:
        s.add(models.AuditEvent(tenant_id=db.tenant, action="DISCOVERY_STARTED", resource_type="M365",
                                resource_name="Tenant discovery", occurred_at=datetime.utcnow()))
    monkeypatch.setattr(_as, "async_session_factory", db.factory)
    monkeypatch.setattr(_as, "ACTIVITY_ROLLUP_TIMEOUT_MS", 300)
    monkeypatch.setattr(batch_rollup, "build_batch_rollup_query",
                        lambda **kw: text("SELECT 1 FROM pg_sleep(5) WHERE random() < 0"))
    return db


async def test_feed_returns_its_other_rows_when_the_backup_rollup_times_out(slow_rollup):
    started = time.monotonic()
    body = await _as.list_activities(tenantId=str(slow_rollup.tenant), serviceType="m365", start_date=None,
                                     end_date=None, operation=None, status=None, page=1, size=50, group="batch")
    assert time.monotonic() - started < 3
    assert body["backup_rollup_timed_out"] is True
    assert [item["operation"] for item in body["items"]] == ["DISCOVERY"]


async def test_csv_export_refuses_when_the_backup_rollup_times_out(slow_rollup):
    with pytest.raises(HTTPException) as refused:
        await _as.export_activity_csv(tenantId=str(slow_rollup.tenant), serviceType="m365", start_date=None,
                                      end_date=None, operation=None, status=None, group="batch")
    assert refused.value.status_code == 503
