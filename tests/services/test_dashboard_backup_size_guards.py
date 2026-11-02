"""Summary-only size requests preserve scope, deadlines and honest freshness."""
from __future__ import annotations

import asyncio
import importlib.util
import os
from pathlib import Path
from uuid import uuid4

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")

from fastapi import HTTPException
import pytest


@pytest.fixture
def dashboard():
    path = Path(__file__).resolve().parents[2] / 'services/dashboard-service/main.py'
    spec = importlib.util.spec_from_file_location('dashboard_size_read_model', path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class Result:
    def __init__(self, rows=()):
        self.rows = rows

    def all(self):
        return list(self.rows)

    def scalars(self):
        return self


class DB:
    def __init__(self, *, org_tenants=(), delay=0, history=()):
        self.calls = []
        self.org_tenants = org_tenants
        self.delay = delay
        self.history = history
        self.rollbacks = 0

    async def execute(self, sql, params=None):
        statement = str(sql)
        self.calls.append((statement, params))
        if self.delay:
            await asyncio.sleep(self.delay)
        if 'SET ' in statement or 'set_config' in statement:
            return Result()
        if 'tenants' in statement and 'org_id' in statement:
            return Result(self.org_tenants)
        if 'GROUP BY' in statement and 'completed_at' in statement:
            return Result(self.history)
        raise AssertionError('unexpected SQL on size read path: ' + statement[:200])

    async def rollback(self):
        self.rollbacks += 1


class Request:
    def __init__(self, principal=None, disconnected=False):
        self.scope = {'tmvault_user': principal or {'roles': ['SUPER_ADMIN'], 'tenant_ids': []}}
        self.disconnected = disconnected

    async def is_disconnected(self):
        return self.disconnected


def summary(**changes):
    return dict(totalBytes=200, eligibleCount=2, freshCount=2, staleCount=0,
                missingCount=0, partial=False, stale=False, freshness='fresh',
                asOf='2026-11-02T00:00:00Z', initializingScope=False,
                metricVersion='retained-logical-v1.1') | changes


async def invoke(mod, monkeypatch, *, result=None, db=None, request=None, tenant=None):
    seen = []

    async def read(db, *, tenant_ids, service_types):
        seen.append((tenant_ids, service_types))
        return summary() if result is None else result

    monkeypatch.setattr(mod, '_read_size_summary', read, raising=False)
    response = await mod.get_backup_size(request=request or Request(), tenantId=tenant,
                                        serviceType='m365', tz='Asia/Kolkata', db=db or DB())
    return response, seen


async def test_endpoint_reads_saved_totals_without_history_item_queries(dashboard, monkeypatch):
    db = DB()
    response, seen = await invoke(dashboard, monkeypatch, db=db)
    assert response['totalBytes'] == 200
    assert response['freshness'] == 'fresh'
    assert response['partial'] is False
    assert len(seen) == 1
    assert 'USER_ARCHIVE' in seen[0][1]
    assert not any('snapshot_items' in sql or 'GROUP BY snapshots.resource_id' in sql for sql, _ in db.calls)


@pytest.mark.parametrize('state', [
    dict(stale=True, freshness='stale', freshCount=1, staleCount=1),
    dict(partial=True, freshness='building', totalBytes=None, missingCount=2, freshCount=0),
    dict(partial=True, freshness='building', initializingScope=True),
])
async def test_uncertain_baseline_never_produces_projection(dashboard, monkeypatch, state):
    response, _ = await invoke(dashboard, monkeypatch, result=summary(**state))
    assert response['projectedBytes1y'] is None
    assert response['projectedBytes3y'] is None
    assert response['freshness'] == state['freshness']
    if response['totalBytes'] is None:
        assert response['total'] == 'Calculating'


async def test_omitted_tenant_uses_verified_user_entitlements(dashboard, monkeypatch):
    allowed = uuid4()
    req = Request({'roles': [], 'tenant_ids': [str(allowed)]})
    _, seen = await invoke(dashboard, monkeypatch, request=req)
    assert seen[0][0] == (allowed,)


async def test_empty_user_scope_does_not_become_global(dashboard, monkeypatch):
    _, seen = await invoke(dashboard, monkeypatch, request=Request({'roles': [], 'tenant_ids': []}))
    assert seen[0][0] == ()


async def test_explicit_forbidden_tenant_is_rejected_before_summary(dashboard, monkeypatch):
    with pytest.raises(HTTPException) as error:
        await invoke(dashboard, monkeypatch, request=Request({'roles': [], 'tenant_ids': []}), tenant=str(uuid4()))
    assert error.value.status_code == 403


async def test_org_admin_scope_is_own_organization_not_global(dashboard, monkeypatch):
    tenant, organization = uuid4(), uuid4()
    _, seen = await invoke(dashboard, monkeypatch, db=DB(org_tenants=[tenant]),
        request=Request({'roles': ['ORG_ADMIN'], 'org_id': str(organization), 'tenant_ids': []}))
    assert seen[0][0] == (tenant,)


async def test_tenant_admin_does_not_get_global_admin_scope(dashboard, monkeypatch):
    tenant = uuid4()
    _, seen = await invoke(dashboard, monkeypatch,
        request=Request({'roles': ['TENANT_ADMIN'], 'tenant_ids': [str(tenant)]}))
    assert seen[0][0] == (tenant,)


async def test_entitlement_query_is_inside_whole_handler_budget(dashboard, monkeypatch):
    monkeypatch.setattr(dashboard, '_SIZE_REQUEST_BUDGET_S', .01, raising=False)
    with pytest.raises(HTTPException) as error:
        await invoke(dashboard, monkeypatch, db=DB(delay=.2),
            request=Request({'roles': ['ORG_ADMIN'], 'org_id': str(uuid4()), 'tenant_ids': []}))
    assert error.value.status_code == 503


async def test_disconnected_client_does_not_start_summary_or_history(dashboard, monkeypatch):
    db = DB()
    with pytest.raises(HTTPException) as error:
        await invoke(dashboard, monkeypatch, db=db, request=Request(disconnected=True))
    assert error.value.status_code == 499
    assert db.calls == []
