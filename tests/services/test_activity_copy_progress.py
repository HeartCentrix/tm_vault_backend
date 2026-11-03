"""Existing Activity pages attach only visible jobs' bounded copy summaries."""
import asyncio
import importlib.util
import os
from pathlib import Path
import sys
import uuid

from fastapi.testclient import TestClient
import pytest

os.environ.setdefault('ALLOW_DEV_JWT_SECRETS', 'true')
os.environ.setdefault('JWT_SECRET', 'unit-test-only-not-a-real-secret')
path = Path(__file__).resolve().parents[2] / 'services' / 'audit-service' / 'main.py'
spec = importlib.util.spec_from_file_location('activity_copy_progress_under_test', path)
audit = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = audit
spec.loader.exec_module(audit)


@pytest.fixture(autouse=True)
def clear_dependencies():
    yield
    audit.app.dependency_overrides.clear()


@pytest.mark.parametrize('redesign', [False, True])
def test_copy_progress_aggregates_only_final_visible_page(monkeypatch, redesign):
    tenant = str(uuid.uuid4())
    jobs = [str(uuid.uuid4()) for _ in range(4)]
    items = [{'id': job, 'jobIds': [job], 'operation': 'BACKUP', 'status': 'Done',
              'start_time': f'2026-11-0{4-index}T00:00:00'} for index, job in enumerate(jobs)]
    calls, provider_calls = [], []

    async def batches(**params):
        assert params['tenantId'] == tenant
        return {'items': items if redesign else items[1:2], 'total': 4, 'page': 2,
                'size': 1, 'has_more': True}

    async def v2(**params):
        return []

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

    async def summaries(session, *, job_ids):
        calls.append(job_ids)
        return {jobs[1]: {'state': 'QUEUED', 'pending_tasks': 1, 'queued_tasks': 1,
                         'committed_rows': 7, 'committed_bytes': 70,
                         'policies': [{'sla_policy_id': 'private-id'}]}}

    async def providers(session, *, job_ids):
        provider_calls.append(job_ids)
        return {jobs[1]: {'state': 'WAITING', 'pending_leaves': 2, 'completed_units': 7,
                         'next_eligible_at': '2026-11-04T12:00:00+00:00', 'oldest_queue_age_s': 50}}

    monkeypatch.setattr('shared.provider_work.guards_enabled', lambda: True)
    monkeypatch.setattr('shared.provider_observability.provider_work_summaries', providers)
    monkeypatch.setattr(audit.settings, 'BATCH_ROW_REDESIGN_ENABLED', redesign)
    monkeypatch.setattr(audit, '_list_activities_batch', batches)
    monkeypatch.setattr(audit, '_list_activities_batch_v2', v2)
    monkeypatch.setattr(audit, 'copy_work_guards_enabled', lambda: True, raising=False)
    monkeypatch.setattr(audit, 'copy_work_summaries', summaries, raising=False)
    monkeypatch.setattr(audit, 'async_session_factory', Session)
    audit.app.dependency_overrides[audit.require_principal] = lambda: {'id': 'authenticated'}
    response = TestClient(audit.app).get('/api/v1/activity', params={
        'tenantId': tenant, 'page': 2, 'size': 1,
    })
    assert response.status_code == 200
    assert calls == provider_calls == [[jobs[1]]]
    assert response.json()['total'] == 4
    assert response.json()['has_more'] is True
    item = response.json()['items'][0]
    assert item['status'] == 'Done'
    assert item['copy_work']['state'] == 'QUEUED'
    assert item['copy_work']['committed_rows'] == 7
    assert item['provider_work']['state'] == 'WAITING'
    assert item['provider_work']['pending_leaves'] == 2
    assert item['provider_work']['completed_units'] == 7
    assert 'private-id' not in response.text


def test_activity_copy_progress_still_requires_authentication(monkeypatch):
    async def batches(**params):
        pytest.fail('anonymous request reached Activity data')

    monkeypatch.setattr(audit, '_list_activities_batch', batches)
    assert TestClient(audit.app).get('/api/v1/activity').status_code == 401


def test_flag_off_preserves_payload_and_does_not_open_a_session(monkeypatch):
    payload = {'items': [{'id': str(uuid.uuid4()), 'operation': 'BACKUP'}], 'total': 1}
    monkeypatch.setattr(audit, 'copy_work_guards_enabled', lambda: False, raising=False)

    def session():
        pytest.fail('flag-off copy progress opened a database session')

    monkeypatch.setattr(audit, 'async_session_factory', session)
    assert asyncio.run(audit._attach_activity_copy_work(payload)) is payload


def test_batch_copy_summary_combines_jobs_without_exposing_diagnostics(monkeypatch):
    jobs = [str(uuid.uuid4()) for _ in range(2)]
    item = {'id': 'batch', 'batchId': 'batch', 'jobIds': jobs + jobs[:1],
            'operation': 'BACKUP', 'status': 'Done'}
    payload = {'items': [item, {'id': 'warning-event', 'operation': 'BACKUP', 'status': 'Warning'}]}

    async def summaries(session, *, job_ids):
        assert job_ids == jobs
        return {
            jobs[0]: {'state': 'COMPLETE', 'committed_rows': 5, 'sql_runtime_s': 3},
            jobs[1]: {'state': 'FAILED', 'committed_rows': 2, 'failed_tasks': 1,
                      'gap_counts': {'copy_execution_failed': 1}, 'sql_runtime_s': 4},
        }

    monkeypatch.setattr(audit, 'copy_work_guards_enabled', lambda: True, raising=False)
    monkeypatch.setattr(audit, 'copy_work_summaries', summaries, raising=False)
    result = asyncio.run(audit._attach_activity_copy_work(payload, session=object()))
    summary = result['items'][0]['copy_work']
    assert summary['state'] == 'FAILED'
    assert summary['committed_rows'] == 7
    assert summary['sql_runtime_s'] == 7
    assert summary['gap_counts'] == {'copy_execution_failed': 1}
    assert 'copy_work' not in result['items'][1]
