"""Workflow errors retain their internal reason without logging provider content."""
from uuid import uuid4
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from shared.copy_runtime import CopyWorkRunner
from shared.copy_work import CopyWorkConflict


async def test_copy_resume_logs_conflict_reason_and_location(caplog):
    async def resume(workflow_id):
        raise CopyWorkConflict('copy resume leaf generation moved')

    runner = CopyWorkRunner(None, resume)
    key = uuid4()
    with caplog.at_level('WARNING', logger='shared.copy_runtime'):
        assert not await runner._resume(key)
    assert str(key) in caplog.text
    assert 'copy resume leaf generation moved' in caplog.text
    assert 'test_workflow_diagnostics.py' in caplog.text


@pytest.mark.parametrize('sensitive', [False, True])
async def test_renewal_failure_cancels_owner_and_logs_safe_reason(monkeypatch, caplog, sensitive):
    from shared import provider_work
    from shared.provider_runtime import ProviderWorkRunner

    @asynccontextmanager
    async def begin():
        yield object()

    async def renew(session, claim):
        if sensitive:
            raise RuntimeError('https://private.invalid?access_token=secret-example')
        raise provider_work.ProviderWorkConflict('provider work ownership changed')

    monkeypatch.setattr(provider_work, 'renew_work', renew)
    runner = ProviderWorkRunner(SimpleNamespace(begin=begin), None, None, renew_s=.01)
    claim = SimpleNamespace(work_id=uuid4(), snapshot_id=uuid4(), generation=3)
    execution = SimpleNamespace(claim=claim, lease_lost=False)
    owner = Mock()
    with caplog.at_level('WARNING', logger='shared.provider_runtime'):
        await runner._renew(execution, owner)
    assert execution.lease_lost
    owner.cancel.assert_called_once_with()
    assert str(claim.work_id) in caplog.text
    assert 'provider_lease_renewal_failed' in caplog.text
    assert ('RuntimeError' if sensitive else 'provider work ownership changed') in caplog.text
    assert 'secret-example' not in caplog.text


async def test_unexpected_resume_error_does_not_log_sensitive_exception_text(caplog):
    async def resume(workflow_id):
        raise RuntimeError('https://private.invalid?access_token=secret-example')

    runner = CopyWorkRunner(None, resume)
    with caplog.at_level('WARNING', logger='shared.copy_runtime'):
        assert not await runner._resume(uuid4())
    assert 'RuntimeError' in caplog.text
    assert 'secret-example' not in caplog.text
    assert 'test_workflow_diagnostics.py' in caplog.text
