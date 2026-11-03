"""Workflow errors retain their internal reason without logging provider content."""
from uuid import uuid4

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


async def test_unexpected_resume_error_does_not_log_sensitive_exception_text(caplog):
    async def resume(workflow_id):
        raise RuntimeError('https://private.invalid?access_token=secret-example')

    runner = CopyWorkRunner(None, resume)
    with caplog.at_level('WARNING', logger='shared.copy_runtime'):
        assert not await runner._resume(uuid4())
    assert 'RuntimeError' in caplog.text
    assert 'secret-example' not in caplog.text
    assert 'test_workflow_diagnostics.py' in caplog.text
