import asyncio
import importlib
import logging

import pytest


async def test_activity_phases_correlate_without_query_or_error_payload(caplog):
    diagnostics = importlib.import_module('shared.activity_diagnostics')
    async def fail():
        raise RuntimeError('secret-row SQL WHERE credential=private')
    @diagnostics.trace_activity
    async def handler():
        with pytest.raises(RuntimeError):
            await diagnostics.activity_phase('batch_sql', fail())
        return {'items': []}
    with caplog.at_level(logging.INFO, logger='shared.activity_diagnostics'):
        assert await handler() == {'items': []}
    assert len(caplog.records) >= 2
    assert 'batch_sql' in caplog.text and 'request' in caplog.text
    assert 'secret-row' not in caplog.text and 'credential' not in caplog.text
    ids = {record.activity_trace_id for record in caplog.records}
    assert len(ids) == 1


async def test_activity_cancellation_is_not_swallowed(caplog):
    diagnostics = importlib.import_module('shared.activity_diagnostics')
    async def cancelled():
        raise asyncio.CancelledError
    @diagnostics.trace_activity
    async def handler():
        return await diagnostics.activity_phase('batch_sql', cancelled())
    with caplog.at_level(logging.INFO, logger='shared.activity_diagnostics'), pytest.raises(asyncio.CancelledError):
        await handler()
    assert 'cancelled' in caplog.text
