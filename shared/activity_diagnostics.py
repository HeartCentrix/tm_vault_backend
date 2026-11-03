"""Correlate Activity work without logging filters, queries or row contents."""
from __future__ import annotations

import asyncio
from contextvars import ContextVar
from functools import wraps
import logging
import time
from uuid import uuid4

logger = logging.getLogger(__name__)
_trace_id = ContextVar('activity_trace_id', default=None)


async def activity_phase(phase, awaitable):
    token = None
    if _trace_id.get() is None:
        token = _trace_id.set(uuid4().hex)
    started, outcome = time.monotonic(), 'complete'
    try:
        return await awaitable
    except asyncio.CancelledError:
        outcome = 'cancelled'
        raise
    except Exception as exc:
        outcome = type(exc).__name__
        raise
    finally:
        logger.info('activity trace=%s phase=%s elapsed_ms=%.1f outcome=%s',
                    _trace_id.get(), phase, (time.monotonic() - started) * 1000, outcome,
                    extra={'activity_trace_id': _trace_id.get()})
        if token is not None:
            _trace_id.reset(token)


def activity_span(phase):
    def decorate(function):
        @wraps(function)
        async def traced(*args, **kwargs):
            return await activity_phase(phase, function(*args, **kwargs))
        return traced
    return decorate


trace_activity = activity_span('request')
