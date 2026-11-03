"""audit-service's queue callback must not swallow persistence failures.

`consume_audit_events`'s callback wrapped its whole body in
`except Exception: print(...)`, so a failed INSERT looked like success to
the message bus and the audit event was destroyed. shared.message_bus
now nacks/dead-letters a message whose handler raises — but only if the
handler actually raises, so the callback has to re-raise after logging.

The async function is extracted from main.py via AST (same trick as
tests/services/test_group_batch_jobs_labels.py) so the test doesn't pull
in FastAPI / SQLAlchemy engines / RabbitMQ.
"""
from __future__ import annotations

import ast
import asyncio
import pathlib
import sys
import uuid as _uuid
from datetime import datetime

import pytest


_AUDIT_MAIN = (
    pathlib.Path(__file__).resolve().parents[2]
    / "services" / "audit-service" / "main.py"
)


class _FakeBus:
    def __init__(self):
        self.callback = None
        self.queue = None

    async def consume(self, queue_name, callback):
        self.queue = queue_name
        self.callback = callback


class _FailingSession:
    """Session whose commit blows up, like a DB outage mid-INSERT."""

    def __init__(self, added):
        self._added = added

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, model, ident):
        return None

    def add(self, obj):
        self._added.append(obj)

    async def commit(self):
        raise RuntimeError("db down")


def _load_consumer(added):
    source = _AUDIT_MAIN.read_text()
    tree = ast.parse(source)
    fn = next(
        (n for n in tree.body
         if isinstance(n, ast.AsyncFunctionDef)
         and n.name == "consume_audit_events"),
        None,
    )
    if fn is None:
        pytest.skip("consume_audit_events not found in audit-service/main.py",
                    allow_module_level=True)

    module = type(sys)("audit_consumer_under_test")
    bus = _FakeBus()
    module.__dict__.update({
        "message_bus": bus,
        "async_session_factory": lambda: _FailingSession(added),
        "_normalize_event": lambda e: e,
        "_detect_risk_signals": lambda e: [],
        "AuditEvent": type("AuditEvent", (), {
            "__init__": lambda self, **kw: self.__dict__.update(kw),
        }),
        "Resource": type("Resource", (), {}),
        "SlaPolicy": type("SlaPolicy", (), {}),
        "uuid": _uuid,
        "datetime": datetime,
    })
    code = compile(
        ast.Module(body=[fn], type_ignores=[]), str(_AUDIT_MAIN), "exec",
    )
    exec(code, module.__dict__)
    return module.consume_audit_events, bus


def _run(coro):
    """Run on a live loop, installing one if the suite left none behind.

    Other modules in this suite use asyncio.run(), which uninstalls the
    current event loop; get_event_loop() then raises depending on test
    order. Leave the loop installed (not closed) so later modules find a
    usable one too.
    """
    try:
        loop = asyncio.get_event_loop_policy().get_event_loop()
        if loop.is_closed():
            raise RuntimeError("closed")
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)


def test_audit_callback_reraises_so_the_bus_can_dead_letter():
    added = []
    consume_audit_events, bus = _load_consumer(added)
    _run(consume_audit_events())
    assert bus.callback is not None, "consumer never registered a callback"

    with pytest.raises(RuntimeError):
        _run(bus.callback({"action": "BACKUP_COMPLETED"}))

    assert added, "the event was never staged for INSERT"
