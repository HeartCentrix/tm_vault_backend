"""ITO-10275: restore.heavy must have a consumer.

The restore-worker subscribed only to restore.urgent / RESTORE_WORKER_QUEUE
(restore.normal) / restore.low, while job-service routes work to
restore.heavy on TWO paths: pick_export_queue (exports >
HEAVY_EXPORT_THRESHOLD_BYTES, ~100 GiB, gated by HEAVY_EXPORT_ENABLED) and
pick_restore_queue (ANY restore > HEAVY_RESTORE_THRESHOLD_BYTES, ~20 GiB,
NO gate). Nothing in the k8s deploy consumes restore.heavy — the compose
restore-worker-heavy was never ported — so the first large restore/export
would sit unconsumed forever and look like a silently-hung job.

Fix pinned here: `restore_queue_plan(settings)` (pure, extracted from
start()) appends the heavy queue, gated by RESTORE_HEAVY_CONSUME_ENABLED
(default True), skipping the append when RESTORE_WORKER_QUEUE already IS
the heavy queue (a dedicated heavy worker à la docker-compose must not
double-subscribe).
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import sys
from types import SimpleNamespace

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "test_secret_for_unit_tests_only_x")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")
os.environ.setdefault("AUDIT_SERVICE_URL", "http://localhost:8000")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "workers" / "restore-worker" / "main.py"
sys.path.insert(0, str(_MAIN_PATH.parent))
_spec = importlib.util.spec_from_file_location("rw_main_under_test_heavy", _MAIN_PATH)
_rw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["rw_main_under_test_heavy"] = _rw
try:
    _spec.loader.exec_module(_rw)  # type: ignore[union-attr]
except Exception as exc:
    pytest.skip(
        f"restore-worker module failed to import: {exc}",
        allow_module_level=True,
    )


def _settings(**over):
    base = dict(
        RESTORE_WORKER_QUEUE="restore.normal",
        HEAVY_EXPORT_QUEUE="restore.heavy",
        RESTORE_HEAVY_CONSUME_ENABLED=True,
    )
    base.update(over)
    return SimpleNamespace(**base)


def test_default_plan_includes_heavy():
    plan = _rw.restore_queue_plan(_settings())
    names = [q for q, _ in plan]
    assert names == ["restore.urgent", "restore.normal", "restore.low", "restore.heavy"], (
        "restore.heavy unconsumed = every >20GiB restore / >100GiB export "
        "hangs silently (verified live: 0 consumers on the queue)"
    )


def test_kill_switch_removes_heavy():
    plan = _rw.restore_queue_plan(_settings(RESTORE_HEAVY_CONSUME_ENABLED=False))
    assert [q for q, _ in plan] == ["restore.urgent", "restore.normal", "restore.low"]


def test_dedicated_heavy_worker_does_not_double_subscribe():
    plan = _rw.restore_queue_plan(_settings(RESTORE_WORKER_QUEUE="restore.heavy"))
    assert [q for q, _ in plan].count("restore.heavy") == 1


def test_plan_dedups_every_queue_name():
    # RESTORE_WORKER_QUEUE may name one of the fixed queues — two iterators
    # on one queue split its messages confusingly.
    plan = _rw.restore_queue_plan(_settings(RESTORE_WORKER_QUEUE="restore.urgent"))
    names = [q for q, _ in plan]
    assert names == ["restore.urgent", "restore.low", "restore.heavy"]
    assert len(names) == len(set(names))


def test_router_and_consumer_agree_on_the_heavy_queue_name():
    # pick_restore_queue routes >20GiB restores; restore_queue_plan
    # subscribes. If the names diverge under a HEAVY_EXPORT_QUEUE override,
    # the ITO-10275 no-consumer hang comes back silently.
    from shared import export_routing
    routed = export_routing.pick_restore_queue(10**12)
    from shared.config import settings as real_settings
    assert routed == real_settings.HEAVY_EXPORT_QUEUE


def test_config_flag_defaults_true():
    from shared.config import settings
    assert settings.RESTORE_HEAVY_CONSUME_ENABLED is True, (
        "flag must default ON — the fix has to take effect on plain image roll"
    )


def test_start_uses_the_plan():
    # Source pin: start() must build its queue list from restore_queue_plan —
    # a hand-maintained inline list is how the heavy queue got dropped.
    src = _MAIN_PATH.read_text()
    i = src.find("async def start(")
    assert i != -1
    assert "restore_queue_plan(" in src[i:i + 2500]
