"""Detached version-list work must settle callers on deferral or cancellation."""
import ast
import asyncio
import os
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")

import httpx
import pytest

from shared.provider_admission import ProviderDeferred
from shared.provider_runtime import ProviderBudgetExhausted, ProviderTurnDeferred


@pytest.fixture
async def coordinator(monkeypatch):
    path = Path(__file__).resolve().parents[2] / "workers/backup-worker/main.py"
    tree = ast.parse(path.read_text())
    node = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "VersionListCoordinator")
    namespace = dict(asyncio=asyncio, Any=Any, Dict=Dict, List=List, Optional=Optional, Tuple=Tuple)
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), "exec"), namespace)
    created, cancel_before_start = [], set()
    create = asyncio.create_task

    def track(coro, *args, **kwargs):
        task = create(coro, *args, **kwargs)
        created.append(task)
        if coro.__name__ in cancel_before_start:
            task.cancel()
        return task

    monkeypatch.setattr(asyncio, "create_task", track)
    yield namespace["VersionListCoordinator"], created, cancel_before_start
    for task in created:
        if not task.done():
            task.cancel()
    await asyncio.gather(*created, return_exceptions=True)


def control_error(kind):
    return {
        "defer": ProviderDeferred(None, 60),
        "deadline": ProviderBudgetExhausted("original deadline exhausted"),
        "turn_defer": ProviderTurnDeferred(ProviderDeferred(None, 60)),
        "cancel": asyncio.CancelledError(),
    }[kind]


async def completed_results(tasks):
    _, pending = await asyncio.wait(tasks, timeout=.3)
    assert not pending, "version-list waiter remained unresolved after its flush ended"
    return await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.parametrize("stage", ["batch", "fallback"])
@pytest.mark.parametrize("kind", ["defer", "deadline", "turn_defer", "cancel"])
async def test_provider_control_flow_settles_every_waiter_without_retry(coordinator, stage, kind):
    Coordinator, created, _ = coordinator
    error, singles = control_error(kind), []

    async def batch(drive, ids):
        if stage == "batch":
            raise error
        return dict.fromkeys(ids)

    async def single(drive, id_):
        singles.append(id_)
        raise error

    coord = Coordinator(SimpleNamespace(list_file_versions_batch=batch, list_file_versions=single),
                        "drive", batch_size=2, max_wait_ms=10000)
    waiters = [asyncio.create_task(coord.get_versions(id_)) for id_ in ("a", "b")]
    results = await completed_results(waiters)

    assert all(isinstance(result, type(error)) for result in results)
    assert singles == ([] if stage == "batch" else ["a"])
    assert all(task.done() for task in created)


async def test_timer_triggered_deferral_reaches_its_caller(coordinator):
    Coordinator, created, _ = coordinator
    error = control_error("turn_defer")

    async def batch(drive, ids):
        raise error

    coord = Coordinator(SimpleNamespace(list_file_versions_batch=batch), "drive", max_wait_ms=50)
    waiter = asyncio.create_task(coord.get_versions("a"))
    assert (await completed_results([waiter]))[0] is error
    assert all(task.done() for task in created)


@pytest.mark.parametrize("stage", ["batch", "fallback"])
async def test_cancelled_waiter_does_not_break_its_live_sibling(coordinator, stage):
    Coordinator, created, _ = coordinator
    entered, release = asyncio.Event(), asyncio.Event()

    async def batch(drive, ids):
        if stage == "fallback":
            return dict.fromkeys(ids)
        entered.set()
        await release.wait()
        return {id_: [{"id": id_}] for id_ in ids}

    async def single(drive, id_):
        if id_ == "a":
            entered.set()
            await release.wait()
        return [{"id": id_}]

    coord = Coordinator(SimpleNamespace(list_file_versions_batch=batch, list_file_versions=single),
                        "drive", batch_size=2, max_wait_ms=10000)
    first, second = [asyncio.create_task(coord.get_versions(id_)) for id_ in ("a", "b")]
    await asyncio.wait_for(entered.wait(), .3)
    first.cancel()
    await asyncio.gather(first, return_exceptions=True)
    release.set()

    assert await completed_results([second]) == [[{"id": "b"}]]
    assert all(task.done() for task in created)


async def test_cancelling_all_waiters_joins_the_inflight_flush(coordinator):
    Coordinator, created, _ = coordinator
    entered, closed = asyncio.Event(), asyncio.Event()

    async def batch(drive, ids):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            closed.set()

    coord = Coordinator(SimpleNamespace(list_file_versions_batch=batch), "drive", batch_size=2, max_wait_ms=10000)
    waiters = [asyncio.create_task(coord.get_versions(id_)) for id_ in ("a", "b")]
    await asyncio.wait_for(entered.wait(), .3)
    for waiter in waiters:
        waiter.cancel()
    await completed_results(waiters)

    assert closed.is_set(), "provider flush outlived all of its cancelled callers"
    assert all(task.done() for task in created)


async def test_cancelling_queued_waiter_removes_timer_and_pending_work(coordinator):
    Coordinator, created, _ = coordinator
    sent = []

    async def batch(drive, ids):
        sent.append(ids)
        return {id_: [] for id_ in ids}

    coord = Coordinator(SimpleNamespace(list_file_versions_batch=batch), "drive", max_wait_ms=10000)
    waiter = asyncio.create_task(coord.get_versions("a"))
    await asyncio.sleep(0)
    waiter.cancel()
    await completed_results([waiter])

    assert sent == [] and coord._pending == []
    assert all(task.done() for task in created)


@pytest.mark.parametrize("task_name", ["_flush", "_wait_and_flush"])
async def test_detached_task_cancelled_before_start_cancels_waiters(coordinator, task_name):
    Coordinator, created, cancel_before_start = coordinator
    cancel_before_start.add(task_name)
    coord = Coordinator(SimpleNamespace(), "drive", batch_size=1 if task_name == "_flush" else 20,
                        max_wait_ms=10000)
    waiter = asyncio.create_task(coord.get_versions("a"))

    assert isinstance((await completed_results([waiter]))[0], asyncio.CancelledError)
    assert all(task.done() for task in created)


async def test_ordinary_batch_failure_keeps_bounded_per_item_fallback(coordinator):
    Coordinator, created, _ = coordinator
    singles = []

    async def batch(drive, ids):
        raise httpx.ReadError("batch failed")

    async def single(drive, id_):
        singles.append(id_)
        if id_ == "a":
            raise httpx.ReadError("item failed")
        return [{"id": "b"}]

    coord = Coordinator(SimpleNamespace(list_file_versions_batch=batch, list_file_versions=single),
                        "drive", batch_size=2, max_wait_ms=10000)
    waiters = [asyncio.create_task(coord.get_versions(id_)) for id_ in ("a", "b")]
    first, second = await completed_results(waiters)

    assert isinstance(first, httpx.ReadError) and second == [{"id": "b"}]
    assert singles == ["a", "b"]
    assert all(task.done() for task in created)


@pytest.mark.parametrize("order", [("a", "b", "c"), ("b", "a", "c")])
async def test_fallback_deferral_preserves_already_confirmed_result(coordinator, order):
    Coordinator, created, _ = coordinator
    error = control_error("turn_defer")
    singles = []

    async def batch(drive, ids):
        return {"a": [{"id": "version-a"}], "b": None, "c": None}

    async def single(drive, id_):
        singles.append(id_)
        raise error

    coord = Coordinator(SimpleNamespace(list_file_versions_batch=batch, list_file_versions=single),
                        "drive", batch_size=3, max_wait_ms=10000)
    waiters = [asyncio.create_task(coord.get_versions(id_)) for id_ in order]
    result = dict(zip(order, await completed_results(waiters)))

    assert result["a"] == [{"id": "version-a"}] and result["b"] is error and result["c"] is error
    assert singles == ["b"]
    assert all(task.done() for task in created)


async def test_unexpected_flush_error_remains_explicit_to_every_waiter(coordinator):
    Coordinator, created, _ = coordinator

    async def batch(drive, ids):
        return None  # Invalid client result must neither complete empty nor hang.

    coord = Coordinator(SimpleNamespace(list_file_versions_batch=batch), "drive", batch_size=2,
                        max_wait_ms=10000)
    waiters = [asyncio.create_task(coord.get_versions(id_)) for id_ in ("a", "b")]

    assert all(isinstance(error, AttributeError) for error in await completed_results(waiters))
    assert all(task.done() for task in created)
