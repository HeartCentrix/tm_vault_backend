"""Source-observation accounting stays bounded and matches repository identity."""
import asyncio
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timezone
import gc
import importlib
import os
from types import SimpleNamespace
from uuid import uuid4

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")

import pytest

from shared.chat_catalogue_identity import ChatCaptureScope, CapturedChatItem, source_inventory_fingerprint


def scope():
    return ChatCaptureScope(uuid4(), uuid4(), uuid4(), "chat", "user", "a" * 64, uuid4())


def item(mid="message", body="body"):
    return CapturedChatItem("TEAMS_CHAT_MESSAGE", mid, None, {"id": mid, "body": {"content": body}}, None)


def test_disk_ledger_matches_canonical_source_fingerprint():
    module = importlib.import_module("shared.chat_capture")
    selected = scope()
    values = [item("b"), item("a")]
    with module.ChatObservationLedger(selected) as ledger:
        for value in values:
            assert ledger.record(0, value)
        assert not ledger.record(0, values[0])
        assert ledger.fingerprint(0) == source_inventory_fingerprint(scope=selected, items=values)
        assert ledger.item_count(0) == 2
        path = ledger.path
        assert os.path.exists(path)
    assert not os.path.exists(path)


def test_disk_ledger_rejects_conflicting_observation():
    module = importlib.import_module("shared.chat_capture")
    with module.ChatObservationLedger(scope()) as ledger:
        ledger.record(0, item())
        with pytest.raises(module.ChatSourceMovement):
            ledger.record(0, item(body="edited"))


def test_observation_passes_detect_old_edit_and_missing_membership():
    module = importlib.import_module("shared.chat_capture")
    with module.ChatObservationLedger(scope()) as ledger:
        ledger.record(0, item("old"))
        ledger.record(0, item("new"))
        ledger.record(1, item("old", body="edited"))
        assert ledger.fingerprint(0) != ledger.fingerprint(1)


def test_required_manifest_counts_missing_child_separately():
    module = importlib.import_module("shared.chat_capture")
    with module.ChatObservationLedger(scope()) as ledger:
        ledger.expect(0, "CHAT_ATTACHMENT", "file", "message", required=True)
        ledger.expect(0, "CHAT_ATTACHMENT", "card", "message", required=False)
        assert ledger.required_count(0) == 1
        assert ledger.durable_count(0) == 0


def test_capture_heartbeat_preserves_stolen_work_lease_failure():
    module = importlib.import_module("shared.chat_capture")
    keeper = module._CaptureLeaseKeeper(None, None)
    failure = module.catalogue.ChatCatalogueWorkLeaseMoved("snapshot lease changed")
    keeper.failure = failure
    with pytest.raises(module.catalogue.ChatCatalogueWorkLeaseMoved) as error:
        keeper.check()
    assert error.value is failure


def test_capture_heartbeat_other_failure_remains_generic_conflict():
    module = importlib.import_module("shared.chat_capture")
    keeper = module._CaptureLeaseKeeper(None, None)
    failure = RuntimeError("database connection lost")
    keeper.failure = failure
    with pytest.raises(module.catalogue.ChatCatalogueConflict) as error:
        keeper.check()
    assert type(error.value) is module.catalogue.ChatCatalogueConflict
    assert error.value.__cause__ is failure


def _ordered_children(reverse=False):
    hosted = [{"id": "a"}, {"id": "b"}]
    manifest = [{"item_type": "CHAT_HOSTED_CONTENT", "graph_id": c["id"], "requires_content": True} for c in hosted]
    if reverse:
        hosted.reverse()
        manifest.reverse()
    return CapturedChatItem("TEAMS_CHAT_MESSAGE", "m", None, {
        "id": "m", "body": {"content": "same body"}, "hostedContents": hosted,
        "chat_catalogue_expected_children": {"complete": True, "items": manifest}}, None)


def test_v2_observation_ignores_set_order_without_changing_stored_identity():
    from shared.chat_catalogue_identity import canonical_payload_sha256
    module = importlib.import_module("shared.chat_capture")
    selected = scope()
    first, second = _ordered_children(), _ordered_children(True)
    assert canonical_payload_sha256(first.payload) != canonical_payload_sha256(second.payload)
    with module.ChatObservationLedger(selected, observation_hash_version=2) as ledger:
        ledger.record(0, first)
        ledger.record(1, second)
        assert ledger.fingerprint(0) == ledger.fingerprint(1)
        assert ledger.fingerprint(0) == source_inventory_fingerprint(
            scope=selected, items=[first], observation_hash_version=2)
        assert ledger.fingerprint(0) != source_inventory_fingerprint(scope=selected, items=[first])


def test_v2_rejects_conflicting_children_in_same_set():
    from dataclasses import replace
    module = importlib.import_module("shared.chat_capture")
    first = _ordered_children()
    bad = replace(first, payload=first.payload | {"hostedContents": [{"id": "a", "contentType": "x"}, {"id": "a", "contentType": "y"}]})
    with module.ChatObservationLedger(scope(), observation_hash_version=2) as ledger:
        with pytest.raises((ValueError, module.ChatSourceMovement), match="conflict"):
            ledger.record(0, bad)


def test_v2_keeps_body_and_non_set_order_changes():
    from dataclasses import replace
    module = importlib.import_module("shared.chat_capture")
    first = _ordered_children()
    edited = replace(first, payload=first.payload | {"body": {"content": "edited"}})
    with module.ChatObservationLedger(scope(), observation_hash_version=2) as ledger:
        ledger.record(0, first)
        ledger.record(1, edited)
        assert ledger.fingerprint(0) != ledger.fingerprint(1)


def test_observation_unknown_version_fails_closed():
    module = importlib.import_module("shared.chat_capture")
    with pytest.raises(ValueError, match="version"):
        module.ChatObservationLedger(scope(), observation_hash_version=3)


def test_failure_diagnostics_retain_safe_provider_fields():
    from types import SimpleNamespace
    module = importlib.import_module("shared.chat_capture")
    with module.ChatObservationLedger(scope()) as ledger:
        ledger.failure(0, "messages", SimpleNamespace(status_code=403, code="Forbidden",
            inner_code="InsufficientPrivileges", request_id="request-id-123"))
        detail = ledger.failure_details()[0]
        assert detail["provider_code"] == "Forbidden"
        assert detail["provider_inner_code"] == "InsufficientPrivileges"
        assert detail["provider_request_id"] == "request-id-123"


def test_mismatch_diagnostics_distinguish_missing_and_changed_keys_without_payload():
    module = importlib.import_module("shared.chat_capture")
    with module.ChatObservationLedger(scope()) as ledger:
        ledger.record(0, item("removed", body="secret"))
        ledger.record(0, item("changed", body="secret"))
        ledger.record(1, item("changed", body="other secret"))
        ledger.record(1, item("added"))
        detail = ledger.difference_details()
        assert detail["only_first"] == detail["only_second"] == detail["changed"] == 1
        assert len(detail["sample_keys"]) == 3
        assert "secret" not in str(detail)


@pytest.fixture(params=[False, True], ids=["abort", "suspend"])
async def cancelled_capture(monkeypatch, request):
    """Exercise real capture/transaction ownership around a blocked DB adapter."""
    from shared import chat_capture as capture, provider_runtime
    from shared.provider_work import ProviderWorkConflict

    selected = scope()
    lease = SimpleNamespace(observation_hash_version=1)
    execution = SimpleNamespace(claim=object(), deferred=None) if request.param else None
    unit = SimpleNamespace(created_at=datetime.now(timezone.utc), reconciliations=0, key="chat") if request.param else None
    state = SimpleNamespace(entered=asyncio.Event(), started=asyncio.Event(), release=asyncio.Event(),
        cancelled=asyncio.Event(), finished=asyncio.Event(), committed=[], rolled_back=[], errors=[],
        failure=None, late_failure=None, capture=capture)

    @asynccontextmanager
    async def session_factory():
        pending = []

        @asynccontextmanager
        async def transaction():
            try:
                yield
            except BaseException:
                state.rolled_back.extend(pending)
                raise
            else:
                state.committed.extend(pending)

        yield SimpleNamespace(begin=transaction, pending=pending)

    async def begin(session, **kwargs):
        return lease

    async def cleanup(session, **kwargs):
        assert kwargs["lease"] is lease
        if request.param:
            assert kwargs["claim"] is execution.claim and kwargs["unit_key"] == unit.key
        else:
            assert kwargs["reason"] == "capture cancelled before verification"
        session.pending.append("suspended" if request.param else "aborted")
        state.started.set()
        try:
            await state.release.wait()
            if state.failure is not None:
                raise state.failure
        except asyncio.CancelledError:
            state.cancelled.set()
            if state.late_failure is not None:
                # A driver can fail while unwinding an interrupted transaction.
                await state.release.wait()
                raise state.late_failure
            raise
        finally:
            state.finished.set()

    async def pages(chat_id):
        state.entered.set()
        await asyncio.Event().wait()
        yield []

    @asynccontextmanager
    async def pinned(client, *, principal_id):
        yield SimpleNamespace(permission_scope_hash=selected.permission_scope_hash, iter_message_pages=pages)

    monkeypatch.setattr(capture, "pinned_chat_source", pinned)
    monkeypatch.setattr(capture.catalogue, "begin_capture", begin)
    monkeypatch.setattr(capture.catalogue, "suspend_capture", cleanup)
    monkeypatch.setattr(capture.catalogue, "abort_capture", cleanup)
    monkeypatch.setattr(provider_runtime, "current_execution", lambda: execution)
    monkeypatch.setattr(provider_runtime, "current_unit", lambda: unit)
    state.conflict = ProviderWorkConflict("provider work ownership changed")
    loop = asyncio.get_running_loop()
    previous_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda loop, context: state.errors.append(context))
    state.task = asyncio.create_task(capture.capture_chat_inventory(scope=selected,
        graph_client=object(), storage=object(), session_factory=session_factory))
    await asyncio.wait_for(state.entered.wait(), 1)
    try:
        yield state
    finally:
        state.task.cancel()
        state.release.set()
        with suppress(asyncio.CancelledError):
            await state.task
        await asyncio.sleep(0)
        gc.collect()
        loop.set_exception_handler(previous_handler)


@pytest.mark.parametrize("conflict", [False, True], ids=["success", "ownership_changed"])
async def test_capture_cleanup_preserves_cancellation_after_transaction_result(cancelled_capture, conflict):
    state = cancelled_capture
    state.failure = state.conflict if conflict else None
    state.task.cancel("original cancellation")
    await asyncio.wait_for(state.started.wait(), 1)
    state.release.set()
    with pytest.raises(asyncio.CancelledError) as error:
        await state.task
    assert error.value.args == ("original cancellation",)
    assert bool(state.committed) is not conflict
    assert bool(state.rolled_back) is conflict
    assert not state.errors


async def test_repeated_capture_cancel_stops_cleanup_and_retrieves_late_failure(cancelled_capture):
    state = cancelled_capture
    state.late_failure = state.conflict
    state.task.cancel("original cancellation")
    await asyncio.wait_for(state.started.wait(), 1)
    state.task.cancel("second cancellation")
    with pytest.raises(asyncio.CancelledError) as error:
        await state.task
    # The owner may exit while rollback is still running, but the write must stop.
    was_cancelled = state.cancelled.is_set()
    state.failure = state.conflict
    state.release.set()
    await asyncio.wait_for(state.finished.wait(), 1)
    await asyncio.sleep(0)
    gc.collect()
    assert not state.errors
    assert was_cancelled
    assert error.value.args == ("original cancellation",)
    assert not state.committed
    assert state.rolled_back


async def test_capture_cancel_cleanup_has_bounded_wait(cancelled_capture, monkeypatch):
    from shared.lease import run_bounded

    state = cancelled_capture

    async def short_bound(coro, timeout_s):
        return await run_bounded(coro, timeout_s=0.01)

    monkeypatch.setattr(state.capture, "run_bounded", short_bound, raising=False)
    state.task.cancel("original cancellation")
    await asyncio.wait_for(state.started.wait(), 1)
    done, _ = await asyncio.wait({state.task}, timeout=0.5)
    assert state.task in done, "capture cleanup exceeded its bounded wait"
    with pytest.raises(asyncio.CancelledError) as error:
        await state.task
    assert error.value.args == ("original cancellation",)
    assert state.cancelled.is_set()
    assert not state.committed
    assert state.rolled_back
