"""Source-observation accounting stays bounded and matches repository identity."""
import importlib
import os
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
