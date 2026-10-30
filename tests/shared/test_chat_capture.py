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
