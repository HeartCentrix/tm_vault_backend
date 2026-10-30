"""Pure catalogue identity and evidence checks; no Graph, storage or database."""
from __future__ import annotations

import base64
from dataclasses import FrozenInstanceError, fields, replace
from datetime import datetime, timedelta, timezone
import hashlib
import importlib
from uuid import UUID

import pytest


NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
IDS = [UUID(int=n) for n in range(1, 12)]


def api():
    return importlib.import_module("shared.chat_catalogue_identity")


@pytest.mark.parametrize("content_type", ["image/png", "application/octet-stream", "unknown"])
def test_non_card_non_pointer_attachment_without_bytes_is_not_captured(content_type):
    with pytest.raises(ValueError, match="binary content"):
        api().CapturedChatItem(
            item_type="CHAT_ATTACHMENT", graph_id="attachment-alpha",
            parent_graph_id="message-alpha", payload={"contentType": content_type}, content=None,
        )


def scope(**changes):
    values = dict(snapshot_id=IDS[0], resource_id=IDS[1], tenant_id=IDS[2],
                  chat_id="chat-alpha", principal_id="principal-alpha",
                  permission_scope_hash="a" * 64, backend_id=IDS[3])
    return api().ChatCaptureScope(**(values | changes))


def content(**changes):
    values = dict(object_id=IDS[4], backend_id=IDS[3], container="catalogue",
                  path="chat-catalogue/v1/generation-a", sha256="b" * 64,
                  size=3, encryption_key_id=None, backup_version=1,
                  inline_b64=None, source_drive_id="drive-alpha",
                  source_item_id="file-alpha", source_etag="etag-a",
                  source_ctag="ctag-a")
    return api().ChatContentDescriptor(**(values | changes))


def item(**changes):
    values = dict(item_type="TEAMS_CHAT_MESSAGE", graph_id="message-alpha",
                  parent_graph_id=None,
                  payload={"id": "message-alpha", "body": {"content": "old body"}},
                  content=None)
    return api().CapturedChatItem(**(values | changes))


def evidence(**changes):
    values = dict(enumeration_complete=True, reconciliation_complete=True,
                  membership_sha256="a" * 64, reconciliation_sha256="a" * 64,
                  conflicting_observations=0, required_content_count=1,
                  durable_content_count=1, unresolved_required_count=0,
                  started_at=NOW, finished_at=NOW + timedelta(seconds=1))
    return api().CaptureEvidence(**(values | changes))


def result(**changes):
    values = dict(outcome="complete", inventory_id=IDS[5], reused=False,
                  item_count=1, logical_bytes=3, uploaded_bytes=3,
                  started_at=NOW, finished_at=NOW + timedelta(seconds=1),
                  failure_reason=None)
    return api().CaptureResult(**(values | changes))


def test_qualified_identity_distinguishes_chat_type_and_parent_without_delimiter_collisions():
    key = api().qualified_chat_item_key
    assert len(key("chat-alpha", "TEAMS_CHAT_MESSAGE", "message-alpha")) == 64
    values = {
        key("chat-alpha", "TEAMS_CHAT_MESSAGE", "message-alpha"),
        key("chat-beta", "TEAMS_CHAT_MESSAGE", "message-alpha"),
        key("chat-alpha", "TEAMS_MESSAGE", "message-alpha"),
        key("chat-alpha", "CHAT_ATTACHMENT", "child-alpha", "message-alpha"),
        key("chat-alpha", "CHAT_ATTACHMENT", "child-alpha", "message-beta"),
        key("chat-alpha", "CHAT_HOSTED_CONTENT", "child-alpha", "message-alpha"),
    }
    assert len(values) == 6
    assert key("a:b", "TEAMS_MESSAGE", "c") != key("a", "TEAMS_MESSAGE", "b:c")


@pytest.mark.parametrize("item_type", ["CHAT_ATTACHMENT", "CHAT_HOSTED_CONTENT"])
@pytest.mark.parametrize("parent", [None, "", " "])
def test_child_identity_requires_raw_parent(item_type, parent):
    with pytest.raises(ValueError, match="parent"):
        api().qualified_chat_item_key("chat-alpha", item_type, "child-alpha", parent)


@pytest.mark.parametrize("argument", ["chat_id", "item_type", "graph_id"])
def test_identity_rejects_missing_source_identity(argument):
    values = dict(chat_id="chat-alpha", item_type="TEAMS_MESSAGE_REPLY", graph_id="message-alpha")
    with pytest.raises(ValueError):
        api().qualified_chat_item_key(**(values | {argument: ""}))


def test_canonical_json_preserves_metadata_and_ignores_object_key_order():
    first = {"body": {"contentType": "html", "content": "hello"}, "id": "message-alpha"}
    second = {"id": "message-alpha", "body": {"content": "hello", "contentType": "html"}}
    assert api().canonical_payload_sha256(first) == api().canonical_payload_sha256(second)
    assert api().canonical_payload_sha256(first) != api().canonical_payload_sha256(
        first | {"_source_extension": {"value": "retained"}})
    assert api().canonical_payload_sha256({"status": "old"}) != api().canonical_payload_sha256({"status": "new"})


def test_known_worker_controls_and_download_credentials_do_not_change_semantic_payload():
    first = {"id": "file-alpha", "_chat_folder_path": "old label", "_skip_content_fetch": False,
             "@microsoft.graph.downloadUrl": "https://download.invalid/a?token=old",
             "contentUrl": "https://files.invalid/a?item=alpha&sig=old&se=old",
             "nested": {"downloadUrl": "https://download.invalid/b?token=old", "etag": "version-a"}}
    second = {"id": "file-alpha", "_chat_folder_path": "new label", "_skip_content_fetch": True,
              "@microsoft.graph.downloadUrl": "https://download.invalid/a?token=new",
              "contentUrl": "https://files.invalid/a?se=new&sig=new&item=alpha",
              "nested": {"downloadUrl": "https://download.invalid/b?token=new", "etag": "version-a"}}
    assert api().canonical_payload_sha256(first) == api().canonical_payload_sha256(second)
    assert api().canonical_payload_sha256(first) != api().canonical_payload_sha256(
        first | {"contentUrl": "https://files.invalid/a?item=beta&sig=old&se=old"})


def test_nested_card_fields_named_like_worker_controls_remain_source_metadata():
    first = {"content": {"_chat_folder_path": "source-card-value"}}
    second = {"content": {"_chat_folder_path": "different-card-value"}}
    assert api().canonical_payload_sha256(first) != api().canonical_payload_sha256(second)


def test_unsigned_source_query_parameters_are_not_mistaken_for_credentials():
    first = {"contentUrl": "https://files.invalid/source?sv=source-version-a&se=source-field"}
    second = {"contentUrl": "https://files.invalid/source?sv=source-version-b&se=source-field"}
    assert api().canonical_payload_sha256(first) != api().canonical_payload_sha256(second)


def test_signed_url_protocol_metadata_rotates_without_hiding_source_identity():
    first = {"contentUrl": "https://files.invalid/source?item=alpha&X-Amz-Signature=old&X-Amz-Date=old&X-Amz-Expires=60"}
    second = {"contentUrl": "https://files.invalid/source?X-Amz-Expires=120&X-Amz-Signature=new&X-Amz-Date=new&item=alpha"}
    assert api().canonical_payload_sha256(first) == api().canonical_payload_sha256(second)


@pytest.mark.parametrize("payload", [{"x": float("nan")}, {"x": object()}, {1: "not-a-json-key"}])
def test_non_json_payloads_are_rejected(payload):
    with pytest.raises((TypeError, ValueError)):
        api().canonical_payload_sha256(payload)


def test_version_hash_detects_old_body_and_card_changes_with_identical_ids():
    fingerprint = api().canonical_version_fingerprint
    original = item()
    changed = item(payload={"id": "message-alpha", "body": {"content": "edited old body"}})
    assert fingerprint(tenant_id=IDS[2], chat_id="chat-alpha", item=original) != fingerprint(
        tenant_id=IDS[2], chat_id="chat-alpha", item=changed)
    card = item(item_type="CHAT_ATTACHMENT", graph_id="card-alpha", parent_graph_id="message-alpha",
                payload={"contentType": "application/vnd.microsoft.card.adaptive", "content": {"text": "old"}})
    assert fingerprint(tenant_id=IDS[2], chat_id="chat-alpha", item=card) != fingerprint(
        tenant_id=IDS[2], chat_id="chat-alpha", item=replace(card, payload=card.payload | {"content": {"text": "new"}}))


def test_version_identity_includes_tenant_chat_and_parent():
    fingerprint = api().canonical_version_fingerprint
    child = item(item_type="CHAT_ATTACHMENT", graph_id="child-alpha", parent_graph_id="message-alpha", payload={"contentType": "messageReference"})
    hashes = {
        fingerprint(tenant_id=IDS[2], chat_id="chat-alpha", item=child),
        fingerprint(tenant_id=IDS[6], chat_id="chat-alpha", item=child),
        fingerprint(tenant_id=IDS[2], chat_id="chat-beta", item=child),
        fingerprint(tenant_id=IDS[2], chat_id="chat-alpha", item=replace(child, parent_graph_id="message-beta")),
    }
    assert len(hashes) == 4


def test_inventory_detects_file_checksum_source_version_drive_and_membership_changes():
    fingerprint = api().source_inventory_fingerprint
    binary = item(item_type="CHAT_ATTACHMENT", graph_id="child-alpha", parent_graph_id="message-alpha",
                  payload={"contentType": "reference"}, content=content())
    initial = fingerprint(scope=scope(), items=[item(), binary])
    for changed in (replace(binary, content=content(sha256="c" * 64)),
                    replace(binary, content=content(source_etag="etag-b")),
                    replace(binary, content=content(source_drive_id="drive-beta")),
                    replace(binary, content=content(source_item_id="file-beta"))):
        assert initial != fingerprint(scope=scope(), items=[item(), changed])
    assert initial != fingerprint(scope=scope(), items=[binary, item(payload={"body": {"content": "edited old body"}})])
    assert initial != fingerprint(scope=scope(), items=[binary])


def test_source_inventory_is_order_independent_and_ignores_physical_generations():
    fingerprint = api().source_inventory_fingerprint
    original = item(item_type="CHAT_ATTACHMENT", graph_id="child-alpha", parent_graph_id="message-alpha", payload={}, content=content())
    relocated = replace(original, content=content(object_id=IDS[7], backend_id=IDS[8],
                                                container="other-container", path="another-generation",
                                                encryption_key_id="key-version-b", backup_version=2))
    assert fingerprint(scope=scope(), items=[item(), original]) == fingerprint(
        scope=scope(snapshot_id=IDS[8], resource_id=IDS[9], backend_id=IDS[10]), items=[relocated, item()])
    assert api().canonical_version_fingerprint(tenant_id=IDS[2], chat_id="chat-alpha", item=original) != api().canonical_version_fingerprint(
        tenant_id=IDS[2], chat_id="chat-alpha", item=relocated)


def test_source_inventory_ignores_inline_vs_object_representation_of_identical_bytes():
    raw = b"abc"
    checksum = hashlib.sha256(raw).hexdigest()
    stored = item(item_type="CHAT_HOSTED_CONTENT", graph_id="child-alpha", parent_graph_id="message-alpha", payload={}, content=content(sha256=checksum))
    inline = replace(stored, content=content(object_id=None, backend_id=None, container=None,
                                            path=None, sha256=checksum, inline_b64=base64.b64encode(raw).decode()))
    assert api().source_inventory_fingerprint(scope=scope(), items=[stored]) == api().source_inventory_fingerprint(scope=scope(), items=[inline])


def test_repeated_identical_observations_deduplicate_and_conflicts_are_rejected():
    fingerprint = api().source_inventory_fingerprint
    assert fingerprint(scope=scope(), items=[item(), item()]) == fingerprint(scope=scope(), items=[item()])
    with pytest.raises(ValueError, match="conflict"):
        fingerprint(scope=scope(), items=[item(), item(payload={"body": {"content": "changed"}})])


def test_empty_inventory_is_stable_but_scoped_to_recorded_authentication_context():
    fingerprint = api().source_inventory_fingerprint
    original = fingerprint(scope=scope(), items=[])
    assert len(original) == 64
    assert original == fingerprint(scope=scope(snapshot_id=IDS[9]), items=iter([]))
    for changed in (scope(tenant_id=IDS[6]), scope(chat_id="chat-beta"),
                    scope(principal_id="principal-beta"), scope(permission_scope_hash="b" * 64)):
        assert original != fingerprint(scope=changed, items=[])


def test_authentication_context_hash_is_not_source_visibility_or_completeness_evidence():
    fingerprint = api().authentication_context_fingerprint
    assert fingerprint(scope()) == fingerprint(scope(snapshot_id=IDS[8], backend_id=IDS[9]))
    assert fingerprint(scope()) != fingerprint(scope(permission_scope_hash="b" * 64))
    # The same credentials can observe different source data or fail enumeration.
    assert api().source_inventory_fingerprint(scope=scope(), items=[]) != api().source_inventory_fingerprint(scope=scope(), items=[item()])
    assert not evidence(enumeration_complete=False).satisfies_completeness_conditions


@pytest.mark.parametrize("changes", [
    {"object_id": None}, {"inline_b64": "YWJj"}, {"backend_id": None},
    {"container": None}, {"path": "https://download.invalid/a?sig=example"},
    {"sha256": "not-a-sha256"}, {"size": -1}, {"size": True},
    {"source_drive_id": None},
])
def test_binary_descriptors_reject_ambiguous_or_incomplete_representation(changes):
    with pytest.raises((ValueError, TypeError)):
        content(**changes)


@pytest.mark.parametrize("changes", [
    {"inline_b64": "not base64"}, {"inline_b64": "YWJj", "sha256": "b" * 64},
    {"inline_b64": "YWJj", "sha256": hashlib.sha256(b"abc").hexdigest(), "size": 4},
])
def test_inline_descriptor_verifies_actual_bytes(changes):
    with pytest.raises(ValueError):
        content(object_id=None, backend_id=None, container=None, path=None, **changes)


def test_metadata_only_cards_and_pointer_items_do_not_require_binary_content():
    for content_type in ("application/vnd.microsoft.card.adaptive", "messageReference", "forwardedMessageReference", "meetingReference"):
        assert item(item_type="CHAT_ATTACHMENT", graph_id="card-alpha", parent_graph_id="message-alpha",
                    payload={"contentType": content_type, "content": {"text": "captured"}}).content is None


@pytest.mark.parametrize("kind,payload", [
    ("CHAT_HOSTED_CONTENT", {}), ("CHAT_ATTACHMENT", {"contentType": "reference"}),
    ("CHAT_ATTACHMENT", {"contentType": "fileAttachment"}),
    ("CHAT_ATTACHMENT", {"contentType": "image/png", "contentUrl": "https://files.invalid/image"}),
])
def test_required_binary_content_cannot_be_staged_as_metadata_only(kind, payload):
    with pytest.raises(ValueError, match="content"):
        item(item_type=kind, graph_id="child-alpha", parent_graph_id="message-alpha", payload=payload)


def test_frozen_records_copy_source_payload_before_hashing():
    source = {"body": {"content": "original"}}
    captured = item(payload=source)
    source["body"]["content"] = "changed outside capture"
    assert captured.payload["body"]["content"] == "original"
    with pytest.raises(FrozenInstanceError):
        captured.graph_id = "changed"


def test_captured_payload_cannot_mutate_through_nested_mappings_or_arrays():
    source = {"content": {"cards": [{"text": "original"}]}}
    captured = item(payload=source)
    before = api().canonical_payload_sha256(source)
    with pytest.raises(TypeError):
        captured.payload["content"]["cards"][0]["text"] = "changed"
    with pytest.raises(AttributeError):
        captured.payload["content"]["cards"].append({"text": "added"})
    assert before == api().canonical_payload_sha256(captured.payload)


@pytest.mark.parametrize("changes", [{"snapshot_id": "not-a-uuid"}, {"chat_id": ""}, {"permission_scope_hash": ""}])
def test_capture_scope_requires_uuid_and_nonempty_recorded_scope(changes):
    with pytest.raises((TypeError, ValueError)):
        scope(**changes)


def test_lease_requires_uuid_fence_and_aware_utc_expiry():
    values = dict(attempt_id=IDS[0], generation=1, expires_at=NOW, staged_inventory_id=IDS[1])
    assert api().CaptureLease(**values).generation == 1
    for change in ({"attempt_id": "not-a-uuid"}, {"generation": -1}, {"generation": True},
                   {"expires_at": NOW.replace(tzinfo=None)},
                   {"expires_at": NOW.astimezone(timezone(timedelta(hours=1)))}):
        with pytest.raises((ValueError, TypeError)):
            api().CaptureLease(**(values | change))


@pytest.mark.parametrize("changes", [
    {"enumeration_complete": False}, {"reconciliation_complete": False},
    {"membership_sha256": "b" * 64}, {"conflicting_observations": 1},
    {"durable_content_count": 0}, {"unresolved_required_count": 1},
])
def test_complete_flags_cannot_hide_gaps_or_movement(changes):
    assert not evidence(**changes).satisfies_completeness_conditions


def test_successful_empty_observation_needs_no_required_content():
    assert evidence(required_content_count=0, durable_content_count=0).satisfies_completeness_conditions
    assert result(item_count=0, logical_bytes=0, uploaded_bytes=0).outcome == "complete"


@pytest.mark.parametrize("changes", [
    {"required_content_count": -1}, {"durable_content_count": True},
    {"enumeration_complete": 1}, {"membership_sha256": ""},
    {"started_at": NOW.replace(tzinfo=None)}, {"finished_at": NOW - timedelta(seconds=1)},
])
def test_invalid_evidence_is_rejected_without_inventing_completion(changes):
    with pytest.raises((ValueError, TypeError)):
        evidence(**changes)


@pytest.mark.parametrize("changes", [
    {"outcome": "COMPLETED"}, {"inventory_id": None}, {"failure_reason": "gap"},
    {"outcome": "partial", "reused": True, "failure_reason": "gap"},
    {"outcome": "partial", "failure_reason": None},
    {"outcome": "failed", "inventory_id": None, "failure_reason": None},
    {"item_count": -1}, {"uploaded_bytes": True}, {"finished_at": NOW - timedelta(seconds=1)},
])
def test_results_reject_false_success_and_partial_reuse(changes):
    with pytest.raises((ValueError, TypeError)):
        result(**changes)


def test_partial_and_failed_results_keep_explicit_outcomes_and_reasons():
    assert result(outcome="partial", failure_reason="required content unresolved").inventory_id == IDS[5]
    assert result(outcome="failed", inventory_id=None, item_count=0, logical_bytes=0,
                  uploaded_bytes=0, failure_reason="enumeration failed").inventory_id is None


def test_stage_counts_cannot_report_more_new_versions_than_successfully_staged_items():
    assert api().StageResult(staged_items=3, new_versions=1).staged_items == 3
    for values in ((-1, 0), (1, 2), (True, 0)):
        with pytest.raises((ValueError, TypeError)):
            api().StageResult(staged_items=values[0], new_versions=values[1])


def test_specified_dataclass_field_contract_is_exact():
    contracts = {
        "ChatCaptureScope": "snapshot_id resource_id tenant_id chat_id principal_id permission_scope_hash backend_id partition_id",
        "CaptureLease": "attempt_id generation expires_at staged_inventory_id",
        "CapturedChatItem": "item_type graph_id parent_graph_id payload content",
        "ChatContentDescriptor": "object_id backend_id container path sha256 size encryption_key_id backup_version inline_b64 source_drive_id source_item_id source_etag source_ctag",
        "CaptureEvidence": "enumeration_complete reconciliation_complete membership_sha256 reconciliation_sha256 conflicting_observations required_content_count durable_content_count unresolved_required_count started_at finished_at",
        "CaptureResult": "outcome inventory_id reused item_count logical_bytes uploaded_bytes started_at finished_at failure_reason",
        "StageResult": "staged_items new_versions",
    }
    for name, expected in contracts.items():
        assert [field.name for field in fields(getattr(api(), name))] == expected.split()
