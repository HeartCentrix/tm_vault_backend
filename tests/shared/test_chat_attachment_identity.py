"""Metadata-only attachments (cards, references) get an identity derived from their content, not Graph's per-read id."""
import os

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

from shared.chat_capture import stable_attachment_identity, stable_attachment_references

CARD = {"contentType": "application/vnd.microsoft.card.adaptive", "content": '{"type":"AdaptiveCard","body":[]}'}


def test_same_card_content_with_different_graph_ids_shares_identity():
    assert stable_attachment_identity({**CARD, "id": "a"}) == stable_attachment_identity({**CARD, "id": "b"})


def test_different_card_content_differs():
    other = {**CARD, "id": "a", "content": '{"type":"AdaptiveCard","body":[{"type":"TextBlock","text":"x"}]}'}
    assert stable_attachment_identity({**CARD, "id": "a"}) != stable_attachment_identity(other)


def test_body_reference_follows_stable_card_identity():
    message = {"id": "m", "body": {"contentType": "html", "content": '<attachment id="a"></attachment> see "a" quoted'},
               "attachments": [{**CARD, "id": "a"}]}
    stable = stable_attachment_references(message)
    new_id = stable["attachments"][0]["id"]
    assert new_id.startswith("card:") and stable["body"]["content"] == f'<attachment id="{new_id}"></attachment> see "a" quoted'
    assert message["body"]["content"].startswith('<attachment id="a">')  # input untouched


def test_file_attachments_keep_their_graph_id():
    file = {"id": "f1", "contentType": "reference", "contentUrl": "https://tenant.sharepoint.com/file.pdf"}
    assert stable_attachment_identity(file) == "f1"


def _shared_id_message(order=(0, 1)):
    # Sanitized shape of a real Bhavya message: a messageReference and a
    # forwardedMessageReference share one original attachment id.
    refs = [{"id": "shared-id", "contentType": "messageReference",
             "content": '{"messageId":"referenced","text":"quoted"}'},
            {"id": "shared-id", "contentType": "forwardedMessageReference",
             "content": '{"messageId":"referenced","text":"forwarded"}'}]
    return {"id": "message", "attachments": [refs[i] for i in order],
            "body": {"content": '<attachment id="shared-id"></attachment><attachment id="shared-id"></attachment>'}}


def test_references_sharing_an_original_id_keep_distinct_identities():
    normalized = stable_attachment_references(_shared_id_message())
    ids = [a["id"] for a in normalized["attachments"]]
    assert len(set(ids)) == 2 and all(i.startswith("card:") for i in ids)
    # The body cannot tell the two apart either; it maps deterministically.
    assert normalized["body"]["content"].count(f'id="{min(ids)}"') == 2
    assert stable_attachment_references(_shared_id_message(order=(1, 0)))["body"] == normalized["body"]
    assert {a["id"] for a in stable_attachment_references(_shared_id_message(order=(1, 0)))["attachments"]} == set(ids)


def test_shared_original_id_does_not_conflict_in_the_ledger():
    from types import SimpleNamespace
    from shared.chat_capture import ChatObservationLedger
    from shared.chat_catalogue_identity import CapturedChatItem
    normalized = stable_attachment_references(_shared_id_message())
    with ChatObservationLedger(SimpleNamespace(chat_id="chat"), observation_hash_version=2) as ledger:
        for attachment in normalized["attachments"]:
            assert ledger.record(0, CapturedChatItem("CHAT_ATTACHMENT", attachment["id"], "message", attachment, None))
