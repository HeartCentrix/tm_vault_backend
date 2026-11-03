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
