"""Hosted-content listing is a per-message Graph request; only reference-bearing messages pay it."""
from shared.chat_capture import message_references_hosted_content

URL = "https://graph.microsoft.com/v1.0/chats/19:abc@thread.v2/messages/1700000000000/hostedContents/aWQ9/$value"


def test_inline_image_body_reference_is_enumerated():
    assert message_references_hosted_content(
        {"id": "m1", "body": {"contentType": "html", "content": f'<p>hi</p><img src="{URL}" width="250">'}})


def test_plain_message_mentioning_hosted_contents_is_skipped():
    assert not message_references_hosted_content(
        {"id": "m2", "body": {"contentType": "text", "content": "read the hostedContents docs"}, "attachments": []})


def test_reference_inside_attachment_payload_is_enumerated():
    assert message_references_hosted_content(
        {"id": "m3", "body": {"contentType": "text", "content": "snippet"},
         "attachments": [{"id": "a", "contentType": "application/vnd.microsoft.card.codesnippet", "contentUrl": URL}]})
