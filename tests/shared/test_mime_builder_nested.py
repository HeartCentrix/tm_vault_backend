"""Tests for build_nested_message_attachment — the shared core that lets
mail-in-mail (itemAttachment / a nested email) embed natively in EML/MBOX/PST
for EVERY mailbox type (primary, shared, archive), since they share this layer."""
from __future__ import annotations

import base64
import email

from shared.mime_builder import build_nested_message_attachment, AttachmentRef


def test_nested_message_becomes_rfc822_eml_attachment():
    nested = {
        "subject": "Fwd: Q3 numbers",
        "from": {"emailAddress": {"name": "Alice", "address": "alice@x.com"}},
        "toRecipients": [{"emailAddress": {"address": "bob@y.com"}}],
        "body": {"contentType": "text", "content": "see attached figures"},
    }
    ref = build_nested_message_attachment(nested)
    assert isinstance(ref, AttachmentRef)
    assert ref.content_type == "message/rfc822"
    assert ref.name == "Fwd_ Q3 numbers.eml"
    m = email.message_from_bytes(ref.data_bytes)
    assert m["subject"] == "Fwd: Q3 numbers"
    assert "alice@x.com" in m["from"]
    assert "see attached figures" in ref.data_bytes.decode("utf-8", "replace")


def test_nested_message_embeds_its_own_fileattachment():
    # a nested email that itself carries a PDF -> the PDF rides inside the .eml
    nested = {
        "subject": "has pdf",
        "from": {"emailAddress": {"address": "a@x.com"}},
        "body": {"contentType": "text", "content": "x"},
        "attachments": [{
            "@odata.type": "#microsoft.graph.fileAttachment",
            "name": "doc.pdf", "contentType": "application/pdf",
            "contentBytes": base64.b64encode(b"%PDF-1.4 hello").decode(),
        }],
    }
    ref = build_nested_message_attachment(nested)
    assert b"doc.pdf" in ref.data_bytes            # attachment declared in nested eml
    assert base64.b64encode(b"%PDF-1.4 hello") in ref.data_bytes.replace(b"\n", b"")


def test_empty_or_missing_subject_falls_back_to_stable_name():
    ref = build_nested_message_attachment({"body": {"content": ""}})
    assert ref.name == "attached-message.eml"
    assert build_nested_message_attachment({}).name == "attached-message.eml"
