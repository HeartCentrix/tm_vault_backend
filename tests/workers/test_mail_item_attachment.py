"""Mail-in-mail (itemAttachment) export wiring — PST + EML/MBOX.

Pins that a nested email (a ``#microsoft.graph.itemAttachment`` whose bytes are
the nested Graph message JSON, stored by the backup as an EMAIL_ATTACHMENT
sibling) is rebuilt into a ``message/rfc822`` part so it embeds natively in the
exported PST and EML. Also pins that fileAttachment stays a plain
fileAttachment and referenceAttachment stays skipped.

Worker dirs have hyphens, so ``workers/restore-worker`` is put on sys.path and
the modules imported directly. ``shared.azure_storage`` (which pulls real
config) is only touched by the PST blob-download branch, so we inject a fake
into sys.modules and patch the download helpers — no real storage/config.
"""
from __future__ import annotations

import base64
import email
import json
import pathlib
import sys
import types

import pytest

_ROOT = pathlib.Path(__file__).resolve().parents[2]
_WORKER = _ROOT / "workers" / "restore-worker"
for _p in (str(_ROOT), str(_WORKER)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import mail_fetch  # noqa: E402


NESTED = {
    "subject": "NESTED-SUBJECT-42",
    "from": {"emailAddress": {"name": "Nia", "address": "nia@inner.com"}},
    "toRecipients": [{"emailAddress": {"address": "bob@y.com"}}],
    "body": {"contentType": "text", "content": "inner body text"},
}
NESTED_BYTES = json.dumps(NESTED).encode("utf-8")

PARENT_MSG = {
    "subject": "PARENT",
    "from": {"emailAddress": {"address": "outer@x.com"}},
    "body": {"contentType": "text", "content": "outer body"},
}


class _Att:
    """SnapshotItem-like EMAIL_ATTACHMENT row."""

    def __init__(self, kind, *, blob_path=None, inline_b64=None,
                 name="thing", content_type="application/json"):
        self.name = name
        self.blob_path = blob_path
        self.backend_id = "be"
        self.extra_data = {
            "attachment_kind": kind,
            "content_type": content_type,
            "is_inline": False,
            "inline_b64": inline_b64,
        }


class _Item:
    """SnapshotItem-like EMAIL row."""

    def __init__(self, atts):
        self.external_id = "MSG-1"
        self.name = "parent"
        self.folder_path = "/Inbox"
        self.blob_path = "msg/blob"
        self.attachment_blob_paths = []          # file-attachment channel: empty
        self._email_attachment_items = atts


class _FakeShard:
    kind = "azure_blob"


def _patch_common(monkeypatch, download_bytes):
    """Patch fetch_message + _download_first_hit; inject fake azure_storage."""
    async def _fake_fetch(item, shard, container):
        return dict(PARENT_MSG)

    async def _fake_dfh(shard, containers, blob_path):
        return download_bytes, "container-x"

    monkeypatch.setattr(mail_fetch, "fetch_message", _fake_fetch, raising=False)
    monkeypatch.setattr(mail_fetch, "_download_first_hit", _fake_dfh)

    fake_az = types.ModuleType("shared.azure_storage")
    fake_az.azure_storage_manager = types.SimpleNamespace(
        get_shard_for_item=lambda att: _FakeShard()
    )
    monkeypatch.setitem(sys.modules, "shared.azure_storage", fake_az)


# --------------------------------------------------------------------------- PST


async def test_pst_item_attachment_becomes_rfc822(monkeypatch):
    _patch_common(monkeypatch, NESTED_BYTES)
    from pst_writers.mail import MailPstWriter

    item = _Item([_Att("#microsoft.graph.itemAttachment", blob_path="nested/blob")])
    msg_json = await MailPstWriter()._collect_graph_item(item, _FakeShard(), "src-c")

    atts = msg_json["attachments"]
    rfc822 = [a for a in atts if a.get("contentType") == "message/rfc822"]
    assert len(rfc822) == 1
    raw = base64.b64decode(rfc822[0]["contentBytes"])
    parsed = email.message_from_bytes(raw)
    assert parsed["subject"] == "NESTED-SUBJECT-42"
    assert rfc822[0]["@odata.type"] == "#microsoft.graph.fileAttachment"
    assert rfc822[0]["name"].endswith(".eml")


async def test_pst_item_attachment_inline_bytes(monkeypatch):
    # blob_path is None; nested JSON rides in extra_data.inline_b64.
    _patch_common(monkeypatch, None)
    from pst_writers.mail import MailPstWriter

    att = _Att(
        "#microsoft.graph.itemAttachment",
        blob_path=None,
        inline_b64=base64.b64encode(NESTED_BYTES).decode("ascii"),
    )
    msg_json = await MailPstWriter()._collect_graph_item(item := _Item([att]), _FakeShard(), "src-c")
    assert item  # silence linters
    rfc822 = [a for a in msg_json["attachments"] if a.get("contentType") == "message/rfc822"]
    assert len(rfc822) == 1
    assert email.message_from_bytes(base64.b64decode(rfc822[0]["contentBytes"]))["subject"] == "NESTED-SUBJECT-42"


async def test_pst_file_attachment_unaffected(monkeypatch):
    _patch_common(monkeypatch, b"%PDF-1.4 real file bytes")
    from pst_writers.mail import MailPstWriter

    att = _Att("#microsoft.graph.fileAttachment", blob_path="file/blob",
               name="doc.pdf", content_type="application/pdf")
    msg_json = await MailPstWriter()._collect_graph_item(_Item([att]), _FakeShard(), "src-c")

    atts = msg_json["attachments"]
    assert len(atts) == 1
    a = atts[0]
    assert a["contentType"] == "application/pdf"          # unchanged
    assert a["contentType"] != "message/rfc822"
    assert base64.b64decode(a["contentBytes"]) == b"%PDF-1.4 real file bytes"


async def test_pst_reference_attachment_skipped(monkeypatch):
    _patch_common(monkeypatch, b"ignored")
    from pst_writers.mail import MailPstWriter

    att = _Att("#microsoft.graph.referenceAttachment", blob_path="ref/blob")
    msg_json = await MailPstWriter()._collect_graph_item(_Item([att]), _FakeShard(), "src-c")
    # No attachments injected -> key absent or empty.
    assert not msg_json.get("attachments")


# --------------------------------------------------------------------------- EML


def _folder_task(item):
    from mail_export import FolderExportTask
    return FolderExportTask(
        folder_name="Inbox",
        items=[item],
        shard=_FakeShard(),
        source_container="src-c",
        dest_container="dst-c",
        dest_blob_prefix="job/Inbox",
        split_bytes=10 ** 9,
        block_size=1024,
        fetch_batch_size=4,
        queue_maxsize=4,
        format="EML",
        include_attachments=True,
        manifest=None,
    )


async def test_eml_item_attachment_embeds_rfc822(monkeypatch):
    _patch_common(monkeypatch, NESTED_BYTES)
    item = _Item([_Att("#microsoft.graph.itemAttachment", blob_path="nested/blob")])
    _msg, eml_bytes = await _folder_task(item)._build_eml_for_item(item)

    assert b"message/rfc822" in eml_bytes
    # The nested subject rides inside the embedded .eml part.
    assert b"NESTED-SUBJECT-42" in eml_bytes
    # And the whole thing still parses as one message with a nested payload.
    parsed = email.message_from_bytes(eml_bytes)
    assert any(
        part.get_content_type() == "message/rfc822"
        for part in parsed.walk()
    )


async def test_eml_reference_attachment_skipped(monkeypatch):
    _patch_common(monkeypatch, b"ignored")
    item = _Item([_Att("#microsoft.graph.referenceAttachment", blob_path="ref/blob")])
    _msg, eml_bytes = await _folder_task(item)._build_eml_for_item(item)
    assert b"message/rfc822" not in eml_bytes
