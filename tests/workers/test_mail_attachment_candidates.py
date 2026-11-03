"""USER_MAIL attachment candidates: Graph's hasAttachments ignores inline (cid:) images,
and the streaming persist path filtered on hasAttachments only, so 44 of Divyansh's
messages with 297 inline attachments never reached the attachment helper
(2026-11-04 local audit). One predicate must gate both sites."""
import importlib.util
import sys
from pathlib import Path

import pytest


@pytest.fixture(scope="module")
def bw():
    path = Path(__file__).resolve().parents[2] / "workers/backup-worker/main.py"
    spec = importlib.util.spec_from_file_location("bw_mail_candidates", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_inline_only_message_is_an_attachment_candidate(bw):
    assert bw._mail_needs_attachments({"hasAttachments": False, "body": {"content": '<img src="cid:logo">'}})
    assert bw._mail_needs_attachments({"hasAttachments": True, "body": {"content": "plain"}})
    assert not bw._mail_needs_attachments({"hasAttachments": False, "body": {"content": "plain"}})
    assert not bw._mail_needs_attachments({})


def test_streaming_candidate_filter_keeps_inline_only_messages(bw):
    rows = [("EMAIL", "a", "x", {"raw": {"hasAttachments": False, "body": {"content": "cid:img1"}}}),
            ("EMAIL", "b", "y", {"raw": {"hasAttachments": True}}),
            ("EMAIL", "c", "z", {"raw": {"hasAttachments": False, "body": {"content": "nothing"}}}),
            ("EMAIL", "d", "w", {})]
    assert [t[1] for t in bw._mail_attachment_candidates(rows)] == ["a", "b"]
