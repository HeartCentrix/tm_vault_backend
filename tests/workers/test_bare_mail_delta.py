"""Pins the detector for bare /messages/delta change-notifications.

Bug: Microsoft Graph /messages/delta returns a message that merely CHANGED
(flagged/read/moved) as a bare entry carrying only {id, @odata.etag,
@odata.type} — and, stickily, returns EVERY changed message that way once the
saved deltaLink was produced by a $select=id walk (_finalize_deltalink_idonly).
Persisting that verbatim yields a '(no subject)' snapshot item with no body and
no mail_message_bodies row (observed live: Rohit's Jul-3 Inbox items were 265 B
each, metadata = {"raw": {id, etag, type}}). The capture must detect these and
re-fetch the full message by id before persisting.

_is_bare_mail_delta lives at module scope in backup-worker/main.py (hyphen dir),
loaded here via AST extraction — same pattern as test_anomaly_cursor_guard.py.
"""
from __future__ import annotations

import ast
import pathlib
import sys

_BW_MAIN = (
    pathlib.Path(__file__).resolve().parents[2]
    / "workers" / "backup-worker" / "main.py"
)


def _load_fn():
    tree = ast.parse(_BW_MAIN.read_text())
    fn = None
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == "_is_bare_mail_delta":
            fn = node
    if fn is None:
        raise RuntimeError("_is_bare_mail_delta not found at module scope")
    module = type(sys)("bare_mail_delta_under_test")
    module.__dict__["Any"] = object  # satisfy the type annotation reference
    code = compile(ast.Module(body=[fn], type_ignores=[]), str(_BW_MAIN), "exec")
    exec(code, module.__dict__)
    return module._is_bare_mail_delta


is_bare = _load_fn()


def test_bare_id_etag_only_is_bare():
    m = {"@odata.type": "#microsoft.graph.message",
         "@odata.etag": "W/\"abc\"", "id": "AAMk123"}
    assert is_bare(m) is True


def test_full_message_is_not_bare():
    m = {"id": "AAMk123", "subject": "Hello", "body": {"content": "hi"}}
    assert is_bare(m) is False


def test_empty_subject_present_is_not_bare():
    # a genuinely blank-subject mail still carries the subject KEY (and a body)
    m = {"id": "AAMk123", "subject": "", "body": {"content": ""}}
    assert is_bare(m) is False


def test_body_present_without_subject_is_not_bare():
    m = {"id": "AAMk123", "body": {"content": "x"}}
    assert is_bare(m) is False


def test_removed_tombstone_is_not_bare():
    # deletion markers legitimately carry only id + @removed; keep them as-is.
    assert is_bare({"id": "AAMk123", "@removed": {"reason": "deleted"}}) is False


def test_no_id_is_not_bare():
    assert is_bare({"@odata.etag": "W/\"abc\""}) is False


def test_non_dict_is_not_bare():
    assert is_bare(None) is False
    assert is_bare("nope") is False
