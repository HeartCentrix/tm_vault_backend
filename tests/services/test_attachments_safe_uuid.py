"""Regression: GET /snapshots/{id}/items/{id}/attachments must not 500 on a
literal 'None' path param.

The frontend serializes a null id into the URL as the string 'None' — e.g. a
chat message reconstructed from the shared durable store (the f0481cb union)
carries no snapshot_item id, so the chips fetch hits
GET /api/v1/resources/snapshots/None/items/None/attachments. The handler did
`UUID(item_id)` / `UUID(snapshot_id)`, and UUID('None') raises ValueError ->
unhandled -> HTTP 500. `_safe_uuid` parses to None on junk input so the handler
can degrade to [] (no attachments) instead of 500ing.
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import sys
import uuid

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")

_MAIN_PATH = (
    pathlib.Path(__file__).resolve().parents[2]
    / "services" / "snapshot-service" / "main.py"
)
_spec = importlib.util.spec_from_file_location("snap_main_safeuuid", _MAIN_PATH)
_m = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["snap_main_safeuuid"] = _m
try:
    _spec.loader.exec_module(_m)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover
    pytest.skip(
        f"snapshot-service module failed to import: {exc}",
        allow_module_level=True,
    )


def test_safe_uuid_rejects_none_and_junk():
    # The literal strings the frontend serializes a null id into.
    assert _m._safe_uuid("None") is None
    assert _m._safe_uuid("null") is None
    assert _m._safe_uuid("undefined") is None
    assert _m._safe_uuid("") is None
    assert _m._safe_uuid(None) is None
    assert _m._safe_uuid("not-a-uuid") is None


def test_safe_uuid_parses_valid():
    u = str(uuid.uuid4())
    assert _m._safe_uuid(u) == uuid.UUID(u)


# ── ARCHIVE_ATTACHMENT chips resolve on blob presence alone ────────────────
# Archive fileAttachments are captured as sibling ARCHIVE_ATTACHMENT rows whose
# bytes always live in a blob (enrichment only creates the row after it
# decoded+uploaded them). Unlike EMAIL/CHAT rows they carry NO `resolved` meta
# flag, so the attachments endpoint must treat blob presence as resolved — else
# the preview chip stays stuck on "Has attachments (capturing…)" forever.
def test_archive_attachment_resolves_on_blob_without_meta_flag():
    assert _m._attachment_resolved("ARCHIVE_ATTACHMENT", {}, "tenant/x/blob") is True
    # inline_b64 path also counts as local bytes
    assert _m._attachment_resolved("ARCHIVE_ATTACHMENT", {"inline_b64": "abc"}, None) is True


def test_archive_attachment_unresolved_without_bytes():
    assert _m._attachment_resolved("ARCHIVE_ATTACHMENT", {}, None) is False


def test_email_attachment_still_requires_explicit_resolved_flag():
    # referenceAttachment: has a row + blob path but resolved flag absent → not local
    assert _m._attachment_resolved("EMAIL_ATTACHMENT", {}, "tenant/x/blob") is False
    assert _m._attachment_resolved("EMAIL_ATTACHMENT", {"resolved": True}, "tenant/x/blob") is True
