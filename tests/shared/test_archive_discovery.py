"""discover_user_content emits a USER_ARCHIVE Tier-2 child ONLY when an archive
mailbox is actually provisioned AND ARCHIVE_BACKUP_ENABLED is on.

Archive is opt-in per user (not every licensed user enables one), so ABSENCE =>
no row — unlike mail, which emits a license_missing marker. Flag off => the
archive is never even probed (zero added Graph calls / zero prod exposure)."""
from __future__ import annotations

import os

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")

from shared.graph_client import GraphClient
from shared import archive_graph


def _fake_get(archive_id):
    """A stub _get: settings/exchange carries the archive id; every category
    probe (mail/onedrive/contacts/calendar/chats) gets a benign empty body."""
    async def _get(url, params=None):
        if "/settings/exchange" in url:
            j = {"primaryMailboxId": "MBX:prim@t"}
            if archive_id:
                j["inPlaceArchiveMailboxId"] = archive_id
            return j
        if url.endswith("/drive"):
            return {"id": "drive-1", "quota": {}}
        return {"value": []}
    return _get


def _archive_rows(rows):
    return [r for r in rows if r["type"] == "USER_ARCHIVE"]


@pytest.mark.asyncio
async def test_emits_user_archive_when_provisioned_and_flag_on(monkeypatch):
    monkeypatch.setattr(archive_graph, "ARCHIVE_BACKUP_ENABLED", True)
    gc = GraphClient("id", "secret", "tenant")
    monkeypatch.setattr(gc, "_get", _fake_get("MBX:arch@t"))

    rows = await gc.discover_user_content("U1", "u1@t", "User One")
    arch = _archive_rows(rows)

    assert len(arch) == 1
    r = arch[0]
    assert r["external_id"] == "U1:archive"
    assert r["parent_external_id"] == "U1"
    assert r["metadata"]["archive_mailbox_id"] == "MBX:arch@t"
    # still emits the five normal categories too (archive is additive)
    assert {x["type"] for x in rows} >= {"USER_MAIL", "USER_ARCHIVE"}


@pytest.mark.asyncio
async def test_no_archive_row_when_not_provisioned(monkeypatch):
    monkeypatch.setattr(archive_graph, "ARCHIVE_BACKUP_ENABLED", True)
    gc = GraphClient("id", "secret", "tenant")
    monkeypatch.setattr(gc, "_get", _fake_get(None))  # no inPlaceArchiveMailboxId

    rows = await gc.discover_user_content("U1", "u1@t", "User One")
    assert _archive_rows(rows) == []


@pytest.mark.asyncio
async def test_flag_off_never_probes_or_emits_archive(monkeypatch):
    monkeypatch.setattr(archive_graph, "ARCHIVE_BACKUP_ENABLED", False)
    seen = []

    async def _get(url, params=None):
        seen.append(url)
        if url.endswith("/drive"):
            return {"id": "d", "quota": {}}
        return {"value": []}

    gc = GraphClient("id", "secret", "tenant")
    monkeypatch.setattr(gc, "_get", _get)

    rows = await gc.discover_user_content("U1", "u1@t", "User One")
    assert _archive_rows(rows) == []
    assert not any("/settings/exchange" in u for u in seen)  # never even probed


def test_tier2_persist_map_includes_archive_but_completeness_set_excludes_it():
    """The persist map MUST map USER_ARCHIVE (else discovery rows are silently
    dropped at persist), but the completeness set must NOT contain it — archive
    is opt-in, so requiring it would make has_complete_tier2 never-complete and
    force endless re-discovery. This asymmetry is the whole correctness story."""
    from shared.tier2_discovery import _TIER2_TYPE_MAP, TIER2_CHILD_TYPES
    from shared.models import ResourceType

    assert _TIER2_TYPE_MAP.get("USER_ARCHIVE") == ResourceType.USER_ARCHIVE
    assert ResourceType.USER_ARCHIVE not in TIER2_CHILD_TYPES


@pytest.mark.asyncio
async def test_archive_probe_error_yields_no_row(monkeypatch):
    """settings/exchange 404s for mailbox-less / unlicensed users — the probe
    must swallow it and emit nothing, never crash the whole discovery gather."""
    monkeypatch.setattr(archive_graph, "ARCHIVE_BACKUP_ENABLED", True)

    async def _get(url, params=None):
        if "/settings/exchange" in url:
            raise RuntimeError("404 MailboxNotEnabledForRESTAPI")
        if url.endswith("/drive"):
            return {"id": "d", "quota": {}}
        return {"value": []}

    gc = GraphClient("id", "secret", "tenant")
    monkeypatch.setattr(gc, "_get", _get)

    rows = await gc.discover_user_content("U1", "u1@t", "User One")
    assert _archive_rows(rows) == []
    # the five normal categories are unaffected by the archive probe failing
    assert {x["type"] for x in rows} >= {"USER_MAIL", "USER_ONEDRIVE"}
