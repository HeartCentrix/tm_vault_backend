"""Pure core of archive PREVIEW ENRICHMENT
(workers/restore-worker/archive_enrich.py): turn a staged Graph JSON message
(read back from the scratch mailbox) into the update applied to the source
ARCHIVE_ITEM row so it becomes previewable like normal mail. No DB/Graph."""
from __future__ import annotations

import pathlib
import sys

_ROOT = pathlib.Path(__file__).resolve().parents[2]
_WORKER = _ROOT / "workers" / "restore-worker"
for _p in (str(_ROOT), str(_WORKER)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import archive_enrich as ax  # noqa: E402


NESTED = {
    "subject": "Q3 Invoice",
    "from": {"emailAddress": {"name": "Alice", "address": "alice@x.com"}},
    "toRecipients": [{"emailAddress": {"address": "bob@y.com"}}],
    "receivedDateTime": "2026-07-14T08:40:58Z",
    "hasAttachments": True,
    "body": {"contentType": "html", "content": "<p>see attached</p>"},
    "attachments": [{"@odata.type": "#microsoft.graph.fileAttachment", "name": "inv.pdf"}],
}


def test_enrichment_from_message_extracts_preview_fields():
    e = ax.enrichment_from_message(NESTED)
    assert e["name"] == "Q3 Invoice"              # row name becomes the real subject
    patch = e["extra_data_patch"]
    assert patch["enrich_status"] == ax.ENRICHED
    assert patch["raw"] == NESTED                 # full JSON stored for preview
    assert patch["subject"] == "Q3 Invoice"
    assert "alice@x.com" in patch["from"]
    assert patch["to"] == ["bob@y.com"]
    assert patch["received"] == "2026-07-14T08:40:58Z"
    assert patch["has_attachments"] is True


def test_enrichment_handles_missing_subject_and_addresses():
    e = ax.enrichment_from_message({"body": {"content": ""}})
    assert e["name"] == "(no subject)"
    p = e["extra_data_patch"]
    assert p["enrich_status"] == ax.ENRICHED
    assert p["subject"] == "(no subject)"
    assert p["from"] == "" and p["to"] == []
    assert p["has_attachments"] is False


def test_has_attachments_true_when_attachments_present_without_flag():
    e = ax.enrichment_from_message({"subject": "x", "attachments": [{"name": "a"}]})
    assert e["extra_data_patch"]["has_attachments"] is True


def test_status_constants_and_needs_enrichment():
    assert {ax.PENDING, ax.PROCESSING, ax.ENRICHED, ax.FAILED} == {
        "pending", "processing", "enriched", "failed"}
    # a row is enrichable unless already enriched or currently processing
    assert ax.needs_enrichment({}) is True                       # never touched
    assert ax.needs_enrichment({"enrich_status": ax.FAILED}) is True   # retry failed
    assert ax.needs_enrichment({"enrich_status": ax.ENRICHED}) is False
    assert ax.needs_enrichment({"enrich_status": ax.PROCESSING}) is False


def test_addr_formats_name_and_address():
    assert ax._addr({"emailAddress": {"name": "Al", "address": "al@x.com"}}) == "Al <al@x.com>"
    assert ax._addr({"emailAddress": {"address": "al@x.com"}}) == "al@x.com"
    assert ax._addr({}) == ""


# ── enrich_items: orchestration over a stager (staged pairs -> row updates) ──

import pytest  # noqa: E402


class _Row:
    def __init__(self, ext):
        self.external_id = ext


class _FakeStager:
    def __init__(self, pairs):
        self._pairs = pairs

    async def stage_and_map(self, items):
        for p in self._pairs:
            yield p


@pytest.mark.asyncio
async def test_enrich_items_applies_one_update_per_staged_message():
    a, b = _Row("A"), _Row("B")
    stager = _FakeStager([(a, {"subject": "one"}), (b, {"subject": "two"})])
    updates = []

    async def apply(item, name, patch):
        updates.append((item.external_id, name, patch["enrich_status"]))

    n = await ax.enrich_items(stager, [a, b], apply)
    assert n == 2
    assert updates == [("A", "one", "enriched"), ("B", "two", "enriched")]


@pytest.mark.asyncio
async def test_enrich_items_isolates_per_item_failure():
    a, b = _Row("A"), _Row("B")
    stager = _FakeStager([(a, {"subject": "ok"}), (b, {"subject": "boom"})])

    async def apply(item, name, patch):
        if name == "boom":
            raise RuntimeError("db write failed")

    n = await ax.enrich_items(stager, [a, b], apply)
    assert n == 1  # A enriched; B failed but did not abort the run
