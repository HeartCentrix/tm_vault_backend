"""Archive→PST wiring for the PST exporter (workers/restore-worker/pst_export.py).

ARCHIVE_ITEM rides the MAIL writer, but ONLY once enriched: the mail writer reads
the Graph message JSON from ``extra_data.raw``; an un-enriched archive row is just
the opaque MAPI blob, so feeding it to ``fetch_message`` would blow up on
``json.loads``. The exporter must therefore register ARCHIVE_ITEM AND gate it on
enrichment.

workers/restore-worker has a hyphen → put it on sys.path and import directly."""
from __future__ import annotations

import pathlib
import sys

_ROOT = pathlib.Path(__file__).resolve().parents[2]
_WORKER = _ROOT / "workers" / "restore-worker"
for _p in (str(_ROOT), str(_WORKER)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from pst_export import PstGroupPlanner, _archive_pst_exportable  # noqa: E402


class _Item:
    def __init__(self, item_type, external_id="e1", extra_data=None):
        self.item_type = item_type
        self.snapshot_id = "00000000-0000-0000-0000-000000000001"
        self.external_id = external_id
        self.folder_path = "Archive"
        self.name = "x"
        self.extra_data = extra_data or {}


def test_archive_item_is_registered_for_pst():
    assert "ARCHIVE_ITEM" in PstGroupPlanner.PST_ITEM_TYPES
    assert PstGroupPlanner.TYPE_SUFFIX["ARCHIVE_ITEM"] == "archive"


def test_enrichment_gate_passes_only_enriched_archive():
    # Non-archive always exportable.
    assert _archive_pst_exportable(_Item("EMAIL")) is True
    # Enriched archive (extra_data.raw present) exportable.
    assert _archive_pst_exportable(_Item("ARCHIVE_ITEM", extra_data={"raw": {"subject": "hi"}})) is True
    # Un-enriched archive (opaque blob only) NOT exportable.
    assert _archive_pst_exportable(_Item("ARCHIVE_ITEM", extra_data={})) is False
    assert _archive_pst_exportable(_Item("ARCHIVE_ITEM", extra_data={"opaque": True})) is False


def test_plan_groups_only_enriched_archive_with_archive_suffix():
    items = [
        _Item("ARCHIVE_ITEM", external_id="a", extra_data={"raw": {"subject": "1"}}),
        _Item("ARCHIVE_ITEM", external_id="b", extra_data={}),  # un-enriched → dropped
    ]
    groups = PstGroupPlanner().plan(items, "MAILBOX", {"ARCHIVE_ITEM"})
    assert len(groups) == 1
    g = groups[0]
    assert g.item_type == "ARCHIVE_ITEM"
    assert [it.external_id for it in g.items] == ["a"]
    assert g.pst_filename.endswith("-archive.pst")
