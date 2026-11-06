"""USER_ONEDRIVE deletions were only counted, never recorded, so a deleted file stayed visible in
every later restore point (Recovery merges restore points newest-wins). The delta walk now records
each Graph deletion as an is_deleted ONEDRIVE_FILE row in the run's snapshot, like SharePoint's
library tombstones; older restore points keep their live row.

Gated by ONEDRIVE_RECORD_DELETIONS (default off) until the Recovery OneDrive reads drop
is_deleted rows AFTER the newest-wins dedup: today /onedrive and /folders filter them before it
(so the older live row would still win) and /onedrive/ids does not filter them at all."""
from __future__ import annotations

import pathlib
import re
import uuid
from types import SimpleNamespace

_SRC = (pathlib.Path(__file__).resolve().parents[2] / "workers" / "backup-worker" / "main.py").read_text()
_FLAT = re.sub(r"\s+", " ", _SRC)


def _load(name):
    start = _SRC.index(f"def {name}(")
    end = _SRC.index("\ndef ", start + 1)
    ns: dict = {"uuid": uuid}
    exec("from typing import Any, Dict\n" + _SRC[start:end], ns)
    return ns[name]


def test_a_graph_deletion_becomes_an_is_deleted_onedrive_row_in_this_snapshot():
    row = _load("_onedrive_tombstone_row")(
        SimpleNamespace(id="snap", backend_id="be"), "ten",
        {"id": "f1", "name": "a.docx", "deleted": {"state": "deleted"},
         "parentReference": {"path": "/drive/root:/Docs"}})
    assert row["snapshot_id"] == "snap" and row["tenant_id"] == "ten" and row["backend_id"] == "be"
    assert (row["external_id"], row["item_type"], row["is_deleted"], row["blob_path"]) == \
        ("f1", "ONEDRIVE_FILE", True, None)
    assert row["name"] == "a.docx" and row["folder_path"] == "/Docs" and row["content_size"] == 0


def test_unnamed_deletion_falls_back_to_its_id():
    row = _load("_onedrive_tombstone_row")(SimpleNamespace(id="s", backend_id="b"), "t", {"id": "f2", "@removed": {}})
    assert row["name"] == "f2" and row["folder_path"] == "/"


def test_walk_records_deletions_only_when_enabled():
    assert 'os.getenv( "ONEDRIVE_RECORD_DELETIONS", "false")' in _FLAT
    assert "deleted_items.append(f)" in _FLAT
    assert "await self._insert_snapshot_item_rows([ _onedrive_tombstone_row(snapshot, resource.tenant_id, d)" in _FLAT


def test_full_walk_tombstones_unseen_files_only_when_enabled_and_complete():
    i = _FLAT.index("await self._tombstone_unseen_onedrive_files(")
    gate = _FLAT[i - 400:i]
    assert "not saved_delta" in gate and "pages_seen <= 10000" in gate and "_record_deletions" in gate
