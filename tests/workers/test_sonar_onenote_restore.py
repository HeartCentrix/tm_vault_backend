"""OneNote restore preserves hierarchy and counts failures without losing later pages."""
import importlib.util
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest


def _item(kind, identity, name=None, **metadata):
    return SimpleNamespace(item_type=kind, external_id=identity, name=name, extra_data=metadata)


@pytest.mark.asyncio
async def test_hierarchy_failures_and_placeholders_preserve_counts(monkeypatch):
    path = Path(__file__).resolve().parents[2] / "workers" / "restore-worker" / "main.py"
    monkeypatch.syspath_prepend(str(path.parent))
    name = "restore_worker_main_for_tests"
    module = sys.modules.get(name)
    if module is None:
        spec = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(spec)
        monkeypatch.setitem(sys.modules, name, module)
        spec.loader.exec_module(module)
    worker = module.RestoreWorker()
    posts = []

    class Graph:
        GRAPH_URL = "https://graph.test/v1.0"

        async def _post(self, url, body, headers=None):
            posts.append((url, body, headers))
            if isinstance(body, dict):
                display_name = body["displayName"]
                if display_name.startswith("Bad"):
                    raise RuntimeError("create rejected")
                return {"id": f"new-{display_name}"}
            if b"Page rejected" in body:
                raise RuntimeError("page rejected")
            return {"id": "page"}

    async def get_graph(tenant):
        return Graph()

    async def load_bytes(item, workload):
        assert workload == "onenote"
        assert item.external_id == "captured:content"
        return b"<html>Captured page</html>"

    monkeypatch.setattr(worker, "get_graph_client", get_graph)
    monkeypatch.setattr(worker, "_load_snapshot_item_bytes", load_bytes)
    items = [
        _item("ONENOTE_NOTEBOOK", "notebook", "Notebook"),
        _item("ONENOTE_NOTEBOOK", "bad-notebook", "Bad notebook"),
        _item("ONENOTE_NOTEBOOK", "nameless"),
        _item("ONENOTE_SECTION", "section", "Section", notebookId="notebook"),
        _item("ONENOTE_SECTION", "orphan-section", "Orphan section", notebookId="bad-notebook"),
        _item("ONENOTE_SECTION", "bad-section", "Bad section", notebookId="notebook"),
        _item("ONENOTE_PAGE", "orphan-page", "Orphan page", sectionId="orphan-section"),
        _item("ONENOTE_PAGE", "rejected", "Page rejected", sectionId="section"),
        _item("ONENOTE_PAGE", "placeholder", "Placeholder", sectionId="section"),
        _item("ONENOTE_PAGE", "captured", "Captured", sectionId="section"),
        _item("ONENOTE_PAGE_CONTENT", "captured:content"),
        _item("ONENOTE_PAGE_CONTENT", "orphan:content"),
        _item("ONENOTE_RESOURCE", "image"),
        _item("ONENOTE_RESOURCE", "attachment"),
    ]
    result = await worker._restore_onenote_items(
        None, SimpleNamespace(external_id="target-user"), items, SimpleNamespace(),
    )
    assert result["restored_count"] == 4
    assert result["failed_count"] == 6
    assert result["restore_type"] == "ONENOTE"
    actions = result["manual_actions"]
    assert actions == sorted(set(actions))
    assert "Bad notebook (notebook): create failed — create rejected" in actions
    assert "Orphan section: parent notebook not restored, skipping section." in actions
    assert "Bad section (section): create failed — create rejected" in actions
    assert "Orphan page: parent section not restored, skipping page." in actions
    assert "Page rejected: page create failed — page rejected" in actions
    assert "Placeholder: no ONENOTE_PAGE_CONTENT captured; placeholder body used." in actions
    assert any(action.startswith("2 inline resource(s) skipped:") for action in actions)
    page_posts = [(url, body, headers) for url, body, headers in posts if url.endswith("/pages")]
    assert len(page_posts) == 3
    assert all("/users/target-user/onenote/sections/new-Section/pages" in url for url, _, _ in page_posts)
    assert all(headers == {"Content-Type": "text/html"} for _, _, headers in page_posts)
    assert b"(content not captured)" in page_posts[1][1]
    assert page_posts[2][1] == b"<html>Captured page</html>"
