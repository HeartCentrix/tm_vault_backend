"""Regression: a bad ONENOTE_PAGE_CONTENT blob read is a per-page failure.

The blob loader used to short-circuit to None whenever the Azure-only
``blob_service_client`` was unset (always, on SeaweedFS deployments), so
the OneNote page loop could not raise. Once it was converted to the
storage facade it propagates transport errors, and this call site had no
guard: one 5xx on a single page blob unwound out of
``_restore_onenote_items`` — which ``restore_in_place`` calls bare — and
failed the whole multi-resource job.

Pinned behavior:
  * an unreadable content blob is counted in ``failed_count`` and named
    in ``manual_actions``; the exception never escapes;
  * the remaining pages in the same restore still get created;
  * a *missing* blob (loader returns None) keeps the pre-existing
    placeholder-body behavior — only read *errors* refuse to post.
"""
from __future__ import annotations

import asyncio
import importlib.util
import pathlib
import sys
from types import SimpleNamespace

_WORKER_DIR = pathlib.Path(__file__).resolve().parents[2] / "workers" / "restore-worker"
if str(_WORKER_DIR) not in sys.path:
    sys.path.insert(0, str(_WORKER_DIR))


def _load_main():
    name = "restore_worker_main_for_tests"
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, _WORKER_DIR / "main.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class _FakeGraph:
    GRAPH_URL = "https://graph.test/v1.0"

    def __init__(self):
        self.posts = []

    async def _post(self, url, body, headers=None):
        self.posts.append((url, body))
        return {"id": f"new-{len(self.posts)}"}


class _ExplodingShard:
    """Raises on the named blob paths, returns bytes for the rest."""

    def __init__(self, blobs, exploding):
        self.blobs = blobs
        self.exploding = exploding

    async def download_blob(self, container, blob_path):
        if blob_path in self.exploding:
            raise RuntimeError("SeaweedFS 503 while reading page content")
        return self.blobs.get(blob_path)


_TENANT_ID = "11111111-1111-1111-1111-111111111111"


def _item(item_type, external_id, name, extra_data=None, blob_path=None):
    return SimpleNamespace(
        item_type=item_type,
        external_id=external_id,
        name=name,
        blob_path=blob_path,
        tenant_id=_TENANT_ID,
        extra_data=extra_data or {},
    )


def _onenote_items():
    """One notebook, one section, two pages — page A's content blob is
    the one that will explode."""
    return [
        _item("ONENOTE_NOTEBOOK", "nb1", "Notebook", {"raw": {"displayName": "Notebook"}}),
        _item("ONENOTE_SECTION", "sec1", "Section",
              {"raw": {"displayName": "Section"}, "notebookId": "nb1"}),
        _item("ONENOTE_PAGE", "pgA", "Page A", {"raw": {"title": "Page A"}, "sectionId": "sec1"}),
        _item("ONENOTE_PAGE_CONTENT", "pgA:content", "Page A content",
              blob_path="blob/pgA"),
        _item("ONENOTE_PAGE", "pgB", "Page B", {"raw": {"title": "Page B"}, "sectionId": "sec1"}),
        _item("ONENOTE_PAGE_CONTENT", "pgB:content", "Page B content",
              blob_path="blob/pgB"),
    ]


def _run(monkeypatch, exploding):
    main_mod = _load_main()
    worker = main_mod.RestoreWorker()
    graph = _FakeGraph()

    async def _graph(_tenant):
        return graph

    monkeypatch.setattr(worker, "get_graph_client", _graph)
    monkeypatch.setattr(
        main_mod.azure_storage_manager, "get_shard_for_item",
        lambda item: _ExplodingShard({"blob/pgA": b"<html>A</html>",
                                      "blob/pgB": b"<html>B</html>"}, exploding),
    )

    result = asyncio.run(worker._restore_onenote_items(
        session=None,
        target_resource=SimpleNamespace(external_id="user-1"),
        items=_onenote_items(),
        tenant=SimpleNamespace(external_tenant_id="tid", graph_client_id=None),
    ))
    return result, graph


def test_unreadable_content_blob_is_counted_not_raised(monkeypatch):
    result, graph = _run(monkeypatch, exploding={"blob/pgA"})

    # Job-level: no exception, and the loss is visible.
    assert result["failed_count"] == 1
    assert any("Page A" in a and "unreadable" in a for a in result["manual_actions"])

    # Page B (and the notebook + section) still restored.
    assert result["restored_count"] == 3
    page_posts = [u for u, _ in graph.posts if "/pages" in u]
    assert len(page_posts) == 1


def test_all_pages_restore_when_blobs_are_readable(monkeypatch):
    result, graph = _run(monkeypatch, exploding=set())

    assert result["failed_count"] == 0
    assert result["restored_count"] == 4
    bodies = [b for u, b in graph.posts if "/pages" in u]
    assert sorted(bodies) == [b"<html>A</html>", b"<html>B</html>"]
