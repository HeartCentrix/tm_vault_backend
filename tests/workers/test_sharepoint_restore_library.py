"""SharePoint restore must land each file in its own library, inside its own sub-folder.

Found 2026-11-04 (code + Railway data): every SharePoint restore PUT into the site's DEFAULT library
with the stored '{site}/lists/{library}/...' trail as folders (a Site Assets file landed in Documents
under 'Donor Desk/lists/Site Assets/...'), and library files (item_type FILE) were routed to the
OneDrive restore, which used the site id as a drive id."""
from __future__ import annotations

import asyncio
import importlib.util
import pathlib
import sys
from datetime import datetime
from types import SimpleNamespace
from uuid import UUID

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


class Graph:
    GRAPH_URL = "https://graph.test/v1.0"

    def __init__(self):
        self.puts, self.gets = [], []

    async def _put(self, url, content, headers=None):
        self.puts.append(url)
        return {"id": "new-item"}

    async def _get(self, url, params=None):
        self.gets.append(url)
        if url.endswith("/lists/list-sa/drive") or url.endswith("/lists/test-sa/drive"):
            return {"id": "drive-sa" if "list-sa" in url else "test-drive-sa"}
        if url.endswith("/lists/test-docs/drive"):
            return {"id": "test-drive-docs"}
        if "/sites/test-site/lists" in url:
            # Graph hides system libraries (Site Assets, Style Library, Site Pages) unless `system` is selected.
            system = "system" in (params or {}).get("$select", "").split(",")
            return {"value": [{"id": "test-docs", "displayName": "Documents"}]
                    + ([{"id": "test-sa", "displayName": "Site Assets", "system": {}}] if system else [])}
        raise AssertionError("unexpected GET " + url)


def worker_with(monkeypatch, main):
    worker = main.RestoreWorker()

    async def no_perms(*a, **k):
        return None

    class Shard:
        async def download_blob(self, container, path):
            return b"bytes"

        async def get_blob_properties(self, container, path):
            return {"name": path, "size": 5}

    monkeypatch.setattr(worker, "_replay_file_permissions", no_perms)
    monkeypatch.setattr(main.azure_storage_manager, "get_shard_for_item", lambda item: Shard())
    return worker


def library_file():
    return SimpleNamespace(item_type="FILE", external_id="f1", name="plan.docx", blob_path="b/f1",
                           folder_path="Donor Desk/lists/Documents/General", tenant_id="t",
                           extra_data={"structured": {"drive_id": "drive-docs", "library": "Documents"}})


def site_asset():
    """Stored shape (Railway, Donor Desk): the list pass keeps the library's URL folder in the trail."""
    return SimpleNamespace(item_type="SHAREPOINT_FILE", external_id="list-sa:7", name="logo.png", blob_path="b/7",
                           folder_path="Donor Desk/lists/Site Assets/SiteAssets/SitePages", tenant_id="t",
                           snapshot_id=SNAPSHOT,
                           extra_data={"list_id": "list-sa", "list_name": "Site Assets", "source": "sp_rest",
                                       "server_relative_url": "/sites/DonorDesk/SiteAssets/SitePages/logo.png"})


SNAPSHOT = UUID("00000000-0000-0000-0000-0000000000a1")


class Session:
    """The snapshot's list row for Site Assets (its web_url is the library's root folder)."""

    async def execute(self, stmt, params=None):   # shared.snapshot_reuse.items_scope: a full snapshot
        return SimpleNamespace(first=lambda: SimpleNamespace(target_id=SNAPSHOT, overlay_base_id=None))

    async def scalar(self, stmt):
        sql = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "'list-sa'" in sql and SNAPSHOT.hex in sql and "'SHAREPOINT_LIST'" in sql, sql
        return {"web_url": "https://qfion.sharepoint.com/sites/DonorDesk/SiteAssets"}


def test_a_library_file_restores_into_its_own_library_and_folder(monkeypatch):
    main = _load_main(); graph = Graph(); worker = worker_with(monkeypatch, main)
    asyncio.run(worker._restore_file_to_sharepoint(graph, SimpleNamespace(external_id="site-1"), library_file(),
                                                   conflict_mode="OVERWRITE"))
    assert graph.puts == ["https://graph.test/v1.0/drives/drive-docs/root:/General/plan.docx:/content"]


def test_a_list_file_restores_into_its_library_found_from_the_list(monkeypatch):
    main = _load_main(); graph = Graph(); worker = worker_with(monkeypatch, main)
    asyncio.run(worker._restore_file_to_sharepoint(graph, SimpleNamespace(external_id="site-1"), site_asset(),
                                                   conflict_mode="SEPARATE_FOLDER", session=Session()))
    day = datetime.utcnow().strftime("%Y-%m-%d")
    assert graph.puts == [f"https://graph.test/v1.0/drives/drive-sa/root:/Restored%20by%20TM/{day}/SitePages/logo.png:/content"]


def test_restore_into_another_site_matches_the_library_by_name(monkeypatch):
    main = _load_main(); graph = Graph(); worker = worker_with(monkeypatch, main)
    for item in (library_file(), site_asset()):
        asyncio.run(worker._restore_file_to_sharepoint(graph, SimpleNamespace(external_id="site-1"), item,
                                                       conflict_mode="OVERWRITE", target_site_id="test-site",
                                                       session=Session()))
    assert graph.puts == ["https://graph.test/v1.0/drives/test-drive-docs/root:/General/plan.docx:/content",
                          "https://graph.test/v1.0/drives/test-drive-sa/root:/SitePages/logo.png:/content"]


def test_library_files_of_a_site_go_to_the_sharepoint_restore():
    main = _load_main()
    site = SimpleNamespace(type=SimpleNamespace(value="SHAREPOINT_SITE"))
    drive = SimpleNamespace(type=SimpleNamespace(value="USER_ONEDRIVE"))
    assert main.RestoreWorker._is_sharepoint_file(site, library_file())
    assert main.RestoreWorker._is_sharepoint_file(site, site_asset())
    assert not main.RestoreWorker._is_sharepoint_file(drive, library_file())


def test_cross_resource_restore_never_reuses_the_source_sites_drive(monkeypatch):
    """restore_cross_resource passes the TARGET resource; the source site must decide same-site."""
    main = _load_main(); graph = Graph(); worker = worker_with(monkeypatch, main)
    asyncio.run(worker._restore_file_to_sharepoint(graph, SimpleNamespace(external_id="test-site"), library_file(),
                                                   conflict_mode="OVERWRITE", target_site_id="test-site",
                                                   source_site_id="site-1"))
    assert graph.puts == ["https://graph.test/v1.0/drives/test-drive-docs/root:/General/plan.docx:/content"]


def test_every_resource_type_the_restore_worker_branches_on_exists():
    """`if resource_type == "SHAREPOINT"` never matched (the type is SHAREPOINT_SITE), so 'restore to
    another site' and 'restore to a new site' silently restored into the ORIGINAL site since 2026-04-20."""
    import ast
    from shared.models import ResourceType
    tree = ast.parse((_WORKER_DIR / "main.py").read_text())
    valid = {t.value for t in ResourceType}
    compared = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Compare) and isinstance(node.left, ast.Name) and node.left.id in ("resource_type", "target_type"):
            for right in node.comparators:
                values = right.elts if isinstance(right, (ast.Tuple, ast.List, ast.Set)) else [right]
                compared |= {v.value for v in values if isinstance(v, ast.Constant) and isinstance(v.value, str)}
    assert compared and compared <= valid, sorted(compared - valid)
