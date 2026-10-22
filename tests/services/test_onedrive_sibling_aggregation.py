"""Pins that the /onedrive and /onedrive/ids endpoints aggregate across
sibling snapshots like /mail, /chats, /calendar and /contacts do.

Origin: 2026-05-15 — Amit's My Drive root showed the 4 expected
folder rows (because /folders already aggregates) but ZERO files. Cause:
/onedrive filtered to a single `snapshot_id` while the latest snapshot
was an empty delta and the actual files lived in an earlier full-pull
sibling snapshot.

Symptom: empty file list on a OneDrive that obviously has content,
specifically right after any incremental backup that adds nothing.

This test is a source-level pin: it AST-walks snapshot-service/main.py
and confirms each OneDrive endpoint calls _resolve_sibling_snapshot_ids
with target_child_type=ResourceType.USER_ONEDRIVE. A future refactor
that removes the call (and reintroduces the bug) will fail here loudly
instead of regressing silently on prod.
"""
from __future__ import annotations

import ast
import pathlib

import pytest


_SNAPSHOT_MAIN = (
    pathlib.Path(__file__).resolve().parents[2]
    / "services" / "snapshot-service" / "main.py"
)


def _find_endpoint(tree: ast.Module, route_path: str) -> ast.FunctionDef:
    """Return the FunctionDef immediately preceded by an @app.get(<route>)
    decorator. snapshot-service stacks multiple @app.get's on one
    function (e.g. /emails + /mail) — we match any decorator whose
    string arg endswith `route_path`."""
    for node in ast.walk(tree):
        if not isinstance(node, ast.AsyncFunctionDef):
            continue
        for dec in node.decorator_list:
            if not isinstance(dec, ast.Call):
                continue
            for arg in dec.args:
                if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                    if arg.value.endswith(route_path):
                        return node
    raise LookupError(f"endpoint not found for route ending in {route_path!r}")


def _function_calls_helper_with_user_onedrive(
    fn: ast.AsyncFunctionDef,
) -> bool:
    """Return True iff `fn` calls _resolve_sibling_snapshot_ids with
    target_child_type=ResourceType.USER_ONEDRIVE as a keyword argument."""
    for node in ast.walk(fn):
        if not isinstance(node, ast.Await):
            continue
        call = node.value
        if not isinstance(call, ast.Call):
            continue
        func = call.func
        name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", None)
        if name != "_resolve_sibling_snapshot_ids":
            continue
        for kw in call.keywords:
            if kw.arg != "target_child_type":
                continue
            # Expect ResourceType.USER_ONEDRIVE (Attribute on Name).
            val = kw.value
            if (
                isinstance(val, ast.Attribute)
                and val.attr == "USER_ONEDRIVE"
                and isinstance(val.value, ast.Name)
                and val.value.id == "ResourceType"
            ):
                return True
    return False


@pytest.fixture(scope="module")
def parsed():
    return ast.parse(_SNAPSHOT_MAIN.read_text())


def test_onedrive_list_endpoint_aggregates_siblings(parsed):
    """/onedrive returns OneDrive files across every sibling snapshot,
    not just the literal `snapshot_id` from the URL — newest-wins
    dedup by external_id."""
    fn = _find_endpoint(parsed, "/onedrive")
    assert _function_calls_helper_with_user_onedrive(fn), (
        "list_snapshot_onedrive must call "
        "_resolve_sibling_snapshot_ids(..., target_child_type="
        "ResourceType.USER_ONEDRIVE)"
    )


def test_onedrive_ids_endpoint_aggregates_siblings(parsed):
    """/onedrive/ids (used by the folder-row bulk-select checkbox)
    needs the same aggregation as /onedrive — without it the
    'select all files in folder X' returns the empty set whenever
    the latest snapshot is a no-op delta."""
    fn = _find_endpoint(parsed, "/onedrive/ids")
    assert _function_calls_helper_with_user_onedrive(fn), (
        "list_onedrive_ids_by_prefix must call "
        "_resolve_sibling_snapshot_ids(..., target_child_type="
        "ResourceType.USER_ONEDRIVE)"
    )


def test_onedrive_endpoints_do_not_filter_single_snapshot_id(parsed):
    """Belt-and-braces: ensure neither endpoint still contains a
    'SnapshotItem.snapshot_id == UUID(snapshot_id)' pattern (which
    would override the sibling list). Aggregation is wired either as
    'SnapshotItem.snapshot_id.in_(sibling_ids)' (ORM path) or by
    handing sibling_ids to _paged_latest_items (SQL-side path,
    2026-07-21 OOM fix).
    """
    for route in ("/onedrive", "/onedrive/ids"):
        fn = _find_endpoint(parsed, route)
        source = ast.unparse(fn)
        assert "snapshot_id == UUID(snapshot_id)" not in source, (
            f"endpoint {route} still filters by single snapshot_id — "
            f"this re-introduces the 2026-05-15 empty-OneDrive bug."
        )
        assert (
            "snapshot_id.in_(sibling_ids)" in source
            or "_paged_latest_items(db, sibling_ids" in source
        ), (
            f"endpoint {route} does not aggregate over sibling_ids — "
            f"sibling aggregation isn't wired up correctly."
        )
