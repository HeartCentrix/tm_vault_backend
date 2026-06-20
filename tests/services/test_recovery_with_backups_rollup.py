"""Regression pins for Recovery's resources-with-backups list.

When an SLA backs up only Mail/OneDrive/Chats/etc. for an M365 user, the
completed snapshots live on hidden Tier-2 child resources such as USER_MAIL.
The Recovery list still needs to show the parent ENTRA_USER row by rolling
those child stats up before applying the UI-hidden resource filter.
"""
from __future__ import annotations

import ast
import pathlib

import pytest


_SNAPSHOT_MAIN = (
    pathlib.Path(__file__).resolve().parents[2]
    / "services" / "snapshot-service" / "main.py"
)


def _find_endpoint(tree: ast.Module, route_path: str) -> ast.AsyncFunctionDef:
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


@pytest.fixture(scope="module")
def resources_with_backups_source() -> str:
    text = _SNAPSHOT_MAIN.read_text()
    tree = ast.parse(text)
    return ast.get_source_segment(text, _find_endpoint(tree, "/resources/with-backups")) or ""


def test_resources_with_backups_collects_hidden_children_before_display_filter(
    resources_with_backups_source: str,
):
    """The DB fetch feeding rollup must not exclude USER_MAIL children.

    Filtering hidden Tier-2 types in the initial Resource query makes
    mailbox-only backups disappear from Recovery because there is no parent
    snapshot to count.
    """
    rollup_marker = "Roll children up under their parent"
    assert rollup_marker in resources_with_backups_source
    before_rollup = resources_with_backups_source.split(rollup_marker, 1)[0]

    assert "Resource.type.notin_(_HIDDEN_RECOVERY_TYPES)" not in before_rollup
