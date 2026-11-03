"""Regression: the incremental chat drain must not send $orderby.

Graph rejects it on chatMessage collections. Captured live from prod on
2026-08-06 (the graph-error-body logging added in 55e3a11 finally showed the
body behind a 400 that had been opaque since 2026-06-17)::

    GET /v1.0/chats/19:meeting_…@thread.v2/messages
        ?$top=50&$filter=lastModifiedDateTime gt 2025-02-03T19:45:37.387Z
        &$orderby=lastModifiedDateTime asc
    -> 400 {"error":{"code":"BadRequest","message":"QueryOptions to order by
            'LastModifiedDateTime' in 'Ascending' direction is not supported."}}

$orderby is only attached on the incremental-$filter branch, which is why
first-run chats succeeded and every chat WITH a saved cursor failed — all 20
of Corp_Esplanade's chats, and 119 across the six shard apps.

Ordering is not load-bearing: the per-chat cursor is a running MAX over the
page's stamps (`stamp > max_stamp`), not "last message wins", so the pages may
arrive in any order.

This is a source assertion because the params dict is built deep inside
_drain_one_chat's nested closure with no injectable seam; same convention as
tests/shared/test_graph_get_params.py.
"""
from __future__ import annotations

import io
import tokenize
import pathlib
import re

_MAIN = (
    pathlib.Path(__file__).resolve().parents[2]
    / "workers" / "backup-worker" / "main.py"
).read_text()


def test_no_orderby_anywhere_in_the_worker():
    # STRING tokens that are standalone docstrings do not execute requests.
    import ast
    tree = ast.parse(_MAIN)
    doc_lines = {
        n.body[0].lineno
        for n in ast.walk(tree)
        if isinstance(n, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
        and n.body and isinstance(n.body[0], ast.Expr)
        and isinstance(n.body[0].value, ast.Constant)
        and isinstance(n.body[0].value.value, str)
    }
    offenders = [
        token.string for token in tokenize.generate_tokens(io.StringIO(_MAIN).readline)
        if token.type == tokenize.STRING and "$orderby" in token.string
        and token.start[0] not in doc_lines
    ]
    assert offenders == [], (
        "Graph rejects $orderby on chatMessage collections "
        f"(400 BadRequest): {offenders}"
    )


def test_incremental_filter_is_still_applied():
    """Removing $orderby must not remove the incremental $filter with it —
    that filter is what keeps incrementals cheap."""
    assert 'params["$filter"] = (' in _MAIN
    assert "lastModifiedDateTime gt {saved_cursor}" in _MAIN


def test_chat_cursor_is_a_running_max_not_last_page_wins():
    """Pins the invariant that makes dropping $orderby safe."""
    assert re.search(r"or\s+stamp\s*>\s*max_stamp", _MAIN), (
        "per-chat cursor no longer a running max — page order would matter"
    )
