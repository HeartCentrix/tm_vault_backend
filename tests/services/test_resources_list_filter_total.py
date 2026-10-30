"""Regression: GET /api/v1/resources must apply the search/SLA filter it is
given — to the rows AND to `item_number`.

Bug (QA-04, manual UI sweep 2026-08-11, build v0.1.371). The Protection
page's pager showed "2 / 1" and "No resources found" after searching. Half
of that is frontend (the page-count state is stale while the search request
is in flight — see the return notes), but the backend half is real and is
what "a denominator computed from an unfiltered total" literally means:
`list_resources` never declared `query` or `sla`, so FastAPI discarded both.
Measured live against prod tenant 163c487b-14b9-4105-bf06-3494cf11190d on
2026-08-11 through the in-cluster service:

    GET /api/v1/resources?tenantId=…&size=5                 item_number 29641
    GET /api/v1/resources?tenantId=…&size=5&query=aaron     item_number 29641
    GET /api/v1/resources?tenantId=…&size=5&query=zzzzznotathing
                                                            item_number 29641
    GET /api/v1/resources?tenantId=…&size=5&sla=none        item_number 29641

…and the returned rows were the unfiltered newest-first page ("zWayne
Scotch (Shared)", "Kelly Lloyd", …) for every one of them. The real match
count for "aaron" on that tenant is 28, which /by-type reports correctly.

This is the same defect ITO-9652 fixed on /by-type (2026-07-13). The fix
routes BOTH endpoints through `build_by_type_search_filters`, so they can
never answer the same filter differently again.
"""
from __future__ import annotations

import importlib.util
import inspect
import os
import pathlib
import sqlite3
import sys

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "services" / "resource-service" / "main.py"
_spec = importlib.util.spec_from_file_location(
    "rs_main_list_filter_total", _MAIN_PATH,
)
_mod = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["rs_main_list_filter_total"] = _mod
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(_mod)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import-env guard
    pytest.skip(f"resource-service/main.py failed to import: {exc}",
                allow_module_level=True)


# ── the params exist at all ───────────────────────────────────────────────

def test_list_resources_declares_query_and_sla():
    params = inspect.signature(_mod.list_resources).parameters
    assert "query" in params, (
        "GET /api/v1/resources dropped `query` again — FastAPI discards "
        "undeclared params silently and the endpoint answers with the whole "
        "tenant"
    )
    assert "sla" in params


# ── one filter definition, shared with /by-type ───────────────────────────

def test_list_resources_uses_the_by_type_filter_builder():
    src = inspect.getsource(_mod.list_resources)
    assert (
        "search_clause, search_params = build_by_type_search_filters("
        "query, status, sla)" in src
    ), "list_resources must build its filters with the same helper /by-type " \
       "uses, or the two endpoints can answer one filter differently"
    # The old hand-rolled status fragment must not come back: it skipped the
    # helper's validation and its ::text cast.
    assert 'AND r.status = :rstatus' not in src


def test_page_and_count_share_one_where_clause():
    """`item_number` is the pager's denominator. If it is built from a
    different WHERE than the page, Next offers a page that renders empty —
    which is the QA-04 symptom."""
    src = inspect.getsource(_mod.list_resources)
    body = src.split("build_by_type_search_filters", 1)[1]
    assert body.count("{search_clause}") == 2, (
        "expected {search_clause} in both the page query and the count "
        f"query; found {body.count('{search_clause}')}"
    )
    # …and the same for the type/hidden fragments.
    assert body.count("{type_clause} {hidden_clause}") == 2


def test_by_type_still_uses_the_same_builder():
    src = inspect.getsource(_mod.get_resources_by_type)
    assert (
        "search_clause, search_params = build_by_type_search_filters("
        "query, status, sla)" in src
    )


def test_both_endpoints_bind_the_search_params():
    for func in (_mod.list_resources, _mod.get_resources_by_type):
        src = inspect.getsource(func)
        assert "**search_params" in src, (
            f"{func.__name__} builds a filter clause but never binds its "
            "params — the query would raise at execution time"
        )


# ── behaviour: the clause the endpoint now carries really filters ─────────

_TENANT = "163c487b-14b9-4105-bf06-3494cf11190d"


def _fixture_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE resources (id TEXT, tenant_id TEXT, type TEXT, "
        "status TEXT, display_name TEXT, email TEXT, sla_policy_id TEXT, "
        "metadata TEXT)"
    )
    rows = [
        ("ENTRA_USER", "DISCOVERED", "Aaron Blake", "aaron.blake@x.com", None),
        ("ENTRA_USER", "DISCOVERED", "Sam Aaronson", "sam.a@x.com", None),
        ("ENTRA_USER", "DISCOVERED", "Kelly Lloyd", "kelly@x.com", "policy-1"),
        ("ENTRA_USER", "INACCESSIBLE", "Ross Smith", "ross@x.com", None),
        ("SHARED_MAILBOX", "DISCOVERED", "zWayne Scotch", "wayne@x.com", None),
    ]
    conn.executemany(
        "INSERT INTO resources VALUES (?,?,?,?,?,?,?,?)",
        [
            (f"r{i}", _TENANT, t, s, n, e, p, None)
            for i, (t, s, n, e, p) in enumerate(rows)
        ],
    )
    conn.commit()
    return conn


def _count_with(conn, clause: str, params: dict) -> int:
    """Run the endpoint's count shape with the produced clause.

    SQLite has no ILIKE and no ESCAPE-on-ILIKE; LIKE is case-insensitive for
    ASCII there, which is the behaviour ILIKE gives on Postgres, so the
    substitution preserves what is under test. `metadata->>'web_url'` is
    Postgres JSON syntax — rewritten to the column, which is NULL in the
    fixture (a no-op OR branch, exactly as it is for non-SharePoint types).
    """
    sql_clause = (
        clause.replace("ILIKE", "LIKE")
        .replace("ESCAPE '\\'", "")
        .replace("r.metadata->>'web_url'", "r.metadata")
        .replace("::text", "")
    )
    sql = (
        f"SELECT count(*) FROM resources r WHERE r.tenant_id = :rtenant "
        f"{sql_clause}"
    )
    return conn.execute(sql, {"rtenant": _TENANT, **params}).fetchone()[0]


def test_search_clause_narrows_the_count():
    conn = _fixture_db()
    clause, params = _mod.build_by_type_search_filters("aaron", None, None)
    assert _count_with(conn, clause, params) == 2
    # Pre-fix this endpoint answered 5 (every row) for any search term.
    assert _count_with(conn, "", {}) == 5


def test_search_with_no_match_counts_zero_not_everything():
    conn = _fixture_db()
    clause, params = _mod.build_by_type_search_filters(
        "zzzzznotathing", None, None,
    )
    assert _count_with(conn, clause, params) == 0


def test_sla_none_filter_reaches_the_count():
    conn = _fixture_db()
    clause, params = _mod.build_by_type_search_filters(None, None, "none")
    assert _count_with(conn, clause, params) == 4


def test_status_filter_is_validated_not_passed_through():
    from fastapi import HTTPException

    with pytest.raises(HTTPException) as exc:
        _mod.build_by_type_search_filters(None, "bogus-status", None)
    assert exc.value.status_code == 400
