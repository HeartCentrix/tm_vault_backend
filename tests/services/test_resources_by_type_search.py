"""Regression: /api/v1/resources/by-type must honor the `query` and
`status` params the Protection page sends, and paginate with a stable
sort order.

Bug (ITO-9652 follow-up, 2026-07-13): the handler accepted only
type/tenantId/page/size, so FastAPI silently discarded `query=jrue` and
`status=ACTIVE` — every search returned the whole tenant. The frontend's
fetch-10000-and-filter-client-side workaround then broke at 11,190
ENTRA_USERs: ORDER BY created_at DESC with 10,841 rows sharing one
bulk-discovery timestamp (and no tiebreaker) sliced the tie group
nondeterministically, so accounts like JRue@taylormorrison.com randomly
vanished from the Protection page.
"""
import importlib.util
import os
import pathlib
import sys

import pytest
from fastapi import HTTPException

# shared.config validates JWT_SECRET at import time; these tests never
# touch auth, so run with the sanctioned ephemeral-test escape hatch.
os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "services" / "resource-service" / "main.py"
_spec = importlib.util.spec_from_file_location(
    "resource_main_under_test", _MAIN_PATH,
)
_mod = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
assert _spec and _spec.loader
_spec.loader.exec_module(_mod)  # type: ignore[union-attr]


# ─── search / status filter builder ────────────────────────────────────────

def test_query_builds_ilike_on_display_name_and_email():
    clause, params = _mod.build_by_type_search_filters("jrue", None)
    assert "r.display_name ILIKE :rquery" in clause
    assert "r.email ILIKE :rquery" in clause
    assert params["rquery"] == "%jrue%"


def test_query_escapes_ilike_wildcards():
    clause, params = _mod.build_by_type_search_filters("50%_off\\x", None)
    assert params["rquery"] == "%50\\%\\_off\\\\x%"
    assert "ESCAPE" in clause


def test_blank_query_and_status_produce_no_filters():
    clause, params = _mod.build_by_type_search_filters("   ", None)
    assert clause == ""
    assert params == {}


def test_status_filter_casts_enum_to_text():
    clause, params = _mod.build_by_type_search_filters(None, "active")
    assert "r.status::text = :rstatus" in clause
    assert params["rstatus"] == "ACTIVE"


def test_invalid_status_raises_400():
    with pytest.raises(HTTPException) as exc:
        _mod.build_by_type_search_filters(None, "BOGUS")
    assert exc.value.status_code == 400


# ─── stable pagination order ───────────────────────────────────────────────

def test_by_type_order_has_id_tiebreaker():
    # 10,841 resources shared one bulk-discovery created_at; without a
    # unique secondary key LIMIT/OFFSET slices the tie group randomly.
    assert "r.created_at DESC" in _mod.BY_TYPE_ORDER_BY
    assert "r.id" in _mod.BY_TYPE_ORDER_BY


# ─── SharePoint group-connected sites (2026-07-15, /sites/ITOPS) ───────────

def test_query_also_matches_web_url():
    # Site titles often differ from the URL slug ("IT OPS NOTES" lives at
    # /sites/ITOPS); users search the slug. web_url is NULL for types
    # without one, so the OR is a no-op elsewhere.
    clause, _ = _mod.build_by_type_search_filters("ITOPS", None)
    assert "r.metadata->>'web_url' ILIKE :rquery" in clause


def test_by_type_lists_group_connected_sites_with_flag():
    """Group-connected SP sites must be LISTED (with a group_connected
    flag), not excluded. Group backups capture profile+members only — not
    site content — so the old NOT EXISTS dedup left 3,465 of 4,204 pilot
    sites with no UI path to an SLA."""
    src = _MAIN_PATH.read_text()
    fn = src[src.index("def get_resources_by_type"):]
    fn = fn[:fn.index("\n@app.")] if "\n@app." in fn else fn
    assert "AS group_connected" in fn, (
        "by-type lost the group_connected annotation column"
    )
    assert "NOT EXISTS" not in fn, (
        "group-connected SP sites are being excluded from /by-type again"
    )
    assert '"group_connected": bool(' in fn, (
        "group_connected flag missing from the by-type item payload"
    )


# ─── server-side SLA filter (2026-07-15, Gold filter showed nothing) ───────

def test_sla_none_filters_unprotected():
    clause, params = _mod.build_by_type_search_filters(None, None, "none")
    assert "r.sla_policy_id IS NULL" in clause
    assert "rsla" not in params


def test_sla_uuid_filters_by_policy():
    pid = "7c9e6679-7425-40de-944b-e07fc1f90ae7"
    clause, params = _mod.build_by_type_search_filters(None, None, pid)
    assert "r.sla_policy_id = CAST(:rsla AS uuid)" in clause
    assert params["rsla"] == pid


def test_sla_garbage_raises_400():
    # The SLA filter used to be client-side over the fetched page only;
    # after the 4,204-site SharePoint discovery, newest-first pagination
    # pushed every SLA'd resource out of the window and "Gold" matched
    # nothing. Server-side filtering is the fix — and bad input must be
    # loud, not silently ignored.
    with pytest.raises(HTTPException) as exc:
        _mod.build_by_type_search_filters(None, None, "Gold")
    assert exc.value.status_code == 400
