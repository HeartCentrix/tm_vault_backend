"""Regression: /api/v1/activity must report a total that describes the FILTER,
not the request, and the risk feed must not re-sort a page it already cut.

BUG-001 (2026-08-12, prod build v0.1.374). ``_list_activities_batch`` scanned
``fetch_limit = max(size, page * size)`` rows per source and then reported
``total = len(items)``. Because the scan window grew with the request, so did
the "total". Measured against prod with an identical (empty) filter set:

    size=10  -> total=25     size=50  -> total=93     size=200 -> total=280
    page=2   -> total=168    page=4   -> total=280

The Activity UI derives its page count from ``total``, so Next went dead at
page 2 of ~6 and roughly two thirds of the activity history was unreachable.
``has_more`` was no help either — it is computed from the same bad number.

The fix is a FIXED scan cap (``ACTIVITY_SCAN_CAP``), independent of page and
size, plus a ``total_capped`` flag for when the count is a floor. A single
COUNT(*) is not usable here: the feed is a union of three sources that are
de-duplicated and status-filtered in Python, so a SQL count would drift from
the list it describes.

BUG-009. ``get_high_risk_events`` paginated by ``occurred_at DESC`` in SQL and
then sorted the fetched page by ``risk_score`` in Python. The visible order
therefore disagreed with the order the pages were cut in — "highest risk" only
meant "highest risk among the 50 most recent" — and the DATE column's sort
indicator described an order the table was never in. Ordering now happens in
SQL, keyed by an explicit ``sort`` parameter.

These are source-level pins: the service module imports FastAPI + the full DB
stack, which is not available in the unit environment, so the guarantees are
asserted against the source text (the established convention in this repo —
see tests/shared/test_graph_get_params.py).
"""
from __future__ import annotations

import pathlib
import re

_SRC = (
    pathlib.Path(__file__).resolve().parents[2]
    / "services" / "audit-service" / "main.py"
).read_text()


def _block(anchor: str, size: int = 20000) -> str:
    i = _SRC.find(anchor)
    assert i != -1, f"anchor not found: {anchor}"
    return _SRC[i:i + size]


# ─────────────────────────── BUG-001 ────────────────────────────

def test_batch_scan_window_is_not_derived_from_page_or_size():
    """The defect verbatim: a scan window that grows with the request.

    Scoped to the batch path deliberately. The legacy ``group=job`` path also
    fetches ``max(size, page * size)`` rows, but it derives its total from
    real ``count_stmt`` COUNT queries (job_total / warning_total /
    discovery_total) — there the limit is only a merge window and the total is
    honest. Banning the expression globally would have "fixed" working code.
    """
    blk = _block("def _list_activities_batch(")
    # Only the prose reference to the old expression may remain.
    code = "\n".join(
        line for line in blk.splitlines()
        if not line.lstrip().startswith("#")
    )
    assert "max(size, page * size)" not in code, (
        "the batch scan window is derived from page/size again — this is "
        "exactly what made `total` report rows-scanned, not rows-matching"
    )


def test_activity_scan_cap_is_a_fixed_constant():
    m = re.search(r"^ACTIVITY_SCAN_CAP\s*=\s*int\(", _SRC, re.M)
    assert m, "ACTIVITY_SCAN_CAP must exist as a module-level constant"
    # It must not be computed from request state.
    line = _SRC[m.start():_SRC.index("\n", m.start())]
    assert "page" not in line and "size" not in line, (
        f"ACTIVITY_SCAN_CAP must not depend on the request: {line}"
    )


def test_batch_listing_uses_the_fixed_cap():
    blk = _block("def _list_activities_batch(")
    assert "fetch_limit = ACTIVITY_SCAN_CAP" in blk


def test_v2_merge_path_also_uses_the_fixed_cap():
    """The merge path in list_activities repeated the same mistake."""
    blk = _block("async def list_activities(")
    assert "limit=ACTIVITY_SCAN_CAP" in blk, (
        "the batch+v2 merge path still sizes its fetch from page*size"
    )


def test_total_capped_is_reported():
    """When total is a floor rather than a count, say so."""
    assert '"total_capped"' in _SRC, (
        "callers cannot distinguish an exact total from a truncated one"
    )
    blk = _block("def _list_activities_batch(")
    assert "capped = total >= ACTIVITY_SCAN_CAP" in blk


def test_has_more_still_derives_from_total():
    """has_more must remain (page*size) < total — correct once total is."""
    assert _SRC.count('"has_more": (page * size) < total') >= 2


# ─────────────────────────── BUG-009 ────────────────────────────

def test_risk_page_is_not_resorted_in_python():
    assert 'enriched_items.sort(key=lambda x: x.get("risk_score", 0), reverse=True)' not in _SRC, (
        "the fetched page is being re-sorted in Python again — that is what "
        "made the visible order disagree with the pagination (BUG-009)"
    )


def test_risk_endpoint_accepts_a_validated_sort_parameter():
    blk = _block("async def get_high_risk_events(")
    assert 'sort: str = Query("date", pattern="^(date|risk_score)$")' in blk
    assert 'order: str = Query("desc", pattern="^(asc|desc)$")' in blk


def test_risk_sort_is_applied_in_sql():
    blk = _block("async def get_high_risk_events(")
    assert "order_by(*order_by)" in blk, "ordering must reach the SQL query"
    assert "sort_expr = score_sql if sort == \"risk_score\" else AuditEvent.occurred_at" in blk


def test_risk_response_echoes_the_sort_state():
    """The UI must be able to drive its indicator from the real sort key."""
    blk = _block("async def get_high_risk_events(")
    assert '"sort": sort' in blk and '"order": order' in blk


# ──────────────────── injection found alongside ─────────────────

def test_risk_level_filter_is_bound_not_interpolated():
    assert "details->'risk_signals'->>'risk_level' = '{risk_level}'" not in _SRC, (
        "risk_level is being f-string interpolated into SQL again — it comes "
        "straight from the query string"
    )
    blk = _block("async def get_high_risk_events(")
    assert "bindparams(risk_level=risk_level)" in blk


def test_no_fstring_interpolation_into_text_clauses_in_this_endpoint():
    blk = _block("async def get_high_risk_events(")
    offenders = re.findall(r'text\(\s*f["\']', blk)
    assert offenders == [], f"f-string SQL in the risk endpoint: {offenders}"


def test_v2_merge_paginates_only_after_merging_all_candidates(monkeypatch):
    import asyncio
    import importlib.util
    import sys

    path = pathlib.Path(__file__).resolve().parents[2] / "services" / "audit-service" / "main.py"
    spec = importlib.util.spec_from_file_location("audit_activity_pagination_test", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    rows = [
        {"id": str(i), "operation": "RESTORE", "start_time": f"2026-10-{20-i:02d}"}
        for i in range(5)
    ]

    async def legacy(**kwargs):
        start = (kwargs["page"] - 1) * kwargs["size"]
        return {"items": rows[start:start + kwargs["size"]], "total": len(rows), "total_capped": False}

    async def v2(**kwargs):
        return []

    monkeypatch.setattr(module.settings, "BATCH_ROW_REDESIGN_ENABLED", True)
    monkeypatch.setattr(module, "_list_activities_batch", legacy)
    monkeypatch.setattr(module, "_list_activities_batch_v2", v2)
    seen = []
    for page in (1, 2, 3):
        result = asyncio.run(module.list_activities(
            tenantId=None, serviceType=None, start_date=None, end_date=None,
            operation=None, status=None, page=page, size=2, group="batch",
        ))
        assert result["total"] == 5
        seen.extend(row["id"] for row in result["items"])
    assert seen == [str(i) for i in range(5)]
