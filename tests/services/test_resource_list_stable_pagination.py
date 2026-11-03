"""Regression: every LIMIT/OFFSET page carries a UNIQUE total order.

TMV-006 (QA sweep 2026-08-11, build v0.1.371). "Protection › All resources
page 1 already spans A→Z and page 2 restarts at Accounts Payable, so rows
are duplicated across pages and others are never reachable."

Cause: `GET /api/v1/resources` paginated with `ORDER BY r.created_at DESC`
and nothing else. `created_at` is nowhere near unique — discovery
bulk-commits its rows under one timestamp. Measured on prod tenant
163c487b-14b9-4105-bf06-3494cf11190d (2026-08-11): 39,782 resources hold
only 88 distinct created_at values and the largest single tie group is
32,360 rows. Postgres may return a tie group in any order per execution, so
LIMIT/OFFSET slices it differently every time.

Measured live through the in-cluster API BEFORE the fix (identical request
run twice, size=50):

    page 1   same_order=False  same ids 33/50
    page 2   same_order=False  same ids 23/50
    union of pages 1-4:  167 ids, then 179 ids   (200 expected)

/by-type had already been fixed this way (ITO-9652); the two now share one
constant so they cannot drift apart again.

Two layers here:
  * behavioural — the tie-group slicing itself, reproduced on SQLite;
  * structural — a sweep asserting no paged query in the four services
    under this sweep orders by a non-unique key alone. That is what stops
    the NEXT endpoint from shipping the same defect.
"""
from __future__ import annotations

import importlib.util
import inspect
import os
import pathlib
import re
import sqlite3
import sys

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


def _load(name: str, path: pathlib.Path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    sys.modules[name] = mod
    assert spec and spec.loader
    try:
        spec.loader.exec_module(mod)  # type: ignore[union-attr]
    except Exception as exc:  # pragma: no cover - import-env guard
        pytest.skip(f"{path.name} failed to import: {exc}",
                    allow_module_level=True)
    return mod


_RS = _load(
    "rs_main_stable_pagination",
    _REPO_ROOT / "services" / "resource-service" / "main.py",
)


# ── behaviour: a tie group sliced by LIMIT/OFFSET ────────────────────────

_TENANT = "163c487b-14b9-4105-bf06-3494cf11190d"


def _fixture_db() -> sqlite3.Connection:
    """120 rows sharing ONE created_at — the shape prod is actually in."""
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE resources (id TEXT PRIMARY KEY, tenant_id TEXT, "
        "created_at TEXT, display_name TEXT)"
    )
    conn.executemany(
        "INSERT INTO resources VALUES (?,?,?,?)",
        [
            (f"{i:04d}", _TENANT, "2026-06-16 23:33:17.362229", f"user-{i:04d}")
            for i in range(120)
        ],
    )
    conn.commit()
    return conn


def _walk(conn, order_by: str, size: int = 50) -> list:
    """Page the whole table with the given ORDER BY, return the id sequence.

    The scan direction is flipped between pages to stand in for Postgres's
    freedom to return an unordered tie group differently per execution.
    SQLite is deterministic on its own, so without this the unstable case
    could not be demonstrated at all.
    """
    seen = []
    for page, direction in enumerate(("", " DESC") * 3):
        rows = conn.execute(
            f"SELECT id FROM resources r "
            f"WHERE r.tenant_id = ? "
            f"{order_by or f'ORDER BY r.rowid{direction}'} "
            f"LIMIT ? OFFSET ?",
            (_TENANT, size, page * size),
        ).fetchall()
        if not rows:
            break
        seen.extend(r[0] for r in rows)
    return seen


def test_non_unique_order_loses_and_repeats_rows():
    """The pre-fix shape: ORDER BY created_at alone over a tie group."""
    conn = _fixture_db()
    walked = _walk(conn, "")
    assert len(set(walked)) < 120, (
        "fixture is not reproducing the unstable case — the whole point is "
        "that a non-unique sort key lets pages overlap"
    )


def test_unique_tiebreak_reaches_every_row_exactly_once():
    conn = _fixture_db()
    walked = _walk(conn, "ORDER BY r.created_at DESC, r.id DESC")
    assert len(walked) == 120
    assert len(set(walked)) == 120, "paging must visit each row exactly once"


# ── the endpoint actually uses a total order ─────────────────────────────


def test_list_resources_orders_by_a_unique_key():
    src = inspect.getsource(_RS.list_resources)
    assert "ORDER BY r.created_at DESC\n" not in src, (
        "GET /api/v1/resources is back on a non-unique sort key — page 2 "
        "will re-slice the 32,360-row created_at tie group"
    )
    assert "{RESOURCE_PAGE_ORDER_BY}" in src


def test_resources_and_by_type_share_one_order_constant():
    """Two copies of the same ORDER BY is how they drifted apart before.

    Asserted on the source, not on `is`: equal string literals are interned,
    so an identity check would pass even with the constant duplicated.
    """
    src = (
        _REPO_ROOT / "services" / "resource-service" / "main.py"
    ).read_text()
    assert "BY_TYPE_ORDER_BY = RESOURCE_PAGE_ORDER_BY" in src, (
        "/by-type must reuse the one ORDER BY constant, not carry its own "
        "copy of the string"
    )
    assert "r.id" in _RS.RESOURCE_PAGE_ORDER_BY
    assert _RS.BY_TYPE_ORDER_BY == _RS.RESOURCE_PAGE_ORDER_BY


def test_policy_list_orders_by_a_unique_key():
    src = inspect.getsource(_RS.list_policies)
    assert "SlaPolicy.id.desc()" in src, (
        "/api/v1/policies paginates with offset/limit; created_at alone is "
        "not unique when the wizard inserts several policies in one commit"
    )


# ── structural sweep: no paged query on a non-unique key ─────────────────

_SERVICES = (
    "resource-service", "dashboard-service", "snapshot-service", "job-service",
)

# Sort keys that are unique on their own, so they need no tiebreak.
_UNIQUE_KEY_RE = re.compile(
    r"\.id[.\s)]|\.id$|r\.id|si\.id|latest\.id|snap_alias\.id|Resource\.id|"
    r"Job\.id|JobLog\.id|SnapshotItem\.id|Snapshot\.id|SlaPolicy\.id"
)


_DISTINCT_ON_RE = re.compile(r"DISTINCT ON \(([^)]+)\)")


def _distinct_on_leads_the_order_by(window: str) -> bool:
    """`DISTINCT ON (X) … ORDER BY X, …` is already a total order.

    The output holds exactly one row per distinct X, so X alone orders it
    uniquely — no extra tiebreak needed. The chats page query is built this
    way and is correct; without this the sweep would flag it.
    """
    m = _DISTINCT_ON_RE.search(window)
    if not m:
        return False
    key = m.group(1).strip()
    return bool(re.search(r"ORDER BY \s*" + re.escape(key), window))


def _paged_order_by_blocks(path: pathlib.Path):
    """Yield (line_no, text) for each ORDER-BY-then-OFFSET/LIMIT window.

    Deliberately crude: it reads the source as text so it also catches the
    raw-SQL endpoints, which is where TMV-006 actually lived.
    """
    lines = path.read_text().splitlines()
    for i, line in enumerate(lines):
        if not re.search(r"\.offset\(|OFFSET :", line):
            continue
        window = "\n".join(lines[max(0, i - 26): i + 3])
        if not re.search(r"order_by\(|ORDER BY|_ORDER_BY", window):
            continue
        yield i + 1, window


def test_no_paged_query_orders_by_a_non_unique_key_alone():
    offenders = []
    for svc in _SERVICES:
        path = _REPO_ROOT / "services" / svc / "main.py"
        for line_no, window in _paged_order_by_blocks(path):
            if _UNIQUE_KEY_RE.search(window):
                continue
            if _distinct_on_leads_the_order_by(window):
                continue
            offenders.append(f"{svc}/main.py:{line_no}")
    assert not offenders, (
        "LIMIT/OFFSET pagination without a unique tiebreak — Postgres may "
        "return the tie group in a different order per request, so pages "
        "overlap and rows become unreachable (TMV-006): "
        + ", ".join(offenders)
    )


def test_snapshot_item_lists_have_an_order_by_at_all():
    """Both /items endpoints paginated with NO ORDER BY whatsoever."""
    snap = _REPO_ROOT / "services" / "snapshot-service" / "main.py"
    src = snap.read_text()
    assert "select(SnapshotItem).where(*filters).offset((page-1)*size)" not in src, (
        "a snapshot-items page query is back to LIMIT/OFFSET with no ORDER "
        "BY — the order is then whatever the plan happens to emit"
    )
