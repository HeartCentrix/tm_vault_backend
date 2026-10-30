"""Regression: the Overview "Protection Status" cards and the Protection
tab lists must count ONE universe.

Bug (QA-05, manual UI sweep 2026-08-11, build v0.1.371). The card totals
and the tab totals were two independent queries and had drifted on every
bucket. Measured live against prod tenant
163c487b-14b9-4105-bf06-3494cf11190d on 2026-08-11 — card vs
/api/v1/resources/by-type `item_number`:

    Users              6,680 vs 13,812
    Shared mailboxes     120 vs  1,293
    Rooms                132 vs    154
    SharePoint sites     752 vs  4,239
    Groups & Teams    10,109 vs 10,142

Two causes, both on the dashboard side:

  1. dashboard-service filtered ``status IN (DISCOVERED, ACTIVE)``;
     /by-type applies no status filter. 8,373 INACCESSIBLE rows were
     listed by the tab and missing from the card. It cost the NUMERATOR
     too: 2 of that tenant's 13 SLA-assigned resources are INACCESSIBLE
     (1 ENTRA_USER, 1 SHARED_MAILBOX) and were dropped from
     ``protectedCount``.
  2. dashboard-service re-counted SHAREPOINT_SITE with a
     group-name-collision exclusion that /by-type deleted on 2026-07-15
     (it now only annotates those sites with ``group_connected``).

The fix expresses the scope as data — ``PROTECTION_SCOPE_EXCLUDED_STATUSES``
/ ``PROTECTION_SCOPE_EXCLUDED_TYPES`` — in both services. They are
duplicated rather than imported because the services ship as separate
images with no shared service-level module; the right long-term home is
``shared/models.py`` next to ``UI_HIDDEN_TYPES``, and this file is the
guard until that move happens.

The scope predicate is executed verbatim against in-memory SQLite over a
fixture that contains INACCESSIBLE rows and a group-name-colliding
SharePoint site, so this is a behaviour test and not only a source pin.
``::text`` is a Postgres cast SQLite doesn't parse; it is stripped for
execution (it is a no-op for the NOT IN comparison being tested).
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


def _load(mod_name: str, rel_path: str):
    path = _REPO_ROOT / rel_path
    spec = importlib.util.spec_from_file_location(mod_name, path)
    mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    sys.modules[mod_name] = mod
    assert spec and spec.loader
    try:
        spec.loader.exec_module(mod)  # type: ignore[union-attr]
    except Exception as exc:  # pragma: no cover - import-env guard
        pytest.skip(f"{rel_path} failed to import: {exc}", allow_module_level=True)
    return mod


_rs = _load("rs_main_protection_scope", "services/resource-service/main.py")
_ds = _load("ds_main_protection_scope", "services/dashboard-service/main.py")

_DS_SRC = (
    _REPO_ROOT / "services" / "dashboard-service" / "main.py"
).read_text()


# ── the two services hold the same definition ─────────────────────────────

def test_scope_constants_match_across_services():
    assert (
        _rs.PROTECTION_SCOPE_EXCLUDED_STATUSES
        == _ds.PROTECTION_SCOPE_EXCLUDED_STATUSES
    ), "resource-service and dashboard-service disagree on which statuses " \
       "the Protection surfaces count — that is exactly the QA-05 drift"
    assert (
        _rs.PROTECTION_SCOPE_EXCLUDED_TYPES
        == _ds.PROTECTION_SCOPE_EXCLUDED_TYPES
    ), "resource-service and dashboard-service disagree on which types the " \
       "Protection surfaces count"


def test_scope_excludes_no_status():
    """Pins the decision, not just the agreement.

    A terminated user whose mailbox we still hold is INACCESSIBLE but must
    stay listed AND counted — otherwise there is no UI path to recover it
    (shared/tier2_discovery.py: "readers (UI, audit) want the full list").
    Re-introducing a status filter on either side has to fail here first.
    """
    assert _rs.PROTECTION_SCOPE_EXCLUDED_STATUSES == frozenset()
    assert _ds.PROTECTION_SCOPE_EXCLUDED_STATUSES == frozenset()


def test_scope_hides_exactly_the_ui_hidden_types():
    from shared.models import UI_HIDDEN_TYPES

    assert _rs.PROTECTION_SCOPE_EXCLUDED_TYPES == frozenset(UI_HIDDEN_TYPES)


# ── the endpoint actually uses it ─────────────────────────────────────────

def _protection_status_src() -> str:
    start = _DS_SRC.find("async def get_protection_status")
    assert start != -1, "get_protection_status was renamed — retarget this test"
    end = _DS_SRC.find("\n# Newest-version content-size rollup", start)
    assert end != -1
    return _DS_SRC[start:end]


def test_protection_status_applies_no_hardcoded_status_filter():
    src = _protection_status_src()
    assert "ResourceStatus.DISCOVERED" not in src and "ResourceStatus.ACTIVE" not in src, (
        "get_protection_status re-hardcoded a status filter; it must go "
        "through PROTECTION_SCOPE_EXCLUDED_STATUSES so /by-type stays in sync"
    )
    assert "PROTECTION_SCOPE_EXCLUDED_STATUSES" in src
    assert "PROTECTION_SCOPE_EXCLUDED_TYPES" in src


def _strip_comments(src: str) -> str:
    return "\n".join(
        line for line in src.splitlines() if not line.lstrip().startswith("#")
    )


def test_protection_status_has_no_second_sharepoint_query():
    """The generic GROUP BY already counts SHAREPOINT_SITE the way the tab
    does. A bespoke re-count is a second source of truth by construction —
    and the one that shipped was pinned to a /by-type rule deleted on
    2026-07-15, costing the denominator 3,487 protectable sites."""
    code = _strip_comments(_protection_status_src())
    assert "SHAREPOINT_SITE" not in code, (
        "a SharePoint-specific query is back in get_protection_status"
    )
    assert "sp_total_row" not in code and "NOT EXISTS" not in code


def test_by_type_count_and_page_share_the_scope_clause():
    """Both the page query and its count must carry the scope, or
    `item_number` describes a different universe than `items`."""
    src = inspect.getsource(_rs.get_resources_by_type)
    assert "scope_clause = protection_scope_clause(" in src
    body_after = src.split("scope_clause = protection_scope_clause(", 1)[1]
    assert body_after.count("{scope_clause}") == 2, (
        "expected the scope clause in both the page query and the count "
        f"query; found {body_after.count('{scope_clause}')}"
    )


# ── behaviour: the predicate keeps INACCESSIBLE rows ──────────────────────

_TENANT = "163c487b-14b9-4105-bf06-3494cf11190d"


def _fixture_db() -> sqlite3.Connection:
    """Miniature of the prod shape: live rows, INACCESSIBLE rows, a
    group-name-colliding SharePoint site, and a hidden-type row."""
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE resources (id TEXT, tenant_id TEXT, type TEXT, "
        "status TEXT, display_name TEXT, email TEXT, sla_policy_id TEXT)"
    )
    rows = [
        # (type, status, name, sla)
        ("ENTRA_USER", "DISCOVERED", "Live User", None),
        ("ENTRA_USER", "ACTIVE", "Protected User", "policy-1"),
        # terminated user we still hold a backup for, WITH a policy: the
        # old card dropped it from both numerator and denominator.
        ("ENTRA_USER", "INACCESSIBLE", "Terminated User", "policy-1"),
        ("SHARED_MAILBOX", "INACCESSIBLE", "Old Shared", "policy-1"),
        ("ROOM_MAILBOX", "DISCOVERED", "Board Room", None),
        # group-connected site: /by-type lists it, the old card hid it.
        ("SHAREPOINT_SITE", "DISCOVERED", "IT OPS NOTES", None),
        ("SHAREPOINT_SITE", "DISCOVERED", "Plain Site", None),
        ("M365_GROUP", "DISCOVERED", "IT OPS NOTES", None),
        # hidden type — must be counted by NEITHER surface.
        ("TEAMS_CHANNEL", "ARCHIVED", "General", "policy-1"),
    ]
    conn.executemany(
        "INSERT INTO resources VALUES (?,?,?,?,?,?,?)",
        [(f"r{i}", _TENANT, t, s, n, None, p) for i, (t, s, n, p) in enumerate(rows)],
    )
    conn.commit()
    return conn


def _sqlite(sql: str) -> str:
    """Drop the Postgres ``::text`` casts SQLite can't parse. They are
    no-ops for the NOT IN string comparison under test."""
    return sql.replace("::text", "")


def _count(conn, where: str) -> int:
    return conn.execute(
        f"SELECT count(*) FROM resources r WHERE r.tenant_id = '{_TENANT}' {where}"
    ).fetchone()[0]


def test_scope_counts_inaccessible_rows():
    conn = _fixture_db()
    clause = _sqlite(_rs.protection_scope_clause("r"))
    # 3 ENTRA_USER rows, one of them INACCESSIBLE.
    assert _count(conn, clause + " AND r.type = 'ENTRA_USER'") == 3
    assert _count(conn, clause + " AND r.type = 'SHARED_MAILBOX'") == 1


def test_scope_counts_group_connected_sharepoint_sites():
    conn = _fixture_db()
    clause = _sqlite(_rs.protection_scope_clause("r"))
    # "IT OPS NOTES" collides with an M365_GROUP of the same name. The tab
    # lists it (group backups don't capture site content), so the card has
    # to count it: 2, not 1.
    assert _count(conn, clause + " AND r.type = 'SHAREPOINT_SITE'") == 2


def test_scope_drops_hidden_types():
    conn = _fixture_db()
    clause = _sqlite(_rs.protection_scope_clause("r"))
    assert _count(conn, clause + " AND r.type = 'TEAMS_CHANNEL'") == 0
    # …but the opt-in path still sees them (Recovery needs the rows).
    open_clause = _sqlite(
        _rs.protection_scope_clause("r", include_hidden_types=True)
    )
    assert _count(conn, open_clause + " AND r.type = 'TEAMS_CHANNEL'") == 1


def test_card_and_tab_totals_agree_bucket_by_bucket():
    """The end-to-end invariant, computed from each module's OWN definition.

    Tab total  = resource-service's by-type count for the bucket's types.
    Card total = dashboard-service's PROTECTION_BUCKETS summed over the
                 same scope. Pre-fix these differ on users, shared
                 mailboxes and SharePoint.
    """
    conn = _fixture_db()
    tab_clause = _sqlite(
        _rs.protection_scope_clause("r", include_hidden_types=True)
    )
    card_clause = _sqlite(_rs.protection_scope_clause("r"))

    for bucket, types in _ds.PROTECTION_BUCKETS.items():
        type_names = sorted(
            t.value if hasattr(t, "value") else str(t) for t in types
        )
        if not type_names:
            continue
        in_list = ", ".join(f"'{t}'" for t in type_names)
        # The tab returns an empty page for hidden types, so mirror that.
        visible = [
            t for t in type_names
            if t not in _rs.PROTECTION_SCOPE_EXCLUDED_TYPES
        ]
        tab_total = sum(
            _count(conn, f"{tab_clause} AND r.type = '{t}'") for t in visible
        )
        card_total = _count(conn, f"{card_clause} AND r.type IN ({in_list})")
        assert card_total == tab_total, (
            f"bucket {bucket!r}: card counts {card_total}, tab counts "
            f"{tab_total}"
        )


def test_scope_clause_is_bind_free_and_injection_safe():
    """The fragment is interpolated into f-string SQL, so it must contain no
    caller-supplied text and no bind placeholders."""
    clause = _rs.protection_scope_clause("r")
    # `::text` is a cast, not a bind — a bind is `:name`.
    assert not re.search(r"(?<!:):[A-Za-z_]", clause), (
        f"scope clause grew a bind parameter: {clause!r}"
    )
    assert not re.search(r"[^A-Za-z0-9_', ()\n.:]", clause.replace("\n", " "))
    for name in _rs.PROTECTION_SCOPE_EXCLUDED_TYPES:
        assert re.fullmatch(r"[A-Z0-9_]+", name), (
            f"{name!r} is not an enum-shaped literal — it must not be inlined"
        )
