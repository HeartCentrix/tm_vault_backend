"""Regression: the failed-snapshot retry sweep must not accept a
``skipped_concurrent`` placeholder as the "newer success" that heals a
FAILED snapshot.

Bug (found 2026-08-04): ``retry_failed_snapshots`` in
services/backup-scheduler/main.py looked for a newer snapshot of the same
resource with ``status == COMPLETED`` and, on finding one, wrote
``extra_data.retry_attempted = True`` plus
``retry_skipped_reason = "newer_success_exists"`` and skipped the retry.

backup-worker's cross-job concurrency guard creates a snapshot row and
immediately marks it COMPLETED with item_count=0 and
``extra_data.skipped_concurrent = true`` (~19 ms end to end) purely so the
skipped job's FANOUT/FINALIZE counters balance — see SKIP_CONCURRENT in
workers/backup-worker/main.py. It captures NOTHING. Six exist in
production.

So a placeholder landing moments after a genuine failure satisfied the
"newer COMPLETED" test, and because the sweep stamps ``retry_attempted``
on the way out, the very next pass short-circuits on that marker: the
FAILED backup's ONE automatic retry was cancelled permanently, no data was
captured, and nothing alerted. The resource simply stopped being retried.

Pins:
  * the healing-snapshot query the sweep actually builds carries the
    placeholder exclusion, compiled as real Postgres SQL;
  * the exclusion uses the *json* accessor (``snapshots.extra_data`` is a
    json column — ``jsonb_extract_path_text`` would raise at runtime) and
    is COALESCE-wrapped so rows predating the flag (extra_data NULL, or
    key absent) still count as real successes;
  * rejection is a negative match, not an equality test, for the same
    reason;
  * the healing snapshot still has to be COMPLETED — a PARTIAL run captured
    only some items and does not prove the failure healed.
"""
from __future__ import annotations

import asyncio
import importlib.util
import inspect
import os
import pathlib
import sys
import uuid
from datetime import datetime, timedelta

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "services" / "backup-scheduler" / "main.py"
_spec = importlib.util.spec_from_file_location("bs_main_retry_healing", _MAIN_PATH)
_bs = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bs_main_retry_healing"] = _bs
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(_bs)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import-env guard
    pytest.skip(
        f"backup-scheduler module failed to import: {exc}",
        allow_module_level=True,
    )


# ── premise guard ────────────────────────────────────────────────────────

def test_extra_data_is_plain_json_not_jsonb():
    """If extra_data were ever JSONB the jsonb_* accessors would be legal;
    while it is json, only json_extract_path_text works at runtime."""
    from sqlalchemy import JSON
    from sqlalchemy.dialects.postgresql import JSONB
    from shared.models import Snapshot

    col_type = Snapshot.__table__.c.extra_data.type
    assert isinstance(col_type, JSON)
    assert not isinstance(col_type, JSONB)


# ── the predicate itself ─────────────────────────────────────────────────

def _compiled_predicate() -> str:
    from sqlalchemy.dialects import postgresql

    return str(
        _bs._not_skipped_concurrent().compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )


def test_predicate_rejects_placeholders_with_the_json_accessor():
    sql = _compiled_predicate()
    assert "skipped_concurrent" in sql, (
        "a 0-item ~19ms concurrency placeholder must never count as the "
        "success that heals a FAILED snapshot"
    )
    assert "json_extract_path_text" in sql
    assert "jsonb_extract_path_text" not in sql, (
        "extra_data is a json column — the jsonb accessor raises at runtime"
    )


def test_predicate_is_null_safe_and_negative():
    sql = _compiled_predicate()
    assert "coalesce" in sql.lower(), (
        "without COALESCE, NOT IN over a NULL extra_data yields NULL and "
        "would filter out every real snapshot — cancelling nothing and "
        "retrying everything forever"
    )
    assert "'false'" in sql, "absent/NULL flag must default to 'not skipped'"
    assert "NOT IN" in sql.upper(), (
        "rejection must be a negative match so real snapshots (flag "
        "absent) still qualify as healing successes"
    )


# ── behavioral: the query the sweep actually builds ──────────────────────

class _FakeResult:
    """Minimal stand-in for a SQLAlchemy Result."""

    def __init__(self, rows):
        self._rows = rows

    def scalars(self):
        return self

    def all(self):
        return self._rows

    def first(self):
        return self._rows[0] if self._rows else None


class _RecordingSession:
    """Records every statement the sweep executes.

    First execute() answers the FAILED-snapshot scan; every later one
    answers "no healing snapshot found" so the sweep walks the whole
    decision path. session.get() returns None so the sweep hits its
    ``if not resource: continue`` exit before touching the message bus.
    """

    def __init__(self, failed_rows):
        self.statements = []
        self._failed_rows = failed_rows
        self._calls = 0

    async def execute(self, stmt, *a, **kw):
        self.statements.append(stmt)
        self._calls += 1
        if self._calls == 1:
            return _FakeResult(self._failed_rows)
        return _FakeResult([])

    async def get(self, model, ident):
        return None

    async def merge(self, obj):
        return obj

    async def commit(self):
        return None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _FakeFailedSnapshot:
    def __init__(self):
        self.id = uuid.uuid4()
        self.resource_id = uuid.uuid4()
        self.completed_at = datetime.utcnow() - timedelta(hours=6)
        self.extra_data = None


def test_healing_query_excludes_placeholders(monkeypatch):
    """Compile the *actual* healing-snapshot query the sweep builds and
    assert the placeholder exclusion is in its WHERE clause. Source
    inspection alone would not catch the predicate being built but never
    applied."""
    from sqlalchemy.dialects import postgresql

    session = _RecordingSession([_FakeFailedSnapshot()])
    monkeypatch.setattr(_bs, "async_session_factory", lambda: session)

    asyncio.run(_bs.retry_failed_snapshots())

    assert len(session.statements) >= 2, (
        "sweep should have run the FAILED scan and then the healing-snapshot "
        f"lookup; saw {len(session.statements)} statements"
    )
    compiled = session.statements[1].compile(dialect=postgresql.dialect())
    sql = str(compiled)
    param_values = {str(v) for v in compiled.params.values()}

    assert "json_extract_path_text" in sql, (
        "the healing-snapshot lookup does not consult extra_data at all — a "
        "skipped_concurrent placeholder would cancel the retry"
    )
    assert "skipped_concurrent" in param_values, (
        "the healing-snapshot lookup must exclude the concurrency placeholder"
    )
    assert "coalesce" in sql.lower()
    assert "NOT IN" in sql.upper()
    # The healing snapshot must still be a COMPLETED one. The status bind
    # renders as the enum member, e.g. "SnapshotStatus.COMPLETED".
    assert any(v.endswith("COMPLETED") for v in param_values), (
        f"healing lookup lost its COMPLETED filter; binds were {param_values}"
    )


def test_healing_query_does_not_accept_partial(monkeypatch):
    """PARTIAL captured only some items, so it does not prove the failure
    healed. Erring toward one extra retry is cheap; erring toward a
    cancelled retry loses data. Guards against a well-meaning widening of
    the healing test that would reintroduce silent retry cancellation."""
    src = inspect.getsource(_bs.retry_failed_snapshots)
    _, _, healing = src.partition("newer_stmt")
    healing = healing.split("limit(1)")[0]
    assert "COMPLETED" in healing
    assert "PARTIAL" not in healing, (
        "if PARTIAL is ever allowed to heal a FAILED snapshot, justify it "
        "in the comment and update this test deliberately"
    )


def test_skip_path_is_terminal_so_a_false_positive_is_permanent():
    """Why this bug had no second chance: the skip path stamps
    retry_attempted, and the top of the loop short-circuits on that marker.
    One bad "healed" verdict cancels the retry forever."""
    src = inspect.getsource(_bs.retry_failed_snapshots)
    assert "newer_success_exists" in src
    assert 'retry_attempted' in src
    marker_gate, _, rest = src.partition("newer_stmt")
    assert 'get("retry_attempted")' in marker_gate, (
        "the marker gate must precede the healing lookup — that ordering is "
        "what makes a false 'healed' verdict permanent"
    )
