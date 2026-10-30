"""Regression: DR replication must copy every REAL recovery point —
COMPLETED *or* PARTIAL — and must never copy a concurrency placeholder.

Bug (found 2026-08-04): ``scan_and_replicate`` in
services/dr-replication-worker/main.py selected candidates with a raw
``text("status = 'COMPLETED'")`` fragment. Three consequences:

1. Every PARTIAL snapshot was silently ineligible for DR replication. A
   PARTIAL snapshot lost some shards but still holds every item it *did*
   capture, so it is a real recovery point — in production the only backup
   of Jason Rue's 1,114,848-item / 152 GB Exchange Online Archive is
   PARTIAL, and it therefore had NO cross-region copy at all. The
   lossy-shard derivation deployed the same day makes PARTIAL the normal
   outcome for any SharePoint/OneDrive run with files_failed > 0, so the
   unprotected population was about to grow sharply.
2. The scan is bounded by a look-back window (was a hardcoded
   ``created_at > NOW() - INTERVAL '2 days'``), and nothing else ever
   revisits a snapshot. So the omission was PERMANENT, not merely delayed:
   once a PARTIAL aged past the window it could never become a candidate
   again, even after someone noticed. The fix keeps the window bounded by
   default (deploying it must not unleash a copy storm over the whole
   history) but makes it operator-tunable via DR_SCAN_LOOKBACK_DAYS and
   logs the out-of-window backlog so the hole is a decision, not a
   silence.
3. backup-worker's cross-job concurrency guard (SKIP_CONCURRENT) creates
   0-item snapshots that are COMPLETED with
   ``extra_data.skipped_concurrent = true``, finished in ~19 ms. Those
   matched the old filter exactly, so DR work was spent on fake successes
   holding no data — and because a placeholder has no snapshot_items it
   fell through ``replicate_snapshot``'s "metadata-only" branch and was
   marked ``replicated`` with a dr_blob_path, registering an empty DR copy
   that reads as a genuine recovery point.

Pins:
  * the candidate predicate admits COMPLETED and PARTIAL;
  * it rejects skipped_concurrent placeholders as a *negative* match;
  * it reads extra_data with the **json** accessor (the column is json,
    not jsonb — jsonb_extract_path_text would raise at runtime) and
    COALESCEs, since rows predating the flag hold NULL and a bare NOT IN
    over NULL yields NULL, filtering out every real snapshot;
  * it still excludes non-recovery-point states;
  * the scan routes through the shared predicate and no bare
    ``status = 'COMPLETED'`` fragment survives;
  * the look-back window stays bounded by default (no storm on deploy)
    and the out-of-window backlog is reported;
  * ``replicate_snapshot`` refuses a placeholder without registering a DR
    copy — behavioural, not a grep.
"""
from __future__ import annotations

import asyncio
import importlib.util
import inspect
import os
import pathlib
import sys
import types

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "services" / "dr-replication-worker" / "main.py"
_spec = importlib.util.spec_from_file_location("drw_main_partial_dr", _MAIN_PATH)
_drw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["drw_main_partial_dr"] = _drw
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(_drw)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import-env guard
    pytest.skip(
        f"dr-replication-worker module failed to import: {exc}",
        allow_module_level=True,
    )


def _run(coro):
    """Run on a live loop, installing one if the suite left none behind.

    Matches the helper in test_audit_consumer_propagates_failure.py: other
    modules here call asyncio.run(), which uninstalls the current event
    loop, so get_event_loop() raises depending on test order. Leave the loop
    installed (not closed) so later modules find a usable one too.
    """
    try:
        loop = asyncio.get_event_loop_policy().get_event_loop()
        if loop.is_closed():
            raise RuntimeError("closed")
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)


def _compiled_predicate() -> str:
    """Render _dr_replicable_snapshot() as literal Postgres SQL."""
    from sqlalchemy.dialects import postgresql

    return str(
        _drw._dr_replicable_snapshot().compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )


def test_predicate_admits_both_completed_and_partial():
    sql = _compiled_predicate()
    assert "COMPLETED" in sql, "a clean run must still get a DR copy"
    assert "PARTIAL" in sql, (
        "PARTIAL snapshots hold real captured items — the only copy of the "
        "1.1M-item / 152 GB archive is PARTIAL. Excluding it leaves a real "
        "recovery point with no cross-region copy at all."
    )


def test_predicate_rejects_skipped_concurrent_placeholders():
    sql = _compiled_predicate()
    assert "skipped_concurrent" in sql, (
        "0-item ~19ms concurrency placeholders must not consume DR work or "
        "register an empty DR copy"
    )
    # Rejection must be a NOT-match so absent/false keeps the row; an
    # equality test would drop every real snapshot instead.
    assert "NOT IN" in sql.upper(), (
        "placeholder rejection must be a negative match so real snapshots "
        "(flag absent) still qualify for replication"
    )


def test_predicate_uses_json_accessor_and_is_null_safe():
    sql = _compiled_predicate()
    assert "json_extract_path_text" in sql, (
        "extra_data is a json column; the json accessor is required"
    )
    assert "jsonb_extract_path_text" not in sql, (
        "jsonb_extract_path_text on a json column raises at runtime — "
        "Snapshot.extra_data is Column(JSON), not JSONB"
    )
    assert "coalesce" in sql.lower(), (
        "rows written before the flag existed hold NULL here; without "
        "COALESCE the NOT IN yields NULL and the row is filtered out, which "
        "would stop DR replication for every pre-flag snapshot"
    )


def test_predicate_excludes_non_recovery_point_states():
    """An in-flight run has no complete item set, a FAILED run's leftovers
    are not restorable, and PENDING_DELETION rows are mid-teardown — their
    blobs are being deleted right now, so copying them to DR is pointless."""
    sql = _compiled_predicate()
    for state in ("IN_PROGRESS", "FAILED", "PENDING_DELETION"):
        assert state not in sql, f"{state} is not a recovery point"


def test_scan_routes_through_the_predicate_and_drops_the_raw_filter():
    """The candidate query and the backlog counter must both use the shared
    predicate; a surviving raw COMPLETED fragment reintroduces the bug."""
    src = inspect.getsource(_drw.scan_and_replicate)
    assert src.count("_dr_replicable_snapshot()") >= 2, (
        "expected the candidate query + the backlog count to use "
        f"_dr_replicable_snapshot(); found {src.count('_dr_replicable_snapshot()')}"
    )
    assert "status = 'COMPLETED'" not in src, (
        "the raw COMPLETED-only SQL fragment is back — it excludes every "
        "PARTIAL recovery point from DR and admits 0-item placeholders"
    )


def test_lookback_window_is_bounded_by_default_and_backlog_is_reported():
    """Deploying the widened status filter must not, by itself, make a large
    history of never-replicated PARTIAL snapshots eligible in one pass. The
    window therefore stays bounded by default; the out-of-window backlog is
    logged so the permanent gap is visible and draining it is deliberate."""
    assert _drw._DR_SCAN_LOOKBACK_DAYS == 2, (
        "default look-back must stay at the pre-fix 2 days so the fix cannot "
        "unleash a re-replication storm on deploy"
    )
    src = inspect.getsource(_drw.scan_and_replicate)
    assert "BACKLOG" in src, (
        "snapshots aged out of the window are never reconsidered — the count "
        "must be surfaced, not silent"
    )
    assert "DR_SCAN_LOOKBACK_DAYS" in inspect.getsource(_drw).split("def scan_and_replicate")[0], (
        "the window must be operator-tunable so the backlog can be drained "
        "without a code change"
    )


class _FakeSession:
    """Any use at all is a failure: the placeholder guard must return before
    the function flushes an in_progress marker or touches storage."""

    def __getattr__(self, name):  # pragma: no cover - only on regression
        raise AssertionError(
            f"replicate_snapshot touched session.{name}() for a placeholder"
        )


def test_replicate_snapshot_refuses_a_placeholder_without_registering_a_dr_copy():
    """Behavioural: a skipped_concurrent placeholder reaching
    replicate_snapshot directly must be marked skipped with NO dr_blob_path
    and NO dr_replicated_at. Before the fix it had zero snapshot_items, so
    it took the "metadata-only" path and was recorded as `replicated` with a
    DR path — an empty DR copy indistinguishable from a real one."""
    snapshot = types.SimpleNamespace(
        id="11111111-1111-1111-1111-111111111111",
        resource_id="22222222-2222-2222-2222-222222222222",
        extra_data={"skipped_concurrent": True, "concurrent_snapshot_id": "x"},
        dr_replication_status="pending",
        dr_replication_attempts=0,
        dr_blob_path=None,
        dr_replicated_at=None,
        dr_error=None,
    )
    tenant = types.SimpleNamespace(
        id="33333333-3333-3333-3333-333333333333",
        dr_region_enabled=True,
        dr_storage_account_name="drstoracct",
        dr_storage_account_key_encrypted="enc",
        extra_data={},
    )

    _run(_drw.replicate_snapshot(snapshot, tenant, _FakeSession()))

    assert snapshot.dr_replication_status == "skipped"
    assert snapshot.dr_blob_path is None, (
        "a 0-item placeholder must not register a DR blob path"
    )
    assert snapshot.dr_replicated_at is None, (
        "a 0-item placeholder must not be stamped as replicated"
    )
    # Guard must not burn a retry attempt either.
    assert snapshot.dr_replication_attempts == 0


def test_replicate_snapshot_still_processes_a_real_partial_snapshot():
    """Negative control: the placeholder guard must not swallow real work.
    A PARTIAL snapshot with no skipped_concurrent flag must proceed past the
    guard (it gets as far as flushing in_progress and resolving storage)."""
    snapshot = types.SimpleNamespace(
        id="44444444-4444-4444-4444-444444444444",
        resource_id="55555555-5555-5555-5555-555555555555",
        extra_data={},  # pre-flag rows look like this too
        dr_replication_status="pending",
        dr_replication_attempts=0,
        dr_blob_path=None,
        dr_replicated_at=None,
        dr_error=None,
    )
    tenant = types.SimpleNamespace(
        id="66666666-6666-6666-6666-666666666666",
        dr_region_enabled=True,
        dr_storage_account_name="drstoracct",
        dr_storage_account_key_encrypted="enc",
        extra_data={},
    )

    flushed = []

    class _RecordingSession:
        async def flush(self):
            flushed.append(True)

        async def get(self, *_a, **_kw):
            return None

        async def execute(self, *_a, **_kw):
            raise AssertionError("should not reach the items query in this test")

    _run(_drw.replicate_snapshot(snapshot, tenant, _RecordingSession()))

    assert flushed, "a real snapshot must get past the placeholder guard"
    assert snapshot.dr_replication_status != "skipped", (
        "the guard must key off skipped_concurrent only, never off item count"
    )
