"""Graph's year-1 chat watermark must never become a stored `-infinity`.

Bug (2026-08-17): every backup cycle settled PARTIAL for all seven
USER_CHATS resources, three times a day, for days. 90% of the counted
failures came from ONE unhandled exception at the claim step, and the
round trip that produced it is asymmetric:

    Graph "0001-01-01T00:00:00Z"     (chats Graph declines to describe:
      -> _parse_iso_to_dt             chatType `unknownFutureValue` and
         => AWARE datetime.min        cross-tenant `19:meeting_*`)
      -> asyncpg encode
         => TIMESTAMPTZ '-infinity'
      -> asyncpg decode
         => NAIVE datetime(1, 1, 1)

So the value written is aware and the value read back is naive. The claim
step then did a bare ``new_lu > prior_lu`` against the aware incoming
value and raised ``TypeError: can't compare offset-naive and offset-aware
datetimes`` — before the ``age_s`` freshness fallback on the very next
line, before any drain branch was chosen, and outside every try/except in
``_drain_one_chat``. The exception escaped into
``gather(return_exceptions=True)`` and was counted as a failed drain with
the exception object discarded.

The population it killed was exactly the 556 PERMISSION (526) + GONE (30)
chats that the ``suppressed`` class exists to absorb — so a mechanism
built to stop permanently-unreadable chats settling PARTIAL forever was
unreachable for the only chats it was ever meant to cover. And because
the UPSERT deliberately does not advance ``last_updated_at``, the
``-infinity`` was immortal: the loop could not self-heal.

These tests pin both halves — the read side may not raise, and the write
side may not store the sentinel in the first place.
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import sys
from datetime import datetime, timezone

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "workers" / "backup-worker" / "main.py"
_spec = importlib.util.spec_from_file_location("bw_watermark", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_watermark"] = _bw
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(_bw)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(
        f"backup-worker module failed to import: {exc}",
        allow_module_level=True,
    )


# What asyncpg hands back for a TIMESTAMPTZ column holding '-infinity'.
_DECODED_MINUS_INFINITY = datetime(1, 1, 1, 0, 0)
# What Graph actually sends for a chat it will not describe.
_GRAPH_SENTINEL = "0001-01-01T00:00:00Z"


# ─── the original defect, reproduced ──────────────────────────────────────

def test_naive_sentinel_vs_aware_incoming_raises_without_the_guard():
    """Pins the ORIGINAL failure so nobody 'simplifies' the guard away.

    This is the exact comparison the claim step used to perform.
    """
    prior_lu = _DECODED_MINUS_INFINITY                       # naive
    new_lu = datetime(2026, 8, 17, 10, 12, tzinfo=timezone.utc)  # aware
    with pytest.raises(TypeError):
        _ = new_lu > prior_lu


# ─── read side: the sentinel becomes "no watermark" ───────────────────────

def test_watermark_dt_drops_the_decoded_minus_infinity():
    assert _bw._chat_watermark_dt(_DECODED_MINUS_INFINITY) is None


def test_watermark_dt_drops_aware_datetime_min():
    """The pre-encode shape, so a value can't slip through before its
    first round trip through Postgres."""
    assert _bw._chat_watermark_dt(datetime.min.replace(tzinfo=timezone.utc)) is None


def test_watermark_dt_passes_a_real_timestamp_through_unchanged():
    real = datetime(2026, 8, 17, 10, 12, tzinfo=timezone.utc)
    assert _bw._chat_watermark_dt(real) == real


def test_watermark_dt_tolerates_none():
    assert _bw._chat_watermark_dt(None) is None


def test_watermark_dt_accepts_a_naive_real_timestamp():
    """Naive but genuine (not the sentinel) must survive — the guard is
    about the year-1 floor, not about awareness."""
    real_naive = datetime(2026, 8, 17, 10, 12)
    assert _bw._chat_watermark_dt(real_naive) == real_naive


# ─── write side: the sentinel never reaches the column ────────────────────

def test_watermark_ts_returns_none_for_the_graph_sentinel():
    """The bind site fix: this is what stops '-infinity' being written."""
    assert _bw._chat_watermark_ts(_GRAPH_SENTINEL) is None


def test_watermark_ts_parses_a_real_graph_timestamp():
    got = _bw._chat_watermark_ts("2026-08-17T10:12:53.772482Z")
    assert got is not None
    assert got.year == 2026 and got.month == 8 and got.day == 17
    assert got.tzinfo is not None


def test_watermark_ts_tolerates_none_and_empty():
    assert _bw._chat_watermark_ts(None) is None
    assert _bw._chat_watermark_ts("") is None


def test_watermark_ts_matches_parse_iso_for_normal_values():
    """The guard must be a pure narrowing of _parse_iso_to_dt — identical
    everywhere except the sentinel."""
    for s in (
        "2026-08-17T10:12:53.772482Z",
        "2020-01-02T03:04:05Z",
        "2026-08-17T14:26:46.1Z",
    ):
        assert _bw._chat_watermark_ts(s) == _bw._parse_iso_to_dt(s)


# ─── the claim comparison is now total ────────────────────────────────────

def _has_new_activity(new_lu, prior_lu):
    """Mirror of the guarded comparison in _claim_or_load_chat_thread."""
    prior_lu = _bw._chat_watermark_dt(prior_lu)
    _new_cmp = _bw._naive_utc_ts(new_lu)
    _prior_cmp = _bw._naive_utc_ts(prior_lu)
    return bool(
        _new_cmp is not None
        and (_prior_cmp is None or _new_cmp > _prior_cmp)
    )


def test_claim_compare_does_not_raise_on_the_sentinel_row():
    """The regression that mattered: 556 rows, three cycles a day."""
    assert _has_new_activity(
        datetime(2026, 8, 17, 10, 12, tzinfo=timezone.utc),
        _DECODED_MINUS_INFINITY,
    ) is True


def test_claim_compare_survives_mixed_awareness_generally():
    """Defence in depth — aware prior, naive incoming, and the reverse."""
    aware = datetime(2026, 8, 17, 10, 12, tzinfo=timezone.utc)
    naive = datetime(2026, 8, 17, 9, 12)
    assert _has_new_activity(aware, naive) is True
    assert _has_new_activity(naive, aware) is False


def test_sentinel_row_is_claimed_deterministically_not_by_freshness():
    """A sentinel row must claim on the activity arm, NOT depend on the 7h
    freshness window having elapsed.

    Being claimed is what carries the chat into the smart-skip, which is
    where a permanently-unreadable chat is finally classified `suppressed`
    instead of counted as a failure. If this regressed to relying on
    freshness, the fix would work only while the cycle interval happened
    to exceed the window.
    """
    assert _has_new_activity(
        datetime(2026, 8, 17, 10, 12, tzinfo=timezone.utc),
        _DECODED_MINUS_INFINITY,
    ) is True


def test_real_watermark_still_suppresses_activity_when_unchanged():
    """The guard must not turn every chat into 'has new activity' — that
    would re-drain the whole tenant every cycle."""
    same = datetime(2026, 8, 17, 10, 12, tzinfo=timezone.utc)
    assert _has_new_activity(same, same) is False


# ─── GONE chats that 404 while still listed ───────────────────────────────

def test_gone_suppress_threshold_is_conservative():
    """Affected rows sit at 19-23 consecutive 404s and the fleet-wide GONE
    mean is 57, so the threshold must be well above transient-fault range
    but far below what these rows have accumulated."""
    assert 2 <= _bw._CHAT_GONE_SUPPRESS_AFTER <= 10


# ─── suppressed chats must not settle the run PARTIAL ─────────────────────

def test_suppressed_chats_alone_do_not_settle_partial():
    """The whole point of the fix: once the sentinel stops killing the
    claim, these chats reach the smart-skip and land in `suppressed` —
    which must leave the snapshot COMPLETED."""
    status, reason = _bw.evaluate_drain_honesty(
        planned=997,
        drained_ok=900,
        failed=0,
        items_persisted=720899,
        suppressed=97,
    )
    assert status is None and reason is None


def test_real_failures_still_settle_partial():
    """Narrowing check — the fix must not loosen the honesty gate."""
    status, reason = _bw.evaluate_drain_honesty(
        planned=997,
        drained_ok=900,
        failed=7,
        items_persisted=720899,
        suppressed=90,
    )
    assert status is _bw.SnapshotStatus.PARTIAL
    assert reason and "7 of 997" in reason
