"""Regression: the backup consumer's retry ceiling was inert on classic queues.

``_handle_one`` gated requeue on the ``x-delivery-count`` header::

    delivery_count = headers.get("x-delivery-count", 0)
    if delivery_count < settings.MAX_RETRIES:
        await message.reject(requeue=True)

Only QUORUM queues set ``x-delivery-count``. Every queue in this deployment is
classic (verified 2026-08-06 via ``rabbitmqctl list_queues name type``), so the
value was permanently 0, the branch always requeued, and any handler exception
looped forever. main.py:3810 already documents this exact trap for the poison
path — the generic arm never got the same treatment.

Observed 2026-08-06: one USER_ARCHIVE message on ``backup.heavy`` produced
**138 snapshots in 7h (137 FAILED)**, kept a heavy worker pinned, and emitted
140 BACKUP_FAILED audit events for a single broken resource.

Fix mirrors the house idiom already documented in
``shared/message_bus.consume`` — "requeue=True + reject_on_redelivered=True …
Two attempts, then the message is parked for an operator — never a hot-loop,
never a drop" — while still honouring ``x-delivery-count`` when a quorum queue
does supply it.
"""
from __future__ import annotations

import importlib.util
import pathlib
import sys

import pytest

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


def _load_helper():
    """Import just the pure helper out of the worker module without executing
    the 20k-line module body (no DB/Graph imports at test time)."""
    src = (_REPO_ROOT / "workers" / "backup-worker" / "main.py").read_text()
    marker = "def should_requeue_failed_delivery("
    idx = src.find(marker)
    assert idx != -1, "pure helper should_requeue_failed_delivery not found"
    end = src.find("\ndef ", idx + 1)
    nxt = src.find("\nclass ", idx + 1)
    if nxt != -1 and (end == -1 or nxt < end):
        end = nxt
    block = src[idx:] if end == -1 else src[idx:end]
    ns: dict = {}
    exec(compile(block, "should_requeue_failed_delivery", "exec"), ns)
    return ns["should_requeue_failed_delivery"]


should_requeue = _load_helper()


# ───────────────── classic queues (no x-delivery-count) ─────────────────

def test_classic_first_delivery_is_requeued():
    assert should_requeue({}, redelivered=False, max_retries=5) is True


def test_classic_redelivery_is_dlqd_not_requeued():
    """The storm-stopper: a second failure must NOT go back on the queue."""
    assert should_requeue({}, redelivered=True, max_retries=5) is False


def test_classic_none_headers_tolerated():
    assert should_requeue(None, redelivered=False, max_retries=5) is True
    assert should_requeue(None, redelivered=True, max_retries=5) is False


# ───────────────── quorum queues (x-delivery-count present) ─────────────────

def test_quorum_counts_up_to_max_retries():
    for n in range(0, 5):
        assert should_requeue({"x-delivery-count": n},
                              redelivered=True, max_retries=5) is True


def test_quorum_stops_at_max_retries():
    assert should_requeue({"x-delivery-count": 5},
                          redelivered=True, max_retries=5) is False
    assert should_requeue({"x-delivery-count": 9},
                          redelivered=True, max_retries=5) is False


def test_quorum_header_wins_over_redelivered_flag():
    """A quorum queue's counter is authoritative — a redelivered message with
    budget left must still be retried (otherwise quorum queues silently
    regress to 2 attempts)."""
    assert should_requeue({"x-delivery-count": 1},
                          redelivered=True, max_retries=5) is True


@pytest.mark.parametrize("bad", ["nonsense", None, "", [], {}])
def test_unparseable_header_falls_back_to_redelivered(bad):
    """A junk header must not resurrect the infinite loop."""
    assert should_requeue({"x-delivery-count": bad},
                          redelivered=True, max_retries=5) is False
    assert should_requeue({"x-delivery-count": bad},
                          redelivered=False, max_retries=5) is True


def test_zero_max_retries_never_requeues():
    assert should_requeue({}, redelivered=False, max_retries=0) is False


# ───────────────── the call site actually uses it ─────────────────

def test_handle_one_no_longer_reads_the_header_directly():
    src = (_REPO_ROOT / "workers" / "backup-worker" / "main.py").read_text()
    assert 'delivery_count = headers.get("x-delivery-count", 0)' not in src, (
        "the inert classic-queue gate is still live"
    )
    assert "should_requeue_failed_delivery(" in src
