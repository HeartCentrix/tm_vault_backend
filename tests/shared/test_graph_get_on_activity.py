"""ITO-10146: ``_get`` must expose a per-round-trip ``on_activity`` callback.

Root cause of the 562-event archive stall: these beta admin/exchange
endpoints page at 10 items per response, so ONE ``_get`` call for a
332k-item folder paginates for hours — and the archive stall guard's only
walk-phase liveness signal was *call completion*. Every run since
2026-08-11 was aborted by its own guard at exactly 562 progress events
(the deterministic point where only giant folders remain in flight).

The fix: ``_get`` fires ``on_activity`` once per Graph round-trip — every
successful page AND every throttle/transient retry decision (a retrying
call is genuinely talking to Graph; a true wedge makes no round-trips at
all, and hostile endless-throttles still exhaust the policy's cumulative
cap). These pins keep the wiring from regressing.
"""
from __future__ import annotations

import pathlib
import re

_SRC = (
    pathlib.Path(__file__).resolve().parents[2] / "shared" / "graph_client.py"
).read_text()


def _fn_block(name: str) -> str:
    idx = _SRC.find(f"async def {name}(")
    assert idx != -1, f"{name} not found"
    nxt = _SRC.find("\n    async def ", idx + 10)
    return _SRC[idx: nxt if nxt != -1 else len(_SRC)]


def test_get_threads_on_activity_to_both_paths():
    blk = _fn_block("_get")
    assert "on_activity: Optional[Callable[[], None]] = None" in blk
    assert "self._get_hardened(url, params, on_activity=on_activity)" in blk
    assert "self._get_legacy(url, params, on_activity=on_activity)" in blk


def test_hardened_fires_activity_per_page_and_per_retry():
    blk = _fn_block("_get_hardened")
    assert "on_activity: Optional[Callable[[], None]] = None" in blk
    # Success page: after reset_on_success, i.e. once per fetched page.
    assert re.search(
        r"policy\.reset_on_success\(\)\s*\n\s*_activity\(\)", blk,
    ), "hardened path must stamp activity on every successful page"
    # Retry decisions: throttle + transient + malformed body all stamp
    # (each is a real Graph round-trip, bounded by the cumulative cap).
    assert blk.count("_activity()") >= 4, (
        "expected activity stamps on success, throttle, transient and "
        "malformed-body branches"
    )
    # The callback must never raise into the client.
    assert "except Exception:" in blk.split("def _activity", 1)[1][:300]


def test_legacy_fires_activity_per_page_and_per_retry():
    blk = _fn_block("_get_legacy")
    assert "on_activity: Optional[Callable[[], None]] = None" in blk
    assert re.search(
        r"retry_count = 0  # Reset on success\s*\n\s*_activity\(\)", blk,
    ), "legacy path must stamp activity on every successful page"
    assert blk.count("_activity()") >= 3


def test_archive_walk_threads_progress_into_get():
    """The consumer side: the archive walk passes its progress stamp as
    on_activity on every enumeration _get, so pagination counts as
    progress for the stall guard."""
    src = (
        pathlib.Path(__file__).resolve().parents[2]
        / "workers" / "backup-worker" / "archive_backup.py"
    ).read_text()
    starts = [m.start() for m in re.finditer(r"self\.g\._get\(", src)]
    assert starts, "archive walker _get call sites not found"
    for s in starts:
        window = src[s: s + 250]
        assert "on_activity=self.on_progress" in window, (
            f"an archive walker _get call site lacks on_activity: {window[:120]}"
        )
