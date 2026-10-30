"""Unit tests for discovery-worker._seconds_until helper.

These tests are hermetic: they supply a fixed *now* so they never depend on
wall-clock time or an external database.
"""
import sys
import os
from datetime import datetime
from zoneinfo import ZoneInfo

# Make the discovery-worker directory importable without installing it.
_WORKER_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "workers", "discovery-worker")
sys.path.insert(0, os.path.abspath(_WORKER_DIR))

# Import only the pure helper — do NOT import the full main module which
# requires heavy runtime deps (sqlalchemy, aio_pika, etc.).
# We achieve this by importing from a fresh module scope.
import importlib.util, types

_spec = importlib.util.spec_from_file_location(
    "_discovery_main",
    os.path.join(os.path.abspath(_WORKER_DIR), "main.py"),
)


def _load_seconds_until():
    """Extract _seconds_until without executing the module-level imports."""
    src_path = os.path.join(os.path.abspath(_WORKER_DIR), "main.py")
    with open(src_path) as fh:
        source = fh.read()

    # Pull out just the _seconds_until function definition so we can exec it
    # without triggering the heavy imports at the top of the file.
    import ast, textwrap

    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "_seconds_until":
            func_src = ast.get_source_segment(source, node)
            ns: dict = {"Optional": __import__("typing").Optional, "datetime": datetime, "timedelta": __import__("datetime").timedelta, "ZoneInfo": ZoneInfo}
            exec(textwrap.dedent(func_src), ns)  # noqa: S102
            return ns["_seconds_until"]
    raise RuntimeError("_seconds_until not found in main.py")


_seconds_until = _load_seconds_until()

TZ = "America/Phoenix"


def _now(h: int, m: int) -> datetime:
    tz = ZoneInfo(TZ)
    return datetime(2024, 6, 15, h, m, 0, tzinfo=tz)


def test_target_in_future_same_day():
    """now=01:00, target=02:00 → ~1 h."""
    s = _seconds_until("02:00", TZ, now=_now(1, 0))
    assert 3500 < s <= 3600, f"expected ~3600s, got {s}"


def test_target_already_passed_today():
    """now=10:00, target=02:00 → ~16 h (next day)."""
    s = _seconds_until("02:00", TZ, now=_now(10, 0))
    expected = (24 - 8) * 3600  # 16 h
    assert abs(s - expected) < 5, f"expected ~{expected}s, got {s}"


def test_target_exact_moment_rolls_to_tomorrow():
    """now == target → schedule for tomorrow (timedelta +1d), not 0s."""
    s = _seconds_until("02:00", TZ, now=_now(2, 0))
    assert s >= 86390, f"expected ~86400s, got {s}"


def test_minimum_sleep_floor():
    """Result is always >= 1.0 second."""
    s = _seconds_until("02:00", TZ, now=_now(1, 59))
    assert s >= 1.0


def test_different_timezone():
    """Sanity check with UTC timezone."""
    tz_utc = "UTC"
    tz = ZoneInfo(tz_utc)
    now_utc = datetime(2024, 6, 15, 3, 0, 0, tzinfo=tz)
    s = _seconds_until("04:00", tz_utc, now=now_utc)
    assert abs(s - 3600) < 5, f"expected ~3600s, got {s}"
