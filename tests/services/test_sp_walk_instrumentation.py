"""Pins for the SharePoint walk instrumentation (2026-07-21).

The first ITOPS site backup ran 30h at a dead-flat ~450 files/hour with
20 idle-looking drive consumers — and nothing in the logs said whether
the producer (Graph enumeration) or the consumers (download/upload/DB)
were the bottleneck. These pins keep the two diagnostics alive:

  * the drive producer logs a periodic [SP_DRIVE_RATE] line with its
    streamed count and the queue depth (queue near-empty = producer
    starved; queue full = consumers slow);
  * the drive consumer logs a periodic [SP_CONSUME_RATE] rolling
    summary (files done, avg/max seconds per file) and samples slow
    files individually.

Source-inspection pins — importing the worker module drags in the full
runtime.
"""
from __future__ import annotations

import pathlib

_SRC = (
    pathlib.Path(__file__).resolve().parents[2]
    / "workers" / "backup-worker" / "main.py"
).read_text()


def test_drive_producer_logs_rate_and_queue_depth():
    assert "[SP_DRIVE_RATE]" in _SRC
    block = _SRC[_SRC.index("async def drive_producer"): _SRC.index("async def drive_producer") + 6000]
    assert "[SP_DRIVE_RATE]" in block, "rate log must live in the producer loop"
    assert "qsize()" in block, "producer rate line must include queue depth"


def test_drive_consumer_logs_rolling_summary_and_slow_files():
    assert "[SP_CONSUME_RATE]" in _SRC
    assert "[SP_SLOW_FILE]" in _SRC
