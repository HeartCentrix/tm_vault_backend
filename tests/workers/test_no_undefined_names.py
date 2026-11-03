"""Pyflakes undefined-name gate for the backup-worker.

py_compile can't catch unresolved names — they only blow up when the
code path finally runs. Two of them shipped in rarely-exercised paths
and one detonated in prod (2026-07-15): backup_sharepoint referenced
the caller's ``job_id`` local, so the FIRST partition-sized SharePoint
site backup crashed with NameError and poison-looped its queue message
(crash → nack/requeue → fresh snapshot row, ~100/min across the heavy
pool until the queue was purged). pyflakes finds this class statically;
this test keeps the worker permanently clean.
"""
from __future__ import annotations

import pathlib

import pytest

pyflakes_api = pytest.importorskip("pyflakes.api")
import pyflakes.reporter  # noqa: E402

_WORKER_MAIN = (
    pathlib.Path(__file__).resolve().parents[2]
    / "workers" / "backup-worker" / "main.py"
)


class _Collector:
    def __init__(self):
        self.messages = []

    def flake(self, message):
        self.messages.append(str(message))

    def unexpectedError(self, filename, msg):  # noqa: N802 (pyflakes API)
        self.messages.append(f"{filename}: {msg}")

    def syntaxError(self, filename, msg, lineno, offset, text):  # noqa: N802
        self.messages.append(f"{filename}:{lineno}: {msg}")


def test_backup_worker_has_no_undefined_names():
    collector = _Collector()
    reporter = pyflakes.reporter.Reporter(collector, collector)
    # Reporter writes via flake(); route both streams through it.
    pyflakes_api.check(_WORKER_MAIN.read_text(), str(_WORKER_MAIN), collector)
    undefined = [m for m in collector.messages if "undefined name" in m]
    assert not undefined, (
        "undefined names in backup-worker main.py (NameError time bombs "
        "in un-exercised paths — see 2026-07-15 backup_sharepoint poison "
        f"loop): {undefined}"
    )
