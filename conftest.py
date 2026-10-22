"""Repo-root pytest configuration.

Sole job: make a test module that could not be imported FAIL the run
instead of quietly skipping it.

Why this exists (2026-08-05). 38 test modules load a hyphenated service /
worker file via importlib and wrap it in::

    try:
        _spec.loader.exec_module(_bw)
    except Exception as exc:
        pytest.skip(f"... failed to import: {exc}", allow_module_level=True)

On a machine without the heavy deps (azure, aiohttp, fastapi) that turns
"this file tested nothing" into a skip. Skips do not fail a run, so::

    $ pytest tests/shared/test_batch_rollup.py tests/workers/test_chat_drain_honesty.py \
             tests/workers/test_shard_failure_accounting.py \
             tests/workers/test_enumeration_failure_reconcile.py \
             tests/workers/test_redelivery_liveness_guard.py
    31 passed, 4 skipped
    EXIT=0

...is a green run in which every module covering a backup-worker change
executed nothing. 20 of the ~40 worker modules behave this way on system
Python, including the pre-existing chat-drain-honesty and
shard-failure-accounting suites. That is how a worker change reaches
review "with a green suite" having never been exercised.

A pure all-skip invocation does exit 5 (NO_TESTS_COLLECTED), but the
moment ONE real test runs alongside it the exit code goes back to 0 and
the signal is gone. Exit codes can't be relied on here; this guard can.

Escape hatch: set ``TMVAULT_ALLOW_MISSING_TEST_DEPS=1`` to downgrade to a
warning — for deliberately running one narrow suite on a bare
interpreter. CI and any pre-merge run must NOT set it.

Install the deps instead: ``scripts/run-tests.sh`` (or
``pip install -r requirements-test.txt`` in a venv — system pip is
PEP-668 blocked).
"""
from __future__ import annotations

import os

# Substrings that mark a skip as "the module could not be imported"
# rather than a deliberate, meaningful skip (platform gate, opt-in
# integration marker, ...). Matched case-insensitively against the skip
# reason recorded at collection time.
_IMPORT_FAILURE_MARKERS = (
    "failed to import",
    "not importable",
    "no module named",
    "modulenotfounderror",
    "importerror",
    "cannot import name",
)

_ALLOW_ENV = "TMVAULT_ALLOW_MISSING_TEST_DEPS"

# (nodeid, reason) for every module skipped at collection time because of
# an import failure.
_import_skips: list[tuple[str, str]] = []


def _reason_of(report) -> str:
    """Extract the skip reason from a CollectReport.

    A module-level ``pytest.skip(..., allow_module_level=True)`` surfaces
    as ``longrepr = (path, lineno, "Skipped: <reason>")``.
    """
    longrepr = getattr(report, "longrepr", None)
    if isinstance(longrepr, tuple) and len(longrepr) == 3:
        return str(longrepr[2])
    return str(longrepr or "")


def pytest_collectreport(report):
    if getattr(report, "outcome", None) != "skipped":
        return
    reason = _reason_of(report)
    low = reason.lower()
    if any(marker in low for marker in _IMPORT_FAILURE_MARKERS):
        _import_skips.append((report.nodeid or "<unknown>", reason))


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    if not _import_skips:
        return
    allowed = bool(os.environ.get(_ALLOW_ENV))
    write = terminalreporter.write_line
    terminalreporter.section(
        "UNIMPORTABLE TEST MODULES" + (" (allowed)" if allowed else " — FAILING RUN"),
        red=not allowed,
        yellow=allowed,
        bold=True,
    )
    write(
        f"{len(_import_skips)} test module(s) were SKIPPED because they "
        f"could not be imported. They tested nothing:"
    )
    for nodeid, reason in _import_skips:
        write(f"  - {nodeid}")
        write(f"      {reason.strip().splitlines()[-1][:160]}")
    write("")
    if allowed:
        write(f"{_ALLOW_ENV} is set, so this is a warning only.")
        return
    write("Install the test dependencies and re-run:")
    write("    ./scripts/run-tests.sh            # bootstraps .venv-test, then runs pytest")
    write("    # or, in an existing venv:")
    write("    pip install -r requirements-test.txt")
    write("")
    write(f"To bypass deliberately (never in CI): export {_ALLOW_ENV}=1")


def pytest_sessionfinish(session, exitstatus):
    """Force a non-zero exit when modules failed to import.

    Must not clobber an existing failure code — only promote a
    would-be-success (or the exit-5 'no tests collected') to a failure.
    """
    if not _import_skips or os.environ.get(_ALLOW_ENV):
        return
    import pytest as _pytest

    if exitstatus in (0, int(_pytest.ExitCode.NO_TESTS_COLLECTED)):
        session.exitstatus = int(_pytest.ExitCode.TESTS_FAILED)
