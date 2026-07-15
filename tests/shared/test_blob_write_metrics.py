"""TDD — at-rest encryption write-coverage metrics are safe no-ops when
Prometheus isn't wired, and callable regardless."""
from __future__ import annotations

from shared import sla_metrics


def test_record_blob_write_is_noop_safe_before_init():
    # Must never raise even when prometheus_client is absent / init not called.
    sla_metrics.record_blob_write(True)
    sla_metrics.record_blob_write(False)


def test_set_encryption_write_enabled_is_noop_safe():
    sla_metrics.set_encryption_write_enabled(True)
    sla_metrics.set_encryption_write_enabled(False)


def test_helpers_exist():
    assert callable(sla_metrics.record_blob_write)
    assert callable(sla_metrics.set_encryption_write_enabled)
