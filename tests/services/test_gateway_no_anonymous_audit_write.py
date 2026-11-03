"""api-gateway must not expose the audit-write route to the internet.

POST /api/v1/audit/log writes the compliance audit trail from an
unvalidated body. It is an internal service-to-service route gated on the
X-Internal-Api-Key shared secret — and proxy_request forwards only
Authorization / Cookie / Content-Type, so the gateway cannot carry that
secret anyway. Publishing it through the gateway only exposed an anonymous
forgery endpoint at the ingress. Nothing in the SPA calls it; every emitter
(job-service, snapshot-service, backup/restore/discovery/azure/chat-export
workers, shared/audit.py) posts straight to AUDIT_SERVICE_URL in-cluster.
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import sys

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "test_secret_for_unit_tests_only_x")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "api-gateway" / "main.py"
_spec = importlib.util.spec_from_file_location("gateway_main_under_test", _MAIN_PATH)
_gw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["gateway_main_under_test"] = _gw
try:
    _spec.loader.exec_module(_gw)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(
        f"api-gateway module failed to import: {exc}",
        allow_module_level=True,
    )


def _routes(path: str) -> list:
    return [
        r for r in _gw.app.routes
        if getattr(r, "path", None) == path
    ]


def test_audit_log_write_not_proxied():
    assert _routes("/api/v1/audit/log") == [], (
        "api-gateway still proxies the audit-write route to the internet"
    )


def test_audit_read_routes_still_proxied():
    """Only the write route goes away — the SIEM/UI read paths must stay."""
    for path in (
        "/api/v1/audit/events",
        "/api/v1/audit/export",
        "/api/v1/audit/siem/stream",
        "/api/v1/activity",
    ):
        assert _routes(path), f"gateway lost read route {path}"
