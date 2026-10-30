"""Admin-storage API: one server-side backend label, and a toggle that
fails closed on a target the data plane cannot serve.

QA sweep 2026-08-11 (build v0.1.371):

QA-11 — System > Storage and System > Health described the same
system_config row differently ("Cloud Storage / WASABI / US-WEST-2" vs
"active=seaweedfs/seaweedfs-local"). The Storage page derived its label in
the browser from `endpoint` (src/services/adminStorage.ts), /health/deep
printed `kind`/`name`. Both now come from shared.storage.labels, and
/status + /backends serve the label so the SPA can stop deriving it.

QA-10 — the toggle button offered `azure-primary` purely because its id
differs from the active backend. That row is is_enabled=false with
endpoint https://PLACEHOLDER.blob.core.windows.net/, and
shared/storage/router.py only registers rows WHERE is_enabled = true, so
promoting it would make every write raise BackendNotFoundError. The toggle
worker's preflight does catch it — but only after this endpoint has
written a storage_toggle_events row and published to storage.toggle.
submit_toggle now rejects it before either side effect.

Prod values used below are verbatim from the 2026-08-11 read-only query of
public.storage_backends / public.system_config.
"""
from __future__ import annotations

import asyncio
import importlib.util
import os
import pathlib
import sys
import uuid
from types import SimpleNamespace

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "test_secret_for_unit_tests_only_x")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))

_ADMIN_PATH = _REPO_ROOT / "api-gateway" / "routes" / "admin_storage.py"
_spec = importlib.util.spec_from_file_location(
    "admin_storage_labels_under_test", _ADMIN_PATH,
)
admin_storage = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["admin_storage_labels_under_test"] = admin_storage
try:
    _spec.loader.exec_module(admin_storage)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(
        f"admin_storage module failed to import: {exc}",
        allow_module_level=True,
    )

from fastapi import HTTPException  # noqa: E402

from shared.health import check_storage  # noqa: E402

_SEAWEED_ID = uuid.UUID("cf0dcac3-871c-4ef6-ae35-73d85e6d1552")
_AZURE_ID = uuid.UUID("424ed1b9-2f8a-4715-806f-4022aeb00180")

_ROWS = {
    _SEAWEED_ID: {
        "id": str(_SEAWEED_ID), "kind": "seaweedfs", "name": "seaweedfs-local",
        "endpoint": "https://s3.us-west-2.wasabisys.com", "is_enabled": True,
    },
    _AZURE_ID: {
        "id": str(_AZURE_ID), "kind": "azure_blob", "name": "azure-primary",
        "endpoint": "https://PLACEHOLDER.blob.core.windows.net/",
        "is_enabled": False,
    },
}


class FakeConn:
    """asyncpg-conn stand-in covering /status, /backends and submit_toggle."""

    def __init__(self, *, transition_state="stable"):
        self.transition_state = transition_state
        self.inserted_events: list[tuple] = []
        self.closed = False

    async def execute(self, sql, *args):
        if "INSERT INTO storage_toggle_events" in sql:
            self.inserted_events.append(args)
            return "INSERT 0 1"
        return "UPDATE 0"

    async def fetchrow(self, sql, *args):
        if "FROM system_config sc" in sql:
            active = _ROWS[_SEAWEED_ID]
            return {
                "active_backend_id": _SEAWEED_ID,
                "transition_state": self.transition_state,
                "last_toggle_at": None, "cooldown_until": None,
                "b_id": _SEAWEED_ID, "kind": active["kind"],
                "name": active["name"], "endpoint": active["endpoint"],
                "is_enabled": active["is_enabled"],
            }
        if "FROM system_config WHERE id=1" in sql:
            return {
                "active_backend_id": _SEAWEED_ID,
                "transition_state": self.transition_state,
                "cooldown_until": None,
            }
        if "FROM storage_backends WHERE id=" in sql:
            return _ROWS.get(args[0])
        raise AssertionError(f"unexpected fetchrow: {sql}")

    async def fetchval(self, sql, *args):
        if "FROM jobs" in sql:
            return 7
        raise AssertionError(f"unexpected fetchval: {sql}")

    async def fetch(self, sql, *args):
        if "FROM storage_backends ORDER BY name" in sql:
            return [_ROWS[_AZURE_ID], _ROWS[_SEAWEED_ID]]
        raise AssertionError(f"unexpected fetch: {sql}")

    async def close(self):
        self.closed = True


@pytest.fixture()
def conn(monkeypatch):
    c = FakeConn()

    async def _fake_connect():
        return c

    monkeypatch.setattr(admin_storage, "_connect", _fake_connect)
    return c


# ------------------------------------------------------------- QA-11

def test_status_serves_the_backend_label_from_the_server(conn):
    out = asyncio.run(admin_storage.status(user={"id": "u"}))
    ab = out["active_backend"]
    # Raw columns preserved — the SPA and any script still see them.
    assert ab["kind"] == "seaweedfs"
    assert ab["name"] == "seaweedfs-local"
    assert ab["endpoint"] == "https://s3.us-west-2.wasabisys.com"
    # New: the words the page shows, decided once, on the server.
    assert ab["label"] == "Wasabi (S3)"
    assert ab["detail"] == "wasabi · us-west-2"
    assert ab["provider"] == "wasabi"
    assert out["transition_state"] == "stable"
    assert out["inflight_jobs_count"] == 7


def test_backends_list_carries_the_same_labels(conn):
    rows = asyncio.run(admin_storage.list_backends(user={"id": "u"}))
    by_name = {r["name"]: r for r in rows}
    assert by_name["seaweedfs-local"]["label"] == "Wasabi (S3)"
    assert by_name["azure-primary"]["label"] == "Azure Blob"
    # is_enabled is what the button must gate on (QA-10).
    assert by_name["azure-primary"]["is_enabled"] is False


def test_storage_page_and_health_agree_on_the_active_backend(conn, monkeypatch):
    """The QA-11 comparison itself: /status and /health/deep, same row."""
    import types

    class _Result:
        def __init__(self, scalar=None, rows=None):
            self._scalar, self._rows = scalar, rows or []

        def scalar(self):
            return self._scalar

        def mappings(self):
            return self

        def first(self):
            return self._rows[0] if self._rows else None

    class _Conn:
        async def execute(self, stmt, params=None):
            sql = str(stmt)
            if "to_regclass" in sql:
                return _Result(scalar=True)
            if "system_config" in sql:
                return _Result(rows=[{"aid": str(_SEAWEED_ID), "ts": "stable"}])
            return _Result(rows=[_ROWS[_SEAWEED_ID]])

    class _Engine:
        def connect(self):
            class _Ctx:
                async def __aenter__(self):
                    return _Conn()

                async def __aexit__(self, *exc):
                    return False

            return _Ctx()

    mod = types.ModuleType("shared.database")
    mod.engine = _Engine()
    monkeypatch.setitem(sys.modules, "shared.database", mod)

    api = asyncio.run(admin_storage.status(user={"id": "u"}))["active_backend"]
    health = asyncio.run(check_storage())["active_backend"]
    for field in ("label", "detail", "provider", "endpoint", "kind", "name"):
        assert api[field] == health[field], field


# ------------------------------------------------------------- QA-10

def test_toggle_refuses_a_disabled_target_backend(conn):
    """azure-primary is is_enabled=false, so the router never registers it —
    promoting it would break every write. Reject before writing an event."""
    req = SimpleNamespace(
        target_backend_id=_AZURE_ID, confirmation_text="TMVAULT-ORG",
        reason="qa",
    )
    request = SimpleNamespace(client=SimpleNamespace(host="10.0.0.1"))
    with pytest.raises(HTTPException) as ei:
        asyncio.run(admin_storage.submit_toggle(
            req, request, user={"id": str(uuid.uuid4()), "org_id": "TMVAULT-ORG"},
        ))
    assert ei.value.status_code == 400
    assert "disabled" in str(ei.value.detail)
    # The point of failing here rather than in the worker: no audit row, no
    # queued message, nothing to drain.
    assert conn.inserted_events == []
    assert conn.closed is True


def test_toggle_refuses_an_unknown_target_backend(conn):
    req = SimpleNamespace(
        target_backend_id=uuid.uuid4(), confirmation_text="TMVAULT-ORG",
        reason="qa",
    )
    request = SimpleNamespace(client=SimpleNamespace(host="10.0.0.1"))
    with pytest.raises(HTTPException) as ei:
        asyncio.run(admin_storage.submit_toggle(
            req, request, user={"id": str(uuid.uuid4()), "org_id": "TMVAULT-ORG"},
        ))
    assert ei.value.status_code == 404
    assert conn.inserted_events == []


def test_toggle_still_rejects_the_already_active_backend(conn):
    """Unchanged guard — kept so the new checks can't reorder past it."""
    req = SimpleNamespace(
        target_backend_id=_SEAWEED_ID, confirmation_text="TMVAULT-ORG",
        reason="qa",
    )
    request = SimpleNamespace(client=SimpleNamespace(host="10.0.0.1"))
    with pytest.raises(HTTPException) as ei:
        asyncio.run(admin_storage.submit_toggle(
            req, request, user={"id": str(uuid.uuid4()), "org_id": "TMVAULT-ORG"},
        ))
    assert ei.value.status_code == 400
    assert "already active" in str(ei.value.detail)
