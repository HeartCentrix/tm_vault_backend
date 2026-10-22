"""System > Health must identify the active storage backend the same way
System > Storage does.

QA sweep 2026-08-11 (build v0.1.371), QA-11: the two System pages read the
SAME row of system_config -> storage_backends and described it differently,
which on a backup product reads as "we don't know where the bytes are":

    System > Storage : "Cloud Storage - WASABI - US-WEST-2 - STABLE"
    System > Health  : "active=seaweedfs/seaweedfs-local, state=stable"

Measured ground truth on prod the same day: system_config.active_backend_id
= cf0dcac3-871c-4ef6-ae35-73d85e6d1552, whose row is
kind=seaweedfs / name=seaweedfs-local / endpoint=https://s3.us-west-2.wasabisys.com;
all 3,579,233 snapshot_items written in the previous 24h carry that
backend_id, and storage_toggle_events is empty (0 toggles ever). The
`seaweedfs` kind is the S3-compatible slot, pointed at Wasabi by
ONPREM_S3_ENDPOINT — so the health string named a SeaweedFS host that
receives nothing and omitted the endpoint that would have reconciled the
two screens.

Pinned contract: check_storage() reports the endpoint-derived provider
label, the raw slot identifiers and the endpoint, and returns the same
description dict the admin-storage API serves, so the surfaces cannot drift.
"""
from __future__ import annotations

import asyncio
import os
import pathlib
import sys
import types

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "test_secret_for_unit_tests_only_x")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))

from shared import health as health_mod  # noqa: E402
from shared.storage.labels import (  # noqa: E402
    backend_label,
    describe_backend,
)

_ACTIVE_ID = "cf0dcac3-871c-4ef6-ae35-73d85e6d1552"
# Verbatim prod row, 2026-08-11.
_PROD_ROW = {
    "kind": "seaweedfs",
    "name": "seaweedfs-local",
    "endpoint": "https://s3.us-west-2.wasabisys.com",
    "is_enabled": True,
}


class _Result:
    def __init__(self, scalar=None, rows=None):
        self._scalar = scalar
        self._rows = rows or []

    def scalar(self):
        return self._scalar

    def mappings(self):
        return self

    def first(self):
        return self._rows[0] if self._rows else None


class _Conn:
    def __init__(self, row):
        self._row = row

    async def execute(self, stmt, params=None):
        sql = str(stmt)
        if "to_regclass" in sql:
            return _Result(scalar=True)
        if "system_config" in sql:
            return _Result(rows=[{"aid": _ACTIVE_ID, "ts": "stable"}])
        if "storage_backends" in sql:
            assert "endpoint" in sql, (
                "check_storage must select the endpoint — it is the only "
                "column that says which provider is receiving writes"
            )
            return _Result(rows=[self._row])
        raise AssertionError(f"unexpected SQL: {sql}")


class _Engine:
    def __init__(self, row):
        self._row = row

    def connect(self):
        engine = self

        class _Ctx:
            async def __aenter__(self):
                return _Conn(engine._row)

            async def __aexit__(self, *exc):
                return False

        return _Ctx()


@pytest.fixture()
def fake_db(monkeypatch):
    def _install(row):
        mod = types.ModuleType("shared.database")
        mod.engine = _Engine(row)
        mod._has_required_tables = None
        mod.REQUIRED_TABLES = ()
        monkeypatch.setitem(sys.modules, "shared.database", mod)

    return _install


def test_health_names_the_provider_that_receives_the_writes(fake_db):
    """The detail string must not name SeaweedFS when the endpoint is Wasabi."""
    fake_db(_PROD_ROW)
    out = asyncio.run(health_mod.check_storage())

    assert out["status"] == health_mod.HEALTHY
    detail = out["detail"]
    # Pre-fix this was exactly "active=seaweedfs/seaweedfs-local, state=stable".
    assert "Wasabi (S3)" in detail, detail
    assert "s3.us-west-2.wasabisys.com" in detail, detail
    # The slot identifiers stay — they are what the DB row literally says and
    # what an engineer greps for — but they can no longer stand alone.
    assert "seaweedfs/seaweedfs-local" in detail, detail
    assert "state=stable" in detail, detail


def test_health_returns_the_same_description_the_storage_page_shows(fake_db):
    """Structured fields, so no consumer has to parse the detail string."""
    fake_db(_PROD_ROW)
    out = asyncio.run(health_mod.check_storage())

    ab = out["active_backend"]
    assert ab["id"] == _ACTIVE_ID
    assert ab["kind"] == "seaweedfs"
    assert ab["name"] == "seaweedfs-local"
    assert ab["endpoint"] == "https://s3.us-west-2.wasabisys.com"
    # Identical to what GET /api/admin/storage/status now serves.
    expected = describe_backend(
        _PROD_ROW["kind"], _PROD_ROW["name"], _PROD_ROW["endpoint"],
    )
    assert {k: ab[k] for k in expected} == expected
    assert expected == {
        "label": "Wasabi (S3)",
        "detail": "wasabi · us-west-2",
        "provider": "wasabi",
    }
    assert out["transition_state"] == "stable"


def test_a_real_seaweedfs_endpoint_still_reads_as_seaweedfs(fake_db):
    """The endpoint-wins rule must not hard-code Wasabi: if the slot is ever
    repointed at an actual SeaweedFS filer the label follows it."""
    fake_db({**_PROD_ROW, "endpoint": "http://seaweed-filer.itops.svc:8333"})
    out = asyncio.run(health_mod.check_storage())
    assert "SeaweedFS" in out["detail"], out["detail"]
    assert out["active_backend"]["provider"] == "seaweedfs"


def test_azure_blob_row_labels_as_azure():
    assert backend_label(
        "azure_blob", "azure-primary",
        "https://tmvaultprod.blob.core.windows.net/",
    ) == "Azure Blob"


def test_transition_state_still_degrades(fake_db):
    """Unchanged behaviour: a non-stable transition is DEGRADED, and the
    state is still carried in the detail string."""
    fake_db(_PROD_ROW)

    class _DrainingConn(_Conn):
        async def execute(self, stmt, params=None):
            sql = str(stmt)
            if "system_config" in sql and "to_regclass" not in sql:
                return _Result(rows=[{"aid": _ACTIVE_ID, "ts": "draining"}])
            return await super().execute(stmt, params)

    mod = sys.modules["shared.database"]

    class _DrainingEngine(_Engine):
        def connect(self):
            row = self._row

            class _Ctx:
                async def __aenter__(self):
                    return _DrainingConn(row)

                async def __aexit__(self, *exc):
                    return False

            return _Ctx()

    mod.engine = _DrainingEngine(_PROD_ROW)
    out = asyncio.run(health_mod.check_storage())
    assert out["status"] == health_mod.DEGRADED
    assert "state=draining" in out["detail"]
