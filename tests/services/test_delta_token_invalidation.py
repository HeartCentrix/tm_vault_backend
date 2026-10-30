"""Regression: DELETE /delta-token/{id} must clear the cursors the
backup-worker actually reads.

Bug (2026-08 review): force-full-sync invalidation only deleted one Redis
cache key and nulled delta_token on the single latest snapshot row. The
worker's real incremental cursors live in resource.extra_data
(mail_delta_tokens_by_folder, drive_delta_tokens_by_site, chat/calendar
maps, generic delta_token) and in the mail_folder_delta /
sharepoint_drive_delta tables — all untouched. The endpoint returned
{"status": "invalidated"} while the next backup kept running incremental
on the corrupt cursor (items silently missing until restore time).
Compounding: GET falls back to the newest snapshot WHERE delta_token IS
NOT NULL, so nulling only the latest row resurrected the second-newest
token; and a folderless invalidation left every per-folder Redis key
(delta_token:{rid}:{fid}) alive.
"""
from __future__ import annotations

import asyncio
import fnmatch
import importlib.util
import os
import pathlib
import sys
import uuid
from types import SimpleNamespace

from sqlalchemy.sql import Delete, Select, Update

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "services" / "delta-token" / "main.py"
_spec = importlib.util.spec_from_file_location("delta_token_main_under_test", _MAIN_PATH)
_mod = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["delta_token_main_under_test"] = _mod
assert _spec and _spec.loader
_spec.loader.exec_module(_mod)  # type: ignore[union-attr]


class FakeRedis:
    def __init__(self, keys=()):
        self.store = {k: "tok" for k in keys}
        self.deleted = []

    async def set(self, key, val, ex=None, nx=False):
        if nx and key in self.store:
            return None
        self.store[key] = val
        return True

    async def delete(self, key):
        self.deleted.append(key)
        self.store.pop(key, None)

    async def scan_iter(self, match=None):
        for k in list(self.store):
            if match is None or fnmatch.fnmatch(k, match):
                yield k


class FakeScalars:
    def __init__(self, first_value):
        self._first = first_value

    def first(self):
        return self._first


class FakeResult:
    def __init__(self, first_value=None, rowcount=1):
        self._first = first_value
        self.rowcount = rowcount

    def scalars(self):
        return FakeScalars(self._first)


class FakeSession:
    def __init__(self, resource=None, latest_snapshot=None):
        self.resource = resource
        self.latest_snapshot = latest_snapshot
        self.executed = []
        self.committed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, model, pk):
        return self.resource

    async def execute(self, stmt):
        self.executed.append(stmt)
        if isinstance(stmt, Select):
            return FakeResult(first_value=self.latest_snapshot)
        return FakeResult(rowcount=2)

    async def commit(self):
        self.committed = True


def _fake_request():
    return SimpleNamespace(client=SimpleNamespace(host="10.0.0.1"))


def _cursor_laden_resource():
    return SimpleNamespace(
        extra_data={
            "user_id": "u-keep-me",
            "delta_token": "corrupt-generic",
            "mail_delta_tokens_by_folder": {"f1": "tokA", "f2": "tokB"},
            "drive_delta_tokens_by_site": {"s1": "tokC"},
            "chat_delta_tokens": {"c1": "tokD"},
            "calendar_delta_tokens": {"cal1": "tokE"},
            "mail_folder_fingerprints": {"f1": {"n": 1}},
            "onedrive_delta_link": "https://graph/delta?token=corrupt",
        }
    )


def test_resource_wide_invalidation_clears_worker_cursors():
    rid = str(uuid.uuid4())
    resource = _cursor_laden_resource()
    session = FakeSession(resource=resource)
    _mod.async_session_factory = lambda: session
    redis = FakeRedis(keys=(
        f"delta_token:{rid}",
        f"delta_token:{rid}:f1",
        f"delta_token:{rid}:f2",
        "delta_token:other-resource:f9",
    ))
    _mod.redis_client = redis

    resp = asyncio.run(_mod.invalidate_delta_token(
        rid, _fake_request(), folder_id=None,
        confirm="force-full-sync", x_internal_api_key="k",
    ))

    assert resp["status"] == "invalidated"
    # Every worker cursor key stripped; non-cursor metadata survives.
    for key in _mod.WORKER_CURSOR_KEYS:
        assert key not in resource.extra_data, key
    assert resource.extra_data["user_id"] == "u-keep-me"
    assert resp["cleared"]["resource_cursor_keys"] >= 6

    # Per-folder Redis keys must die with the base key; other resources' keys survive.
    assert f"delta_token:{rid}" in redis.deleted
    assert f"delta_token:{rid}:f1" in redis.deleted
    assert f"delta_token:{rid}:f2" in redis.deleted
    assert "delta_token:other-resource:f9" in redis.store

    # All three per-folder/per-drive cursor tables cleared for the resource.
    delete_tables = {s.table.name for s in session.executed if isinstance(s, Delete)}
    assert delete_tables == {
        "mail_folder_delta", "mail_folder_fingerprint", "sharepoint_drive_delta",
    }

    # Snapshot cursor cleared on EVERY row (bulk UPDATE), not a
    # latest-row-only write that the GET fallback can step around.
    updates = [s for s in session.executed if isinstance(s, Update)]
    assert any(s.table.name == "snapshots" for s in updates)
    assert not any(isinstance(s, Select) for s in session.executed)
    assert session.committed


def test_folder_scoped_invalidation_pops_only_that_folder():
    rid = str(uuid.uuid4())
    resource = _cursor_laden_resource()
    snapshot = SimpleNamespace(delta_token="corrupt-latest")
    session = FakeSession(resource=resource, latest_snapshot=snapshot)
    _mod.async_session_factory = lambda: session
    redis = FakeRedis(keys=(
        f"delta_token:{rid}",
        f"delta_token:{rid}:f1",
        f"delta_token:{rid}:f2",
    ))
    _mod.redis_client = redis

    resp = asyncio.run(_mod.invalidate_delta_token(
        rid, _fake_request(), folder_id="f1",
        confirm="force-full-sync", x_internal_api_key="k",
    ))

    assert resp["status"] == "invalidated"
    # Only f1 dropped from the per-folder maps; f2's cursor is intact.
    assert "f1" not in resource.extra_data["mail_delta_tokens_by_folder"]
    assert resource.extra_data["mail_delta_tokens_by_folder"]["f2"] == "tokB"
    # Generic/resource-level cursors untouched on a folder-scoped call.
    assert resource.extra_data["delta_token"] == "corrupt-generic"

    assert redis.deleted == [f"delta_token:{rid}:f1"]
    assert f"delta_token:{rid}" in redis.store
    assert f"delta_token:{rid}:f2" in redis.store

    # Table deletes are folder-constrained.
    deletes = [s for s in session.executed if isinstance(s, Delete)]
    assert len(deletes) == 3
    for stmt in deletes:
        assert "folder_id" in str(stmt) or "drive_id" in str(stmt)

    # Latest-snapshot cursor still cleared.
    assert snapshot.delta_token is None
    assert resp["cleared"]["snapshot_rows"] == 1
