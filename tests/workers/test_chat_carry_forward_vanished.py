"""Regression: chat carry-forward must abort cleanly when the target
snapshot is deleted out from under it.

Observed on Taylor Morrison prod (2026-06-22): a USER_CHATS snapshot was
cleaned up (stale-snapshot GC / cancel-revert / a superseding concurrent
drain of the same user) while _carry_forward_prior_chat_snapshot_items was
still running. The raw text() INSERT...SELECT hit
`snapshot_items_snapshot_id_fkey` and surfaced as an unhandled
ForeignKeyViolationError:

    Chats — Jason Rue: carry-forward failed: ForeignKeyViolationError …
    Key (snapshot_id)=(fa74399c…) is not present in table "snapshots"

Every other snapshot_items writer (_bulk_upsert_snapshot_items) already
converts that exact FK violation into SnapshotVanishedError so handlers
abort cleanly. Carry-forward was the one writer bypassing the contract.
This test pins the contract: FK-vanished → SnapshotVanishedError; any
unrelated IntegrityError re-raises unchanged (no over-broad swallow).

Loads workers/backup-worker/main.py via importlib (hyphen path) — same
pattern as test_chat_sparse_incremental.py.
"""
from __future__ import annotations

import asyncio
import importlib.util
import os
import pathlib
import sys

import pytest
from sqlalchemy.exc import IntegrityError

# Ephemeral unit run that never touches real user data — sanctioned dev path
# (same as tests/shared/test_tier2_discovery_parallelism.py). Without this the
# worker module aborts at import on the JWT_SECRET guard and the test skips.
os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")

_MAIN_PATH = (
    pathlib.Path(__file__).resolve().parents[2]
    / "workers" / "backup-worker" / "main.py"
)
_spec = importlib.util.spec_from_file_location("bw_main_carry_forward", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_main_carry_forward"] = _bw
try:
    _spec.loader.exec_module(_bw)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover
    pytest.skip(
        f"backup-worker module failed to import: {exc}",
        allow_module_level=True,
    )


class _FakeSession:
    """Async-context session stand-in whose execute() raises a chosen error."""

    def __init__(self, err):
        self._err = err
        self.rolled_back = False
        self.committed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def execute(self, *a, **k):
        raise self._err

    async def rollback(self):
        self.rolled_back = True

    async def commit(self):
        self.committed = True


def _fk_integrity_error():
    # str(e) must contain the FK constraint name — that's what the writer
    # matches on (robust across asyncpg / driver wording).
    return IntegrityError(
        "INSERT INTO snapshot_items ... violates foreign key constraint "
        "\"snapshot_items_snapshot_id_fkey\"",
        {},
        Exception("snapshot_items_snapshot_id_fkey"),
    )


def test_carry_forward_raises_snapshot_vanished_on_fk_violation(monkeypatch):
    fake = _FakeSession(_fk_integrity_error())
    monkeypatch.setattr(_bw, "async_session_factory", lambda: fake)

    with pytest.raises(_bw.SnapshotVanishedError):
        asyncio.run(
            _bw._carry_forward_prior_chat_snapshot_items(
                tenant_id="11111111-1111-1111-1111-111111111111",
                resource_id="5f91bb24-9d7a-459f-a02a-a9517e88751d",
                target_snapshot_id="fa74399c-0063-408e-b285-3ef6aa854c9b",
            )
        )
    assert fake.rolled_back, "must roll back the aborted txn before re-raising"


def test_carry_forward_reraises_unrelated_integrity_error(monkeypatch):
    # A different constraint (e.g. the unique idempotency guard) must NOT be
    # misclassified as a vanished snapshot — it re-raises unchanged.
    dup = IntegrityError(
        "INSERT INTO snapshot_items ... violates unique constraint "
        "\"uq_snapshot_items_snap_ext_type\"",
        {},
        Exception("uq_snapshot_items_snap_ext_type"),
    )
    monkeypatch.setattr(_bw, "async_session_factory", lambda: _FakeSession(dup))

    with pytest.raises(IntegrityError):
        asyncio.run(
            _bw._carry_forward_prior_chat_snapshot_items(
                tenant_id="11111111-1111-1111-1111-111111111111",
                resource_id="5f91bb24-9d7a-459f-a02a-a9517e88751d",
                target_snapshot_id="fa74399c-0063-408e-b285-3ef6aa854c9b",
            )
        )
