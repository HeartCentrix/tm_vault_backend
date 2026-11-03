"""chat_threads skip-claim must never be satisfied by a FAILED drain.

Bug (adversarial review, 2026-07-31): two writes conspired to lose Teams
messages permanently.

  1. ``_claim_or_load_chat_thread``'s UPSERT stamped
     ``chat_threads.last_updated_at`` with the FRESH Graph
     ``lastUpdatedDateTime`` before a single message was drained.
  2. ``_mark_chat_thread_drained`` set ``last_drained_at = NOW()`` even
     when ``drain_succeeded=False``.

The drain-skip path compares those two columns. So: run 1 sees new
activity, claims, 403s / 429-storms out, records the failure — but has
already advanced the observed-change watermark to the new timestamp and
refreshed last_drained_at. Run 2 sees watermark == Graph timestamp and a
fresh last_drained_at, so it skips the chat. The messages that arrived
between the two runs are never fetched again.

These tests drive the real helpers against an in-memory emulation of the
two statements they issue, so the skip decision can only be satisfied by
a SUCCESSFUL drain of the observed change.
"""
from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import pathlib
import re
import sys
from datetime import datetime, timedelta, timezone

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "workers" / "backup-worker" / "main.py"
_spec = importlib.util.spec_from_file_location("bw_chat_claim", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_chat_claim"] = _bw
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(_bw)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(
        f"backup-worker module failed to import: {exc}",
        allow_module_level=True,
    )


TENANT = "11111111-1111-1111-1111-111111111111"
CHAT = "19:aaaaaaaabbbbbbbbccccccccdddddddd@thread.v2"
THREAD_ID = "22222222-2222-2222-2222-222222222222"

T0 = "2026-07-28T10:00:00Z"   # watermark from the last successful drain
T1 = "2026-07-30T10:00:00Z"   # Graph reports new activity here


def _dt(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


# ─── in-memory emulation of the two statements the helpers issue ───────────

def _split_assignments(set_clause: str):
    """Split a SET clause on top-level commas — function-call arguments
    (GREATEST(a, b)) are not assignment boundaries."""
    parts, depth, cur = [], 0, []
    for ch in set_clause:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(cur))
            cur = []
            continue
        cur.append(ch)
    parts.append("".join(cur))
    return [p for p in parts if p.strip()]


class _Row:
    def __init__(self, mapping):
        for k, v in mapping.items():
            setattr(self, k, v)


class _Result:
    def __init__(self, row):
        self._row = row

    def first(self):
        return self._row


class _FakeChatThreads:
    """One chat_threads row + the statement semantics the helpers rely on.

    The UPSERT / UPDATE effects are derived from the SQL the helper
    actually emits (which columns its SET clause assigns), so a helper
    that advances a column it shouldn't is observable here exactly as it
    would be in Postgres.
    """

    def __init__(self, row=None, max_msg_ts=None):
        self.row = row  # None → no existing row
        self.max_msg_ts = max_msg_ts
        self.statements = []

    # -- statement dispatch ------------------------------------------------
    async def run(self, sql, params):
        self.statements.append((sql, params))
        if "INSERT INTO chat_threads" in sql:
            return _Result(self._upsert(sql, params))
        if sql.strip().startswith("UPDATE chat_threads"):
            self._update(sql, params)
            return _Result(None)
        if "MAX(created_date_time)" in sql:
            return _Result(_Row({"mx": self.max_msg_ts}))
        raise AssertionError(f"unexpected statement: {sql[:80]}")

    # -- WITH prior … INSERT … ON CONFLICT DO UPDATE … RETURNING ----------
    def _upsert(self, sql, params):
        prior_lu = self.row.get("last_updated_at") if self.row else None
        if self.row is None:
            self.row = {
                "id": THREAD_ID,
                "drain_cursor": None,
                "last_updated_at": params.get("lu"),
                "drain_failure_state": {},
                "last_drained_at": None,
                "last_drained_msg_count": 0,
            }
            inserted = True
        else:
            inserted = False
            set_clause = sql.split("DO UPDATE SET", 1)[1].split("RETURNING", 1)[0]
            # Postgres only moves a column the SET clause names.
            if re.search(r"\blast_updated_at\s*=", set_clause):
                if params.get("lu") is not None:
                    self.row["last_updated_at"] = params["lu"]
        return _Row({
            "id": self.row["id"],
            "drain_cursor": self.row["drain_cursor"],
            "last_updated_at": self.row["last_updated_at"],
            "drain_failure_state": self.row["drain_failure_state"],
            "last_drained_at": self.row["last_drained_at"],
            "last_drained_msg_count": self.row["last_drained_msg_count"],
            "inserted": inserted,
            "prior_lu": prior_lu,
        })

    # -- UPDATE chat_threads SET <dynamic parts> WHERE id = :tid ----------
    def _update(self, sql, params):
        assert self.row is not None
        set_clause = sql.split("SET", 1)[1].split("WHERE", 1)[0]
        now = datetime.now(timezone.utc)
        for part in _split_assignments(set_clause):
            if "=" not in part:
                continue
            col, expr = part.split("=", 1)
            col, expr = col.strip(), expr.strip()
            if col not in (
                "last_drained_at", "updated_at", "drain_cursor",
                "drain_failure_state", "last_drained_msg_count",
                "last_updated_at",
            ):
                raise AssertionError(f"unexpected SET column: {col}")
            bind = re.search(r":(\w+)", expr)
            if bind:
                val = params[bind.group(1)]
                if col == "drain_failure_state" and isinstance(val, str):
                    val = json.loads(val)
                if "GREATEST" in expr.upper():
                    cur = self.row.get(col)
                    val = max(v for v in (cur, val) if v is not None)
                self.row[col] = val
            elif "NOW()" in expr.upper():
                self.row[col] = now


class _FakeSession:
    def __init__(self, db):
        self._db = db

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def execute(self, clause, params=None):
        return await self._db.run(str(clause), params or {})

    async def commit(self):
        return None


@pytest.fixture
def db(monkeypatch):
    holder = {}

    def _factory():
        return _FakeSession(holder["db"])

    monkeypatch.setattr(_bw, "async_session_factory", _factory)
    return holder


def _drained_row(*, watermark, drained_ago_s, cursor=T0, failure=None):
    return {
        "id": THREAD_ID,
        "drain_cursor": cursor,
        "last_updated_at": _dt(watermark) if watermark else None,
        "drain_failure_state": failure or {},
        "last_drained_at": (
            datetime.now(timezone.utc) - timedelta(seconds=drained_ago_s)
        ),
        "last_drained_msg_count": 500,
    }



def _claim(observed_lu):
    return asyncio.run(_bw._claim_or_load_chat_thread(
        tenant_id=TENANT, chat_id=CHAT, chat_type="group",
        chat_topic=None, member_names=[], last_updated_at_iso=observed_lu,
    ))


def _mark(**kw):
    return asyncio.run(_bw._mark_chat_thread_drained(thread_id=THREAD_ID, **kw))


# ─── the regression: a failed drain may not satisfy the next skip ─────────

def test_failed_drain_leaves_chat_claimable_next_run(db):
    """Run 1 claims on new activity and its drain FAILS. Run 2 must claim
    the same chat again — the pre-fix code skipped it forever."""
    db["db"] = _FakeChatThreads(
        row=_drained_row(watermark=T0, drained_ago_s=60),
    )

    first = _claim(T1)
    assert first["claimed"] is True, "new Graph activity must win the claim"

    # Drain failed: no cursor advance, failure recorded.
    _mark(
        drain_cursor=None,
        failure_state={"count": 1, "error_class": "THROTTLE"},
        drain_succeeded=False,
        observed_last_updated_at=T1,
    )

    second = _claim(T1)
    assert second["claimed"] is True, (
        "a chat whose drain failed must be re-claimed, not skipped"
    )


def test_failed_drain_does_not_bump_last_drained_at(db):
    """last_drained_at is the freshness clock other backups skip on. A
    failed drain may not refresh it."""
    fake = _FakeChatThreads(row=_drained_row(watermark=T0, drained_ago_s=99))
    db["db"] = fake
    before = fake.row["last_drained_at"]

    _mark(
        drain_cursor=None,
        failure_state={"count": 1, "error_class": "TRANSIENT"},
        drain_succeeded=False,
    )

    assert fake.row["last_drained_at"] == before
    assert fake.row["drain_failure_state"]["count"] == 1


def test_failed_drain_does_not_advance_watermark(db):
    fake = _FakeChatThreads(row=_drained_row(watermark=T0, drained_ago_s=99))
    db["db"] = fake

    _mark(
        drain_cursor=None,
        failure_state={"count": 1, "error_class": "TRANSIENT"},
        drain_succeeded=False,
        observed_last_updated_at=T1,
    )

    assert fake.row["last_updated_at"] == _dt(T0)


def test_claim_does_not_advance_watermark_before_draining(db):
    """The claim itself must leave the observed-change watermark alone —
    it is the record of what has been SUCCESSFULLY drained."""
    fake = _FakeChatThreads(row=_drained_row(watermark=T0, drained_ago_s=60))
    db["db"] = fake

    _claim(T1)

    assert fake.row["last_updated_at"] == _dt(T0)


# ─── successful drains still short-circuit the next run ───────────────────

def test_successful_drain_advances_watermark_and_clock(db):
    fake = _FakeChatThreads(row=_drained_row(watermark=T0, drained_ago_s=99))
    db["db"] = fake

    _mark(
        drain_cursor=T1, failure_state={}, msg_count=510,
        drain_succeeded=True, observed_last_updated_at=T1,
    )

    assert fake.row["last_updated_at"] == _dt(T1)
    assert fake.row["drain_cursor"] == T1
    assert fake.row["last_drained_msg_count"] == 510
    assert (
        datetime.now(timezone.utc) - fake.row["last_drained_at"]
    ).total_seconds() < 30


def test_successful_drain_then_no_new_activity_skips(db):
    """The freshness short-circuit must still work — that's the whole
    point of the tenant-singleton store."""
    fake = _FakeChatThreads(row=_drained_row(watermark=T0, drained_ago_s=99))
    db["db"] = fake

    _mark(
        drain_cursor=T1, failure_state={},
        drain_succeeded=True, observed_last_updated_at=T1,
    )

    again = _claim(T1)
    assert again["claimed"] is False, (
        "a successfully-drained chat with no new activity must skip"
    )


def test_activity_after_a_successful_drain_reclaims(db):
    fake = _FakeChatThreads(row=_drained_row(watermark=T0, drained_ago_s=60))
    db["db"] = fake

    _mark(
        drain_cursor=T0, failure_state={},
        drain_succeeded=True, observed_last_updated_at=T0,
    )
    assert _claim(T1)["claimed"] is True


def test_stale_clock_still_reclaims(db):
    """Older than CHAT_THREAD_DRAIN_FRESHNESS_S → plain time-based
    re-drain, even with no new activity."""
    db["db"] = _FakeChatThreads(
        row=_drained_row(
            watermark=T1,
            drained_ago_s=_bw._CHAT_THREAD_DRAIN_FRESHNESS_S + 600,
        ),
    )
    assert _claim(T1)["claimed"] is True


def test_first_ever_row_claims(db):
    db["db"] = _FakeChatThreads(row=None)
    out = _claim(T1)
    assert out["claimed"] is True
    assert out["thread_id"] == THREAD_ID


def test_missing_watermark_with_fresh_clock_still_claims(db):
    """A row drained by an older build that never stored a watermark must
    fail SAFE (re-drain), not skip."""
    db["db"] = _FakeChatThreads(
        row=_drained_row(watermark=None, drained_ago_s=60),
    )
    assert _claim(T1)["claimed"] is True
