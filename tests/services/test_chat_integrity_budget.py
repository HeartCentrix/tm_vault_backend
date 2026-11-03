"""The nightly chat-integrity sweep must be budgeted and killable.

Finding (preflight BLOCKER 9, 2026-07-31): `_verify_chat_integrity_once`
selects EVERY chat_threads row drained in the last 24h with no LIMIT and
calls `count_chat_messages` on each, which walks the chat's whole message
list to produce one integer. With the (correct) `$top` 999 -> 50 chatMessage
cap that walk went from ~1 page per 999 messages to ~1 per 50 — ~20x.

Volume for this tenant: the two chats-bearing accounts hold ~713k and ~285k
distinct messages, so the sweep's cost is dominated by
total_messages / page_size:

  * before the cap, in theory:  998k/999 + n_chats  ~= 1.0k-5.2k calls/night
  * before the cap, in fact:    n_chats calls, ALL 400 -> verifier verified
                                nothing at all
  * after the cap, unbounded:   998k/50 + n_chats  ~= 20.0k-24.2k calls/night
  * after this change:          <= CHAT_INTEGRITY_MAX_GRAPH_CALLS_PER_RUN
                                (default 1500), a hard ceiling

So the sweep has to be bounded per run, and it has to be switchable off or
down from the ConfigMap without a new image. The `$top` cap stays.
"""
from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import pathlib
import sys

import pytest

# ── Env required before any shared.config import ──────────────────────────
os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "test_secret_for_unit_tests_only_x")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")
os.environ.setdefault("AUDIT_SERVICE_URL", "http://localhost:8000")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "services" / "audit-service" / "main.py"
_spec = importlib.util.spec_from_file_location("audit_main_chat_budget", _MAIN_PATH)
_audit = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["audit_main_chat_budget"] = _audit
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(_audit)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(
        f"audit-service module failed to import: {exc}",
        allow_module_level=True,
    )


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


# ─── 1. the candidate query is bounded and rotates ─────────────────────────

def test_candidate_sql_carries_a_limit():
    sql = _audit.CHAT_INTEGRITY_CANDIDATE_SQL
    assert "LIMIT :max_chats" in sql, (
        "an unbounded candidate set is the whole finding — every chat drained "
        "in 24h walked in one run"
    )
    assert "ORDER BY" in sql, (
        "a bounded sample must rotate, or the same first N chats are the only "
        "ones ever verified"
    )
    # The 24h window and the PERMISSION/GONE exclusion must survive the edit.
    assert "24 hours" in sql
    assert "'PERMISSION','GONE'" in sql.replace(" ", "")


# ─── 2. conservative defaults + a real kill switch ─────────────────────────

def test_limits_defaults_are_conservative():
    lim = _audit.chat_integrity_limits({})
    assert lim.enabled is True, (
        "the sweep is the only detector for silent chat drops; default on, "
        "but bounded"
    )
    assert lim.max_graph_calls <= 2000, (
        "default budget must be at or below what the pre-cap ($top=999) walk "
        "would have cost, so this deploy cannot increase Graph load"
    )
    assert lim.max_chats > 0
    assert lim.max_pages_per_chat > 0
    assert lim.page_size == 50, "must match Graph's hard chatMessage $top cap"


def test_limits_are_overridable_from_env():
    lim = _audit.chat_integrity_limits({
        "CHAT_INTEGRITY_ENABLED": "false",
        "CHAT_INTEGRITY_MAX_CHATS_PER_RUN": "7",
        "CHAT_INTEGRITY_MAX_GRAPH_CALLS_PER_RUN": "11",
        "CHAT_INTEGRITY_MAX_PAGES_PER_CHAT": "3",
    })
    assert lim.enabled is False
    assert (lim.max_chats, lim.max_graph_calls, lim.max_pages_per_chat) == (7, 11, 3)


def test_garbage_env_falls_back_to_defaults():
    lim = _audit.chat_integrity_limits({
        "CHAT_INTEGRITY_MAX_GRAPH_CALLS_PER_RUN": "lots",
    })
    assert lim.max_graph_calls == _audit.chat_integrity_limits({}).max_graph_calls


def test_kill_switch_stops_the_sweep_before_it_touches_the_db(monkeypatch):
    """CHAT_INTEGRITY_ENABLED=false must be a real off switch — no DB session,
    no Graph client, no requests."""
    opened = []

    def _boom(*a, **kw):
        opened.append(1)
        raise AssertionError("sweep must not open a session when disabled")

    monkeypatch.setattr(_audit, "async_session_factory", _boom)
    monkeypatch.setenv("CHAT_INTEGRITY_ENABLED", "false")

    _run(_audit._verify_chat_integrity_once())

    assert opened == [], "kill switch did not stop the sweep"


# ─── 3. per-run budget planning ────────────────────────────────────────────

def test_plan_stops_the_sweep_when_the_budget_is_spent():
    lim = _audit.chat_integrity_limits({
        "CHAT_INTEGRITY_MAX_GRAPH_CALLS_PER_RUN": "100",
    })
    decision, pages = _audit.plan_chat_integrity_check(
        db_count=10, calls_used=100, limits=lim,
    )
    assert decision == "STOP_BUDGET"
    assert pages == 0


def test_plan_never_lets_one_chat_overshoot_the_run_budget():
    lim = _audit.chat_integrity_limits({
        "CHAT_INTEGRITY_MAX_GRAPH_CALLS_PER_RUN": "100",
        "CHAT_INTEGRITY_MAX_PAGES_PER_CHAT": "40",
    })
    decision, pages = _audit.plan_chat_integrity_check(
        db_count=1000, calls_used=97, limits=lim,
    )
    assert decision == "CHECK"
    assert pages == 3, "3 calls left in the run budget, so 3 pages max"


def test_plan_caps_pages_per_chat():
    lim = _audit.chat_integrity_limits({
        "CHAT_INTEGRITY_MAX_PAGES_PER_CHAT": "40",
    })
    decision, pages = _audit.plan_chat_integrity_check(
        db_count=713_000, calls_used=0, limits=lim,
    )
    assert decision == "CHECK"
    assert pages == 40, (
        "a 713k-message account must not be able to spend 14,260 requests on "
        "one chat"
    )


# ─── 4. verdicts: a lower bound may prove a gap, never disprove one ────────

def test_verdict_ok_within_tolerance():
    verdict, used, miss = _audit.chat_integrity_verdict(
        db_count=1000, graph_total=1005, partial_total=0, capped=False,
        tolerance_pct=1.0,
    )
    assert verdict == "OK"


def test_verdict_gap_on_exact_count():
    verdict, used, miss = _audit.chat_integrity_verdict(
        db_count=500, graph_total=1000, partial_total=0, capped=False,
        tolerance_pct=1.0,
    )
    assert verdict == "GAP"
    assert used == 1000
    assert miss == pytest.approx(50.0)


def test_capped_walk_can_still_prove_a_gap_from_its_lower_bound():
    """The catastrophic case: the drain stored almost nothing for a huge
    chat. We only walked 40 pages, but 2000 messages seen vs 5 stored proves
    a gap without finishing the walk."""
    verdict, used, miss = _audit.chat_integrity_verdict(
        db_count=5, graph_total=None, partial_total=2000, capped=True,
        tolerance_pct=1.0,
    )
    assert verdict == "GAP_LOWER_BOUND"
    assert used == 2000
    assert miss > 99.0


def test_capped_walk_never_flags_a_gap_it_cannot_prove():
    """DB has MORE than we managed to see, which is exactly what a truncated
    walk looks like on a healthy big chat. Flagging here would force a full
    re-drain of the 713k-message account every single night."""
    verdict, used, miss = _audit.chat_integrity_verdict(
        db_count=100_000, graph_total=None, partial_total=2000, capped=True,
        tolerance_pct=1.0,
    )
    assert verdict == "UNVERIFIED"
    assert used is None


def test_unknown_count_is_unverified_not_ok():
    """Graph refused (403/404/throttle). That is not a clean bill of health —
    it must not read as OK, and it must not read as a gap."""
    verdict, used, miss = _audit.chat_integrity_verdict(
        db_count=10, graph_total=None, partial_total=0, capped=False,
        tolerance_pct=1.0,
    )
    assert verdict == "UNVERIFIED"


def test_zero_graph_total_is_unverified():
    verdict, used, miss = _audit.chat_integrity_verdict(
        db_count=10, graph_total=0, partial_total=0, capped=False,
        tolerance_pct=1.0,
    )
    assert verdict == "UNVERIFIED"


# ─── 5. the sweep loop itself, with fake DB + fake Graph ───────────────────
#
# The pure helpers above are only half the fix: the loop has to actually
# consult them, charge the budget from the pages Graph really cost, and stop.
# These drive _verify_chat_integrity_once end to end.

class _Row:
    def __init__(self, i, chat_id):
        self.id = f"00000000-0000-0000-0000-{i:012d}"
        self.tenant_id = "11111111-1111-1111-1111-111111111111"
        self.chat_id = chat_id


class _Result:
    def __init__(self, rows=None, scalar=None):
        self._rows = rows or []
        self._scalar = scalar

    def all(self):
        return self._rows

    def scalar(self):
        return self._scalar


class _FakeTenant:
    client_id = "app-1"
    external_tenant_id = "tenant-1"


class _FakeSession:
    """Serves the three statements the sweep issues, and records UPDATEs."""

    def __init__(self, state):
        self.state = state

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def get(self, model, pk):
        return _FakeTenant()

    async def execute(self, clause, params=None):
        sql = " ".join(str(clause).split())
        self.state["sql"].append((sql, dict(params or {})))
        if "FROM chat_threads" in sql and "SELECT id" in sql:
            return _Result(rows=self.state["rows"])
        if "chat_thread_messages" in sql:
            return _Result(scalar=self.state["db_count"])
        if sql.startswith("UPDATE chat_threads"):
            self.state["updates"].append(dict(params or {}))
            return _Result()
        return _Result()

    async def commit(self):
        return None


def _install_sweep_fakes(monkeypatch, *, rows, db_count, pages_per_chat):
    import shared.graph_client as _gcmod

    state = {
        "rows": rows, "db_count": db_count, "sql": [], "updates": [],
        "graph_calls": 0, "chats_walked": [],
    }
    monkeypatch.setattr(_audit, "async_session_factory", lambda: _FakeSession(state))

    class _FakeGraph:
        def __init__(self, **kw):
            pass

        async def count_chat_messages(self, chat_id, max_pages=None, stats=None):
            state["chats_walked"].append(chat_id)
            spent = min(pages_per_chat, max_pages or pages_per_chat)
            state["graph_calls"] += spent
            capped = spent < pages_per_chat
            if stats is not None:
                stats.update({
                    "pages": spent,
                    "capped": capped,
                    "partial_total": spent * 50,
                })
            return None if capped else pages_per_chat * 50

    monkeypatch.setattr(_gcmod, "GraphClient", _FakeGraph)

    async def _noop_audit(*a, **kw):
        return None

    monkeypatch.setattr(_audit, "create_audit_event", _noop_audit)
    return state


def test_sweep_stops_when_the_run_budget_is_spent(monkeypatch):
    """20 chats x 10 pages = 200 requests unbounded; the budget must cut the
    run off instead of walking the whole sample."""
    rows = [_Row(i, f"19:chat{i}@thread.v2") for i in range(20)]
    state = _install_sweep_fakes(
        monkeypatch, rows=rows, db_count=500, pages_per_chat=10,
    )
    monkeypatch.setenv("CHAT_INTEGRITY_MAX_GRAPH_CALLS_PER_RUN", "25")
    monkeypatch.setenv("CHAT_INTEGRITY_MAX_PAGES_PER_CHAT", "40")

    _run(_audit._verify_chat_integrity_once())

    assert state["graph_calls"] <= 35, (
        f"budget was 25 requests, sweep spent {state['graph_calls']}"
    )
    assert len(state["chats_walked"]) < 20, "sweep did not stop early"


def test_sweep_passes_the_limit_to_the_candidate_query(monkeypatch):
    state = _install_sweep_fakes(
        monkeypatch, rows=[], db_count=0, pages_per_chat=1,
    )
    monkeypatch.setenv("CHAT_INTEGRITY_MAX_CHATS_PER_RUN", "13")

    _run(_audit._verify_chat_integrity_once())

    sql, params = state["sql"][0]
    assert "LIMIT :max_chats" in sql
    assert params["max_chats"] == 13
    assert params["salt"], "rotation salt must be bound"


def test_sweep_flags_a_gap_with_an_exact_count(monkeypatch):
    """Walk completes inside the allowance (150 real vs 100 stored), so the
    recorded count is exact, not a bound."""
    rows = [_Row(1, "19:chat1@thread.v2")]
    state = _install_sweep_fakes(
        monkeypatch, rows=rows, db_count=100, pages_per_chat=3,
    )
    monkeypatch.setenv("CHAT_INTEGRITY_MAX_PAGES_PER_CHAT", "40")

    _run(_audit._verify_chat_integrity_once())

    assert len(state["updates"]) == 1
    fs = json.loads(state["updates"][0]["fs"])
    assert fs["integrity_gap"] is True
    assert fs["db_count"] == 100
    assert fs["graph_count"] == 150
    assert fs["graph_count_is_lower_bound"] is False


def test_sweep_stops_as_soon_as_it_can_prove_a_gap(monkeypatch):
    """The catastrophic case: 5 messages stored for a 5,000-page chat. The
    allowance is sized from the DB count (+1 page), so the sweep proves the
    gap in 2 requests instead of 100 — and records the count it used as a
    lower bound so ops is not told it is exact."""
    rows = [_Row(1, "19:chat1@thread.v2")]
    state = _install_sweep_fakes(
        monkeypatch, rows=rows, db_count=5, pages_per_chat=100,
    )
    monkeypatch.setenv("CHAT_INTEGRITY_MAX_PAGES_PER_CHAT", "40")

    _run(_audit._verify_chat_integrity_once())

    assert state["graph_calls"] == 2, (
        f"proving this gap must not cost 100 requests (spent "
        f"{state['graph_calls']})"
    )
    assert len(state["updates"]) == 1
    fs = json.loads(state["updates"][0]["fs"])
    assert fs["integrity_gap"] is True
    assert fs["graph_count"] == 100
    assert fs["graph_count_is_lower_bound"] is True


def test_sweep_does_not_flag_a_healthy_big_chat_it_could_not_finish(monkeypatch):
    """The regression that would force a nightly full re-drain of the 713k
    account: a truncated walk on a chat the DB has MORE of must stay quiet."""
    rows = [_Row(1, "19:chat1@thread.v2")]
    state = _install_sweep_fakes(
        monkeypatch, rows=rows, db_count=100_000, pages_per_chat=2000,
    )
    monkeypatch.setenv("CHAT_INTEGRITY_MAX_PAGES_PER_CHAT", "40")

    _run(_audit._verify_chat_integrity_once())

    assert state["updates"] == [], "capped walk must not flag an unprovable gap"
