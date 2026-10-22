"""USER_CHATS admission is bounded, and bounding it changes nothing else.

Prod, 2026-08-11 11:15 tick, worker-d1c55967:

    [USER_CHATS] [shard 4/6] Chats — John Lucas: 640/806 chats, 45 msgs so
      far (skipped chat 19:meeti — no activity since cursor)
    [USER_CHATS] fetched 45 messages across 806 chats in 2747.8s (0.0 msg/s)

806 chats, 2,747.8s, FORTY-FIVE messages. Of the shard's 75 progress lines
67 were the skip-by-activity branch and 8 were claim-refused — so almost
every chat returned before its first Graph call, and Graph is not where the
time went. Measured instead:

  * the queries are free. 200 real chat_threads ids / 17,071
    chat_thread_messages rows: claim-read p50 0.20ms, pointer-read p50
    0.29ms, 0.11s for the whole sample.
  * the shard's ceiling is the 6-permit bookkeeping gate: 806 chats x 2
    gated sessions / 6 permits = 269 serialized passes; the drain loop's
    own progress lines span 2,484s, so each gated DB op took ~9.2s —
    ~31,000x the measured query time. The residue is connection-checkout
    wait, and the shard logged it: `QueuePool limit of size 40 overflow 40
    reached, connection timed out, timeout 30.00` plus 51 asyncpg
    `ConnectionDoesNotExistError: connection was closed in the middle of
    operation`.

Why the pool ran dry. `asyncio.gather` over the whole chat list started 806
coroutines at t=0 and each opened a chat_threads claim session immediately.
`USER_CHATS_PARALLEL_CHATS` (`per_chat_sem`, 32) does not bound that — it is
acquired around the Graph page loop, far below the claim. 94291b9 gated the
pointer read and the pointer upsert but left the claim and the drain
checkpoint ungated, so 2 of the 4 sessions a skipped chat opens were still
unbounded.

A/B against the prod pooler, 806 real thread ids, pool 40+40, gate 6:

    unbounded fan-out, 2 of 4 gated   peak checked-out conns 80/80  p50 6516ms
    bounded fan-out,   4 of 4 gated   peak checked-out conns  6     p50  698ms

The earlier attempt at this bug was rejected for hoisting the skip test into
a pre-pass that ran BEFORE `_claim_or_load_chat_thread`, which silently ate
the claim-refused branch — a different branch with a side effect the skip
branch does not have. So the tests below spend most of their effort proving
the opposite property: admission order changes, branch selection does not.
"""
from __future__ import annotations

import asyncio
import importlib.util
import os
import pathlib
import sys

import pytest

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_MAIN_PATH = _REPO_ROOT / "workers" / "backup-worker" / "main.py"
_spec = importlib.util.spec_from_file_location("bw_chat_inflight_uut", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_chat_inflight_uut"] = _bw
assert _spec and _spec.loader
try:
    _spec.loader.exec_module(_bw)  # type: ignore[union-attr]
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(
        f"backup-worker module failed to import: {exc}",
        allow_module_level=True,
    )


def _run(coro):
    """Each test gets a fresh loop: _chat_pointer_db_gate() memoises a
    Semaphore onto the module, and a Semaphore bound to a closed loop
    poisons every later test."""
    _bw._chat_pointer_db_sem = None
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()
        _bw._chat_pointer_db_sem = None


# ─── the admission width ─────────────────────────────────────────────────

def test_default_admission_is_wider_than_the_graph_fanout():
    """A chat that reaches Graph holds its slot for the whole paginated
    drain. Sizing admission at exactly the Graph ceiling would let 32 slow
    drains starve every cheap skip queued behind them."""
    assert _bw.chat_drain_inflight_limit(None, 32) == 64
    assert _bw.chat_drain_inflight_limit(None, 8) == 16


def test_admission_is_finite_for_every_input():
    """The bug was an infinite window (one coroutine per chat). No input may
    reproduce it."""
    for raw in (None, "", "  ", "abc", "0", "-5", "1", "999999"):
        for fan in (0, 1, 32):
            n = _bw.chat_drain_inflight_limit(raw, fan)
            assert isinstance(n, int) and 1 <= n <= 999999


def test_explicit_override_is_honoured():
    assert _bw.chat_drain_inflight_limit("100", 32) == 100
    assert _bw.chat_drain_inflight_limit(" 96 ", 32) == 96


def test_override_below_the_graph_fanout_is_clamped_up():
    """USER_CHATS_INFLIGHT_CHATS is a pool-pressure knob. Letting it drop
    under USER_CHATS_PARALLEL_CHATS would silently cut Graph concurrency,
    which is a different knob with a different failure mode."""
    assert _bw.chat_drain_inflight_limit("4", 32) == 32
    assert _bw.chat_drain_inflight_limit("0", 32) == 32
    assert _bw.chat_drain_inflight_limit("-9", 32) == 32


def test_unparseable_override_falls_back_to_the_default():
    """A typo'd env var must not fail a backup, and must not fall back to
    unbounded."""
    assert _bw.chat_drain_inflight_limit("thirty-two", 32) == 64
    assert _bw.chat_drain_inflight_limit("", 32) == 64


# ─── the runner actually bounds, and preserves the gather contract ───────

def _tracking_drain(limit_probe):
    """A stand-in _drain_one_chat that records peak concurrency and the
    order it was called in."""
    state = {"cur": 0, "peak": 0, "calls": []}

    async def drain(cid):
        state["calls"].append(cid)
        state["cur"] += 1
        state["peak"] = max(state["peak"], state["cur"])
        # Two suspension points so the scheduler has real opportunities to
        # over-admit if nothing is holding it back.
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        state["cur"] -= 1
        return (cid, [], None)

    limit_probe.update(state)
    return drain, state


def test_runner_never_exceeds_the_limit():
    state_holder: dict = {}
    drain, state = _tracking_drain(state_holder)
    ids = [f"19:chat{i}" for i in range(806)]
    res = _run(_bw.run_chat_drains_bounded(ids, drain, limit=64))
    assert state["peak"] <= 64, f"admitted {state['peak']} chats at once"
    assert len(res) == 806


def test_unbounded_gather_would_have_admitted_every_chat():
    """Premise guard: without the bound the same workload really does put
    all 806 in flight, which is the 806-simultaneous-claim-sessions shape
    that drained DB_POOL_SIZE(40)+DB_MAX_OVERFLOW(40)."""
    state_holder: dict = {}
    drain, state = _tracking_drain(state_holder)
    ids = [f"19:chat{i}" for i in range(806)]

    async def unbounded():
        return await asyncio.gather(*[drain(c) for c in ids],
                                    return_exceptions=True)

    _run(unbounded())
    assert state["peak"] == 806


def test_every_chat_is_invoked_exactly_once():
    state_holder: dict = {}
    drain, state = _tracking_drain(state_holder)
    ids = [f"19:chat{i}" for i in range(200)]
    _run(_bw.run_chat_drains_bounded(ids, drain, limit=16))
    assert sorted(state["calls"]) == sorted(ids)
    assert len(state["calls"]) == len(set(state["calls"]))


def test_results_come_back_in_input_order():
    """The caller zips results back onto chats positionally
    (`cid, msgs_local, delta_out = r`), so order is load-bearing."""
    ids = [f"19:chat{i}" for i in range(120)]

    async def drain(cid):
        # Deliberately finish out of order.
        await asyncio.sleep((hash(cid) % 5) / 1000.0)
        return (cid, [], None)

    res = _run(_bw.run_chat_drains_bounded(ids, drain, limit=8))
    assert [r[0] for r in res] == ids


def test_one_chat_raising_does_not_cancel_the_rest():
    """Same contract as gather(return_exceptions=True): the caller counts a
    raised chat into _chat_outcomes['failed'] and keeps the others."""
    ids = [f"19:chat{i}" for i in range(50)]

    async def drain(cid):
        if cid == "19:chat7":
            raise RuntimeError("graph exploded")
        return (cid, [], None)

    res = _run(_bw.run_chat_drains_bounded(ids, drain, limit=4))
    assert isinstance(res[7], Exception)
    assert sum(1 for r in res if isinstance(r, Exception)) == 1
    assert all(r[0] == c for r, c in zip(res, ids) if not isinstance(r, Exception))


def test_empty_chat_list_is_not_an_error():
    assert _run(_bw.run_chat_drains_bounded([], lambda c: None, limit=8)) == []


# ─── admission does not reroute anything ─────────────────────────────────

def _branch_for(cid: str, drained_by_someone_else: set) -> str:
    """Stand-in for _drain_one_chat's branch ladder. The real one decides
    from the chat's own inputs (claim result, failure_state, chat_lu vs
    saved_cursor) — never from how many siblings are in flight."""
    if cid in drained_by_someone_else:
        return "claim-refused"
    if cid.startswith("19:meeting_gone"):
        return "smart-skip"
    if cid.endswith("-quiet"):
        return "skip-by-activity"
    return "drained"


def test_bounding_does_not_change_which_branch_any_chat_takes():
    """The property the rejected first attempt broke. It hoisted the skip
    test into a pre-pass ahead of the claim, so chats that should have taken
    the claim-refused branch took the skip branch instead — a silent
    data-completeness hole. An admission bound cannot do that: the branch is
    computed inside the per-chat body from that chat's own inputs."""
    ids = (
        [f"19:chat{i}-quiet" for i in range(300)]
        + [f"19:meeting_gone{i}" for i in range(60)]
        + [f"19:chat{i}-live" for i in range(120)]
    )
    refused = {f"19:chat{i}-quiet" for i in range(0, 300, 7)}

    def make():
        seen: list = []

        async def drain(cid):
            await asyncio.sleep(0)
            b = _branch_for(cid, refused)
            seen.append((cid, b))
            await asyncio.sleep(0)
            return (cid, [], b)

        return drain, seen

    d1, seen_unbounded = make()

    async def unbounded():
        return await asyncio.gather(*[d1(c) for c in ids],
                                    return_exceptions=True)

    res_unbounded = _run(unbounded())

    d2, seen_bounded = make()
    res_bounded = _run(_bw.run_chat_drains_bounded(ids, d2, limit=64))

    assert dict(seen_bounded) == dict(seen_unbounded)
    assert [r[2] for r in res_bounded] == [r[2] for r in res_unbounded]
    # and the branch mix is the real one, not all-one-branch
    mix = {}
    for _, b in seen_bounded:
        mix[b] = mix.get(b, 0) + 1
    assert mix == {
        "skip-by-activity": 300 - len(refused),
        "claim-refused": len(refused),
        "smart-skip": 60,
        "drained": 120,
    }


def test_every_chat_still_reaches_the_claim_step():
    """The claim is not just a branch selector — winning or losing it has
    side effects (the chat_threads upsert, the P7 sanity check). Admission
    may delay a chat; it may never let one skip the claim."""
    ids = [f"19:chat{i}" for i in range(400)]
    claims: list = []

    async def drain(cid):
        claims.append(cid)          # stands in for _claim_or_load_chat_thread
        await asyncio.sleep(0)
        return (cid, [], None)

    _run(_bw.run_chat_drains_bounded(ids, drain, limit=32))
    assert sorted(claims) == sorted(ids)


# ─── the gate now covers all four bookkeeping sessions ───────────────────

class _FakeResult:
    def first(self):
        return None

    def all(self):
        return []


class _FakeSession:
    """Counts how many sessions are simultaneously open — i.e. how many
    pooled connections this path would be asking QueuePool for."""

    def __init__(self, live):
        self._live = live

    async def __aenter__(self):
        self._live["cur"] += 1
        self._live["peak"] = max(self._live["peak"], self._live["cur"])
        return self

    async def __aexit__(self, *exc):
        self._live["cur"] -= 1
        return False

    async def execute(self, *a, **kw):
        # A real checkout+statement suspends; without a suspension point
        # nothing can overlap and the test would pass vacuously.
        await asyncio.sleep(0)
        return _FakeResult()

    async def commit(self):
        await asyncio.sleep(0)


def _patch_sessions(monkeypatch):
    live = {"cur": 0, "peak": 0}
    monkeypatch.setattr(_bw, "async_session_factory", lambda: _FakeSession(live))
    return live


def test_claim_holds_at_most_gate_many_sessions(monkeypatch):
    """Pre-fix this peaked at the number of concurrent claims (806 on the
    John Lucas shard) against a pool of 40+40."""
    from shared.config import settings
    live = _patch_sessions(monkeypatch)

    async def main():
        await asyncio.gather(*[
            _bw._claim_or_load_chat_thread(
                tenant_id="t", chat_id=f"19:c{i}", chat_type="group",
                chat_topic=None, member_names=[], last_updated_at_iso=None,
            )
            for i in range(200)
        ])

    _run(main())
    assert live["peak"] <= settings.chat_pointer_db_concurrency, (
        f"claim opened {live['peak']} concurrent DB sessions; the gate is "
        f"{settings.chat_pointer_db_concurrency}"
    )
    assert live["peak"] > 0, "test never opened a session — fake not wired"


def test_checkpoint_holds_at_most_gate_many_sessions(monkeypatch):
    from shared.config import settings
    live = _patch_sessions(monkeypatch)

    async def main():
        await asyncio.gather(*[
            _bw._mark_chat_thread_drained(
                thread_id=f"00000000-0000-0000-0000-{i:012d}",
                drain_cursor=None, failure_state=None, drain_succeeded=True,
            )
            for i in range(200)
        ])

    _run(main())
    assert live["peak"] <= settings.chat_pointer_db_concurrency, (
        f"checkpoint opened {live['peak']} concurrent DB sessions; the gate "
        f"is {settings.chat_pointer_db_concurrency}"
    )
    assert live["peak"] > 0, "test never opened a session — fake not wired"


def test_claim_and_checkpoint_share_one_process_wide_ceiling(monkeypatch):
    """A pod runs several user drains at once (Corp_Esplanade and John Lucas
    overlapped in worker-d1c55967). Per-shard bounds would not have stopped
    the pool exhaustion; the ceiling has to be process-wide, and both halves
    have to share it."""
    from shared.config import settings
    live = _patch_sessions(monkeypatch)

    async def main():
        await asyncio.gather(
            *[
                _bw._claim_or_load_chat_thread(
                    tenant_id="t", chat_id=f"19:a{i}", chat_type=None,
                    chat_topic=None, member_names=None,
                    last_updated_at_iso=None,
                ) for i in range(150)
            ],
            *[
                _bw._mark_chat_thread_drained(
                    thread_id=f"00000000-0000-0000-0000-{i:012d}",
                    drain_cursor="2026-08-11T00:00:00Z",
                    failure_state=None, drain_succeeded=True,
                ) for i in range(150)
            ],
        )

    _run(main())
    assert live["peak"] <= settings.chat_pointer_db_concurrency


# ─── the call site actually uses the bounded runner ──────────────────────

def _drain_chats_parallel_ast():
    """`_drain_chats_parallel` is a closure ~9,150 lines into a handler that
    needs Graph, RabbitMQ, a tenant and a live snapshot to reach, so no unit
    test can call it. This walks the parse tree instead.

    Structural, not textual, and deliberately so: the earlier attempt at this
    bug shipped tests that matched source strings and a comment, and a
    reviewer kept them green while mutating the logic underneath. An AST
    assertion about WHICH callable the fan-out site invokes cannot be
    satisfied by a comment, and flips the moment the call is swapped back to
    a bare gather — verified by mutation, see the module docstring.
    """
    import ast
    tree = ast.parse(_MAIN_PATH.read_text())
    for node in ast.walk(tree):
        if (isinstance(node, ast.AsyncFunctionDef)
                and node.name == "_drain_chats_parallel"):
            return node
    raise AssertionError("_drain_chats_parallel no longer exists")


def _called_names(node):
    import ast
    out = set()
    for n in ast.walk(node):
        if isinstance(n, ast.Call):
            f = n.func
            if isinstance(f, ast.Name):
                out.add(f.id)
            elif isinstance(f, ast.Attribute):
                base = f.value
                prefix = base.id + "." if isinstance(base, ast.Name) else ""
                out.add(prefix + f.attr)
    return out


def test_chat_fanout_site_routes_through_the_bounded_runner():
    names = _called_names(_drain_chats_parallel_ast())
    assert "run_chat_drains_bounded" in names, (
        "the USER_CHATS fan-out no longer goes through the bounded runner — "
        "an unbounded fan-out is what put 806 claim sessions against a "
        "40+40 pool on 2026-08-11"
    )


def test_chat_fanout_site_does_not_also_gather_unbounded():
    names = _called_names(_drain_chats_parallel_ast())
    assert "asyncio.gather" not in names, (
        "a bare asyncio.gather is back in the chat fan-out site; that is the "
        "regression shape exactly"
    )


def test_the_bounded_runner_is_given_a_finite_limit():
    """A `limit=` that came from something unbounded would satisfy the two
    tests above while restoring the bug."""
    import ast
    node = _drain_chats_parallel_ast()
    call = next(
        n for n in ast.walk(node)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Name)
        and n.func.id == "run_chat_drains_bounded"
    )
    kw = {k.arg: k.value for k in call.keywords}
    assert "limit" in kw, "run_chat_drains_bounded called without limit="
    assert isinstance(kw["limit"], ast.Name), (
        "limit= must be the precomputed admission window"
    )
    assert kw["limit"].id == "_chat_inflight_limit"
    # and that name must be produced by the clamping helper, not len(chats)
    src = _MAIN_PATH.read_text()
    handler = ast.parse(src)
    assigns = [
        n for n in ast.walk(handler)
        if isinstance(n, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == "_chat_inflight_limit"
                for t in n.targets)
    ]
    assert assigns, "_chat_inflight_limit is never assigned"
    assert all(
        isinstance(a.value, ast.Call)
        and isinstance(a.value.func, ast.Name)
        and a.value.func.id == "chat_drain_inflight_limit"
        for a in assigns
    ), "_chat_inflight_limit must come from chat_drain_inflight_limit()"


def test_gating_the_claim_does_not_change_any_claim_outcome(monkeypatch):
    """The gate must change only WHEN a claim runs. Runs the same rows
    through concurrently and asserts every returned decision matches what
    the row dictates."""
    import datetime as _dt

    class _Row:
        def __init__(self, i):
            self.id = f"00000000-0000-0000-0000-{i:012d}"
            self.drain_cursor = f"cursor-{i}"
            self.last_updated_at = None
            self.drain_failure_state = {"count": i % 3}
            self.last_drained_msg_count = i
            # even rows: never drained -> claim. odd rows: drained just now
            # with no new activity -> skip.
            self.last_drained_at = (
                None if i % 2 == 0 else _dt.datetime.utcnow()
            )
            self.inserted = False
            self.prior_lu = None

    class _Res:
        def __init__(self, row):
            self._row = row

        def first(self):
            return self._row

    live = {"cur": 0, "peak": 0}

    class _Sess(_FakeSession):
        def __init__(self, idx):
            super().__init__(live)
            self._idx = idx

        async def execute(self, *a, **kw):
            await asyncio.sleep(0)
            return _Res(_Row(self._idx))

    counter = {"n": -1}

    def _factory():
        counter["n"] += 1
        return _Sess(counter["n"])

    monkeypatch.setattr(_bw, "async_session_factory", _factory)

    async def main():
        return await asyncio.gather(*[
            _bw._claim_or_load_chat_thread(
                tenant_id="t", chat_id=f"19:c{i}", chat_type="group",
                chat_topic=None, member_names=[], last_updated_at_iso=None,
            ) for i in range(60)
        ])

    out = _run(main())
    assert live["peak"] <= 6
    # Session index == claim index because the gate serialises them into a
    # single queue; whatever the pairing, each result must agree with the row
    # it actually read.
    for r in out:
        idx = int(r["thread_id"].rsplit("-", 1)[1])
        assert r["drain_cursor"] == f"cursor-{idx}"
        assert r["last_drained_msg_count"] == idx
        assert r["claimed"] is (idx % 2 == 0), (
            f"row {idx} claim outcome flipped under the gate"
        )
