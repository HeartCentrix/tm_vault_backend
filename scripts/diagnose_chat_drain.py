"""One-shot diagnostic for "chat backup returned 0 messages for every user."

Two questions, two answers:

1. **DB state** — is ``chat_thread_messages`` actually empty? Is the
   companion ``chat_threads`` table consistent? Counts pre-stat the
   in-flight backup and tells us whether a wipe was complete vs partial.

2. **Graph state** — does ``/chats/{id}/messages`` actually return data
   right now, using the same multi-app pool the worker uses? We pick
   one chat per known user and hit the endpoint with each of the 12
   app shards. If every shard returns ``value: []`` → it's a tenant /
   permission issue. If some return data and some don't → it's a
   per-app scope-grant issue. If all return data → there's a bug in the
   worker's drain path.

How to run::

    railway run --service backup_worker python3 scripts/diagnose_chat_drain.py

(Run against the same Railway env as the worker so multi_app_manager
sees the same credentials. ``DB_SCHEMA`` and the apps are read straight
from the service environment.)

Output is printed to stdout — copy/paste back so we can identify the
exact failure mode.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


async def _db_state() -> dict:
    """Return raw counts + a sample row from chat_threads to see if
    drain_cursor / last_drained_at look set (indicates an incremental-
    drain edge case) or empty (full-drain edge case)."""
    import asyncpg
    from shared.config import settings

    dsn = settings.DATABASE_URL
    if dsn.startswith("postgresql+asyncpg://"):
        dsn = dsn.replace("postgresql+asyncpg://", "postgresql://", 1)
    schema = os.environ.get("DB_SCHEMA", "tm_vault")

    conn = await asyncpg.connect(dsn, statement_cache_size=0)
    try:
        ct = await conn.fetchval(f'SELECT count(*) FROM "{schema}".chat_threads')
        ctm = await conn.fetchval(f'SELECT count(*) FROM "{schema}".chat_thread_messages')
        cti = await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".chat_threads '
            f"WHERE drain_cursor IS NOT NULL"
        )
        # Pick three sample chat_threads rows so we can see if drain_cursor
        # is suspiciously in the future or weirdly old.
        rows = await conn.fetch(
            f'SELECT chat_id, drain_cursor, last_drained_at, last_drained_msg_count '
            f'FROM "{schema}".chat_threads '
            f'ORDER BY last_drained_at DESC NULLS LAST LIMIT 5'
        )
        # And a sample chat_thread_id from chat_thread_messages
        sample_msgs = await conn.fetch(
            f'SELECT chat_thread_id, count(*) AS n '
            f'FROM "{schema}".chat_thread_messages '
            f'GROUP BY chat_thread_id ORDER BY n DESC LIMIT 5'
        )
        return {
            "chat_threads_count": int(ct or 0),
            "chat_thread_messages_count": int(ctm or 0),
            "chat_threads_with_cursor": int(cti or 0),
            "sample_chat_threads": [
                {
                    "chat_id": r["chat_id"],
                    "drain_cursor": str(r["drain_cursor"])[:40] if r["drain_cursor"] else None,
                    "last_drained_at": str(r["last_drained_at"]) if r["last_drained_at"] else None,
                    "last_drained_msg_count": (
                        int(r["last_drained_msg_count"])
                        if r["last_drained_msg_count"] is not None else None
                    ),
                }
                for r in rows
            ],
            "top_msg_threads": [
                {"chat_thread_id": str(r["chat_thread_id"]), "n": int(r["n"])}
                for r in sample_msgs
            ],
        }
    finally:
        await conn.close()


async def _pick_one_chat_id_per_user() -> list[tuple[str, str, str]]:
    """Look in the workers' last discovery output to find a real chat ID.

    We hit Graph's ``/users/{uid}/chats`` ourselves with one of the apps
    so we don't depend on any worker-side cache. Returns
    ``[(user_id, user_email_hint, chat_id), ...]`` for up to 3 users.
    """
    import asyncpg
    from shared.config import settings
    from shared.multi_app_manager import multi_app_manager
    from shared.graph_client import GraphClient

    dsn = settings.DATABASE_URL
    if dsn.startswith("postgresql+asyncpg://"):
        dsn = dsn.replace("postgresql+asyncpg://", "postgresql://", 1)
    schema = os.environ.get("DB_SCHEMA", "tm_vault")

    conn = await asyncpg.connect(dsn, statement_cache_size=0)
    try:
        rows = await conn.fetch(
            f'SELECT r.id, r.name, t.external_tenant_id '
            f'FROM "{schema}".resources r '
            f'JOIN "{schema}".tenants t ON t.id = r.tenant_id '
            f'WHERE r.type::text = $1 LIMIT 3',
            "USER_CHATS",
        )
    finally:
        await conn.close()

    if not rows:
        return []

    apps = [a for a in multi_app_manager.apps if a.client_id and a.client_secret]
    if not apps:
        return []
    app = apps[0]
    tenant_id = rows[0]["external_tenant_id"]
    gc = GraphClient(app.client_id, app.client_secret, tenant_id)

    out: list[tuple[str, str, str]] = []
    for r in rows:
        # USER_CHATS resource_id is NOT the AAD user_id — we need to
        # find the parent ENTRA_USER for the same row. The resource
        # name normally encodes "Chats — <user>" so we cross-look up
        # via parent_resource_id (column added in two-tier discovery).
        try:
            page = await gc._get(f"{gc.GRAPH_URL}/users")
            users = (page or {}).get("value", [])[:3]
            for u in users:
                uid = u.get("id")
                if not uid:
                    continue
                chats_page = await gc._get(
                    f"{gc.GRAPH_URL}/users/{uid}/chats",
                    params={"$top": "1"},
                )
                cv = (chats_page or {}).get("value", [])
                if cv:
                    out.append((uid, u.get("displayName") or "", cv[0].get("id")))
                    break
        except Exception as e:
            print(f"  could not enumerate chats for {r['name']}: {type(e).__name__}: {e}")
            continue
        if len(out) >= 1:
            break
    return out


async def _graph_state() -> dict:
    """Hit /chats/{cid}/messages with each of the 12 app shards
    independently. Report per-shard outcome so we see if SOME apps
    return data and others don't (= per-app permission grant gap)."""
    from shared.multi_app_manager import multi_app_manager
    from shared.graph_client import GraphClient

    # First find ONE real chat ID using the SAME path the worker uses.
    samples = await _pick_one_chat_id_per_user()
    if not samples:
        return {"error": "could not find any chat id to probe"}
    uid, uname, cid = samples[0]
    print(f"  probing chat {cid[:30]}... (user={uname})")

    apps = [a for a in multi_app_manager.apps if a.client_id and a.client_secret]
    if not apps:
        return {"error": "no apps configured"}

    # The chat lives in this tenant.
    import asyncpg
    from shared.config import settings
    dsn = settings.DATABASE_URL
    if dsn.startswith("postgresql+asyncpg://"):
        dsn = dsn.replace("postgresql+asyncpg://", "postgresql://", 1)
    schema = os.environ.get("DB_SCHEMA", "tm_vault")
    conn = await asyncpg.connect(dsn, statement_cache_size=0)
    try:
        tid = await conn.fetchval(
            f'SELECT external_tenant_id FROM "{schema}".tenants LIMIT 1'
        )
    finally:
        await conn.close()

    results = []
    for i, app in enumerate(apps):
        gc = GraphClient(app.client_id, app.client_secret, tid)
        try:
            page = await gc._get(
                f"{gc.GRAPH_URL}/chats/{cid}/messages",
                params={"$top": "5"},
            )
            n = len((page or {}).get("value", []) or [])
            results.append({
                "app_idx": i,
                "app_id": app.client_id[:8],
                "status": "OK",
                "messages_returned": n,
            })
        except Exception as e:
            results.append({
                "app_idx": i,
                "app_id": app.client_id[:8],
                "status": type(e).__name__,
                "error": str(e)[:200],
            })
    return {
        "probed_chat_id": cid,
        "probed_user_id": uid,
        "per_app": results,
    }


async def _main() -> int:
    print("=" * 72)
    print("CHAT DRAIN DIAGNOSTIC")
    print("=" * 72)
    print("\n[1/2] DB state ---------------------------------------------------")
    try:
        db = await _db_state()
        print(json.dumps(db, indent=2, default=str))
    except Exception as e:
        print(f"DB query FAILED: {type(e).__name__}: {e}")
        db = {}

    print("\n[2/2] Graph state ------------------------------------------------")
    try:
        gs = await _graph_state()
        print(json.dumps(gs, indent=2))
    except Exception as e:
        print(f"Graph probe FAILED: {type(e).__name__}: {e}")
        gs = {}

    print("\n=== summary ===")
    ct = db.get("chat_threads_count", 0)
    ctm = db.get("chat_thread_messages_count", 0)
    cti = db.get("chat_threads_with_cursor", 0)
    print(f"  chat_threads rows:          {ct}")
    print(f"  chat_thread_messages rows:  {ctm}")
    print(f"  chat_threads w/ cursor:     {cti}")
    if gs.get("per_app"):
        ok = sum(1 for r in gs["per_app"] if r.get("status") == "OK" and r.get("messages_returned", 0) > 0)
        empty = sum(1 for r in gs["per_app"] if r.get("status") == "OK" and r.get("messages_returned", 0) == 0)
        errs = sum(1 for r in gs["per_app"] if r.get("status") != "OK")
        print(f"  apps returning messages:    {ok}/{len(gs['per_app'])}")
        print(f"  apps returning empty:       {empty}/{len(gs['per_app'])}")
        print(f"  apps erroring:              {errs}/{len(gs['per_app'])}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(_main()))
