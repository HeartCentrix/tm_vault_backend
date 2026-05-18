"""Pure-Graph probe of /chats/{id}/messages across all 12 apps.

Does NOT touch the database. Walks /users via app #1, finds the first
user with chats, picks the first chat, then hits
/chats/{cid}/messages?$top=5 with EACH of the 12 apps independently and
reports messages-returned per app.

Run::

    python3 scripts/probe_chat_endpoint.py

Reads ``APP_<N>_CLIENT_ID`` etc. from ``./.env`` via python-dotenv.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(".env")

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


async def _token(client_id: str, client_secret: str, tenant_id: str) -> str:
    import httpx
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://graph.microsoft.com/.default",
        "grant_type": "client_credentials",
    }
    async with httpx.AsyncClient(timeout=30.0) as c:
        r = await c.post(url, data=data)
        r.raise_for_status()
        return r.json()["access_token"]


async def _get(token: str, url: str, params: dict | None = None) -> dict:
    import httpx
    async with httpx.AsyncClient(timeout=30.0) as c:
        r = await c.get(
            url,
            headers={"Authorization": f"Bearer {token}"},
            params=params or {},
        )
        if r.status_code >= 400:
            return {"_error": f"HTTP {r.status_code}", "_body": r.text[:300]}
        return r.json()


async def _main() -> int:
    apps = []
    for i in range(1, 31):
        cid = os.getenv(f"APP_{i}_CLIENT_ID") or (
            os.getenv("AZURE_AD_CLIENT_ID") if i == 1 else None
        )
        csec = os.getenv(f"APP_{i}_CLIENT_SECRET") or (
            os.getenv("AZURE_AD_CLIENT_SECRET") if i == 1 else None
        )
        tid = (
            os.getenv(f"APP_{i}_TENANT_ID")
            or os.getenv("AZURE_AD_TENANT_ID")
            or "common"
        )
        if cid and csec:
            apps.append({
                "idx": i, "client_id": cid, "client_secret": csec,
                "tenant_id": tid,
            })
    print(f"loaded {len(apps)} app(s) from .env")
    if not apps:
        print("ERROR: no APP_*_CLIENT_ID in .env")
        return 1

    # Find a chat ID using app #1
    print("\nfinding a real chat to probe...")
    try:
        tok1 = await _token(apps[0]["client_id"], apps[0]["client_secret"], apps[0]["tenant_id"])
    except Exception as e:
        print(f"  app#1 token FAILED: {type(e).__name__}: {e}")
        return 1
    users_resp = await _get(tok1, "https://graph.microsoft.com/v1.0/users", {"$top": "10"})
    if "_error" in users_resp:
        print(f"  /users FAILED: {users_resp}")
        return 1

    cid_found = None
    uid_found = None
    uname_found = None
    for u in users_resp.get("value", []):
        uid = u.get("id")
        if not uid:
            continue
        chats = await _get(tok1, f"https://graph.microsoft.com/v1.0/users/{uid}/chats", {"$top": "1"})
        if "_error" in chats:
            continue
        cv = chats.get("value", [])
        if cv:
            cid_found = cv[0].get("id")
            uid_found = uid
            uname_found = u.get("displayName") or ""
            break
    if not cid_found:
        print("  no chats discovered for any user — Graph access issue at /chats level")
        return 1
    print(f"  probing chat {cid_found[:30]}... (user={uname_found})")

    # Hit /messages on this chat with each app
    print("\nper-app /messages probe:")
    results = []
    for app in apps:
        try:
            tok = await _token(app["client_id"], app["client_secret"], app["tenant_id"])
        except Exception as e:
            results.append({"app": app["client_id"][:8], "status": "TOKEN_FAIL", "err": str(e)[:120]})
            continue
        resp = await _get(
            tok,
            f"https://graph.microsoft.com/v1.0/chats/{cid_found}/messages",
            {"$top": "5"},
        )
        if "_error" in resp:
            results.append({
                "app": app["client_id"][:8],
                "status": resp["_error"],
                "body": resp.get("_body", "")[:200],
            })
        else:
            results.append({
                "app": app["client_id"][:8],
                "status": "OK",
                "msgs": len(resp.get("value", []) or []),
            })

    print(json.dumps(results, indent=2))

    ok = sum(1 for r in results if r.get("status") == "OK" and r.get("msgs", 0) > 0)
    empty = sum(1 for r in results if r.get("status") == "OK" and r.get("msgs", 0) == 0)
    err = sum(1 for r in results if r.get("status") not in ("OK",))
    print(f"\nSUMMARY: {ok} apps returned messages, {empty} returned empty, {err} errored")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(_main()))
