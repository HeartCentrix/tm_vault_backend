"""Test the hypothesis that $expand=hostedContents is what causes the
worker to get 0 messages while a plain call works.

Probes the same chat endpoint twice — once without $expand, once with
$expand=hostedContents (matching the worker's exact params).
"""
import asyncio
import os

import httpx
from dotenv import load_dotenv

load_dotenv(".env")

BASE = "https://graph.microsoft.com/v1.0"


async def _token(c: httpx.AsyncClient, cid: str, csec: str, tid: str) -> str:
    r = await c.post(
        f"https://login.microsoftonline.com/{tid}/oauth2/v2.0/token",
        data={
            "client_id": cid,
            "client_secret": csec,
            "scope": "https://graph.microsoft.com/.default",
            "grant_type": "client_credentials",
        },
    )
    r.raise_for_status()
    return r.json()["access_token"]


async def main() -> None:
    cid = os.getenv("APP_1_CLIENT_ID") or os.getenv("AZURE_AD_CLIENT_ID")
    csec = os.getenv("APP_1_CLIENT_SECRET") or os.getenv("AZURE_AD_CLIENT_SECRET")
    tid = os.getenv("APP_1_TENANT_ID") or os.getenv("AZURE_AD_TENANT_ID")
    print(f"using app={cid[:8]}... tenant={tid}")
    async with httpx.AsyncClient(timeout=30.0) as c:
        tok = await _token(c, cid, csec, tid)
        h = {"Authorization": f"Bearer {tok}"}

        # Find a user with chats
        users = (await c.get(f"{BASE}/users", headers=h, params={"$top": "20"})).json()
        chat_id = None
        for u in users.get("value", []):
            uid = u.get("id")
            if not uid:
                continue
            chats = await c.get(
                f"{BASE}/users/{uid}/chats", headers=h, params={"$top": "1"}
            )
            if chats.status_code == 200 and chats.json().get("value"):
                chat_id = chats.json()["value"][0]["id"]
                print(f"found chat {chat_id[:30]}... (user={u.get('displayName')})")
                break
        if not chat_id:
            print("no chats found")
            return

        # Test 1 — no $expand (probe-style)
        r1 = await c.get(
            f"{BASE}/chats/{chat_id}/messages",
            headers=h, params={"$top": "50"},
        )
        print(f"\nWITHOUT $expand:")
        print(f"  HTTP {r1.status_code}")
        if r1.status_code == 200:
            v = r1.json().get("value", [])
            print(f"  messages returned: {len(v)}")
        else:
            print(f"  body: {r1.text[:400]}")

        # Test 2 — with $expand=hostedContents (worker-style)
        r2 = await c.get(
            f"{BASE}/chats/{chat_id}/messages",
            headers=h, params={"$top": "50", "$expand": "hostedContents"},
        )
        print(f"\nWITH $expand=hostedContents:")
        print(f"  HTTP {r2.status_code}")
        if r2.status_code == 200:
            v = r2.json().get("value", [])
            print(f"  messages returned: {len(v)}")
            if v:
                print(f"  first msg id: {v[0].get('id', '')[:30]}")
                print(f"  has hostedContents key on msgs: "
                      f"{sum(1 for m in v if 'hostedContents' in m)}/{len(v)}")
        else:
            print(f"  body: {r2.text[:400]}")


if __name__ == "__main__":
    asyncio.run(main())
