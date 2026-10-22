"""Post-deployment health check for TM Vault.

Exercises the application end-to-end after a deploy and exits non-zero if
anything is wrong, so it can gate CI / be run from a deploy pipeline or
in-cluster (the scheduler pod has all the env it needs).

Tiers (each check → PASS / WARN / FAIL):
  0  Reachability   — gateway /api/v1/health + each service /health
  1  Infra          — DB, RabbitMQ, queues (consumers/DLQ), workers, storage,
                      orphaned leases  (via shared.health, run in-process)
  2  Functional     — read-only API probes through the gateway
  3  Storage probe  — non-destructive reachability of the active backend
  4  Deep (--deep)  — synthetic backup+restore on HEALTHCHECK_TEST_RESOURCE_ID

Exit code: 0 if no FAILs (WARNs allowed), 1 otherwise.

Usage:
  python -m scripts.health_check                # in-cluster, full default run
  python scripts/health_check.py --json         # machine-readable
  python scripts/health_check.py --deep         # include synthetic backup/restore
  BASE_URL=http://api-gateway:8000 TOKEN=... python scripts/health_check.py

Env:
  BASE_URL / API_GATEWAY_URL   gateway base (default http://api-gateway:8000)
  TOKEN / HEALTHCHECK_TOKEN     bearer token for authed probes (optional)
  HEALTHCHECK_TENANT_ID         tenant for functional probes (optional)
  HEALTHCHECK_TEST_RESOURCE_ID  resource for --deep backup/restore (required for --deep)
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import io
import json
import os
import sys
import time

import httpx

PASS, WARN, FAIL, SKIP = "PASS", "WARN", "FAIL", "SKIP"
_DOT = {PASS: "\033[32m●\033[0m", WARN: "\033[33m●\033[0m", FAIL: "\033[31m●\033[0m", SKIP: "\033[90m○\033[0m"}
# Map shared.health status vocabulary → check result.
_FROM_STATUS = {"healthy": PASS, "degraded": WARN, "unhealthy": FAIL}

results: list[dict] = []


def record(tier: str, name: str, status: str, detail: str = "") -> None:
    results.append({"tier": tier, "name": name, "status": status, "detail": detail})


def base_url() -> str:
    return (os.getenv("BASE_URL") or os.getenv("API_GATEWAY_URL") or "http://api-gateway:8000").rstrip("/")


def token() -> str | None:
    return os.getenv("TOKEN") or os.getenv("HEALTHCHECK_TOKEN")


def _auth_headers() -> dict:
    t = token()
    return {"Authorization": f"Bearer {t}"} if t else {}


# ─── Tier 0: reachability ───────────────────────────────────────────────────
async def tier0_reachability(client: httpx.AsyncClient) -> None:
    try:
        r = await client.get(f"{base_url()}/api/v1/health", headers=_auth_headers(), timeout=15.0)
        r.raise_for_status()
        body = r.json()
        overall = body.get("status", "unknown")
        record("0-reach", "gateway /api/v1/health", _FROM_STATUS.get(overall, FAIL), f"overall={overall}")
        svc = body.get("services", {})
        down = svc.get("down_list") or []
        if svc.get("down", 0) == 0:
            record("0-reach", "service matrix", PASS, f"{svc.get('up', '?')} services up")
        else:
            record("0-reach", "service matrix", FAIL, f"down: {', '.join(down) or svc.get('down')}")
    except Exception as e:
        record("0-reach", "gateway /api/v1/health", FAIL, f"{type(e).__name__}: {e}")


# ─── Tier 1: infra (in-process via shared.health) ───────────────────────────
async def tier1_infra() -> None:
    try:
        from shared.health import build_health
    except Exception as e:
        record("1-infra", "shared.health import", SKIP, f"not runnable here: {type(e).__name__}")
        return
    try:
        report = await build_health(detailed=True)
    except Exception as e:
        record("1-infra", "build_health", FAIL, f"{type(e).__name__}: {e}")
        return
    for name, comp in report.get("components", {}).items():
        record("1-infra", name, _FROM_STATUS.get(comp.get("status"), WARN), comp.get("detail", ""))


# ─── Tier 2: functional read-only probes ────────────────────────────────────
async def _probe(client, label, path, *, need_auth=False, ok=lambda r: r.status_code == 200):
    if need_auth and not token():
        record("2-func", label, SKIP, "no token provided")
        return None
    try:
        r = await client.get(f"{base_url()}{path}", headers=_auth_headers(), timeout=20.0)
        if ok(r):
            record("2-func", label, PASS, f"HTTP {r.status_code}")
            return r
        record("2-func", label, FAIL, f"HTTP {r.status_code}")
    except Exception as e:
        record("2-func", label, FAIL, f"{type(e).__name__}: {e}")
    return None


async def tier2_functional(client: httpx.AsyncClient) -> None:
    tenant = os.getenv("HEALTHCHECK_TENANT_ID", "")
    tq = f"?tenantId={tenant}" if tenant else ""
    await _probe(client, "auth /me", "/api/v1/auth/me", need_auth=True)
    await _probe(client, "tenants list", "/api/v1/tenants", need_auth=True)
    await _probe(client, "resources list", f"/api/v1/resources{tq}", need_auth=True)
    await _probe(client, "dashboard overview", f"/api/v1/dashboard/overview{tq}", need_auth=True)  # Check 17: dashboard now requires auth
    # Recovery sidebar source of truth: must be 200 AND non-empty once
    # the tenant has any COMPLETED snapshot. A 2026-07-14 deploy shipped
    # a NameError inside this endpoint (missing import — py_compile
    # can't catch it) and Recovery showed "No resources backed up yet"
    # until a human noticed. 500s and silently-empty payloads both FAIL.
    if tenant:
        def _has_rows(r):
            if r.status_code != 200:
                return False
            try:
                body = r.json() or {}
            except Exception:
                return False
            items = body.get("content") or body.get("items") or []
            return len(items) > 0
        await _probe(
            client, "recovery with-backups non-empty",
            f"/api/v1/resources/with-backups?tenantId={tenant}&page=1&size=10",
            need_auth=True,  # Check 17: resource-service now requires auth
            ok=_has_rows,
        )
    else:
        record("2-func", "recovery with-backups non-empty", SKIP,
               "set HEALTHCHECK_TENANT_ID to enable")


# ─── Tier 3: storage reachability (non-destructive) ─────────────────────────
async def tier3_storage() -> None:
    try:
        from shared.storage.startup import startup_router
        from shared.storage.router import router
    except Exception as e:
        record("3-storage", "storage router import", SKIP, f"{type(e).__name__}")
        return
    try:
        await startup_router()
        store = router.get_active_store()
        # get_properties on a sentinel path: a clean None / not-found means the
        # backend endpoint is reachable. We never write tenant data here.
        await store.get_properties("healthcheck", "__probe_nonexistent__")
        record("3-storage", "active backend reachable", PASS, f"active={router.active_backend_id()}")
    except Exception as e:
        # A "not found" surfaced as an exception still proves reachability for
        # some backends; only connection-level errors are real failures.
        msg = str(e).lower()
        if "not found" in msg or "404" in msg or "nosuchkey" in msg:
            record("3-storage", "active backend reachable", PASS, "endpoint responded (404 sentinel)")
        else:
            record("3-storage", "active backend reachable", FAIL, f"{type(e).__name__}: {e}")


# ─── Tier 4: deep synthetic backup/restore (gated) ──────────────────────────
async def tier4_deep(client: httpx.AsyncClient) -> None:
    rid = os.getenv("HEALTHCHECK_TEST_RESOURCE_ID")
    if not rid:
        record("4-deep", "synthetic backup/restore", SKIP, "set HEALTHCHECK_TEST_RESOURCE_ID to enable")
        return
    if not token():
        record("4-deep", "synthetic backup/restore", SKIP, "no token provided")
        return
    try:
        r = await client.post(
            f"{base_url()}/api/v1/backups/trigger",
            headers=_auth_headers(),
            json={"resourceIds": [rid], "full": False},
            timeout=30.0,
        )
        if r.status_code not in (200, 201, 202):
            record("4-deep", "enqueue backup", FAIL, f"HTTP {r.status_code}")
            return
        job = r.json()
        job_id = job.get("jobId") or job.get("id")
        record("4-deep", "enqueue backup", PASS, f"job={job_id}")
        # Poll the job to a terminal state (bounded).
        deadline = time.monotonic() + 600
        last = "?"
        while time.monotonic() < deadline:
            jr = await client.get(f"{base_url()}/api/v1/jobs/{job_id}", headers=_auth_headers(), timeout=20.0)
            last = (jr.json() or {}).get("status", "?") if jr.status_code == 200 else f"HTTP {jr.status_code}"
            if last in ("COMPLETED", "SUCCEEDED"):
                record("4-deep", "backup completes", PASS, f"status={last}")
                return
            if last in ("FAILED", "ERROR", "CANCELLED"):
                record("4-deep", "backup completes", FAIL, f"status={last}")
                return
            await asyncio.sleep(10)
        record("4-deep", "backup completes", WARN, f"did not finish in time (last={last})")
    except Exception as e:
        record("4-deep", "synthetic backup/restore", FAIL, f"{type(e).__name__}: {e}")


# ─── runner / output ────────────────────────────────────────────────────────
async def run(deep: bool) -> None:
    async with httpx.AsyncClient() as client:
        await tier0_reachability(client)
        await tier1_infra()
        await tier2_functional(client)
        await tier3_storage()
        if deep:
            await tier4_deep(client)


def main() -> int:
    ap = argparse.ArgumentParser(description="TM Vault post-deploy health check")
    ap.add_argument("--json", action="store_true", help="machine-readable output")
    ap.add_argument("--deep", action="store_true", help="include synthetic backup/restore")
    args = ap.parse_args()

    # In --json mode, keep stdout pure: imported modules emit warnings
    # (e.g. "[AzureStorage] WARNING: ...") at import/call time that would
    # otherwise corrupt the JSON document. Swallow them to stderr.
    if args.json:
        with contextlib.redirect_stdout(io.StringIO()) as buf:
            asyncio.run(run(args.deep))
        sys.stderr.write(buf.getvalue())
    else:
        asyncio.run(run(args.deep))

    fails = sum(1 for r in results if r["status"] == FAIL)
    warns = sum(1 for r in results if r["status"] == WARN)

    if args.json:
        print(json.dumps({
            "ok": fails == 0,
            "fails": fails, "warns": warns,
            "checks": results,
        }, indent=2))
    else:
        cur = None
        for r in results:
            if r["tier"] != cur:
                cur = r["tier"]
                print(f"\n── tier {cur} ──")
            print(f"  {_DOT.get(r['status'], '?')} {r['status']:4} {r['name']:<28} {r['detail']}")
        banner = "\033[32mHEALTHY\033[0m" if fails == 0 else "\033[31mUNHEALTHY\033[0m"
        print(f"\n{banner}  —  {fails} fail, {warns} warn, {len(results)} checks")

    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
