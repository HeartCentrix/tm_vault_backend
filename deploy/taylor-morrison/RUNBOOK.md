# TMvault — Taylor Morrison Deployment Runbook

**Target:** 5,000 users / 260 TiB / single tenant (on-prem deployment)

This runbook documents the configuration, provisioning, and operational
plan for Taylor Morrison's TMvault deployment. Pair with
`.env.template` in this directory.

---

## 1. Sizing & throughput projection

| Dimension | Number |
|---|---|
| Users (mailboxes / OneDrive / Teams) | 5,000 |
| Total M365 data | 260 TiB |
| Avg per user | ~52 GB |
| Estimated file count | ~50 million |
| Estimated email count | ~25 million |
| Day-1 first-sync Graph calls | ~105 million |
| Daily change rate (typical enterprise) | 1–3% |

### First-sync wall-time, per pipe-to-Azure

| Network pipe | Bytes-only wall | Metadata-only wall | Combined (parallel) |
|---|---|---|---|
| 1 Gbps (commodity) | ~24 days | ~20 days (12 apps × 5 RPS) | **~24 days** (byte-bound) |
| 10 Gbps | ~60 hours | ~20 days | **~20 days** (metadata-bound) |
| 25 Gbps ExpressRoute | ~24 hours | ~20 days | **~20 days** (metadata-bound) |

**The metadata path is the binding constraint above 10 Gbps.** Adding
network bandwidth past 10 Gbps doesn't speed up first sync further —
Graph throttle limits do. To go below 20 days, the lever is **more
apps** (15 apps → ~16 days; 20 apps → ~12 days).

### Day-2 onward

Daily incremental at 1–3% change rate = ~3–8 TiB / day fresh content.
Wall-time: **~3–8 hours**, easily within a nightly window.

---

## 2. Azure AD app registrations

Register **12 apps** in Taylor's Entra ID tenant. Naming convention:
`TMvault-Backup-01` … `TMvault-Backup-12`.

### Required Application permissions (admin-consented in Taylor's tenant)

| Permission | What it covers |
|---|---|
| `Mail.Read`, `Mail.ReadBasic.All` | User mailboxes |
| `Files.Read.All`, `Sites.Read.All` | OneDrive + SharePoint |
| `Chat.Read.All`, `ChannelMessage.Read.All` | Teams |
| `Calendars.Read`, `Contacts.Read` | Personal calendar/contacts |
| `User.Read.All`, `Group.Read.All` | User + group discovery |
| `Directory.Read.All` | Entra metadata |
| `DeviceManagementManagedDevices.Read.All` | Intune (optional) |

### Why 12 apps in customer tenant is fine

- Enterprise tenants routinely have 100+ apps registered (every SaaS
  integration adds one). 12 backup-specific apps is well within normal.
- Microsoft's anti-pattern scrutiny is for **vendors** spinning up apps
  in **vendor tenant** to bypass per-customer throttle ceilings. Taylor
  registering apps in their own tenant for their own data is the
  opposite — they're consuming **their own** quota, not anyone else's.
- The codebase supports up to 30 apps (`GRAPH_APP_MAX=30`). Going to 15
  or 20 is fine if first-sync wall-time needs to drop further; just
  watch the 429 rate (more apps → more rotation overhead).

### Provisioning script

For each app (1..12):

```bash
APP_NAME="TMvault-Backup-$(printf '%02d' $N)"
TENANT_ID="<taylor-tenant-id>"

az ad app create --display-name "$APP_NAME"
# Get the app id back, then add Application permissions:
APP_ID=$(az ad app list --filter "displayName eq '$APP_NAME'" --query "[0].appId" -o tsv)

# Repeat for each permission GUID (Mail.Read, Files.Read.All, etc.)
az ad app permission add --id "$APP_ID" --api 00000003-0000-0000-c000-000000000000 \
  --api-permissions "<perm-guid>=Role"

# Grant admin consent
az ad app permission admin-consent --id "$APP_ID"

# Create a client secret (rotate annually)
az ad app credential reset --id "$APP_ID" --years 2 --display-name "tmvault-prod"
```

Output secrets go into `.env` as `APP_N_CLIENT_ID`, `APP_N_CLIENT_SECRET`,
`APP_N_TENANT_ID`.

---

## 3. Infrastructure sizing

### Worker replicas

```yaml
# docker-compose.prod.yml
services:
  backup-worker:        # light workers
    deploy:
      replicas: 8
    environment:
      BACKUP_CONCURRENCY: 200
      BACKUP_WORKER_QUEUE: backup.normal

  backup-worker-heavy:  # for >10 GB files
    deploy:
      replicas: 2
    environment:
      BACKUP_CONCURRENCY: 200
      BACKUP_WORKER_QUEUE: backup.heavy
      MAX_CONCURRENT_ONEDRIVE_BACKUPS_PER_WORKER: 4
```

- 8 light × 32 file concurrency = **256 files in flight simultaneously**
- 2 heavy reserved for huge-file workloads

### Postgres

| Resource | Recommendation |
|---|---|
| RAM | 32 GB |
| Disk | 4 TB NVMe SSD (snapshot_items grows large) |
| `work_mem` | 64 MB |
| `shared_buffers` | 8 GB |
| `maintenance_work_mem` | 2 GB |
| Connection pool | 200 |

Expected `snapshot_items` row count after first sync: **~80-100M rows**.

### Azure Blob storage

- Hot tier for first 30 days post-backup
- Auto-tier to Cool after 30 days (saved access)
- Auto-tier to Archive after 90 days (rarely accessed)

| Tier | $/TB/mo | 260 TiB cost |
|---|---|---|
| Hot | $20.80 | ~$5,400/mo |
| Cool | $10.00 | ~$2,600/mo |
| Archive | $1.00 | ~$260/mo |

### Network pipe

Minimum: **10 Gbps** internet uplink.
Recommended: **25 Gbps Azure ExpressRoute** (~$1,200/mo, eliminates
internet variability, lowers latency to Graph endpoints).

---

## 4. First-sync rollout plan

The first sync is the painful one. Plan for **3 weeks total**.

### Week 1 — onboarding (Monday day 1 to Sunday day 7)

- Day 1: Bring up infrastructure, validate connectivity, run discovery
  (`POST /api/discovery/start`). Discovery alone: ~30 min for 5k users.
- Day 2: First sync of **CEO/critical 5%** (250 users). Wall: ~12 hours.
  Validate restore path against 3 random files per critical user.
- Day 3-5: Tier-1 sync: **next 25%** (1,250 users). Stagger across 3
  nights to validate cumulative load.
- Day 6-7: All remaining users **queued** via the scheduler but throttle
  policies cap aggregate to 60 RPS so Graph quota is respected. Run
  continuously.

### Week 2 — bulk first-sync continuation

- Continuous backup; ~25-30% of remaining users complete per day under
  20-RPS-per-resource-type sustained throughput.
- Daily progress check via the snapshot dashboard. Expect ~25 TiB/day
  ingested at 25 Gbps.

### Week 3 — tail + tier-down

- Last ~15% of users finish first sync.
- Switch SLA policies on archival users to `WEEKLY` to reduce ongoing
  load (90% of mailboxes are dormant, daily backups waste cycles).
- Validate cross-user restore (full mailbox + folder + file).

### Day 21+ — steady-state

- Daily incremental: 3–8 hours per nightly window.
- Auto-tier policies move 30-day-old blobs to Cool tier.

---

## 5. Day-2 operations

### Monitor 429 rate (the canary)

```bash
# % of paginated requests that hit 429 in last hour
docker logs --since 1h tm_backend-backup-worker-1 2>&1 \
  | awk '/iter\] migrating 429/ { m++ } /iter\] returning to original/ { r++ }
         END { print "migrations:", m, "returns:", r }'
```

| 429 rate | Action |
|---|---|
| < 1% | Healthy. No action. |
| 1–5% | Normal during first sync. Watch trend. |
| 5–15% | Add 2 more apps (`APP_13`, `APP_14`). Restart workers. |
| > 15% | Throttle is dominant. Add more apps **AND** raise `GRAPH_POST_THROTTLE_BRAKE_MS` to 5000. |

### Monitor wall-time per resource

```sql
SELECT r.type,
       PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY wall) AS p50,
       PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY wall) AS p95,
       PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY wall) AS p99
FROM (
  SELECT r.type, EXTRACT(EPOCH FROM s.completed_at - s.started_at) AS wall
  FROM tm.snapshots s
  JOIN tm.resources r ON r.id = s.resource_id
  WHERE s.status = 'COMPLETED'
    AND s.completed_at > NOW() - INTERVAL '24 hours'
) sub
GROUP BY type;
```

### Storage growth tracking

```sql
SELECT pg_size_pretty(SUM(bytes_total)) AS total_backup_bytes,
       COUNT(*) AS snapshot_count
FROM tm.snapshots
WHERE status = 'COMPLETED';
```

### Disaster recovery validation

Quarterly: restore a random user fully to a sandbox tenant. Verify
mailbox / OneDrive / Teams chat fidelity.

### Prometheus alerts

Scrape and alert definitions live in `k8s/monitoring/` (PodMonitors, NetworkPolicy,
PrometheusRule `tmvault-alerts`, blackbox Probe); `k8s/monitoring/README.md` lists every
alert, its threshold and what it depends on. Routing is the cluster default:
`severity: critical` emails the on-call mailbox **and** opens an ITO Jira ticket,
`severity: warning` opens the ticket only. First checks per alert family:

#### Worker, scheduler or target down

`TMVaultWorkerDown`, `TMVaultHeavyWorkerDown`, `TMVaultSchedulerDown`, `TMVaultTargetDown`:
Prometheus cannot scrape the pod(s) named in the alert (`job` = the pod's `app` label).

1. `kubectl -n itops get pods -l app=<job>` — CrashLoopBackOff / Error is the image or
   config, not the network: `kubectl -n itops logs <pod> --previous --tail=50`.
2. All pods Running but every target `up == 0`: the scrape path is blocked. Confirm
   `kubectl -n itops get netpol tmvault-ingress-prometheus-scrape` exists and read the
   target error in the Prometheus UI (Status → Targets). The exporter itself can be
   checked from inside the pod on its port (9103 core, 9101 scheduler, 9100 restore,
   9102 chat-export): `kubectl -n itops exec <pod> -- python3 -c "import urllib.request;print(urllib.request.urlopen('http://127.0.0.1:9103/metrics').read()[:200])"`.
3. Restarting a single-replica role (scheduler, restore/discovery worker, an API service)
   is safe. Restarting backup workers aborts their in-flight jobs; those are re-queued
   only after the job lease expires, so prefer waiting for the current wave to drain.

#### Snapshot failures

`TMVaultFailedSnapshotsSpike` (warning), `TMVaultSnapshotFailureRatioHigh` (critical),
`TMVaultNoSnapshotsCompleted` (warning) — all from `tmvault_snapshots_total{status}`.

1. Find the dominant error: `kubectl -n itops logs deploy/backup-worker --since=1h | grep -iE 'failed|error' | sort | uniq -c | sort -rn | head`
   (repeat for `deploy/backup-worker-heavy`).
2. Break the failures down by resource type and tenant in `tm.snapshots` (same table as
   the wall-time query above, filter `status = 'FAILED'` and the last hour).
3. If the failures cluster on Graph 429/503 responses, follow "Monitor 429 rate (the
   canary)" above; if on storage writes, check the storage backend and the storage router
   (`[storage]` log lines); if on the database, check `databases/postgres-ha` first.
4. `TMVaultNoSnapshotsCompleted` with healthy workers means nothing is being scheduled:
   check the scheduler logs and the RabbitMQ queue alerts before assuming the workers.

#### Worker memory

`TMVaultWorkerMemoryHigh` (warning, > 85% of limit), `TMVaultWorkerMemoryCritical`
(critical, > 95%), `TMVaultContainerOOMKilled` (warning).

1. `kubectl -n itops top pod <pod>` and `kubectl -n itops describe pod <pod> | grep -A4 'Last State'`
   (an OOMKilled last state confirms the kernel already killed it once).
2. Backup workers grow with the mailbox/site they are walking. A sustained high value on
   one pod is usually one huge resource; an OOMKill loses that job and it is retried
   later, so repeated kills on the same resource mean the limit is too small for it.
3. Limits live in `k8s/50-workers.yaml` (backup-worker 8Gi, backup-worker-heavy 16Gi,
   others 1–2Gi) and `k8s/40-services.yaml` (512Mi; snapshot-service 3Gi). Raise with
   section 3 (Infrastructure sizing) in mind and re-apply the one Deployment.

#### RabbitMQ queues and broker

`TMVaultRabbitmqDown` (critical), `TMVaultRabbitmqResourceAlarm` (critical),
`TMVaultQueueBacklog`, `TMVaultQueueNoConsumers`, `TMVaultDeadLetterQueueParked` (warning).

1. State of every queue:
   `kubectl -n itops exec deploy/tmvault-rabbitmq -- rabbitmqctl list_queues name messages_ready messages_unacknowledged consumers`
   — or the management UI via `kubectl -n itops port-forward svc/tmvault-rabbitmq 15672:15672`.
2. Queue prefix → consuming Deployment: `backup.*` → backup-worker / backup-worker-heavy,
   `restore.*` → restore-worker, `discovery.*` → discovery-worker, `azure.*` →
   azure-workload-worker. Zero consumers with messages waiting means that Deployment is
   down or wedged before subscribing: restart it and re-check consumers.
3. `*.dlq` / `dlq.*` messages are jobs that exhausted their retries. They are never retried
   on their own — inspect them (management UI → queue → Get messages), fix the cause,
   then re-publish to the work queue or purge them deliberately.
4. A resource alarm (`rabbitmqctl status`, "alarms") makes the broker block every
   publisher, so the pipeline stalls silently: free disk on the `/var/lib/rabbitmq` PVC
   or raise the pod's memory before anything else.

#### Redis down

`TMVaultRedisDown` (critical) — `tmvault-redis` has no available replica (Redis has no
exporter, so this is the only signal). `kubectl -n itops get pods -l app=tmvault-redis`,
then the pod logs; the Deployment is in `k8s/21-redis.yaml`.

#### Ingress probe failing

`TMVaultIngressProbeFailing` (critical) — the blackbox exporter in `monitoring` cannot get
a 2xx from `https://tmvault.taylormorrison.com/` through the ingress VIP.

1. `curl -sk -o /dev/null -w '%{http_code}\n' https://tmvault.taylormorrison.com/` from any
   host; `kubectl -n itops get pods -l app=tmvault-frontend`; `kubectl -n itops get ingress tmvault`.
2. If the cluster-wide `metallb-vip-ingress-https` probe is failing at the same time, the
   problem is the ingress VIP / ingress-nginx, not TM Vault.

#### SLA audit publish failing

`TMVaultSlaAuditPublishFailing` (warning) — backup-scheduler could not publish SLA
reconcile audit events (`sla_audit_publish_failed_total`), a silent compliance gap.
`kubectl -n itops logs deploy/backup-scheduler --since=1h | grep -i audit`, check
`audit-service` health, and confirm `INTERNAL_API_KEY` reaches both pods
(`tests/deploy/test_manifest_internal_api_key.py` explains the two halves).

---

## 6. What to tune if it's still too slow

In priority order:

1. **More apps.** 12 → 15 → 20 in `.env`. Linear gain. Watch 429 rate.
2. **Raise `GRAPH_APP_PACE_REQS_PER_SEC` from 5.0 to 7.0.** More
   aggressive pacing per app. Will cause more 429s but more total
   throughput. Stop here if 429 rate exceeds 10%.
3. **More worker replicas.** 8 → 16. Helps if bandwidth is the bind.
   Doesn't help if metadata is the bind.
4. **Hybrid with Microsoft 365 Backup Storage API.** Microsoft's
   official bulk-export path. Requires per-user license (~$3/mo).
   Add-on to existing Graph code; bulk goes via Backup Storage,
   metadata + restore stays on Graph. Cuts byte-transfer wall by ~10×
   and doesn't count against Graph throttle.

---

## 7. What NOT to do

- ❌ Don't lower `GRAPH_APP_PACE_REQS_PER_SEC` below 3.0 thinking
  you're being polite. Throttling triggers on burst, not sustained
  rate. Lower pace just makes it slower without avoiding 429s.
- ❌ Don't disable the multi-app rotation by setting `GRAPH_APP_MAX=1`.
  Single-app deployments hit throttle ceilings in minutes at this
  scale.
- ❌ Don't try to compress first sync into one weekend. The 20-day
  metadata floor is real — no amount of bandwidth or workers fixes it
  without more apps OR Backup Storage API.
- ❌ Don't run backups during M365 maintenance windows. Microsoft
  publishes these in the M365 admin center; subscribe to that feed
  and pause the scheduler during announced windows.

---

## 8. Support / escalation contacts

(Fill in during handover)

- **TMvault product owner:** _____________
- **Taylor M365 admin lead:** _____________
- **Azure subscription owner:** _____________
- **On-call rotation:** _____________
