# TM Vault — Prometheus monitoring manifests

Scrape + alert definitions for the TM Vault stack on the cluster's kube-prometheus-stack
(namespace `monitoring`: Prometheus v3.9.1, prometheus-operator v0.89.0, kube-state-metrics,
cAdvisor via the kubelet ServiceMonitor, blackbox-exporter v0.28). Prometheus selects
PodMonitor / Probe / PrometheusRule objects in **any** namespace that carry
`release: kube-prometheus-stack`, so everything here lives in `itops` next to the pods.

**Nothing in this folder is applied by `build-images.sh`, `gen-*.sh` or any script.**
Before 2026-10-14 nothing scraped a TM Vault pod and no rule mentioned tmvault/rabbitmq/redis.

| File | Creates | Needs |
|---|---|---|
| `netpol-tmvault-prometheus-scrape.yaml` | NetworkPolicy `tmvault-ingress-prometheus-scrape` | nothing — **apply first**. itops is default-deny; the cluster's catch-all `allow-prometheus-scrape` only opens 9090/9100/8080/8443, so without this the PodMonitors scrape nothing but restore-worker:9100 |
| `podmonitor-tmvault.yaml` | 7 PodMonitors (`tmvault-backup-worker`, `-backup-scheduler`, `-restore-worker`, `-chat-export-worker`, `-aux-workers`, `-api-services`, `-rabbitmq`) | the NetworkPolicy; pods labelled `stack=tmvault` + `app=<role>` (40-services.yaml / 50-workers.yaml / 20-rabbitmq.yaml) |
| `prometheusrule-tmvault.yaml` | PrometheusRule `tmvault-alerts` — 18 alerts in 5 groups | the PodMonitors (for `up`, `tmvault_*`, `sla_*`, `rabbitmq_*`), kube-state-metrics (`kube_*`), cAdvisor (`container_memory_working_set_bytes`), the Probe (`probe_success`) |
| `probe-tmvault-ingress.yaml` | Probe `tmvault-ingress` (job `tmvault-ingress`) | blackbox-exporter module `http_2xx_tls` (present in its ConfigMap); the ingress VIP 192.168.207.41 reachable from `monitoring` (it already is for the existing VIP probe) |

## Apply / verify (manual, in this order)

```bash
cd deploy/taylor-morrison/k8s/monitoring
kubectl apply --dry-run=server -f .            # all four validate against the live CRDs
kubectl apply -f netpol-tmvault-prometheus-scrape.yaml
kubectl apply -f podmonitor-tmvault.yaml
kubectl apply -f probe-tmvault-ingress.yaml
kubectl apply -f prometheusrule-tmvault.yaml
```

Apply the PrometheusRule **last, only after the targets show UP**: `absent(up{...} == 1)`
evaluates to 1 while the PodMonitors do not exist, so applying the rule first fires every
`*Down` critical (email + Jira) within 10 minutes. Expect `TMVaultWorkerMemoryHigh` to open
a ticket on the first apply if a backup-worker is already above 85% of its 8Gi limit (one
was at 92% on 2026-10-14) — that is a real finding, not a rule defect.

Within ~1 minute Prometheus (Status → Targets) should list `podMonitor/itops/tmvault-*`
targets as UP and `probe/itops/tmvault-ingress` as UP. Quick checks from the Prometheus pod:

```bash
P=prometheus-kube-prometheus-stack-prometheus-0
kubectl -n monitoring exec $P -c prometheus -- wget -qO- 'http://127.0.0.1:9090/api/v1/query?query=count%20by%20(job)%20(up%7Bnamespace%3D%22itops%22%2Cstack%3D%22tmvault%22%7D)'
kubectl -n monitoring exec $P -c prometheus -- wget -qO- 'http://127.0.0.1:9090/api/v1/query?query=probe_success%7Bjob%3D%22tmvault-ingress%22%7D'
kubectl -n monitoring exec $P -c prometheus -- promtool check rules /dev/stdin < <(kubectl -n itops get prometheusrule tmvault-alerts -o jsonpath='{.spec}' | python3 -c 'import json,sys,yaml;print(yaml.safe_dump(json.load(sys.stdin)))')
```

If every target of a PodMonitor is DOWN with `connection refused`/`i/o timeout`, the
NetworkPolicy is missing or the exporter port changed in code (see the table below).
`tests/deploy/test_monitoring_manifests.py` pins the ports, selectors, metric names, job
labels, severities and runbook anchors to the code and the other manifests.

## How the pods are scraped

No TM Vault Deployment declares its metrics port as a `containerPort`, and
prometheus-operator implements `port:`/`portNumber:` as a keep-relabel on the *declared*
container ports — so each endpoint instead rewrites `__address__` to
`<podIP>:<exporter port>`. `jobLabel: app` makes the `job` label equal the pod's `app` label.

| PodMonitor | app label(s) | port | exporter in code |
|---|---|---|---|
| tmvault-backup-worker | backup-worker, backup-worker-heavy | 9103 | `shared/core_metrics.py` (`CORE_METRICS_PORT`) |
| tmvault-backup-scheduler | backup-scheduler | 9101 | `shared/sla_metrics.py` (`SLA_METRICS_PORT`); same registry also on 9103 |
| tmvault-restore-worker | restore-worker | 9100 | `shared/pst_metrics.py` (`PST_METRICS_PORT`); same registry also on 9103 |
| tmvault-chat-export-worker | chat-export-worker | 9102 | `workers/chat-export-worker/main.py` (process metrics only) |
| tmvault-aux-workers | discovery-worker, azure-workload-worker | 9103 | `shared/core_metrics.py` |
| tmvault-api-services | auth-, tenant-, resource-, job-, snapshot-, dashboard-, alert-service, delta-token, audit-service | 9103 | `shared/core_metrics.py` |
| tmvault-rabbitmq | tmvault-rabbitmq | 15692 | `rabbitmq_prometheus` plugin: `/metrics` + `/metrics/detailed?family=queue_coarse_metrics&family=queue_metrics` |

Not scraped (nothing listens): progress-tracker, report-service, api-gateway, tmvault-frontend,
tmvault-redis.

## Alerts

Routing is the cluster default (Alertmanager, verified 2026-10-14): **critical** → on-call
email + ITO Jira ticket; **warning** → ITO Jira ticket only. Every alert carries
`service: tmvault`, `team: it-ops` and a `runbook_url` into `deploy/taylor-morrison/RUNBOOK.md`
("Prometheus alerts" under section 5).

| Alert | Severity | Fires when | Needs |
|---|---|---|---|
| TMVaultWorkerDown | critical | no `up==1` for job `backup-worker` for 10m | PodMonitor tmvault-backup-worker |
| TMVaultHeavyWorkerDown | critical | same for `backup-worker-heavy` | same |
| TMVaultSchedulerDown | critical | no `up==1` for job `backup-scheduler` for 10m | PodMonitor tmvault-backup-scheduler |
| TMVaultRabbitmqDown | critical | no `up==1` for job `tmvault-rabbitmq` for 5m | PodMonitor tmvault-rabbitmq |
| TMVaultRedisDown | critical | `kube_deployment_status_replicas_available{deployment="tmvault-redis"} == 0` for 5m | kube-state-metrics (Redis has no exporter) |
| TMVaultTargetDown | warning | `up == 0` for 15m on any other scraped pod | the other PodMonitors |
| TMVaultFailedSnapshotsSpike | warning | > 10 `tmvault_snapshots_total{status="FAILED"}` per type in 1h, for 15m | backup workers' core_metrics |
| TMVaultSnapshotFailureRatioHigh | critical | > 25% of ≥ 20 terminal snapshots in 1h FAILED, for 15m | same |
| TMVaultNoSnapshotsCompleted | warning | zero COMPLETED/PARTIAL snapshots in 12h, for 1h | same (assumes a continuous backup cycle — lengthen the window if a tenant goes idle) |
| TMVaultSlaAuditPublishFailing | warning | `sla_audit_publish_failed_total` increased in 1h | backup-scheduler sla_metrics |
| TMVaultWorkerMemoryHigh | warning | working set > 85% of the container memory limit for 15m | cAdvisor + kube-state-metrics; container names from 40-services/50-workers |
| TMVaultWorkerMemoryCritical | critical | > 95% for 5m | same |
| TMVaultContainerOOMKilled | warning | restart in the last 1h with last reason OOMKilled | kube-state-metrics |
| TMVaultQueueBacklog | warning | > 1000 ready messages in a non-DLQ queue for 30m | rabbitmq `/metrics/detailed` endpoint |
| TMVaultQueueNoConsumers | warning | backup/restore/discovery/azure queue has ready messages and 0 consumers for 15m | same |
| TMVaultDeadLetterQueueParked | warning | any `*.dlq` / `dlq.*` queue non-empty for 30m | same |
| TMVaultRabbitmqResourceAlarm | critical | memory / disk / fd alarm raised for 2m | rabbitmq `/metrics` endpoint |
| TMVaultIngressProbeFailing | critical | `probe_success{job="tmvault-ingress"}` 0 for 10m | the Probe |

Thresholds marked as guesses (queue backlog 1000, snapshot spike 10/h, 12h idle window) are
starting points; review after a week of data in Grafana before trusting them for paging.

## Known gaps (deliberately not alerted)

- **Graph 429 ratio** — `tmvault_graph_api_calls_total` / `tmvault_graph_throttles_total` are
  defined in `shared/core_metrics.py` but no production code path increments them (only the
  unused `time_graph_call()` helper); only `tmvault_graph_rate_limited_waits_total` from
  `shared/graph_rate_limiter.py` is live. The rule is in `prometheusrule-tmvault.yaml`
  commented out; wire the counters, confirm samples, then enable it.
- `tmvault_queue_depth` / `tmvault_worker_active_jobs` / `tmvault_worker_rss_mb` are only set by
  `services/autoscaler`, which is not deployed — the RabbitMQ and cAdvisor series are used instead.
- `tmvault_jobs_total` is never incremented outside a docstring; snapshot counters are used.
- Pod crash loops / replica mismatch / waiting containers are already covered cluster-wide by
  the kube-prometheus-stack rules (KubePodCrashLooping, KubeDeploymentReplicasMismatch,
  KubeContainerWaiting) and are not duplicated here.
- Expiry of the `wildcard-tls` certificate is covered by x509-cert-exporter / `tls-certificate-expiry`.
- Postgres (`databases/postgres-ha`) has its own CNPG PodMonitor and rules.
