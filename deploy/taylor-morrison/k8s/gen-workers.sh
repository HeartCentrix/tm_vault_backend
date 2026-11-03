#!/usr/bin/env bash
# WARNING (ITO-9652 final review): regenerating this file clobbers the
# immutable @sha256 image digest pins applied by build-images.sh /
# the deploy runbook. This script writes floating :$TAG image refs.
# After any regen, re-apply the correct per-repo digests to
# 50-workers.yaml before `kubectl apply` — never apply a freshly
# generated file with floating tags straight to prod.
set -euo pipefail
REG=harbor.taylormorrison.com/library
TAG=${1:-pilot}
OUT=~/tm_vault_backend/deploy/taylor-morrison/k8s/50-workers.yaml
: > "$OUT"
# queue-consumer workers: no HTTP probe. discovery/azure-workload don't need S3 keys
# but including them is harmless and keeps the template uniform.
WORKERS="backup-worker restore-worker discovery-worker chat-export-worker azure-workload-worker"
for name in $WORKERS; do
  # discovery-worker holds large tenant resource lists in memory during scoped
  # fan-out — give it a 2Gi ceiling; all other workers stay at 1Gi.
  if [ "$name" = "discovery-worker" ]; then
    mem_limit="2Gi"
  else
    mem_limit="1Gi"
  fi
  # replicas: match current fleet reality (ITO-9652 final review). backup-worker
  # runs 10 to keep pace with pgbouncer-pooled ingest concurrency; all other
  # light-lane workers stay at 1. (backup-worker-heavy is its own block below.)
  if [ "$name" = "backup-worker" ]; then
    replicas=10
  else
    replicas=1
  fi
  cat >> "$OUT" <<YAML
---
apiVersion: apps/v1
kind: Deployment
metadata: { name: $name, namespace: itops, labels: { app: $name, stack: tmvault } }
spec:
  replicas: $replicas
  selector: { matchLabels: { app: $name } }
  template:
    metadata: { labels: { app: $name, stack: tmvault } }
    spec:
      imagePullSecrets: [ { name: harbor-creds } ]
      securityContext:
        runAsNonRoot: true
        runAsUser: 10001
        runAsGroup: 10001
        fsGroup: 10001
        seccompProfile: { type: RuntimeDefault }
      containers:
        - name: $name
          image: $REG/tmvault-$name:$TAG
          securityContext:
            allowPrivilegeEscalation: false
            capabilities: { drop: ["ALL"] }
            runAsNonRoot: true
          envFrom: [ { configMapRef: { name: tmvault-config } } ]
          env:
            - { name: DB_PASSWORD,         valueFrom: { secretKeyRef: { name: tmvault-secrets, key: DB_PASSWORD } } }
            - { name: JWT_SECRET,          valueFrom: { secretKeyRef: { name: tmvault-secrets, key: JWT_SECRET } } }
            - { name: ENCRYPTION_KEY,      valueFrom: { secretKeyRef: { name: tmvault-secrets, key: ENCRYPTION_KEY } } }
            - { name: RABBITMQ_PASSWORD,   valueFrom: { secretKeyRef: { name: tmvault-secrets, key: RABBITMQ_PASSWORD } } }
            - { name: ONPREM_S3_ACCESS_KEY,valueFrom: { secretKeyRef: { name: tmvault-secrets, key: ONPREM_S3_ACCESS_KEY } } }
            - { name: ONPREM_S3_SECRET_KEY,valueFrom: { secretKeyRef: { name: tmvault-secrets, key: ONPREM_S3_SECRET_KEY } } }
            - { name: APP_1_CLIENT_ID,     valueFrom: { secretKeyRef: { name: tmvault-secrets, key: APP_1_CLIENT_ID } } }
            - { name: APP_1_CLIENT_SECRET, valueFrom: { secretKeyRef: { name: tmvault-secrets, key: APP_1_CLIENT_SECRET } } }
            - { name: APP_1_TENANT_ID,     valueFrom: { secretKeyRef: { name: tmvault-secrets, key: APP_1_TENANT_ID } } }
          resources:
            requests: { cpu: "100m", memory: "256Mi" }
            limits: { cpu: "1", memory: "${mem_limit}" }
YAML
done

# backup-worker-heavy: dedicated deployment consuming the "backup.heavy"
# queue lane for large OneDrive drives. Shares the backup-worker image but
# is pinned to 1 concurrent large-drive backup per pod (MAX_CONCURRENT_
# ONEDRIVE_BACKUPS_PER_WORKER=1) so it never steals urgent/normal lanes
# from the light pool. Kept as its own block (not part of $WORKERS) because
# its env/resources/rollout strategy differ from the generic template.
# replicas=2 matches current fleet reality (ITO-9652 final review).
cat >> "$OUT" <<YAML
---
apiVersion: apps/v1
kind: Deployment
metadata: { name: backup-worker-heavy, namespace: itops, labels: { app: backup-worker-heavy, stack: tmvault } }
spec:
  replicas: 2
  # maxSurge=0 keeps a roll under the itops memory ResourceQuota (no surge pod).
  strategy: { rollingUpdate: { maxSurge: 0, maxUnavailable: 1 } }
  selector: { matchLabels: { app: backup-worker-heavy } }
  template:
    metadata: { labels: { app: backup-worker-heavy, stack: tmvault } }
    spec:
      imagePullSecrets: [ { name: harbor-creds } ]
      securityContext:
        runAsNonRoot: true
        runAsUser: 10001
        runAsGroup: 10001
        fsGroup: 10001
        seccompProfile: { type: RuntimeDefault }
      containers:
        - name: backup-worker
          image: $REG/tmvault-backup-worker:$TAG
          securityContext:
            allowPrivilegeEscalation: false
            capabilities: { drop: ["ALL"] }
            runAsNonRoot: true
          envFrom: [ { configMapRef: { name: tmvault-config } } ]
          env:
            - { name: BACKUP_WORKER_QUEUE, value: "backup.heavy" }
            - { name: MAX_CONCURRENT_ONEDRIVE_BACKUPS_PER_WORKER, value: "1" }
            - { name: BACKUP_CHATS_PARTITION_PREFETCH, value: "2" }  # 2026-10-15: prefetch == per-tenant cap (2) so pods stop hoarding claimed shards
            - { name: BACKUP_MAIL_PARTITION_PREFETCH,  value: "2" }
            - { name: DB_PASSWORD,         valueFrom: { secretKeyRef: { name: tmvault-secrets, key: DB_PASSWORD } } }
            - { name: JWT_SECRET,          valueFrom: { secretKeyRef: { name: tmvault-secrets, key: JWT_SECRET } } }
            - { name: ENCRYPTION_KEY,      valueFrom: { secretKeyRef: { name: tmvault-secrets, key: ENCRYPTION_KEY } } }
            - { name: RABBITMQ_PASSWORD,   valueFrom: { secretKeyRef: { name: tmvault-secrets, key: RABBITMQ_PASSWORD } } }
            - { name: ONPREM_S3_ACCESS_KEY,valueFrom: { secretKeyRef: { name: tmvault-secrets, key: ONPREM_S3_ACCESS_KEY } } }
            - { name: ONPREM_S3_SECRET_KEY,valueFrom: { secretKeyRef: { name: tmvault-secrets, key: ONPREM_S3_SECRET_KEY } } }
            - { name: APP_1_CLIENT_ID,     valueFrom: { secretKeyRef: { name: tmvault-secrets, key: APP_1_CLIENT_ID } } }
            - { name: APP_1_CLIENT_SECRET, valueFrom: { secretKeyRef: { name: tmvault-secrets, key: APP_1_CLIENT_SECRET } } }
            - { name: APP_1_TENANT_ID,     valueFrom: { secretKeyRef: { name: tmvault-secrets, key: APP_1_TENANT_ID } } }
          resources:
            requests: { cpu: "200m", memory: "1Gi" }
            limits: { cpu: "2", memory: "4Gi" }
YAML

echo "wrote $OUT"
