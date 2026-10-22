#!/usr/bin/env bash
# WARNING (ITO-9652 final review): regenerating this file clobbers the
# immutable @sha256 image digest pins applied by build-images.sh /
# the deploy runbook. This script writes floating :$TAG image refs.
# After any regen, re-apply the correct per-repo digests to
# 40-services.yaml before `kubectl apply` — never apply a freshly
# generated file with floating tags straight to prod. Regen also drops
# the hand-written header comments (rollout order, INTERNAL_API_KEY
# secret-first note) — restore them from git.
#
# Every secret var is enumerated per service because services do NOT use
# envFrom/secretRef (only 50-workers.yaml does). A key missing from this
# list can never reach a service pod, which is exactly how the audit-write
# gate ended up un-configurable — hence INTERNAL_API_KEY below.
set -euo pipefail
REG=harbor.taylormorrison.com/library
TAG=${1:-pilot}
OUT=~/tm_vault_backend/deploy/taylor-morrison/k8s/40-services.yaml
: > "$OUT"
# name port replicas
# graph-proxy runs 2 replicas (reality as of ITO-9652 final review) — it
# fronts Graph API throttling/pacing and a single replica was a SPOF for
# every downstream service's Graph calls.
SVCS="
auth-service 8001 1
tenant-service 8002 1
resource-service 8003 1
job-service 8004 1
snapshot-service 8005 1
dashboard-service 8006 1
alert-service 8007 1
backup-scheduler 8008 1
graph-proxy 8009 2
delta-token 8010 1
progress-tracker 8011 1
audit-service 8012 1
search-service 8013 1
report-service 8014 1
api-gateway 8000 1
"
echo "$SVCS" | while read -r name port replicas; do
  [ -z "$name" ] && continue
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
          ports: [ { containerPort: $port } ]
          envFrom:
            - configMapRef: { name: tmvault-config }
          env:
            - { name: PORT, value: "$port" }
            - name: DB_PASSWORD
              valueFrom: { secretKeyRef: { name: tmvault-secrets, key: DB_PASSWORD } }
            - name: JWT_SECRET
              valueFrom: { secretKeyRef: { name: tmvault-secrets, key: JWT_SECRET } }
            - name: ENCRYPTION_KEY
              valueFrom: { secretKeyRef: { name: tmvault-secrets, key: ENCRYPTION_KEY } }
            - name: INTERNAL_API_KEY
              valueFrom: { secretKeyRef: { name: tmvault-secrets, key: INTERNAL_API_KEY } }
            - name: RABBITMQ_PASSWORD
              valueFrom: { secretKeyRef: { name: tmvault-secrets, key: RABBITMQ_PASSWORD } }
            - name: ONPREM_S3_ACCESS_KEY
              valueFrom: { secretKeyRef: { name: tmvault-secrets, key: ONPREM_S3_ACCESS_KEY } }
            - name: ONPREM_S3_SECRET_KEY
              valueFrom: { secretKeyRef: { name: tmvault-secrets, key: ONPREM_S3_SECRET_KEY } }
            - name: SPA_CLIENT_ID
              valueFrom: { secretKeyRef: { name: tmvault-secrets, key: SPA_CLIENT_ID } }
            - name: SPA_CLIENT_SECRET
              valueFrom: { secretKeyRef: { name: tmvault-secrets, key: SPA_CLIENT_SECRET } }
            - name: SPA_TENANT_ID
              valueFrom: { secretKeyRef: { name: tmvault-secrets, key: SPA_TENANT_ID } }
            - name: APP_1_CLIENT_ID
              valueFrom: { secretKeyRef: { name: tmvault-secrets, key: APP_1_CLIENT_ID } }
            - name: APP_1_CLIENT_SECRET
              valueFrom: { secretKeyRef: { name: tmvault-secrets, key: APP_1_CLIENT_SECRET } }
            - name: APP_1_TENANT_ID
              valueFrom: { secretKeyRef: { name: tmvault-secrets, key: APP_1_TENANT_ID } }
          readinessProbe:
            httpGet: { path: /health, port: $port }
            initialDelaySeconds: 10
            periodSeconds: 15
          livenessProbe:
            httpGet: { path: /health, port: $port }
            initialDelaySeconds: 30
            periodSeconds: 30
          resources:
            requests: { cpu: "50m", memory: "128Mi" }
            limits: { cpu: "500m", memory: "512Mi" }
---
apiVersion: v1
kind: Service
metadata: { name: $name, namespace: itops, labels: { app: $name, stack: tmvault } }
spec:
  selector: { app: $name }
  ports: [ { port: $port, targetPort: $port } ]
YAML
done
echo "wrote $OUT"
