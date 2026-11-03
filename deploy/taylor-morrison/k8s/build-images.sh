#!/usr/bin/env bash
set -euo pipefail
REG=harbor.taylormorrison.com/library
TAG=${1:-pilot}
cd "$(git rev-parse --show-toplevel)"   # repo root = docker build context

# name<TAB>dockerfile-path
SERVICES="
api-gateway	api-gateway/Dockerfile
auth-service	services/auth-service/Dockerfile
tenant-service	services/tenant-service/Dockerfile
resource-service	services/resource-service/Dockerfile
job-service	services/job-service/Dockerfile
snapshot-service	services/snapshot-service/Dockerfile
dashboard-service	services/dashboard-service/Dockerfile
alert-service	services/alert-service/Dockerfile
backup-scheduler	services/backup-scheduler/Dockerfile
graph-proxy	services/graph-proxy/Dockerfile
delta-token	services/delta-token/Dockerfile
progress-tracker	services/progress-tracker/Dockerfile
audit-service	services/audit-service/Dockerfile
search-service	services/search-service/Dockerfile
report-service	services/report-service/Dockerfile
backup-worker	workers/backup-worker/Dockerfile
restore-worker	workers/restore-worker/Dockerfile
discovery-worker	workers/discovery-worker/Dockerfile
chat-export-worker	workers/chat-export-worker/Dockerfile
azure-workload-worker	workers/azure-workload-worker/Dockerfile
"
echo "$SERVICES" | while IFS=$'\t' read -r name df; do
  [ -z "$name" ] && continue
  img="$REG/tmvault-$name:$TAG"
  echo "==> building $img (-f $df)"
  docker build -t "$img" -f "$df" .
  docker push "$img"
done
