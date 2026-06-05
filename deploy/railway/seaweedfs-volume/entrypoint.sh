#!/bin/sh
# SeaweedFS VOLUME-SERVER entrypoint (sharding). Each volume-server is its own
# Railway service with its own volume at /data; it registers with the master on
# the coordinator service and holds a fraction of the blobs. Separate container
# = separate memory, so no single instance hits the dirty-page OOM that the
# all-in-one did under heavy write.
set -eu

: "${SEAWEED_MASTER:?SEAWEED_MASTER must be set, e.g. seaweedfs.railway.internal:9333}"
DATA_DIR="${SEAWEED_DATA_DIR:-/data}"
# Advertise THIS server's Railway-internal hostname so the master + S3 can
# reach it for reads/writes. Railway injects RAILWAY_PRIVATE_DOMAIN.
ADVERTISE="${SEAWEED_ADVERTISE_IP:-${RAILWAY_PRIVATE_DOMAIN:-0.0.0.0}}"
VMAX="${SEAWEED_VOLUME_MAX:-100}"
PORT="${SEAWEED_VOLUME_PORT:-8080}"

set -- volume \
  -mserver="${SEAWEED_MASTER}" \
  -dir="${DATA_DIR}" \
  -ip="${ADVERTISE}" \
  -ip.bind=0.0.0.0 \
  -port="${PORT}" \
  -max="${VMAX}" \
  -index=leveldb2 \
  -preStopSeconds=1

# Start as root to chown the runtime-mounted /data volume (Railway mounts it
# root-owned), then drop to the unprivileged seaweed user via su-exec.
if [ "$(id -u)" = "0" ]; then
  chown seaweed:seaweed "${DATA_DIR}" 2>/dev/null || true
  chmod u+rwx "${DATA_DIR}" 2>/dev/null || true
  exec su-exec seaweed /usr/bin/weed "$@"
fi
exec /usr/bin/weed "$@"
