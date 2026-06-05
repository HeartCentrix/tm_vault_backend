#!/bin/sh
# SeaweedFS COORDINATOR entrypoint (sharding Phase 2).
#
# Runs master + filer + s3 ONLY — NO local volume server. With no local
# volume to prefer, the master is forced to place every write on the external
# seaweedfs-vol-* services (each its own Railway service + volume). That gives
# real write distribution (no single-instance dirty-page OOM) and lets the
# blob store scale ~linearly with volume-server count.
#
# The all-in-one prior version kept a local volume server with huge capacity,
# so the master never used the external servers (they sat at 0 volumes). This
# split fixes that.
set -eu

: "${ONPREM_S3_ACCESS_KEY:?ONPREM_S3_ACCESS_KEY must be set — do not bake S3 credentials into the image}"
: "${ONPREM_S3_SECRET_KEY:?ONPREM_S3_SECRET_KEY must be set — do not bake S3 credentials into the image}"
: "${ONPREM_S3_IDENTITY_NAME:=tmvault}"

CONFIG_PATH="${ONPREM_S3_CONFIG_PATH:-/data/s3.json}"
VOL_SIZE_MB="${SEAWEED_VOLUME_SIZE_LIMIT_MB:-30000}"
# Railway injects the service's own internal hostname here; the master/filer
# advertise it so the volume servers + s3 can reach them. Bind 0.0.0.0.
ADV="${RAILWAY_PRIVATE_DOMAIN:-0.0.0.0}"

# Refuse known-weak/demo credentials (D-C4).
case "${ONPREM_S3_ACCESS_KEY}" in
  tmvault-local-access|admin|guest|"") echo "ONPREM_S3_ACCESS_KEY is weak/demo; refusing." >&2; exit 1;; esac
case "${ONPREM_S3_SECRET_KEY}" in
  tmvault-local-secret|admin|guest|"") echo "ONPREM_S3_SECRET_KEY is weak/demo; refusing." >&2; exit 1;; esac

umask 077
cat > "${CONFIG_PATH}" <<EOF
{
  "identities": [
    {
      "name": "${ONPREM_S3_IDENTITY_NAME}",
      "credentials": [
        { "accessKey": "${ONPREM_S3_ACCESS_KEY}", "secretKey": "${ONPREM_S3_SECRET_KEY}" }
      ],
      "actions": ["Admin"]
    }
  ],
  "buckets": { "objectLockEnabled": true, "versioning": "Enabled" }
}
EOF

# Filer metadata store -> persistent leveldb2 on /data (NOT in-memory, or S3
# objects become unfindable after a restart). The filer reads filer.toml from
# /etc/seaweedfs.
mkdir -p /data/master /data/filerdb /etc/seaweedfs
cat > /etc/seaweedfs/filer.toml <<TOML
[leveldb2]
enabled = true
dir = "/data/filerdb"
TOML

start_cluster() {
  # master (background) — coordinates volume placement across the external
  # volume servers; volumeSizeLimitMB controls per-volume rollover.
  /usr/bin/weed master -ip="${ADV}" -ip.bind=0.0.0.0 -port=9333 -mdir=/data/master \
      -volumeSizeLimitMB="${VOL_SIZE_MB}" -volumePreallocate=false &
  sleep 5
  # filer (background) — talks to the local master; persistent leveldb2 store.
  /usr/bin/weed filer -master=127.0.0.1:9333 -ip="${ADV}" -ip.bind=0.0.0.0 -port=8888 &
  sleep 5
  # s3 (foreground / PID 1 after exec) — the endpoint the workers hit.
  exec /usr/bin/weed s3 -filer=127.0.0.1:8888 -ip.bind=0.0.0.0 -port=8333 -config="${CONFIG_PATH}"
}

# Start as root to chown the runtime-mounted /data (Railway mounts it root-
# owned), then drop to the unprivileged seaweed user via su-exec. The
# coordinator's /data now holds only metadata (master + filerdb), so the
# recursive chown is cheap.
DATA_DIR="${SEAWEED_DATA_DIR:-/data}"
if [ "$(id -u)" = "0" ]; then
  chown -R seaweed:seaweed "${DATA_DIR}" /etc/seaweedfs 2>/dev/null || true
  chmod u+rwx "${DATA_DIR}" 2>/dev/null || true
  # re-exec this script as seaweed so the 3 children all run unprivileged
  exec su-exec seaweed "$0" "$@"
fi

start_cluster
