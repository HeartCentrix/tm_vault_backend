#!/bin/sh
# SeaweedFS entrypoint — renders the S3 identity config from env vars at
# container start instead of baking credentials into the image layer.
#
# Rationale (see audit D-C4): anyone with pull access to the registry
# image can `docker save | tar -x` and extract any file COPY'd at build
# time, so secrets must never live inside an image layer. Railway (and
# any reasonable orchestrator) injects ONPREM_S3_ACCESS_KEY /
# ONPREM_S3_SECRET_KEY at runtime; this script writes them to a
# tmpfs-style path that exists only for the life of the container.
set -eu

: "${ONPREM_S3_ACCESS_KEY:?ONPREM_S3_ACCESS_KEY must be set — do not bake S3 credentials into the image}"
: "${ONPREM_S3_SECRET_KEY:?ONPREM_S3_SECRET_KEY must be set — do not bake S3 credentials into the image}"
: "${ONPREM_S3_IDENTITY_NAME:=tmvault}"

# Render the S3 config onto the PERSISTENT, seaweed-owned /data volume — NOT
# ephemeral /tmp. On Railway /tmp is not reliably writable by the runtime
# user across restarts, which crash-looped the container ("can't create
# /tmp/s3.json: Permission denied"). /data is chowned to the seaweed user
# below, so it stays writable on every restart.
CONFIG_PATH="${ONPREM_S3_CONFIG_PATH:-/data/s3.json}"

# Refuse to run with the legacy demo credentials. These strings appeared
# in the old committed s3.json; if they ever reach prod via a stale
# secret reference, fail fast.
case "${ONPREM_S3_ACCESS_KEY}" in
  tmvault-local-access|admin|guest|"")
    echo "ONPREM_S3_ACCESS_KEY is a known-weak/demo value; refusing to start." >&2
    exit 1
    ;;
esac
case "${ONPREM_S3_SECRET_KEY}" in
  tmvault-local-secret|admin|guest|"")
    echo "ONPREM_S3_SECRET_KEY is a known-weak/demo value; refusing to start." >&2
    exit 1
    ;;
esac

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

# Railway mounts the persistent volume at /data ROOT-OWNED at runtime,
# which shadows the build-time `chown seaweed:seaweed /data`. The non-root
# seaweed user (UID 10001) then crash-loops on
# "mkdir /data/m9333: permission denied" (see Dockerfile note + issue #717).
#
# Fix without giving up the non-root hardening (D-C5): when the container
# starts as root, take ownership of the mount POINT only — NON-recursive,
# so it is O(1) regardless of volume size and safe for the 250 TiB prod
# store (weed creates and owns its own subdirectories as 10001 from here).
# Hand the rendered S3 config to seaweed too, then drop privileges and run
# the long-lived process as the unprivileged seaweed user via su-exec.
DATA_DIR="${SEAWEED_DATA_DIR:-/data}"
if [ "$(id -u)" = "0" ]; then
  chown seaweed:seaweed "${DATA_DIR}" 2>/dev/null || true
  chmod u+rwx "${DATA_DIR}" 2>/dev/null || true
  chown seaweed:seaweed "${CONFIG_PATH}" 2>/dev/null || true
  exec su-exec seaweed /usr/bin/weed "$@"
fi

# Already unprivileged (e.g. local run with a pre-chowned volume).
exec /usr/bin/weed "$@"
