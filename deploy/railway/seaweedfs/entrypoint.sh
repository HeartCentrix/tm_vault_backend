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

CONFIG_PATH="${ONPREM_S3_CONFIG_PATH:-/tmp/s3.json}"

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

exec /usr/bin/weed "$@"
