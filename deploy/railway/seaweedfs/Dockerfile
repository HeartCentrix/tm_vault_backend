# SeaweedFS for Railway — thin wrapper over the official image. Data
# persists via a Railway volume mounted at /data (attached per-service
# via the Railway dashboard or `railway volume` CLI — not declared here,
# since Docker VOLUME would shadow the attached mount on Railway).
#
# Internal URL once deployed:
#   http://seaweedfs.railway.internal:8333   (S3 API)
#   http://seaweedfs.railway.internal:9333   (master, not needed by clients)
#
# Clients (backup-worker, restore-worker, snapshot-service, …) reach
# it via the ONPREM_S3_ENDPOINT / ONPREM_S3_ACCESS_KEY / ONPREM_S3_SECRET_KEY
# env vars. Those same vars are consumed by entrypoint.sh at container
# start to render /tmp/s3.json — credentials are NEVER baked into the
# image layer (see D-C4).
#
# Pinned by digest (D-C5 / D-H2). Refresh with:
#     docker buildx imagetools inspect chrislusf/seaweedfs:3.71 \
#       --format '{{.Manifest.Digest}}'
# A floating tag can be force-pushed by the upstream maintainer; only a
# digest commits the build to a specific bit-for-bit image. This image
# guards the 250 TiB customer backup store — supply-chain integrity is
# load-bearing.
FROM chrislusf/seaweedfs:3.71@sha256:c47aadbe7b6f78ff52e03bd0a27c6f61c2b27640cba3cf1215cbcafd9045122f

# Drop privileges (D-C5). SeaweedFS does not need root — it binds to
# high ports (8333/9333) and writes only to /data (the runtime-mounted
# volume) and /tmp (where entrypoint.sh writes the rendered S3 config).
# A container escape or path-traversal in SeaweedFS would otherwise
# yield root on the host data volume — which on prod is the 250 TiB
# customer backup store.
#
# Railway mounts the attached /data volume ROOT-OWNED at runtime, which
# shadows the build-time chown below and previously crash-looped the
# non-root process ("mkdir /data/m9333: permission denied"). Rather than a
# manual one-shot `chown -R 10001 /data`, the entrypoint now starts as root,
# chowns the mount POINT (non-recursive — O(1), safe for the 250 TiB store),
# then drops to UID 10001 via su-exec. So we deliberately do NOT set
# `USER 10001` here — the entrypoint owns the privilege drop.
# su-exec lets the entrypoint start as root (to chown the runtime-mounted
# /data volume), then drop to the unprivileged seaweed user before exec'ing
# the long-lived weed process — D-C5 hardening is preserved.
RUN apk add --no-cache su-exec \
    && addgroup -S seaweed \
    && adduser -S -G seaweed -u 10001 seaweed \
    && mkdir -p /data \
    && chown -R seaweed:seaweed /data

COPY --chown=seaweed:seaweed deploy/railway/seaweedfs/entrypoint.sh /usr/local/bin/tmvault-entrypoint.sh
# Strip CRLF in case the script was checked out on Windows; alpine /bin/sh
# refuses to interpret a "\r" in the shebang. Also enforce executable mode.
RUN sed -i 's/\r$//' /usr/local/bin/tmvault-entrypoint.sh \
    && chmod 0755 /usr/local/bin/tmvault-entrypoint.sh

# NOTE: no `USER 10001` — see the note above. The container starts as root
# so the entrypoint can chown the runtime-mounted /data volume; it then
# drops to the seaweed user via su-exec before exec'ing weed. The
# long-lived process never runs as root.

EXPOSE 8333 9333 8888

# Force a root start so the entrypoint can chown the runtime-mounted /data
# volume and render /data/s3.json on EVERY restart. The entrypoint drops to the
# unprivileged seaweed user via su-exec, so the long-lived weed processes never
# run as root — D-C5 hardening preserved.
USER root

# COORDINATOR mode (sharding Phase 2): the entrypoint launches master + filer
# + s3 itself (NO local volume server) — blob storage lives on the external
# seaweedfs-vol-* services. No CMD args needed; the entrypoint owns the cluster
# launch.
ENTRYPOINT ["/usr/local/bin/tmvault-entrypoint.sh"]
