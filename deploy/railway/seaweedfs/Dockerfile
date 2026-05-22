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
# TODO(supply-chain, D-C5): pin by digest, e.g.
#     FROM chrislusf/seaweedfs:3.71@sha256:<digest>
# Obtain the current digest with:
#     docker buildx imagetools inspect chrislusf/seaweedfs:3.71 \
#       --format '{{.Manifest.Digest}}'
# A floating tag can be force-pushed by the upstream maintainer; only a
# digest commits the build to a specific bit-for-bit image.
FROM chrislusf/seaweedfs:3.71

# Drop privileges (D-C5). SeaweedFS does not need root — it binds to
# high ports (8333/9333) and writes only to /data (the runtime-mounted
# volume) and /tmp (where entrypoint.sh writes the rendered S3 config).
# A container escape or path-traversal in SeaweedFS would otherwise
# yield root on the host data volume — which on prod is the 250 TiB
# customer backup store.
#
# Operational note: on Railway, the attached volume at /data must be
# owned by UID 10001 (the `seaweed` user created below). If you see
# permission-denied writing to /data after this change, chown the
# mount from a one-shot debug container: `chown -R 10001:10001 /data`.
RUN addgroup -S seaweed \
    && adduser -S -G seaweed -u 10001 seaweed \
    && mkdir -p /data \
    && chown -R seaweed:seaweed /data

COPY --chown=seaweed:seaweed deploy/railway/seaweedfs/entrypoint.sh /usr/local/bin/tmvault-entrypoint.sh
# Strip CRLF in case the script was checked out on Windows; alpine /bin/sh
# refuses to interpret a "\r" in the shebang. Also enforce executable mode.
RUN sed -i 's/\r$//' /usr/local/bin/tmvault-entrypoint.sh \
    && chmod 0755 /usr/local/bin/tmvault-entrypoint.sh

USER 10001

EXPOSE 8333 9333

ENTRYPOINT ["/usr/local/bin/tmvault-entrypoint.sh"]
CMD ["-logtostderr=true", \
     "-stderrthreshold=WARNING", \
     "server", \
     "-dir=/data", \
     "-ip=0.0.0.0", \
     "-s3", \
     "-s3.config=/tmp/s3.json", \
     "-filer", \
     "-master.volumeSizeLimitMB=5120", \
     "-volume.max=24", \
     "-volume.index=leveldb2", \
     "-volume.preStopSeconds=1", \
     "-master.volumePreallocate=false"]
