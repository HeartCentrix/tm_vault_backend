# SeaweedFS volume-server for Railway (sharding). One per Railway service, each
# with its own volume at /data, registering with the master on the coordinator.
FROM chrislusf/seaweedfs:3.71@sha256:c47aadbe7b6f78ff52e03bd0a27c6f61c2b27640cba3cf1215cbcafd9045122f

RUN apk add --no-cache su-exec \
    && addgroup -S seaweed \
    && adduser -S -G seaweed -u 10001 seaweed \
    && mkdir -p /data \
    && chown -R seaweed:seaweed /data

COPY --chown=seaweed:seaweed deploy/railway/seaweedfs-volume/entrypoint.sh /usr/local/bin/tmvault-vol-entrypoint.sh
RUN sed -i 's/\r$//' /usr/local/bin/tmvault-vol-entrypoint.sh \
    && chmod 0755 /usr/local/bin/tmvault-vol-entrypoint.sh

# Root start so the entrypoint can chown the runtime-mounted /data; it drops to
# the seaweed user via su-exec before exec'ing weed (long-lived process non-root).
USER root
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/tmvault-vol-entrypoint.sh"]
