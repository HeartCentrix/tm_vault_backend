# PgBouncer for TMvault — transaction-pooled connections in front of
# the managed Postgres instance.
#
# Why bitnami/pgbouncer:
#  - maintained, multi-arch (arm64/amd64)
#  - reads config from env vars + mounted pgbouncer.ini
#  - logs to stdout (Railway-friendly)
#
# Built-in healthcheck hits the admin console on :6432.

FROM bitnami/pgbouncer:1.22.1

# Render the config from env at container start. The image's entrypoint
# already supports POSTGRESQL_HOST / POSTGRESQL_PORT / POSTGRESQL_USERNAME
# / POSTGRESQL_PASSWORD / POSTGRESQL_DATABASE — but TMvault wants pool-
# tuning beyond what their env vars expose (pool_mode, max_client_conn,
# pool_size, etc.) so we ship a custom pgbouncer.ini and let the
# entrypoint env-substitute the connection target.

COPY services/pgbouncer/pgbouncer.ini /etc/pgbouncer/pgbouncer.ini

ENV PGBOUNCER_AUTH_TYPE=scram-sha-256
ENV PGBOUNCER_POOL_MODE=transaction
ENV PGBOUNCER_MAX_CLIENT_CONN=5000
ENV PGBOUNCER_DEFAULT_POOL_SIZE=400
ENV PGBOUNCER_RESERVE_POOL_SIZE=50
ENV PGBOUNCER_RESERVE_POOL_TIMEOUT=3
ENV PGBOUNCER_QUERY_WAIT_TIMEOUT=120
ENV PGBOUNCER_SERVER_IDLE_TIMEOUT=60
ENV PGBOUNCER_SERVER_LIFETIME=3600
ENV PGBOUNCER_LISTEN_PORT=6432

EXPOSE 6432
