# PgBouncer service

A transaction-pool PgBouncer in front of Railway-managed Postgres. Lets us scale workers past the raw PG `max_connections` ceiling.

## Why

At 5k users × ~50-worker scale we'd need ~2,500 PG connections. Railway-managed PG caps at 800 (and even raising that to 3000 is wasteful — backends idle 90% of the time). PgBouncer in `pool_mode=transaction` lets 5,000 client connections share ~400 backend connections.

## Deploy on Railway

1. **Create a new Railway service** in the `tm_vault` project, name it `pgbouncer`.
2. Point it at this directory as the build source (`services/pgbouncer/Dockerfile`).
3. Set the following env vars on the service:

   | Variable | Value |
   |----------|-------|
   | `POSTGRESQL_HOST` | `${{Postgres-n0Hq.PGHOST}}` |
   | `POSTGRESQL_PORT` | `${{Postgres-n0Hq.PGPORT}}` |
   | `POSTGRESQL_USERNAME` | `${{Postgres-n0Hq.PGUSER}}` |
   | `POSTGRESQL_PASSWORD` | `${{Postgres-n0Hq.PGPASSWORD}}` |
   | `POSTGRESQL_DATABASE` | `${{Postgres-n0Hq.PGDATABASE}}` |

   (Use Railway's reference-variable syntax so PgBouncer auto-updates when the PG password rotates.)

4. **Generate userlist.txt** so PgBouncer can authenticate clients against the SCRAM hash. From a Railway shell against the Postgres service:

   ```bash
   psql $DATABASE_URL -tAc "SELECT '\"' || usename || '\" \"' || passwd || '\"' FROM pg_shadow WHERE usename = 'postgres'" \
     > /tmp/userlist.txt
   ```

   Mount that into the PgBouncer container at `/opt/bitnami/pgbouncer/conf/userlist.txt`. On Railway you can do this via a volume or a build-time secret file.

5. **Switch app workers** to use PgBouncer's host instead of PG directly. On every worker / service, change `DB_HOST` to `${{pgbouncer.RAILWAY_PRIVATE_DOMAIN}}` and `DB_PORT` to `6432`. Keep `DB_USERNAME` / `DB_PASSWORD` / `DB_NAME` unchanged.

6. **Leave migrations bypassing PgBouncer.** Add a `DATABASE_URL_DIRECT` env on each service that still points at the raw PG service. `alembic upgrade head` should run against the direct DSN to avoid transaction-pool restrictions on DDL.

## Tuning knobs

The Dockerfile defaults are sized for **5k users × ~50 backup workers × pool 50 each = 2,500 client conns**. Adjust via env on the service:

| Var | Default | When to change |
|-----|---------|---------------|
| `PGBOUNCER_DEFAULT_POOL_SIZE` | 400 | Lower if PG can't sustain that many backends — watch `pg_stat_activity` count. Raise if `cl_waiting` is consistently high in `SHOW POOLS`. |
| `PGBOUNCER_MAX_CLIENT_CONN` | 5000 | Total clients × pool 50 ceiling. At 50 workers × 50 conn = 2500; 5000 leaves slack. |
| `PGBOUNCER_RESERVE_POOL_SIZE` | 50 | Burst capacity. Don't go above 25% of `default_pool_size`. |
| `PGBOUNCER_QUERY_WAIT_TIMEOUT` | 120 | Client-side timeout for getting a server backend. Match this with app-side connect timeouts. |

## Observability

PgBouncer exposes a stats console on the admin port:

```sql
psql 'host=pgbouncer port=6432 user=pgbouncer dbname=pgbouncer'
> SHOW POOLS;
> SHOW STATS;
> SHOW CLIENTS;
```

A `pgbouncer_exporter` sidecar can publish these as Prometheus metrics; pair it with the autoscaler's CORE_METRICS_PORT (9103) scrape config.

## Sanity test

After cutover, every service should still come up. Quick check:

```bash
railway logs --service backup_worker | grep "TooManyConnections"
```

Should be empty. If you see them, raise `PGBOUNCER_DEFAULT_POOL_SIZE` and the backend `max_connections` together.
