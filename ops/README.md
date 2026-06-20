# Azure ops

## Exports retention (1 day)

Apply `azure-lifecycle-exports.json` to every prod storage shard:

    az storage account management-policy create \
      --account-name $AZURE_STORAGE_ACCOUNT_NAME \
      --policy @ops/azure-lifecycle-exports.json \
      --resource-group $AZURE_BACKUP_RESOURCE_GROUP

Idempotent — re-applying is a no-op.

## Tier-2 discovery parallelism

Tier-2 discovery creates the per-user workload resources under an `ENTRA_USER`:
`USER_MAIL`, `USER_ONEDRIVE`, `USER_CONTACTS`, `USER_CALENDAR`, and
`USER_CHATS`. These rows must exist before scheduled or manual user backups
can fan out to actual workload backup jobs.

The system parallelizes Tier-2 discovery in two layers:

- Producers split `discovery.tier2` publishes into small user chunks. RabbitMQ
  distributes those chunks across however many `discovery-worker` replicas are
  running, so this stays portable across Railway, Kubernetes, ECS, or another
  cloud.
- Each `discovery-worker` processes users inside its chunk with bounded
  concurrency. Each user still probes the five workload types in parallel.

Environment defaults are sized for large tenants around 4,000 users:

```text
TIER2_DISCOVERY_MESSAGE_CHUNK_SIZE=25
TIER2_DISCOVERY_USER_CONCURRENCY=4
```

With 4,000 users, the default chunk size creates `4000 / 25 = 160` queue
messages, enough for multiple discovery-worker replicas to share the run. Per
worker, concurrency 4 means up to `4 * 5 = 20` workload probes before the shared
Graph rate limiter applies backpressure.

Scale `discovery-worker` replicas first. If Graph throttling and database
pressure stay healthy, raise `TIER2_DISCOVERY_USER_CONCURRENCY` gradually, for
example from `4` to `6`. Avoid very large chunk sizes for large tenants because
one huge message can be owned by one worker while other replicas sit idle.
