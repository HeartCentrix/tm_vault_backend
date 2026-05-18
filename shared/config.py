"""Shared configuration for all microservices"""
import os
from typing import List
from urllib.parse import quote, urlparse


class Settings:
    def __init__(self):
        # Railway provides DATABASE_URL directly. Be permissive and also
        # accept a full Postgres URL accidentally pasted into DB_HOST.
        self._database_url_override = self._resolve_database_url_override()
        if self._database_url_override:
            parsed = urlparse(self._database_url_override)
            self.DB_HOST = parsed.hostname or "localhost"
            self.DB_PORT = str(parsed.port or "5432")
            self.DB_NAME = parsed.path.lstrip("/") if parsed.path else "tm_vault_db"
            self.DB_USERNAME = parsed.username or "postgres"
            self.DB_PASSWORD = parsed.password or ""
        else:
            self.DB_HOST = os.getenv("DB_HOST")
            self.DB_PORT = os.getenv("DB_PORT", "5432")
            self.DB_NAME = os.getenv("DB_NAME")
            self.DB_USERNAME = os.getenv("DB_USERNAME")
            self.DB_PASSWORD = os.getenv("DB_PASSWORD")

        self.DB_SCHEMA = os.getenv("DB_SCHEMA", "public")
        # Pool sizing: 2+2 was producing TooManyConnectionsError on
        # Defaults sized for production backup workers: 32 concurrent
        # OneDrive file uploads × per-upload snapshot_items inserts means
        # a pool ≪ 32 serializes the worker on asyncpg.acquire(). Old
        # default (5+5) silently capped a 4-replica deployment to
        # ~40 concurrent DB ops total — the smoking gun behind the
        # "slow during ingest" symptom Railway exhibited.
        #
        # 30+20 covers steady-state + burst (FILE_VERSION batch flush,
        # chat-message bulk inserts). Per-replica × replica-count must
        # stay under postgres max_connections; calibrate via env when
        # going beyond 6-8 worker replicas, or front PG with PgBouncer
        # (transaction-mode) and treat pool size as in-flight transaction
        # slots instead of backend conns.
        #
        # Connection budget (current deployment):
        #   16 services + 5 workers = 21 processes, 50 max conns each
        #   ⇒ theoretical peak ≈ 1050 conns (rare; only if every process
        #     bursts simultaneously). Realistic concurrent peak during a
        #     bulk backup is ~10 hot processes × 50 = ~500 conns.
        #   Railway PG must be configured with POSTGRES_MAX_CONNECTIONS=800
        #   (set on the Postgres service, not on app services). Local
        #   docker-compose already runs PG with `-c max_connections=800`.
        #   Less than 800 → "FATAL: sorry, too many clients already"
        #   surfaces as SQLAlchemy QueuePool / asyncpg TooManyConnections
        #   errors during bulks. Do NOT shrink DB_POOL_SIZE /
        #   DB_MAX_OVERFLOW to "fix" this — pool starvation cascades into
        #   stuck snapshots. Raise PG max_connections instead.
        self.DB_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "30"))
        self.DB_MAX_OVERFLOW = int(os.getenv("DB_MAX_OVERFLOW", "20"))
        self.DB_POOL_TIMEOUT = int(os.getenv("DB_POOL_TIMEOUT", "30"))
        self.DB_POOL_RECYCLE = int(os.getenv("DB_POOL_RECYCLE", "1800"))
        self.DB_POOL_USE_LIFO = os.getenv("DB_POOL_USE_LIFO", "true").lower() in ("true", "1", "yes")
        self.JWT_SECRET = os.getenv("JWT_SECRET", "")
        self.JWT_ALGORITHM = "HS256"
        # Access TTL is short on purpose: a stolen access cookie is useful
        # for at most this long. The SPA's auto-refresh hides the boundary
        # from the user, so 1h has zero UX cost. Override via env in dev for
        # faster iteration. Refresh TTL stays at 7d — that one is bounded by
        # rotation + revocation, not by clock.
        self.JWT_EXPIRATION_HOURS = int(os.getenv("JWT_EXPIRATION_HOURS", "1"))
        self.JWT_REFRESH_EXPIRATION_DAYS = int(os.getenv("JWT_REFRESH_EXPIRATION_DAYS", "7"))
        # Distinct per-class secrets prevent access<->refresh token swaps. Fall
        # back to JWT_SECRET so existing single-secret deployments keep booting;
        # the type-claim check in decode_token is the primary defense.
        self.ACCESS_TOKEN_SECRET = os.getenv("ACCESS_TOKEN_SECRET", "") or self.JWT_SECRET
        self.REFRESH_TOKEN_SECRET = os.getenv("REFRESH_TOKEN_SECRET", "") or self.JWT_SECRET
        # Shared secret for service-to-service calls on internal-only services
        # (delta-token, etc.). Callers send it as the X-Internal-Api-Key header.
        # Must be set in every environment — empty disables the affected
        # services (they fail closed with 503).
        self.INTERNAL_API_KEY = os.getenv("INTERNAL_API_KEY", "")

        # Auth cookies (HttpOnly access_token / refresh_token). The browser
        # never reads them — XSS can no longer steal a Bearer token from
        # localStorage. `Secure` is auto-on when FRONTEND_URL is https; on
        # local http://localhost dev we must leave it off so the browser
        # accepts the cookie at all. Override with COOKIE_SECURE=true|false.
        cookie_secure_env = os.getenv("COOKIE_SECURE", "").strip().lower()
        _frontend_url_for_cookie = os.getenv("FRONTEND_URL", "http://localhost:4200")
        if cookie_secure_env in ("true", "1", "yes"):
            self.COOKIE_SECURE = True
        elif cookie_secure_env in ("false", "0", "no"):
            self.COOKIE_SECURE = False
        else:
            self.COOKIE_SECURE = _frontend_url_for_cookie.startswith("https://")
        # SameSite=Strict blocks the cookie on every cross-site request,
        # including link-clicks from email/Slack into the SPA. Override
        # to "lax" via env if you want deep links to keep the session.
        self.COOKIE_SAMESITE = os.getenv("COOKIE_SAMESITE", "strict").strip().lower()
        # Cookie domain (omit to default to the request host). Set when
        # the SPA and the API are on sibling subdomains.
        self.COOKIE_DOMAIN = os.getenv("COOKIE_DOMAIN", "") or None

        # Railway provides REDIS_URL; fall back to individual vars.
        # Critical: REDIS_URL on Railway carries the auth password —
        # earlier code parsed host/port/db but dropped the password,
        # causing every Redis op on Railway to crash with
        # `AuthenticationError: Authentication required`. The chat-
        # export-worker hit it first (no try/except around the
        # `progress.publish` call); auth-service silently swallowed
        # the same error in a dev-mode fallback. Save the password
        # separately and expose a `REDIS_URL_FULL` property that
        # callers can hand directly to `Redis.from_url(...)`.
        railway_redis_url = os.getenv("REDIS_URL")
        if railway_redis_url:
            parsed = urlparse(railway_redis_url)
            self.REDIS_HOST = parsed.hostname or "localhost"
            self.REDIS_PORT = parsed.port or 6379
            self.REDIS_DB = int((parsed.path.lstrip("/") or "0"))
            self.REDIS_PASSWORD = parsed.password or os.getenv("REDIS_PASSWORD", "") or ""
            self.REDIS_USERNAME = parsed.username or os.getenv("REDIS_USERNAME", "") or ""
        else:
            self.REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
            self.REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
            self.REDIS_DB = int(os.getenv("REDIS_DB", "0"))
            self.REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "") or ""
            self.REDIS_USERNAME = os.getenv("REDIS_USERNAME", "") or ""
        self.REDIS_ENABLED = os.getenv("REDIS_ENABLED", "false").lower() in ("true", "1", "yes")
        # Single source of truth for any caller that hands a URL to
        # redis-py (Redis.from_url / aioredis.from_url). Encodes the
        # password with quote() so URL-unsafe characters in Railway-
        # generated credentials (e.g. '/', '+', '=') don't break
        # parsing. Username is normally empty on Railway managed
        # Redis (default user, password-only auth).
        from urllib.parse import quote as _q
        if self.REDIS_PASSWORD:
            _auth = (
                f"{_q(self.REDIS_USERNAME, safe='')}:" if self.REDIS_USERNAME else ":"
            ) + f"{_q(self.REDIS_PASSWORD, safe='')}@"
        else:
            _auth = ""
        self.REDIS_URL_FULL = (
            f"redis://{_auth}{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"
        )

        # Railway provides RABBITMQ_URL or AMQP_URL; fall back to individual vars
        railway_rabbitmq_url = os.getenv("RABBITMQ_URL") or os.getenv("AMQP_URL")
        if railway_rabbitmq_url:
            parsed = urlparse(railway_rabbitmq_url)
            self.RABBITMQ_HOST = parsed.hostname or "localhost"
            self.RABBITMQ_PORT = parsed.port or 5672
            # Empty rather than "guest" — the RABBITMQ_URL property fails
            # closed if creds aren't supplied, which is safer than silently
            # connecting as the default RabbitMQ user.
            self.RABBITMQ_USER = parsed.username or ""
            self.RABBITMQ_PASSWORD = parsed.password or ""
        else:
            self.RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
            self.RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
            self.RABBITMQ_USER = (
                os.getenv("RABBITMQ_USERNAME") or os.getenv("RABBITMQ_USER", "")
            )
            self.RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "")
        self.RABBITMQ_ENABLED = os.getenv("RABBITMQ_ENABLED", "false").lower() in ("true", "1", "yes")
        self.AZURE_STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "")
        self.AZURE_STORAGE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY", "")
        self.AZURE_STORAGE_BLOB_ENDPOINT = os.getenv("AZURE_STORAGE_BLOB_ENDPOINT", "https://blob.core.windows.net")

        # On-prem SeaweedFS backend (enabled when a storage_backends row exists)
        self.ONPREM_S3_ENDPOINT = os.getenv("ONPREM_S3_ENDPOINT", "")
        self.ONPREM_S3_ACCESS_KEY = os.getenv("ONPREM_S3_ACCESS_KEY", "")
        self.ONPREM_S3_SECRET_KEY = os.getenv("ONPREM_S3_SECRET_KEY", "")
        onprem_buckets = os.getenv("ONPREM_S3_BUCKETS", "")
        self.ONPREM_S3_BUCKETS = [b.strip() for b in onprem_buckets.split(",") if b.strip()]
        self.ONPREM_S3_REGION = os.getenv("ONPREM_S3_REGION", "us-east-1")
        self.ONPREM_S3_VERIFY_TLS = os.getenv("ONPREM_S3_VERIFY_TLS", "true").lower() in ("true", "1", "yes")
        self.ONPREM_S3_CA_BUNDLE = os.getenv("ONPREM_S3_CA_BUNDLE", "") or None
        # S3 multipart parts in flight per file upload. 16 keeps the
        # per-file throughput ceiling well above realistic Graph download
        # rates (8 MB chunk × 16 / 5 ms RTT ≈ 25 GB/s); RAM per upload is
        # bounded by chunk_size × concurrency = 8 MB × 16 = 128 MB, fits
        # under ONEDRIVE_HUGE_FILE_RAM_BUDGET_GIB. Old default of 8
        # silently capped Seaweed/S3 ingest at half what the upstream
        # ONEDRIVE_LARGE_FILE_SEGMENT_CONCURRENCY=8 could feed it.
        self.ONPREM_UPLOAD_CONCURRENCY = int(os.getenv("ONPREM_UPLOAD_CONCURRENCY", "16"))
        self.ONPREM_MULTIPART_THRESHOLD_MB = int(os.getenv("ONPREM_MULTIPART_THRESHOLD_MB", "100"))
        self.ONPREM_RETRY_MAX = int(os.getenv("ONPREM_RETRY_MAX", "3"))

        # Azure ARM (Azure Resource Manager) credentials for VM/SQL/PostgreSQL backup
        # Service Principal for ARM API access
        self.AZURE_ARM_CLIENT_ID = os.getenv("AZURE_ARM_CLIENT_ID", "")
        self.AZURE_ARM_CLIENT_SECRET = os.getenv("AZURE_ARM_CLIENT_SECRET", "")
        self.AZURE_ARM_TENANT_ID = os.getenv("AZURE_ARM_TENANT_ID", "")
        self.AZURE_SUBSCRIPTION_ID = os.getenv("AZURE_SUBSCRIPTION_ID", "")

        # Azure Backup resource group (for VM restore point collections)
        self.AZURE_BACKUP_RESOURCE_GROUP = os.getenv("AZURE_BACKUP_RESOURCE_GROUP", "rg-tmvault-backup")
        # Backup storage region (for RPC placement)
        self.AZURE_BACKUP_REGION = os.getenv("AZURE_BACKUP_REGION", "eastus")
        
        # High-Performance Backup Configuration
        # Parallelism: Max concurrent Graph API calls per worker.
        # Raised 50 -> 100 when we moved to a single backup-worker replica; the
        # per-replica asyncio.Semaphore covers what three replicas × 50 used to.
        # Tune higher only if Graph throttling (multi_app_manager) isn't saturating.
        self.BACKUP_CONCURRENCY = int(os.getenv("BACKUP_CONCURRENCY", "100"))
        # Parallelism: Max concurrent Server-Side Copy operations
        self.COPY_CONCURRENCY = int(os.getenv("COPY_CONCURRENCY", "100"))
        # File size threshold (bytes) - above this, use Server-Side Copy
        self.SERVER_SIDE_COPY_THRESHOLD = int(os.getenv("SERVER_SIDE_COPY_THRESHOLD", "10485760"))  # 10MB
        # Workload parallelism: concurrent jobs per workload type
        self.WORKLOAD_CONCURRENCY = int(os.getenv("WORKLOAD_CONCURRENCY", "5"))
        # Storage sharding: number of storage accounts to distribute across
        self.STORAGE_SHARD_COUNT = int(os.getenv("STORAGE_SHARD_COUNT", "1"))
        # Comma-separated list of storage account names (for sharding)
        storage_shards = os.getenv("STORAGE_SHARD_ACCOUNTS", "")
        self.STORAGE_SHARD_ACCOUNTS = [s.strip() for s in storage_shards.split(",") if s.strip()] if storage_shards else []
        # Comma-separated list of storage account keys (matching order)
        storage_shard_keys = os.getenv("STORAGE_SHARD_KEYS", "")
        self.STORAGE_SHARD_KEYS = [k.strip() for k in storage_shard_keys.split(",") if k.strip()] if storage_shard_keys else []
        # Retry configuration
        self.MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))

        # Backup streaming performance settings (cloud-to-cloud, matches Afi.ai architecture)
        # Azure Blob supports up to 4GB block size, 50,000 blocks per blob
        # 100MB blocks are optimal for throughput (per Azure perf tuning docs)
        self.AZURE_BLOCK_SIZE_MB = int(os.getenv("AZURE_BLOCK_SIZE_MB", "100"))
        # Parallel block uploads per file (5-8 is optimal)
        self.AZURE_UPLOAD_CONCURRENCY = int(os.getenv("AZURE_UPLOAD_CONCURRENCY", "5"))
        # (duplicate removed — single canonical declaration above sets BACKUP_CONCURRENCY)
        # How many resource groups to process in parallel (workload parallelism)
        self.WORKLOAD_CONCURRENCY = int(os.getenv("WORKLOAD_CONCURRENCY", "5"))
        self.RETRY_DELAY_MS = int(os.getenv("RETRY_DELAY_MS", "2000"))
        self.RETRY_BACKOFF_MULTIPLIER = float(os.getenv("RETRY_BACKOFF_MULTIPLIER", "2.0"))
        # Encryption key for storing secrets (Fernet key, base64-encoded 32-byte key)
        self.ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY", "")
        # Batch size for Graph API $batch endpoint
        self.GRAPH_BATCH_SIZE = int(os.getenv("GRAPH_BATCH_SIZE", "20"))
        # Chunk size for processing resources
        self.RESOURCE_CHUNK_SIZE = int(os.getenv("RESOURCE_CHUNK_SIZE", "50"))
        # Discovery staging / merge batch sizes for large tenant onboarding
        self.DISCOVERY_STAGE_CHUNK_SIZE = int(os.getenv("DISCOVERY_STAGE_CHUNK_SIZE", "500"))
        self.DISCOVERY_PROGRESS_LOG_EVERY = int(os.getenv("DISCOVERY_PROGRESS_LOG_EVERY", "250"))

        # ── Mail export (MBOX / EML) — see docs/superpowers/specs/2026-04-19-mbox-mail-export-design.md ──
        self.EXPORT_PARALLELISM = int(os.getenv("EXPORT_PARALLELISM", "12"))
        self.EXPORT_MBOX_SPLIT_BYTES = int(os.getenv("EXPORT_MBOX_SPLIT_BYTES", str(5 * 1024 * 1024 * 1024)))
        self.EXPORT_BLOCK_SIZE_BYTES = int(os.getenv("EXPORT_BLOCK_SIZE_BYTES", str(4 * 1024 * 1024)))
        self.EXPORT_FOLDER_QUEUE_MAXSIZE = int(os.getenv("EXPORT_FOLDER_QUEUE_MAXSIZE", "20"))
        self.MAX_CONCURRENT_EXPORTS_PER_WORKER = int(os.getenv("MAX_CONCURRENT_EXPORTS_PER_WORKER", "2"))
        # Deployment-wide default for the per-tenant chat-export gate.
        # Legacy behaviour (SaaS canary rollout) kept /api/v1/exports/chat
        # locked behind `tenant.extra_data.limits.chat_export_enabled` —
        # every new tenant had to be opted in by hand via
        # scripts/flip_chat_export_flag.py, and on-prem single-tenant
        # installs tripped 503 FEATURE_NOT_ENABLED on the first click.
        # With this flag true (default) the gate honours an explicit
        # tenant-level opt-out (limits.chat_export_enabled=false) but
        # otherwise allows. SaaS operators running a progressive rollout
        # can set CHAT_EXPORT_DEFAULT_ENABLED=false to restore the old
        # "explicit opt-in per tenant" behaviour.
        self.CHAT_EXPORT_DEFAULT_ENABLED = os.getenv("CHAT_EXPORT_DEFAULT_ENABLED", "true").lower() == "true"
        # Mail Restore v2 — AFI-parity pipeline. Default on; set
        # MAIL_RESTORE_V2_ENABLED=false in env to roll back to the legacy
        # _restore_email_to_mailbox path if the engine misbehaves.
        self.MAIL_RESTORE_V2_ENABLED = os.getenv("MAIL_RESTORE_V2_ENABLED", "true").lower() == "true"
        # Activity batch-row redesign — first-class backup_batches row per click.
        # Flag-off behaviour: legacy CTE rollup, batch_batches row inserted but
        # unused by reads. Flag-on: tenant-service mandates the row, job-service
        # validates batchId presence, audit-service reads backup_batches directly.
        # Rollback = unset env var, redeploy. Spec at
        # docs/superpowers/specs/2026-05-15-backup-batch-row-redesign-design.md.
        self.BATCH_ROW_REDESIGN_ENABLED = os.getenv(
            "BATCH_ROW_REDESIGN_ENABLED", "false"
        ).lower() in ("true", "1", "yes")
        self.BATCH_STALL_TIMEOUT_HOURS = int(
            os.getenv("BATCH_STALL_TIMEOUT_HOURS", "24")
        )
        # Watchdog deadline for batch_pending_users rows. Beyond this,
        # the scheduler's watchdog flips WAITING_DISCOVERY → DISCOVERY_FAILED
        # so the batch can finalize as PARTIAL instead of hanging when
        # discovery never publishes a terminal state (worker crash, queue
        # stuck, publish failure). See spec
        # docs/superpowers/specs/2026-05-15-backup-batch-race-fix-design.md.
        self.DISCOVERY_DEADLINE_MIN = int(
            os.getenv("DISCOVERY_DEADLINE_MIN", "60"),
        )
        # Per-worker global cap on concurrent mail-restore tasks across all
        # in-flight jobs. Keeps Graph traffic bounded even if many jobs run at once.
        self.MAIL_RESTORE_GLOBAL_POOL = int(os.getenv("MAIL_RESTORE_GLOBAL_POOL", "32"))
        # Per-target-mailbox concurrency cap. Graph throttles per-mailbox
        # at ~4 concurrent requests; exceeding this triggers 429s faster
        # than the retry loop can absorb them.
        self.MAIL_RESTORE_PER_MAILBOX = int(os.getenv("MAIL_RESTORE_PER_MAILBOX", "4"))
        # Max retries per item on 429 / 5xx before marking it failed.
        self.MAIL_RESTORE_MAX_RETRIES = int(os.getenv("MAIL_RESTORE_MAX_RETRIES", "5"))
        # Small-attachment threshold. >= this size uses Graph's upload-session
        # endpoint (chunked PUT). Units = megabytes.
        self.MAIL_RESTORE_ATTACH_LARGE_MB = int(os.getenv("MAIL_RESTORE_ATTACH_LARGE_MB", "3"))
        # ---- Contact Restore engine ----
        # Batches up to 20 contacts per Graph /$batch call, resolves target
        # contactFolder from snapshot_item.folder_path, and caps concurrency
        # globally + per-user so a 5k-user restore stays inside Graph throttle
        # envelopes. Flag off → legacy per-item POST path (one POST per contact,
        # default Contacts folder only).
        self.CONTACT_RESTORE_ENGINE_ENABLED = os.getenv("CONTACT_RESTORE_ENGINE_ENABLED", "true").lower() == "true"
        self.CONTACT_RESTORE_GLOBAL_POOL = int(os.getenv("CONTACT_RESTORE_GLOBAL_POOL", "32"))
        # Outlook serializes Contacts sub-requests 4-at-a-time per mailbox; any
        # value >4 triggers 429s faster than the retry loop absorbs them.
        self.CONTACT_RESTORE_PER_USER = int(os.getenv("CONTACT_RESTORE_PER_USER", "4"))
        self.CONTACT_RESTORE_MAX_RETRIES = int(os.getenv("CONTACT_RESTORE_MAX_RETRIES", "5"))
        # ---- OneDrive Restore engine ----
        # Streams files back via Graph upload-session for ≥ 4 MB, simple PUT
        # otherwise. Flag off → legacy _restore_file_to_onedrive shim (still
        # delegates to the engine — the flag is an emergency escape hatch).
        self.ONEDRIVE_RESTORE_ENGINE_ENABLED = os.getenv("ONEDRIVE_RESTORE_ENGINE_ENABLED", "true").lower() == "true"
        self.ONEDRIVE_RESTORE_CONCURRENCY = int(os.getenv("ONEDRIVE_RESTORE_CONCURRENCY", "16"))
        self.ONEDRIVE_RESTORE_CHUNK_BYTES = int(os.getenv("ONEDRIVE_RESTORE_CHUNK_BYTES", str(10 * 1024 * 1024)))
        self.ONEDRIVE_RESTORE_PER_TARGET_USER_CAP = int(os.getenv("ONEDRIVE_RESTORE_PER_TARGET_USER_CAP", "5"))
        # Files above this size restore via uploadSession fed directly
        # by the backend's download_stream — avoids materialising the
        # whole blob in worker RAM. Below this, stay on the simpler
        # buffered path for backwards-compatibility with existing
        # restore tests.
        self.ONEDRIVE_RESTORE_STREAMING_THRESHOLD_BYTES = int(os.getenv(
            "ONEDRIVE_RESTORE_STREAMING_THRESHOLD_BYTES",
            str(64 * 1024 * 1024),
        ))
        # ---- Entra Restore v2 ----
        # Default on; set to "false" in env to disable EntraRestoreEngine
        # + EntraExportPipeline and fall back to the legacy PATCH-only
        # restore path for ENTRA_DIR_* items.
        self.ENTRA_RESTORE_V2_ENABLED = os.getenv("ENTRA_RESTORE_V2_ENABLED", "true").lower() == "true"
        # Default on; gates the server-side Entra ZIP export pipeline.
        self.ENTRA_EXPORT_V2_ENABLED = os.getenv("ENTRA_EXPORT_V2_ENABLED", "true").lower() == "true"
        # Files folder-select v2. Enables the folderPaths/excludedItemIds
        # payload on the /export-or-restore endpoint and the generic
        # FileBrowserTable on SharePoint/Teams/Groups tabs. Rollback by
        # setting FILES_FOLDER_SELECT_V2=false in env.
        self.FILES_FOLDER_SELECT_V2 = os.getenv("FILES_FOLDER_SELECT_V2", "true").lower() == "true"
        # Per-worker cap on concurrent Entra-restore tasks across all
        # in-flight jobs. Keeps Graph traffic bounded.
        self.ENTRA_RESTORE_GLOBAL_POOL = int(os.getenv("ENTRA_RESTORE_GLOBAL_POOL", "32"))
        # Per-tenant concurrency cap for $batch / PATCH calls. Graph's
        # directory throttle budget sits around 4-6 concurrent app-only
        # calls — stay under that.
        self.ENTRA_RESTORE_PER_TENANT = int(os.getenv("ENTRA_RESTORE_PER_TENANT", "4"))
        # Max retries per item on 429 / 5xx before marking failed.
        self.ENTRA_RESTORE_MAX_RETRIES = int(os.getenv("ENTRA_RESTORE_MAX_RETRIES", "5"))
        self.EXPORT_FETCH_BATCH_SIZE = int(os.getenv("EXPORT_FETCH_BATCH_SIZE", "50"))
        self.EXPORT_MEMORY_SOFT_LIMIT_PCT = int(os.getenv("EXPORT_MEMORY_SOFT_LIMIT_PCT", "80"))
        self.EXPORT_MEMORY_KILL_GRACE_SECONDS = int(os.getenv("EXPORT_MEMORY_KILL_GRACE_SECONDS", "60"))
        # Default on: the v2 streaming mail export is what powers Download
        # (ZIP / MBOX) from Recovery. Flip off in env only for emergency
        # rollback to the legacy in-memory export path.
        self.EXPORT_MAIL_V2_ENABLED = os.getenv("EXPORT_MAIL_V2_ENABLED", "true").lower() in ("true", "1", "yes")
        # MBOX tiering: folders under this byte size accumulate in memory and go
        # straight into the final ZIP without an intermediate Azure blob.
        # Folders over the limit stream via intermediate blob (bounded memory,
        # ZIP assembly re-reads). Default 100 MiB.
        self.EXPORT_MBOX_INLINE_LIMIT_BYTES = int(os.getenv("EXPORT_MBOX_INLINE_LIMIT_BYTES", str(100 * 1024 * 1024)))

        # ── OneDrive export v2 (see 2026-04-19-onedrive-export-and-backup-uncap-design.md) ──
        # Default on: folder-tree preserving ZIP + single-file raw stream
        # for Download flows. Disable per env for rollback only.
        self.EXPORT_ONEDRIVE_V2_ENABLED = os.getenv("EXPORT_ONEDRIVE_V2_ENABLED", "true").lower() in ("true", "1", "yes")
        self.EXPORT_ONEDRIVE_MISSING_POLICY = os.getenv("EXPORT_ONEDRIVE_MISSING_POLICY", "skip").lower()
        self.EXPORT_ONEDRIVE_INCLUDE_VERSIONS = os.getenv("EXPORT_ONEDRIVE_INCLUDE_VERSIONS", "false").lower() in ("true", "1", "yes")
        self.EXPORT_ONEDRIVE_MAX_FILE_BYTES = int(os.getenv("EXPORT_ONEDRIVE_MAX_FILE_BYTES", str(200 * 1024 * 1024 * 1024)))
        self.EXPORT_ONEDRIVE_PATH_MAX_LEN = int(os.getenv("EXPORT_ONEDRIVE_PATH_MAX_LEN", "260"))
        self.EXPORT_ONEDRIVE_SANITIZE_CHARS = os.getenv("EXPORT_ONEDRIVE_SANITIZE_CHARS", '<>:"/\\|?*')

        # ── OneDrive backup uncap ──
        # Default on: removes the legacy per-drive cap + uses resumable
        # streaming so multi-TB drives survive transient failures.
        self.ONEDRIVE_BACKUP_V2_ENABLED = os.getenv("ONEDRIVE_BACKUP_V2_ENABLED", "true").lower() in ("true", "1", "yes")
        self.ONEDRIVE_BACKUP_FILE_CONCURRENCY = int(os.getenv("ONEDRIVE_BACKUP_FILE_CONCURRENCY", "16"))
        self.MAX_CONCURRENT_ONEDRIVE_BACKUPS_PER_WORKER = int(os.getenv("MAX_CONCURRENT_ONEDRIVE_BACKUPS_PER_WORKER", "4"))
        self.ONEDRIVE_BACKUP_FILE_TIMEOUT_SECONDS = int(os.getenv("ONEDRIVE_BACKUP_FILE_TIMEOUT_SECONDS", "21600"))
        self.ONEDRIVE_BACKUP_CHECKPOINT_EVERY_FILES = int(os.getenv("ONEDRIVE_BACKUP_CHECKPOINT_EVERY_FILES", "500"))
        self.ONEDRIVE_BACKUP_CHECKPOINT_EVERY_BYTES = int(os.getenv("ONEDRIVE_BACKUP_CHECKPOINT_EVERY_BYTES", str(1024 * 1024 * 1024)))
        # Parallel Range-GET for huge files: files above this size use
        # N concurrent Range requests against the Graph pre-signed URL
        # to saturate enterprise bandwidth (single TCP caps ~80 MB/s).
        # Peak mem per file ≈ segment_concurrency * segment_size.
        self.ONEDRIVE_LARGE_FILE_THRESHOLD_BYTES = int(os.getenv(
            "ONEDRIVE_LARGE_FILE_THRESHOLD_BYTES",
            str(256 * 1024 * 1024),
        ))
        self.ONEDRIVE_LARGE_FILE_SEGMENT_BYTES = int(os.getenv(
            "ONEDRIVE_LARGE_FILE_SEGMENT_BYTES",
            str(64 * 1024 * 1024),
        ))
        # Default bumped from 4 to 8 — single-TCP from Graph CDN caps
        # around 50-100 MB/s, so 8 concurrent Range fetches per huge
        # file scale to ~600 MB/s of effective bandwidth on enterprise
        # links. Peak RAM per file = segment_concurrency * segment_size
        # = 8 * 64 MB = 512 MB, still bounded by the global huge-file
        # RAM budget below.
        self.ONEDRIVE_LARGE_FILE_SEGMENT_CONCURRENCY = int(os.getenv(
            "ONEDRIVE_LARGE_FILE_SEGMENT_CONCURRENCY", "8",
        ))

        # ── OneDrive cross-replica partition split ──
        # When a single OneDrive's file work would otherwise pin one
        # backup_worker replica, partition the file_items list into N
        # shards and publish one message per shard so multiple replicas
        # drain the same drive in parallel. Default ON. Falls back to
        # the inline path for drives below the size/file thresholds.
        #
        # Routing:  backup.onedrive_partition (own queue lane, prefetch=2).
        # Gate:     drive_quota_used >= MIN_BYTES AND len(file_items) >= MIN_FILES.
        # Shards:   min(MAX_SHARDS, ceil(total_bytes / TARGET_BYTES_PER_SHARD)).
        # Cap:      MAX_SHARDS=4 → at most 4 partitions per snapshot, matching
        #           a typical 4× backup_worker replica count on Railway.
        self.ONEDRIVE_PARTITION_ENABLED = os.getenv(
            "ONEDRIVE_PARTITION_ENABLED", "true",
        ).lower() in ("true", "1", "yes")
        self.ONEDRIVE_PARTITION_MIN_BYTES = int(os.getenv(
            "ONEDRIVE_PARTITION_MIN_BYTES", str(5 * 1024 * 1024 * 1024),
        ))
        self.ONEDRIVE_PARTITION_MIN_FILES = int(os.getenv(
            "ONEDRIVE_PARTITION_MIN_FILES", "200",
        ))
        self.ONEDRIVE_PARTITION_MAX_SHARDS = int(os.getenv(
            "ONEDRIVE_PARTITION_MAX_SHARDS", "4",
        ))
        self.ONEDRIVE_PARTITION_TARGET_BYTES_PER_SHARD = int(os.getenv(
            "ONEDRIVE_PARTITION_TARGET_BYTES_PER_SHARD",
            str(20 * 1024 * 1024 * 1024),
        ))
        # Partition consumer's per-shard timeout. Default 6h matches the
        # legacy per-file timeout; a stuck shard past this triggers
        # stale-sweep retry.
        self.ONEDRIVE_PARTITION_STALE_SWEEP_MIN = int(os.getenv(
            "ONEDRIVE_PARTITION_STALE_SWEEP_MIN", "30",
        ))

        # ── USER_CHATS cross-replica partition ──
        # Whale-user chat backups (a single user with hundreds of chats
        # across many teams + 1:1s) partition by chat-id batches so
        # multiple backup_worker replicas drain the same user's chats in
        # parallel. Each shard reuses the existing USER_CHATS pipeline
        # with a `chat_ids_filter` scoping its drain to the shard's chats.
        # Default ON; falls back to inline when the user has fewer chats
        # than MIN_CHATS.
        self.CHATS_PARTITION_ENABLED = os.getenv(
            "CHATS_PARTITION_ENABLED", "true",
        ).lower() in ("true", "1", "yes")
        # 2026-05-17 prod tuning: lowered 100 → 25 so medium-chat users
        # (e.g. 30-99 chats — the long tail of regular employees) also
        # get the partition treatment. With the 8-light replica fleet this
        # means ~4750 of 5K users fan out to 1-2 shards each, parallelising
        # across the backup.chats_partition lane instead of crowding
        # backup.high serially. Tiny users (<25 chats) still run inline
        # to avoid the per-shard publish overhead.
        self.CHATS_PARTITION_MIN_CHATS = int(os.getenv(
            "CHATS_PARTITION_MIN_CHATS", "25",
        ))
        self.CHATS_PARTITION_TARGET_CHATS_PER_SHARD = int(os.getenv(
            "CHATS_PARTITION_TARGET_CHATS_PER_SHARD", "50",
        ))
        # 2026-05-17 prod tuning: 4 → 6 so power users (executives with
        # 500+ chats) drain ~33% faster. The 8-light × 12-prefetch =
        # 96-shard cluster budget for backup.chats_partition still
        # comfortably absorbs the new shard count.
        self.CHATS_PARTITION_MAX_SHARDS = int(os.getenv(
            "CHATS_PARTITION_MAX_SHARDS", "6",
        ))

        # ── Phase 3.2: Mail partition (covers USER_MAIL / MAILBOX /
        # SHARED_MAILBOX / ROOM_MAILBOX — all four share the same
        # backup_mailbox handler, so one set of flags applies). ──
        self.MAIL_PARTITION_ENABLED = os.getenv(
            "MAIL_PARTITION_ENABLED", "true",
        ).lower() in ("true", "1", "yes")
        # Minimum folder count to consider partitioning. Smaller
        # mailboxes are dominated by single-folder drains, not folder
        # fan-out, so partitioning adds overhead without benefit.
        self.MAIL_PARTITION_MIN_FOLDERS = int(os.getenv(
            "MAIL_PARTITION_MIN_FOLDERS", "20",
        ))
        # Minimum total mailbox bytes (sum of folder sizeInBytes) for
        # partition split. 2 GiB default — typical enterprise mailbox
        # threshold beyond which single-replica drain serializes too
        # much I/O.
        self.MAIL_PARTITION_MIN_BYTES = int(os.getenv(
            "MAIL_PARTITION_MIN_BYTES", str(2 * 1024 * 1024 * 1024),
        ))
        self.MAIL_PARTITION_MAX_SHARDS = int(os.getenv(
            "MAIL_PARTITION_MAX_SHARDS", "4",
        ))
        self.MAIL_PARTITION_TARGET_BYTES_PER_SHARD = int(os.getenv(
            "MAIL_PARTITION_TARGET_BYTES_PER_SHARD",
            str(2 * 1024 * 1024 * 1024),
        ))

        # ── Phase 3.3: SharePoint site partition (by drive list) ──
        self.SP_PARTITION_ENABLED = os.getenv(
            "SP_PARTITION_ENABLED", "true",
        ).lower() in ("true", "1", "yes")
        self.SP_PARTITION_MIN_DRIVES = int(os.getenv(
            "SP_PARTITION_MIN_DRIVES", "3",
        ))
        self.SP_PARTITION_MIN_BYTES = int(os.getenv(
            "SP_PARTITION_MIN_BYTES", str(5 * 1024 * 1024 * 1024),
        ))
        self.SP_PARTITION_MAX_SHARDS = int(os.getenv(
            "SP_PARTITION_MAX_SHARDS", "4",
        ))
        self.SP_PARTITION_TARGET_BYTES_PER_SHARD = int(os.getenv(
            "SP_PARTITION_TARGET_BYTES_PER_SHARD",
            str(20 * 1024 * 1024 * 1024),
        ))

        # ── Phase 3.4: Groups & Teams partition (by channel list) ──
        # When a single Team has many channels, _backup_teams_resource
        # parallelizes them via asyncio.gather on ONE worker — fine until
        # one worker's bandwidth/Graph-quota becomes the cap. The
        # partition lane lets a fat team's channels split across N
        # worker replicas the same way SP splits one site's drives.
        self.GROUPS_PARTITION_ENABLED = os.getenv(
            "GROUPS_PARTITION_ENABLED", "true",
        ).lower() in ("true", "1", "yes")
        self.GROUPS_PARTITION_MIN_CHANNELS = int(os.getenv(
            "GROUPS_PARTITION_MIN_CHANNELS", "8",
        ))
        self.GROUPS_PARTITION_MAX_SHARDS = int(os.getenv(
            "GROUPS_PARTITION_MAX_SHARDS", "4",
        ))
        self.GROUPS_PARTITION_CHANNELS_PER_SHARD = int(os.getenv(
            "GROUPS_PARTITION_CHANNELS_PER_SHARD", "4",
        ))

        # ── Phase 3.5: Entra directory partition (by category list) ──
        # `backup_entra_directory` captures 8 independent Graph
        # categories (Users / Groups / Roles / Security / Audit /
        # Applications / Intune / Administrative Units) inline and
        # serially. On a 10k-user tenant the Users + Groups fetches
        # alone dominate — Groups is especially slow because every
        # group triggers per-group owners + first-page-members calls.
        # The partition lane splits the 8 categories into shards so
        # different replicas can drain them concurrently.
        self.ENTRA_PARTITION_ENABLED = os.getenv(
            "ENTRA_PARTITION_ENABLED", "true",
        ).lower() in ("true", "1", "yes")
        self.ENTRA_PARTITION_MIN_CATEGORIES = int(os.getenv(
            "ENTRA_PARTITION_MIN_CATEGORIES", "4",
        ))
        self.ENTRA_PARTITION_MAX_SHARDS = int(os.getenv(
            "ENTRA_PARTITION_MAX_SHARDS", "4",
        ))
        self.ENTRA_PARTITION_CATEGORIES_PER_SHARD = int(os.getenv(
            "ENTRA_PARTITION_CATEGORIES_PER_SHARD", "2",
        ))

        # ── Generic partition resilience knobs ──
        # Per-tenant concurrency cap on partition shards (across all
        # partition_types). Prevents one tenant's whale OneDrive +
        # whale chats from monopolizing every partition slot on a
        # replica when other tenants also want service.
        self.MAX_CONCURRENT_PARTITIONS_PER_TENANT = int(os.getenv(
            "MAX_CONCURRENT_PARTITIONS_PER_TENANT", "2",
        ))
        # Stale-sweep retry budget — once retry_count crosses this
        # threshold, the partition row is marked FAILED and stops
        # being re-published. The parent snapshot then flips to
        # PARTIAL on the next finalize.
        self.PARTITION_MAX_RETRIES = int(os.getenv(
            "PARTITION_MAX_RETRIES", "5",
        ))

        # ── Heavy backup pool ──
        # Default on: route OneDrive drives above the heavy threshold to
        # backup.heavy so one monster drive doesn't starve every regular
        # backup-worker replica.
        self.BACKUP_HEAVY_ENABLED = os.getenv("BACKUP_HEAVY_ENABLED", "true").lower() in ("true", "1", "yes")
        self.BACKUP_HEAVY_THRESHOLD_BYTES = int(os.getenv("BACKUP_HEAVY_THRESHOLD_BYTES", str(100 * 1024 * 1024 * 1024)))
        self.BACKUP_HEAVY_QUEUE = os.getenv("BACKUP_HEAVY_QUEUE", "backup.heavy")
        self.BACKUP_WORKER_QUEUE = os.getenv("BACKUP_WORKER_QUEUE", "backup.normal")

        # ── Contact backup expansion ──
        # Capture IPM.Contact items from Deleted Items + Recoverable Items
        # mail folders in addition to /contactFolders. Default on so backups
        # cover the full mailbox-side contact estate. Disable per tenant if
        # Graph perms are restricted.
        self.BACKUP_CONTACTS_INCLUDE_DELETED = os.getenv(
            "BACKUP_CONTACTS_INCLUDE_DELETED", "true"
        ).lower() in ("true", "1", "yes")
        self.BACKUP_CONTACTS_INCLUDE_RECOVERABLE = os.getenv(
            "BACKUP_CONTACTS_INCLUDE_RECOVERABLE", "true"
        ).lower() in ("true", "1", "yes")

        # ── RabbitMQ long-run safety ──
        self.RABBITMQ_CONSUMER_HEARTBEAT_SECONDS = int(os.getenv("RABBITMQ_CONSUMER_HEARTBEAT_SECONDS", str(7 * 24 * 3600)))
        self.RABBITMQ_CONSUMER_TIMEOUT_MS = int(os.getenv("RABBITMQ_CONSUMER_TIMEOUT_MS", str(7 * 24 * 3600 * 1000)))

        # ── Graph API throttle hardening ──
        # Spec: docs/superpowers/specs/2026-04-19-graph-api-throttle-hardening-design.md
        self.GRAPH_HARDENING_ENABLED = os.getenv(
            "GRAPH_HARDENING_ENABLED", "true"
        ).lower() in ("true", "1", "yes")
        # Per-(app, tenant) sustained rate cap. Microsoft Graph's per-app
        # per-tenant ceiling is ~200 req/s before throttling; 40 rps stays
        # at 20% of that soft cap with plenty of headroom for the burst
        # spikes the multi-app rotator can absorb.
        #
        # 2026-05-17 prod tuning: 8 → 40 to use the 20-app fleet
        # efficiently. Cluster-wide budget: 20 apps × 40 RPS = 800 RPS.
        # 12-replica fleet × ~67 RPS share = matches a 50-user manual
        # burst peak (50 × ~10 RPS/user ≈ 500 RPS) with 60% headroom.
        # Microsoft hard ceiling per app is 1000 RPS (10K req/10s window)
        # so 40 RPS is 4% of the hard cap — practically zero risk of
        # admin-visible alerts.
        #
        # Override down (to e.g. 20) for skittish tenant admins;
        # override up (to 60-80) only after measuring 429 rates and
        # confirming no compliance flag.
        self.GRAPH_APP_PACE_REQS_PER_SEC = float(
            os.getenv("GRAPH_APP_PACE_REQS_PER_SEC", "40.0")
        )
        # Per-stream (= per concurrent backup) sustained rate cap.
        # 1.0 → 2.0 so individual user backups can issue ~2 Graph calls/sec
        # of sustained work. The per-app rate-limiter still caps the
        # aggregate, so a single fat user can't monopolize an app.
        self.GRAPH_STREAM_PACE_REQS_PER_SEC = float(
            os.getenv("GRAPH_STREAM_PACE_REQS_PER_SEC", "2.0")
        )
        # Priority scheduling on the Graph rate limiter. When true, HIGH/
        # URGENT callers (user-triggered restores, interactive UI ops)
        # jump the per-app token-bucket queue ahead of NORMAL (scheduled
        # backups). When false, all callers are served at NORMAL priority
        # (byte-identical to pre-priority behaviour).
        # Mapping of queue -> priority lives in shared/graph_priority.py.
        self.GRAPH_PRIORITY_SCHEDULING_ENABLED = os.getenv(
            "GRAPH_PRIORITY_SCHEDULING_ENABLED", "true"
        ).lower() in ("true", "1", "yes")
        self.GRAPH_MAX_RETRIES = int(os.getenv("GRAPH_MAX_RETRIES", "5"))
        # Sequence walked when 429/503 arrives without a Retry-After header.
        # Loops back to the start on exhaustion; the real ceiling is the
        # cumulative-wait cap below.
        #
        # 2026-05-17 prod tuning: 60,120,240,480,600 → 10,30,60,180,300.
        # With 20 apps and the c0c34ed app-migration-on-throttle code, a
        # throttled call should rarely sit on its original app — the
        # rotator migrates to a healthy app on next attempt. The backoff
        # ladder is only the fallback when EVERY app is throttled. A 60s
        # first-hop wait there was over-conservative and added minutes to
        # tail latency on hot tenants.
        self.GRAPH_THROTTLE_BACKOFF_SECONDS = [
            int(x) for x in os.getenv(
                "GRAPH_THROTTLE_BACKOFF_SECONDS", "10,30,60,180,300"
            ).split(",") if x.strip()
        ]
        # Sequence for transient network errors (not throttle).
        self.GRAPH_TRANSIENT_BACKOFF_SECONDS = [
            int(x) for x in os.getenv(
                "GRAPH_TRANSIENT_BACKOFF_SECONDS", "2,4,8,16,32"
            ).split(",") if x.strip()
        ]
        self.GRAPH_JITTER_RATIO = float(os.getenv("GRAPH_JITTER_RATIO", "0.2"))
        # 2026-05-17 prod tuning: 500 → 250. With 20 apps + migration,
        # we don't need to brake the whole replica for half a second
        # after one 429 — the next call rotates to a healthy app and
        # only a 250ms safety margin is needed to avoid micro-burst
        # contention.
        self.GRAPH_POST_THROTTLE_BRAKE_MS = int(
            os.getenv("GRAPH_POST_THROTTLE_BRAKE_MS", "250")
        )
        # Hard cap on total wait time per stream. After this, raise and let
        # RabbitMQ redeliver (resume from last checkpoint).
        self.GRAPH_MAX_CUMULATIVE_WAIT_SECONDS = int(
            os.getenv("GRAPH_MAX_CUMULATIVE_WAIT_SECONDS", "14400")
        )
        # Clamp on the all-apps-throttled wait.
        self.GRAPH_MAX_THROTTLE_WAIT_SECONDS = int(
            os.getenv("GRAPH_MAX_THROTTLE_WAIT_SECONDS", "1800")
        )
        self.GRAPH_STICKY_PAGES_BEFORE_RETURN = int(
            os.getenv("GRAPH_STICKY_PAGES_BEFORE_RETURN", "50")
        )
        self.GRAPH_BATCH_MAX_SIZE = int(os.getenv("GRAPH_BATCH_MAX_SIZE", "20"))

        # Heavy export pool — routes >100 GB-with-attachments exports to a dedicated
        # worker set with a larger memory budget. See spec §13 promoted scope.
        self.HEAVY_EXPORT_THRESHOLD_BYTES = int(os.getenv(
            "HEAVY_EXPORT_THRESHOLD_BYTES", str(100 * 1024 * 1024 * 1024)
        ))
        self.HEAVY_EXPORT_QUEUE = os.getenv("HEAVY_EXPORT_QUEUE", "restore.heavy")
        # Default on: heavy exports (large mail / drive downloads) route
        # to restore.heavy — keeps the normal restore pool responsive.
        self.HEAVY_EXPORT_ENABLED = os.getenv("HEAVY_EXPORT_ENABLED", "true").lower() in ("true", "1", "yes")
        self.RESTORE_WORKER_QUEUE = os.getenv("RESTORE_WORKER_QUEUE", "restore.normal")

        # --- Chat export (v1) ---
        # See docs/superpowers/specs/2026-04-19-teams-chat-download-design.md §11.
        self.chat_export_tenant_concurrent_min: int = int(
            os.getenv("CHAT_EXPORT_TENANT_CONCURRENT_MIN", "200")
        )
        self.chat_export_tenant_concurrent_per_user: float = float(
            os.getenv("CHAT_EXPORT_TENANT_CONCURRENT_PER_USER", "0.5")
        )
        self.chat_export_blob_account_shards: int = int(
            os.getenv("CHAT_EXPORT_BLOB_ACCOUNT_SHARDS", "4")
        )
        _chat_export_blob_accounts = os.getenv("CHAT_EXPORT_BLOB_ACCOUNTS", "").strip()
        self.chat_export_blob_accounts: list[str] = (
            [s.strip() for s in _chat_export_blob_accounts.split(",") if s.strip()]
            if _chat_export_blob_accounts
            else ["stexport1", "stexport2", "stexport3", "stexport4"]
        )
        # "sse" | "poll"
        self.chat_export_progress_transport: str = os.getenv(
            "CHAT_EXPORT_PROGRESS_TRANSPORT", "sse"
        )
        # 20 GB
        self.chat_export_size_soft_cap_bytes: int = int(
            os.getenv("CHAT_EXPORT_SIZE_SOFT_CAP_BYTES", str(21_474_836_480))
        )
        # 1 TiB
        self.chat_export_size_hard_cap_bytes: int = int(
            os.getenv("CHAT_EXPORT_SIZE_HARD_CAP_BYTES", str(1_099_511_627_776))
        )
        self.chat_export_blob_ttl_hours: int = int(
            os.getenv("CHAT_EXPORT_BLOB_TTL_HOURS", "168")
        )
        self.chat_export_sas_ttl_hours: int = int(
            os.getenv("CHAT_EXPORT_SAS_TTL_HOURS", "168")
        )
        self.chat_export_hot_tier_hours: int = int(
            os.getenv("CHAT_EXPORT_HOT_TIER_HOURS", "24")
        )
        self.chat_export_dynamic_prefetch: bool = os.getenv(
            "CHAT_EXPORT_DYNAMIC_PREFETCH", "true"
        ).lower() in ("true", "1", "yes")

        # --- hostedContents capture (backup-worker) ---
        self.chat_hosted_content_concurrency: int = int(
            os.getenv("CHAT_HOSTED_CONTENT_CONCURRENCY", "8")
        )
        self.chat_hosted_content_max_bytes: int = int(
            os.getenv("CHAT_HOSTED_CONTENT_MAX_BYTES", str(25_000_000))
        )

        self.ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
        self.ELASTICSEARCH_ENABLED = False
        origins = os.getenv("CORS_ORIGINS") or os.getenv("CORS_ALLOWED_ORIGINS", "http://localhost:4200,http://localhost:3000,http://localhost:5173")
        self.CORS_ORIGINS = [o.strip() for o in origins.split(",")]

        # Frontend URL for OAuth redirects
        self.FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:4200").rstrip("/")

        # Multi-app registration for Microsoft Graph API
        # Parse from env: APP_1_CLIENT_ID, APP_1_CLIENT_SECRET, APP_1_TENANT_ID, etc.
        self.GRAPH_APPS = self._parse_graph_apps()

        # Microsoft Auth URLs (constructed from tenant ID)
        self._tenant_id = self.GRAPH_APPS[0]["tenant_id"] if self.GRAPH_APPS else "common"
        self.MICROSOFT_AUTH_URL = os.getenv("MICROSOFT_AUTH_URL", f"https://login.microsoftonline.com/{self._tenant_id}/oauth2/v2.0/authorize")
        self.MICROSOFT_TOKEN_URL = os.getenv("MICROSOFT_TOKEN_URL", f"https://login.microsoftonline.com/{self._tenant_id}/oauth2/v2.0/token")

        # Dedicated Power BI / Fabric app credentials (optional).
        # Falls back to the primary Microsoft app when not provided.
        self.POWER_BI_CLIENT_ID = os.getenv("POWER_BI_CLIENT_ID", "")
        self.POWER_BI_CLIENT_SECRET = os.getenv("POWER_BI_CLIENT_SECRET", "")
        self.POWER_BI_TENANT_ID = os.getenv("POWER_BI_TENANT_ID", "")
        self.POWER_BI_FULL_SNAPSHOT_DAYS = int(os.getenv("POWER_BI_FULL_SNAPSHOT_DAYS", "7"))

        # Datasource OAuth URLs (multi-tenant for connecting other orgs)
        self.DATASOURCE_AUTH_URL = os.getenv("DATASOURCE_AUTH_URL", f"https://login.microsoftonline.com/organizations/oauth2/v2.0/authorize")
        self.DATASOURCE_TOKEN_URL = os.getenv("DATASOURCE_TOKEN_URL", f"https://login.microsoftonline.com/organizations/oauth2/v2.0/token")

        # Microservice URLs (Railway or local)
        self.AUTH_SERVICE_URL = os.getenv("AUTH_SERVICE_URL", "http://auth-service:8001")
        self.TENANT_SERVICE_URL = os.getenv("TENANT_SERVICE_URL", "http://tenant-service:8002")
        self.RESOURCE_SERVICE_URL = os.getenv("RESOURCE_SERVICE_URL", "http://resource-service:8003")
        self.JOB_SERVICE_URL = os.getenv("JOB_SERVICE_URL", "http://job-service:8004")
        self.SNAPSHOT_SERVICE_URL = os.getenv("SNAPSHOT_SERVICE_URL", "http://snapshot-service:8005")
        self.DASHBOARD_SERVICE_URL = os.getenv("DASHBOARD_SERVICE_URL", "http://dashboard-service:8006")
        self.ALERT_SERVICE_URL = os.getenv("ALERT_SERVICE_URL", "http://alert-service:8007")
        self.BACKUP_SCHEDULER_URL = os.getenv("BACKUP_SCHEDULER_URL", "http://backup-scheduler:8008")
        self.GRAPH_PROXY_URL = os.getenv("GRAPH_PROXY_URL", "http://graph-proxy:8009")
        self.DELTA_TOKEN_URL = os.getenv("DELTA_TOKEN_URL", "http://delta-token:8010")
        self.PROGRESS_TRACKER_URL = os.getenv("PROGRESS_TRACKER_URL", "http://progress-tracker:8011")
        self.AUDIT_SERVICE_URL = os.getenv("AUDIT_SERVICE_URL", "http://audit-service:8012")
        self.REPORT_SERVICE_URL = os.getenv("REPORT_SERVICE_URL", "http://report-service:8014")

    def _resolve_database_url_override(self) -> str:
        raw_database_url = os.getenv("DATABASE_URL", "").strip()
        if not raw_database_url:
            raw_db_host = os.getenv("DB_HOST", "").strip()
            if raw_db_host.startswith(("postgres://", "postgresql://", "postgresql+asyncpg://")):
                raw_database_url = raw_db_host

        if not raw_database_url:
            return ""

        if raw_database_url.startswith("postgresql+asyncpg://"):
            return raw_database_url
        if raw_database_url.startswith("postgres://"):
            return "postgresql+asyncpg://" + raw_database_url[len("postgres://"):]
        if raw_database_url.startswith("postgresql://"):
            return "postgresql+asyncpg://" + raw_database_url[len("postgresql://"):]
        return raw_database_url

    def _parse_graph_apps(self) -> List[dict]:
        """Parse multiple Graph app registrations from env vars.

        Each app contributes an independent 200 RPS quota against the
        Microsoft Graph per-app-per-tenant throttle ceiling, so the
        aggregate RPS scales linearly with the number of registered
        apps. Enterprise-grade installs (5k users) typically run
        20-30+ apps with dedicated admin consent per customer tenant.

        Format: APP_<N>_CLIENT_ID, APP_<N>_CLIENT_SECRET,
                APP_<N>_TENANT_ID for N in 1..GRAPH_APP_MAX (default 30).
        Sparse configuration is supported — gaps are silently skipped,
        so you can assign APP_1, APP_5, APP_9 without filling the
        intermediate slots. Legacy single-app deployments fall back
        to AZURE_AD_* env names on APP_1.
        """
        apps = []
        max_slots = int(os.getenv("GRAPH_APP_MAX", "30"))
        for i in range(1, max_slots + 1):
            if i == 1:
                client_id = os.getenv(f"APP_{i}_CLIENT_ID") or os.getenv("AZURE_AD_CLIENT_ID", "")
                client_secret = os.getenv(f"APP_{i}_CLIENT_SECRET") or os.getenv("AZURE_AD_CLIENT_SECRET", "")
                tenant_id = os.getenv(f"APP_{i}_TENANT_ID") or os.getenv("AZURE_AD_TENANT_ID", "common")
            else:
                client_id = os.getenv(f"APP_{i}_CLIENT_ID", "")
                client_secret = os.getenv(f"APP_{i}_CLIENT_SECRET", "")
                tenant_id = os.getenv(f"APP_{i}_TENANT_ID", "common")

            if client_id and client_secret:
                apps.append({
                    "index": i,
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "tenant_id": tenant_id,
                })
            # Continue scanning — don't break on gaps. Previous code
            # stopped at the first missing slot, so sparse configs
            # (APP_1 + APP_5 + APP_9) only registered APP_1. That
            # silently cost customers 2/3 of the intended Graph
            # throughput they'd provisioned.

        return apps or [{
            "index": 1,
            "client_id": "",
            "client_secret": "",
            "tenant_id": "common",
        }]

    @property
    def GRAPH_APP_COUNT(self) -> int:
        return len(self.GRAPH_APPS)

    # Backward compatibility properties for auth-service
    @property
    def MICROSOFT_CLIENT_ID(self) -> str:
        return self.GRAPH_APPS[0]["client_id"] if self.GRAPH_APPS else ""

    @property
    def MICROSOFT_CLIENT_SECRET(self) -> str:
        return self.GRAPH_APPS[0]["client_secret"] if self.GRAPH_APPS else ""

    @property
    def MICROSOFT_TENANT_ID(self) -> str:
        return self.GRAPH_APPS[0]["tenant_id"] if self.GRAPH_APPS else "common"

    @property
    def EFFECTIVE_POWER_BI_CLIENT_ID(self) -> str:
        return self.POWER_BI_CLIENT_ID or self.MICROSOFT_CLIENT_ID

    @property
    def EFFECTIVE_POWER_BI_CLIENT_SECRET(self) -> str:
        return self.POWER_BI_CLIENT_SECRET or self.MICROSOFT_CLIENT_SECRET

    @property
    def EFFECTIVE_POWER_BI_TENANT_ID(self) -> str:
        return self.POWER_BI_TENANT_ID or self.MICROSOFT_TENANT_ID

    # ARM credentials fallback to Graph app if not explicitly set
    @property
    def EFFECTIVE_ARM_CLIENT_ID(self) -> str:
        return self.AZURE_ARM_CLIENT_ID or self.MICROSOFT_CLIENT_ID

    @property
    def EFFECTIVE_ARM_CLIENT_SECRET(self) -> str:
        return self.AZURE_ARM_CLIENT_SECRET or self.MICROSOFT_CLIENT_SECRET

    @property
    def EFFECTIVE_ARM_TENANT_ID(self) -> str:
        return self.AZURE_ARM_TENANT_ID or self.MICROSOFT_TENANT_ID

    @property
    def DATABASE_URL(self) -> str:
        if self._database_url_override:
            return self._database_url_override

        username = quote(self.DB_USERNAME or "")
        password = quote(self.DB_PASSWORD or "")
        return f"postgresql+asyncpg://{username}:{password}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

    @property
    def RABBITMQ_URL(self) -> str:
        # Refuse to hand out a connection URL with the default RabbitMQ
        # guest:guest credential. Any process that can reach the broker on
        # those credentials gets full publish/consume rights, including the
        # storage.toggle queue that drives storage-backend switching.
        # Callers that have built a RABBITMQ_URL env var directly bypass this
        # entirely (and are responsible for their own credentials).
        u = self.RABBITMQ_USER
        p = self.RABBITMQ_PASSWORD
        if not u or not p:
            raise RuntimeError(
                "RabbitMQ credentials not set. Provide RABBITMQ_URL or set "
                "RABBITMQ_USERNAME and RABBITMQ_PASSWORD."
            )
        if u == "guest" and p == "guest":
            raise RuntimeError(
                "Refusing to connect to RabbitMQ with the default guest:guest "
                "credential. Provision a dedicated user."
            )
        return f"amqp://{u}:{p}@{self.RABBITMQ_HOST}:{self.RABBITMQ_PORT}/"


settings = Settings()
