"""Shared database connection and schema bootstrap helpers."""

import asyncio
import logging
import os
from time import monotonic
from typing import AsyncGenerator

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from shared.config import settings

SCHEMA_INIT_LOCK_ID = 1234567890
DDL_LOCK_TIMEOUT = "2000ms"
DDL_STATEMENT_TIMEOUT = "30000ms"
SCHEMA_READY_TIMEOUT_SECONDS = 120
SCHEMA_READY_POLL_INTERVAL_SECONDS = 1.0
SEARCH_PATH = f"{settings.DB_SCHEMA},public"

REQUIRED_TABLES = (
    "organizations",
    "tenants",
    "platform_users",
    "user_roles",
    "sla_policies",
    "resources",
    "jobs",
    "snapshots",
    "snapshot_items",
    "job_logs",
    "alerts",
    "audit_events",
    "admin_consent_tokens",
    "discovery_runs",
    "resource_discovery_staging",
    # Phase 1 SLA expansion tables — listing them here makes wait_for_schema_ready
    # return False until they exist, so cold-boot services actually run migrations.
    "sla_exclusions",
    "resource_groups",
    "group_policy_assignments",
    # On-prem storage toggle (2026-04-21). Required so a cold-boot service
    # waits for the bootstrapper instead of racing ahead and erroring out on
    # missing system_config during router.load().
    "storage_backends",
    "system_config",
    "storage_toggle_events",
    # Cross-user chat dedup (2026-05-13). Required so the worker waits for
    # init_db before issuing its first INSERT…ON CONFLICT drain claim.
    "chat_url_cache",
    "chat_threads",
    "chat_thread_messages",
    # Cross-user mail dedup (2026-05-17). Required so the backup-worker
    # waits for init_db before issuing its first mail-body upsert.
    "mail_message_bodies",
    # OneDrive per-file retry queue (2026-05-17). Producer + consumer
    # both expect the table to exist before issuing their INSERT /
    # SELECT FOR UPDATE SKIP LOCKED.
    "onedrive_file_retries",
)

REQUIRED_COLUMNS = {
    "sla_policies": (
        "service_type",
        "backup_azure_vm",
        "backup_azure_sql",
        "backup_azure_postgresql",
        # Phase 1 marker — ensures init_db won't short-circuit on older schemas.
        "retention_mode",
    ),
    "resources": ("resource_hash",),
    "snapshot_items": ("parent_external_id", "backend_id"),
    "snapshots": ("backend_id",),
    "jobs": ("retry_reason", "pre_toggle_job_id"),
    "resource_discovery_staging": (
        "azure_subscription_id",
        "azure_resource_group",
        "azure_region",
    ),
}

logger = logging.getLogger(__name__)


engine = create_async_engine(
    settings.DATABASE_URL,
    echo=False,
    pool_pre_ping=True,
    pool_use_lifo=settings.DB_POOL_USE_LIFO,
    pool_size=settings.DB_POOL_SIZE,
    max_overflow=settings.DB_MAX_OVERFLOW,
    pool_timeout=settings.DB_POOL_TIMEOUT,
    pool_recycle=settings.DB_POOL_RECYCLE,
    # statement_cache_size=0 prevents asyncpg's prepared-statement cache from
    # outliving a schema change. Without this, a tenant-wipe / DROP SCHEMA
    # leaves dashboard_service holding cached statement plans with stale
    # enum OIDs, producing `cache lookup failed for type 119228`. The cache
    # is a 5-15% throughput win on hot paths; correctness matters more during
    # demos/dev. Heavy-load prod can override by setting STATEMENT_CACHE_SIZE>0.
    connect_args={
        "server_settings": {"search_path": SEARCH_PATH},
        "statement_cache_size": int(os.getenv("STATEMENT_CACHE_SIZE", "0")),
    },
)

async_session_factory = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


class Base(DeclarativeBase):
    pass


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with async_session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def _has_required_tables(conn) -> bool:
    for table_name in REQUIRED_TABLES:
        result = await conn.execute(
            text("SELECT to_regclass(:qualified_name) IS NOT NULL"),
            {"qualified_name": f"{settings.DB_SCHEMA}.{table_name}"},
        )
        if not bool(result.scalar()):
            return False
    return True


async def _has_required_columns(conn) -> bool:
    for table_name, columns in REQUIRED_COLUMNS.items():
        for column_name in columns:
            result = await conn.execute(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.columns
                        WHERE table_schema = :schema_name
                          AND table_name = :table_name
                          AND column_name = :column_name
                    )
                    """
                ),
                {
                    "schema_name": settings.DB_SCHEMA,
                    "table_name": table_name,
                    "column_name": column_name,
                },
            )
            if not bool(result.scalar()):
                return False
    return True


async def wait_for_schema_ready(
    timeout_seconds: int = SCHEMA_READY_TIMEOUT_SECONDS,
    poll_interval_seconds: float = SCHEMA_READY_POLL_INTERVAL_SECONDS,
) -> bool:
    deadline = monotonic() + timeout_seconds
    last_error: Exception | None = None

    while monotonic() < deadline:
        try:
            async with engine.connect() as conn:
                if await _has_required_tables(conn) and await _has_required_columns(conn):
                    return True
        except Exception as exc:  # pragma: no cover - defensive startup logging
            last_error = exc

        await asyncio.sleep(poll_interval_seconds)

    if last_error:
        logger.warning("[DB INIT] Timed out waiting for schema readiness: %s", last_error)
    return False


async def _execute_batch(conn, statements: list[str]) -> None:
    for stmt in statements:
        await conn.execute(text(stmt))


async def _ensure_enum_values() -> None:
    statements = [
        "ALTER TYPE snapshotstatus ADD VALUE IF NOT EXISTS 'IN_PROGRESS';",
        "ALTER TYPE snapshotstatus ADD VALUE IF NOT EXISTS 'COMPLETED';",
        "ALTER TYPE snapshotstatus ADD VALUE IF NOT EXISTS 'FAILED';",
        "ALTER TYPE snapshotstatus ADD VALUE IF NOT EXISTS 'PARTIAL';",
        "ALTER TYPE snapshotstatus ADD VALUE IF NOT EXISTS 'PENDING_DELETION';",
        "ALTER TYPE resourcetype ADD VALUE IF NOT EXISTS 'ENTRA_SERVICE_PRINCIPAL';",
        "ALTER TYPE resourcetype ADD VALUE IF NOT EXISTS 'AZURE_POSTGRESQL_SINGLE';",
        "ALTER TYPE resourcetype ADD VALUE IF NOT EXISTS 'ENTRA_ROLE';",
        "ALTER TYPE resourcetype ADD VALUE IF NOT EXISTS 'ENTRA_ADMIN_UNIT';",
        "ALTER TYPE resourcetype ADD VALUE IF NOT EXISTS 'ENTRA_AUDIT_LOG';",
        "ALTER TYPE resourcetype ADD VALUE IF NOT EXISTS 'INTUNE_MANAGED_DEVICE';",
        "ALTER TYPE resourcetype ADD VALUE IF NOT EXISTS 'M365_GROUP';",
        "ALTER TYPE resourcetype ADD VALUE IF NOT EXISTS 'ENTRA_CONDITIONAL_ACCESS';",
        "ALTER TYPE resourcetype ADD VALUE IF NOT EXISTS 'ENTRA_BITLOCKER_KEY';",
        # Per-user Teams chat export shard — emitted by the legacy full-discovery
        # path; we keep it valid in the enum so cold boots that ran a stale
        # image (or older queued discovery messages) don't crash the staging
        # insert.
        "ALTER TYPE resourcetype ADD VALUE IF NOT EXISTS 'TEAMS_CHAT_EXPORT';",
        # Tier 2 per-user content categories — children of an ENTRA_USER row.
        "ALTER TYPE resourcetype ADD VALUE IF NOT EXISTS 'USER_MAIL';",
        "ALTER TYPE resourcetype ADD VALUE IF NOT EXISTS 'USER_ONEDRIVE';",
        "ALTER TYPE resourcetype ADD VALUE IF NOT EXISTS 'USER_CONTACTS';",
        "ALTER TYPE resourcetype ADD VALUE IF NOT EXISTS 'USER_CALENDAR';",
        "ALTER TYPE resourcetype ADD VALUE IF NOT EXISTS 'USER_CHATS';",
        # Singleton per-tenant "Azure Active Directory" resource.
        "ALTER TYPE resourcetype ADD VALUE IF NOT EXISTS 'ENTRA_DIRECTORY';",
        "ALTER TYPE tenantstatus ADD VALUE IF NOT EXISTS 'PENDING_DISCOVERY';",
        "ALTER TYPE jobstatus ADD VALUE IF NOT EXISTS 'QUEUED';",
        "ALTER TYPE jobstatus ADD VALUE IF NOT EXISTS 'RUNNING';",
        "ALTER TYPE jobstatus ADD VALUE IF NOT EXISTS 'COMPLETED';",
        "ALTER TYPE jobstatus ADD VALUE IF NOT EXISTS 'FAILED';",
        "ALTER TYPE jobstatus ADD VALUE IF NOT EXISTS 'CANCELLED';",
        "ALTER TYPE jobstatus ADD VALUE IF NOT EXISTS 'RETRYING';",
        # Chat export v1 — PENDING is the queued-but-idempotency-safe state,
        # CANCELLING is the transient state while the worker wraps up.
        "ALTER TYPE jobstatus ADD VALUE IF NOT EXISTS 'PENDING';",
        "ALTER TYPE jobstatus ADD VALUE IF NOT EXISTS 'CANCELLING';",
        "ALTER TYPE snapshottype ADD VALUE IF NOT EXISTS 'FULL';",
        "ALTER TYPE snapshottype ADD VALUE IF NOT EXISTS 'INCREMENTAL';",
        "ALTER TYPE snapshottype ADD VALUE IF NOT EXISTS 'PREEMPTIVE';",
        "ALTER TYPE snapshottype ADD VALUE IF NOT EXISTS 'MANUAL';",
        "ALTER TYPE resourcestatus ADD VALUE IF NOT EXISTS 'DISCOVERED';",
        "ALTER TYPE resourcestatus ADD VALUE IF NOT EXISTS 'ACTIVE';",
        "ALTER TYPE resourcestatus ADD VALUE IF NOT EXISTS 'ARCHIVED';",
        "ALTER TYPE resourcestatus ADD VALUE IF NOT EXISTS 'SUSPENDED';",
        "ALTER TYPE resourcestatus ADD VALUE IF NOT EXISTS 'PENDING_DELETION';",
        "ALTER TYPE tenantstatus ADD VALUE IF NOT EXISTS 'PENDING';",
        "ALTER TYPE tenantstatus ADD VALUE IF NOT EXISTS 'ACTIVE';",
        "ALTER TYPE tenantstatus ADD VALUE IF NOT EXISTS 'DISCONNECTED';",
        "ALTER TYPE tenantstatus ADD VALUE IF NOT EXISTS 'SUSPENDED';",
        "ALTER TYPE tenantstatus ADD VALUE IF NOT EXISTS 'PENDING_DELETION';",
        "ALTER TYPE tenantstatus ADD VALUE IF NOT EXISTS 'DISCOVERING';",
        "ALTER TYPE tenanttype ADD VALUE IF NOT EXISTS 'M365';",
        "ALTER TYPE tenanttype ADD VALUE IF NOT EXISTS 'AZURE';",
        # 'BOTH' removed; legacy deployments may still have it as an orphaned enum
        # value (harmless — no rows reference it after the migration).
        "ALTER TYPE jobtype ADD VALUE IF NOT EXISTS 'BACKUP';",
        "ALTER TYPE jobtype ADD VALUE IF NOT EXISTS 'RESTORE';",
        "ALTER TYPE jobtype ADD VALUE IF NOT EXISTS 'EXPORT';",
        "ALTER TYPE jobtype ADD VALUE IF NOT EXISTS 'DISCOVERY';",
        "ALTER TYPE jobtype ADD VALUE IF NOT EXISTS 'DELETE';",
        "ALTER TYPE userrole ADD VALUE IF NOT EXISTS 'SUPER_ADMIN';",
        "ALTER TYPE userrole ADD VALUE IF NOT EXISTS 'ORG_ADMIN';",
        "ALTER TYPE userrole ADD VALUE IF NOT EXISTS 'TENANT_ADMIN';",
        "ALTER TYPE userrole ADD VALUE IF NOT EXISTS 'BACKUP_OPERATOR';",
        "ALTER TYPE userrole ADD VALUE IF NOT EXISTS 'RESTORE_OPERATOR';",
        "ALTER TYPE userrole ADD VALUE IF NOT EXISTS 'CONTENT_VIEWER';",
        "ALTER TYPE userrole ADD VALUE IF NOT EXISTS 'USER';",
    ]

    for stmt in statements:
        try:
            async with engine.begin() as conn:
                await conn.execute(text(f"SET LOCAL lock_timeout = '{DDL_LOCK_TIMEOUT}'"))
                await conn.execute(text(f"SET LOCAL statement_timeout = '{DDL_STATEMENT_TIMEOUT}'"))
                await conn.execute(text(stmt))
        except Exception:
            # Postgres version / transaction semantics differ a bit here; IF NOT EXISTS
            # plus the guarded fallback keeps startup idempotent.
            pass


async def init_db() -> None:
    """Create schema objects on first boot and wait for readiness on contended boot."""

    enum_type_statements = [
        """DO $$ BEGIN
            CREATE TYPE userrole AS ENUM ('SUPER_ADMIN', 'ORG_ADMIN', 'TENANT_ADMIN', 'BACKUP_OPERATOR', 'RESTORE_OPERATOR', 'CONTENT_VIEWER', 'USER');
        EXCEPTION WHEN duplicate_object THEN null; END $$;""",
        """DO $$ BEGIN
            CREATE TYPE tenanttype AS ENUM ('M365', 'AZURE');
        EXCEPTION WHEN duplicate_object THEN null; END $$;""",
        """DO $$ BEGIN
            CREATE TYPE tenantstatus AS ENUM ('PENDING', 'ACTIVE', 'DISCONNECTED', 'SUSPENDED', 'PENDING_DELETION', 'DISCOVERING', 'PENDING_DISCOVERY');
        EXCEPTION WHEN duplicate_object THEN null; END $$;""",
        """DO $$ BEGIN
            CREATE TYPE resourcetype AS ENUM ('MAILBOX', 'SHARED_MAILBOX', 'ROOM_MAILBOX', 'ONEDRIVE', 'SHAREPOINT_SITE', 'TEAMS_CHANNEL', 'TEAMS_CHAT', 'ENTRA_USER', 'ENTRA_GROUP', 'ENTRA_APP', 'ENTRA_SERVICE_PRINCIPAL', 'ENTRA_DEVICE', 'ENTRA_ROLE', 'ENTRA_ADMIN_UNIT', 'ENTRA_AUDIT_LOG', 'INTUNE_MANAGED_DEVICE', 'AZURE_VM', 'AZURE_SQL_DB', 'AZURE_POSTGRESQL', 'AZURE_POSTGRESQL_SINGLE', 'RESOURCE_GROUP', 'DYNAMIC_GROUP', 'POWER_BI', 'POWER_APPS', 'POWER_AUTOMATE', 'POWER_DLP', 'COPILOT', 'PLANNER', 'TODO', 'ONENOTE');
        EXCEPTION WHEN duplicate_object THEN null; END $$;""",
        """DO $$ BEGIN
            CREATE TYPE resourcestatus AS ENUM ('DISCOVERED', 'ACTIVE', 'ARCHIVED', 'SUSPENDED', 'PENDING_DELETION', 'INACCESSIBLE');
        EXCEPTION WHEN duplicate_object THEN null; END $$;""",
        """DO $$ BEGIN
            CREATE TYPE jobtype AS ENUM ('BACKUP', 'RESTORE', 'EXPORT', 'DISCOVERY', 'DELETE');
        EXCEPTION WHEN duplicate_object THEN null; END $$;""",
        """DO $$ BEGIN
            CREATE TYPE jobstatus AS ENUM ('QUEUED', 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED', 'RETRYING');
        EXCEPTION WHEN duplicate_object THEN null; END $$;""",
        """DO $$ BEGIN
            CREATE TYPE snapshottype AS ENUM ('FULL', 'INCREMENTAL', 'PREEMPTIVE', 'MANUAL');
        EXCEPTION WHEN duplicate_object THEN null; END $$;""",
        """DO $$ BEGIN
            CREATE TYPE snapshotstatus AS ENUM ('IN_PROGRESS', 'COMPLETED', 'FAILED', 'PARTIAL', 'PENDING_DELETION');
        EXCEPTION WHEN duplicate_object THEN null; END $$;""",
    ]

    table_statements = [
        """
        CREATE TABLE IF NOT EXISTS organizations (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name VARCHAR NOT NULL,
            slug VARCHAR UNIQUE NOT NULL,
            storage_region VARCHAR,
            encryption_mode VARCHAR DEFAULT 'TMVAULT_MANAGED',
            storage_quota_bytes BIGINT DEFAULT 536870912000,
            storage_bytes_used BIGINT DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS tenants (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            org_id UUID REFERENCES organizations(id),
            type VARCHAR DEFAULT 'M365',
            display_name VARCHAR NOT NULL,
            external_tenant_id VARCHAR,
            customer_id VARCHAR,
            subscription_id VARCHAR,
            client_id VARCHAR,
            client_secret_ref VARCHAR,
            graph_client_id VARCHAR,
            graph_client_secret_encrypted BYTEA,
            status VARCHAR DEFAULT 'PENDING',
            storage_region VARCHAR,
            last_discovery_at TIMESTAMP,
            graph_delta_tokens JSON DEFAULT '{}',
            extra_data JSON DEFAULT '{}',
            dr_region_enabled BOOLEAN DEFAULT FALSE,
            dr_region VARCHAR,
            dr_storage_account_name VARCHAR,
            dr_storage_account_key_encrypted BYTEA,
            dr_last_replicated_at TIMESTAMP,
            azure_refresh_token_encrypted BYTEA,
            azure_refresh_token_updated_at TIMESTAMP,
            azure_subscriptions_cached JSON DEFAULT '[]',
            azure_sql_servers_configured JSON DEFAULT '[]',
            archived_at TIMESTAMPTZ,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS platform_users (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            email VARCHAR UNIQUE NOT NULL,
            name VARCHAR NOT NULL,
            external_user_id VARCHAR,
            org_id UUID REFERENCES organizations(id),
            tenant_id UUID REFERENCES tenants(id),
            mfa_enabled BOOLEAN DEFAULT FALSE,
            last_login_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS user_roles (
            user_id UUID REFERENCES platform_users(id),
            role VARCHAR,
            PRIMARY KEY (user_id, role)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS sla_policies (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            tenant_id UUID REFERENCES tenants(id),
            service_type VARCHAR DEFAULT 'm365',
            name VARCHAR NOT NULL,
            frequency VARCHAR DEFAULT 'DAILY',
            backup_days VARCHAR[],
            backup_window_start VARCHAR,
            backup_window_end VARCHAR,
            resource_types VARCHAR[],
            batch_size INTEGER DEFAULT 1000,
            max_concurrent_backups INTEGER DEFAULT 5,
            sla_violation_alert BOOLEAN DEFAULT TRUE,
            retention_days INTEGER DEFAULT 2555,
            retention_versions INTEGER DEFAULT 10,
            backup_exchange BOOLEAN DEFAULT TRUE,
            backup_exchange_archive BOOLEAN DEFAULT FALSE,
            backup_exchange_recoverable BOOLEAN DEFAULT FALSE,
            backup_onedrive BOOLEAN DEFAULT TRUE,
            backup_sharepoint BOOLEAN DEFAULT TRUE,
            backup_teams BOOLEAN DEFAULT TRUE,
            backup_teams_chats BOOLEAN DEFAULT FALSE,
            backup_entra_id BOOLEAN DEFAULT TRUE,
            backup_power_platform BOOLEAN DEFAULT FALSE,
            backup_copilot BOOLEAN DEFAULT FALSE,
            contacts BOOLEAN DEFAULT TRUE,
            calendars BOOLEAN DEFAULT TRUE,
            tasks BOOLEAN DEFAULT FALSE,
            group_mailbox BOOLEAN DEFAULT TRUE,
            planner BOOLEAN DEFAULT FALSE,
            backup_azure_vm BOOLEAN DEFAULT TRUE,
            backup_azure_sql BOOLEAN DEFAULT TRUE,
            backup_azure_postgresql BOOLEAN DEFAULT TRUE,
            retention_type VARCHAR DEFAULT 'INDEFINITE',
            retention_hot_days INTEGER DEFAULT 7,
            retention_cool_days INTEGER DEFAULT 30,
            retention_archive_days INTEGER,
            legal_hold_enabled BOOLEAN DEFAULT FALSE,
            legal_hold_until TIMESTAMP,
            immutability_mode VARCHAR DEFAULT 'None',
            retention_mode VARCHAR DEFAULT 'FLAT' NOT NULL,
            gfs_daily_count INTEGER,
            gfs_weekly_count INTEGER,
            gfs_monthly_count INTEGER,
            gfs_yearly_count INTEGER,
            item_retention_days INTEGER,
            item_retention_basis VARCHAR DEFAULT 'SNAPSHOT' NOT NULL,
            archived_retention_mode VARCHAR DEFAULT 'SAME' NOT NULL,
            archived_retention_days INTEGER,
            storage_region VARCHAR,
            encryption_mode VARCHAR DEFAULT 'VAULT_MANAGED' NOT NULL,
            key_vault_uri VARCHAR,
            key_name VARCHAR,
            key_version VARCHAR,
            auto_apply_to_matching BOOLEAN DEFAULT FALSE NOT NULL,
            enabled BOOLEAN DEFAULT TRUE,
            is_default BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS sla_exclusions (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            policy_id UUID NOT NULL REFERENCES sla_policies(id) ON DELETE CASCADE,
            exclusion_type VARCHAR NOT NULL,
            pattern VARCHAR NOT NULL,
            workload VARCHAR,
            apply_to_historical BOOLEAN DEFAULT FALSE NOT NULL,
            enabled BOOLEAN DEFAULT TRUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS resource_groups (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
            name VARCHAR NOT NULL,
            description TEXT,
            group_type VARCHAR DEFAULT 'DYNAMIC' NOT NULL,
            rules JSON DEFAULT '[]' NOT NULL,
            combinator VARCHAR DEFAULT 'AND' NOT NULL,
            priority INTEGER DEFAULT 100 NOT NULL,
            auto_protect_new BOOLEAN DEFAULT FALSE NOT NULL,
            enabled BOOLEAN DEFAULT TRUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS group_policy_assignments (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            group_id UUID NOT NULL REFERENCES resource_groups(id) ON DELETE CASCADE,
            policy_id UUID NOT NULL REFERENCES sla_policies(id) ON DELETE CASCADE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (group_id, policy_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS resources (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            tenant_id UUID REFERENCES tenants(id),
            type VARCHAR NOT NULL,
            external_id VARCHAR NOT NULL,
            display_name VARCHAR NOT NULL,
            email VARCHAR,
            metadata JSON DEFAULT '{}',
            sla_policy_id UUID REFERENCES sla_policies(id),
            status VARCHAR DEFAULT 'DISCOVERED',
            last_backup_job_id UUID,
            last_backup_at TIMESTAMP,
            last_backup_status VARCHAR,
            storage_bytes BIGINT DEFAULT 0,
            discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            archived_at TIMESTAMP,
            deletion_queued_at TIMESTAMP,
            azure_subscription_id VARCHAR,
            azure_resource_group VARCHAR,
            azure_region VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS jobs (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            type VARCHAR NOT NULL,
            tenant_id UUID REFERENCES tenants(id),
            resource_id UUID REFERENCES resources(id),
            batch_resource_ids UUID[] DEFAULT '{}',
            snapshot_id UUID,
            status VARCHAR DEFAULT 'QUEUED',
            priority INTEGER DEFAULT 5,
            attempts INTEGER DEFAULT 0,
            max_attempts INTEGER DEFAULT 5,
            error_message TEXT,
            progress_pct INTEGER DEFAULT 0,
            items_processed BIGINT DEFAULT 0,
            bytes_processed BIGINT DEFAULT 0,
            result JSON DEFAULT '{}',
            spec JSON DEFAULT '{}',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS backup_batches (
            id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            tenant_id       UUID NOT NULL,
            created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            completed_at    TIMESTAMP,
            source          TEXT NOT NULL,
            actor_email     TEXT,
            scope_user_ids  UUID[] NOT NULL,
            bytes_expected  BIGINT,
            status          TEXT NOT NULL DEFAULT 'IN_PROGRESS'
                              CHECK (status IN ('IN_PROGRESS','COMPLETED','PARTIAL','FAILED','CANCELLED'))
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS snapshots (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            resource_id UUID REFERENCES resources(id),
            job_id UUID REFERENCES jobs(id),
            type VARCHAR DEFAULT 'INCREMENTAL',
            status VARCHAR DEFAULT 'COMPLETED',
            started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            duration_secs INTEGER,
            item_count INTEGER DEFAULT 0,
            new_item_count INTEGER DEFAULT 0,
            bytes_added BIGINT DEFAULT 0,
            bytes_total BIGINT DEFAULT 0,
            delta_token VARCHAR,
            delta_tokens_json JSON DEFAULT '{}',
            extra_data JSON DEFAULT '{}',
            snapshot_label VARCHAR,
            content_checksum VARCHAR,
            blob_path VARCHAR,
            storage_version INTEGER DEFAULT 1,
            azure_restore_point_id VARCHAR,
            azure_operation_id VARCHAR,
            dr_replication_status VARCHAR DEFAULT 'pending',
            dr_blob_path VARCHAR,
            dr_replicated_at TIMESTAMP,
            dr_error TEXT,
            dr_replication_attempts INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS snapshot_items (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            snapshot_id UUID REFERENCES snapshots(id),
            tenant_id UUID REFERENCES tenants(id),
            external_id VARCHAR NOT NULL,
            item_type VARCHAR NOT NULL,
            name VARCHAR NOT NULL,
            folder_path VARCHAR,
            content_hash VARCHAR,
            content_checksum VARCHAR,
            content_size BIGINT DEFAULT 0,
            blob_path VARCHAR,
            encryption_key_id VARCHAR,
            backup_version INTEGER DEFAULT 1,
            metadata JSON DEFAULT '{}',
            is_deleted BOOLEAN DEFAULT FALSE,
            indexed_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS job_logs (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            job_id UUID REFERENCES jobs(id),
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            level VARCHAR DEFAULT 'INFO',
            message TEXT,
            details TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS alerts (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            tenant_id UUID REFERENCES tenants(id),
            org_id UUID REFERENCES organizations(id),
            type VARCHAR NOT NULL,
            severity VARCHAR DEFAULT 'MEDIUM',
            message TEXT NOT NULL,
            resource_id UUID,
            resource_type VARCHAR,
            resource_name VARCHAR,
            triggered_by VARCHAR,
            resolved BOOLEAN DEFAULT FALSE,
            resolved_at TIMESTAMP,
            resolved_by UUID,
            resolution_note TEXT,
            details JSON DEFAULT '{}',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS audit_events (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            org_id UUID REFERENCES organizations(id),
            tenant_id UUID REFERENCES tenants(id),
            actor_id UUID,
            actor_email VARCHAR,
            actor_type VARCHAR DEFAULT 'SYSTEM',
            action VARCHAR NOT NULL,
            resource_id UUID,
            resource_type VARCHAR,
            resource_name VARCHAR,
            outcome VARCHAR DEFAULT 'SUCCESS',
            job_id UUID,
            snapshot_id UUID,
            details JSONB DEFAULT '{}',
            occurred_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS admin_consent_tokens (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            org_id UUID REFERENCES organizations(id),
            tenant_id UUID REFERENCES tenants(id),
            consent_type VARCHAR NOT NULL,
            access_token_encrypted BYTEA,
            refresh_token_encrypted BYTEA,
            token_type VARCHAR DEFAULT 'Bearer',
            expires_at TIMESTAMP,
            granted_by VARCHAR,
            consented_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_used_at TIMESTAMP,
            is_active BOOLEAN DEFAULT TRUE,
            scope VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS discovery_runs (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            tenant_id UUID REFERENCES tenants(id) NOT NULL,
            scope JSON DEFAULT '[]',
            status VARCHAR DEFAULT 'RUNNING',
            fetched_count INTEGER DEFAULT 0,
            staged_count INTEGER DEFAULT 0,
            inserted_count INTEGER DEFAULT 0,
            updated_count INTEGER DEFAULT 0,
            unchanged_count INTEGER DEFAULT 0,
            stale_marked_count INTEGER DEFAULT 0,
            error_message TEXT,
            started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            finished_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS resource_discovery_staging (
            id BIGSERIAL PRIMARY KEY,
            run_id UUID REFERENCES discovery_runs(id) ON DELETE CASCADE,
            tenant_id UUID REFERENCES tenants(id) NOT NULL,
            resource_type VARCHAR NOT NULL,
            external_id VARCHAR NOT NULL,
            display_name VARCHAR NOT NULL,
            email VARCHAR,
            metadata JSONB DEFAULT '{}'::jsonb,
            resource_status VARCHAR DEFAULT 'DISCOVERED',
            resource_hash VARCHAR,
            azure_subscription_id VARCHAR,
            azure_resource_group VARCHAR,
            azure_region VARCHAR,
            discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS report_configs (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            org_id UUID REFERENCES organizations(id),
            enabled BOOLEAN DEFAULT FALSE NOT NULL,
            schedule_type VARCHAR DEFAULT 'daily' NOT NULL,
            send_empty_report BOOLEAN DEFAULT TRUE NOT NULL,
            empty_message VARCHAR DEFAULT 'No updates. No backups occurred.',
            send_detailed_report BOOLEAN DEFAULT FALSE NOT NULL,
            email_recipients JSON DEFAULT '[]',
            slack_webhooks JSON DEFAULT '[]',
            teams_webhooks JSON DEFAULT '[]',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS report_history (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            org_id UUID REFERENCES organizations(id),
            report_config_id UUID REFERENCES report_configs(id),
            report_type VARCHAR NOT NULL,
            period_start TIMESTAMP,
            period_end TIMESTAMP,
            generated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            total_backups INTEGER DEFAULT 0,
            successful_backups INTEGER DEFAULT 0,
            failed_backups INTEGER DEFAULT 0,
            success_rate VARCHAR,
            coverage_rate VARCHAR,
            report_data JSON DEFAULT '{}',
            is_empty BOOLEAN DEFAULT FALSE NOT NULL,
            delivery_status JSON DEFAULT '{}',
            error_message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS tenant_secrets (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
            type VARCHAR NOT NULL,
            name VARCHAR NOT NULL,
            description TEXT,
            metadata_hints JSON DEFAULT '{}',
            encrypted_payload TEXT,
            is_default BOOLEAN NOT NULL DEFAULT FALSE,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """,
        # File-level index of every Azure VM VHD captured during backup.
        # Drives backup-based (not live) Volumes browsing + download.
        # See shared.models.VmFileIndex for schema rationale.
        """
        CREATE TABLE IF NOT EXISTS vm_file_index (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            snapshot_id UUID NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
            volume_item_id UUID NOT NULL,
            parent_path VARCHAR NOT NULL,
            name VARCHAR NOT NULL,
            is_directory BOOLEAN NOT NULL DEFAULT FALSE,
            size_bytes BIGINT NOT NULL DEFAULT 0,
            modified_at TIMESTAMP,
            fs_inode BIGINT,
            fs_type VARCHAR,
            partition_offset BIGINT,
            blob_path VARCHAR,
            extents_json JSON,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """,
        # On-prem storage toggle tables. Must exist before add_column_statements
        # runs so downstream ADD COLUMN ... REFERENCES storage_backends(id)
        # can resolve the FK target.
        """
        CREATE TABLE IF NOT EXISTS storage_backends (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            kind VARCHAR NOT NULL,
            name VARCHAR NOT NULL UNIQUE,
            endpoint VARCHAR NOT NULL,
            config JSONB NOT NULL DEFAULT '{}'::jsonb,
            secret_ref VARCHAR NOT NULL,
            is_enabled BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS system_config (
            id SMALLINT PRIMARY KEY CHECK (id = 1),
            active_backend_id UUID NOT NULL REFERENCES storage_backends(id),
            transition_state VARCHAR NOT NULL DEFAULT 'stable',
            last_toggle_at TIMESTAMPTZ,
            cooldown_until TIMESTAMPTZ,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS storage_toggle_events (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            actor_id UUID NOT NULL,
            actor_ip INET,
            from_backend_id UUID NOT NULL REFERENCES storage_backends(id),
            to_backend_id UUID NOT NULL REFERENCES storage_backends(id),
            reason VARCHAR,
            status VARCHAR NOT NULL DEFAULT 'started',
            started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            drain_completed_at TIMESTAMPTZ,
            flip_completed_at TIMESTAMPTZ,
            completed_at TIMESTAMPTZ,
            error_message TEXT,
            pre_flight_checks JSONB,
            drained_job_count INTEGER,
            retried_job_count INTEGER
        )
        """,
        # Cross-user chat dedup (2026-05-13). See shared.models docstrings for
        # the full design notes.
        """
        CREATE TABLE IF NOT EXISTS chat_url_cache (
            tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
            url_sha256 CHAR(64) NOT NULL,
            drive_item_id VARCHAR(256),
            content_hash CHAR(64),
            blob_path TEXT,
            content_size BIGINT,
            inline_b64 TEXT,
            unreachable BOOLEAN NOT NULL DEFAULT FALSE,
            first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            last_used_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (tenant_id, url_sha256)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS chat_threads (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
            chat_id VARCHAR(256) NOT NULL,
            chat_type VARCHAR(32),
            chat_topic TEXT,
            member_names_json JSONB,
            last_updated_at TIMESTAMPTZ,
            last_drained_at TIMESTAMPTZ,
            drain_cursor TEXT,
            drain_failure_state JSONB,
            archived_at TIMESTAMPTZ,
            last_drained_msg_count INTEGER,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (tenant_id, chat_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS chat_thread_messages (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            chat_thread_id UUID NOT NULL REFERENCES chat_threads(id) ON DELETE RESTRICT,
            message_external_id VARCHAR(256) NOT NULL,
            created_date_time TIMESTAMPTZ,
            last_modified_date_time TIMESTAMPTZ,
            from_user_id VARCHAR(128),
            from_display_name VARCHAR(256),
            body_content TEXT,
            body_content_type VARCHAR(16),
            deleted_date_time TIMESTAMPTZ,
            metadata_raw JSONB,
            content_hash CHAR(64),
            content_size BIGINT,
            archived_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (chat_thread_id, message_external_id)
        )
        """,
        # Cross-user mail dedup. Same role chat_thread_messages plays
        # for chats: one row per logical message body, shared across
        # every user whose mailbox contained that email. snapshot_items
        # carries thin pointer rows that JOIN here at read time once
        # Phase 2 lands. See MailMessageBody in shared/models.py.
        """
        CREATE TABLE IF NOT EXISTS mail_message_bodies (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
            fingerprint CHAR(64) NOT NULL,
            first_user_id VARCHAR(128),
            first_snapshot_id UUID,
            from_user_id VARCHAR(128),
            from_address VARCHAR(256),
            from_display_name VARCHAR(256),
            subject TEXT,
            sent_date_time TIMESTAMPTZ,
            received_date_time TIMESTAMPTZ,
            body_content TEXT,
            body_content_type VARCHAR(16),
            has_attachments BOOLEAN,
            metadata_raw JSONB,
            content_hash CHAR(64),
            content_size BIGINT,
            ref_count INTEGER NOT NULL DEFAULT 1,
            last_referenced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (tenant_id, fingerprint)
        )
        """,
        # snapshot_partitions — cross-replica OneDrive partition split.
        # One row per shard of a partitioned USER_ONEDRIVE snapshot;
        # multiple backup_worker replicas drain one drive in parallel.
        # Mirrors `SnapshotPartition` in shared/models.py. The FK to
        # snapshots uses ON DELETE CASCADE so deleting a partitioned
        # snapshot reaps its shard rows automatically.
        """
        CREATE TABLE IF NOT EXISTS snapshot_partitions (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            snapshot_id UUID NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
            tenant_id UUID NOT NULL,
            resource_id UUID NOT NULL,
            job_id UUID NOT NULL,
            partition_type VARCHAR NOT NULL DEFAULT 'ONEDRIVE_FILES',
            drive_id TEXT,
            partition_index INTEGER NOT NULL,
            file_ids JSON,
            payload JSON,
            total_files INTEGER NOT NULL DEFAULT 0,
            total_bytes_est BIGINT NOT NULL DEFAULT 0,
            status VARCHAR NOT NULL DEFAULT 'QUEUED',
            worker_id VARCHAR,
            worker_region VARCHAR,
            retry_count INTEGER NOT NULL DEFAULT 0,
            enqueued_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            files_uploaded INTEGER NOT NULL DEFAULT 0,
            bytes_uploaded BIGINT NOT NULL DEFAULT 0,
            failure_state JSON,
            UNIQUE (snapshot_id, partition_index)
        )
        """,
        # mail_folder_delta — per-folder Graph delta tokens for the
        # four mailbox resource types (USER_MAIL, MAILBOX,
        # SHARED_MAILBOX, ROOM_MAILBOX). Replaces the JSON dict that
        # used to live in resources.extra_data; the dict was unsafe
        # under concurrent folder drains (RMW race). Promoting each
        # (resource_id, folder_id) to its own row makes writes
        # commute under per-row locks.
        """
        CREATE TABLE IF NOT EXISTS mail_folder_delta (
            resource_id UUID NOT NULL REFERENCES resources(id) ON DELETE CASCADE,
            folder_id TEXT NOT NULL,
            delta_token TEXT NOT NULL,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (resource_id, folder_id)
        )
        """,
        # mail_folder_fingerprint — per-folder Graph stats fingerprint
        # for USER_MAIL skip-by-fp. Replaces the whole-mailbox JSON
        # dict in resources.extra_data['mail_folder_fingerprints'] +
        # 'mail_folder_baseline_at'. The dict was clobbered by sibling
        # MAIL_FOLDERS partition shards: the second-finishing shard
        # re-read the first shard's fingerprint writes for folders it
        # didn't own and skipped its allowlist via skip-by-fp. One row
        # per (resource, folder) makes writes commute. `baseline_at`
        # is per folder so the 3-day full-rescan window doesn't reset
        # mailbox-wide on every drain.
        """
        CREATE TABLE IF NOT EXISTS mail_folder_fingerprint (
            resource_id UUID NOT NULL REFERENCES resources(id) ON DELETE CASCADE,
            folder_id TEXT NOT NULL,
            total_item_count INTEGER NOT NULL DEFAULT 0,
            unread_item_count INTEGER NOT NULL DEFAULT 0,
            size_in_bytes BIGINT NOT NULL DEFAULT 0,
            baseline_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (resource_id, folder_id)
        )
        """,
        # batch_pending_users — per-user deferred-backup state when
        # discovery hasn't completed at batch creation. Closes the
        # race where a `/backup-all` click for newly-SLA'd users
        # produced a backup_batches row stuck at IN_PROGRESS forever
        # (no Tier-2 children → finalizer gate-1 never passes). See
        # docs/superpowers/specs/2026-05-15-backup-batch-race-fix-design.md.
        """
        CREATE TABLE IF NOT EXISTS batch_pending_users (
            batch_id    UUID NOT NULL REFERENCES backup_batches(id) ON DELETE CASCADE,
            user_id     UUID NOT NULL REFERENCES resources(id) ON DELETE CASCADE,
            state       TEXT NOT NULL,
            deadline_at TIMESTAMP NOT NULL,
            updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (batch_id, user_id)
        )
        """,
        # sharepoint_drive_delta — per-drive Graph delta tokens for
        # SHAREPOINT_SITE resources. Same fix for the same RMW
        # pattern in resources.extra_data['drive_delta_tokens_by_site'].
        """
        CREATE TABLE IF NOT EXISTS sharepoint_drive_delta (
            resource_id UUID NOT NULL REFERENCES resources(id) ON DELETE CASCADE,
            drive_id TEXT NOT NULL,
            delta_token TEXT NOT NULL,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (resource_id, drive_id)
        )
        """,
        # bulk_fanout_seen — per-resource dedup marker for the bulk
        # backup fanout coordinator (_fanout_bulk_to_per_resource).
        # Inserted BEFORE per-resource publish; ON CONFLICT DO NOTHING
        # is the atomic dedup point that prevents bulk-message
        # redelivery from re-publishing duplicate per-resource backups
        # (the observed loop where one user's USER_CHATS spawns 2-3
        # snapshots per session). Stale-sweep prunes rows > 24h.
        """
        CREATE TABLE IF NOT EXISTS bulk_fanout_seen (
            job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
            resource_id UUID NOT NULL REFERENCES resources(id) ON DELETE CASCADE,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (job_id, resource_id)
        )
        """,
        # Distributed reconciliation (2026-05-16 design).
        # worker_heartbeats — liveness signal. Workers UPSERT every 10s;
        # sweeper considers a worker dead after 60s of silence. Used to
        # decide whether a held lease is "alive" or orphan.
        """
        CREATE TABLE IF NOT EXISTS worker_heartbeats (
            worker_id    UUID PRIMARY KEY,
            replica_id   TEXT NOT NULL,
            service_name TEXT NOT NULL,
            pid          INTEGER,
            queues       TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
            last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            started_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            version      TEXT
        )
        """,
        # work_dead_letter — append-only audit trail for poison-pill
        # messages (>3 requeues) and route-loop violations. Operator
        # alert source; not consumed by any worker.
        """
        CREATE TABLE IF NOT EXISTS work_dead_letter (
            id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            work_kind    TEXT NOT NULL,
            work_id      UUID NOT NULL,
            reason       TEXT NOT NULL,
            last_payload JSON,
            created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        # onedrive_file_retries — per-file retry queue for OneDrive
        # downloads that exhaust their inline resume budget. The main
        # gather no longer blocks on slow/throttled files; instead it
        # INSERTs a row here and lets the snapshot complete. A separate
        # consumer drains this table, retrying each file individually
        # with exponential backoff (next_retry_at gates pickup). On
        # success the file is upserted into snapshot_items pointing at
        # the original snapshot; on exhaustion it's marked
        # FAILED_PERMANENT for the audit trail.
        """
        CREATE TABLE IF NOT EXISTS onedrive_file_retries (
            id                       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            tenant_id                UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
            resource_id              UUID NOT NULL REFERENCES resources(id) ON DELETE CASCADE,
            snapshot_id              UUID NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
            file_external_id         VARCHAR(256) NOT NULL,
            file_name                TEXT,
            drive_id                 TEXT,
            file_payload             JSONB NOT NULL,
            attempt_count            INTEGER NOT NULL DEFAULT 0,
            last_error               TEXT,
            last_error_class         VARCHAR(32),
            next_retry_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            status                   VARCHAR(16) NOT NULL DEFAULT 'PENDING',
            rescued_snapshot_item_id UUID,
            created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (snapshot_id, file_external_id)
        )
        """,
    ]

    index_statements = [
        "CREATE INDEX IF NOT EXISTS idx_audit_events_tenant ON audit_events(tenant_id)",
        "CREATE INDEX IF NOT EXISTS idx_audit_events_action ON audit_events(action)",
        "CREATE INDEX IF NOT EXISTS idx_audit_events_occurred ON audit_events(occurred_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_audit_events_org ON audit_events(org_id)",
        "CREATE INDEX IF NOT EXISTS idx_audit_events_resource ON audit_events(resource_id)",
        "CREATE INDEX IF NOT EXISTS idx_admin_consent_org ON admin_consent_tokens(org_id)",
        "CREATE INDEX IF NOT EXISTS idx_admin_consent_tenant ON admin_consent_tokens(tenant_id)",
        "CREATE INDEX IF NOT EXISTS idx_admin_consent_type ON admin_consent_tokens(consent_type)",
        "CREATE INDEX IF NOT EXISTS idx_admin_consent_active ON admin_consent_tokens(is_active)",
        "CREATE INDEX IF NOT EXISTS idx_sla_policies_tenant_service ON sla_policies(tenant_id, service_type)",
        "CREATE INDEX IF NOT EXISTS idx_resources_tenant_type_external ON resources(tenant_id, type, external_id)",
        "CREATE INDEX IF NOT EXISTS idx_resources_tenant_status_type ON resources(tenant_id, status, type)",
        "CREATE INDEX IF NOT EXISTS idx_discovery_runs_tenant_started ON discovery_runs(tenant_id, started_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_discovery_runs_status ON discovery_runs(status)",
        "CREATE INDEX IF NOT EXISTS idx_discovery_stage_run ON resource_discovery_staging(run_id)",
        "CREATE INDEX IF NOT EXISTS idx_discovery_stage_lookup ON resource_discovery_staging(run_id, tenant_id, resource_type, external_id)",
        # vm_file_index — hot lookup is (snapshot, volume, parent_path)
        # driving per-folder listings in the Volumes tab.
        "CREATE INDEX IF NOT EXISTS idx_vm_file_index_lookup ON vm_file_index(snapshot_id, volume_item_id, parent_path)",
        "CREATE INDEX IF NOT EXISTS idx_vm_file_index_snap ON vm_file_index(snapshot_id)",
        "CREATE INDEX IF NOT EXISTS idx_report_configs_org ON report_configs(org_id)",
        "CREATE INDEX IF NOT EXISTS idx_report_history_org ON report_history(org_id)",
        "CREATE INDEX IF NOT EXISTS idx_report_history_generated ON report_history(generated_at DESC)",
        # Phase 1 — SLA expansion
        "CREATE INDEX IF NOT EXISTS idx_sla_exclusions_policy ON sla_exclusions(policy_id, enabled)",
        "CREATE INDEX IF NOT EXISTS idx_resource_groups_tenant ON resource_groups(tenant_id, enabled)",
        "CREATE INDEX IF NOT EXISTS idx_resource_groups_priority ON resource_groups(tenant_id, priority, enabled)",
        "CREATE INDEX IF NOT EXISTS idx_group_policy_assignments_group ON group_policy_assignments(group_id)",
        "CREATE INDEX IF NOT EXISTS idx_group_policy_assignments_policy ON group_policy_assignments(policy_id)",
        # Two-tier discovery — fast lookup of all child rows under a parent user.
        "CREATE INDEX IF NOT EXISTS ix_resources_parent_id ON resources(parent_resource_id)",
        # backup_batches — operator-intent row for one Backup-all click.
        "CREATE INDEX IF NOT EXISTS ix_backup_batches_tenant_started ON backup_batches (tenant_id, created_at DESC)",
        "CREATE INDEX IF NOT EXISTS ix_backup_batches_status_inprogress ON backup_batches (status) WHERE status = 'IN_PROGRESS'",
        # batch_pending_users — per-user index for the watchdog sweep
        # and the discovery-worker's per-user UPDATE path.
        "CREATE INDEX IF NOT EXISTS ix_batch_pending_users_user ON batch_pending_users(user_id)",
        # Teams-chat packed-blob content-hash dedup. When two users share a
        # chat, their backups hit the same message bytes — this tenant-scoped
        # checksum index lets the writer reuse an existing blob_path instead
        # of re-uploading. Also speeds up the ZIP export dedup pass.
        "CREATE INDEX IF NOT EXISTS idx_snapshot_items_tenant_checksum ON snapshot_items(tenant_id, content_checksum)",
        # Partial BTREE on the JSON `metadata->>'chatId'` projection, scoped
        # to chat messages. Powers the UI's "scope to one chat" filter on a
        # per-user TEAMS_CHAT_EXPORT snapshot (which holds messages from
        # many chats). When the metadata column migrates to JSONB we can add
        # a GIN companion for containment queries.
        "CREATE INDEX IF NOT EXISTS idx_snapshot_items_metadata_chat_id "
        "ON snapshot_items ((metadata->>'chatId')) "
        "WHERE item_type = 'TEAMS_CHAT_MESSAGE'",
        # Covers the cross-snapshot aggregation queries added for mail /
        # chats / calendar / folders — filter on snapshot_id IN (...) and
        # item_type IN (...), then dedupe by external_id. Makes the
        # /folders endpoint + the left-panel count endpoints bounded by
        # index scan instead of full table scan.
        "CREATE INDEX IF NOT EXISTS idx_snapshot_items_snap_type_ext "
        "ON snapshot_items (snapshot_id, item_type, external_id)",
        # Chat export v1 — scope resolution + attachment joins + concurrency cap.
        "CREATE INDEX IF NOT EXISTS idx_snapshot_items_folder_type "
        "ON snapshot_items (folder_path, item_type)",
        "CREATE INDEX IF NOT EXISTS idx_snapshot_items_parent_ext "
        "ON snapshot_items (parent_external_id) "
        "WHERE item_type IN ('CHAT_ATTACHMENT', 'CHAT_HOSTED_CONTENT')",
        "CREATE INDEX IF NOT EXISTS idx_snapshot_items_snapshot_type "
        "ON snapshot_items (snapshot_id, item_type)",
        "CREATE INDEX IF NOT EXISTS idx_snapshot_items_chat_created "
        "ON snapshot_items (snapshot_id, folder_path, created_at) "
        "WHERE item_type IN ('TEAMS_CHAT_MESSAGE','TEAMS_MESSAGE','TEAMS_MESSAGE_REPLY')",
        # idx_jobs_tenant_type_status is intentionally NOT here — it depends
        # on jobs.status having already been converted from VARCHAR to the
        # jobstatus enum. It's created in post_alter_index_statements below.
        # Cross-user chat dedup — read-side filter on last_used_at + drain-claim
        # freshness check both want this index.
        "CREATE INDEX IF NOT EXISTS ix_chat_url_cache_last_used "
        "ON chat_url_cache (tenant_id, last_used_at)",
        "CREATE INDEX IF NOT EXISTS ix_chat_threads_tenant "
        "ON chat_threads (tenant_id, last_drained_at)",
        # Hot read path: hydrate a chat thread newest-first.
        "CREATE INDEX IF NOT EXISTS ix_chat_thread_messages_thread_time "
        "ON chat_thread_messages (chat_thread_id, created_date_time DESC)",
        # Cross-user mail dedup — tenant-scoped fingerprint lookup is the
        # hot upsert key. UNIQUE (tenant_id, fingerprint) is enforced
        # at the DDL level; this index also covers the dedup-ratio
        # rollup query (count by tenant_id).
        "CREATE INDEX IF NOT EXISTS ix_mail_message_bodies_tenant_fp "
        "ON mail_message_bodies (tenant_id, fingerprint)",
        # Purge worker: find unreferenced bodies older than retention.
        "CREATE INDEX IF NOT EXISTS ix_mail_message_bodies_purge "
        "ON mail_message_bodies (last_referenced_at) "
        "WHERE ref_count <= 0",
        # OneDrive file-retry queue — consumer scans for PENDING rows
        # whose next_retry_at has elapsed; partial index keeps the
        # scan O(eligible) instead of O(table). Filtering on status
        # in (PENDING, IN_PROGRESS) keeps the IN_PROGRESS reaper fast
        # too (rescued/permanent terminal rows are not scanned).
        "CREATE INDEX IF NOT EXISTS ix_onedrive_file_retries_ready "
        "ON onedrive_file_retries (next_retry_at) "
        "WHERE status IN ('PENDING', 'IN_PROGRESS')",
        # Per-snapshot lookup for the finalizer's pending-retry count.
        "CREATE INDEX IF NOT EXISTS ix_onedrive_file_retries_snapshot "
        "ON onedrive_file_retries (snapshot_id, status)",
        # snapshot_partitions — finalizer + stale-sweep hot paths.
        # Finalizer: SELECT WHERE snapshot_id=... AND status=...
        # Sweep: oldest enqueued non-terminal first.
        "CREATE INDEX IF NOT EXISTS ix_snap_partition_status "
        "ON snapshot_partitions (snapshot_id, status)",
        "CREATE INDEX IF NOT EXISTS ix_snap_partition_claim "
        "ON snapshot_partitions (enqueued_at) "
        "WHERE status IN ('QUEUED', 'IN_PROGRESS')",
        # NOTE: ix_snapshots_job_resource_inprogress lives in
        # post_alter_index_statements (below) — its WHERE predicate
        # references the snapshots.status enum, so it MUST be created
        # AFTER alter_statements converts the column from VARCHAR to
        # snapshotstatus. Created here it forces the cast `(status)::text`
        # into the index predicate, and the next ALTER COLUMN TYPE then
        # fails with "functions in index predicate must be marked
        # IMMUTABLE" because enum→text is STABLE.
    ]

    add_column_statements = [
        # snapshot_partitions — Phase-2 generalization for multi-workload
        # + multi-region reuse. New columns are nullable / defaulted so
        # existing OneDrive rows (Phase 1) keep working unchanged.
        # IMPORTANT: the ALTER TABLEs run BEFORE everything else in this
        # list so subsequent statements can reference the new columns
        # safely if needed.
        "ALTER TABLE snapshot_partitions "
        "ADD COLUMN IF NOT EXISTS partition_type VARCHAR NOT NULL DEFAULT 'ONEDRIVE_FILES';",
        "ALTER TABLE snapshot_partitions "
        "ADD COLUMN IF NOT EXISTS payload JSON;",
        "ALTER TABLE snapshot_partitions "
        "ADD COLUMN IF NOT EXISTS retry_count INTEGER NOT NULL DEFAULT 0;",
        "ALTER TABLE snapshot_partitions "
        "ADD COLUMN IF NOT EXISTS worker_region VARCHAR;",
        # drive_id was NOT NULL in Phase 1 (OneDrive-only). Loosen so
        # CHATS / MAIL_FOLDERS / SHAREPOINT_DRIVES rows don't need a
        # synthetic value. OneDrive rows still set it; consumer code
        # validates per partition_type.
        "ALTER TABLE snapshot_partitions "
        "ALTER COLUMN drive_id DROP NOT NULL;",
        # file_ids was NOT NULL in Phase 1. Same reasoning.
        "ALTER TABLE snapshot_partitions "
        "ALTER COLUMN file_ids DROP NOT NULL;",
        # Cover the new partition_type filter on hot finalize queries.
        "CREATE INDEX IF NOT EXISTS ix_snap_partition_type_status "
        "ON snapshot_partitions (snapshot_id, partition_type, status);",
        # P2: soft-delete columns on tenants + chat_thread tables. A
        # tenant or chat marked with archived_at is invisible to read
        # paths but physically present until the 30-day purge worker
        # collects it. RESTRICT FKs (DDL above) prevent accidental
        # cascade deletes from wiping chat singletons.
        "ALTER TABLE tenants ADD COLUMN IF NOT EXISTS archived_at TIMESTAMPTZ;",
        "ALTER TABLE chat_threads ADD COLUMN IF NOT EXISTS archived_at TIMESTAMPTZ;",
        "ALTER TABLE chat_thread_messages ADD COLUMN IF NOT EXISTS archived_at TIMESTAMPTZ;",
        # Item C — HC drain overlap (2026-05-17). Tracks whether a
        # USER_CHATS snapshot's hostedContent download is still in
        # flight. Restore paths must check this column before allowing
        # a restore — see Snapshot.hc_drain_status doc in shared/models.
        # Idempotent ADD COLUMN IF NOT EXISTS heals existing prod DBs
        # on next boot without alembic. Default 'NOT_APPLICABLE' so
        # every pre-existing row is restore-ready.
        "ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS "
        "hc_drain_status VARCHAR(16) NOT NULL DEFAULT 'NOT_APPLICABLE';",
        # Hot read path for the post-drain monitor that polls for
        # PENDING snapshots and emits a warning if any are stuck.
        "CREATE INDEX IF NOT EXISTS ix_snapshots_hc_pending "
        "ON snapshots (hc_drain_status) "
        "WHERE hc_drain_status = 'PENDING';",
        # Drain-completeness baseline for the chat partial-drain gate
        # (workers/backup-worker/main.py). NULL = "no baseline yet, skip gate".
        # Idempotent ADD COLUMN IF NOT EXISTS so existing prod DBs heal on
        # next service boot without an explicit alembic step.
        "ALTER TABLE chat_threads ADD COLUMN IF NOT EXISTS last_drained_msg_count INTEGER;",
        # Chat export v1 — link CHAT_ATTACHMENT / CHAT_HOSTED_CONTENT rows to
        # their parent message without scanning the metadata JSONB.
        "ALTER TABLE snapshot_items ADD COLUMN IF NOT EXISTS parent_external_id VARCHAR;",
        "ALTER TABLE tenants ADD COLUMN IF NOT EXISTS customer_id VARCHAR;",
        "ALTER TABLE resources ADD COLUMN IF NOT EXISTS azure_subscription_id VARCHAR;",
        "ALTER TABLE resources ADD COLUMN IF NOT EXISTS azure_resource_group VARCHAR;",
        "ALTER TABLE resources ADD COLUMN IF NOT EXISTS azure_region VARCHAR;",
        "ALTER TABLE resources ADD COLUMN IF NOT EXISTS resource_hash VARCHAR;",
        # Two-tier discovery: child rows point at their parent user resource.
        "ALTER TABLE resources ADD COLUMN IF NOT EXISTS parent_resource_id UUID REFERENCES resources(id) ON DELETE CASCADE;",
        # Matching tier-2 pointer on snapshot items — models.py declared the
        # column but the DB was missing it, which made every Azure Postgres /
        # SQL / VM snapshot persist zero items (INSERT aborted inside the
        # handler's try/except, Recovery returned an empty list).
        "ALTER TABLE snapshot_items ADD COLUMN IF NOT EXISTS parent_external_id VARCHAR;",
        "CREATE INDEX IF NOT EXISTS ix_snapshot_items_parent_external_id ON snapshot_items(parent_external_id);",
        "ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS azure_restore_point_id VARCHAR;",
        "ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS azure_operation_id VARCHAR;",
        "ALTER TABLE sla_policies ADD COLUMN IF NOT EXISTS retention_hot_days INTEGER DEFAULT 7;",
        "ALTER TABLE sla_policies ADD COLUMN IF NOT EXISTS retention_cool_days INTEGER DEFAULT 30;",
        "ALTER TABLE sla_policies ADD COLUMN IF NOT EXISTS retention_archive_days INTEGER;",
        "ALTER TABLE sla_policies ADD COLUMN IF NOT EXISTS legal_hold_enabled BOOLEAN DEFAULT FALSE;",
        "ALTER TABLE sla_policies ADD COLUMN IF NOT EXISTS legal_hold_until TIMESTAMP;",
        "ALTER TABLE sla_policies ADD COLUMN IF NOT EXISTS immutability_mode VARCHAR DEFAULT 'None';",
        "ALTER TABLE sla_policies ADD COLUMN IF NOT EXISTS service_type VARCHAR DEFAULT 'm365';",
        "ALTER TABLE sla_policies ADD COLUMN IF NOT EXISTS backup_azure_vm BOOLEAN DEFAULT TRUE;",
        "ALTER TABLE sla_policies ADD COLUMN IF NOT EXISTS backup_azure_sql BOOLEAN DEFAULT TRUE;",
        "ALTER TABLE sla_policies ADD COLUMN IF NOT EXISTS backup_azure_postgresql BOOLEAN DEFAULT TRUE;",
        "UPDATE sla_policies SET service_type = 'm365' WHERE service_type IS NULL OR service_type = '';",
        "ALTER TABLE tenants ADD COLUMN IF NOT EXISTS dr_region_enabled BOOLEAN DEFAULT FALSE;",
        "ALTER TABLE tenants ADD COLUMN IF NOT EXISTS dr_region VARCHAR;",
        "ALTER TABLE tenants ADD COLUMN IF NOT EXISTS dr_storage_account_name VARCHAR;",
        "ALTER TABLE tenants ADD COLUMN IF NOT EXISTS dr_storage_account_key_encrypted BYTEA;",
        "ALTER TABLE tenants ADD COLUMN IF NOT EXISTS dr_last_replicated_at TIMESTAMP;",
        "ALTER TABLE tenants ADD COLUMN IF NOT EXISTS extra_data JSON DEFAULT '{}';",
        "ALTER TABLE tenants ADD COLUMN IF NOT EXISTS azure_refresh_token_encrypted BYTEA;",
        "ALTER TABLE tenants ADD COLUMN IF NOT EXISTS azure_refresh_token_updated_at TIMESTAMP;",
        "ALTER TABLE tenants ADD COLUMN IF NOT EXISTS azure_subscriptions_cached JSON DEFAULT '{}';",
        "ALTER TABLE tenants ADD COLUMN IF NOT EXISTS azure_sql_servers_configured JSON DEFAULT '{}';",
        "ALTER TABLE tenants ADD COLUMN IF NOT EXISTS azure_pg_servers_configured JSON DEFAULT '{}';",
        "ALTER TABLE report_configs ADD COLUMN IF NOT EXISTS send_detailed_report BOOLEAN DEFAULT FALSE NOT NULL;",
        "ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS dr_replication_status VARCHAR DEFAULT 'pending';",
        "ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS dr_blob_path VARCHAR;",
        "ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS dr_replicated_at TIMESTAMP;",
        "ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS dr_error TEXT;",
        "ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS dr_replication_attempts INTEGER DEFAULT 0;",
        "ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS extra_data JSON DEFAULT '{}';",
        # Snapshot-reuse chain (2026-05-15 design). Two nullable
        # self-references on snapshots.id with ON DELETE RESTRICT so
        # the retention path is forced through the rehydration
        # sequence (shared/retention_cleanup.py::_rehydrate_reuse_heir)
        # — direct DELETE of a snapshot that still has descendants
        # errors out at the DB level, which is the safety net we want.
        # Both columns are NULL together (full snapshot) or NOT NULL
        # together (reuse snapshot); validation trigger below enforces.
        "ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS reuse_of_snapshot_id UUID REFERENCES snapshots(id) ON DELETE RESTRICT;",
        "ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS reuse_chain_root_id UUID REFERENCES snapshots(id) ON DELETE RESTRICT;",
        # Distributed reconciliation lease columns (2026-05-16 design).
        # Three columns per work table: who holds the lease, when it
        # expires, and a monotonic fence token. The fence token defends
        # against resurrected stale workers writing over the
        # reconciler's decision — every status write a worker makes
        # carries WHERE lease_token = :my_token, so a worker whose
        # lease was reassigned writes 0 rows and aborts cleanly.
        # Defaults are nullable lease + token=0 so existing rows stay
        # behaviour-compatible (sweep predicate covers them via the
        # 15-min age fallback in Step C — see spec section 7.3).
        "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS lease_owner_id UUID;",
        "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ;",
        "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS lease_token BIGINT NOT NULL DEFAULT 0;",
        "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS requeue_count INTEGER NOT NULL DEFAULT 0;",
        "ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS lease_owner_id UUID;",
        "ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ;",
        "ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS lease_token BIGINT NOT NULL DEFAULT 0;",
        "ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS requeue_count INTEGER NOT NULL DEFAULT 0;",
        "ALTER TABLE snapshot_partitions ADD COLUMN IF NOT EXISTS lease_owner_id UUID;",
        "ALTER TABLE snapshot_partitions ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ;",
        "ALTER TABLE snapshot_partitions ADD COLUMN IF NOT EXISTS lease_token BIGINT NOT NULL DEFAULT 0;",
        "ALTER TABLE snapshot_partitions ADD COLUMN IF NOT EXISTS requeue_count INTEGER NOT NULL DEFAULT 0;",
        "ALTER TABLE resource_discovery_staging ADD COLUMN IF NOT EXISTS azure_subscription_id VARCHAR;",
        "ALTER TABLE resource_discovery_staging ADD COLUMN IF NOT EXISTS azure_resource_group VARCHAR;",
        "ALTER TABLE resource_discovery_staging ADD COLUMN IF NOT EXISTS azure_region VARCHAR;",
        # Phase 1 schema additions — SLA policy expansion (GFS, item-level, archived rules,
        # storage region, BYOK, auto-apply). All idempotent — safe to re-run on every boot.
        "ALTER TABLE sla_policies ADD COLUMN IF NOT EXISTS retention_mode VARCHAR DEFAULT 'FLAT' NOT NULL;",
        "ALTER TABLE sla_policies ADD COLUMN IF NOT EXISTS gfs_daily_count INTEGER;",
        "ALTER TABLE sla_policies ADD COLUMN IF NOT EXISTS gfs_weekly_count INTEGER;",
        "ALTER TABLE sla_policies ADD COLUMN IF NOT EXISTS gfs_monthly_count INTEGER;",
        "ALTER TABLE sla_policies ADD COLUMN IF NOT EXISTS gfs_yearly_count INTEGER;",
        "ALTER TABLE sla_policies ADD COLUMN IF NOT EXISTS item_retention_days INTEGER;",
        "ALTER TABLE sla_policies ADD COLUMN IF NOT EXISTS item_retention_basis VARCHAR DEFAULT 'SNAPSHOT' NOT NULL;",
        "ALTER TABLE sla_policies ADD COLUMN IF NOT EXISTS archived_retention_mode VARCHAR DEFAULT 'SAME' NOT NULL;",
        "ALTER TABLE sla_policies ADD COLUMN IF NOT EXISTS archived_retention_days INTEGER;",
        "ALTER TABLE sla_policies ADD COLUMN IF NOT EXISTS encryption_mode VARCHAR DEFAULT 'VAULT_MANAGED' NOT NULL;",
        "ALTER TABLE sla_policies ADD COLUMN IF NOT EXISTS key_vault_uri VARCHAR;",
        "ALTER TABLE sla_policies ADD COLUMN IF NOT EXISTS key_name VARCHAR;",
        "ALTER TABLE sla_policies ADD COLUMN IF NOT EXISTS key_version VARCHAR;",
        "ALTER TABLE sla_policies ADD COLUMN IF NOT EXISTS auto_apply_to_matching BOOLEAN DEFAULT FALSE NOT NULL;",
        "ALTER TABLE sla_policies ADD COLUMN IF NOT EXISTS encryption_status VARCHAR DEFAULT '' NOT NULL;",
        # Durability columns — paired with the 5-minute sweeper in
        # backup-scheduler. lifecycle_dirty is set transactionally when an
        # operator saves the policy, the sweeper picks it up if the on-save
        # HTTP nudge fails. Index so the sweeper's WHERE clause is cheap.
        "ALTER TABLE sla_policies ADD COLUMN IF NOT EXISTS lifecycle_dirty BOOLEAN DEFAULT FALSE NOT NULL;",
        "ALTER TABLE sla_policies ADD COLUMN IF NOT EXISTS last_reconciled_at TIMESTAMP;",
        "ALTER TABLE sla_policies ADD COLUMN IF NOT EXISTS reconcile_attempts INTEGER DEFAULT 0 NOT NULL;",
        "ALTER TABLE sla_policies ADD COLUMN IF NOT EXISTS key_version_resolved VARCHAR;",
        # 24h cooldown timestamp for the cap-reached audit alert. Without
        # this the 5-min sweeper refires the same alert 288×/day per stuck
        # policy. Reset to NULL on the next successful reconcile.
        "ALTER TABLE sla_policies ADD COLUMN IF NOT EXISTS last_cap_alert_at TIMESTAMP;",
        "CREATE INDEX IF NOT EXISTS ix_sla_policies_lifecycle_dirty "
        "ON sla_policies (lifecycle_dirty) WHERE lifecycle_dirty = TRUE;",
        # storage_region was a phantom — never read by any consumer (Phase 1
        # confirmed zero readers across tm_backend/). Dropping it here
        # now that the wizard no longer writes it. The matching ADD COLUMN
        # above is a no-op on fresh DBs; on existing DBs the DROP wins.
        "ALTER TABLE sla_policies DROP COLUMN IF EXISTS storage_region;",
        # Singleton default-policy per tenant. Paired with `_enforce_default_singleton`
        # in resource-service/main.py which flips other rows on save. The partial
        # unique index defends against concurrent writes that could otherwise
        # leave two policies marked is_default=true for the same tenant.
        "CREATE UNIQUE INDEX IF NOT EXISTS ix_sla_policies_one_default_per_tenant "
        "ON sla_policies (tenant_id) WHERE is_default = TRUE;",
        # On-prem storage toggle plumbing (migrations 2026-04-21 / 2026-04-22).
        # Ship these via ADD COLUMN IF NOT EXISTS so a fresh DB boot ends up
        # with the full schema even when the raw .sql migrations are never
        # invoked. Everything is idempotent.
        "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS retry_reason TEXT;",
        "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS pre_toggle_job_id UUID REFERENCES jobs(id);",
        "ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS backend_id UUID REFERENCES storage_backends(id);",
        "ALTER TABLE snapshot_items ADD COLUMN IF NOT EXISTS backend_id UUID REFERENCES storage_backends(id);",
        "CREATE INDEX IF NOT EXISTS idx_snapshots_backend ON snapshots(backend_id);",
        "CREATE INDEX IF NOT EXISTS idx_snapshot_items_backend ON snapshot_items(backend_id);",
        # Idempotency guard — pairs with the idempotent create_snapshot()
        # resume path in backup-worker. When a RabbitMQ message is
        # redelivered (worker killed mid-batch / redeploy), the resume
        # path reuses the existing IN_PROGRESS snapshot row; without
        # this unique constraint, both deliveries could write duplicate
        # snapshot_items. On conflict the insert IntegrityErrors and
        # rolls back, the next redelivery picks up cleanly.
        #
        # The triple includes item_type because the same external_id
        # can legitimately appear under multiple types in one snapshot:
        # a user is commonly both GROUP_MEMBER and GROUP_OWNER of the
        # same M365 group (Microsoft Graph returns them separately,
        # backup-worker emits a row for each role). The earlier
        # (snapshot_id, external_id) pair rejected the whole batch with
        # a UniqueViolationError and the scheduler looped on the same
        # group indefinitely, burning CPU.
        #
        # Drop any pre-existing pair-form index so upgrades re-use the
        # slot for the correct triple form.
        "DROP INDEX IF EXISTS uq_snapshot_items_snap_ext;",
        "ALTER TABLE snapshot_items "
        "  DROP CONSTRAINT IF EXISTS uq_snapshot_items_snap_ext;",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_snapshot_items_snap_ext_type "
        "ON snapshot_items(snapshot_id, external_id, item_type);",
        # Hot-path composite index — tenant-scoped time-range scans of
        # a specific item type (e.g. "all EMAILs for tenant X in the
        # last 30 days" for the Recovery tab). Covers the dominant
        # query pattern across /mail, /chats, /calendar, /contacts.
        # At ~500M rows this replaces a seq-scan-and-sort plan with
        # an index scan and cuts p99 from seconds to milliseconds;
        # at small row counts it costs ~200 ms to build and is
        # otherwise invisible.
        "CREATE INDEX IF NOT EXISTS idx_snapshot_items_tenant_type_time "
        "ON snapshot_items (tenant_id, item_type, created_at DESC);",
        # Backfill chat_export_enabled on every existing tenant whose
        # limits dict doesn't mention the flag. Safe to re-run: the
        # ?| operator filters out rows that already have the key set
        # (true OR false — explicit opt-out stays honoured). Without
        # this, /api/v1/exports/chat would 503 FEATURE_NOT_ENABLED on
        # every first-time click for any tenant that predates the
        # three-state gate introduced in chat_export.py:~229.
        #
        # SaaS operators running a progressive rollout can skip this
        # by setting CHAT_EXPORT_DEFAULT_ENABLED=false in env before
        # first boot — the gate then still denies for tenants whose
        # limits doesn't mention the flag, matching the old behaviour.
        "UPDATE tenants "
        "  SET extra_data = jsonb_set("
        "    COALESCE(extra_data::jsonb, '{}'::jsonb),"
        "    '{limits,chat_export_enabled}',"
        "    'true'::jsonb,"
        "    true"
        "  )::json "
        "  WHERE extra_data IS NULL "
        "     OR NOT (COALESCE(extra_data::jsonb, '{}'::jsonb) -> 'limits' ? 'chat_export_enabled');",
    ]

    # Indexes that must be created AFTER alter_statements runs, because they
    # reference enum-typed columns (e.g. jobs.status after it's been converted
    # from VARCHAR to the jobstatus enum). Creating them earlier would block
    # the ALTER COLUMN TYPE conversion.
    post_alter_index_statements = [
        "CREATE INDEX IF NOT EXISTS idx_jobs_tenant_type_status "
        "ON jobs (tenant_id, type, status) "
        "WHERE status IN ('QUEUED'::jobstatus,'PENDING'::jobstatus,'RUNNING'::jobstatus)",
        # Per-resource single-claim guarantee for the fan-out path. The
        # model carries this in Snapshot.__table_args__, but SQLAlchemy
        # create_all() only emits CREATE INDEX for *new* tables — pre-
        # existing snapshots tables never picked it up, which let RMQ
        # message redelivery silently create duplicate IN_PROGRESS rows
        # and re-run the same drain. Explicit raw CREATE INDEX IF NOT
        # EXISTS here makes the migration deterministic. Must be in
        # post_alter list because the WHERE predicate references the
        # snapshotstatus enum (created by alter_statements above).
        "CREATE UNIQUE INDEX IF NOT EXISTS ix_snapshots_job_resource_inprogress "
        "ON snapshots (job_id, resource_id) "
        "WHERE status = 'IN_PROGRESS'::snapshotstatus",
        # Snapshot-reuse chain indexes. Both partial so they cost nothing
        # for the (overwhelming) NULL majority of rows. Built only after
        # alter_statements so the columns exist.
        "CREATE INDEX IF NOT EXISTS ix_snapshots_reuse_of_id "
        "ON snapshots (reuse_of_snapshot_id) "
        "WHERE reuse_of_snapshot_id IS NOT NULL;",
        "CREATE INDEX IF NOT EXISTS ix_snapshots_reuse_chain_root "
        "ON snapshots (reuse_chain_root_id) "
        "WHERE reuse_chain_root_id IS NOT NULL;",
        # Reconciliation sweeper hot-path indexes — partial on the
        # status enum so the index is tiny (only in-flight rows). Must
        # live in post_alter_index_statements because the WHERE
        # predicate references the *status enum cast, which is only
        # available after alter_statements.
        "CREATE INDEX IF NOT EXISTS ix_jobs_running_lease "
        "ON jobs (lease_expires_at) "
        "WHERE status = 'RUNNING'::jobstatus;",
        "CREATE INDEX IF NOT EXISTS ix_snapshots_inprog_lease "
        "ON snapshots (lease_expires_at) "
        "WHERE status = 'IN_PROGRESS'::snapshotstatus;",
        # snapshot_partitions.status is plain VARCHAR today (no enum
        # type), so the partial index doesn't need an enum cast.
        "CREATE INDEX IF NOT EXISTS ix_snapshot_partitions_inprog_lease "
        "ON snapshot_partitions (lease_expires_at) "
        "WHERE status IN ('QUEUED','IN_PROGRESS');",
        "CREATE INDEX IF NOT EXISTS ix_worker_heartbeats_last_seen "
        "ON worker_heartbeats (last_seen_at);",
        "CREATE INDEX IF NOT EXISTS ix_work_dead_letter_kind_created "
        "ON work_dead_letter (work_kind, created_at DESC);",
        # Validation trigger: enforce that any reuse_of_snapshot_id
        # points at a COMPLETED snapshot of the SAME resource taken
        # strictly earlier. PG can't express this in a CHECK
        # constraint (no cross-row predicates). Trigger runs on every
        # INSERT/UPDATE that touches the column; NULL is a fast
        # bypass. The reuse_chain_root_id must also be either NULL
        # (when reuse_of_snapshot_id is NULL) or point at a full
        # snapshot of the same resource.
        """CREATE OR REPLACE FUNCTION validate_snapshot_reuse_target()
        RETURNS TRIGGER AS $$
        BEGIN
            IF NEW.reuse_of_snapshot_id IS NULL
               AND NEW.reuse_chain_root_id IS NULL THEN
                RETURN NEW;
            END IF;
            IF NEW.reuse_of_snapshot_id IS NULL
               OR  NEW.reuse_chain_root_id IS NULL THEN
                RAISE EXCEPTION
                    'snapshot % reuse columns must both be NULL or both NOT NULL',
                    NEW.id;
            END IF;
            -- Parent must be a COMPLETED snapshot of the same resource
            -- taken strictly earlier than this one.
            PERFORM 1
               FROM snapshots p
              WHERE p.id = NEW.reuse_of_snapshot_id
                AND p.resource_id = NEW.resource_id
                AND p.status = 'COMPLETED'::snapshotstatus
                AND COALESCE(p.started_at, p.created_at)
                  < COALESCE(NEW.started_at, NEW.created_at);
            IF NOT FOUND THEN
                RAISE EXCEPTION
                    'reuse_of_snapshot_id % invalid for snapshot % '
                    '(must be COMPLETED, same resource, earlier started_at)',
                    NEW.reuse_of_snapshot_id, NEW.id;
            END IF;
            -- Chain root must point at a full snapshot of the same
            -- resource (reuse_of_snapshot_id IS NULL on that row).
            -- When the parent itself is a full snapshot the chain
            -- root equals the parent; otherwise it inherits.
            PERFORM 1
               FROM snapshots r
              WHERE r.id = NEW.reuse_chain_root_id
                AND r.resource_id = NEW.resource_id
                AND r.reuse_of_snapshot_id IS NULL;
            IF NOT FOUND THEN
                RAISE EXCEPTION
                    'reuse_chain_root_id % invalid for snapshot % '
                    '(must be a full snapshot of the same resource)',
                    NEW.reuse_chain_root_id, NEW.id;
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;""",
        # Trigger is dropped+recreated so re-runs pick up function
        # changes without leaving a stale binding.
        "DROP TRIGGER IF EXISTS snapshots_reuse_validate ON snapshots;",
        "CREATE TRIGGER snapshots_reuse_validate "
        "BEFORE INSERT OR UPDATE OF reuse_of_snapshot_id, reuse_chain_root_id "
        "ON snapshots FOR EACH ROW "
        "EXECUTE FUNCTION validate_snapshot_reuse_target();",
    ]

    alter_statements = [
        """ALTER TABLE tenants ALTER COLUMN type DROP DEFAULT, ALTER COLUMN type TYPE tenanttype USING type::tenanttype, ALTER COLUMN type SET DEFAULT 'M365'::tenanttype;""",
        """ALTER TABLE tenants ALTER COLUMN status DROP DEFAULT, ALTER COLUMN status TYPE tenantstatus USING status::tenantstatus, ALTER COLUMN status SET DEFAULT 'PENDING'::tenantstatus;""",
        """ALTER TABLE resources ALTER COLUMN type DROP DEFAULT, ALTER COLUMN type TYPE resourcetype USING type::resourcetype, ALTER COLUMN type SET DEFAULT 'ENTRA_USER'::resourcetype;""",
        """ALTER TABLE resources ALTER COLUMN status DROP DEFAULT, ALTER COLUMN status TYPE resourcestatus USING status::resourcestatus, ALTER COLUMN status SET DEFAULT 'DISCOVERED'::resourcestatus;""",
        """ALTER TABLE jobs ALTER COLUMN type DROP DEFAULT, ALTER COLUMN type TYPE jobtype USING type::jobtype, ALTER COLUMN type SET DEFAULT 'BACKUP'::jobtype;""",
        """ALTER TABLE jobs ALTER COLUMN status DROP DEFAULT, ALTER COLUMN status TYPE jobstatus USING status::jobstatus, ALTER COLUMN status SET DEFAULT 'QUEUED'::jobstatus;""",
        """ALTER TABLE snapshots ALTER COLUMN type DROP DEFAULT, ALTER COLUMN type TYPE snapshottype USING type::snapshottype, ALTER COLUMN type SET DEFAULT 'FULL'::snapshottype;""",
        """ALTER TABLE snapshots ALTER COLUMN status DROP DEFAULT, ALTER COLUMN status TYPE snapshotstatus USING status::snapshotstatus, ALTER COLUMN status SET DEFAULT 'IN_PROGRESS'::snapshotstatus;""",
        """ALTER TABLE user_roles ALTER COLUMN role TYPE userrole USING role::userrole;""",
        # M365 and Azure datasources share the same Entra tenant id but
        # live in separate tenant rows (distinct Protection pages). Drop
        # the single-column unique constraint on external_tenant_id and
        # replace with a composite (external_tenant_id, type) constraint
        # so both rows can coexist without a duplicate-key error at
        # onboarding time.
        """ALTER TABLE tenants DROP CONSTRAINT IF EXISTS tenants_external_tenant_id_key;""",
        """DROP INDEX IF EXISTS tenants_external_tenant_id_key;""",
        """CREATE UNIQUE INDEX IF NOT EXISTS tenants_external_tenant_id_type_key ON tenants (external_tenant_id, type) WHERE external_tenant_id IS NOT NULL;""",
    ]

    try:
        async with engine.begin() as conn:
            locked = (
                await conn.execute(text(f"SELECT pg_try_advisory_xact_lock({SCHEMA_INIT_LOCK_ID});"))
            ).scalar()
            if not locked:
                if await wait_for_schema_ready():
                    return
                raise RuntimeError(
                    "Timed out waiting for another service to finish database schema initialization"
                )

            await conn.execute(text(f"SET LOCAL lock_timeout = '{DDL_LOCK_TIMEOUT}'"))
            await conn.execute(text(f"SET LOCAL statement_timeout = '{DDL_STATEMENT_TIMEOUT}'"))
            await conn.execute(text("CREATE EXTENSION IF NOT EXISTS pgcrypto"))
            await conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {settings.DB_SCHEMA}"))
            await conn.execute(text(f"SET search_path TO {SEARCH_PATH}"))

            await _execute_batch(conn, enum_type_statements)
            await _execute_batch(conn, table_statements)

            for stmt in add_column_statements:
                await conn.execute(text("SAVEPOINT add_col_sp"))
                try:
                    await conn.execute(text(stmt))
                    await conn.execute(text("RELEASE SAVEPOINT add_col_sp"))
                except Exception as stmt_exc:
                    await conn.execute(text("ROLLBACK TO SAVEPOINT add_col_sp"))
                    logger.debug("[DB INIT] add_column skipped: %s — %s", stmt[:80], stmt_exc)

            await _execute_batch(conn, index_statements)

            for stmt in alter_statements:
                await conn.execute(text("SAVEPOINT alter_sp"))
                try:
                    await conn.execute(text(stmt))
                    await conn.execute(text("RELEASE SAVEPOINT alter_sp"))
                except Exception:
                    await conn.execute(text("ROLLBACK TO SAVEPOINT alter_sp"))  # Already converted

            await conn.run_sync(Base.metadata.create_all)

        # Enum value additions (e.g. jobstatus PENDING/CANCELLING) must run
        # OUTSIDE a transaction. Chat-export indexes that reference those
        # values then go in their own transaction.
        await _ensure_enum_values()
        async with engine.begin() as conn:
            await _execute_batch(conn, post_alter_index_statements)
        await _seed_preset_policies_for_existing_tenants()
        try:
            from shared.storage_bootstrap import ensure_storage_bootstrap
            await ensure_storage_bootstrap(engine)
        except Exception as sb_exc:
            logger.error("[DB INIT] storage bootstrap failed: %s", sb_exc)
            raise
    except Exception as exc:
        logger.warning("[DB INIT] Schema sync phase failed: %s", exc)
        if await wait_for_schema_ready(timeout_seconds=30):
            return
        raise


async def _seed_preset_policies_for_existing_tenants() -> None:
    """Backfill afi-style preset SLA policies (Gold/Silver/Bronze/Manual) for any
    tenant that doesn't already have them. Idempotent — re-running is a no-op.
    Auth-service seeds on tenant creation; this catches tenants that pre-date that
    hook. Safe to call from every service's init_db (advisory lock above serializes)."""
    try:
        from shared.models import Tenant
        from shared.sla_presets import seed_preset_policies
    except Exception as exc:
        logger.warning("[DB INIT] preset seeder unavailable: %s", exc)
        return

    from sqlalchemy import select
    total = 0
    try:
        async with async_session_factory() as session:
            tenants = (await session.execute(select(Tenant))).scalars().all()
        for t in tenants:
            ttype = getattr(t.type, "value", t.type)
            try:
                async with async_session_factory() as session:
                    n = await seed_preset_policies(session, t.id, str(ttype))
                    if n:
                        await session.commit()
                        total += n
                        logger.info("[DB INIT] Seeded %d preset policies for tenant %s (%s)", n, t.id, t.display_name)
            except Exception as exc:
                logger.warning("[DB INIT] preset seeding failed for tenant %s: %s", t.id, exc)
        if total:
            logger.info("[DB INIT] Preset SLA backfill complete — %d policies inserted across %d tenant(s)", total, len(tenants))
    except Exception as exc:
        logger.warning("[DB INIT] preset backfill skipped: %s", exc)


async def close_db():
    await engine.dispose()
