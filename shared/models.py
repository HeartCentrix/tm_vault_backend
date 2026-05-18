"""Shared database models"""
import uuid
from datetime import datetime, timezone
from typing import Tuple
from sqlalchemy import (
    Column, String, DateTime, Boolean, Integer, BigInteger,
    Text, ForeignKey, Enum as SAEnum, JSON, ARRAY, func, LargeBinary,
    Index, text as sql_text,
)
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID
import enum

from shared.database import Base


def utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)


class UserRole(str, enum.Enum):
    SUPER_ADMIN = "SUPER_ADMIN"
    ORG_ADMIN = "ORG_ADMIN"
    TENANT_ADMIN = "TENANT_ADMIN"
    BACKUP_OPERATOR = "BACKUP_OPERATOR"
    RESTORE_OPERATOR = "RESTORE_OPERATOR"
    CONTENT_VIEWER = "CONTENT_VIEWER"
    USER = "USER"


class TenantType(str, enum.Enum):
    M365 = "M365"
    AZURE = "AZURE"
    # Legacy 'BOTH' removed. A tenant is now exactly one workload type; to back up
    # M365 + Azure for the same Microsoft tenant, create two tenant rows.


class TenantStatus(str, enum.Enum):
    PENDING = "PENDING"
    ACTIVE = "ACTIVE"
    DISCONNECTED = "DISCONNECTED"
    SUSPENDED = "SUSPENDED"
    PENDING_DELETION = "PENDING_DELETION"
    DISCOVERING = "DISCOVERING"
    PENDING_DISCOVERY = "PENDING_DISCOVERY"  # Tenant saved but discovery not yet enqueued


class ResourceType(str, enum.Enum):
    MAILBOX = "MAILBOX"
    SHARED_MAILBOX = "SHARED_MAILBOX"
    ROOM_MAILBOX = "ROOM_MAILBOX"
    ONEDRIVE = "ONEDRIVE"
    SHAREPOINT_SITE = "SHAREPOINT_SITE"
    TEAMS_CHANNEL = "TEAMS_CHANNEL"
    TEAMS_CHAT = "TEAMS_CHAT"
    TEAMS_CHAT_EXPORT = "TEAMS_CHAT_EXPORT"
    # Singleton per-tenant "Azure Active Directory" resource — matches
    # AFI's `office_directory` kind. Stores the 8 Entra-wide content
    # categories (users, groups, roles, security, audit, applications,
    # intune, admin units) as snapshot items under this one resource.
    ENTRA_DIRECTORY = "ENTRA_DIRECTORY"
    ENTRA_USER = "ENTRA_USER"
    ENTRA_GROUP = "ENTRA_GROUP"
    M365_GROUP = "M365_GROUP"  # Unified (modern) group — links group mailbox + SP site + (optional) Team
    ENTRA_CONDITIONAL_ACCESS = "ENTRA_CONDITIONAL_ACCESS"  # CA policy (full JSON definition incl. conditions + grants)
    ENTRA_BITLOCKER_KEY = "ENTRA_BITLOCKER_KEY"  # Per-device recovery key (no key bytes — just the metadata Graph exposes)
    ENTRA_APP = "ENTRA_APP"
    ENTRA_SERVICE_PRINCIPAL = "ENTRA_SERVICE_PRINCIPAL"
    ENTRA_DEVICE = "ENTRA_DEVICE"
    ENTRA_ROLE = "ENTRA_ROLE"
    ENTRA_ADMIN_UNIT = "ENTRA_ADMIN_UNIT"
    ENTRA_AUDIT_LOG = "ENTRA_AUDIT_LOG"
    INTUNE_MANAGED_DEVICE = "INTUNE_MANAGED_DEVICE"
    AZURE_VM = "AZURE_VM"
    AZURE_SQL_DB = "AZURE_SQL_DB"
    AZURE_POSTGRESQL = "AZURE_POSTGRESQL"
    AZURE_POSTGRESQL_SINGLE = "AZURE_POSTGRESQL_SINGLE"
    RESOURCE_GROUP = "RESOURCE_GROUP"
    DYNAMIC_GROUP = "DYNAMIC_GROUP"
    POWER_BI = "POWER_BI"
    POWER_APPS = "POWER_APPS"
    POWER_AUTOMATE = "POWER_AUTOMATE"
    POWER_DLP = "POWER_DLP"
    COPILOT = "COPILOT"
    PLANNER = "PLANNER"
    TODO = "TODO"
    ONENOTE = "ONENOTE"
    # Tier 2 per-user content categories — children of an ENTRA_USER row,
    # linked via parent_resource_id. Distinct from the Tier 1 MAILBOX /
    # ONEDRIVE / TEAMS_CHAT types so stale-marking on a Tier 1 run doesn't
    # touch Tier 2 children.
    USER_MAIL = "USER_MAIL"
    USER_ONEDRIVE = "USER_ONEDRIVE"
    USER_CONTACTS = "USER_CONTACTS"
    USER_CALENDAR = "USER_CALENDAR"
    USER_CHATS = "USER_CHATS"


# Resource types hidden from UI listing endpoints by default. Shared between
# resource-service (filters /by-type and /resources listings) and
# dashboard-service (filters the Protection Status GROUP BY) so the
# Overview cards and the underlying tab lists always agree on the same
# universe of rows. A caller that genuinely needs hidden rows can opt in via
# ?includeHidden=true on the listing endpoints.
#
# Why each is hidden:
#   TEAMS_CHAT_EXPORT — backup-scheduler-internal per-user shard that carries
#       the Graph delta token for the whole-user chat export; not a
#       user-facing entity (TEAMS_CHAT rows are).
#   USER_MAIL / USER_ONEDRIVE / USER_CONTACTS / USER_CALENDAR / USER_CHATS —
#       Tier 2 per-content-category children under an ENTRA_USER parent.
#       The parent row already rolls up their storage_bytes and last-backup
#       timestamps, so surfacing them in Protection creates five dupes per
#       user each needing their own SLA — which isn't how protection works.
#   TEAMS_CHANNEL — duplicates an M365_GROUP row for the same Team (same
#       external_id). M365_GROUP backup fans out into channels + group
#       mailbox + team site, so the TEAMS_CHANNEL row is redundant.
UI_HIDDEN_TYPES: set[str] = {
    "TEAMS_CHAT_EXPORT",
    "USER_MAIL", "USER_ONEDRIVE", "USER_CONTACTS", "USER_CALENDAR", "USER_CHATS",
    "TEAMS_CHANNEL",
}


class ResourceStatus(str, enum.Enum):
    DISCOVERED = "DISCOVERED"
    ACTIVE = "ACTIVE"
    ARCHIVED = "ARCHIVED"
    SUSPENDED = "SUSPENDED"
    PENDING_DELETION = "PENDING_DELETION"
    INACCESSIBLE = "INACCESSIBLE"  # Resource not found (404) or locked (423) in source system


class JobType(str, enum.Enum):
    BACKUP = "BACKUP"
    RESTORE = "RESTORE"
    EXPORT = "EXPORT"
    DISCOVERY = "DISCOVERY"
    DELETE = "DELETE"


class JobStatus(str, enum.Enum):
    QUEUED = "QUEUED"
    PENDING = "PENDING"  # T0 migration: chat-export PENDING (queued-but-idempotency-safe)
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    CANCELLING = "CANCELLING"  # T0 migration: transient state while worker wraps up
    RETRYING = "RETRYING"


class SnapshotType(str, enum.Enum):
    FULL = "FULL"
    INCREMENTAL = "INCREMENTAL"
    PREEMPTIVE = "PREEMPTIVE"
    MANUAL = "MANUAL"


class SnapshotStatus(str, enum.Enum):
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    PARTIAL = "PARTIAL"
    PENDING_DELETION = "PENDING_DELETION"


class Organization(Base):
    __tablename__ = "organizations"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String, nullable=False)
    slug = Column(String, unique=True, nullable=False)
    storage_region = Column(String)
    encryption_mode = Column(String, default="TMVAULT_MANAGED")
    storage_quota_bytes = Column(BigInteger, default=500 * 1024**3)
    storage_bytes_used = Column(BigInteger, default=0)
    created_at = Column(DateTime, default=utcnow)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)


class Tenant(Base):
    __tablename__ = "tenants"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False)
    type = Column(SAEnum(TenantType), default=TenantType.M365)
    display_name = Column(String, nullable=False)
    external_tenant_id = Column(String, unique=True, index=True)

    customer_id = Column(String)
    subscription_id = Column(String)
    client_id = Column(String)
    client_secret_ref = Column(String)
    # Graph API app-only credentials (encrypted)
    graph_client_id = Column(String, nullable=True)
    graph_client_secret_encrypted = Column(LargeBinary, nullable=True)
    status = Column(SAEnum(TenantStatus), default=TenantStatus.PENDING)
    storage_region = Column(String)
    last_discovery_at = Column(DateTime)
    graph_delta_tokens = Column(MutableDict.as_mutable(JSON), default=dict)
    created_at = Column(DateTime, default=utcnow)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)

    # AZ-4: Cross-region DR replication fields
    dr_region_enabled = Column(Boolean, default=False, nullable=False)
    dr_region = Column(String, nullable=True)  # e.g., "westeurope"
    dr_storage_account_name = Column(String, nullable=True)
    dr_storage_account_key_encrypted = Column(LargeBinary, nullable=True)
    dr_last_replicated_at = Column(DateTime, nullable=True)

    # Afi.ai-style Azure onboarding fields
    azure_refresh_token_encrypted = Column(LargeBinary, nullable=True)
    azure_refresh_token_updated_at = Column(DateTime(timezone=True), nullable=True)
    azure_subscriptions_cached = Column(JSON, default=dict, nullable=False)
    azure_sql_servers_configured = Column(JSON, default=dict, nullable=False)
    azure_pg_servers_configured = Column(JSON, default=dict, nullable=False)
    extra_data = Column(MutableDict.as_mutable(JSON), default=dict, nullable=True)

    # P2: soft delete. archived_at != NULL hides the tenant from all read
    # paths but keeps rows physically present until tenant-purge-worker
    # collects them after a 30-day grace period.
    archived_at = Column(DateTime(timezone=True), nullable=True)


class PlatformUser(Base):
    __tablename__ = "platform_users"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email = Column(String, unique=True, nullable=False, index=True)
    name = Column(String, nullable=False)
    external_user_id = Column(String)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"))
    tenant_id = Column(UUID(as_uuid=True), ForeignKey("tenants.id"))
    mfa_enabled = Column(Boolean, default=False)
    last_login_at = Column(DateTime)
    created_at = Column(DateTime, default=utcnow)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)


class UserRoleMapping(Base):
    __tablename__ = "user_roles"
    user_id = Column(UUID(as_uuid=True), ForeignKey("platform_users.id"), primary_key=True)
    role = Column(SAEnum(UserRole), primary_key=True)


class Resource(Base):
    __tablename__ = "resources"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id = Column(UUID(as_uuid=True), ForeignKey("tenants.id"), nullable=False, index=True)
    type = Column(SAEnum(ResourceType), nullable=False)
    external_id = Column(String, nullable=False)
    display_name = Column(String, nullable=False)
    email = Column(String)
    extra_data = Column("metadata", MutableDict.as_mutable(JSON), default=dict)
    resource_hash = Column(String, nullable=True)
    sla_policy_id = Column(UUID(as_uuid=True), ForeignKey("sla_policies.id"))
    status = Column(SAEnum(ResourceStatus), default=ResourceStatus.DISCOVERED)
    last_backup_job_id = Column(UUID(as_uuid=True), ForeignKey("jobs.id"))
    last_backup_at = Column(DateTime)
    last_backup_status = Column(String)
    storage_bytes = Column(BigInteger, default=0)
    discovered_at = Column(DateTime, default=utcnow)
    archived_at = Column(DateTime)
    deletion_queued_at = Column(DateTime)
    created_at = Column(DateTime, default=utcnow)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)

    # Azure workload metadata (for VM, SQL, PostgreSQL)
    azure_subscription_id = Column(String, nullable=True)
    azure_resource_group = Column(String, nullable=True)
    azure_region = Column(String, nullable=True)

    # Two-tier discovery: Tier 1 creates parent rows (ENTRA_USER, etc.) with
    # parent_resource_id NULL; Tier 2 creates child rows (MAILBOX, ONEDRIVE,
    # USER_CONTACTS, USER_CALENDAR, TEAMS_CHAT) pointing at the parent user
    # via parent_resource_id. Lets the UI group all of a user's backup-ables
    # under one card.
    parent_resource_id = Column(UUID(as_uuid=True), ForeignKey("resources.id", ondelete="CASCADE"), nullable=True, index=True)

    # Eager-load target for the scheduler dispatcher
    # (backup-scheduler uses selectinload(Resource.tenant)).
    tenant = relationship("Tenant", foreign_keys=[tenant_id], lazy="raise")


class SlaPolicy(Base):
    __tablename__ = "sla_policies"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id = Column(UUID(as_uuid=True), ForeignKey("tenants.id"), nullable=False, index=True)
    service_type = Column(String, default="m365", nullable=False, index=True)
    name = Column(String, nullable=False)
    frequency = Column(String, default="DAILY")
    backup_days = Column(ARRAY(String), default=["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"])
    backup_window_start = Column(String)
    backup_window_end = Column(String)
    backup_exchange = Column(Boolean, default=True)
    backup_exchange_archive = Column(Boolean, default=False)
    backup_exchange_recoverable = Column(Boolean, default=False)
    backup_onedrive = Column(Boolean, default=True)
    backup_sharepoint = Column(Boolean, default=True)
    backup_teams = Column(Boolean, default=True)
    backup_teams_chats = Column(Boolean, default=False)
    backup_entra_id = Column(Boolean, default=True)
    backup_power_platform = Column(Boolean, default=False)
    backup_copilot = Column(Boolean, default=False)
    contacts = Column(Boolean, default=True)
    calendars = Column(Boolean, default=True)
    tasks = Column(Boolean, default=False)
    group_mailbox = Column(Boolean, default=True)
    planner = Column(Boolean, default=False)
    backup_azure_vm = Column(Boolean, default=True)
    backup_azure_sql = Column(Boolean, default=True)
    backup_azure_postgresql = Column(Boolean, default=True)
    resource_types = Column(ARRAY(String), default=[])
    batch_size = Column(Integer, default=20)
    max_concurrent_backups = Column(Integer, default=50)
    sla_violation_alert = Column(Boolean, default=True)
    retention_type = Column(String, default="INDEFINITE")
    retention_days = Column(Integer)
    retention_versions = Column(Integer)
    # AZ-0: Tiered retention policy (Hot → Cool → Archive → Delete)
    retention_hot_days = Column(Integer, default=7, nullable=False)
    retention_cool_days = Column(Integer, default=30, nullable=False)
    retention_archive_days = Column(Integer, nullable=True)  # NULL = unlimited (no delete rule)
    legal_hold_enabled = Column(Boolean, default=False, nullable=False)
    legal_hold_until = Column(DateTime, nullable=True)
    immutability_mode = Column(String, default="None", nullable=False)  # "None", "Unlocked", "Locked"

    # Retention scheme (afi.ai parity): how the worker decides which snapshots/items to prune.
    # FLAT       = simple days-since cutoff (uses retention_days / retention_hot/cool/archive_days)
    # GFS        = Grandfather-Father-Son: keep N daily, N weekly, N monthly, N yearly
    # ITEM_LEVEL = per-item cutoff based on the item's own date (email receivedDate, file mTime)
    # HYBRID     = FLAT for snapshots + ITEM_LEVEL for items within retained snapshots
    retention_mode = Column(String, default="FLAT", nullable=False)
    gfs_daily_count = Column(Integer, nullable=True)
    gfs_weekly_count = Column(Integer, nullable=True)
    gfs_monthly_count = Column(Integer, nullable=True)
    gfs_yearly_count = Column(Integer, nullable=True)
    item_retention_days = Column(Integer, nullable=True)
    item_retention_basis = Column(String, default="SNAPSHOT", nullable=False)  # "SNAPSHOT" | "ITEM_DATE"

    # Separate retention for resources marked ARCHIVED (afi dropdown):
    # SAME       = reuse the live retention rules (default)
    # KEEP_ALL   = never prune
    # KEEP_LAST  = keep only the most recent snapshot
    # CUSTOM     = use archived_retention_days for a flat cutoff
    archived_retention_mode = Column(String, default="SAME", nullable=False)
    archived_retention_days = Column(Integer, nullable=True)

    # storage_region was a phantom column with zero readers across the
    # codebase (Phase 1 verified). Dropped via the migration in
    # shared/database.py — TMvault routes by single global active backend
    # via StorageRouter, not per-policy region.
    encryption_mode = Column(String, default="VAULT_MANAGED", nullable=False)  # VAULT_MANAGED | CUSTOMER_KEY
    key_vault_uri = Column(String, nullable=True)  # for BYOK
    key_name = Column(String, nullable=True)
    key_version = Column(String, nullable=True)
    # Reconciler-set status: "" (not applicable / VAULT_MANAGED), "OK",
    # "KEY_VAULT_ACCESS_DENIED" (managed identity missing role assignment),
    # or "ERROR". Surfaced in the SLA list UI as a red dot when not OK.
    encryption_status = Column(String, default="", nullable=False)
    # When the operator changes any reconciler-driven setting (retention
    # tiers, immutability, legal hold, encryption mode/key) we set
    # lifecycle_dirty=true in the same transaction as the policy write.
    # The 5-minute sweeper picks up dirty policies and re-applies — this
    # is the durable fallback if the on-save HTTP nudge to the scheduler
    # drops on the floor (network blip, scheduler restart, etc.).
    lifecycle_dirty = Column(Boolean, default=False, nullable=False, index=True)
    last_reconciled_at = Column(DateTime, nullable=True)
    reconcile_attempts = Column(Integer, default=0, nullable=False)
    # CMK key version actually applied. Distinct from `key_version` which
    # is the operator-stated intent ("latest" or a pinned version). On
    # drift detection / rotation, the reconciler resolves "latest" against
    # Key Vault and stamps the resolved version here.
    key_version_resolved = Column(String, nullable=True)
    # Last time the cap-reached alert was emitted for this policy. The
    # 5-minute sweeper would otherwise refire `SLA_RECONCILE_ATTEMPT_CAP_REACHED`
    # every tick (288×/day) for every stuck policy — drowning the audit
    # channel and any downstream PagerDuty rule. We dedupe to one alert
    # per 24h until reconcile_attempts is reset (a successful run zeroes
    # both attempts and last_cap_alert_at).
    last_cap_alert_at = Column(DateTime, nullable=True)

    # Auto-apply hook — when true, the discovery-worker will assign this policy to
    # any newly-discovered resource that matches one of its resource-group rules.
    auto_apply_to_matching = Column(Boolean, default=False, nullable=False)

    enabled = Column(Boolean, default=True)
    is_default = Column(Boolean, default=False)
    created_at = Column(DateTime, default=utcnow)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)


class SlaExclusion(Base):
    """Exclusion rule attached to an SLA policy.

    Backup handlers consult the parent policy's exclusions before staging each
    item. apply_to_historical means an offline job should also purge matching
    items from prior snapshots (retroactive compliance / space reclaim)."""
    __tablename__ = "sla_exclusions"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    policy_id = Column(UUID(as_uuid=True), ForeignKey("sla_policies.id", ondelete="CASCADE"), nullable=False, index=True)
    # FOLDER_PATH | FILE_EXTENSION | SUBJECT_REGEX | MIME_TYPE | EMAIL_ADDRESS | FILENAME_GLOB
    exclusion_type = Column(String, nullable=False)
    pattern = Column(String, nullable=False)
    # Optional scope to a single workload family. NULL = applies everywhere relevant.
    # Values: EMAIL | FILE | CALENDAR | CONTACT | TEAMS_MESSAGE | CHAT_MESSAGE | ALL
    workload = Column(String, nullable=True)
    apply_to_historical = Column(Boolean, default=False, nullable=False)
    enabled = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=utcnow)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)


class ResourceGroup(Base):
    """Named group of resources. Two kinds:
    - DYNAMIC: rules[] evaluated against resource attributes (name, email, dept, etc.)
    - STATIC:  explicit list of resource IDs stored via resource.metadata or join table
    rules is a list of {field, operator, value} dicts; combinator picks AND vs OR.
    Priority affects tie-breaking when a resource matches multiple groups
    (lower = higher priority — matches afi's Dynamic > Provider > Default ordering)."""
    __tablename__ = "resource_groups"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id = Column(UUID(as_uuid=True), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False, index=True)
    name = Column(String, nullable=False)
    description = Column(Text, nullable=True)
    group_type = Column(String, default="DYNAMIC", nullable=False)  # STATIC | DYNAMIC | PROVIDER_NATIVE
    rules = Column(JSON, default=list, nullable=False)
    combinator = Column(String, default="AND", nullable=False)  # AND | OR
    priority = Column(Integer, default=100, nullable=False)
    auto_protect_new = Column(Boolean, default=False, nullable=False)
    enabled = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=utcnow)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)


class GroupPolicyAssignment(Base):
    """Link table: which SLA policies are attached to which resource groups."""
    __tablename__ = "group_policy_assignments"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    group_id = Column(UUID(as_uuid=True), ForeignKey("resource_groups.id", ondelete="CASCADE"), nullable=False, index=True)
    policy_id = Column(UUID(as_uuid=True), ForeignKey("sla_policies.id", ondelete="CASCADE"), nullable=False, index=True)
    created_at = Column(DateTime, default=utcnow)


class Job(Base):
    __tablename__ = "jobs"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    type = Column(SAEnum(JobType), nullable=False)
    tenant_id = Column(UUID(as_uuid=True), ForeignKey("tenants.id"), index=True)
    resource_id = Column(UUID(as_uuid=True), ForeignKey("resources.id"))
    batch_resource_ids = Column(ARRAY(UUID(as_uuid=True)), default=[])  # NEW: for mass backup
    snapshot_id = Column(UUID(as_uuid=True), ForeignKey("snapshots.id"))
    status = Column(SAEnum(JobStatus), default=JobStatus.QUEUED)
    priority = Column(Integer, default=5)
    attempts = Column(Integer, default=0)
    max_attempts = Column(Integer, default=5)
    error_message = Column(Text)
    progress_pct = Column(Integer, default=0)
    items_processed = Column(BigInteger, default=0)
    bytes_processed = Column(BigInteger, default=0)
    result = Column(JSON, default=dict)
    spec = Column(JSON, default=dict)
    created_at = Column(DateTime, default=utcnow)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)
    completed_at = Column(DateTime)
    # Storage toggle retry plumbing (2026-04-21)
    retry_reason = Column(Text)
    pre_toggle_job_id = Column(UUID(as_uuid=True), ForeignKey("jobs.id"))
    # Distributed reconciliation lease (2026-05-16 design). See
    # docs/superpowers/specs/2026-05-16-distributed-reconciliation-design.md.
    lease_owner_id = Column(UUID(as_uuid=True), nullable=True)
    lease_expires_at = Column(DateTime(timezone=True), nullable=True)
    lease_token = Column(BigInteger, nullable=False, default=0)
    requeue_count = Column(Integer, nullable=False, default=0)


class BackupBatch(Base):
    """Operator-intent row for one 'Backup all' / 'Backup user' click.

    Inserted at click time. Every Job, RMQ message, and discovery
    follow-up stamps spec.batch_id = this.id. Activity feed reads this
    row directly; the strict 4-condition finalizer
    (shared.batch_rollup._finalize_batch_if_complete) flips status
    terminal only when every scoped leaf has a terminal snapshot AND
    no snapshot_partitions row remains in-flight.
    """
    __tablename__ = "backup_batches"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    created_at = Column(DateTime, nullable=False, default=utcnow)
    completed_at = Column(DateTime, nullable=True)
    source = Column(String, nullable=False)  # 'manual_bulk' | 'manual_user' | 'scheduler'
    actor_email = Column(String, nullable=True)
    scope_user_ids = Column(ARRAY(UUID(as_uuid=True)), nullable=False)
    bytes_expected = Column(BigInteger, nullable=True)
    status = Column(String, nullable=False, default="IN_PROGRESS")


class Snapshot(Base):
    __tablename__ = "snapshots"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    resource_id = Column(UUID(as_uuid=True), ForeignKey("resources.id"), nullable=False, index=True)
    job_id = Column(UUID(as_uuid=True), ForeignKey("jobs.id"))
    type = Column(SAEnum(SnapshotType), default=SnapshotType.INCREMENTAL)
    status = Column(SAEnum(SnapshotStatus), default=SnapshotStatus.IN_PROGRESS)
    started_at = Column(DateTime, default=utcnow)
    completed_at = Column(DateTime)
    duration_secs = Column(Integer)
    item_count = Column(Integer, default=0)
    new_item_count = Column(Integer, default=0)
    bytes_added = Column(BigInteger, default=0)
    bytes_total = Column(BigInteger, default=0)
    delta_token = Column(String)
    delta_tokens_json = Column(MutableDict.as_mutable(JSON), default=dict)  # per-folder/resource delta tokens
    extra_data = Column(MutableDict.as_mutable(JSON), default=dict)  # VM backup metadata (config blobs, disk info, etc.)
    snapshot_label = Column(String)
    content_checksum = Column(String)  # NEW: SHA-256 of stored blob
    blob_path = Column(String)  # NEW: full Azure Blob path
    storage_version = Column(Integer, default=1)  # NEW: storage schema version
    azure_restore_point_id = Column(String, nullable=True)  # VM restore point ID for restore
    azure_operation_id = Column(String, nullable=True)  # Track in-flight Azure LROs for resume
    # AZ-4: Cross-region DR replication fields
    dr_replication_status = Column(String, default="pending", nullable=False)  # "pending", "in_progress", "replicated", "failed", "skipped"
    dr_blob_path = Column(String, nullable=True)
    dr_replicated_at = Column(DateTime, nullable=True)
    dr_error = Column(Text, nullable=True)
    dr_replication_attempts = Column(Integer, default=0, nullable=False)
    # Storage backend that holds this snapshot's blobs (2026-04-21).
    # NOT NULL enforced after backfill migration.
    backend_id = Column(UUID(as_uuid=True), ForeignKey("storage_backends.id"), nullable=False)
    # Snapshot-reuse chain (2026-05-15 design — see
    # docs/superpowers/specs/2026-05-15-snapshot-reuse-pointer-design.md).
    # A "reuse" snapshot owns ZERO snapshot_items rows; reads resolve
    # via reuse_chain_root_id to the row-bearing ancestor. NULL on
    # pre-deploy snapshots and on every full (non-reuse) snapshot —
    # behaviour is unchanged for those rows. Validation trigger
    # (shared/database.py:snapshots_reuse_validate) enforces same-
    # tenant/same-resource/COMPLETED/earlier-started_at on the
    # parent so a chain can never cross isolation boundaries.
    reuse_of_snapshot_id = Column(
        UUID(as_uuid=True),
        ForeignKey("snapshots.id", ondelete="RESTRICT"),
        nullable=True,
    )
    # Denormalised: terminal ancestor in the chain. Lets the read-path
    # resolver be one indexed lookup instead of a recursive walk. Equal
    # to reuse_of_snapshot_id when the parent is a full snapshot;
    # inherits the parent's reuse_chain_root_id otherwise. Always NULL
    # together with reuse_of_snapshot_id.
    reuse_chain_root_id = Column(
        UUID(as_uuid=True),
        ForeignKey("snapshots.id", ondelete="RESTRICT"),
        nullable=True,
    )
    # Reconciliation lease (2026-05-16 design).
    lease_owner_id = Column(UUID(as_uuid=True), nullable=True)
    lease_expires_at = Column(DateTime(timezone=True), nullable=True)
    lease_token = Column(BigInteger, nullable=False, default=0)
    requeue_count = Column(Integer, nullable=False, default=0)
    # Item C: HC drain overlap (2026-05-17).
    # USER_CHATS snapshots fire hostedContent (inline images) downloads
    # as background tasks. Previously the handler waited at a barrier
    # before returning — blocking the consumer slot until HC drained.
    # With USER_CHATS_HC_BARRIER_DETACHED=true the handler returns
    # immediately after persisting message bodies; HC drains in the
    # background and flips this column to COMPLETE when done. Restore
    # paths MUST refuse to restore a snapshot whose hc_drain_status is
    # PENDING — the HC items haven't all written yet.
    #
    # Values:
    #   "NOT_APPLICABLE" — non-chat snapshot, or HC interleave disabled
    #   "PENDING"        — drain started, items still streaming in
    #   "COMPLETE"       — every kicked task settled with no exception
    #   "FAILED"         — at least one task raised; restore disallowed
    hc_drain_status = Column(String(16), nullable=False, default="NOT_APPLICABLE")
    created_at = Column(DateTime, default=utcnow)

    __table_args__ = (
        # Per-resource single-claim guarantee for the fan-out path. When a
        # bulk Job fans out 5k per-resource messages, RMQ may redeliver a
        # message after a worker crash; without this index two workers could
        # both INSERT IN_PROGRESS rows for the same (job_id, resource_id)
        # and we'd double the work + double-bill blob storage. The partial
        # WHERE clause lets historical terminal-state snapshots coexist
        # (multiple COMPLETED rows over time for the same resource is the
        # normal case for retention), only the *active* claim is unique.
        Index(
            "ix_snapshots_job_resource_inprogress",
            "job_id", "resource_id",
            unique=True,
            postgresql_where=sql_text("status = 'IN_PROGRESS'"),
        ),
    )


class SnapshotItem(Base):
    __tablename__ = "snapshot_items"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    snapshot_id = Column(UUID(as_uuid=True), ForeignKey("snapshots.id"), nullable=False, index=True)
    tenant_id = Column(UUID(as_uuid=True), ForeignKey("tenants.id"), index=True)
    external_id = Column(String, nullable=False)
    parent_external_id = Column(String, index=True)
    item_type = Column(String, nullable=False)
    name = Column(String, nullable=False)
    folder_path = Column(String)
    content_hash = Column(String, index=True)
    content_checksum = Column(String)  # NEW: SHA-256 integrity checksum
    content_size = Column(BigInteger, default=0)
    blob_path = Column(String)  # NEW: Azure Blob path for this item
    encryption_key_id = Column(String)  # NEW: DEK version used
    backup_version = Column(Integer, default=1)  # NEW: backup schema version
    extra_data = Column("metadata", JSON, default=dict)
    is_deleted = Column(Boolean, default=False)
    indexed_at = Column(DateTime)
    # Storage backend that holds this item's blob. Permanent — wins over
    # system_config.active_backend_id during passthrough restores.
    backend_id = Column(UUID(as_uuid=True), ForeignKey("storage_backends.id"), nullable=False)
    created_at = Column(DateTime, default=utcnow)


class SnapshotPartition(Base):
    """Per-shard tracking row for a partitioned Snapshot.

    A workload whose work-set exceeds the partition threshold for its
    type fans out into N shards; each shard is a row here, claimable
    via the same atomic UPDATE-RETURNING by any backup_worker replica
    in any cluster/region. The last shard to terminate flips the
    parent Snapshot via `_finalize_partitioned_snapshot`.

    `partition_type` discriminates the work-set shape:
      ONEDRIVE_FILES    — payload: ignored; uses legacy `file_ids` column
                          (one shard owns a list of OneDrive file_ids)
      CHATS             — payload: {"chat_ids": [...]}
      MAIL_FOLDERS      — payload: {"folder_ids": [...]}
      SHAREPOINT_DRIVES — payload: {"drive_ids": [...]}

    The OneDrive path still uses `drive_id` + `file_ids` for backwards
    compat with already-shipped Phase-1 code. Other partition_types
    leave `drive_id` NULL and carry their work-set in `payload`.
    """
    __tablename__ = "snapshot_partitions"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    snapshot_id = Column(UUID(as_uuid=True),
                         ForeignKey("snapshots.id", ondelete="CASCADE"),
                         nullable=False, index=True)
    tenant_id = Column(UUID(as_uuid=True), nullable=False)
    resource_id = Column(UUID(as_uuid=True), nullable=False)
    job_id = Column(UUID(as_uuid=True), nullable=False)
    # Discriminator. Default keeps existing OneDrive rows valid.
    partition_type = Column(String, nullable=False, default="ONEDRIVE_FILES")
    # OneDrive-specific (legacy). Nullable now so MAIL/CHATS/SHAREPOINT
    # rows don't need a synthetic drive_id.
    drive_id = Column(Text, nullable=True)
    partition_index = Column(Integer, nullable=False)
    # Legacy OneDrive payload — list of file_ids for the shard.
    # New partition_types use `payload` instead.
    file_ids = Column(JSON, nullable=True)
    # Generic per-shard payload for non-ONEDRIVE_FILES partition types:
    #   CHATS            → {"chat_ids": [...]}
    #   MAIL_FOLDERS     → {"folder_ids": [...]}
    #   SHAREPOINT_DRIVES→ {"drive_ids": [...]}
    payload = Column(JSON, nullable=True)
    total_files = Column(Integer, nullable=False, default=0)
    total_bytes_est = Column(BigInteger, nullable=False, default=0)
    # QUEUED | IN_PROGRESS | COMPLETED | FAILED
    status = Column(String, nullable=False, default="QUEUED")
    worker_id = Column(String)
    # Worker region that owns this claim. Stamped from
    # os.getenv("WORKER_REGION", "default") on _claim_partition. Pure
    # observability for the multi-region case; today every claim
    # stamps "default".
    worker_region = Column(String)
    # Number of times stale-sweep has re-queued this row after a
    # failed/abandoned attempt. Once it crosses PARTITION_MAX_RETRIES
    # (default 5), the row goes to status='FAILED' and stops being
    # re-published — partition lifecycle audit emits PARTITION_FAILED
    # with reason='max_retries_exceeded'.
    retry_count = Column(Integer, nullable=False, default=0)
    enqueued_at = Column(DateTime, default=utcnow, nullable=False)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    files_uploaded = Column(Integer, nullable=False, default=0)
    bytes_uploaded = Column(BigInteger, nullable=False, default=0)
    failure_state = Column(JSON)
    # Reconciliation lease (2026-05-16 design). ``retry_count`` already
    # exists for the legacy partition stale-sweep; ``requeue_count``
    # added here mirrors the lease design's per-table circuit-breaker
    # name. We keep both columns: retry_count is bumped by the
    # partition stale-sweep, requeue_count by the reconciler sweep —
    # so a noisy partition can hit either path's cap.
    lease_owner_id = Column(UUID(as_uuid=True), nullable=True)
    lease_expires_at = Column(DateTime(timezone=True), nullable=True)
    lease_token = Column(BigInteger, nullable=False, default=0)
    requeue_count = Column(Integer, nullable=False, default=0)

    __table_args__ = (
        # One row per (snapshot, shard-index). Re-publish of the same
        # partition message after worker crash hits this constraint and
        # the publishing INSERT does ON CONFLICT DO NOTHING — making
        # fan-out idempotent under coordinator redelivery.
        Index(
            "uq_snap_partition",
            "snapshot_id", "partition_index",
            unique=True,
        ),
        # Hot read path for the finalizer + stale-sweep:
        #   SELECT WHERE snapshot_id=:sid AND status=...
        Index("ix_snap_partition_status", "snapshot_id", "status"),
        # Stale-sweep scan: oldest enqueued non-terminal first.
        Index(
            "ix_snap_partition_claim", "enqueued_at",
            postgresql_where=sql_text(
                "status IN ('QUEUED','IN_PROGRESS')"
            ),
        ),
    )


class MailFolderDelta(Base):
    """Per-folder delta token for mailbox-style resources.

    Replaces the JSON dict that used to live in
    `resource.extra_data["mail_delta_tokens_by_folder"]`. That dict had
    a Read-Modify-Write race when two folder drains finished
    concurrently — within one worker (asyncio interleave) OR across
    replicas after partitioning — and silently clobbered each other's
    tokens. Promoting each (resource, folder) → its own row gives
    PostgreSQL row-level atomicity, so concurrent UPSERTs commute.

    Covers all mailbox flavours (`USER_MAIL` / `MAILBOX` /
    `SHARED_MAILBOX` / `ROOM_MAILBOX`) — the worker's mail handler
    is shared, and folder ids are unique within a resource regardless
    of resource type.
    """
    __tablename__ = "mail_folder_delta"
    resource_id = Column(
        UUID(as_uuid=True),
        ForeignKey("resources.id", ondelete="CASCADE"),
        primary_key=True,
    )
    folder_id = Column(Text, primary_key=True)
    delta_token = Column(Text, nullable=False)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow, nullable=False)


class MailFolderFingerprint(Base):
    """Per-folder Graph fingerprint for USER_MAIL skip-by-fp.

    Mirrors `MailFolderDelta`. Replaces the whole-mailbox JSON dict
    that used to live in `resource.extra_data["mail_folder_fingerprints"]`
    + `mail_folder_baseline_at`. The dict was clobbered when sibling
    MAIL_FOLDERS partition shards finished sequentially: the
    second-finishing shard re-read the first shard's fingerprint
    writes for folders it did not own, matched them against an
    unchanged Graph view, and skipped its entire allowlist —
    silently dropping every non-Inbox folder from the snapshot.

    One row per (resource, folder) makes writes commute under PG
    row-level locks. `baseline_at` becomes per folder so the
    3-day full-rescan window applies to the folder that actually
    drained instead of resetting mailbox-wide on any drain.
    """
    __tablename__ = "mail_folder_fingerprint"
    resource_id = Column(
        UUID(as_uuid=True),
        ForeignKey("resources.id", ondelete="CASCADE"),
        primary_key=True,
    )
    folder_id = Column(Text, primary_key=True)
    total_item_count = Column(Integer, nullable=False, default=0)
    unread_item_count = Column(Integer, nullable=False, default=0)
    size_in_bytes = Column(BigInteger, nullable=False, default=0)
    baseline_at = Column(
        DateTime, default=utcnow, onupdate=utcnow, nullable=False,
    )


class BatchPendingUser(Base):
    """Per-user state in a backup batch when the user's backup is deferred
    until their Tier-2 discovery completes.

    A user is `deferred` when batch creation finds no Tier-2 children
    for them. Discovery is published with `thenBackup=True` and the
    same batch_id; this row tracks the state machine until discovery
    publishes a terminal state.

    States:
        WAITING_DISCOVERY  — published; discovery has not returned
        BACKUP_ENQUEUED    — discovery returned ≥1 child; backup posted
        NO_CONTENT         — discovery returned 0 children (terminal)
        DISCOVERY_FAILED   — discovery raised or watchdog deadline hit (terminal)

    The finalizer accepts any terminal state as a gate-1 pass for that
    user. See docs/superpowers/specs/2026-05-15-backup-batch-race-fix-design.md.
    """
    __tablename__ = "batch_pending_users"
    batch_id = Column(
        UUID(as_uuid=True),
        ForeignKey("backup_batches.id", ondelete="CASCADE"),
        primary_key=True,
    )
    user_id = Column(
        UUID(as_uuid=True),
        ForeignKey("resources.id", ondelete="CASCADE"),
        primary_key=True,
    )
    state = Column(Text, nullable=False)
    deadline_at = Column(DateTime, nullable=False)
    updated_at = Column(
        DateTime, default=utcnow, onupdate=utcnow, nullable=False,
    )


class SharePointDriveDelta(Base):
    """Per-drive delta token for SharePoint sites.

    Mirrors `MailFolderDelta` for SharePoint's
    `extra_data["drive_delta_tokens_by_site"]` dict. A SharePoint
    site can have many drives (documents libraries); each drive has
    its own `@odata.deltaLink`. The old JSON-dict RMW pattern broke
    the same way; this table fixes it the same way.
    """
    __tablename__ = "sharepoint_drive_delta"
    resource_id = Column(
        UUID(as_uuid=True),
        ForeignKey("resources.id", ondelete="CASCADE"),
        primary_key=True,
    )
    drive_id = Column(Text, primary_key=True)
    delta_token = Column(Text, nullable=False)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow, nullable=False)


class BulkFanoutSeen(Base):
    """Per-resource dedup marker for bulk-fanout publish.

    `_fanout_bulk_to_per_resource` publishes one per-resource backup
    message per resource in a bulk Job. Under RMQ redelivery (visibility
    timeout, NACK on PG saturation, worker restart mid-fanout) the bulk
    coordinator re-runs and would re-publish the same per-resource
    messages a second time — producing 2-3× duplicate USER_CHATS /
    USER_MAIL messages per user, which each spin up their own
    SnapshotPartition fanout, which the operator sees as a backup-loop.

    Fix: insert one row per (job_id, resource_id) BEFORE publishing.
    The PK + ON CONFLICT DO NOTHING is the atomic dedup point — on
    redelivery, the second pass's INSERTs return 0 rows and the
    coordinator skips the publish entirely. Idempotent + crash-safe
    (the row commits in the same txn as the publish-intent batch).

    Cleanup: stale-sweep prunes rows older than 24h. Even with no
    cleanup the table is small — at 5k users × 8 resource-types =
    40k rows per bulk run, growth is bounded by bulk-trigger frequency.
    """
    __tablename__ = "bulk_fanout_seen"
    job_id = Column(
        UUID(as_uuid=True),
        ForeignKey("jobs.id", ondelete="CASCADE"),
        primary_key=True,
    )
    resource_id = Column(
        UUID(as_uuid=True),
        ForeignKey("resources.id", ondelete="CASCADE"),
        primary_key=True,
    )
    created_at = Column(DateTime, default=utcnow, nullable=False)


class JobLog(Base):
    __tablename__ = "job_logs"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id = Column(UUID(as_uuid=True), ForeignKey("jobs.id"), nullable=False, index=True)
    timestamp = Column(DateTime, default=utcnow)
    level = Column(String, default="INFO")
    message = Column(Text, nullable=False)
    details = Column(Text)


class Alert(Base):
    __tablename__ = "alerts"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id = Column(UUID(as_uuid=True), ForeignKey("tenants.id"), index=True)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"))
    type = Column(String, nullable=False)
    severity = Column(String, nullable=False, default="MEDIUM")
    message = Column(Text, nullable=False)
    resource_id = Column(UUID(as_uuid=True))
    resource_type = Column(String)
    resource_name = Column(String)
    triggered_by = Column(String)
    resolved = Column(Boolean, default=False)
    resolved_at = Column(DateTime)
    resolved_by = Column(UUID(as_uuid=True))
    resolution_note = Column(Text)
    details = Column(JSON, default=dict)
    created_at = Column(DateTime, default=utcnow)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)


class AccessGroup(Base):
    __tablename__ = "access_groups"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"))
    tenant_id = Column(UUID(as_uuid=True), ForeignKey("tenants.id"))
    name = Column(String, nullable=False)
    description = Column(String)
    scope = Column(String, default="TENANT")
    resource_ids = Column(ARRAY(UUID(as_uuid=True)), default=list)
    permissions = Column(JSON, default=dict)
    member_ids = Column(ARRAY(UUID(as_uuid=True)), default=list)
    active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=utcnow)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)


class AuditEvent(Base):
    __tablename__ = "audit_events"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), index=True)
    tenant_id = Column(UUID(as_uuid=True), ForeignKey("tenants.id"), index=True)

    # Actor who triggered the action
    actor_id = Column(UUID(as_uuid=True))
    actor_email = Column(String)
    actor_type = Column(String, default="SYSTEM")  # USER | SYSTEM | WORKER

    # Action details
    action = Column(String, nullable=False, index=True)  # BACKUP_COMPLETED, etc.
    resource_id = Column(UUID(as_uuid=True))
    resource_type = Column(String)  # MAILBOX, ONEDRIVE, etc.
    resource_name = Column(String)
    outcome = Column(String, default="SUCCESS")  # SUCCESS | FAILURE | PARTIAL

    # Job/snapshot references
    job_id = Column(UUID(as_uuid=True))
    snapshot_id = Column(UUID(as_uuid=True))

    # Extended details (JSONB for flexibility)
    details = Column(JSON, default=dict)

    occurred_at = Column(DateTime, default=utcnow, index=True, nullable=False)


class AdminConsentToken(Base):
    __tablename__ = "admin_consent_tokens"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), index=True)
    tenant_id = Column(UUID(as_uuid=True), ForeignKey("tenants.id"), index=True)
    
    # Consent type: M365 or AZURE
    consent_type = Column(String, nullable=False, index=True)
    
    # Encrypted tokens
    access_token_encrypted = Column(LargeBinary, nullable=True)
    refresh_token_encrypted = Column(LargeBinary, nullable=True)
    token_type = Column(String, default="Bearer")
    expires_at = Column(DateTime, nullable=True)
    
    # Metadata
    granted_by = Column(String, nullable=True)  # Email of user who granted consent
    consented_at = Column(DateTime, default=utcnow)
    last_used_at = Column(DateTime, nullable=True)
    is_active = Column(Boolean, default=True, index=True)
    scope = Column(String, nullable=True)  # Space-separated list of scopes
    
    created_at = Column(DateTime, default=utcnow)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)


class DiscoveryRun(Base):
    __tablename__ = "discovery_runs"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id = Column(UUID(as_uuid=True), ForeignKey("tenants.id"), nullable=False, index=True)
    scope = Column(JSON, default=list, nullable=False)
    status = Column(String, default="RUNNING", nullable=False, index=True)
    fetched_count = Column(Integer, default=0, nullable=False)
    staged_count = Column(Integer, default=0, nullable=False)
    inserted_count = Column(Integer, default=0, nullable=False)
    updated_count = Column(Integer, default=0, nullable=False)
    unchanged_count = Column(Integer, default=0, nullable=False)
    stale_marked_count = Column(Integer, default=0, nullable=False)
    error_message = Column(Text, nullable=True)
    started_at = Column(DateTime, default=utcnow, nullable=False)
    finished_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=utcnow)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)


class ResourceDiscoveryStaging(Base):
    __tablename__ = "resource_discovery_staging"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    run_id = Column(UUID(as_uuid=True), ForeignKey("discovery_runs.id"), nullable=False, index=True)
    tenant_id = Column(UUID(as_uuid=True), ForeignKey("tenants.id"), nullable=False, index=True)
    resource_type = Column(String, nullable=False)
    external_id = Column(String, nullable=False)
    display_name = Column(String, nullable=False)
    email = Column(String, nullable=True)
    extra_data = Column("metadata", MutableDict.as_mutable(JSON), default=dict)
    resource_status = Column(String, default="DISCOVERED", nullable=False)
    resource_hash = Column(String, nullable=True)
    azure_subscription_id = Column(String, nullable=True)
    azure_resource_group = Column(String, nullable=True)
    azure_region = Column(String, nullable=True)
    discovered_at = Column(DateTime, default=utcnow, nullable=False)
    created_at = Column(DateTime, default=utcnow)


class ReportConfig(Base):
    __tablename__ = "report_configs"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), index=True, nullable=True)

    # Schedule settings
    enabled = Column(Boolean, default=False, nullable=False)
    schedule_type = Column(String, default="daily", nullable=False)  # daily, weekly, monthly

    # Empty report handling
    send_empty_report = Column(Boolean, default=True, nullable=False)
    empty_message = Column(String, default="No updates. No backups occurred.", nullable=True)
    send_detailed_report = Column(Boolean, default=False, nullable=False)

    # Notification endpoints (stored as JSON arrays)
    email_recipients = Column(JSON, default=list, nullable=True)
    slack_webhooks = Column(JSON, default=list, nullable=True)
    teams_webhooks = Column(JSON, default=list, nullable=True)

    created_at = Column(DateTime, default=utcnow)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)


class ReportHistory(Base):
    __tablename__ = "report_history"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), index=True)
    report_config_id = Column(UUID(as_uuid=True), ForeignKey("report_configs.id"), nullable=True)

    # Report metadata
    report_type = Column(String, nullable=False)  # DAILY, WEEKLY, MONTHLY
    period_start = Column(DateTime, nullable=True)
    period_end = Column(DateTime, nullable=True)
    generated_at = Column(DateTime, default=utcnow, nullable=False)

    # Report content summary
    total_backups = Column(Integer, default=0)
    successful_backups = Column(Integer, default=0)
    failed_backups = Column(Integer, default=0)
    success_rate = Column(String, nullable=True)
    coverage_rate = Column(String, nullable=True)

    # Full report data (JSON)
    report_data = Column(JSON, default=dict, nullable=True)
    is_empty = Column(Boolean, default=False, nullable=False)

    # Delivery status
    delivery_status = Column(JSON, default=dict, nullable=True)

    # Error tracking
    error_message = Column(Text, nullable=True)

    created_at = Column(DateTime, default=utcnow)


class TenantSecret(Base):
    """Credentials + KMS-key references the user stores once and reuses
    across restore + other operations. Known `type` values:
      • SQL_SERVER_LOGIN / POSTGRESQL_LOGIN — login + password used
        during Azure DB out-of-place restore.
      • AES_256_KEY — external-KMS key material (AWS/GCP/Azure KV).

    Passwords / key material live in `encrypted_payload` (opaque base64
    of shared.security.encrypt_secret), never returned to the frontend.
    `metadata_hints` carries non-sensitive fields safe to render in
    lists (login username, KMS provider name, etc)."""
    __tablename__ = "tenant_secrets"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id = Column(UUID(as_uuid=True), ForeignKey("tenants.id", ondelete="CASCADE"), index=True, nullable=False)
    type = Column(String, nullable=False, index=True)
    name = Column(String, nullable=False)
    description = Column(Text, nullable=True)
    metadata_hints = Column(JSON, default=dict, nullable=True)
    encrypted_payload = Column(Text, nullable=True)
    is_default = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime, default=utcnow, nullable=False)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow, nullable=False)


class VmFileIndex(Base):
    """Per-file / per-directory index produced by walking the VHD snapshot
    captured during an Azure VM backup. Each row represents one file or
    folder inside a disk at snapshot time.

    The index lets the Volumes tab browse + download files from the
    *backup* (not the live VM) — so locked files, stopped VMs, and deleted
    files all remain recoverable. `fs_inode` + `fs_extents` carry enough
    TSK-level information to re-open the same VHD blob later and stream
    the file's bytes out without re-walking the whole filesystem.

    Storage shape trades a LOT of rows (100k+ per VM is normal) for very
    cheap lookups: `(snapshot_id, volume_item_id, parent_path)` is the
    hot query path driving the directory listing UI."""
    __tablename__ = "vm_file_index"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    snapshot_id = Column(UUID(as_uuid=True), ForeignKey("snapshots.id", ondelete="CASCADE"), nullable=False, index=True)
    volume_item_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    parent_path = Column(String, nullable=False, index=True)
    name = Column(String, nullable=False)
    is_directory = Column(Boolean, default=False, nullable=False)
    size_bytes = Column(BigInteger, default=0, nullable=False)
    modified_at = Column(DateTime, nullable=True)
    fs_inode = Column(BigInteger, nullable=True)
    fs_type = Column(String, nullable=True)
    partition_offset = Column(BigInteger, nullable=True)
    blob_path = Column(String, nullable=True)
    extents_json = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=utcnow, nullable=False)


# ==================== Storage backend abstraction (2026-04-21) ====================


class StorageBackendKind(str, enum.Enum):
    azure_blob = "azure_blob"
    seaweedfs = "seaweedfs"


class TransitionState(str, enum.Enum):
    stable = "stable"
    draining = "draining"
    flipping = "flipping"


class ToggleStatus(str, enum.Enum):
    started = "started"
    drain_started = "drain_started"
    drain_completed = "drain_completed"
    db_promoted = "db_promoted"
    dns_flipped = "dns_flipped"
    workers_restarted = "workers_restarted"
    smoke_passed = "smoke_passed"
    completed = "completed"
    aborted = "aborted"
    failed = "failed"


# Import extras used only by these new classes; placed here instead of at the
# top so unrelated code doesn't pay the import tax until storage is imported.
from sqlalchemy import SmallInteger
from sqlalchemy.dialects.postgresql import INET, JSONB


class StorageBackend(Base):
    __tablename__ = "storage_backends"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    kind = Column(String, nullable=False)
    name = Column(String, unique=True, nullable=False)
    endpoint = Column(String, nullable=False)
    config = Column(JSONB, nullable=False, default=dict)
    secret_ref = Column(String, nullable=False)
    is_enabled = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime(timezone=True), default=utcnow)
    updated_at = Column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)


class SystemConfig(Base):
    __tablename__ = "system_config"
    id = Column(SmallInteger, primary_key=True)
    active_backend_id = Column(UUID(as_uuid=True), ForeignKey("storage_backends.id"), nullable=False)
    transition_state = Column(String, nullable=False, default="stable")
    last_toggle_at = Column(DateTime(timezone=True))
    cooldown_until = Column(DateTime(timezone=True))
    updated_at = Column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)


class StorageToggleEvent(Base):
    __tablename__ = "storage_toggle_events"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    actor_id = Column(UUID(as_uuid=True), nullable=False)
    actor_ip = Column(INET)
    from_backend_id = Column(UUID(as_uuid=True), ForeignKey("storage_backends.id"), nullable=False)
    to_backend_id = Column(UUID(as_uuid=True), ForeignKey("storage_backends.id"), nullable=False)
    reason = Column(String)
    status = Column(String, nullable=False, default="started")
    started_at = Column(DateTime(timezone=True), default=utcnow)
    drain_completed_at = Column(DateTime(timezone=True))
    flip_completed_at = Column(DateTime(timezone=True))
    completed_at = Column(DateTime(timezone=True))
    error_message = Column(Text)
    pre_flight_checks = Column(JSONB)
    drained_job_count = Column(Integer)
    retried_job_count = Column(Integer)


# ==================== Cross-user chat dedup (2026-05-13) ====================
# Two cooperating stores added together:
#   chat_url_cache       - tenant-scoped persisted SharePoint URL → driveItem
#                          resolution cache. Lets the N+1th backup skip the
#                          Graph /shares resolve AND the CDN download for any
#                          URL an earlier backup already drained.
#   chat_threads         - tenant-scoped singleton row per (tenant, chat_id).
#                          The cross-user drain claim lives here: when User B's
#                          backup lands within CHAT_THREAD_DRAIN_FRESHNESS_S of
#                          User A's, B short-circuits and reuses A's drained
#                          messages.
#   chat_thread_messages - the actual message bodies, written once per
#                          (chat_thread_id, message_external_id). snapshot_items
#                          carries thin pointer rows joined at read time.


class ChatUrlCache(Base):
    """Tenant-scoped cache of chat-attachment URL → driveItem resolution.

    Keyed by SHA-256 of the SharePoint share URL (cheaper PK than indexing TEXT).
    A hit short-circuits both the Graph /shares/{id}/driveItem resolve and the
    SharePoint CDN download — caller reuses the existing `blob_path` (or
    `inline_b64` for tiny payloads).

    `unreachable=True` is set when the resolve / download returns a permanent
    4xx, so subsequent backups don't keep re-trying broken URLs. Complements
    GraphClient._unreachable_urls (which is worker-process-lifetime only).
    """
    __tablename__ = "chat_url_cache"
    tenant_id = Column(UUID(as_uuid=True), ForeignKey("tenants.id", ondelete="CASCADE"), primary_key=True)
    url_sha256 = Column(String(64), primary_key=True)
    drive_item_id = Column(String(256), nullable=True)
    content_hash = Column(String(64), nullable=True)
    blob_path = Column(Text, nullable=True)
    content_size = Column(BigInteger, nullable=True)
    inline_b64 = Column(Text, nullable=True)
    unreachable = Column(Boolean, nullable=False, default=False)
    first_seen_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    last_used_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)


class ChatThread(Base):
    """Singleton row per (tenant_id, chat_id). Stores the cross-user drain
    claim, the chat's metadata, and the per-chat drain cursor / failure state
    (relocated from Resource.extra_data so it's tenant-scoped, not user-scoped).

    Drain claim mechanic: each backup attempts an INSERT…ON CONFLICT DO UPDATE
    that bumps `last_drained_at` only if the existing row is older than the
    freshness window. RETURNING (xmax = 0) tells the worker whether it won the
    claim (drain) or lost (skip + reuse messages already in chat_thread_messages).
    """
    __tablename__ = "chat_threads"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    # P2: RESTRICT (not CASCADE) so an accidental tenant DELETE fails
    # loud instead of silently wiping every chat singleton.
    tenant_id = Column(UUID(as_uuid=True), ForeignKey("tenants.id", ondelete="RESTRICT"), nullable=False, index=True)
    chat_id = Column(String(256), nullable=False)
    chat_type = Column(String(32), nullable=True)        # oneOnOne / group / meeting_*
    chat_topic = Column(Text, nullable=True)             # group name; null for 1:1
    member_names_json = Column(JSONB, nullable=True)     # snapshot of members at last drain
    last_updated_at = Column(DateTime(timezone=True), nullable=True)   # mirrors chat.lastUpdatedDateTime
    last_drained_at = Column(DateTime(timezone=True), nullable=True)   # when we last hit Graph
    drain_cursor = Column(Text, nullable=True)
    drain_failure_state = Column(JSONB, nullable=True)
    # Count of messages persisted in chat_thread_messages after the most
    # recent successful drain (across all users). Used by the post-drain
    # completeness gate: if the next drain ends with significantly fewer
    # messages than this baseline (and no purge/retention reason exists),
    # we treat the drain as partial and DO NOT advance the cursor — see
    # _CHAT_DRAIN_COMPLETENESS_DROP_PCT in backup-worker/main.py.
    last_drained_msg_count = Column(Integer, nullable=True)
    archived_at = Column(DateTime(timezone=True), nullable=True)  # P2 soft delete
    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=utcnow, onupdate=utcnow)


class MailMessageBody(Base):
    """Cross-user mail message body store (2026-05-17). Mirrors the
    chat_thread_messages model: deduplicate the bytes of an email body
    across all snapshots/users that reference it within one tenant.

    Why this exists: in a typical enterprise mailbox the same email
    thread is replicated across every recipient's mailbox AND every
    sender's Sent folder. Today we serialize each copy's body into
    snapshot_items.extra_data → 3-5× write amplification for big
    distribution lists. This table caches the body once per
    (tenant_id, fingerprint) and lets future reads JOIN here instead.

    Dedup key: `fingerprint` = sha256(from + sentDateTime + subject +
    body_size + body_first_64KB_hash). Not a hash collision risk in
    practice — sentDateTime is microsecond-precise and the prefix
    hash discriminates beyond what mailbox boundaries do.

    Migration plan:
      Phase 1 (this commit): write-only. Bodies live in BOTH
        snapshot_items.extra_data AND mail_message_bodies. No restore
        path changes — zero risk to existing reads.
      Phase 2 (future): switch restore to JOIN this table; stop
        writing body to snapshot_items.extra_data; reclaim disk.

    Unique constraint: (tenant_id, fingerprint). ON CONFLICT DO
    NOTHING makes the upsert idempotent under concurrent writers
    from different users' drains.
    """
    __tablename__ = "mail_message_bodies"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id = Column(UUID(as_uuid=True), ForeignKey("tenants.id", ondelete="RESTRICT"), nullable=False, index=True)
    fingerprint = Column(String(64), nullable=False)
    # Provenance — the first user/snapshot to land this body. Useful for
    # audit and for the "who got it first" tie-break in claim helpers.
    first_user_id = Column(String(128), nullable=True)
    first_snapshot_id = Column(UUID(as_uuid=True), nullable=True)
    # Mail-specific fields lifted out of body so reads don't need JSON
    # extraction. Identical role to ChatThreadMessage's from_*.
    from_user_id = Column(String(128), nullable=True)
    from_address = Column(String(256), nullable=True)
    from_display_name = Column(String(256), nullable=True)
    subject = Column(Text, nullable=True)
    sent_date_time = Column(DateTime(timezone=True), nullable=True)
    received_date_time = Column(DateTime(timezone=True), nullable=True)
    body_content = Column(Text, nullable=True)
    body_content_type = Column(String(16), nullable=True)
    has_attachments = Column(Boolean, nullable=True)
    # Full Graph payload — same shape backup-worker writes to
    # snapshot_items.extra_data['raw'] today. Phase-2 reads JOIN here.
    metadata_raw = Column(JSONB, nullable=True)
    content_hash = Column(String(64), nullable=True)
    content_size = Column(BigInteger, nullable=True)
    # How many distinct snapshot_items rows currently point at this
    # body. Bumped on every dedup hit. The post-retention purge walks
    # bodies with ref_count=0 + last_referenced_at older than the cap.
    ref_count = Column(Integer, nullable=False, default=1)
    last_referenced_at = Column(DateTime(timezone=True), nullable=False, default=utcnow, onupdate=utcnow)
    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)


class ChatThreadMessage(Base):
    """Tenant-scoped, drained-once-per-batch chat message store.

    snapshot_items carries a thin pointer row per (snapshot, message) keyed by
    parent_external_id=chat_id + external_id=message_external_id; reads JOIN
    here to hydrate body + sender + attachments. metadata_raw holds the full
    Graph payload (attachments, mentions, reactions, hostedContents, etc.) so
    later read paths don't need to widen the column set.
    """
    __tablename__ = "chat_thread_messages"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    # P2: RESTRICT FK — see ChatThread for rationale.
    chat_thread_id = Column(UUID(as_uuid=True), ForeignKey("chat_threads.id", ondelete="RESTRICT"), nullable=False, index=True)
    message_external_id = Column(String(256), nullable=False)
    created_date_time = Column(DateTime(timezone=True), nullable=True)
    last_modified_date_time = Column(DateTime(timezone=True), nullable=True)
    from_user_id = Column(String(128), nullable=True)
    from_display_name = Column(String(256), nullable=True)
    body_content = Column(Text, nullable=True)
    body_content_type = Column(String(16), nullable=True)
    deleted_date_time = Column(DateTime(timezone=True), nullable=True)
    metadata_raw = Column(JSONB, nullable=True)
    content_hash = Column(String(64), nullable=True)
    content_size = Column(BigInteger, nullable=True)
    archived_at = Column(DateTime(timezone=True), nullable=True)  # P2 soft delete
    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)


# OneDrive per-file retry queue (2026-05-17).
# A file that exhausts its inline resume budget no longer blocks
# snapshot completion. The main gather records a row here; a separate
# consumer drains it at its own pace. On success the rescued bytes
# are upserted into snapshot_items pointing at the ORIGINAL snapshot
# (UPSERT on (snapshot_id, external_id, item_type) is idempotent —
# the snapshot's rollup query re-derives counters from the table).
class OneDriveFileRetry(Base):
    __tablename__ = "onedrive_file_retries"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id = Column(UUID(as_uuid=True), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False, index=True)
    resource_id = Column(UUID(as_uuid=True), ForeignKey("resources.id", ondelete="CASCADE"), nullable=False, index=True)
    snapshot_id = Column(UUID(as_uuid=True), ForeignKey("snapshots.id", ondelete="CASCADE"), nullable=False, index=True)
    file_external_id = Column(String(256), nullable=False)
    file_name = Column(Text, nullable=True)
    drive_id = Column(Text, nullable=True)
    # The full Graph file dict — same shape backup_single_file expects.
    file_payload = Column(JSONB, nullable=False)
    attempt_count = Column(Integer, nullable=False, default=0)
    last_error = Column(Text, nullable=True)
    # "throttle" | "stream_drop" | "permanent" | "unknown"
    last_error_class = Column(String(32), nullable=True)
    next_retry_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    # PENDING | IN_PROGRESS | RESCUED | FAILED_PERMANENT
    status = Column(String(16), nullable=False, default="PENDING")
    rescued_snapshot_item_id = Column(UUID(as_uuid=True), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=utcnow, onupdate=utcnow)
    # UNIQUE(snapshot_id, file_external_id) enforced via the table
    # DDL in shared/database.py; mirrored here is not necessary.


def snapshot_is_restore_ready(snapshot: "Snapshot") -> Tuple[bool, str]:
    """Return (is_ready, reason). Restore paths should call this and
    refuse to proceed when is_ready is False.

    Currently checks Item-C's hc_drain_status:
      - NOT_APPLICABLE / COMPLETE → ready.
      - PENDING → background HC drain still in flight; reject with a
        retryable error so callers can poll-and-retry.
      - FAILED → at least one HC task raised; restore is unsafe because
        inline images may be missing. Caller should surface to the user.
    """
    status = getattr(snapshot, "hc_drain_status", "NOT_APPLICABLE") or "NOT_APPLICABLE"
    if status in ("NOT_APPLICABLE", "COMPLETE"):
        return True, ""
    if status == "PENDING":
        return False, "hc_drain_status=PENDING — hostedContent drain still in flight; retry later"
    if status == "FAILED":
        return False, "hc_drain_status=FAILED — at least one hostedContent task raised; restore unsafe"
    return False, f"unknown hc_drain_status={status!r}"
