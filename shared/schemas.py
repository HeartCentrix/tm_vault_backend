"""Shared Pydantic schemas for all microservices"""
from datetime import datetime
from typing import Optional, List, Dict, Any
from uuid import UUID
from pydantic import BaseModel, Field, field_validator, ConfigDict


# ============ Auth ============

class UserResponse(BaseModel):
    id: str
    email: str
    name: str
    roles: List[str]
    organizationId: str
    tenantId: Optional[str] = None


class LoginResponse(BaseModel):
    accessToken: str
    refreshToken: str
    expiresIn: int
    user: UserResponse


class RefreshTokenRequest(BaseModel):
    # Optional: the SPA reads the refresh token from an HttpOnly cookie and
    # sends an empty {} body. Non-browser callers (CLI, integration tests)
    # may still pass the token explicitly.
    refreshToken: Optional[str] = None


class RefreshTokenResponse(BaseModel):
    accessToken: str
    refreshToken: str
    expiresIn: int


class MicrosoftAuthUrlResponse(BaseModel):
    url: str
    state: str


class OAuthCallbackRequest(BaseModel):
    code: str
    state: Optional[str] = None


class DatasourceConsentRequest(BaseModel):
    """Admin consent callback request for M365 datasource onboarding."""
    external_tenant_id: str  # from ?tenant=... query param
    admin_consent: bool
    state: str               # CSRF token issued during initiation


class DatasourceCallbackResponse(BaseModel):
    tenantId: str
    tenantName: str
    discoveryStatus: str


# ============ Dashboard ============

class DashboardOverview(BaseModel):
    totalResources: int
    protectedResources: int
    failedBackups: int
    pendingBackups: int
    storageUsed: str
    lastBackupTime: Optional[str] = None


class BackupStatus24Hour(BaseModel):
    success: int
    warnings: int
    failures: int


class DailyStatus(BaseModel):
    date: str
    success: int
    warnings: int
    failures: int


class ProtectionStatusItem(BaseModel):
    protectedCount: int
    total: int


class ProtectionStatus(BaseModel):
    users: ProtectionStatusItem
    sharedMailboxes: ProtectionStatusItem
    rooms: ProtectionStatusItem
    sharepointSites: ProtectionStatusItem
    groupsAndTeams: ProtectionStatusItem
    entraId: ProtectionStatusItem
    powerPlatform: ProtectionStatusItem
    percentage: float


class BackupSizeDailyData(BaseModel):
    date: str
    bytes: int


class BackupSize(BaseModel):
    total: str
    oneDayChange: str
    oneMonthChange: str
    oneYearChange: str
    dailyData: List[BackupSizeDailyData]


# ============ Tenant ============

class TenantResponse(BaseModel):
    id: str
    displayName: str
    orgId: Optional[str] = None
    type: Optional[str] = None
    externalTenantId: Optional[str] = None
    customerId: Optional[str] = None
    status: str
    storageRegion: Optional[str] = None
    lastDiscoveryAt: Optional[str] = None
    createdAt: Optional[str] = None


class TenantCreateRequest(BaseModel):
    name: str
    organizationId: str
    microsoftTenantId: str
    connectionDetails: Optional[dict] = None


class TenantInfoResponse(BaseModel):
    """Response for tenant info page (Customer ID, Tenant ID, Region)"""
    customerId: str
    tenantId: str
    region: str


class UsageReportEntry(BaseModel):
    """Single entry in the usage report"""
    resourceId: str
    resourceName: str
    resourceKind: str
    sla: str
    isActive: str
    backupSizeGB: float
    dailySizes: dict  # date -> size in GB


class DiscoveryStatus(BaseModel):
    tenantId: str
    status: str
    progress: int
    resourcesDiscovered: int
    startedAt: str
    completedAt: Optional[str] = None
    errorMessage: Optional[str] = None


class StorageSummaryItem(BaseModel):
    workload: str
    size: int
    resourceCount: int


class OrganizationResponse(BaseModel):
    id: str
    name: str
    status: str
    tenantCount: int
    createdAt: str


# ============ Resource ============

class ResourceResponse(BaseModel):
    id: str
    name: str
    email: Optional[str] = None
    type: str
    sla: Optional[str] = None
    totalSize: str
    lastBackup: Optional[str] = None
    status: Optional[str] = None
    tenantId: Optional[str] = None
    archived: bool = False


class ResourceListResponse(BaseModel):
    content: List[ResourceResponse]
    totalPages: int
    totalElements: int
    size: int
    number: int
    first: bool
    last: bool


class UserResourceResponse(BaseModel):
    id: str
    tenantId: str
    email: str
    displayName: str
    hasMailbox: Optional[bool] = None
    mailboxStatus: Optional[str] = None
    hasOneDrive: Optional[bool] = None
    oneDriveStatus: Optional[str] = None
    hasTeamsChat: Optional[bool] = None
    teamsChatStatus: Optional[str] = None
    sla: Optional[str] = None


class AssignPolicyRequest(BaseModel):
    policyId: str


class BulkOperationRequest(BaseModel):
    resourceIds: List[str]


class BulkAssignRequest(BaseModel):
    """Request to assign an SLA policy to multiple resources"""
    resourceIds: List[str]
    policyId: str


class BulkUnassignRequest(BaseModel):
    """Request to remove SLA policy from multiple resources"""
    resourceIds: List[str]


# ============ Job ============

class JobResponse(BaseModel):
    id: str
    type: str
    status: str
    progress: int
    resourceId: Optional[str] = None
    tenantId: Optional[str] = None
    createdAt: str
    updatedAt: str
    completedAt: Optional[str] = None
    errorMessage: Optional[str] = None
    # Derived live rollup from the snapshots table. The Job row's own
    # progress_pct / items_processed columns are write-once at terminal
    # state and not authoritative — these fields are computed on read so
    # they never disagree with reality.
    itemsProcessed: Optional[int] = None
    bytesProcessed: Optional[int] = None
    snapshotsTotal: Optional[int] = None
    snapshotsCompleted: Optional[int] = None
    snapshotsFailed: Optional[int] = None
    snapshotsInProgress: Optional[int] = None


class JobListResponse(BaseModel):
    content: List[JobResponse]
    totalPages: int
    totalElements: int
    size: int
    number: int
    first: bool
    last: bool


class TriggerBackupRequest(BaseModel):
    resourceId: str
    fullBackup: Optional[bool] = True
    priority: Optional[int] = 1
    note: Optional[str] = None


class TriggerBulkBackupRequest(BaseModel):
    resourceIds: List[str]
    fullBackup: Optional[bool] = True
    priority: Optional[int] = 1
    note: Optional[str] = None
    # Optional. When set, every Job created by this call stamps the same
    # batch_id into spec.batch_id, so audit-service can group multi-stage
    # batches (e.g. parent bulk + Tier-2 child fan-out) under one row.
    batchId: Optional[str] = None
    # Set by discovery-worker when fanning out Tier-2 discovered children
    # (USER_ONEDRIVE / USER_MAILBOX / USER_CHATS / …) under an existing
    # batch. Those resources are sub-resources of users the operator
    # already counted in the initial click, so the Activity-row total
    # excludes their resource_count to avoid double-counting (e.g. an
    # 18-user click should keep showing "18 resources", not 72 once the
    # fan-out lands).
    tier2: Optional[bool] = False


class TriggerDatasourceBackupRequest(BaseModel):
    tenantId: str
    serviceType: str
    fullBackup: Optional[bool] = True
    priority: Optional[int] = 1
    note: Optional[str] = None


# ============ Snapshot ============

class SnapshotResponse(BaseModel):
    id: str
    resourceId: str
    createdAt: str
    size: int
    status: str
    type: str
    itemCount: int
    jobId: Optional[str] = None
    # `batch_id` from the Job's spec — shared by Tier-1 + Tier-2-urgent
    # + Tier-2-heavy Jobs that fan out from a single "Backup now" click.
    # Recovery UI uses this to collapse cross-resource children of one
    # bulk run into a single dropdown entry (the dropdown bucketing by
    # jobId alone over-counted because the 3 Jobs have 3 distinct ids).
    # `null` for legacy snapshots whose Job has no batch_id in spec.
    batchId: Optional[str] = None
    # Cross-replica partition rollup. Populated only for snapshots
    # that were partitioned (OneDrive/Chats/Mail/SharePoint shards).
    # `null` for non-partitioned snapshots — backward compatible.
    # Backend-only contract: ops/audit consumers read it; the UI
    # treats it as opaque.
    partitions: Optional[dict] = None


class SnapshotItemResponse(BaseModel):
    id: str
    snapshotId: str
    externalId: str
    itemType: str
    name: str
    folderPath: Optional[str] = None
    contentSize: int
    metadata: dict
    isDeleted: bool
    createdAt: str
    # Blob storage path for items whose real bytes were uploaded (populated
    # for ONEDRIVE_FILE rows the backup worker successfully blobbed, for
    # EMAIL/CHAT_ATTACHMENT rows, etc.). Null for metadata-only rows.
    # Frontend uses `.blobPath` presence to decide whether to render a
    # download link on the item row.
    blobPath: Optional[str] = None


class SnapshotListResponse(BaseModel):
    content: List[SnapshotResponse]
    totalPages: int
    totalElements: int
    size: int
    number: int


class SnapshotItemListResponse(BaseModel):
    content: List[SnapshotItemResponse]
    totalPages: int
    totalElements: int
    size: int
    number: int


class SnapshotDiff(BaseModel):
    added: List[SnapshotItemResponse]
    removed: List[SnapshotItemResponse]
    modified: List[SnapshotItemResponse]


# ============ SLA Policy ============

class SlaPolicyResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True, from_attributes=True)
    
    id: str
    tenantId: str = Field(alias='tenant_id')
    serviceType: str = Field(default='m365', alias='service_type')
    name: str
    frequency: str
    backupDays: Optional[List[str]] = Field(default=None, alias='backup_days')
    backupWindowStart: Optional[str] = Field(default=None, alias='backup_window_start')
    backupExchange: Optional[bool] = Field(default=True, alias='backup_exchange')
    backupExchangeArchive: Optional[bool] = Field(default=False, alias='backup_exchange_archive')
    backupExchangeRecoverable: Optional[bool] = Field(default=False, alias='backup_exchange_recoverable')
    backupOneDrive: Optional[bool] = Field(default=True, alias='backup_onedrive')
    backupSharepoint: Optional[bool] = Field(default=True, alias='backup_sharepoint')
    backupTeams: Optional[bool] = Field(default=True, alias='backup_teams')
    backupTeamsChats: Optional[bool] = Field(default=True, alias='backup_teams_chats')
    backupEntraId: Optional[bool] = Field(default=True, alias='backup_entra_id')
    backupPowerPlatform: Optional[bool] = Field(default=False, alias='backup_power_platform')
    backupCopilot: Optional[bool] = Field(default=False, alias='backup_copilot')
    contacts: Optional[bool] = True
    calendars: Optional[bool] = True
    tasks: Optional[bool] = False
    groupMailbox: Optional[bool] = Field(default=True, alias='group_mailbox')
    planner: Optional[bool] = False
    backupAzureVm: Optional[bool] = Field(default=True, alias='backup_azure_vm')
    backupAzureSql: Optional[bool] = Field(default=True, alias='backup_azure_sql')
    backupAzurePostgresql: Optional[bool] = Field(default=True, alias='backup_azure_postgresql')
    retentionType: str = Field(alias='retention_type')
    retentionDays: Optional[int] = Field(default=None, alias='retention_days')
    # Phase 1 schema expansion (GFS, item-level, archived-rules, BYOK, auto-apply)
    retentionMode: str = Field(default='FLAT', alias='retention_mode')
    retentionHotDays: Optional[int] = Field(default=None, alias='retention_hot_days')
    retentionCoolDays: Optional[int] = Field(default=None, alias='retention_cool_days')
    retentionArchiveDays: Optional[int] = Field(default=None, alias='retention_archive_days')
    gfsDailyCount: Optional[int] = Field(default=None, alias='gfs_daily_count')
    gfsWeeklyCount: Optional[int] = Field(default=None, alias='gfs_weekly_count')
    gfsMonthlyCount: Optional[int] = Field(default=None, alias='gfs_monthly_count')
    gfsYearlyCount: Optional[int] = Field(default=None, alias='gfs_yearly_count')
    itemRetentionDays: Optional[int] = Field(default=None, alias='item_retention_days')
    itemRetentionBasis: str = Field(default='SNAPSHOT', alias='item_retention_basis')
    archivedRetentionMode: str = Field(default='SAME', alias='archived_retention_mode')
    archivedRetentionDays: Optional[int] = Field(default=None, alias='archived_retention_days')
    legalHoldEnabled: Optional[bool] = Field(default=False, alias='legal_hold_enabled')
    legalHoldUntil: Optional[str] = Field(default=None, alias='legal_hold_until')
    immutabilityMode: str = Field(default='None', alias='immutability_mode')
    encryptionMode: str = Field(default='VAULT_MANAGED', alias='encryption_mode')
    keyVaultUri: Optional[str] = Field(default=None, alias='key_vault_uri')
    keyName: Optional[str] = Field(default=None, alias='key_name')
    keyVersion: Optional[str] = Field(default=None, alias='key_version')
    keyVersionResolved: Optional[str] = Field(default=None, alias='key_version_resolved')
    encryptionStatus: Optional[str] = Field(default='', alias='encryption_status')
    autoApplyToMatching: Optional[bool] = Field(default=False, alias='auto_apply_to_matching')
    isDefault: Optional[bool] = Field(default=False, alias='is_default')
    enabled: Optional[bool] = True
    createdAt: str = Field(alias='created_at')

    @field_validator('id', 'tenantId', mode='before')
    @classmethod
    def uuid_to_str(cls, v):
        return str(v) if v else v

    @field_validator('createdAt', 'legalHoldUntil', mode='before')
    @classmethod
    def datetime_to_str(cls, v):
        return v.isoformat() if v else v


class SlaPolicyCreateRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    
    tenantId: str = Field(alias='tenant_id')
    serviceType: str = Field(default='m365', alias='service_type')
    name: str
    frequency: str
    backupDays: Optional[List[str]] = Field(default=None, alias='backup_days')
    backupWindowStart: Optional[str] = Field(default=None, alias='backup_window_start')
    backupExchange: Optional[bool] = Field(default=True, alias='backup_exchange')
    backupExchangeArchive: Optional[bool] = Field(default=False, alias='backup_exchange_archive')
    backupExchangeRecoverable: Optional[bool] = Field(default=False, alias='backup_exchange_recoverable')
    backupOneDrive: Optional[bool] = Field(default=True, alias='backup_onedrive')
    backupSharepoint: Optional[bool] = Field(default=True, alias='backup_sharepoint')
    backupTeams: Optional[bool] = Field(default=True, alias='backup_teams')
    backupTeamsChats: Optional[bool] = Field(default=True, alias='backup_teams_chats')
    backupEntraId: Optional[bool] = Field(default=True, alias='backup_entra_id')
    backupPowerPlatform: Optional[bool] = Field(default=False, alias='backup_power_platform')
    backupCopilot: Optional[bool] = Field(default=False, alias='backup_copilot')
    contacts: Optional[bool] = True
    calendars: Optional[bool] = True
    tasks: Optional[bool] = False
    groupMailbox: Optional[bool] = Field(default=True, alias='group_mailbox')
    planner: Optional[bool] = False
    backupAzureVm: Optional[bool] = Field(default=True, alias='backup_azure_vm')
    backupAzureSql: Optional[bool] = Field(default=True, alias='backup_azure_sql')
    backupAzurePostgresql: Optional[bool] = Field(default=True, alias='backup_azure_postgresql')
    retentionType: str = Field(alias='retention_type')
    retentionDays: Optional[int] = Field(default=None, alias='retention_days')
    # Phase 1 fields
    retentionMode: Optional[str] = Field(default='FLAT', alias='retention_mode')
    retentionHotDays: Optional[int] = Field(default=None, alias='retention_hot_days')
    retentionCoolDays: Optional[int] = Field(default=None, alias='retention_cool_days')
    retentionArchiveDays: Optional[int] = Field(default=None, alias='retention_archive_days')
    gfsDailyCount: Optional[int] = Field(default=None, alias='gfs_daily_count')
    gfsWeeklyCount: Optional[int] = Field(default=None, alias='gfs_weekly_count')
    gfsMonthlyCount: Optional[int] = Field(default=None, alias='gfs_monthly_count')
    gfsYearlyCount: Optional[int] = Field(default=None, alias='gfs_yearly_count')
    itemRetentionDays: Optional[int] = Field(default=None, alias='item_retention_days')
    itemRetentionBasis: Optional[str] = Field(default='SNAPSHOT', alias='item_retention_basis')
    archivedRetentionMode: Optional[str] = Field(default='SAME', alias='archived_retention_mode')
    archivedRetentionDays: Optional[int] = Field(default=None, alias='archived_retention_days')
    legalHoldEnabled: Optional[bool] = Field(default=False, alias='legal_hold_enabled')
    legalHoldUntil: Optional[str] = Field(default=None, alias='legal_hold_until')
    immutabilityMode: Optional[str] = Field(default='None', alias='immutability_mode')
    encryptionMode: Optional[str] = Field(default='VAULT_MANAGED', alias='encryption_mode')
    keyVaultUri: Optional[str] = Field(default=None, alias='key_vault_uri')
    keyName: Optional[str] = Field(default=None, alias='key_name')
    keyVersion: Optional[str] = Field(default=None, alias='key_version')
    keyVersionResolved: Optional[str] = Field(default=None, alias='key_version_resolved')
    autoApplyToMatching: Optional[bool] = Field(default=False, alias='auto_apply_to_matching')
    isDefault: Optional[bool] = Field(default=False, alias='is_default')
    enabled: Optional[bool] = True


# ============ SLA Exclusions + Resource Groups (Phase 1) ============

class SlaExclusionRequest(BaseModel):
    """Create or update an exclusion rule on an SLA policy."""
    model_config = ConfigDict(populate_by_name=True)

    # FOLDER_PATH | FILE_EXTENSION | SUBJECT_REGEX | MIME_TYPE | EMAIL_ADDRESS | FILENAME_GLOB
    exclusionType: str = Field(alias='exclusion_type')
    pattern: str
    # Optional workload scope (EMAIL / FILE / CALENDAR / ...). NULL = apply wherever relevant.
    workload: Optional[str] = None
    applyToHistorical: Optional[bool] = Field(default=False, alias='apply_to_historical')
    enabled: Optional[bool] = True


class SlaExclusionResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True, from_attributes=True)

    id: str
    policyId: str = Field(alias='policy_id')
    exclusionType: str = Field(alias='exclusion_type')
    pattern: str
    workload: Optional[str] = None
    applyToHistorical: bool = Field(alias='apply_to_historical')
    enabled: bool
    createdAt: str = Field(alias='created_at')

    @field_validator('id', 'policyId', mode='before')
    @classmethod
    def uuid_to_str(cls, v):
        return str(v) if v else v

    @field_validator('createdAt', mode='before')
    @classmethod
    def datetime_to_str(cls, v):
        return v.isoformat() if v else v


class ResourceGroupRule(BaseModel):
    """Single rule within a dynamic resource group.

    field     : one of NAME / EMAIL / DEPARTMENT / CITY / COUNTRY / JOB_TITLE /
                RESOURCE_TYPE / EXTERNAL_ID / TAG_VALUE (tenant-kind specific)
    operator  : EQUALS / NOT_EQUALS / CONTAINS / NOT_CONTAINS / STARTS_WITH /
                ENDS_WITH / IN (value is comma-separated)
    value     : right-hand side; string or comma-list for IN"""
    field: str
    operator: str
    value: str


class ResourceGroupRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    description: Optional[str] = None
    groupType: Optional[str] = Field(default='DYNAMIC', alias='group_type')
    rules: Optional[List[ResourceGroupRule]] = Field(default_factory=list)
    combinator: Optional[str] = 'AND'  # AND | OR
    priority: Optional[int] = 100
    autoProtectNew: Optional[bool] = Field(default=False, alias='auto_protect_new')
    enabled: Optional[bool] = True


class ResourceGroupResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True, from_attributes=True)

    id: str
    tenantId: str = Field(alias='tenant_id')
    name: str
    description: Optional[str] = None
    groupType: str = Field(alias='group_type')
    rules: List[Dict[str, Any]] = Field(default_factory=list)
    combinator: str
    priority: int
    autoProtectNew: bool = Field(alias='auto_protect_new')
    enabled: bool
    # Populated when fetched with assignments joined
    attachedPolicyIds: Optional[List[str]] = None
    createdAt: str = Field(alias='created_at')

    @field_validator('id', 'tenantId', mode='before')
    @classmethod
    def uuid_to_str(cls, v):
        return str(v) if v else v

    @field_validator('createdAt', mode='before')
    @classmethod
    def datetime_to_str(cls, v):
        return v.isoformat() if v else v


class GroupPolicyAssignmentRequest(BaseModel):
    """Attach a policy to a resource group (or detach by passing the same id to DELETE)."""
    model_config = ConfigDict(populate_by_name=True)
    policyId: str = Field(alias='policy_id')


# ============ Alert ============

class AlertResponse(BaseModel):
    id: str
    severity: str
    title: str
    description: str
    status: str
    createdAt: str
    resolved: Optional[bool] = None
    tenantId: Optional[str] = None
    type: Optional[str] = None
    message: Optional[str] = None


class AlertListResponse(BaseModel):
    content: List[AlertResponse]
    totalPages: int
    totalElements: int
    size: int
    number: int


# ============ Access Group ============

class AccessGroupResponse(BaseModel):
    id: str
    name: str
    description: Optional[str] = None
    memberCount: Optional[int] = None
    createdAt: Optional[str] = None


class AccessGroupListResponse(BaseModel):
    content: List[AccessGroupResponse]
    totalPages: int
    totalElements: int
    size: int
    number: int


# ============ Admin Consent ============

class AdminConsentResponse(BaseModel):
    """Response for admin consent status"""
    id: str
    consentType: str = Field(alias='consent_type')
    grantedBy: Optional[str] = Field(default=None, alias='granted_by')
    consentedAt: Optional[str] = Field(default=None, alias='consented_at')
    lastUsedAt: Optional[str] = Field(default=None, alias='last_used_at')
    isActive: bool = Field(default=True, alias='is_active')
    scope: Optional[str] = None
    model_config = ConfigDict(populate_by_name=True, from_attributes=True)

    @field_validator('id', mode='before')
    @classmethod
    def uuid_to_str(cls, v):
        return str(v) if v else v

    @field_validator('consentedAt', 'lastUsedAt', mode='before')
    @classmethod
    def datetime_to_str(cls, v):
        return v.isoformat() if v else v


class AdminConsentTokenResponse(BaseModel):
    """Response when granting admin consent"""
    tenantId: str
    consentType: str
    message: str
    consentedAt: str


class PowerBIOAuthCallbackRequest(BaseModel):
    tenantId: str
    code: str
    state: Optional[str] = None


class PowerBIReadinessCheckResponse(BaseModel):
    key: str
    label: str
    status: str
    detail: str


class PowerBIReadinessResponse(BaseModel):
    tenantId: str
    status: str
    summary: str
    authMode: str
    usesDedicatedApp: bool
    accessibleWorkspaceCount: int
    discoveredWorkspaceCount: int
    checks: List[PowerBIReadinessCheckResponse]
    recommendedActions: List[str]


# ============ Storage toggle (2026-04-21) ============

class StorageBackendOut(BaseModel):
    id: UUID
    kind: str
    name: str
    endpoint: str
    is_enabled: bool

    model_config = ConfigDict(from_attributes=True)


class SystemConfigOut(BaseModel):
    active_backend_id: UUID
    active_backend_name: Optional[str] = None
    transition_state: str
    last_toggle_at: Optional[datetime] = None
    cooldown_until: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class ToggleRequest(BaseModel):
    target_backend_id: UUID
    reason: str = Field(..., min_length=10)
    confirmation_text: str


class PreflightCheckOut(BaseModel):
    name: str
    ok: bool
    detail: Optional[str] = None


class PreflightResultOut(BaseModel):
    ok: bool
    checks: List[PreflightCheckOut]


class ToggleEventOut(BaseModel):
    id: UUID
    actor_id: UUID
    from_backend_id: UUID
    to_backend_id: UUID
    reason: Optional[str] = None
    status: str
    started_at: datetime
    drain_completed_at: Optional[datetime] = None
    flip_completed_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    drained_job_count: Optional[int] = None
    retried_job_count: Optional[int] = None

    model_config = ConfigDict(from_attributes=True)


class ToggleStatusOut(BaseModel):
    active_backend: StorageBackendOut
    transition_state: str
    last_toggle_at: Optional[datetime] = None
    cooldown_until: Optional[datetime] = None
    inflight_jobs_count: int
    preflight: Optional[PreflightResultOut] = None
