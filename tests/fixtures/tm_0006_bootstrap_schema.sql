-- Taylor Morrison production schema at alembic 0006, frozen for tests/integration/test_upgrade_from_tm_0006_postgres.py.
-- Generated: tm_vault_backend 98e1398 shared.database.init_db() (TM models, DB_SCHEMA=public) on PostgreSQL 18,
-- then ALTER TYPE resourcetype ADD VALUE 'USER_ARCHIVE' (revision 0006), then
-- pg_dump --schema-only --no-owner --no-privileges --schema=public; psql meta-commands and CREATE SCHEMA public removed.
--
-- PostgreSQL database dump
--


-- Dumped from database version 18.3 (Homebrew)
-- Dumped by pg_dump version 18.3 (Homebrew)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: public; Type: SCHEMA; Schema: -; Owner: -
--



--
-- Name: SCHEMA public; Type: COMMENT; Schema: -; Owner: -
--



--
-- Name: jobstatus; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.jobstatus AS ENUM (
    'QUEUED',
    'RUNNING',
    'COMPLETED',
    'FAILED',
    'CANCELLED',
    'RETRYING',
    'PENDING',
    'CANCELLING'
);


--
-- Name: jobtype; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.jobtype AS ENUM (
    'BACKUP',
    'RESTORE',
    'EXPORT',
    'DISCOVERY',
    'DELETE'
);


--
-- Name: resourcestatus; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.resourcestatus AS ENUM (
    'DISCOVERED',
    'ACTIVE',
    'ARCHIVED',
    'SUSPENDED',
    'PENDING_DELETION',
    'INACCESSIBLE'
);


--
-- Name: resourcetype; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.resourcetype AS ENUM (
    'MAILBOX',
    'SHARED_MAILBOX',
    'ROOM_MAILBOX',
    'ONEDRIVE',
    'SHAREPOINT_SITE',
    'TEAMS_CHANNEL',
    'TEAMS_CHAT',
    'ENTRA_USER',
    'ENTRA_GROUP',
    'ENTRA_APP',
    'ENTRA_SERVICE_PRINCIPAL',
    'ENTRA_DEVICE',
    'ENTRA_ROLE',
    'ENTRA_ADMIN_UNIT',
    'ENTRA_AUDIT_LOG',
    'INTUNE_MANAGED_DEVICE',
    'AZURE_VM',
    'AZURE_SQL_DB',
    'AZURE_POSTGRESQL',
    'AZURE_POSTGRESQL_SINGLE',
    'RESOURCE_GROUP',
    'DYNAMIC_GROUP',
    'POWER_BI',
    'POWER_APPS',
    'POWER_AUTOMATE',
    'POWER_DLP',
    'COPILOT',
    'PLANNER',
    'TODO',
    'ONENOTE',
    'M365_GROUP',
    'ENTRA_CONDITIONAL_ACCESS',
    'ENTRA_BITLOCKER_KEY',
    'TEAMS_CHAT_EXPORT',
    'USER_MAIL',
    'USER_ONEDRIVE',
    'USER_CONTACTS',
    'USER_CALENDAR',
    'USER_CHATS',
    'ENTRA_DIRECTORY',
    'USER_ARCHIVE'
);


--
-- Name: snapshotstatus; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.snapshotstatus AS ENUM (
    'IN_PROGRESS',
    'COMPLETED',
    'FAILED',
    'PARTIAL',
    'PENDING_DELETION'
);


--
-- Name: snapshottype; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.snapshottype AS ENUM (
    'FULL',
    'INCREMENTAL',
    'PREEMPTIVE',
    'MANUAL'
);


--
-- Name: tenantstatus; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.tenantstatus AS ENUM (
    'PENDING',
    'ACTIVE',
    'DISCONNECTED',
    'SUSPENDED',
    'PENDING_DELETION',
    'DISCOVERING',
    'PENDING_DISCOVERY'
);


--
-- Name: tenanttype; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.tenanttype AS ENUM (
    'M365',
    'AZURE'
);


--
-- Name: userrole; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.userrole AS ENUM (
    'SUPER_ADMIN',
    'ORG_ADMIN',
    'TENANT_ADMIN',
    'BACKUP_OPERATOR',
    'RESTORE_OPERATOR',
    'CONTENT_VIEWER',
    'USER'
);


--
-- Name: default_snapshot_backend_id(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.default_snapshot_backend_id() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
    BEGIN
      IF NEW.backend_id IS NULL THEN
        SELECT active_backend_id INTO NEW.backend_id
        FROM system_config WHERE id = 1;
      END IF;
      RETURN NEW;
    END;
    $$;


--
-- Name: notify_storage_backends_changed(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.notify_storage_backends_changed() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
    BEGIN
      PERFORM pg_notify('storage_backends_changed',
                        COALESCE(NEW.id::text, OLD.id::text));
      RETURN COALESCE(NEW, OLD);
    END;
    $$;


--
-- Name: notify_system_config_changed(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.notify_system_config_changed() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
    BEGIN
      PERFORM pg_notify('system_config_changed', NEW.id::text);
      RETURN NEW;
    END;
    $$;


--
-- Name: validate_snapshot_reuse_target(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.validate_snapshot_reuse_target() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
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
        $$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: access_groups; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.access_groups (
    id uuid NOT NULL,
    org_id uuid,
    tenant_id uuid,
    name character varying NOT NULL,
    description character varying,
    scope character varying,
    resource_ids uuid[],
    permissions json,
    member_ids uuid[],
    active boolean,
    created_at timestamp without time zone,
    updated_at timestamp without time zone
);


--
-- Name: admin_consent_tokens; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.admin_consent_tokens (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    org_id uuid,
    tenant_id uuid,
    consent_type character varying NOT NULL,
    access_token_encrypted bytea,
    refresh_token_encrypted bytea,
    token_type character varying DEFAULT 'Bearer'::character varying,
    expires_at timestamp without time zone,
    granted_by character varying,
    consented_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    last_used_at timestamp without time zone,
    is_active boolean DEFAULT true,
    scope character varying,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: alerts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.alerts (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    tenant_id uuid,
    org_id uuid,
    type character varying NOT NULL,
    severity character varying DEFAULT 'MEDIUM'::character varying,
    message text NOT NULL,
    resource_id uuid,
    resource_type character varying,
    resource_name character varying,
    triggered_by character varying,
    resolved boolean DEFAULT false,
    resolved_at timestamp without time zone,
    resolved_by uuid,
    resolution_note text,
    details json DEFAULT '{}'::json,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: app_settings; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.app_settings (
    id integer NOT NULL,
    discovery_time text DEFAULT '02:00'::text NOT NULL,
    timezone text DEFAULT 'America/Phoenix'::text NOT NULL,
    job_retry_max integer DEFAULT 3 NOT NULL,
    job_retry_delay_minutes integer DEFAULT 5 NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT ck_app_settings_singleton CHECK ((id = 1))
);


--
-- Name: app_settings_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.app_settings_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: app_settings_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.app_settings_id_seq OWNED BY public.app_settings.id;


--
-- Name: audit_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.audit_events (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    org_id uuid,
    tenant_id uuid,
    actor_id uuid,
    actor_email character varying,
    actor_type character varying DEFAULT 'SYSTEM'::character varying,
    action character varying NOT NULL,
    resource_id uuid,
    resource_type character varying,
    resource_name character varying,
    outcome character varying DEFAULT 'SUCCESS'::character varying,
    job_id uuid,
    snapshot_id uuid,
    details jsonb DEFAULT '{}'::jsonb,
    occurred_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: backup_batches; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.backup_batches (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    tenant_id uuid NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    completed_at timestamp without time zone,
    source text NOT NULL,
    actor_email text,
    scope_user_ids uuid[] NOT NULL,
    bytes_expected bigint,
    status text DEFAULT 'IN_PROGRESS'::text NOT NULL,
    CONSTRAINT backup_batches_status_check CHECK ((status = ANY (ARRAY['IN_PROGRESS'::text, 'COMPLETED'::text, 'PARTIAL'::text, 'FAILED'::text, 'CANCELLED'::text])))
);


--
-- Name: batch_pending_users; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.batch_pending_users (
    batch_id uuid NOT NULL,
    user_id uuid NOT NULL,
    state text NOT NULL,
    deadline_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: bulk_fanout_seen; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.bulk_fanout_seen (
    job_id uuid NOT NULL,
    resource_id uuid NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: chat_thread_messages; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.chat_thread_messages (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    chat_thread_id uuid NOT NULL,
    message_external_id character varying(256) NOT NULL,
    created_date_time timestamp with time zone,
    last_modified_date_time timestamp with time zone,
    from_user_id character varying(128),
    from_display_name character varying(256),
    body_content text,
    body_content_type character varying(16),
    deleted_date_time timestamp with time zone,
    metadata_raw jsonb,
    content_hash character(64),
    content_size bigint,
    archived_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: chat_threads; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.chat_threads (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    tenant_id uuid NOT NULL,
    chat_id character varying(256) NOT NULL,
    chat_type character varying(32),
    chat_topic text,
    member_names_json jsonb,
    last_updated_at timestamp with time zone,
    last_drained_at timestamp with time zone,
    drain_cursor text,
    drain_failure_state jsonb,
    archived_at timestamp with time zone,
    last_drained_msg_count integer,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: chat_url_cache; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.chat_url_cache (
    tenant_id uuid NOT NULL,
    url_sha256 character(64) NOT NULL,
    drive_item_id character varying(256),
    content_hash character(64),
    blob_path text,
    content_size bigint,
    inline_b64 text,
    unreachable boolean DEFAULT false NOT NULL,
    first_seen_at timestamp with time zone DEFAULT now() NOT NULL,
    last_used_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: discovery_runs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.discovery_runs (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    tenant_id uuid NOT NULL,
    scope json DEFAULT '[]'::json,
    status character varying DEFAULT 'RUNNING'::character varying,
    fetched_count integer DEFAULT 0,
    staged_count integer DEFAULT 0,
    inserted_count integer DEFAULT 0,
    updated_count integer DEFAULT 0,
    unchanged_count integer DEFAULT 0,
    stale_marked_count integer DEFAULT 0,
    error_message text,
    started_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    finished_at timestamp without time zone,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: group_policy_assignments; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.group_policy_assignments (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    group_id uuid NOT NULL,
    policy_id uuid NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: job_logs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.job_logs (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    job_id uuid,
    "timestamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    level character varying DEFAULT 'INFO'::character varying,
    message text,
    details text
);


--
-- Name: jobs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.jobs (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    type public.jobtype DEFAULT 'BACKUP'::public.jobtype NOT NULL,
    tenant_id uuid,
    resource_id uuid,
    batch_resource_ids uuid[] DEFAULT '{}'::uuid[],
    snapshot_id uuid,
    status public.jobstatus DEFAULT 'QUEUED'::public.jobstatus,
    priority integer DEFAULT 5,
    attempts integer DEFAULT 0,
    max_attempts integer DEFAULT 5,
    error_message text,
    progress_pct integer DEFAULT 0,
    items_processed bigint DEFAULT 0,
    bytes_processed bigint DEFAULT 0,
    result json DEFAULT '{}'::json,
    spec json DEFAULT '{}'::json,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    completed_at timestamp without time zone,
    lease_owner_id uuid,
    lease_expires_at timestamp with time zone,
    lease_token bigint DEFAULT 0 NOT NULL,
    requeue_count integer DEFAULT 0 NOT NULL,
    retry_reason text,
    pre_toggle_job_id uuid
);


--
-- Name: mail_folder_delta; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.mail_folder_delta (
    resource_id uuid NOT NULL,
    folder_id text NOT NULL,
    delta_token text NOT NULL,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: mail_folder_fingerprint; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.mail_folder_fingerprint (
    resource_id uuid NOT NULL,
    folder_id text NOT NULL,
    total_item_count integer DEFAULT 0 NOT NULL,
    unread_item_count integer DEFAULT 0 NOT NULL,
    size_in_bytes bigint DEFAULT 0 NOT NULL,
    baseline_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: mail_message_bodies; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.mail_message_bodies (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    tenant_id uuid NOT NULL,
    fingerprint character(64) NOT NULL,
    first_user_id character varying(128),
    first_snapshot_id uuid,
    from_user_id character varying(128),
    from_address character varying(256),
    from_display_name character varying(256),
    subject text,
    sent_date_time timestamp with time zone,
    received_date_time timestamp with time zone,
    body_content text,
    body_content_type character varying(16),
    has_attachments boolean,
    metadata_raw jsonb,
    content_hash character(64),
    content_size bigint,
    ref_count integer DEFAULT 1 NOT NULL,
    last_referenced_at timestamp with time zone DEFAULT now() NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: onedrive_file_retries; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.onedrive_file_retries (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    tenant_id uuid NOT NULL,
    resource_id uuid NOT NULL,
    snapshot_id uuid NOT NULL,
    file_external_id character varying(256) NOT NULL,
    file_name text,
    drive_id text,
    file_payload jsonb NOT NULL,
    attempt_count integer DEFAULT 0 NOT NULL,
    last_error text,
    last_error_class character varying(32),
    next_retry_at timestamp with time zone DEFAULT now() NOT NULL,
    status character varying(16) DEFAULT 'PENDING'::character varying NOT NULL,
    rescued_snapshot_item_id uuid,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: organizations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.organizations (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    name character varying NOT NULL,
    slug character varying NOT NULL,
    storage_region character varying,
    encryption_mode character varying DEFAULT 'TMVAULT_MANAGED'::character varying,
    storage_quota_bytes bigint DEFAULT '536870912000'::bigint,
    storage_bytes_used bigint DEFAULT 0,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: platform_users; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.platform_users (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    email character varying NOT NULL,
    name character varying NOT NULL,
    external_user_id character varying,
    org_id uuid,
    tenant_id uuid,
    mfa_enabled boolean DEFAULT false,
    last_login_at timestamp without time zone,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: report_configs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.report_configs (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    org_id uuid,
    enabled boolean DEFAULT false NOT NULL,
    schedule_type character varying DEFAULT 'daily'::character varying NOT NULL,
    send_empty_report boolean DEFAULT true NOT NULL,
    empty_message character varying DEFAULT 'No updates. No backups occurred.'::character varying,
    send_detailed_report boolean DEFAULT false NOT NULL,
    email_recipients json DEFAULT '[]'::json,
    slack_webhooks json DEFAULT '[]'::json,
    teams_webhooks json DEFAULT '[]'::json,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: report_history; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.report_history (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    org_id uuid,
    report_config_id uuid,
    report_type character varying NOT NULL,
    period_start timestamp without time zone,
    period_end timestamp without time zone,
    generated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    total_backups integer DEFAULT 0,
    successful_backups integer DEFAULT 0,
    failed_backups integer DEFAULT 0,
    success_rate character varying,
    coverage_rate character varying,
    report_data json DEFAULT '{}'::json,
    is_empty boolean DEFAULT false NOT NULL,
    delivery_status json DEFAULT '{}'::json,
    error_message text,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: resource_discovery_staging; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.resource_discovery_staging (
    id bigint NOT NULL,
    run_id uuid,
    tenant_id uuid NOT NULL,
    resource_type character varying NOT NULL,
    external_id character varying NOT NULL,
    display_name character varying NOT NULL,
    email character varying,
    metadata jsonb DEFAULT '{}'::jsonb,
    resource_status character varying DEFAULT 'DISCOVERED'::character varying,
    resource_hash character varying,
    azure_subscription_id character varying,
    azure_resource_group character varying,
    azure_region character varying,
    discovered_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: resource_discovery_staging_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.resource_discovery_staging_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: resource_discovery_staging_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.resource_discovery_staging_id_seq OWNED BY public.resource_discovery_staging.id;


--
-- Name: resource_groups; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.resource_groups (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    tenant_id uuid NOT NULL,
    name character varying NOT NULL,
    description text,
    group_type character varying DEFAULT 'DYNAMIC'::character varying NOT NULL,
    rules json DEFAULT '[]'::json NOT NULL,
    combinator character varying DEFAULT 'AND'::character varying NOT NULL,
    priority integer DEFAULT 100 NOT NULL,
    auto_protect_new boolean DEFAULT false NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: resources; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.resources (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    tenant_id uuid,
    type public.resourcetype DEFAULT 'ENTRA_USER'::public.resourcetype NOT NULL,
    external_id character varying NOT NULL,
    display_name character varying NOT NULL,
    email character varying,
    metadata json DEFAULT '{}'::json,
    sla_policy_id uuid,
    status public.resourcestatus DEFAULT 'DISCOVERED'::public.resourcestatus,
    last_backup_job_id uuid,
    last_backup_at timestamp without time zone,
    last_backup_status character varying,
    storage_bytes bigint DEFAULT 0,
    discovered_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    archived_at timestamp without time zone,
    deletion_queued_at timestamp without time zone,
    azure_subscription_id character varying,
    azure_resource_group character varying,
    azure_region character varying,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    resource_hash character varying,
    parent_resource_id uuid
);


--
-- Name: sharepoint_drive_delta; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.sharepoint_drive_delta (
    resource_id uuid NOT NULL,
    drive_id text NOT NULL,
    delta_token text NOT NULL,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: sla_exclusions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.sla_exclusions (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    policy_id uuid NOT NULL,
    exclusion_type character varying NOT NULL,
    pattern character varying NOT NULL,
    workload character varying,
    apply_to_historical boolean DEFAULT false NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: sla_policies; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.sla_policies (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    tenant_id uuid,
    service_type character varying DEFAULT 'm365'::character varying,
    name character varying NOT NULL,
    frequency character varying DEFAULT 'DAILY'::character varying,
    backup_days character varying[],
    backup_window_start character varying,
    backup_window_end character varying,
    resource_types character varying[],
    batch_size integer DEFAULT 1000,
    max_concurrent_backups integer DEFAULT 5,
    sla_violation_alert boolean DEFAULT true,
    retention_days integer DEFAULT 2555,
    retention_versions integer DEFAULT 10,
    backup_exchange boolean DEFAULT true,
    backup_exchange_archive boolean DEFAULT false,
    backup_exchange_recoverable boolean DEFAULT false,
    backup_onedrive boolean DEFAULT true,
    backup_sharepoint boolean DEFAULT true,
    backup_teams boolean DEFAULT true,
    backup_teams_chats boolean DEFAULT false,
    backup_entra_id boolean DEFAULT true,
    backup_power_platform boolean DEFAULT false,
    backup_copilot boolean DEFAULT false,
    contacts boolean DEFAULT true,
    calendars boolean DEFAULT true,
    tasks boolean DEFAULT false,
    group_mailbox boolean DEFAULT true,
    planner boolean DEFAULT false,
    backup_azure_vm boolean DEFAULT true,
    backup_azure_sql boolean DEFAULT true,
    backup_azure_postgresql boolean DEFAULT true,
    retention_type character varying DEFAULT 'INDEFINITE'::character varying,
    retention_hot_days integer DEFAULT 7,
    retention_cool_days integer DEFAULT 30,
    retention_archive_days integer,
    legal_hold_enabled boolean DEFAULT false,
    legal_hold_until timestamp without time zone,
    immutability_mode character varying DEFAULT 'None'::character varying,
    retention_mode character varying DEFAULT 'FLAT'::character varying NOT NULL,
    gfs_daily_count integer,
    gfs_weekly_count integer,
    gfs_monthly_count integer,
    gfs_yearly_count integer,
    item_retention_days integer,
    item_retention_basis character varying DEFAULT 'SNAPSHOT'::character varying NOT NULL,
    archived_retention_mode character varying DEFAULT 'SAME'::character varying NOT NULL,
    archived_retention_days integer,
    encryption_mode character varying DEFAULT 'VAULT_MANAGED'::character varying NOT NULL,
    key_vault_uri character varying,
    key_name character varying,
    key_version character varying,
    auto_apply_to_matching boolean DEFAULT false NOT NULL,
    enabled boolean DEFAULT true,
    is_default boolean DEFAULT false,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    encryption_status character varying DEFAULT ''::character varying NOT NULL,
    lifecycle_dirty boolean DEFAULT false NOT NULL,
    last_reconciled_at timestamp without time zone,
    reconcile_attempts integer DEFAULT 0 NOT NULL,
    key_version_resolved character varying,
    last_cap_alert_at timestamp without time zone
);


--
-- Name: snapshot_items; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.snapshot_items (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    snapshot_id uuid,
    tenant_id uuid,
    external_id character varying NOT NULL,
    item_type character varying NOT NULL,
    name character varying NOT NULL,
    folder_path character varying,
    content_hash character varying,
    content_checksum character varying,
    content_size bigint DEFAULT 0,
    blob_path character varying,
    encryption_key_id character varying,
    backup_version integer DEFAULT 1,
    metadata json DEFAULT '{}'::json,
    is_deleted boolean DEFAULT false,
    indexed_at timestamp without time zone,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    parent_external_id character varying,
    backend_id uuid
);


--
-- Name: snapshot_partitions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.snapshot_partitions (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    snapshot_id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    resource_id uuid NOT NULL,
    job_id uuid NOT NULL,
    partition_type character varying DEFAULT 'ONEDRIVE_FILES'::character varying NOT NULL,
    drive_id text,
    partition_index integer NOT NULL,
    file_ids json,
    payload json,
    total_files integer DEFAULT 0 NOT NULL,
    total_bytes_est bigint DEFAULT 0 NOT NULL,
    status character varying DEFAULT 'QUEUED'::character varying NOT NULL,
    worker_id character varying,
    worker_region character varying,
    retry_count integer DEFAULT 0 NOT NULL,
    enqueued_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    started_at timestamp without time zone,
    completed_at timestamp without time zone,
    files_uploaded integer DEFAULT 0 NOT NULL,
    bytes_uploaded bigint DEFAULT 0 NOT NULL,
    failure_state json,
    lease_owner_id uuid,
    lease_expires_at timestamp with time zone,
    lease_token bigint DEFAULT 0 NOT NULL,
    requeue_count integer DEFAULT 0 NOT NULL
);


--
-- Name: snapshots; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.snapshots (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    resource_id uuid,
    job_id uuid,
    type public.snapshottype DEFAULT 'FULL'::public.snapshottype,
    status public.snapshotstatus DEFAULT 'IN_PROGRESS'::public.snapshotstatus,
    started_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    completed_at timestamp without time zone,
    duration_secs integer,
    item_count integer DEFAULT 0,
    new_item_count integer DEFAULT 0,
    bytes_added bigint DEFAULT 0,
    bytes_total bigint DEFAULT 0,
    delta_token character varying,
    delta_tokens_json json DEFAULT '{}'::json,
    extra_data json DEFAULT '{}'::json,
    snapshot_label character varying,
    content_checksum character varying,
    blob_path character varying,
    storage_version integer DEFAULT 1,
    azure_restore_point_id character varying,
    azure_operation_id character varying,
    dr_replication_status character varying DEFAULT 'pending'::character varying,
    dr_blob_path character varying,
    dr_replicated_at timestamp without time zone,
    dr_error text,
    dr_replication_attempts integer DEFAULT 0,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    hc_drain_status character varying(16) DEFAULT 'NOT_APPLICABLE'::character varying NOT NULL,
    reuse_of_snapshot_id uuid,
    reuse_chain_root_id uuid,
    lease_owner_id uuid,
    lease_expires_at timestamp with time zone,
    lease_token bigint DEFAULT 0 NOT NULL,
    requeue_count integer DEFAULT 0 NOT NULL,
    backend_id uuid
);


--
-- Name: storage_backends; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.storage_backends (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    kind character varying NOT NULL,
    name character varying NOT NULL,
    endpoint character varying NOT NULL,
    config jsonb DEFAULT '{}'::jsonb NOT NULL,
    secret_ref character varying NOT NULL,
    is_enabled boolean DEFAULT true NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: storage_toggle_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.storage_toggle_events (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    actor_id uuid NOT NULL,
    actor_ip inet,
    from_backend_id uuid NOT NULL,
    to_backend_id uuid NOT NULL,
    reason character varying,
    status character varying DEFAULT 'started'::character varying NOT NULL,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    drain_completed_at timestamp with time zone,
    flip_completed_at timestamp with time zone,
    completed_at timestamp with time zone,
    error_message text,
    pre_flight_checks jsonb,
    drained_job_count integer,
    retried_job_count integer
);


--
-- Name: system_config; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.system_config (
    id smallint NOT NULL,
    active_backend_id uuid NOT NULL,
    transition_state character varying DEFAULT 'stable'::character varying NOT NULL,
    last_toggle_at timestamp with time zone,
    cooldown_until timestamp with time zone,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT system_config_id_check CHECK ((id = 1))
);


--
-- Name: tenant_secrets; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.tenant_secrets (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    tenant_id uuid NOT NULL,
    type character varying NOT NULL,
    name character varying NOT NULL,
    description text,
    metadata_hints json DEFAULT '{}'::json,
    encrypted_payload text,
    is_default boolean DEFAULT false NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: tenants; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.tenants (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    org_id uuid,
    type public.tenanttype DEFAULT 'M365'::public.tenanttype,
    display_name character varying NOT NULL,
    external_tenant_id character varying,
    customer_id character varying,
    subscription_id character varying,
    client_id character varying,
    client_secret_ref character varying,
    graph_client_id character varying,
    graph_client_secret_encrypted bytea,
    status public.tenantstatus DEFAULT 'PENDING'::public.tenantstatus,
    storage_region character varying,
    last_discovery_at timestamp without time zone,
    graph_delta_tokens json DEFAULT '{}'::json,
    extra_data json DEFAULT '{}'::json,
    dr_region_enabled boolean DEFAULT false,
    dr_region character varying,
    dr_storage_account_name character varying,
    dr_storage_account_key_encrypted bytea,
    dr_last_replicated_at timestamp without time zone,
    azure_refresh_token_encrypted bytea,
    azure_refresh_token_updated_at timestamp without time zone,
    azure_subscriptions_cached json DEFAULT '[]'::json,
    azure_sql_servers_configured json DEFAULT '[]'::json,
    archived_at timestamp with time zone,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    azure_pg_servers_configured json DEFAULT '{}'::json
);


--
-- Name: user_roles; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.user_roles (
    user_id uuid NOT NULL,
    role public.userrole NOT NULL
);


--
-- Name: vm_file_index; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.vm_file_index (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    snapshot_id uuid NOT NULL,
    volume_item_id uuid NOT NULL,
    parent_path character varying NOT NULL,
    name character varying NOT NULL,
    is_directory boolean DEFAULT false NOT NULL,
    size_bytes bigint DEFAULT 0 NOT NULL,
    modified_at timestamp without time zone,
    fs_inode bigint,
    fs_type character varying,
    partition_offset bigint,
    blob_path character varying,
    extents_json json,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: work_dead_letter; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.work_dead_letter (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    work_kind text NOT NULL,
    work_id uuid NOT NULL,
    reason text NOT NULL,
    last_payload json,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: worker_heartbeats; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.worker_heartbeats (
    worker_id uuid NOT NULL,
    replica_id text NOT NULL,
    service_name text NOT NULL,
    pid integer,
    queues text[] DEFAULT ARRAY[]::text[] NOT NULL,
    last_seen_at timestamp with time zone DEFAULT now() NOT NULL,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    version text
);


--
-- Name: app_settings id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.app_settings ALTER COLUMN id SET DEFAULT nextval('public.app_settings_id_seq'::regclass);


--
-- Name: resource_discovery_staging id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resource_discovery_staging ALTER COLUMN id SET DEFAULT nextval('public.resource_discovery_staging_id_seq'::regclass);


--
-- Name: access_groups access_groups_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.access_groups
    ADD CONSTRAINT access_groups_pkey PRIMARY KEY (id);


--
-- Name: admin_consent_tokens admin_consent_tokens_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.admin_consent_tokens
    ADD CONSTRAINT admin_consent_tokens_pkey PRIMARY KEY (id);


--
-- Name: alerts alerts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.alerts
    ADD CONSTRAINT alerts_pkey PRIMARY KEY (id);


--
-- Name: app_settings app_settings_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.app_settings
    ADD CONSTRAINT app_settings_pkey PRIMARY KEY (id);


--
-- Name: audit_events audit_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.audit_events
    ADD CONSTRAINT audit_events_pkey PRIMARY KEY (id);


--
-- Name: backup_batches backup_batches_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.backup_batches
    ADD CONSTRAINT backup_batches_pkey PRIMARY KEY (id);


--
-- Name: batch_pending_users batch_pending_users_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.batch_pending_users
    ADD CONSTRAINT batch_pending_users_pkey PRIMARY KEY (batch_id, user_id);


--
-- Name: bulk_fanout_seen bulk_fanout_seen_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bulk_fanout_seen
    ADD CONSTRAINT bulk_fanout_seen_pkey PRIMARY KEY (job_id, resource_id);


--
-- Name: chat_thread_messages chat_thread_messages_chat_thread_id_message_external_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.chat_thread_messages
    ADD CONSTRAINT chat_thread_messages_chat_thread_id_message_external_id_key UNIQUE (chat_thread_id, message_external_id);


--
-- Name: chat_thread_messages chat_thread_messages_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.chat_thread_messages
    ADD CONSTRAINT chat_thread_messages_pkey PRIMARY KEY (id);


--
-- Name: chat_threads chat_threads_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.chat_threads
    ADD CONSTRAINT chat_threads_pkey PRIMARY KEY (id);


--
-- Name: chat_threads chat_threads_tenant_id_chat_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.chat_threads
    ADD CONSTRAINT chat_threads_tenant_id_chat_id_key UNIQUE (tenant_id, chat_id);


--
-- Name: chat_url_cache chat_url_cache_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.chat_url_cache
    ADD CONSTRAINT chat_url_cache_pkey PRIMARY KEY (tenant_id, url_sha256);


--
-- Name: discovery_runs discovery_runs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.discovery_runs
    ADD CONSTRAINT discovery_runs_pkey PRIMARY KEY (id);


--
-- Name: group_policy_assignments group_policy_assignments_group_id_policy_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.group_policy_assignments
    ADD CONSTRAINT group_policy_assignments_group_id_policy_id_key UNIQUE (group_id, policy_id);


--
-- Name: group_policy_assignments group_policy_assignments_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.group_policy_assignments
    ADD CONSTRAINT group_policy_assignments_pkey PRIMARY KEY (id);


--
-- Name: job_logs job_logs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.job_logs
    ADD CONSTRAINT job_logs_pkey PRIMARY KEY (id);


--
-- Name: jobs jobs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.jobs
    ADD CONSTRAINT jobs_pkey PRIMARY KEY (id);


--
-- Name: mail_folder_delta mail_folder_delta_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.mail_folder_delta
    ADD CONSTRAINT mail_folder_delta_pkey PRIMARY KEY (resource_id, folder_id);


--
-- Name: mail_folder_fingerprint mail_folder_fingerprint_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.mail_folder_fingerprint
    ADD CONSTRAINT mail_folder_fingerprint_pkey PRIMARY KEY (resource_id, folder_id);


--
-- Name: mail_message_bodies mail_message_bodies_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.mail_message_bodies
    ADD CONSTRAINT mail_message_bodies_pkey PRIMARY KEY (id);


--
-- Name: mail_message_bodies mail_message_bodies_tenant_id_fingerprint_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.mail_message_bodies
    ADD CONSTRAINT mail_message_bodies_tenant_id_fingerprint_key UNIQUE (tenant_id, fingerprint);


--
-- Name: onedrive_file_retries onedrive_file_retries_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.onedrive_file_retries
    ADD CONSTRAINT onedrive_file_retries_pkey PRIMARY KEY (id);


--
-- Name: onedrive_file_retries onedrive_file_retries_snapshot_id_file_external_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.onedrive_file_retries
    ADD CONSTRAINT onedrive_file_retries_snapshot_id_file_external_id_key UNIQUE (snapshot_id, file_external_id);


--
-- Name: organizations organizations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.organizations
    ADD CONSTRAINT organizations_pkey PRIMARY KEY (id);


--
-- Name: organizations organizations_slug_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.organizations
    ADD CONSTRAINT organizations_slug_key UNIQUE (slug);


--
-- Name: platform_users platform_users_email_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.platform_users
    ADD CONSTRAINT platform_users_email_key UNIQUE (email);


--
-- Name: platform_users platform_users_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.platform_users
    ADD CONSTRAINT platform_users_pkey PRIMARY KEY (id);


--
-- Name: report_configs report_configs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.report_configs
    ADD CONSTRAINT report_configs_pkey PRIMARY KEY (id);


--
-- Name: report_history report_history_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.report_history
    ADD CONSTRAINT report_history_pkey PRIMARY KEY (id);


--
-- Name: resource_discovery_staging resource_discovery_staging_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resource_discovery_staging
    ADD CONSTRAINT resource_discovery_staging_pkey PRIMARY KEY (id);


--
-- Name: resource_groups resource_groups_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resource_groups
    ADD CONSTRAINT resource_groups_pkey PRIMARY KEY (id);


--
-- Name: resources resources_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resources
    ADD CONSTRAINT resources_pkey PRIMARY KEY (id);


--
-- Name: sharepoint_drive_delta sharepoint_drive_delta_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sharepoint_drive_delta
    ADD CONSTRAINT sharepoint_drive_delta_pkey PRIMARY KEY (resource_id, drive_id);


--
-- Name: sla_exclusions sla_exclusions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sla_exclusions
    ADD CONSTRAINT sla_exclusions_pkey PRIMARY KEY (id);


--
-- Name: sla_policies sla_policies_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sla_policies
    ADD CONSTRAINT sla_policies_pkey PRIMARY KEY (id);


--
-- Name: snapshot_items snapshot_items_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.snapshot_items
    ADD CONSTRAINT snapshot_items_pkey PRIMARY KEY (id);


--
-- Name: snapshot_partitions snapshot_partitions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.snapshot_partitions
    ADD CONSTRAINT snapshot_partitions_pkey PRIMARY KEY (id);


--
-- Name: snapshot_partitions snapshot_partitions_snapshot_id_partition_index_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.snapshot_partitions
    ADD CONSTRAINT snapshot_partitions_snapshot_id_partition_index_key UNIQUE (snapshot_id, partition_index);


--
-- Name: snapshots snapshots_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.snapshots
    ADD CONSTRAINT snapshots_pkey PRIMARY KEY (id);


--
-- Name: storage_backends storage_backends_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.storage_backends
    ADD CONSTRAINT storage_backends_name_key UNIQUE (name);


--
-- Name: storage_backends storage_backends_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.storage_backends
    ADD CONSTRAINT storage_backends_pkey PRIMARY KEY (id);


--
-- Name: storage_toggle_events storage_toggle_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.storage_toggle_events
    ADD CONSTRAINT storage_toggle_events_pkey PRIMARY KEY (id);


--
-- Name: system_config system_config_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.system_config
    ADD CONSTRAINT system_config_pkey PRIMARY KEY (id);


--
-- Name: tenant_secrets tenant_secrets_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tenant_secrets
    ADD CONSTRAINT tenant_secrets_pkey PRIMARY KEY (id);


--
-- Name: tenants tenants_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tenants
    ADD CONSTRAINT tenants_pkey PRIMARY KEY (id);


--
-- Name: user_roles user_roles_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_roles
    ADD CONSTRAINT user_roles_pkey PRIMARY KEY (user_id, role);


--
-- Name: vm_file_index vm_file_index_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.vm_file_index
    ADD CONSTRAINT vm_file_index_pkey PRIMARY KEY (id);


--
-- Name: work_dead_letter work_dead_letter_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.work_dead_letter
    ADD CONSTRAINT work_dead_letter_pkey PRIMARY KEY (id);


--
-- Name: worker_heartbeats worker_heartbeats_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.worker_heartbeats
    ADD CONSTRAINT worker_heartbeats_pkey PRIMARY KEY (worker_id);


--
-- Name: idx_admin_consent_active; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_admin_consent_active ON public.admin_consent_tokens USING btree (is_active);


--
-- Name: idx_admin_consent_org; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_admin_consent_org ON public.admin_consent_tokens USING btree (org_id);


--
-- Name: idx_admin_consent_tenant; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_admin_consent_tenant ON public.admin_consent_tokens USING btree (tenant_id);


--
-- Name: idx_admin_consent_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_admin_consent_type ON public.admin_consent_tokens USING btree (consent_type);


--
-- Name: idx_audit_events_action; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_audit_events_action ON public.audit_events USING btree (action);


--
-- Name: idx_audit_events_occurred; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_audit_events_occurred ON public.audit_events USING btree (occurred_at DESC);


--
-- Name: idx_audit_events_org; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_audit_events_org ON public.audit_events USING btree (org_id);


--
-- Name: idx_audit_events_resource; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_audit_events_resource ON public.audit_events USING btree (resource_id);


--
-- Name: idx_audit_events_tenant; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_audit_events_tenant ON public.audit_events USING btree (tenant_id);


--
-- Name: idx_discovery_runs_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_discovery_runs_status ON public.discovery_runs USING btree (status);


--
-- Name: idx_discovery_runs_tenant_started; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_discovery_runs_tenant_started ON public.discovery_runs USING btree (tenant_id, started_at DESC);


--
-- Name: idx_discovery_stage_lookup; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_discovery_stage_lookup ON public.resource_discovery_staging USING btree (run_id, tenant_id, resource_type, external_id);


--
-- Name: idx_discovery_stage_run; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_discovery_stage_run ON public.resource_discovery_staging USING btree (run_id);


--
-- Name: idx_group_policy_assignments_group; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_group_policy_assignments_group ON public.group_policy_assignments USING btree (group_id);


--
-- Name: idx_group_policy_assignments_policy; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_group_policy_assignments_policy ON public.group_policy_assignments USING btree (policy_id);


--
-- Name: idx_jobs_tenant_type_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_jobs_tenant_type_status ON public.jobs USING btree (tenant_id, type, status) WHERE (status = ANY (ARRAY['QUEUED'::public.jobstatus, 'PENDING'::public.jobstatus, 'RUNNING'::public.jobstatus]));


--
-- Name: idx_report_configs_org; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_report_configs_org ON public.report_configs USING btree (org_id);


--
-- Name: idx_report_history_generated; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_report_history_generated ON public.report_history USING btree (generated_at DESC);


--
-- Name: idx_report_history_org; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_report_history_org ON public.report_history USING btree (org_id);


--
-- Name: idx_resource_groups_priority; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_resource_groups_priority ON public.resource_groups USING btree (tenant_id, priority, enabled);


--
-- Name: idx_resource_groups_tenant; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_resource_groups_tenant ON public.resource_groups USING btree (tenant_id, enabled);


--
-- Name: idx_resources_tenant_status_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_resources_tenant_status_type ON public.resources USING btree (tenant_id, status, type);


--
-- Name: idx_resources_tenant_type_external; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_resources_tenant_type_external ON public.resources USING btree (tenant_id, type, external_id);


--
-- Name: idx_sla_exclusions_policy; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_sla_exclusions_policy ON public.sla_exclusions USING btree (policy_id, enabled);


--
-- Name: idx_sla_policies_tenant_service; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_sla_policies_tenant_service ON public.sla_policies USING btree (tenant_id, service_type);


--
-- Name: idx_snapshot_items_backend; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_snapshot_items_backend ON public.snapshot_items USING btree (backend_id);


--
-- Name: idx_snapshot_items_chat_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_snapshot_items_chat_created ON public.snapshot_items USING btree (snapshot_id, folder_path, created_at) WHERE ((item_type)::text = ANY ((ARRAY['TEAMS_CHAT_MESSAGE'::character varying, 'TEAMS_MESSAGE'::character varying, 'TEAMS_MESSAGE_REPLY'::character varying])::text[]));


--
-- Name: idx_snapshot_items_folder_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_snapshot_items_folder_type ON public.snapshot_items USING btree (folder_path, item_type);


--
-- Name: idx_snapshot_items_metadata_chat_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_snapshot_items_metadata_chat_id ON public.snapshot_items USING btree (((metadata ->> 'chatId'::text))) WHERE ((item_type)::text = 'TEAMS_CHAT_MESSAGE'::text);


--
-- Name: idx_snapshot_items_parent_ext; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_snapshot_items_parent_ext ON public.snapshot_items USING btree (parent_external_id) WHERE ((item_type)::text = ANY ((ARRAY['CHAT_ATTACHMENT'::character varying, 'CHAT_HOSTED_CONTENT'::character varying])::text[]));


--
-- Name: idx_snapshot_items_snap_type_ext; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_snapshot_items_snap_type_ext ON public.snapshot_items USING btree (snapshot_id, item_type, external_id);


--
-- Name: idx_snapshot_items_snapshot_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_snapshot_items_snapshot_type ON public.snapshot_items USING btree (snapshot_id, item_type);


--
-- Name: idx_snapshot_items_tenant_checksum; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_snapshot_items_tenant_checksum ON public.snapshot_items USING btree (tenant_id, content_checksum);


--
-- Name: idx_snapshot_items_tenant_type_time; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_snapshot_items_tenant_type_time ON public.snapshot_items USING btree (tenant_id, item_type, created_at DESC);


--
-- Name: idx_snapshots_backend; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_snapshots_backend ON public.snapshots USING btree (backend_id);


--
-- Name: idx_vm_file_index_lookup; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_vm_file_index_lookup ON public.vm_file_index USING btree (snapshot_id, volume_item_id, parent_path);


--
-- Name: idx_vm_file_index_snap; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_vm_file_index_snap ON public.vm_file_index USING btree (snapshot_id);


--
-- Name: ix_backup_batches_status_inprogress; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_backup_batches_status_inprogress ON public.backup_batches USING btree (status) WHERE (status = 'IN_PROGRESS'::text);


--
-- Name: ix_backup_batches_tenant_started; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_backup_batches_tenant_started ON public.backup_batches USING btree (tenant_id, created_at DESC);


--
-- Name: ix_batch_pending_users_user; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_batch_pending_users_user ON public.batch_pending_users USING btree (user_id);


--
-- Name: ix_chat_thread_messages_thread_time; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_chat_thread_messages_thread_time ON public.chat_thread_messages USING btree (chat_thread_id, created_date_time DESC);


--
-- Name: ix_chat_threads_tenant; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_chat_threads_tenant ON public.chat_threads USING btree (tenant_id, last_drained_at);


--
-- Name: ix_chat_url_cache_last_used; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_chat_url_cache_last_used ON public.chat_url_cache USING btree (tenant_id, last_used_at);


--
-- Name: ix_jobs_running_lease; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_jobs_running_lease ON public.jobs USING btree (lease_expires_at) WHERE (status = 'RUNNING'::public.jobstatus);


--
-- Name: ix_mail_message_bodies_purge; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_mail_message_bodies_purge ON public.mail_message_bodies USING btree (last_referenced_at) WHERE (ref_count <= 0);


--
-- Name: ix_mail_message_bodies_tenant_fp; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_mail_message_bodies_tenant_fp ON public.mail_message_bodies USING btree (tenant_id, fingerprint);


--
-- Name: ix_onedrive_file_retries_ready; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_onedrive_file_retries_ready ON public.onedrive_file_retries USING btree (next_retry_at) WHERE ((status)::text = ANY ((ARRAY['PENDING'::character varying, 'IN_PROGRESS'::character varying])::text[]));


--
-- Name: ix_onedrive_file_retries_snapshot; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_onedrive_file_retries_snapshot ON public.onedrive_file_retries USING btree (snapshot_id, status);


--
-- Name: ix_resources_parent_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_resources_parent_id ON public.resources USING btree (parent_resource_id);


--
-- Name: ix_sla_policies_lifecycle_dirty; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_sla_policies_lifecycle_dirty ON public.sla_policies USING btree (lifecycle_dirty) WHERE (lifecycle_dirty = true);


--
-- Name: ix_sla_policies_one_default_per_tenant; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ix_sla_policies_one_default_per_tenant ON public.sla_policies USING btree (tenant_id) WHERE (is_default = true);


--
-- Name: ix_snap_partition_claim; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_snap_partition_claim ON public.snapshot_partitions USING btree (enqueued_at) WHERE ((status)::text = ANY ((ARRAY['QUEUED'::character varying, 'IN_PROGRESS'::character varying])::text[]));


--
-- Name: ix_snap_partition_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_snap_partition_status ON public.snapshot_partitions USING btree (snapshot_id, status);


--
-- Name: ix_snap_partition_type_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_snap_partition_type_status ON public.snapshot_partitions USING btree (snapshot_id, partition_type, status);


--
-- Name: ix_snapshot_items_parent_external_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_snapshot_items_parent_external_id ON public.snapshot_items USING btree (parent_external_id);


--
-- Name: ix_snapshot_partitions_inprog_lease; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_snapshot_partitions_inprog_lease ON public.snapshot_partitions USING btree (lease_expires_at) WHERE ((status)::text = ANY ((ARRAY['QUEUED'::character varying, 'IN_PROGRESS'::character varying])::text[]));


--
-- Name: ix_snapshots_hc_pending; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_snapshots_hc_pending ON public.snapshots USING btree (hc_drain_status) WHERE ((hc_drain_status)::text = 'PENDING'::text);


--
-- Name: ix_snapshots_inprog_lease; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_snapshots_inprog_lease ON public.snapshots USING btree (lease_expires_at) WHERE (status = 'IN_PROGRESS'::public.snapshotstatus);


--
-- Name: ix_snapshots_job_resource_inprogress; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ix_snapshots_job_resource_inprogress ON public.snapshots USING btree (job_id, resource_id) WHERE (status = 'IN_PROGRESS'::public.snapshotstatus);


--
-- Name: ix_snapshots_reuse_chain_root; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_snapshots_reuse_chain_root ON public.snapshots USING btree (reuse_chain_root_id) WHERE (reuse_chain_root_id IS NOT NULL);


--
-- Name: ix_snapshots_reuse_of_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_snapshots_reuse_of_id ON public.snapshots USING btree (reuse_of_snapshot_id) WHERE (reuse_of_snapshot_id IS NOT NULL);


--
-- Name: ix_work_dead_letter_kind_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_work_dead_letter_kind_created ON public.work_dead_letter USING btree (work_kind, created_at DESC);


--
-- Name: ix_worker_heartbeats_last_seen; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_worker_heartbeats_last_seen ON public.worker_heartbeats USING btree (last_seen_at);


--
-- Name: tenants_external_tenant_id_type_key; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX tenants_external_tenant_id_type_key ON public.tenants USING btree (external_tenant_id, type) WHERE (external_tenant_id IS NOT NULL);


--
-- Name: uq_snapshot_items_snap_ext_type; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX uq_snapshot_items_snap_ext_type ON public.snapshot_items USING btree (snapshot_id, external_id, item_type);


--
-- Name: snapshots snapshots_reuse_validate; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER snapshots_reuse_validate BEFORE INSERT OR UPDATE OF reuse_of_snapshot_id, reuse_chain_root_id ON public.snapshots FOR EACH ROW EXECUTE FUNCTION public.validate_snapshot_reuse_target();


--
-- Name: snapshot_items trg_snapshot_items_default_backend; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trg_snapshot_items_default_backend BEFORE INSERT ON public.snapshot_items FOR EACH ROW EXECUTE FUNCTION public.default_snapshot_backend_id();


--
-- Name: snapshots trg_snapshots_default_backend; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trg_snapshots_default_backend BEFORE INSERT ON public.snapshots FOR EACH ROW EXECUTE FUNCTION public.default_snapshot_backend_id();


--
-- Name: storage_backends trg_storage_backends_notify; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trg_storage_backends_notify AFTER INSERT OR DELETE OR UPDATE ON public.storage_backends FOR EACH ROW EXECUTE FUNCTION public.notify_storage_backends_changed();


--
-- Name: system_config trg_system_config_notify; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trg_system_config_notify AFTER INSERT OR UPDATE ON public.system_config FOR EACH ROW EXECUTE FUNCTION public.notify_system_config_changed();


--
-- Name: access_groups access_groups_org_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.access_groups
    ADD CONSTRAINT access_groups_org_id_fkey FOREIGN KEY (org_id) REFERENCES public.organizations(id);


--
-- Name: access_groups access_groups_tenant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.access_groups
    ADD CONSTRAINT access_groups_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES public.tenants(id);


--
-- Name: admin_consent_tokens admin_consent_tokens_org_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.admin_consent_tokens
    ADD CONSTRAINT admin_consent_tokens_org_id_fkey FOREIGN KEY (org_id) REFERENCES public.organizations(id);


--
-- Name: admin_consent_tokens admin_consent_tokens_tenant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.admin_consent_tokens
    ADD CONSTRAINT admin_consent_tokens_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES public.tenants(id);


--
-- Name: alerts alerts_org_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.alerts
    ADD CONSTRAINT alerts_org_id_fkey FOREIGN KEY (org_id) REFERENCES public.organizations(id);


--
-- Name: alerts alerts_tenant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.alerts
    ADD CONSTRAINT alerts_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES public.tenants(id);


--
-- Name: audit_events audit_events_org_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.audit_events
    ADD CONSTRAINT audit_events_org_id_fkey FOREIGN KEY (org_id) REFERENCES public.organizations(id);


--
-- Name: audit_events audit_events_tenant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.audit_events
    ADD CONSTRAINT audit_events_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES public.tenants(id);


--
-- Name: batch_pending_users batch_pending_users_batch_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.batch_pending_users
    ADD CONSTRAINT batch_pending_users_batch_id_fkey FOREIGN KEY (batch_id) REFERENCES public.backup_batches(id) ON DELETE CASCADE;


--
-- Name: batch_pending_users batch_pending_users_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.batch_pending_users
    ADD CONSTRAINT batch_pending_users_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.resources(id) ON DELETE CASCADE;


--
-- Name: bulk_fanout_seen bulk_fanout_seen_job_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bulk_fanout_seen
    ADD CONSTRAINT bulk_fanout_seen_job_id_fkey FOREIGN KEY (job_id) REFERENCES public.jobs(id) ON DELETE CASCADE;


--
-- Name: bulk_fanout_seen bulk_fanout_seen_resource_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bulk_fanout_seen
    ADD CONSTRAINT bulk_fanout_seen_resource_id_fkey FOREIGN KEY (resource_id) REFERENCES public.resources(id) ON DELETE CASCADE;


--
-- Name: chat_thread_messages chat_thread_messages_chat_thread_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.chat_thread_messages
    ADD CONSTRAINT chat_thread_messages_chat_thread_id_fkey FOREIGN KEY (chat_thread_id) REFERENCES public.chat_threads(id) ON DELETE RESTRICT;


--
-- Name: chat_threads chat_threads_tenant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.chat_threads
    ADD CONSTRAINT chat_threads_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES public.tenants(id) ON DELETE RESTRICT;


--
-- Name: chat_url_cache chat_url_cache_tenant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.chat_url_cache
    ADD CONSTRAINT chat_url_cache_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES public.tenants(id) ON DELETE CASCADE;


--
-- Name: discovery_runs discovery_runs_tenant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.discovery_runs
    ADD CONSTRAINT discovery_runs_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES public.tenants(id);


--
-- Name: group_policy_assignments group_policy_assignments_group_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.group_policy_assignments
    ADD CONSTRAINT group_policy_assignments_group_id_fkey FOREIGN KEY (group_id) REFERENCES public.resource_groups(id) ON DELETE CASCADE;


--
-- Name: group_policy_assignments group_policy_assignments_policy_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.group_policy_assignments
    ADD CONSTRAINT group_policy_assignments_policy_id_fkey FOREIGN KEY (policy_id) REFERENCES public.sla_policies(id) ON DELETE CASCADE;


--
-- Name: job_logs job_logs_job_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.job_logs
    ADD CONSTRAINT job_logs_job_id_fkey FOREIGN KEY (job_id) REFERENCES public.jobs(id);


--
-- Name: jobs jobs_pre_toggle_job_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.jobs
    ADD CONSTRAINT jobs_pre_toggle_job_id_fkey FOREIGN KEY (pre_toggle_job_id) REFERENCES public.jobs(id);


--
-- Name: jobs jobs_resource_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.jobs
    ADD CONSTRAINT jobs_resource_id_fkey FOREIGN KEY (resource_id) REFERENCES public.resources(id);


--
-- Name: jobs jobs_tenant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.jobs
    ADD CONSTRAINT jobs_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES public.tenants(id);


--
-- Name: mail_folder_delta mail_folder_delta_resource_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.mail_folder_delta
    ADD CONSTRAINT mail_folder_delta_resource_id_fkey FOREIGN KEY (resource_id) REFERENCES public.resources(id) ON DELETE CASCADE;


--
-- Name: mail_folder_fingerprint mail_folder_fingerprint_resource_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.mail_folder_fingerprint
    ADD CONSTRAINT mail_folder_fingerprint_resource_id_fkey FOREIGN KEY (resource_id) REFERENCES public.resources(id) ON DELETE CASCADE;


--
-- Name: mail_message_bodies mail_message_bodies_tenant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.mail_message_bodies
    ADD CONSTRAINT mail_message_bodies_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES public.tenants(id) ON DELETE RESTRICT;


--
-- Name: onedrive_file_retries onedrive_file_retries_resource_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.onedrive_file_retries
    ADD CONSTRAINT onedrive_file_retries_resource_id_fkey FOREIGN KEY (resource_id) REFERENCES public.resources(id) ON DELETE CASCADE;


--
-- Name: onedrive_file_retries onedrive_file_retries_snapshot_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.onedrive_file_retries
    ADD CONSTRAINT onedrive_file_retries_snapshot_id_fkey FOREIGN KEY (snapshot_id) REFERENCES public.snapshots(id) ON DELETE CASCADE;


--
-- Name: onedrive_file_retries onedrive_file_retries_tenant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.onedrive_file_retries
    ADD CONSTRAINT onedrive_file_retries_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES public.tenants(id) ON DELETE CASCADE;


--
-- Name: platform_users platform_users_org_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.platform_users
    ADD CONSTRAINT platform_users_org_id_fkey FOREIGN KEY (org_id) REFERENCES public.organizations(id);


--
-- Name: platform_users platform_users_tenant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.platform_users
    ADD CONSTRAINT platform_users_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES public.tenants(id);


--
-- Name: report_configs report_configs_org_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.report_configs
    ADD CONSTRAINT report_configs_org_id_fkey FOREIGN KEY (org_id) REFERENCES public.organizations(id);


--
-- Name: report_history report_history_org_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.report_history
    ADD CONSTRAINT report_history_org_id_fkey FOREIGN KEY (org_id) REFERENCES public.organizations(id);


--
-- Name: report_history report_history_report_config_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.report_history
    ADD CONSTRAINT report_history_report_config_id_fkey FOREIGN KEY (report_config_id) REFERENCES public.report_configs(id);


--
-- Name: resource_discovery_staging resource_discovery_staging_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resource_discovery_staging
    ADD CONSTRAINT resource_discovery_staging_run_id_fkey FOREIGN KEY (run_id) REFERENCES public.discovery_runs(id) ON DELETE CASCADE;


--
-- Name: resource_discovery_staging resource_discovery_staging_tenant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resource_discovery_staging
    ADD CONSTRAINT resource_discovery_staging_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES public.tenants(id);


--
-- Name: resource_groups resource_groups_tenant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resource_groups
    ADD CONSTRAINT resource_groups_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES public.tenants(id) ON DELETE CASCADE;


--
-- Name: resources resources_parent_resource_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resources
    ADD CONSTRAINT resources_parent_resource_id_fkey FOREIGN KEY (parent_resource_id) REFERENCES public.resources(id) ON DELETE CASCADE;


--
-- Name: resources resources_sla_policy_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resources
    ADD CONSTRAINT resources_sla_policy_id_fkey FOREIGN KEY (sla_policy_id) REFERENCES public.sla_policies(id);


--
-- Name: resources resources_tenant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resources
    ADD CONSTRAINT resources_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES public.tenants(id);


--
-- Name: sharepoint_drive_delta sharepoint_drive_delta_resource_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sharepoint_drive_delta
    ADD CONSTRAINT sharepoint_drive_delta_resource_id_fkey FOREIGN KEY (resource_id) REFERENCES public.resources(id) ON DELETE CASCADE;


--
-- Name: sla_exclusions sla_exclusions_policy_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sla_exclusions
    ADD CONSTRAINT sla_exclusions_policy_id_fkey FOREIGN KEY (policy_id) REFERENCES public.sla_policies(id) ON DELETE CASCADE;


--
-- Name: sla_policies sla_policies_tenant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sla_policies
    ADD CONSTRAINT sla_policies_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES public.tenants(id);


--
-- Name: snapshot_items snapshot_items_backend_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.snapshot_items
    ADD CONSTRAINT snapshot_items_backend_id_fkey FOREIGN KEY (backend_id) REFERENCES public.storage_backends(id);


--
-- Name: snapshot_items snapshot_items_snapshot_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.snapshot_items
    ADD CONSTRAINT snapshot_items_snapshot_id_fkey FOREIGN KEY (snapshot_id) REFERENCES public.snapshots(id);


--
-- Name: snapshot_items snapshot_items_tenant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.snapshot_items
    ADD CONSTRAINT snapshot_items_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES public.tenants(id);


--
-- Name: snapshot_partitions snapshot_partitions_snapshot_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.snapshot_partitions
    ADD CONSTRAINT snapshot_partitions_snapshot_id_fkey FOREIGN KEY (snapshot_id) REFERENCES public.snapshots(id) ON DELETE CASCADE;


--
-- Name: snapshots snapshots_backend_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.snapshots
    ADD CONSTRAINT snapshots_backend_id_fkey FOREIGN KEY (backend_id) REFERENCES public.storage_backends(id);


--
-- Name: snapshots snapshots_job_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.snapshots
    ADD CONSTRAINT snapshots_job_id_fkey FOREIGN KEY (job_id) REFERENCES public.jobs(id);


--
-- Name: snapshots snapshots_resource_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.snapshots
    ADD CONSTRAINT snapshots_resource_id_fkey FOREIGN KEY (resource_id) REFERENCES public.resources(id);


--
-- Name: snapshots snapshots_reuse_chain_root_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.snapshots
    ADD CONSTRAINT snapshots_reuse_chain_root_id_fkey FOREIGN KEY (reuse_chain_root_id) REFERENCES public.snapshots(id) ON DELETE RESTRICT;


--
-- Name: snapshots snapshots_reuse_of_snapshot_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.snapshots
    ADD CONSTRAINT snapshots_reuse_of_snapshot_id_fkey FOREIGN KEY (reuse_of_snapshot_id) REFERENCES public.snapshots(id) ON DELETE RESTRICT;


--
-- Name: storage_toggle_events storage_toggle_events_from_backend_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.storage_toggle_events
    ADD CONSTRAINT storage_toggle_events_from_backend_id_fkey FOREIGN KEY (from_backend_id) REFERENCES public.storage_backends(id);


--
-- Name: storage_toggle_events storage_toggle_events_to_backend_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.storage_toggle_events
    ADD CONSTRAINT storage_toggle_events_to_backend_id_fkey FOREIGN KEY (to_backend_id) REFERENCES public.storage_backends(id);


--
-- Name: system_config system_config_active_backend_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.system_config
    ADD CONSTRAINT system_config_active_backend_id_fkey FOREIGN KEY (active_backend_id) REFERENCES public.storage_backends(id);


--
-- Name: tenant_secrets tenant_secrets_tenant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tenant_secrets
    ADD CONSTRAINT tenant_secrets_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES public.tenants(id) ON DELETE CASCADE;


--
-- Name: tenants tenants_org_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tenants
    ADD CONSTRAINT tenants_org_id_fkey FOREIGN KEY (org_id) REFERENCES public.organizations(id);


--
-- Name: user_roles user_roles_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_roles
    ADD CONSTRAINT user_roles_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.platform_users(id);


--
-- Name: vm_file_index vm_file_index_snapshot_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.vm_file_index
    ADD CONSTRAINT vm_file_index_snapshot_id_fkey FOREIGN KEY (snapshot_id) REFERENCES public.snapshots(id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--


