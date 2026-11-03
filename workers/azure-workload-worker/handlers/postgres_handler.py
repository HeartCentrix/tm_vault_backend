"""Azure PostgreSQL Backup Handler (2026).

Supports both:
- Flexible Server (2026 standard): Microsoft.DBforPostgreSQL/flexibleServers
- Single Server (deprecated): Microsoft.DBforPostgreSQL/servers

Both use Azure AD authentication via Service Principal — no stored passwords.
Streaming table exports to plain .sql files (viewable in Azure Portal).

Files stored UNCOMPRESSED (.sql) so they can be viewed directly
in Azure Portal's Blob Storage viewer.
"""
import asyncio
import asyncpg
import time
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from azure.mgmt.rdbms.postgresql_flexibleservers.aio import PostgreSQLManagementClient
from azure.mgmt.rdbms.postgresql.aio import PostgreSQLManagementClient as SingleServerClient
from azure.identity.aio import ClientSecretCredential

from shared.models import Resource, Tenant, Snapshot, SnapshotItem
from shared.database import async_session_factory
import hashlib as _hashlib
import uuid as _uuid
from shared.azure_storage import azure_storage_manager
from shared.config import settings
from handlers._progress import update_job_pct


# Timestamp column names to detect for incremental backup
TIMESTAMP_COLUMNS = [
    "updated_at", "updatedat", "updateddate",
    "modified_at", "modifiedat", "modifieddate",
    "last_modified", "lastmodified",
]

# Azure AD token scope for PostgreSQL
# Flexible Server: https://ossrdbms-aad.database.windows.net/.default
# Single Server: same scope but different connection format
PG_AAD_SCOPE = "https://ossrdbms-aad.database.windows.net/.default"


class PostgresBackupHandler:
    """Backup Azure PostgreSQL servers via Afi-style streaming export."""

    def __init__(self, worker_id: str = "azure-pg-worker"):
        self.worker_id = worker_id

    def _log(self, message: str, level: str = "INFO"):
        prefix = f"[{self.worker_id}] [PG]"
        print(f"{prefix} {message}")

    def _get_server_type(self, resource: Resource) -> str:
        """
        Detect PostgreSQL server type from resource type.

        2026 Standard:
        - AZURE_POSTGRESQL = Flexible Server (primary)
        - AZURE_POSTGRESQL_SINGLE = Single Server (deprecated, legacy)
        """
        rtype = resource.type.value if hasattr(resource.type, 'value') else str(resource.type)
        if rtype in ("AZURE_POSTGRESQL_SINGLE", "AZURE_PG_SINGLE"):
            return "SINGLE"
        return "FLEXIBLE"

    async def backup(self, resource: Resource, tenant: Tenant,
                     snapshot: Snapshot, msg: Dict) -> Dict:
        """
        Afi-style PostgreSQL backup:
        - Phase 1: Connect and stream tables to .sql files
        - Phase 2: Capture schema (CREATE TABLE statements)
        - Phase 3: Capture server configuration
        """
        server_type = self._get_server_type(resource)
        server_name = resource.extra_data.get("server_name", "")
        primary_db = resource.extra_data.get("database_name", resource.external_id)
        rg = resource.azure_resource_group or ""
        sub_id = resource.azure_subscription_id

        if not server_name:
            raise ValueError(f"No server_name found in resource {resource.external_id}")

        # Phase 0 — enumerate every database on the server (postgres,
        # template1, plus whatever the user created). Fall back to the
        # resource's single database_name if enumeration fails for any
        # reason so we never lose the scoped db.
        #
        # ARM's list_by_server omits `template1` on Flexible Server, so
        # after we get the ARM list we also connect to `postgres` and
        # query pg_database directly. Anything we find there that ARM
        # missed gets appended.
        all_dbs = await self._list_server_databases(server_name, sub_id, rg, primary_db)
        try:
            probe = await self._get_azure_ad_connection(server_name, "postgres", server_type, resource)
            try:
                rows = await probe.fetch(
                    "SELECT datname FROM pg_database "
                    "WHERE datname NOT IN ('azure_sys','azure_maintenance') "
                    "ORDER BY datname"
                )
                for r in rows:
                    name = r["datname"]
                    if name and name not in all_dbs:
                        all_dbs.append(name)
            finally:
                await probe.close()
        except Exception as e:
            self._log(f"WARN pg_database fallback failed: {e}", "WARN")

        self._log(f"Starting PostgreSQL {server_type} backup on server {server_name} (RG={rg}) — {len(all_dbs)} database(s): {', '.join(all_dbs)}")

        # Extract the job id the worker handed us so we can write live
        # progress to jobs.progress_pct during the multi-database loop.
        job_id = None
        try:
            jid = (msg or {}).get("jobId")
            if jid:
                job_id = _uuid.UUID(jid)
        except Exception:
            job_id = None

        await update_job_pct(job_id, 10)  # enumerated databases

        try:
            # Phase 3 — capture server-level configuration once (no DB
            # scope). Done up front because it doesn't need a live
            # connection and it's cheap.
            config_result = await self._capture_configuration(
                server_name, sub_id, rg, snapshot, resource, server_type
            )

            # Per-database phases — stream tables + capture schema for
            # each database on the server. Accumulated rollups are
            # handed to the persister below.
            per_db_results = {}
            total_tables = 0
            total_rows = 0
            total_data_bytes = 0

            for db_name in all_dbs:
                conn = None
                try:
                    conn = await self._get_azure_ad_connection(server_name, db_name, server_type, resource)
                    data_result = await self._stream_tables(
                        resource, tenant, snapshot, conn, server_name, db_name
                    )
                    schema_result = await self._capture_schema(
                        conn, server_name, db_name, snapshot, resource
                    )
                except Exception as db_exc:
                    # A bad permission / unreachable db shouldn't fail
                    # the whole server backup — log and continue with
                    # an empty placeholder so the DB still shows up in
                    # the left rail.
                    self._log(f"WARN database {db_name} capture failed: {db_exc}", "WARN")
                    data_result = {"tables_count": 0, "rows_count": 0, "total_bytes": 0, "tables": []}
                    schema_result = {"blob_path": "", "size_bytes": 0}
                finally:
                    try:
                        if conn:
                            await conn.close()
                    except Exception:
                        pass

                per_db_results[db_name] = {"data": data_result, "schema": schema_result}
                total_tables += data_result.get("tables_count", 0)
                total_rows += data_result.get("rows_count", 0)
                total_data_bytes += data_result.get("total_bytes", 0)

                # Live progress: 10% after enumerate, +80% spread across
                # databases, remaining 10% for persist/finalize.
                done = len(per_db_results)
                await update_job_pct(job_id, 10 + int(80 * done / max(1, len(all_dbs))))

            # Persist one batch of SnapshotItem rows covering every DB.
            try:
                await self._persist_snapshot_items_multi(
                    resource=resource, tenant=tenant, snapshot=snapshot,
                    server_name=server_name, server_type=server_type,
                    per_db_results=per_db_results, config_result=config_result,
                )
            except Exception as pe:
                self._log(f"WARN snapshot_item persist failed: {pe}", "WARN")

            # Finalize
            total_size = total_data_bytes + sum(r["schema"].get("size_bytes", 0) for r in per_db_results.values()) + config_result.get("size_bytes", 0)

            # Record last backup time on RESOURCE for incremental
            resource.extra_data = {
                **(resource.extra_data or {}),
                "last_full_backup_at": datetime.now(timezone.utc).isoformat(),
                "last_backup_rows": total_rows,
                "databases_captured": list(per_db_results.keys()),
            }

            # Set blob_path for recovery panel — keep pointing at the
            # primary db dir for back-compat.
            snapshot.blob_path = f"{server_name}/{primary_db}/{snapshot.id.hex[:12]}/"

            # Per-database blob locations so the restore handler can
            # look up the right schema tree and data folder for whichever
            # source DB the user picks. Without these the restorer falls
            # back to "primary_db" and can't restore any other DB the
            # snapshot covers. schema_prefixes points at the FOLDER of
            # per-object schema files (the old schema_blobs key held a
            # single file and made restores rebuild one table).
            schema_prefixes = {
                db: r["schema"].get("prefix", "")
                for db, r in per_db_results.items()
                if r["schema"].get("prefix")
            }
            data_prefixes = {
                db: f"{server_name}/{db}/{snapshot.id.hex[:12]}/"
                for db in per_db_results.keys()
            }

            snapshot.extra_data = {
                **(snapshot.extra_data or {}),
                "mode": "AFI_STREAMING",
                "server_type": server_type,
                "total_size_bytes": total_size,
                "tables_exported": total_tables,
                "rows_exported": total_rows,
                "databases_count": len(per_db_results),
                "databases": list(per_db_results.keys()),
                "config_blob": config_result.get("blob_path", ""),
                "schema_prefixes": schema_prefixes,
                "data_prefixes": data_prefixes,
                "server_name": server_name,
                "database_name": primary_db,
                "resource_group": rg,
                "subscription_id": sub_id,
                "auth_method": "AzureADServicePrincipal",
                "backup_timestamp": datetime.now(timezone.utc).isoformat(),
                "last_full_backup_at": datetime.now(timezone.utc).isoformat(),
            }

            size_str = self._format_size(total_size)

            self._log(
                f"PostgreSQL {server_type} backup completed: {len(per_db_results)} db(s), "
                f"{total_tables} tables, {total_rows} rows, {size_str} total"
            )

            return {
                "success": True,
                "mode": "AFI_STREAMING",
                "server_type": server_type,
                "tables_exported": total_tables,
                "rows_exported": total_rows,
                "total_size_bytes": total_size,
                "auth_method": "AzureADServicePrincipal",
            }

        except Exception as e:
            self._log(f"PostgreSQL backup FAILED for {server_name}: {e}", "ERROR")
            raise

    async def _get_azure_ad_connection(self, server_name: str, db_name: str, server_type: str, resource: Resource):
        """
        Connect to PostgreSQL using Azure AD authentication (no stored passwords).

        Uses our Service Principal to get an Azure AD access token for PostgreSQL.

        Flexible Server (2026 standard):
        - User: `azure_ad_admin` (special username for Azure AD auth)
        - Password: Azure AD access token

        Single Server (deprecated):
        - User: `username@servername`
        - Password: Azure AD access token
        """
        sp_client_id = settings.MICROSOFT_CLIENT_ID or settings.AZURE_AD_CLIENT_ID
        sp_client_secret = settings.MICROSOFT_CLIENT_SECRET or settings.AZURE_AD_CLIENT_SECRET
        sp_tenant_id = settings.MICROSOFT_TENANT_ID or settings.AZURE_AD_TENANT_ID

        if not sp_client_id or not sp_client_secret or not sp_tenant_id:
            raise ValueError(
                f"SP credentials not configured. "
                f"Set MICROSOFT_CLIENT_ID/SECRET/TENANT_ID or AZURE_AD_CLIENT_ID/SECRET/TENANT_ID."
            )

        # Get Azure AD access token for PostgreSQL
        credential = ClientSecretCredential(
            client_id=sp_client_id,
            client_secret=sp_client_secret,
            tenant_id=sp_tenant_id,
        )

        try:
            token_result = await credential.get_token(PG_AAD_SCOPE)
            access_token = token_result.token
        except Exception as e:
            raise RuntimeError(
                f"Failed to get Azure AD token for PostgreSQL: {e}. "
                f"Ensure the SP is registered as Azure AD admin of this PostgreSQL server."
            )
        finally:
            await credential.close()

        # Build connection parameters based on server type
        host = f"{server_name}.postgres.database.azure.com"

        if server_type == "FLEXIBLE":
            # Flexible Server: Azure AD auth uses the SP's client ID as username
            # The SP must be registered as Azure AD admin via ARM API during discovery
            user = sp_client_id
        else:
            # Single Server (deprecated): need original admin user + server suffix
            admin_user = resource.extra_data.get("admin_user", "pgadmin")
            user = f"{admin_user}@{server_name}"

        self._log(f"Connecting to {host}/{db_name} using Azure AD auth ({server_type})")

        conn = await asyncpg.connect(
            host=host,
            port=5432,
            user=user,
            password=access_token,
            database=db_name,
            ssl="require",
            timeout=30.0,
        )

        self._log(f"Connected to PostgreSQL {server_type}: {db_name}")
        return conn

    async def _stream_tables(self, resource: Resource, tenant: Tenant,
                              snapshot: Snapshot, conn, server_name: str,
                              db_name: str) -> Dict:
        """
        Stream data row-by-row from all tables into per-table .sql files.

        Incremental support:
        - Checks for timestamp columns (updated_at, modified_at, etc.)
        - Filters rows WHERE timestamp_col >= last_backup_time
        - Falls back to full export if no timestamp column found
        """
        total_bytes = 0
        total_rows = 0
        tables_count = 0
        # Per-table summary for SnapshotItem persistence (populated below).
        table_details: list = []

        # Get incremental watermark
        last_backup_time = resource.last_backup_at
        if last_backup_time and last_backup_time.tzinfo is None:
            last_backup_time = last_backup_time.replace(tzinfo=timezone.utc)

        # Discover all tables
        tables = await conn.fetch("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
              AND table_schema NOT IN ('pg_catalog', 'information_schema')
              AND table_schema !~ '^pg_toast'
            ORDER BY table_schema, table_name
        """)

        self._log(f"  Phase 1: Found {len(tables)} tables in {db_name}")

        if last_backup_time:
            self._log(f"  Incremental backup since {last_backup_time}")
        else:
            self._log(f"  Full backup (no previous backup recorded)")

        # Export each table
        for table_row in tables:
            schema = table_row["table_schema"]
            table = table_row["table_name"]
            full_table = f'"{schema}"."{table}"'

            start = time.monotonic()

            # Check for incremental timestamp column
            timestamp_col = None
            if last_backup_time:
                timestamp_col = await self._find_timestamp_column(conn, schema, table)

            # Delta slice — goes into the .sql dump for ops use.
            if timestamp_col:
                delta_query = f'SELECT * FROM {full_table} WHERE "{timestamp_col}" >= $1 ORDER BY "{timestamp_col}"'
                rows = await conn.fetch(delta_query, last_backup_time)
                self._log(f"    Incremental .sql export for {full_table} via [{timestamp_col}]")
            else:
                rows = await conn.fetch(f"SELECT * FROM {full_table}")

            # Full snapshot for the JSON blob — the Database tab in
            # Recovery reads this to render rows, and it needs the
            # whole table at the time of this snapshot, not just the
            # delta since the prior run. Always re-fetch the full set.
            if timestamp_col:
                rows_full = await conn.fetch(f"SELECT * FROM {full_table}")
            else:
                rows_full = rows

            # Generate INSERT statements as plain .sql
            sql_lines = [f"-- Table: {full_table}"]
            sql_lines.append(f"-- Exported at: {datetime.now(timezone.utc).isoformat()}")
            sql_lines.append(f"-- Rows: {len(rows)}")
            sql_lines.append("")

            if rows:
                columns = list(rows[0].keys())
                col_list = ", ".join(f'"{c}"' for c in columns)

                for row in rows:
                    values = []
                    for col in columns:
                        val = row[col]
                        if val is None:
                            values.append("NULL")
                        elif isinstance(val, (int, float)):
                            values.append(str(val))
                        elif isinstance(val, datetime):
                            values.append(f"'{val.isoformat()}'")
                        elif isinstance(val, bool):
                            values.append("TRUE" if val else "FALSE")
                        else:
                            escaped = str(val).replace("'", "''")
                            values.append(f"'{escaped}'")

                    sql_lines.append(f"INSERT INTO {full_table} ({col_list}) VALUES ({', '.join(values)});")

            sql_content = "\n".join(sql_lines) + "\n"
            sql_bytes = len(sql_content.encode("utf-8"))

            # Upload to blob — SQL dump for download + JSON rows for the
            # Database tab's paginated/search view. Two separate blobs
            # so the SQL file stays intact for ops use while the JSON
            # one is query-friendly.
            blob_name = f"{schema}_{table}.sql"
            blob_path = f"{server_name}/{db_name}/{snapshot.id.hex[:12]}/data/{blob_name}"

            import json as _json
            columns_list: list = []
            if rows_full:
                columns_list = list(rows_full[0].keys())
            rows_json_obj = {
                "schema": schema,
                "table": table,
                "columns": columns_list,
                "rows": [
                    {k: (v.isoformat() if isinstance(v, datetime) else v)
                     for k, v in dict(r).items()}
                    for r in rows_full
                ],
            }
            rows_json = _json.dumps(rows_json_obj, default=str)
            rows_json_bytes = rows_json.encode("utf-8")
            rows_blob_path = f"{server_name}/{db_name}/{snapshot.id.hex[:12]}/data/{schema}_{table}.json"

            # Route uploads through the storage-router facade so this
            # handler honours the active storage backend (Azure Blob
            # or on-prem SeaweedFS). The facade returns a dict with
            # success=False on failure; we raise explicitly so an
            # upload failure can't silently leave the snapshot
            # half-committed — critical at enterprise scale where
            # 5k users × 400 TiB of data means a silent drop is a
            # genuine data-loss event, not a nuisance.
            shard = azure_storage_manager.get_default_shard()
            container = azure_storage_manager.get_container_name(str(tenant.id), "azure-postgresql")
            await shard.ensure_container(container)

            sql_res = await shard.upload_blob(
                container, blob_path, sql_content.encode("utf-8"),
                overwrite=True,
            )
            if not sql_res.get("success"):
                raise RuntimeError(
                    f"SQL dump upload failed for {blob_path}: {sql_res.get('error')}"
                )
            rows_res = await shard.upload_blob(
                container, rows_blob_path, rows_json_bytes,
                overwrite=True,
            )
            if not rows_res.get("success"):
                raise RuntimeError(
                    f"Rows JSON upload failed for {rows_blob_path}: {rows_res.get('error')}"
                )

            elapsed = time.monotonic() - start
            tables_count += 1
            total_rows += len(rows)
            total_bytes += sql_bytes
            table_details.append({
                "schema": schema,
                "table": table,
                "blob_path": blob_path,
                "rows_blob_path": rows_blob_path,
                "columns": columns_list,
                "size_bytes": sql_bytes,
                "row_count": len(rows),
                "incremental": bool(timestamp_col),
            })

            size_str = self._format_size(sql_bytes)
            self._log(f"  ✓ Table {full_table}: {len(rows)} rows, {size_str} in {elapsed:.1f}s")

        return {
            "tables_count": tables_count,
            "rows_count": total_rows,
            "total_bytes": total_bytes,
            "tables": table_details,
        }

    async def _find_timestamp_column(self, conn, schema: str, table: str) -> Optional[str]:
        """Find a timestamp column suitable for incremental filtering."""
        cols = await conn.fetch("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
              AND data_type IN ('timestamp with time zone', 'timestamp without time zone', 'date')
            ORDER BY
                CASE column_name
                    WHEN 'updated_at' THEN 1
                    WHEN 'updatedat' THEN 2
                    WHEN 'updateddate' THEN 3
                    WHEN 'modified_at' THEN 4
                    WHEN 'modifiedat' THEN 5
                    WHEN 'last_modified' THEN 6
                    WHEN 'lastmodified' THEN 7
                    WHEN 'created_at' THEN 8
                    ELSE 99
                END
        """, schema, table)

        for col in cols:
            col_name = col["column_name"].lower()
            if col_name in [c.lower() for c in TIMESTAMP_COLUMNS]:
                return col["column_name"]

        return None

    async def _capture_schema(self, conn, server_name: str, db_name: str,
                               snapshot: Snapshot, resource: Resource) -> Dict:
        """Capture schema artifacts as a multi-level folder tree (AFI-parity).

        Produces (relative to <db_name>):

          sql/Other/grants          — GRANT / role assignments text
          sql/Other/schemas         — CREATE SCHEMA statements
          sql/<schema>/Tables/<t>   — CREATE TABLE per table (one file each)
          sql/<schema>/Sequences/<s>— CREATE SEQUENCE per sequence
          sql/<schema>/Views/<v>    — CREATE VIEW per view (when present)

        Returns {"files": [...], "size_bytes": total}. Each file has
        name, folder_path (relative to db, using forward slashes),
        blob_path, size_bytes. The frontend renders this verbatim as a
        tree.
        """
        shard = azure_storage_manager.get_default_shard()
        container = azure_storage_manager.get_container_name(str(resource.tenant_id), "azure-postgresql")
        await shard.ensure_container(container)

        snap_dir = f"{server_name}/{db_name}/{snapshot.id.hex[:12]}/schema"
        files: list = []
        total_bytes = 0

        async def _upload(rel_path: str, content: str) -> tuple:
            blob_path = f"{snap_dir}/{rel_path}"
            data = content.encode("utf-8") if isinstance(content, str) else content
            # Facade path — honours the storage toggle. Raise on
            # non-success so a failed per-file upload aborts the
            # enclosing snapshot instead of silently missing bytes.
            res = await shard.upload_blob(
                container, blob_path, data, overwrite=True,
            )
            if not res.get("success"):
                raise RuntimeError(
                    f"schema upload failed for {blob_path}: {res.get('error')}"
                )
            return blob_path, len(data)

        def _add(name: str, folder_path: str, blob_path: str, size: int):
            files.append({
                "name": name, "folder_path": folder_path,
                "blob_path": blob_path, "size_bytes": size,
            })

        # ── Tables / columns / sequences / views — one pg query each.
        table_rows = await conn.fetch("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
              AND table_schema NOT IN ('pg_catalog', 'information_schema')
              AND table_schema !~ '^pg_toast'
            ORDER BY table_schema, table_name
        """)

        async def _build_create_table(schema: str, table: str) -> str:
            columns = await conn.fetch("""
                SELECT column_name, data_type, character_maximum_length,
                       is_nullable, column_default, numeric_precision, numeric_scale
                FROM information_schema.columns
                WHERE table_schema = $1 AND table_name = $2
                ORDER BY ordinal_position
            """, schema, table)
            lines = [f"-- Table: {schema}.{table}",
                     f"CREATE TABLE IF NOT EXISTS \"{schema}\".\"{table}\" ("]
            col_defs = []
            for col in columns:
                dtype = col["data_type"].upper()
                if col["character_maximum_length"]:
                    dtype += f"({col['character_maximum_length']})"
                elif col["numeric_precision"]:
                    if col["numeric_scale"]:
                        dtype += f"({col['numeric_precision']},{col['numeric_scale']})"
                    else:
                        dtype += f"({col['numeric_precision']})"
                nullable = "" if col["is_nullable"] == "NO" else " NULL"
                default = f" DEFAULT {col['column_default']}" if col["column_default"] else ""
                col_defs.append(f'    "{col["column_name"]}" {dtype}{nullable}{default}')
            lines.append(",\n".join(col_defs))
            lines.append(");\n")
            return "\n".join(lines)

        for r in table_rows:
            schema, table = r["table_schema"], r["table_name"]
            content = await _build_create_table(schema, table)
            bp, sz = await _upload(f"sql/{schema}/Tables/{table}", content)
            _add(table, f"sql/{schema}/Tables", bp, sz)
            total_bytes += sz

        # Sequences — excluding pg_catalog.
        seq_rows = await conn.fetch("""
            SELECT sequence_schema, sequence_name, data_type,
                   start_value, minimum_value, maximum_value, increment
            FROM information_schema.sequences
            WHERE sequence_schema NOT IN ('pg_catalog', 'information_schema')
            ORDER BY sequence_schema, sequence_name
        """)
        for r in seq_rows:
            schema, seq = r["sequence_schema"], r["sequence_name"]
            content = (
                f"-- Sequence: {schema}.{seq}\n"
                f"CREATE SEQUENCE IF NOT EXISTS \"{schema}\".\"{seq}\"\n"
                f"    AS {r['data_type']}\n"
                f"    INCREMENT {r['increment']}\n"
                f"    MINVALUE {r['minimum_value']}\n"
                f"    MAXVALUE {r['maximum_value']}\n"
                f"    START {r['start_value']};\n"
            )
            bp, sz = await _upload(f"sql/{schema}/Sequences/{seq}", content)
            _add(seq, f"sql/{schema}/Sequences", bp, sz)
            total_bytes += sz

        # Views — CREATE VIEW statements.
        view_rows = await conn.fetch("""
            SELECT table_schema, table_name, view_definition
            FROM information_schema.views
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
            ORDER BY table_schema, table_name
        """)
        for r in view_rows:
            schema, view = r["table_schema"], r["table_name"]
            content = (
                f"-- View: {schema}.{view}\n"
                f"CREATE OR REPLACE VIEW \"{schema}\".\"{view}\" AS\n"
                f"{r['view_definition']}\n"
            )
            bp, sz = await _upload(f"sql/{schema}/Views/{view}", content)
            _add(view, f"sql/{schema}/Views", bp, sz)
            total_bytes += sz

        # ── Other/ — database-wide metadata: schemas + role grants.
        schemas_rows = await conn.fetch("""
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
              AND schema_name NOT LIKE 'pg_temp_%'
              AND schema_name NOT LIKE 'pg_toast_temp_%'
            ORDER BY schema_name
        """)
        schemas_content_lines = [
            f"-- Schemas in database: {db_name}",
            f"-- Exported at: {datetime.now(timezone.utc).isoformat()}",
            "",
        ]
        for r in schemas_rows:
            schemas_content_lines.append(f"CREATE SCHEMA IF NOT EXISTS \"{r['schema_name']}\";")
        schemas_content = "\n".join(schemas_content_lines) + "\n"
        bp, sz = await _upload("sql/Other/schemas", schemas_content)
        _add("schemas", "sql/Other", bp, sz)
        total_bytes += sz

        grants_rows = await conn.fetch("""
            SELECT grantee, table_schema, table_name, privilege_type
            FROM information_schema.table_privileges
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
              AND grantee NOT IN ('PUBLIC')
            ORDER BY table_schema, table_name, grantee, privilege_type
        """)
        grants_content_lines = [
            f"-- Grants in database: {db_name}",
            f"-- Exported at: {datetime.now(timezone.utc).isoformat()}",
            "",
        ]
        for r in grants_rows:
            grants_content_lines.append(
                f'GRANT {r["privilege_type"]} ON "{r["table_schema"]}"."{r["table_name"]}" '
                f'TO "{r["grantee"]}";'
            )
        grants_content = "\n".join(grants_content_lines) + "\n"
        bp, sz = await _upload("sql/Other/grants", grants_content)
        _add("grants", "sql/Other", bp, sz)
        total_bytes += sz

        self._log(
            f"  Phase 2: Schema captured: {len(table_rows)} tables, "
            f"{len(seq_rows)} sequences, {len(view_rows)} views, "
            f"{len(files)} files, {self._format_size(total_bytes)}"
        )

        return {
            "files": files,
            "size_bytes": total_bytes,
            # Folder prefix of the whole schema tree — restore replays
            # every file under it. Never expose a single file here:
            # storing files[0] as "the schema" made FULL restores
            # rebuild exactly one table.
            "prefix": f"{snap_dir}/" if files else "",
        }

    async def _capture_configuration(self, server_name: str, sub_id: str,
                                      rg: str, snapshot: Snapshot,
                                      resource: Resource, server_type: str) -> Dict:
        """Capture Azure PostgreSQL server configuration via ARM API."""
        try:
            credential = ClientSecretCredential(
                client_id=settings.EFFECTIVE_ARM_CLIENT_ID,
                client_secret=settings.EFFECTIVE_ARM_CLIENT_SECRET,
                tenant_id=settings.EFFECTIVE_ARM_TENANT_ID,
            )

            if server_type == "FLEXIBLE":
                pg_client = PostgreSQLManagementClient(credential, sub_id)
                server = await pg_client.servers.get(
                    resource_group_name=rg,
                    server_name=server_name,
                )
                storage_mb = getattr(getattr(server, "storage", None), "storage_size_gb", None)
                if storage_mb is None:
                    storage_mb = getattr(getattr(server, "storage", None), "storage_size_mb", 0)

                # Time created — try in order:
                #   1) server.create_date (legacy SDK attribute)
                #   2) server.system_data.created_at (newer SDK versions)
                #   3) as_dict()["systemData"]["createdAt"] — Azure ARM
                #      always returns this field in the raw JSON even when
                #      the SDK doesn't expose it as a typed attribute.
                #      This is the one that actually resolves on Flexible
                #      Server today.
                #   4) tags["createdAt"] — customer-set fallback.
                time_created = ""
                cd = getattr(server, "create_date", None)
                if cd:
                    try:
                        time_created = cd.isoformat()
                    except Exception:
                        time_created = str(cd)
                if not time_created:
                    sys_data = getattr(server, "system_data", None)
                    sd_created = getattr(sys_data, "created_at", None) if sys_data else None
                    if sd_created:
                        try:
                            time_created = sd_created.isoformat()
                        except Exception:
                            time_created = str(sd_created)
                if not time_created:
                    try:
                        raw_dict = server.as_dict() if hasattr(server, "as_dict") else {}
                        sd = (raw_dict.get("systemData") or raw_dict.get("system_data") or {})
                        time_created = sd.get("createdAt") or sd.get("created_at") or ""
                    except Exception:
                        pass
                if not time_created:
                    tags = getattr(server, "tags", None) or {}
                    time_created = tags.get("createdAt") or tags.get("created_at") or ""
                # Final fallback — direct ARM REST call with a recent
                # api-version that always surfaces systemData.createdAt.
                # The typed SDK response strips it on older API versions,
                # so bypass it entirely for this one field.
                if not time_created:
                    try:
                        import httpx as _httpx
                        tok = await credential.get_token("https://management.azure.com/.default")
                        arm_url = (
                            f"https://management.azure.com/subscriptions/{sub_id}"
                            f"/resourceGroups/{rg}/providers/Microsoft.DBforPostgreSQL"
                            f"/flexibleServers/{server_name}?api-version=2024-08-01"
                        )
                        async with _httpx.AsyncClient(timeout=15.0) as _c:
                            _r = await _c.get(arm_url, headers={"Authorization": f"Bearer {tok.token}"})
                            if _r.status_code == 200:
                                sd = (_r.json().get("systemData") or {})
                                time_created = sd.get("createdAt") or sd.get("created_at") or ""
                    except Exception as tce:
                        self._log(f"WARN systemData.createdAt REST fallback failed: {tce}", "WARN")

                # Firewall rules — the Azure screenshots show the count
                # ("1 firewall rules") rather than the full list. One
                # extra ARM call gives us the count without bloating
                # the captured config blob.
                firewall_rule_count = 0
                try:
                    fw_iter = pg_client.firewall_rules.list_by_server(
                        resource_group_name=rg, server_name=server_name,
                    )
                    async for _ in fw_iter:
                        firewall_rule_count += 1
                except Exception as fe:
                    self._log(f"WARN firewall rules fetch failed: {fe}", "WARN")

                config = {
                    "server_type": "FLEXIBLE",
                    "server_name": server_name,
                    "resource_group": rg,
                    "subscription_id": sub_id,
                    "location": getattr(server, "location", "unknown"),
                    "fully_qualified_domain_name": getattr(server, "fully_qualified_domain_name", "") or "",
                    "administrator_login": getattr(server, "administrator_login", "") or "",
                    "availability_zone": getattr(server, "availability_zone", "") or "",
                    "time_created": time_created,
                    "sku": {
                        "name": getattr(getattr(server, "sku", None), "name", ""),
                        "tier": getattr(getattr(server, "sku", None), "tier", ""),
                    },
                    "version": getattr(server, "version", "unknown"),
                    "state": getattr(server, "state", "unknown"),
                    "storage": {
                        "storageSizeGB": storage_mb,
                        "autoGrow": getattr(getattr(server, "storage", None), "auto_grow", "Disabled"),
                    },
                    "backup": {
                        "backupRetentionDays": getattr(getattr(server, "backup", None), "backup_retention_days", 7),
                        "geoRedundantBackup": getattr(getattr(server, "backup", None), "geo_redundant_backup", "Disabled"),
                    },
                    "network": {
                        "publicNetworkAccess": getattr(getattr(server, "network", None), "public_network_access", "Enabled"),
                        "delegatedSubnetResourceId": getattr(getattr(server, "network", None), "delegated_subnet_resource_id", None),
                    },
                    "firewallRuleCount": firewall_rule_count,
                    "highAvailability": {
                        "mode": getattr(getattr(server, "high_availability", None), "mode", "Disabled"),
                    },
                    "replicationRole": getattr(server, "replication_role", "None"),
                }
            else:
                # Single Server (deprecated)
                pg_client = SingleServerClient(credential, sub_id)
                server = await pg_client.servers.get(
                    resource_group_name=rg,
                    server_name=server_name,
                )
                config = {
                    "server_type": "SINGLE (DEPRECATED)",
                    "server_name": server_name,
                    "resource_group": rg,
                    "subscription_id": sub_id,
                    "location": getattr(server, "location", "unknown"),
                    "sku": getattr(getattr(server, "sku", None), "name", "unknown"),
                    "version": getattr(server, "version", "unknown"),
                    "state": getattr(server, "user_visible_state", "unknown"),
                    "storage_mb": getattr(server, "storage_mb", 0),
                    "backup_retention_days": getattr(server, "backup_retention_days", 7),
                    "geo_redundant_backup": getattr(server, "geo_redundant_backup", "Disabled"),
                    "ssl_enforcement": getattr(server, "ssl_enforcement", "Unknown"),
                }

            config["captured_at"] = datetime.now(timezone.utc).isoformat()

            await pg_client.close()
            await credential.close()

        except Exception as e:
            config = {
                "server_type": server_type,
                "server_name": server_name,
                "resource_group": rg,
                "error": str(e),
                "captured_at": datetime.now(timezone.utc).isoformat(),
            }

        config_content = f"-- PostgreSQL Server Configuration ({server_type})\n-- Captured: {config['captured_at']}\n\n"
        config_content += "-- Server Metadata:\n"
        for key, value in config.items():
            config_content += f"--   {key}: {value}\n"

        config_bytes = len(config_content.encode("utf-8"))

        shard = azure_storage_manager.get_default_shard()
        container = azure_storage_manager.get_container_name(str(resource.tenant_id), "azure-postgresql")
        await shard.ensure_container(container)

        db_name = resource.extra_data.get("database_name", resource.external_id)
        config_blob_path = f"{server_name}/{db_name}/{snapshot.id.hex[:12]}/config/{server_name}-config.json"

        # Facade upload — honours active storage backend. Raise on
        # non-success so we don't stamp a snapshot as COMPLETED with
        # a missing config blob.
        cfg_res = await shard.upload_blob(
            container, config_blob_path,
            config_content.encode("utf-8"), overwrite=True,
        )
        if not cfg_res.get("success"):
            raise RuntimeError(
                f"config upload failed for {config_blob_path}: {cfg_res.get('error')}"
            )

        self._log(f"  Phase 3: Configuration captured: {self._format_size(config_bytes)}")

        return {"blob_path": config_blob_path, "size_bytes": config_bytes, "config": config}

    async def _persist_snapshot_items(
        self, *, resource: Resource, tenant: Tenant, snapshot: Snapshot,
        server_name: str, db_name: str, server_type: str,
        data_result: Dict, schema_result: Dict, config_result: Dict,
    ) -> None:
        """Insert SnapshotItem rows for the Recovery UI.

        Shape matches what AzureDbView expects:
          • 1 × AZURE_DB_CONFIG    — server metadata JSON (detail card).
          • 1 × AZURE_DB_DATABASE  — the database itself (left rails).
          • 1 × AZURE_DB_SCHEMA_FILE — the schema.sql dump file.
          • N × AZURE_DB_TABLE     — one per captured table (Data left rail).

        Each row carries blob_path so the existing
        /snapshots/{id}/items/{id}/content endpoint can stream the bytes
        back on download without handler-side special casing.
        """
        now_hash = _hashlib.sha256(f"{snapshot.id}".encode()).hexdigest()
        rows: list = []

        # AZURE_DB_CONFIG — the rich per-row metadata powers the
        # Configuration tab's detail card directly.
        cfg = config_result.get("config") or {}
        rows.append(SnapshotItem(
            id=_uuid.uuid4(),
            snapshot_id=snapshot.id,
            tenant_id=tenant.id,
            external_id=f"{server_name}:{db_name}:config",
            item_type="AZURE_DB_CONFIG",
            name=f"{server_name}-config.json",
            folder_path="",
            content_size=int(config_result.get("size_bytes") or 0),
            content_hash=now_hash,
            content_checksum=None,
            blob_path=config_result.get("blob_path"),
            extra_data={
                "raw": cfg,
                "server_name": server_name,
                "database_name": db_name,
                "server_type": server_type,
            },
        ))

        # AZURE_DB_DATABASE — one per database. Each AZURE_POSTGRESQL
        # resource today maps to a single database (resource.external_id
        # includes `server/db`), so emit just this one. When discovery
        # expands to enumerate every database on the server we'll loop
        # here instead.
        rows.append(SnapshotItem(
            id=_uuid.uuid4(),
            snapshot_id=snapshot.id,
            tenant_id=tenant.id,
            external_id=f"{server_name}:{db_name}",
            item_type="AZURE_DB_DATABASE",
            name=db_name,
            folder_path="",
            content_size=0,
            content_hash=None,
            content_checksum=None,
            blob_path=None,
            extra_data={
                "server_name": server_name,
                "database_name": db_name,
                "server_type": server_type,
            },
        ))

        # AZURE_DB_SCHEMA_FILE — schema dump lives under the db name in
        # the Schema tab's file list.
        if schema_result.get("blob_path"):
            schema_name = schema_result["blob_path"].rsplit("/", 1)[-1]
            rows.append(SnapshotItem(
                id=_uuid.uuid4(),
                snapshot_id=snapshot.id,
                tenant_id=tenant.id,
                external_id=f"{server_name}:{db_name}:schema",
                item_type="AZURE_DB_SCHEMA_FILE",
                name=schema_name or f"{db_name}-schema.sql",
                folder_path=db_name,
                content_size=int(schema_result.get("size_bytes") or 0),
                content_hash=None,
                content_checksum=None,
                blob_path=schema_result.get("blob_path"),
                extra_data={
                    "server_name": server_name,
                    "database_name": db_name,
                    "server_type": server_type,
                },
            ))

        # AZURE_DB_TABLE — one row per exported table. folder_path is
        # `<db_name>/<schema>` so Data tab left rail can nest tables
        # under schema when we have multiple schemas.
        for t in (data_result.get("tables") or []):
            schema = t.get("schema") or "public"
            table = t.get("table") or "(unnamed)"
            rows.append(SnapshotItem(
                id=_uuid.uuid4(),
                snapshot_id=snapshot.id,
                tenant_id=tenant.id,
                external_id=f"{server_name}:{db_name}:{schema}:{table}",
                item_type="AZURE_DB_TABLE",
                name=table,
                folder_path=f"{db_name}/{schema}",
                content_size=int(t.get("size_bytes") or 0),
                content_hash=None,
                content_checksum=None,
                blob_path=t.get("blob_path"),
                extra_data={
                    "server_name": server_name,
                    "database_name": db_name,
                    "schema": schema,
                    "table": table,
                    "row_count": int(t.get("row_count") or 0),
                    "incremental": bool(t.get("incremental")),
                },
            ))

        async with async_session_factory() as sess:
            sess.add_all(rows)
            await sess.commit()
        self._log(f"  Persisted {len(rows)} SnapshotItem rows for Recovery UI")

    async def _list_server_databases(self, server_name: str, sub_id: str, rg: str, primary_db: str) -> list:
        """Enumerate every database on a PostgreSQL Flexible Server via
        ARM. Includes the built-in ones (postgres, template1) plus any
        user-created dbs. Falls back to [primary_db] on error so a
        permission hiccup never reduces the backup to zero databases.
        """
        try:
            credential = ClientSecretCredential(
                client_id=settings.EFFECTIVE_ARM_CLIENT_ID,
                client_secret=settings.EFFECTIVE_ARM_CLIENT_SECRET,
                tenant_id=settings.EFFECTIVE_ARM_TENANT_ID,
            )
            names: list = []
            pg_client = PostgreSQLManagementClient(credential, sub_id)
            try:
                async for db in pg_client.databases.list_by_server(resource_group_name=rg, server_name=server_name):
                    if getattr(db, "name", None):
                        names.append(db.name)
            finally:
                await pg_client.close()
                await credential.close()
            # Make sure the primary db is in the list even if ARM is
            # lagging; dedupe while preserving order.
            if primary_db and primary_db not in names:
                names.append(primary_db)
            # Drop pgAzure internal dbs that don't matter for backup:
            #   azure_sys / azure_maintenance
            names = [n for n in names if n not in ("azure_sys", "azure_maintenance")]
            return names
        except Exception as e:
            self._log(f"WARN list_by_server failed, falling back to [{primary_db}]: {e}", "WARN")
            return [primary_db] if primary_db else []

    async def _persist_snapshot_items_multi(
        self, *, resource: Resource, tenant: Tenant, snapshot: Snapshot,
        server_name: str, server_type: str,
        per_db_results: Dict, config_result: Dict,
    ) -> None:
        """Multi-database version of _persist_snapshot_items.

        Emits:
          • 1 × AZURE_DB_CONFIG   — server-level (shared across all dbs).
          • N × AZURE_DB_DATABASE — one per database (postgres, template1, …).
          • N × AZURE_DB_SCHEMA_FILE — one per database's schema dump.
          • Σ × AZURE_DB_TABLE    — every captured table, folder_path=<db>/<schema>.
        """
        rows: list = []
        cfg = config_result.get("config") or {}

        rows.append(SnapshotItem(
            id=_uuid.uuid4(),
            snapshot_id=snapshot.id,
            tenant_id=tenant.id,
            external_id=f"{server_name}:config",
            item_type="AZURE_DB_CONFIG",
            name=f"{server_name}-config.json",
            folder_path="",
            content_size=int(config_result.get("size_bytes") or 0),
            content_hash=None,
            content_checksum=None,
            blob_path=config_result.get("blob_path"),
            extra_data={
                "raw": cfg,
                "server_name": server_name,
                "server_type": server_type,
            },
        ))

        for db_name, result in per_db_results.items():
            data_result = result.get("data") or {}
            schema_result = result.get("schema") or {}

            rows.append(SnapshotItem(
                id=_uuid.uuid4(),
                snapshot_id=snapshot.id,
                tenant_id=tenant.id,
                external_id=f"{server_name}:{db_name}",
                item_type="AZURE_DB_DATABASE",
                name=db_name,
                folder_path="",
                content_size=0,
                content_hash=None,
                content_checksum=None,
                blob_path=None,
                extra_data={
                    "server_name": server_name,
                    "database_name": db_name,
                    "server_type": server_type,
                    "tables_count": data_result.get("tables_count", 0),
                    "rows_count": data_result.get("rows_count", 0),
                },
            ))

            # Schema files — one row per captured schema artifact so the
            # Schema tab can render a folder tree. folder_path is
            # `<db_name>` for root-level files and `<db_name>/<sub>`
            # (e.g. `<db_name>/sql`) for files inside a subfolder.
            schema_files = schema_result.get("files") or []
            if not schema_files and schema_result.get("blob_path"):
                # Back-compat — old shape had a single blob_path.
                fallback_name = schema_result["blob_path"].rsplit("/", 1)[-1]
                schema_files = [{
                    "name": fallback_name or f"{db_name}-schema.sql",
                    "folder_path": "",
                    "blob_path": schema_result.get("blob_path"),
                    "size_bytes": schema_result.get("size_bytes") or 0,
                }]

            for sf in schema_files:
                rel_folder = (sf.get("folder_path") or "").strip("/")
                folder = f"{db_name}/{rel_folder}" if rel_folder else db_name
                fname = sf.get("name") or "schema.sql"
                rows.append(SnapshotItem(
                    id=_uuid.uuid4(),
                    snapshot_id=snapshot.id,
                    tenant_id=tenant.id,
                    external_id=f"{server_name}:{db_name}:schema:{folder}/{fname}",
                    item_type="AZURE_DB_SCHEMA_FILE",
                    name=fname,
                    folder_path=folder,
                    content_size=int(sf.get("size_bytes") or 0),
                    content_hash=None,
                    content_checksum=None,
                    blob_path=sf.get("blob_path"),
                    extra_data={
                        "server_name": server_name,
                        "database_name": db_name,
                        "server_type": server_type,
                    },
                ))

            for t in (data_result.get("tables") or []):
                schema = t.get("schema") or "public"
                table = t.get("table") or "(unnamed)"
                rows.append(SnapshotItem(
                    id=_uuid.uuid4(),
                    snapshot_id=snapshot.id,
                    tenant_id=tenant.id,
                    external_id=f"{server_name}:{db_name}:{schema}:{table}",
                    item_type="AZURE_DB_TABLE",
                    name=table,
                    folder_path=f"{db_name}/{schema}",
                    content_size=int(t.get("size_bytes") or 0),
                    content_hash=None,
                    content_checksum=None,
                    blob_path=t.get("blob_path"),
                    extra_data={
                        "server_name": server_name,
                        "database_name": db_name,
                        "schema": schema,
                        "table": table,
                        "row_count": int(t.get("row_count") or 0),
                        "columns": t.get("columns") or [],
                        "rows_blob_path": t.get("rows_blob_path"),
                    },
                ))

        async with async_session_factory() as sess:
            sess.add_all(rows)
            await sess.commit()
        self._log(f"  Persisted {len(rows)} SnapshotItem rows (across {len(per_db_results)} dbs) for Recovery UI")

    def _format_size(self, size_bytes: int) -> str:
        """Human-readable file size."""
        if size_bytes >= 1024 * 1024 * 1024:
            return f"{size_bytes / (1024**3):.2f} GB"
        elif size_bytes >= 1024 * 1024:
            return f"{size_bytes / (1024**2):.2f} MB"
        elif size_bytes >= 1024:
            return f"{size_bytes / 1024:.1f} KB"
        else:
            return f"{size_bytes} bytes"
