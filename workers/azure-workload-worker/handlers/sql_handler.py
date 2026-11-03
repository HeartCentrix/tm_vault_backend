"""
Azure SQL Database Backup Handler — Afi-Style (Passwordless via SP Entra Admin)

Architecture (as per db.md):
1. During discovery, our SP is auto-assigned as Entra admin of SQL server
2. For backup, we connect using our SP's credentials (Active Directory Service Principal)
3. We stream data row-by-row using pyodbc — no stored customer SQL passwords needed

This is exactly how Afi.ai handles Azure SQL backup — the customer never provides
SQL credentials; our own app's SP is the identity.
"""
import asyncio
import json
import tempfile
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional

import pyodbc
from azure.mgmt.sql.aio import SqlManagementClient
from azure.core.exceptions import HttpResponseError

from shared.models import Resource, Tenant, Snapshot, SnapshotItem
from shared.database import async_session_factory
from shared.azure_storage import azure_storage_manager
from shared.config import settings
import uuid as _uuid
from handlers._progress import update_job_pct
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from lro import await_lro
from arm_credentials import get_arm_credential


class SqlBackupHandler:
    """
    Afi-style Azure SQL backup handler.

    Connection flow:
    1. Discovery auto-assigns our SP as SQL Entra admin
    2. Backup connects using our SP via Active Directory Service Principal auth
    3. Data streamed row-by-row → per-table .sql.gz files in blob storage
    4. Schema captured as SQL scripts for restore
    5. Configuration captured as JSON
    """

    def __init__(self, worker_id: str = "azure-sql-worker"):
        self.worker_id = worker_id

    async def backup(self, resource: Resource, tenant: Tenant,
                     snapshot: Snapshot, msg: Dict) -> Dict:
        """
        Full Azure SQL backup pipeline.

        Pipeline:
          Phase 1: Connect to SQL via SP (Active Directory Service Principal)
          Phase 2: Stream data row-by-row → per-table .sql.gz files
          Phase 3: Capture schema as SQL scripts
          Phase 4: Capture SQL server + DB configuration as JSON
          Phase 5: Store all artifacts in organized blob structure
        """
        credential = get_arm_credential()
        sql_mgmt = SqlManagementClient(credential, resource.azure_subscription_id)

        server_name = resource.extra_data.get("server_name", "") if resource.extra_data else ""
        rg = resource.azure_resource_group or ""
        # external_id is now the ARM database id (discovery re-keyed it so
        # same-named DBs on different servers stop colliding); the connectable
        # database name travels in metadata. Fall back to external_id for rows
        # discovered before the re-key.
        db_name = (resource.extra_data.get("database_name", "") if resource.extra_data else "") or resource.external_id

        if not server_name or not rg:
            raise ValueError(f"No server_name or azure_resource_group found in resource {resource.external_id}")

        self._log(f"Starting SQL backup: {db_name} on server {server_name} (RG={rg})")

        # Live-progress job id pulled from the RabbitMQ payload.
        job_id = None
        try:
            jid = (msg or {}).get("jobId")
            if jid:
                job_id = _uuid.UUID(jid)
        except Exception:
            job_id = None

        try:
            # Get our SP credentials for SQL data-plane auth
            # Our SP is Entra admin of this SQL server (assigned during discovery)
            sp_client_id = settings.MICROSOFT_CLIENT_ID or settings.AZURE_AD_CLIENT_ID
            sp_client_secret = settings.MICROSOFT_CLIENT_SECRET or settings.AZURE_AD_CLIENT_SECRET

            if not sp_client_id or not sp_client_secret:
                raise ValueError(
                    f"No SP credentials configured. Set MICROSOFT_CLIENT_ID/SECRET or AZURE_AD_CLIENT_ID/SECRET."
                )

            await update_job_pct(job_id, 10)  # creds resolved

            # Phase 1: Connect and stream data
            data_result = await self._stream_tables(
                resource, tenant, snapshot, rg, server_name, db_name,
                sp_client_id, sp_client_secret
            )
            await update_job_pct(job_id, 70)  # data streamed

            # Phase 2: Capture schema
            schema_result = await self._capture_schema(
                resource, tenant, snapshot, rg, server_name, db_name,
                sp_client_id, sp_client_secret
            )
            await update_job_pct(job_id, 85)  # schema captured

            # Phase 3: Capture configuration
            config_result = await self._capture_configuration(
                sql_mgmt, resource, tenant, snapshot, rg, server_name, db_name
            )
            await update_job_pct(job_id, 95)  # config captured

            # Persist SnapshotItem rows so Recovery can list Config /
            # Database / Schema files / Tables. Without this the backup
            # writes to blob storage but /files returns an empty list.
            try:
                await self._persist_snapshot_items(
                    resource=resource, tenant=tenant, snapshot=snapshot,
                    server_name=server_name, db_name=db_name,
                    data_result=data_result, schema_result=schema_result,
                    config_result=config_result,
                )
            except Exception as pe:
                self._log(f"WARN snapshot_item persist failed: {pe}", "WARNING")

            # Finalize
            total_size = data_result.get("total_bytes", 0) + schema_result.get("size_bytes", 0) + config_result.get("size_bytes", 0)

            # Record last backup time on RESOURCE for next incremental
            resource.extra_data = {
                **(resource.extra_data or {}),
                "last_full_backup_at": datetime.now(timezone.utc).isoformat(),
                "last_backup_rows": data_result.get("rows_count", 0),
            }

            # Set blob_path on snapshot for recovery panel indexing
            snapshot.blob_path = f"{server_name}/{db_name}/{snapshot.id.hex[:12]}/"

            snapshot.extra_data = {
                **(snapshot.extra_data or {}),
                "mode": "AFI_STREAMING",
                "total_size_bytes": total_size,
                "tables_exported": data_result.get("tables_count", 0),
                "rows_exported": data_result.get("rows_count", 0),
                "data_blob_prefix": data_result.get("blob_prefix", ""),
                "schema_blob": schema_result.get("blob_path", ""),
                "config_blob": config_result.get("blob_path", ""),
                "server_name": server_name,
                "database_name": db_name,
                "resource_group": rg,
                "subscription_id": resource.azure_subscription_id,
                "auth_method": "ActiveDirectoryServicePrincipal",
                "backup_timestamp": datetime.now(timezone.utc).isoformat(),
                "last_full_backup_at": datetime.now(timezone.utc).isoformat(),
            }

            # Format size in human-readable units
            if total_size >= 1024 * 1024 * 1024:
                size_str = f"{total_size / (1024**3):.2f} GB"
            elif total_size >= 1024 * 1024:
                size_str = f"{total_size / (1024**2):.2f} MB"
            elif total_size >= 1024:
                size_str = f"{total_size / 1024:.1f} KB"
            else:
                size_str = f"{total_size} bytes"

            self._log(
                f"SQL backup completed: {db_name} — "
                f"{data_result.get('tables_count', 0)} tables, "
                f"{data_result.get('rows_count', 0)} rows, "
                f"{size_str} total"
            )

            return {
                "success": True,
                "mode": "AFI_STREAMING",
                "tables_exported": data_result.get("tables_count", 0),
                "rows_exported": data_result.get("rows_count", 0),
                "total_size_bytes": total_size,
                "auth_method": "ActiveDirectoryServicePrincipal",
            }

        except Exception as e:
            self._log(f"SQL backup FAILED for {db_name}: {e}", "ERROR")
            raise
        finally:
            try:
                await sql_mgmt.close()
                await credential.close()
            except Exception:
                pass

    # ==================== Phase 1: Stream Tables Row-by-Row ====================

    async def _stream_tables(self, resource: Resource, tenant: Tenant,
                              snapshot: Snapshot, rg: str, server_name: str,
                              db_name: str, sp_id: str, sp_secret: str) -> Dict:
        """
        Stream data row-by-row from all tables into per-table .sql files.

        Incremental support:
        - On first run: full backup of all tables
        - On subsequent runs: incremental via watermark tracking
          Only exports rows modified since last backup (tracked in resource.extra_data)

        Files are stored UNCOMPRESSED (.sql) so they can be viewed directly
        in Azure Portal's Blob Storage viewer.
        """
        server_fqdn = f"{server_name}.database.windows.net"

        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={server_fqdn};"
            f"DATABASE={db_name};"
            f"Authentication=ActiveDirectoryServicePrincipal;"
            f"UID={sp_id};"
            f"PWD={sp_secret};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
            f"Connection Timeout=30;"
        )

        loop = asyncio.get_event_loop()

        def _get_tables():
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT TABLE_SCHEMA, TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_SCHEMA, TABLE_NAME
            """)
            tables = [(row[0], row[1]) for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            return tables

        tables = await loop.run_in_executor(None, _get_tables)
        self._log(f"  Phase 1: Found {len(tables)} tables in {db_name}")

        # Must match RESOURCE_TYPE_TO_WORKLOADS["AZURE_SQL_DB"] = ("azure-sql-db",)
        # so the snapshot-service read path and the backup-worker write path
        # agree on the container. Writing "azure-sql" made /azure-db/table
        # look in "backup-azure-sql-db-*" (empty) and return zero rows.
        container = azure_storage_manager.get_container_name(str(tenant.id), "azure-sql-db")
        shard = azure_storage_manager.get_default_shard()
        await shard._ensure_container(container)

        blob_prefix = f"{server_name}/{db_name}/{snapshot.id.hex[:12]}/data"

        total_bytes = 0
        total_rows = 0
        table_details: list = []

        # Check for incremental: get last successful backup time from resource column
        last_backup_time = resource.last_backup_at
        if last_backup_time and last_backup_time.tzinfo is None:
            last_backup_time = last_backup_time.replace(tzinfo=timezone.utc)

        if last_backup_time:
            self._log(f"  Incremental backup since {last_backup_time}")
        else:
            self._log(f"  Full backup (no previous backup recorded)")

        # Export tables in parallel (max 4 concurrent table exports)
        semaphore = asyncio.Semaphore(4)

        async def export_table(schema: str, table: str):
            nonlocal total_bytes, total_rows
            async with semaphore:
                table_start = time.monotonic()
                table_bytes, table_rows, blob_name, rows_blob_name, col_names = await self._export_single_table(
                    conn_str, container, blob_prefix, schema, table, shard,
                    last_backup_time=last_backup_time,
                )
                total_bytes += table_bytes
                total_rows += table_rows
                table_details.append({
                    "schema": schema, "table": table,
                    "blob_path": blob_name,
                    "rows_blob_path": rows_blob_name,
                    "columns": col_names,
                    "size_bytes": table_bytes, "row_count": table_rows,
                })
                table_duration = time.monotonic() - table_start
                self._log(f"  ✓ Table [{schema}].[{table}]: {table_rows} rows, "
                          f"{table_bytes / 1024:.1f} KB in {table_duration:.1f}s")

        tasks = [export_table(schema, table) for schema, table in tables]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # A table whose export raised (ODBC disconnect, upload failure,
        # throttling) must never vanish silently: every incremental after
        # this snapshot builds on the gap and the hole is only discovered
        # at restore time. Inspect each outcome and fail the backup if
        # any table is missing — a FAILED snapshot beats a green one
        # with holes.
        failed_tables = []
        for (schema, table), res in zip(tables, results):
            if isinstance(res, BaseException):
                self._log(f"  ✗ Table [{schema}].[{table}] export FAILED: {res}", "ERROR")
                failed_tables.append(f"{schema}.{table}")
        if failed_tables:
            raise RuntimeError(
                f"{len(failed_tables)}/{len(tables)} table exports failed: "
                + ", ".join(failed_tables[:10])
                + ("…" if len(failed_tables) > 10 else "")
            )

        return {
            "tables_count": len(table_details),
            "rows_count": total_rows,
            "total_bytes": total_bytes,
            "blob_prefix": blob_prefix,
            "tables": table_details,
        }

    async def _export_single_table(self, conn_str: str, container: str,
                                    blob_prefix: str, schema: str, table: str,
                                    shard, last_backup_time: Optional[datetime] = None) -> tuple:
        """
        Export a single table as INSERT statements (uncompressed .sql).

        Incremental support:
        - If last_backup_time is provided and the table has an UpdatedDate/ModifiedAt column,
          only exports rows where UpdatedDate >= last_backup_time
        - Otherwise exports all rows (full backup)

        Files are plain .sql so they can be viewed directly in Azure Portal.
        """
        loop = asyncio.get_event_loop()

        def _stream_table():
            """Export table rows as SQL INSERT statements AND capture the
            full row set in memory so we can emit a JSON blob for the
            Database tab's paginated/search view. Keeping both shapes in
            one pass means we only query SQL Server once per table."""
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()

            # Get column info
            cursor.execute(f"""
                SELECT COLUMN_NAME, DATA_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
                ORDER BY ORDINAL_POSITION
            """)
            columns = [(row[0], row[1]) for row in cursor.fetchall()]
            col_names = [c[0] for c in columns]
            col_names_str = ", ".join(f"[{c}]" for c in col_names)

            # Check for incremental: find a timestamp column (UpdatedDate, ModifiedAt, etc.)
            incremental_col = None
            if last_backup_time:
                ts_candidates = ['UpdatedDate', 'ModifiedAt', 'UpdatedAt', 'LastModified', 'ModifiedDate', 'ModifiedDate', 'Updated']
                for candidate in ts_candidates:
                    if candidate in col_names:
                        incremental_col = candidate
                        break

            # Delta slice for the .sql dump; full slice for JSON blob.
            where_clause = ""
            if incremental_col and last_backup_time:
                where_clause = f" WHERE [{incremental_col}] >= '{last_backup_time.strftime('%Y-%m-%d %H:%M:%S')}'"
                self._log(f"    Incremental .sql export for [{schema}].[{table}] via [{incremental_col}] >= {last_backup_time}")

            batch_size = 1000
            offset = 0
            total_rows = 0

            # Create temp file (plain .sql, uncompressed)
            tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".sql")
            tmp_path = tmp_file.name

            with open(tmp_path, 'w', encoding='utf-8') as f:
                f.write(f"-- Table: [{schema}].[{table}]\n")
                f.write(f"-- Exported: {datetime.now(timezone.utc).isoformat()}\n")
                f.write(f"-- Backup type: {'incremental' if incremental_col else 'full'}\n")
                f.write(f"-- Columns: {col_names_str}\n\n")

                while True:
                    query = f"""
                        SELECT * FROM [{schema}].[{table}]{where_clause}
                        ORDER BY {col_names[0]}
                        OFFSET {offset} ROWS
                        FETCH NEXT {batch_size} ROWS ONLY
                    """
                    cursor.execute(query)
                    rows = cursor.fetchall()

                    if not rows:
                        break

                    for row in rows:
                        values = []
                        for val in row:
                            if val is None:
                                values.append("NULL")
                            elif isinstance(val, (int, float)):
                                values.append(str(val))
                            elif isinstance(val, datetime):
                                values.append(f"'{val.isoformat()}'")
                            else:
                                escaped = str(val).replace("'", "''")
                                values.append(f"'{escaped}'")

                        values_str = ", ".join(values)
                        f.write(f"INSERT INTO [{schema}].[{table}] ({col_names_str}) VALUES ({values_str});\n")

                    total_rows += len(rows)
                    offset += batch_size

            # Full row fetch for the JSON blob — the Database tab shows
            # the whole table at the time of this snapshot, not just the
            # delta since the prior run.
            cursor.execute(f"SELECT * FROM [{schema}].[{table}] ORDER BY {col_names[0]}")
            full_rows = []
            while True:
                chunk = cursor.fetchmany(1000)
                if not chunk:
                    break
                for row in chunk:
                    out_row = []
                    for val in row:
                        if isinstance(val, datetime):
                            out_row.append(val.isoformat())
                        elif isinstance(val, (bytes, bytearray)):
                            out_row.append(val.hex())
                        else:
                            out_row.append(val)
                    full_rows.append(out_row)

            cursor.close()
            conn.close()
            return tmp_path, total_rows, col_names, full_rows

        tmp_path, row_count, col_names, full_rows = await loop.run_in_executor(None, _stream_table)

        try:
            # Upload to blob using parallel chunked upload
            file_size = os.path.getsize(tmp_path)
            blob_name = f"{blob_prefix}/{schema}.{table}.sql"

            result = await shard.upload_blob_from_file(
                container_name=container,
                blob_path=blob_name,
                file_path=tmp_path,
                file_size=file_size,
                metadata={
                    "content_type": "sql-data",
                    "table_schema": schema,
                    "table_name": table,
                    "row_count": str(row_count),
                    "column_count": str(len(col_names)),
                    "compression": "none",
                    "backup_type": "incremental" if row_count > 0 and last_backup_time else "full",
                },
            )

            # JSON companion — the /azure-db/table endpoint reads this to
            # render paginated rows in the Recovery UI. Without it the
            # Database tab looked empty (the .sql blob can't be parsed
            # row-by-row).
            rows_blob_name = f"{blob_prefix}/{schema}.{table}.json"
            rows_payload = {
                "schema": schema,
                "table": table,
                "columns": col_names,
                "rows": full_rows,
            }
            await shard.upload_blob(
                container_name=container,
                blob_path=rows_blob_name,
                content=json.dumps(rows_payload, default=str).encode("utf-8"),
                metadata={"content_type": "sql-data-json",
                          "table_schema": schema, "table_name": table},
            )

            return file_size, row_count, blob_name, rows_blob_name, col_names
        finally:
            # Clean up temp file
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    # ==================== Phase 2: Capture Schema ====================

    async def _capture_schema(self, resource: Resource, tenant: Tenant,
                               snapshot: Snapshot, rg: str, server_name: str,
                               db_name: str, sp_id: str, sp_secret: str) -> Dict:
        """Capture Azure SQL schema as a folder tree mirroring Postgres.

        Produces (relative to <db_name>):

          <db>.dacpac.metadata.json    — root-level metadata
          sql/<schema>/Tables/<table>  — CREATE TABLE per table
          sql/<schema>/Views/<view>    — CREATE VIEW per view
          sql/<schema>/Procedures/<p>  — stored procedure body
          sql/<schema>/Functions/<f>   — function body
          sql/Other/indexes            — aggregate index list
          sql/Other/foreign_keys       — aggregate FK list
          sql/Other/users_permissions  — aggregate grants

        Returns {"artifacts": [...]} with name/folder_path/blob_path/size
        so the shared persister can emit AZURE_DB_SCHEMA_FILE rows and
        the Recovery tree renders expandable folders.
        """
        server_fqdn = f"{server_name}.database.windows.net"

        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={server_fqdn};"
            f"DATABASE={db_name};"
            f"Authentication=ActiveDirectoryServicePrincipal;"
            f"UID={sp_id};"
            f"PWD={sp_secret};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
            f"Connection Timeout=30;"
        )

        # Must match RESOURCE_TYPE_TO_WORKLOADS["AZURE_SQL_DB"] = ("azure-sql-db",)
        # so the snapshot-service read path and the backup-worker write path
        # agree on the container. Writing "azure-sql" made /azure-db/table
        # look in "backup-azure-sql-db-*" (empty) and return zero rows.
        container = azure_storage_manager.get_container_name(str(tenant.id), "azure-sql-db")
        shard = azure_storage_manager.get_default_shard()
        schema_prefix = f"{server_name}/{db_name}/{snapshot.id.hex[:12]}/schema"

        self._log(f"  Phase 2: Capturing schema tree for {db_name}...")

        loop = asyncio.get_event_loop()
        artifacts: list = []
        schema_size = 0

        async def _upload_artifact(rel_path: str, name: str, folder_path: str, content: str) -> None:
            nonlocal schema_size
            blob_path = f"{schema_prefix}/{rel_path}"
            data = content.encode("utf-8")
            await shard.upload_blob(
                container_name=container,
                blob_path=blob_path,
                content=data,
                metadata={"content_type": "sql-schema", "artifact_type": name[:60]},
            )
            schema_size += len(data)
            artifacts.append({
                "name": name,
                "folder_path": folder_path,
                "blob_path": blob_path,
                "size_bytes": len(data),
            })

        def _fetch_rows(query: str) -> list:
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            cursor.execute(query)
            cols = [c[0] for c in cursor.description]
            rows = [dict(zip(cols, row)) for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            return rows

        # ── Per-table CREATE statements from INFORMATION_SCHEMA.COLUMNS.
        try:
            tbls = await loop.run_in_executor(None, _fetch_rows, """
                SELECT TABLE_SCHEMA, TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_SCHEMA, TABLE_NAME
            """)
            for tr in tbls:
                ts, tn = tr["TABLE_SCHEMA"], tr["TABLE_NAME"]
                cols = await loop.run_in_executor(None, _fetch_rows, f"""
                    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH,
                           IS_NULLABLE, COLUMN_DEFAULT, NUMERIC_PRECISION, NUMERIC_SCALE
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = '{ts}' AND TABLE_NAME = '{tn}'
                    ORDER BY ORDINAL_POSITION
                """)
                lines = [f"-- Table: [{ts}].[{tn}]",
                         f"CREATE TABLE [{ts}].[{tn}] ("]
                col_defs = []
                for c in cols:
                    dtype = (c["DATA_TYPE"] or "").upper()
                    maxlen = c.get("CHARACTER_MAXIMUM_LENGTH")
                    prec = c.get("NUMERIC_PRECISION")
                    scale = c.get("NUMERIC_SCALE")
                    if maxlen is not None:
                        if maxlen == -1:
                            dtype += "(MAX)"
                        else:
                            dtype += f"({maxlen})"
                    elif prec is not None and dtype in ("DECIMAL", "NUMERIC"):
                        dtype += f"({prec},{scale or 0})"
                    nullable = " NULL" if c.get("IS_NULLABLE") == "YES" else " NOT NULL"
                    default = f" DEFAULT {c['COLUMN_DEFAULT']}" if c.get("COLUMN_DEFAULT") else ""
                    col_defs.append(f'    [{c["COLUMN_NAME"]}] {dtype}{nullable}{default}')
                lines.append(",\n".join(col_defs))
                lines.append(");\n")
                await _upload_artifact(
                    rel_path=f"sql/{ts}/Tables/{tn}",
                    name=tn, folder_path=f"sql/{ts}/Tables",
                    content="\n".join(lines),
                )
        except Exception as e:
            self._log(f"  Warning: per-table capture failed: {e}", "WARNING")

        # ── Per-view CREATE statements (object_definition gives the body).
        try:
            views = await loop.run_in_executor(None, _fetch_rows, """
                SELECT s.name AS schema_name, v.name AS view_name,
                       OBJECT_DEFINITION(v.object_id) AS body
                FROM sys.views v
                JOIN sys.schemas s ON s.schema_id = v.schema_id
                ORDER BY s.name, v.name
            """)
            for vr in views:
                body = vr.get("body") or f"-- View [{vr['schema_name']}].[{vr['view_name']}] body unavailable"
                await _upload_artifact(
                    rel_path=f"sql/{vr['schema_name']}/Views/{vr['view_name']}",
                    name=vr["view_name"], folder_path=f"sql/{vr['schema_name']}/Views",
                    content=body,
                )
        except Exception as e:
            self._log(f"  Warning: per-view capture failed: {e}", "WARNING")

        # ── Per-procedure + per-function (sys.sql_modules covers both).
        try:
            procs = await loop.run_in_executor(None, _fetch_rows, """
                SELECT s.name AS schema_name, o.name AS obj_name, o.type AS obj_type,
                       OBJECT_DEFINITION(o.object_id) AS body
                FROM sys.objects o
                JOIN sys.schemas s ON s.schema_id = o.schema_id
                WHERE o.type IN ('P','PC','FN','IF','TF','FS','FT')
                ORDER BY s.name, o.name
            """)
            for pr in procs:
                ot = (pr.get("obj_type") or "").strip()
                folder = "Procedures" if ot in ("P", "PC") else "Functions"
                body = pr.get("body") or f"-- {folder[:-1]} [{pr['schema_name']}].[{pr['obj_name']}] body unavailable"
                await _upload_artifact(
                    rel_path=f"sql/{pr['schema_name']}/{folder}/{pr['obj_name']}",
                    name=pr["obj_name"], folder_path=f"sql/{pr['schema_name']}/{folder}",
                    content=body,
                )
        except Exception as e:
            self._log(f"  Warning: per-proc/function capture failed: {e}", "WARNING")

        # ── Aggregate files under sql/Other/ — indexes, FKs, grants.
        #    These stay as one-file-per-kind because they're cross-object
        #    lists, not per-object definitions.
        for aggregate in (
            {"query": self._QUERY_INDEXES, "name": "indexes"},
            {"query": self._QUERY_FK, "name": "foreign_keys"},
            {"query": self._QUERY_USERS, "name": "users_permissions"},
        ):
            try:
                rows = await loop.run_in_executor(None, _fetch_rows, aggregate["query"])
                body = self._format_sql_script(aggregate["name"], rows)
                await _upload_artifact(
                    rel_path=f"sql/Other/{aggregate['name']}",
                    name=aggregate["name"], folder_path="sql/Other",
                    content=body,
                )
            except Exception as e:
                self._log(f"  Warning: Failed to capture {aggregate['name']}: {e}", "WARNING")

        # ── dacpac metadata stays at db root.
        dacpac_metadata = {
            "dacpac_version": "2.0",
            "database_name": db_name,
            "database_platform": "Azure SQL Database",
            "exported_at": datetime.now(timezone.utc).isoformat(),
            "exported_by": "TM Vault",
            "source_server": server_name,
            "source_resource_group": rg,
        }

        dacpac_blob_path = f"{schema_prefix}/{db_name}.dacpac.metadata.json"
        dacpac_content = json.dumps(dacpac_metadata, indent=2, default=str).encode()
        await shard.upload_blob(
            container_name=container,
            blob_path=dacpac_blob_path,
            content=dacpac_content,
            metadata={"content_type": "dacpac-metadata"},
        )
        schema_size += len(dacpac_content)
        artifacts.append({
            "name": f"{db_name}.dacpac.metadata.json",
            "folder_path": "",
            "blob_path": dacpac_blob_path,
            "size_bytes": len(dacpac_content),
        })

        self._log(f"  ✓ Schema captured: {len(artifacts)} artifacts, {schema_size / 1024:.1f} KB")
        return {
            "blob_path": f"{schema_prefix}/",
            "size_bytes": schema_size,
            "artifacts": artifacts,
        }

    # ==================== Phase 3: Capture Configuration ====================

    async def _capture_configuration(self, sql_mgmt, resource: Resource, tenant: Tenant,
                                      snapshot: Snapshot, rg: str, server_name: str,
                                      db_name: str) -> Dict:
        """Capture SQL server + database configuration with the full set of
        fields the Configuration tab renders (Essential / Compute+storage /
        Backup / Maintenance / Networking / HA / Replication).

        Shape mirrors the Postgres handler so the shared AzureDbConfiguration
        React component can render both without branching per server type.
        """
        # Must match RESOURCE_TYPE_TO_WORKLOADS["AZURE_SQL_DB"] = ("azure-sql-db",)
        # so the snapshot-service read path and the backup-worker write path
        # agree on the container. Writing "azure-sql" made /azure-db/table
        # look in "backup-azure-sql-db-*" (empty) and return zero rows.
        container = azure_storage_manager.get_container_name(str(tenant.id), "azure-sql-db")
        shard = azure_storage_manager.get_default_shard()

        self._log(f"  Phase 3: Capturing configuration for {db_name}...")

        config: Dict[str, Any] = {
            "server_type": "AZURE_SQL",
            "server_name": server_name,
            "database_name": db_name,
            "resource_group": rg,
            "subscription_id": resource.azure_subscription_id,
            "captured_at": datetime.now(timezone.utc).isoformat(),
        }

        try:
            server = await sql_mgmt.servers.get(rg, server_name)

            config["location"] = getattr(server, "location", None)
            config["fully_qualified_domain_name"] = getattr(server, "fully_qualified_domain_name", None)
            config["administrator_login"] = getattr(server, "administrator_login", None)
            config["version"] = getattr(server, "version", None)
            config["state"] = getattr(server, "state", None)

            # Server identity (for Security panel).
            identity = getattr(server, "identity", None)
            id_type = str(getattr(identity, "type", "") or "")
            user_ids = getattr(identity, "user_assigned_identities", None) or {}
            primary_uid = getattr(identity, "user_assigned_identity_id", "") or ""
            config["identity"] = {
                "type": id_type,
                "system_assigned": "Enabled" if "SystemAssigned" in id_type else "Disabled",
                "user_assigned_count": len(user_ids) if isinstance(user_ids, dict) else 0,
                "primary_user_assigned": primary_uid or "Not configured",
            }

            # Network — public access + private endpoints.
            public_net = getattr(server, "public_network_access", None)
            pe_list = getattr(server, "private_endpoint_connections", None) or []
            config["network"] = {
                "publicNetworkAccess": public_net,
                "delegatedSubnetResourceId": None,
                "privateEndpointCount": len(pe_list) if isinstance(pe_list, (list, tuple)) else 0,
            }

            # Entra ID admin assignment (who can log in as "AAD admin").
            entra_admin_login = ""
            try:
                async for adm in sql_mgmt.server_azure_ad_administrators.list_by_server(rg, server_name):
                    entra_admin_login = getattr(adm, "login", "") or entra_admin_login
                    break
            except Exception:
                pass
            config["authentication"] = {
                # If an Entra admin exists the server supports both. Pure
                # SQL auth if it's absent.
                "method": "Entra ID & SQL" if entra_admin_login else "SQL",
                "sql_admin": getattr(server, "administrator_login", "") or "",
                "entra_admin": entra_admin_login or "",
            }

            # Firewall rule count — the Configuration tab shows this as
            # "N firewall rules"; list + count rather than embedding.
            fw_count = 0
            try:
                async for _ in sql_mgmt.firewall_rules.list_by_server(rg, server_name):
                    fw_count += 1
            except Exception as fe:
                self._log(f"firewall rules fetch failed: {fe}", "WARNING")
            config["firewallRuleCount"] = fw_count
        except Exception as e:
            self._log(f"server capture failed: {e}", "WARNING")

        try:
            db = await sql_mgmt.databases.get(
                resource_group_name=rg, server_name=server_name, database_name=db_name,
            )

            # The SDK exposes currentSku as `current_sku`; the plain
            # `sku` attribute is often the requested one (or None). Fall
            # back to whichever is populated.
            sku = getattr(db, "current_sku", None) or getattr(db, "sku", None)
            config["sku"] = {
                "name": getattr(sku, "name", "") or "",
                "tier": getattr(sku, "tier", "") or "",
                "capacity": getattr(sku, "capacity", "") or "",
            }

            max_bytes = getattr(db, "max_size_bytes", None) or 0
            max_gb = round(max_bytes / (1024 ** 3), 2) if max_bytes else None
            config["storage"] = {
                "storageSizeGB": max_gb,
                # Azure SQL has no user-visible auto-grow — storage is
                # service-managed. Be explicit about it so the UI shows
                # a meaningful value instead of blank.
                "autoGrow": "Service-managed",
            }
            config["database_state"] = getattr(db, "status", None)
            config["collation"] = getattr(db, "collation", None)
            config["zone_redundant"] = bool(getattr(db, "zone_redundant", False))

            # Auto-pause delay (serverless). -1 means "disabled" in Azure's model.
            ap = getattr(db, "auto_pause_delay", None)
            if ap is None or ap == -1:
                config["auto_pause_delay"] = "Disabled"
            else:
                config["auto_pause_delay"] = f"{ap} minutes"

            # Ledger + secure-enclave flags for the Security panel.
            config["ledger"] = {
                "enabled": "Enabled" if bool(getattr(db, "is_ledger_on", False)) else "Disabled",
                "digest_storage": "Not configured",  # set only when managed digests are configured
            }
            pref = getattr(db, "preferred_enclave_type", None)
            config["secure_enclaves"] = "Enabled" if (pref and str(pref).lower() != "default") else "Disabled"

            # Replication count for the Availability panel. read_scale
            # controls read-only secondary replicas; high_availability
            # reflects primary+secondary copies.
            ha_count = getattr(db, "high_availability_replica_count", 0) or 0
            rs = getattr(db, "read_scale", None)
            rs_str = str(rs) if rs is not None else ""
            config["replication"] = {
                "replica_count": int(ha_count),
                "read_scale": rs_str,
            }

            # Backup storage redundancy (Geo / Local / Zone).
            cbsr = getattr(db, "current_backup_storage_redundancy", None)
            rbsr = getattr(db, "requested_backup_storage_redundancy", None)
            config["backup_storage_redundancy"] = str(cbsr or rbsr or "")

            # availabilityZone lives on the database, not the server
            # (values observed: "NoPreference", "1", "2", "3"). The
            # azure-mgmt-sql SDK version in this image doesn't expose it
            # as a typed attribute, so dig it out of as_dict() first and
            # fall back to a raw ARM REST call if as_dict() strips it.
            az_val = getattr(db, "availability_zone", None)
            if not az_val:
                try:
                    raw_db = db.as_dict() if hasattr(db, "as_dict") else {}
                    az_val = (
                        (raw_db.get("properties") or {}).get("availabilityZone")
                        or raw_db.get("availabilityZone")
                        or raw_db.get("availability_zone")
                    )
                except Exception:
                    az_val = None
            if not az_val:
                try:
                    import httpx as _httpx
                    from azure.identity.aio import ClientSecretCredential
                    cred = ClientSecretCredential(
                        client_id=settings.EFFECTIVE_ARM_CLIENT_ID,
                        client_secret=settings.EFFECTIVE_ARM_CLIENT_SECRET,
                        tenant_id=settings.EFFECTIVE_ARM_TENANT_ID,
                    )
                    tok = await cred.get_token("https://management.azure.com/.default")
                    await cred.close()
                    arm_url = (
                        f"https://management.azure.com/subscriptions/{resource.azure_subscription_id}"
                        f"/resourceGroups/{rg}/providers/Microsoft.Sql/servers/{server_name}"
                        f"/databases/{db_name}?api-version=2023-08-01-preview"
                    )
                    async with _httpx.AsyncClient(timeout=15.0) as _c:
                        _r = await _c.get(arm_url, headers={"Authorization": f"Bearer {tok.token}"})
                        if _r.status_code == 200:
                            az_val = (_r.json().get("properties") or {}).get("availabilityZone")
                except Exception as aze:
                    self._log(f"availability_zone REST fallback failed: {aze}", "WARNING")
            if not az_val:
                az_val = "Zone redundant" if bool(getattr(db, "zone_redundant", False)) else ""
            config["availability_zone"] = str(az_val) if az_val else ""

            # Time created — SQL puts creationDate on the database, not
            # the server (servers have systemData=null in this tenant).
            created = getattr(db, "creation_date", None)
            if created:
                try:
                    config["time_created"] = created.isoformat()
                except Exception:
                    config["time_created"] = str(created)
            else:
                config["time_created"] = ""

            # Short-term backup retention — the Backup panel shows this
            # as "Retention period (days)".
            retention_days = None
            try:
                rp = await sql_mgmt.backup_short_term_retention_policies.get(
                    rg, server_name, db_name, "default",
                )
                retention_days = getattr(rp, "retention_days", None)
            except Exception:
                pass
            config["backup"] = {
                "backupRetentionDays": retention_days,
                "geoRedundantBackup": "Enabled" if (
                    str(getattr(db, "requested_backup_storage_redundancy", "")).lower() == "geo"
                    or str(getattr(db, "current_backup_storage_redundancy", "")).lower() == "geo"
                ) else "Disabled",
            }

            # Maintenance window — surfaces the SQL database's assigned
            # maintenance configuration (e.g. SQL_Default).
            maint_cfg_id = getattr(db, "maintenance_configuration_id", "") or ""
            maint_name = maint_cfg_id.rsplit("/", 1)[-1] if maint_cfg_id else ""
            config["maintenance"] = {
                "configurationId": maint_cfg_id,
                "configurationName": maint_name or "SQL_Default",
            }

            # High-availability + replication.
            config["highAvailability"] = {
                "mode": "Enabled" if bool(getattr(db, "zone_redundant", False)) else "Disabled",
            }
            replication_role = (
                getattr(db, "secondary_type", None)
                or getattr(db, "source_database_id", None) and "Secondary"
                or "None"
            )
            config["replicationRole"] = replication_role or "None"

            # TDE (kept for ops use in the raw JSON, not surfaced on UI yet).
            try:
                tde = await sql_mgmt.transparent_data_encryptions.get(rg, server_name, db_name, "current")
                config["tde"] = {"status": getattr(tde, "status", None)}
            except Exception:
                config["tde"] = {"status": "unknown"}
        except Exception as e:
            self._log(f"database capture failed: {e}", "WARNING")

        blob_path = f"{server_name}/{db_name}/{snapshot.id.hex[:12]}/config/{db_name}-config.json"
        content = json.dumps(config, indent=2, default=str).encode()
        await shard.upload_blob(
            container_name=container,
            blob_path=blob_path,
            content=content,
            metadata={"content_type": "sql-config"},
        )

        self._log(f"  ✓ Configuration captured: {len(content) / 1024:.1f} KB")
        return {"blob_path": blob_path, "size_bytes": len(content), "config": config}

    # ==================== Helper Methods ====================

    def _format_sql_script(self, artifact_name: str, results: List[Dict]) -> str:
        """Format query results as a SQL script."""
        lines = [
            f"-- =============================================",
            f"-- TM Vault SQL Schema Export",
            f"-- Artifact: {artifact_name}",
            f"-- Generated: {datetime.now(timezone.utc).isoformat()}",
            f"-- =============================================",
            f"",
        ]

        if not results:
            lines.append("-- No results (query returned empty or was not supported)")
            lines.append("")
            return "\n".join(lines)

        for row in results:
            if artifact_name == "tables.sql":
                schema = row.get("TABLE_SCHEMA", "dbo")
                table = row.get("TABLE_NAME", "unknown")
                cols = row.get("column_count", 0)
                lines.append(f"-- Table: [{schema}].[{table}] ({cols} columns)")
            elif artifact_name == "table_definitions.sql":
                schema = row.get("TABLE_SCHEMA", "dbo")
                table = row.get("TABLE_NAME", "unknown")
                col = row.get("COLUMN_NAME", "")
                dtype = row.get("DATA_TYPE", "")
                nullable = "NULL" if row.get("IS_NULLABLE") == "YES" else "NOT NULL"
                lines.append(f"-- Column: [{schema}].[{table}].[{col}] {dtype} {nullable}")

        return "\n".join(lines)

    async def _persist_snapshot_items(
        self, *, resource: Resource, tenant: Tenant, snapshot: Snapshot,
        server_name: str, db_name: str,
        data_result: Dict, schema_result: Dict, config_result: Dict,
    ) -> None:
        """Emit the SnapshotItem rows that power the Azure SQL Recovery UI.

        Shape mirrors Postgres so the existing AzureDbView can render:
          • 1 × AZURE_DB_CONFIG     — <db>-config.json (Configuration tab)
          • 1 × AZURE_DB_DATABASE   — the database (left rail)
          • N × AZURE_DB_SCHEMA_FILE — tables.sql / indexes.sql / views.sql / … / .dacpac.json
          • N × AZURE_DB_TABLE      — per-table data dump
        """
        rows: list = []

        cfg_blob = config_result.get("blob_path") or ""
        cfg_name = cfg_blob.rsplit("/", 1)[-1] or f"{db_name}-config.json"
        cfg_raw = config_result.get("config") or {}
        rows.append(SnapshotItem(
            id=_uuid.uuid4(),
            snapshot_id=snapshot.id,
            tenant_id=tenant.id,
            external_id=f"{server_name}:{db_name}:config",
            item_type="AZURE_DB_CONFIG",
            name=cfg_name,
            folder_path="",
            content_size=int(config_result.get("size_bytes") or 0),
            content_hash=None,
            content_checksum=None,
            blob_path=cfg_blob,
            extra_data={
                "server_name": server_name,
                "database_name": db_name,
                "server_type": "AZURE_SQL",
                "raw": cfg_raw,
            },
        ))

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
                "server_type": "AZURE_SQL",
                "tables_count": int(data_result.get("tables_count") or 0),
                "rows_count": int(data_result.get("rows_count") or 0),
            },
        ))

        # Schema artifacts — each artifact carries a folder_path
        # relative to the db (e.g. "sql/dbo/Tables"). Prefix with the
        # db name so the frontend tree renders it under the selected
        # database. Root-level artifacts (dacpac) come through with
        # folder_path == "".
        for art in (schema_result.get("artifacts") or []):
            rel_folder = (art.get("folder_path") or "").strip("/")
            folder = f"{db_name}/{rel_folder}" if rel_folder else db_name
            art_name = art.get("name") or "schema.sql"
            rows.append(SnapshotItem(
                id=_uuid.uuid4(),
                snapshot_id=snapshot.id,
                tenant_id=tenant.id,
                external_id=f"{server_name}:{db_name}:schema:{folder}/{art_name}",
                item_type="AZURE_DB_SCHEMA_FILE",
                name=art_name,
                folder_path=folder,
                content_size=int(art.get("size_bytes") or 0),
                content_hash=None,
                content_checksum=None,
                blob_path=art.get("blob_path"),
                extra_data={
                    "server_name": server_name,
                    "database_name": db_name,
                    "server_type": "AZURE_SQL",
                },
            ))

        # Per-table data dumps.
        for t in (data_result.get("tables") or []):
            schema = t.get("schema") or "dbo"
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
                    "server_type": "AZURE_SQL",
                },
            ))

        async with async_session_factory() as sess:
            sess.add_all(rows)
            await sess.commit()
        self._log(f"  Persisted {len(rows)} SnapshotItem rows for Recovery UI")

    def _log(self, message: str, level: str = "INFO"):
        prefix = f"[{self.worker_id}]"
        if level == "ERROR":
            print(f"{prefix} [SQL] ERROR: {message}")
        elif level == "WARNING":
            print(f"{prefix} [SQL] WARNING: {message}")
        else:
            print(f"{prefix} [SQL] {message}")

    # ==================== SQL Queries ====================

    _QUERY_TABLES = """
SELECT
    t.TABLE_SCHEMA,
    t.TABLE_NAME,
    t.TABLE_TYPE,
    (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS c WHERE c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME) as column_count
FROM INFORMATION_SCHEMA.TABLES t
WHERE t.TABLE_TYPE = 'BASE TABLE'
ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME;
"""

    _QUERY_COLUMNS = """
SELECT
    c.TABLE_SCHEMA,
    c.TABLE_NAME,
    c.COLUMN_NAME,
    c.DATA_TYPE,
    c.CHARACTER_MAXIMUM_LENGTH,
    c.NUMERIC_PRECISION,
    c.IS_NULLABLE,
    c.COLUMN_DEFAULT
FROM INFORMATION_SCHEMA.COLUMNS c
ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION;
"""

    _QUERY_INDEXES = """
SELECT
    t.name as table_name,
    i.name as index_name,
    i.type_desc as index_type,
    i.is_unique,
    i.is_primary_key,
    i.is_unique_constraint
FROM sys.indexes i
JOIN sys.tables t ON i.object_id = t.object_id
WHERE i.type > 0
ORDER BY t.name, i.name;
"""

    _QUERY_FK = """
SELECT
    fk.name as constraint_name,
    OBJECT_NAME(fk.parent_object_id) as table_name,
    COL_NAME(fkc.parent_object_id, fkc.parent_column_id) as column_name,
    OBJECT_NAME(fk.referenced_object_id) as referenced_table,
    COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) as referenced_column
FROM sys.foreign_keys fk
JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
ORDER BY fk.name;
"""

    _QUERY_PROCEDURES = """
SELECT
    ROUTINE_SCHEMA,
    ROUTINE_NAME,
    ROUTINE_DEFINITION
FROM INFORMATION_SCHEMA.ROUTINES
WHERE ROUTINE_TYPE = 'PROCEDURE'
ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME;
"""

    _QUERY_VIEWS = """
SELECT
    TABLE_SCHEMA,
    TABLE_NAME as view_name,
    VIEW_DEFINITION
FROM INFORMATION_SCHEMA.VIEWS
ORDER BY TABLE_SCHEMA, TABLE_NAME;
"""

    _QUERY_USERS = """
SELECT
    dp.name as principal_name,
    dp.type_desc as principal_type,
    dp.default_schema_name,
    perm.permission_name,
    perm.state_desc
FROM sys.database_principals dp
LEFT JOIN sys.database_permissions perm ON dp.principal_id = perm.grantee_principal_id
WHERE dp.type IN ('S', 'U', 'G')
ORDER BY dp.name;
"""
