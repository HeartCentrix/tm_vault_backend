"""Azure PostgreSQL Restore Handler — reverses the afi-style streaming backup.

Restore modes:
1. FULL — schema + all table data re-applied
2. SCHEMA_ONLY — run schema.sql only (produces empty tables)
3. DATA_ONLY — run data .sql files only (tables must already exist)

The restore reuses the backup handler's AAD-token connection path so it works
against Flexible Server and Single Server the same way the backup does. The SP
that runs the restore must already be registered as Azure AD admin on the
target server.
"""
import asyncio
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from handlers._progress import update_job_pct

import asyncpg
from azure.identity.aio import ClientSecretCredential

from shared.models import Resource, Tenant, Snapshot
from shared.azure_storage import azure_storage_manager
from shared.config import settings


PG_AAD_SCOPE = "https://ossrdbms-aad.database.windows.net/.default"


class PostgresRestoreHandler:
    """Restore Azure PostgreSQL databases from afi-style streaming backups."""

    def __init__(self, worker_id: str = "azure-pg-restore"):
        self.worker_id = worker_id

    def _log(self, message: str, level: str = "INFO"):
        prefix = f"[{self.worker_id}] [PG-RESTORE]"
        if level == "ERROR":
            print(f"{prefix} ERROR: {message}")
        elif level == "WARNING":
            print(f"{prefix} WARNING: {message}")
        else:
            print(f"{prefix} {message}")

    async def restore(self, tenant: Tenant, snapshot: Snapshot,
                      restore_params: Dict) -> Dict:
        target_server = restore_params.get("target_server_name")
        target_db = restore_params.get("target_database_name")
        server_type = (restore_params.get("server_type")
                       or snapshot.extra_data.get("server_type", "FLEXIBLE")).upper()
        mode = (restore_params.get("mode") or "FULL").upper()
        drop_existing = bool(restore_params.get("drop_existing", False))

        if not target_server or not target_db:
            raise ValueError("target_server_name and target_database_name are required")

        self._log(f"Restore start: {target_server}/{target_db} (mode={mode}, server_type={server_type})")

        # Live progress so the Activity bar advances during the restore
        # instead of jumping from 5% (worker pickup) to 100% at end.
        job_id = restore_params.get("job_id")
        await update_job_pct(job_id, 10)

        # Ensure the target database exists. Postgres can't CREATE
        # DATABASE inside a transaction, so we open an admin connection
        # to the default `postgres` database first and run it there.
        # Idempotent — silently skipped if the DB is already present.
        await self._ensure_target_database(target_server, target_db, server_type, restore_params)
        await update_job_pct(job_id, 15)

        conn = None
        try:
            conn = await self._connect(target_server, target_db, server_type, restore_params)
            await update_job_pct(job_id, 20)

            # Pick the right schema dump + data prefix for the SOURCE
            # database the user chose in the modal. The backup handler
            # records both as per-DB dicts (schema_blobs / data_prefixes).
            # Falls back to the legacy single-db fields for older snapshots.
            source_db = (restore_params.get("source_database_name")
                         or snapshot.extra_data.get("database_name"))
            schema_blobs = snapshot.extra_data.get("schema_blobs") or {}
            data_prefixes = snapshot.extra_data.get("data_prefixes") or {}
            schema_blob = (schema_blobs.get(source_db)
                           or snapshot.extra_data.get("schema_blob"))
            data_prefix = (data_prefixes.get(source_db)
                           or snapshot.extra_data.get("blob_path_prefix")
                           or self._derive_data_prefix(snapshot))

            if drop_existing:
                await self._drop_user_tables(conn)
                await update_job_pct(job_id, 30)

            schema_applied = 0
            if mode in ("FULL", "SCHEMA_ONLY"):
                # The backup dumps the schema across a folder of
                # per-object files (schema/sql/<schema>/Tables/...,
                # Sequences/..., Other/grants etc). Older snapshots
                # stored schema_blobs[db] = the FIRST of those files,
                # so trusting it rebuilt exactly one table and every
                # data insert after it failed. Always try the schema
                # tree first; the stored blob is only a fallback for
                # genuinely monolithic pre-tree dumps.
                schema_applied = await self._apply_schema_tree(
                    conn, tenant, data_prefix,
                )
                if schema_applied == 0 and schema_blob:
                    schema_applied = await self._apply_schema(conn, tenant, schema_blob)
                await update_job_pct(job_id, 50)

            tables_applied = 0
            rows_applied = 0
            if mode in ("FULL", "DATA_ONLY"):
                stats = await self._apply_data(conn, tenant, data_prefix)
                tables_applied = stats["tables"]
                rows_applied = stats["rows"]
                await update_job_pct(job_id, 95)

            self._log(f"Restore completed: schema_objects={schema_applied}, tables={tables_applied}, rows={rows_applied}")
            return {
                "success": True,
                "mode": mode,
                "server_type": server_type,
                "target_server": target_server,
                "target_database": target_db,
                "schema_statements_applied": schema_applied,
                "tables_restored": tables_applied,
                "rows_restored": rows_applied,
            }

        except Exception as e:
            self._log(f"Restore FAILED: {e}", "ERROR")
            return {"success": False, "error": str(e)[:1000]}
        finally:
            if conn:
                try:
                    await conn.close()
                except Exception:
                    pass

    async def _connect(self, server_name: str, db_name: str, server_type: str,
                       restore_params: Dict):
        sp_client_id = settings.MICROSOFT_CLIENT_ID or settings.AZURE_AD_CLIENT_ID
        sp_client_secret = settings.MICROSOFT_CLIENT_SECRET or settings.AZURE_AD_CLIENT_SECRET
        sp_tenant_id = settings.MICROSOFT_TENANT_ID or settings.AZURE_AD_TENANT_ID
        if not sp_client_id or not sp_client_secret or not sp_tenant_id:
            raise ValueError("SP credentials missing (MICROSOFT_CLIENT_ID/SECRET/TENANT_ID)")

        credential = ClientSecretCredential(
            client_id=sp_client_id, client_secret=sp_client_secret, tenant_id=sp_tenant_id,
        )
        try:
            token = (await credential.get_token(PG_AAD_SCOPE)).token
        finally:
            await credential.close()

        host = f"{server_name}.postgres.database.azure.com"
        if server_type == "FLEXIBLE":
            user = sp_client_id
        else:
            admin_user = restore_params.get("admin_user", "pgadmin")
            user = f"{admin_user}@{server_name}"

        self._log(f"Connecting to {host}/{db_name} as {user} ({server_type})")
        return await asyncpg.connect(
            host=host, port=5432, user=user, password=token,
            database=db_name, ssl="require", timeout=30.0,
        )

    async def _ensure_target_database(self, server_name: str, db_name: str,
                                      server_type: str, restore_params: Dict) -> None:
        """CREATE DATABASE on the destination server if it doesn't already
        exist. We connect to the always-present `postgres` admin DB to run
        the DDL — Postgres forbids CREATE DATABASE inside a transaction
        and asyncpg autostarts one for every command, so we have to open
        a dedicated connection with statement-cache disabled and skip the
        implicit transaction by using `execute()` on the simple-query
        protocol (asyncpg does this automatically for DDL)."""
        admin_conn = await self._connect(server_name, "postgres", server_type, restore_params)
        try:
            existing = await admin_conn.fetchval(
                "SELECT 1 FROM pg_database WHERE datname = $1", db_name,
            )
            if existing:
                self._log(f"Target database {db_name!r} already exists — reusing")
                return
            # Identifier injection is impossible via $1 binds for DDL, so
            # we have to interpolate. Quote with double-quotes and escape
            # any embedded ones — the same pattern psql uses internally.
            safe = db_name.replace('"', '""')
            await admin_conn.execute(f'CREATE DATABASE "{safe}"')
            self._log(f"Created destination database {db_name!r}")
        finally:
            await admin_conn.close()

    async def _drop_user_tables(self, conn):
        """DROP every user table in the target database so a FULL restore lands
        on a clean slate. Leaves extensions and non-default schemas other than
        the ones containing tables intact."""
        rows = await conn.fetch("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
              AND table_schema NOT IN ('pg_catalog', 'information_schema')
              AND table_schema !~ '^pg_toast'
        """)
        for r in rows:
            await conn.execute(f'DROP TABLE IF EXISTS "{r["table_schema"]}"."{r["table_name"]}" CASCADE')
        self._log(f"Dropped {len(rows)} pre-existing tables")

    async def _apply_schema(self, conn, tenant: Tenant, schema_blob: str) -> int:
        container = azure_storage_manager.get_container_name(str(tenant.id), "azure-postgresql")
        shard = azure_storage_manager.get_default_shard()
        # Facade download — works on whichever backend the snapshot was
        # captured on (Azure Blob or on-prem SeaweedFS). Fail loudly if
        # the blob is missing — a missing schema dump means we can't restore.
        content_bytes = await shard.download_blob(container, schema_blob)
        if content_bytes is None:
            raise RuntimeError(
                f"schema blob missing for restore: container={container} "
                f"path={schema_blob}"
            )
        content = content_bytes.decode("utf-8")

        # Execute the whole dump in one shot. The backup handler writes a
        # semicolon-separated series of CREATE TABLE / CREATE INDEX statements;
        # asyncpg.execute handles a multi-statement script fine.
        await conn.execute(content)
        # Count statements for reporting only — not authoritative on syntax.
        return sum(1 for ln in content.splitlines() if ln.strip().upper().startswith(("CREATE", "ALTER")))

    async def _apply_data(self, conn, tenant: Tenant, data_prefix: str) -> Dict[str, int]:
        """Replay every per-table data dump back into the destination.
        The backup handler writes two siblings per table:
            data/<schema>_<table>.json  ← authoritative full snapshot
            data/<schema>_<table>.sql   ← human-readable INSERT dump
                                         (often only delta / comment header)
        We prefer the .json file because it always contains the FULL
        row set for the snapshot, then INSERT the rows via asyncpg's
        executemany. .sql files are used only when no .json sibling
        exists (legacy backups)."""
        import json as _json

        container = azure_storage_manager.get_container_name(str(tenant.id), "azure-postgresql")
        shard = azure_storage_manager.get_default_shard()

        # Bucket every blob under data/ via the facade list_blobs so we
        # can pair .json with .sql siblings and prefer the JSON dump
        # (authoritative full snapshot, always present post-Phase-1).
        # The facade works against whichever backend the snapshot was
        # captured on (Azure Blob or on-prem SeaweedFS).
        prefix = data_prefix + "data/"
        blobs_by_stem: Dict[str, Dict[str, str]] = {}
        async for name in shard.list_blobs(container, name_starts_with=prefix):
            if name.endswith(".json"):
                stem = name[:-5]
                blobs_by_stem.setdefault(stem, {})["json"] = name
            elif name.endswith(".sql"):
                stem = name[:-4]
                blobs_by_stem.setdefault(stem, {})["sql"] = name

        tables = 0
        rows = 0
        for stem, kinds in blobs_by_stem.items():
            chosen = kinds.get("json") or kinds.get("sql")
            if not chosen:
                continue
            payload = await shard.download_blob(container, chosen)
            if payload is None:
                self._log(f"  ✗ {chosen}: blob missing, skipping", "WARNING")
                continue
            if not payload:
                tables += 1
                self._log(f"  ✓ {chosen} is empty — table exists, no rows to apply")
                continue

            try:
                if chosen.endswith(".json"):
                    inserted = await self._apply_table_from_json(conn, payload)
                    tables += 1
                    rows += inserted
                    self._log(f"  ✓ Applied {chosen} ({inserted} rows from JSON)")
                else:
                    content = payload.decode("utf-8") if isinstance(payload, (bytes, bytearray)) else str(payload)
                    insert_count = sum(1 for ln in content.splitlines() if ln.strip().upper().startswith("INSERT"))
                    if insert_count == 0:
                        tables += 1
                        self._log(f"  ✓ {chosen} has no INSERTs — table exists, no rows to apply")
                        continue
                    await conn.execute(content)
                    tables += 1
                    rows += insert_count
                    self._log(f"  ✓ Applied {chosen} ({insert_count} rows)")
            except Exception as e:
                self._log(f"  ✗ Failed to apply {chosen}: {e}", "WARNING")
                # Continue with other tables — partial restores beat total failure.

        return {"tables": tables, "rows": rows}

    async def _apply_schema_tree(self, conn, tenant: Tenant, data_prefix: str) -> int:
        """Apply every per-object schema dump under schema/sql/. Order
        matters: schemas first → tables → sequences → constraints/grants
        last, so foreign-key dependencies resolve. We sort by directory
        name to approximate that ordering."""
        container = azure_storage_manager.get_container_name(str(tenant.id), "azure-postgresql")
        shard = azure_storage_manager.get_default_shard()

        files: list[tuple[int, str]] = []
        # Strict dependency order. Sequences MUST come before Tables —
        # Tables' DEFAULT clauses reference them via nextval('seq').
        # Indexes / FKs / grants come last because they reference rows
        # in the tables they constrain.
        order = {"schemas": 0, "Sequences": 1, "Tables": 2,
                 "Views": 3, "indexes": 4, "foreign_keys": 5,
                 "grants": 6, "users_permissions": 7}
        async for name in shard.list_blobs(container, name_starts_with=data_prefix + "schema/"):
            # Pick the first path segment we recognise to decide order;
            # unknown buckets land at the end.
            rank = 99
            for key, val in order.items():
                if f"/{key}/" in name or name.endswith(f"/{key}"):
                    rank = val
                    break
            files.append((rank, name))
        files.sort()

        applied = 0
        for _, name in files:
            payload = await shard.download_blob(container, name)
            if payload is None:
                self._log(f"  ✗ {name}: blob missing, skipping", "WARNING")
                continue
            if not payload:
                continue
            content = payload.decode("utf-8") if isinstance(payload, (bytes, bytearray)) else str(payload)
            # Comment-only files (zero rows / no real DDL) crash asyncpg.
            real = [ln for ln in content.splitlines()
                    if ln.strip() and not ln.strip().startswith("--")]
            if not real:
                continue
            # Backup-side workaround: the dump emits MySQL-style type
            # widths that PostgreSQL rejects. Strip the parenthesised
            # precision off integer types — `INTEGER(32)` → `INTEGER`,
            # `BIGINT(20)` → `BIGINT`, etc. Decimal/numeric/varchar
            # widths are valid in PG so we leave them alone.
            import re as _re
            cleaned = _re.sub(
                r'\b(INTEGER|BIGINT|SMALLINT|INT|SERIAL|BIGSERIAL)\s*\(\s*\d+\s*\)',
                r'\1', content, flags=_re.IGNORECASE,
            )
            try:
                await conn.execute(cleaned)
                applied += sum(1 for ln in real if ln.strip().upper().startswith(("CREATE", "ALTER", "GRANT")))
            except Exception as e:
                self._log(f"  ✗ Failed to apply {name}: {e}", "WARNING")
        return applied

    async def _apply_table_from_json(self, conn, payload: bytes) -> int:
        """Insert every row from a backup .json blob into its source
        table. The blob format is {schema, table, columns, rows[]}. We
        use a parameterised INSERT per row so type adaptation is left
        to asyncpg (datetimes, JSONB, arrays etc)."""
        import json as _json

        text = payload.decode("utf-8") if isinstance(payload, (bytes, bytearray)) else str(payload)
        data = _json.loads(text)
        schema = data.get("schema") or "public"
        table = data.get("table")
        columns = data.get("columns") or []
        rows = data.get("rows") or []
        if not table or not columns or not rows:
            return 0

        col_list = ", ".join(f'"{c}"' for c in columns)
        placeholders = ", ".join(f"${i + 1}" for i in range(len(columns)))
        stmt = f'INSERT INTO "{schema}"."{table}" ({col_list}) VALUES ({placeholders})'

        # JSON has no native datetime so the backup serialised them
        # as ISO-8601 strings. asyncpg's parameter binder is strict —
        # a string can't go into a TIMESTAMP / DATE column. Detect
        # ISO-8601 strings and convert back to datetime/date so the
        # bind succeeds. Anything that doesn't parse passes through
        # unchanged so plain strings still make it to TEXT columns.
        from datetime import datetime as _dt, date as _date
        import re as _re
        _ISO_DT_RE = _re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")
        _ISO_DATE_RE = _re.compile(r"^\d{4}-\d{2}-\d{2}$")

        def _coerce(v):
            if not isinstance(v, str):
                return v
            if _ISO_DT_RE.match(v):
                # asyncpg accepts both naive and tz-aware datetimes;
                # fromisoformat handles both for Python 3.11+.
                try:
                    return _dt.fromisoformat(v.replace("Z", "+00:00"))
                except ValueError:
                    return v
            if _ISO_DATE_RE.match(v):
                try:
                    return _date.fromisoformat(v)
                except ValueError:
                    return v
            return v

        inserted = 0
        first_err: Optional[str] = None
        for r in rows:
            values = [_coerce(r.get(c)) for c in columns]
            try:
                await conn.execute(stmt, *values)
                inserted += 1
            except Exception as e:
                # One bad row shouldn't sink the whole table — keep
                # the first error so the operator can see WHY rows
                # are being skipped instead of getting a silent zero.
                if first_err is None:
                    first_err = str(e)[:200]
                continue
        if inserted == 0 and first_err:
            self._log(f"    all {len(rows)} rows failed for {schema}.{table}: {first_err}", "WARNING")
        return inserted

    @staticmethod
    def _derive_data_prefix(snapshot: Snapshot) -> str:
        """Reconstruct the blob-path prefix used by the backup handler when it
        isn't stored on the snapshot. Matches
        `{server_name}/{db_name}/{snapshot_id_12}/`."""
        server = snapshot.extra_data.get("server_name", "")
        db = snapshot.extra_data.get("database_name", "")
        sid = snapshot.id.hex[:12]
        return f"{server}/{db}/{sid}/"
