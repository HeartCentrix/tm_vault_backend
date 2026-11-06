"""SQL-only copy adapters. The caller owns admission, fencing and transaction end.

Manifest preparation is one consistent SQL statement and freezes full payloads,
not mutable source IDs. Its scan/materialization cost still needs the caller's
measured statement budget. Batches never read source payloads again.
"""
import os
import re

from sqlalchemy import text

from shared.chat_attachment_copy import (
    LEGACY_CHAT_ATTACHMENT_SELECTION_CTES,
    LegacyChatProvenanceGap,
)


KINDS = frozenset({"CHAT_CARRY", "MAIL_CARRY", "FILE_VERSIONS",
                   "LEGACY_CHAT_ATTACHMENTS", "CHAT_RENAME", "CHAT_LABELS", "CHAT_PRESENTATION", "ARCHIVE_CARRY"})
_CHAT_TYPES = "('TEAMS_CHAT_MESSAGE','TEAMS_MESSAGE','TEAMS_MESSAGE_REPLY','CHAT_MESSAGE','CHAT_ATTACHMENT','CHAT_HOSTED_CONTENT')"
_LEGACY_FOLDERS = """(si.folder_path LIKE 'chats/Untitled chat #%'
 OR si.folder_path LIKE 'chats/Untitled group #%'
 OR si.folder_path LIKE 'chats/1-on-1 Chat (19:%'
 OR si.folder_path LIKE 'chats/Group Chat (19:%'
 OR si.folder_path LIKE 'chats/Chat %')"""


async def _scope(session, task):
    row = (await session.execute(text("""
        SELECT w.tenant_id, w.resource_id, w.target_snapshot_id, w.job_id,
               t.prepared, t.params
          FROM copy_work_tasks t JOIN copy_workflows w ON w.id=t.workflow_id
          JOIN snapshots s ON s.id=w.target_snapshot_id
          JOIN resources r ON r.id=s.resource_id
         WHERE t.id=:task_id AND t.workflow_id=:workflow_id
           AND r.id=w.resource_id AND r.tenant_id=w.tenant_id
           AND s.job_id IS NOT DISTINCT FROM w.job_id
           AND s.status::text='IN_PROGRESS'
    """), {"task_id": task.id, "workflow_id": task.workflow_id})).mappings().one_or_none()
    if row is None:
        raise ValueError("copy target/workflow ownership is unavailable")
    params = dict(row["params"] or {})
    for key, expected in (("target_snapshot_id", row["target_snapshot_id"]),
                          ("tenant_id", row["tenant_id"]), ("resource_id", row["resource_id"])):
        if key in params and str(params[key]) != str(expected):
            raise ValueError(f"copy {key} does not match workflow target ownership")
    return row, params


def _selection(kind, params):
    """Return CTEs, source selector and validity expression; values stay bound."""
    if kind == "LEGACY_CHAT_ATTACHMENTS":
        selection = LEGACY_CHAT_ATTACHMENT_SELECTION_CTES
        if params.get('_copy_p8_sources_fixed'):
            # Filter before ranking/provenance resolution, so a newer source
            # outside the intent cannot hide an older protected candidate.
            selection = selection.replace('src.snapshot_id <> :tsid', """src.snapshot_id <> :tsid
                AND EXISTS (SELECT 1 FROM copy_work_items pin WHERE pin.task_id=:task_id
                    AND pin.ordinal<0 AND pin.source_snapshot_id=src.snapshot_id)""")
        return (
            selection + """,
            source_choice AS (SELECT NULL::uuid AS id),
            selected AS MATERIALIZED (
                SELECT src.snapshot_id AS source_snapshot_id, 0 AS phase,
                       (to_jsonb(src) || jsonb_build_object('parent_external_id', c.message_id)) AS payload
                  FROM chosen c JOIN snapshot_items src ON src.id=c.id
            )
            """,
            "NOT EXISTS (SELECT 1 FROM inspected WHERE source_chat_id IS NULL)",
        )
    if kind == "CHAT_RENAME":
        return (
            """WITH source_choice AS (SELECT NULL::uuid AS id),
            selected AS MATERIALIZED (
                SELECT NULL::uuid AS source_snapshot_id, 0 AS phase,
                       jsonb_build_object('id',si.id,'chat_id',nm.cid,'name',nm.dn) AS payload
                  FROM snapshot_items si
                  JOIN unnest(CAST(:cids AS text[]),CAST(:names AS text[])) AS nm(cid,dn)
                    ON si.metadata->>'chatId'=nm.cid
                 WHERE si.snapshot_id=:tsid AND si.folder_path <> 'chats/' || nm.dn
                   AND (NOT :legacy OR """ + _LEGACY_FOLDERS + ")\n)",
            "true",
        )
    explicit = params.get("prior_snapshot_id") or params.get("prior_snap_id")
    source_where = "s.id=CAST(:prior AS uuid)" if explicit else (
        "false" if params.get('_copy_source_fixed') else "true")
    prior = """WITH frozen_source AS MATERIALIZED (
        SELECT params->>'prior_snapshot_id' AS id FROM copy_work_tasks
         WHERE workflow_id=:workflow_id AND kind=:kind AND id<>:task_id
           AND capture_attempt=(SELECT capture_attempt FROM copy_work_tasks WHERE id=:task_id)
           AND (prepared OR COALESCE(params->>'_copy_source_fixed','false')='true')
           AND (params::jsonb) ? 'prior_snapshot_id'
         ORDER BY id LIMIT 1
    ), source_choice AS MATERIALIZED (
        SELECT s.id FROM snapshots s
         WHERE s.resource_id=:rid AND s.id<>:tsid
           AND s.status::text IN ('COMPLETED','PARTIAL')
           AND s.created_at < (SELECT created_at FROM snapshots WHERE id=:tsid)
           AND s.extra_data->>'cancelled_at' IS NULL
           AND COALESCE(s.extra_data->>'chat_carry_forward_failed','false') <> 'true'
           AND COALESCE(s.extra_data->>'copy_required_failed','false') <> 'true'
           AND (NOT EXISTS (SELECT 1 FROM frozen_source)
                OR s.id=CAST((SELECT id FROM frozen_source) AS uuid))
           AND NOT EXISTS (
               SELECT 1 FROM copy_workflows cw WHERE cw.target_snapshot_id=s.id
                 AND (cw.state IS DISTINCT FROM 'COMPLETED' OR cw.parent_settle_pending OR EXISTS (
                     SELECT 1 FROM copy_work_tasks ct WHERE ct.workflow_id=cw.id AND ct.capture_attempt=cw.capture_attempt
                       AND ct.state IS DISTINCT FROM 'COMPLETED')))
           AND """ + source_where
    if kind == "CHAT_CARRY":
        prior += " AND EXISTS (SELECT 1 FROM snapshot_items x WHERE x.snapshot_id=s.id AND x.item_type IN " + _CHAT_TYPES + ")"
    if kind == "MAIL_CARRY":
        prior += """
           AND s.job_id IS DISTINCT FROM CAST(:jid AS uuid)
           AND s.reuse_of_snapshot_id IS NULL AND s.reuse_chain_root_id IS NULL
           AND COALESCE(s.extra_data->>'mail_carry_forward_failed','false') <> 'true'
           AND COALESCE(s.extra_data->>'mail_delta_only','false') <> 'true'
           AND COALESCE(s.extra_data->>'mail_carry_forward_pending','false') <> 'true'
           AND (COALESCE(s.extra_data->>'mail_union_carry_claimed','false')='true'
                OR NOT EXISTS (SELECT 1 FROM snapshot_partitions sp
                                WHERE sp.snapshot_id=s.id AND sp.partition_type='MAIL_FOLDERS'
                                  AND (sp.payload::jsonb) ? 'carry_forward'))
           AND EXISTS (SELECT 1 FROM snapshot_items x WHERE x.snapshot_id=s.id
                        AND x.item_type IN ('EMAIL','EMAIL_ATTACHMENT'))
        """
    prior += " ORDER BY s.created_at DESC,s.id DESC LIMIT 1)"
    if kind == "MAIL_CARRY":
        return (prior + """,
            emails AS MATERIALIZED (
                SELECT src.* FROM snapshot_items src JOIN source_choice p ON src.snapshot_id=p.id
                 WHERE src.item_type='EMAIL'
                   AND COALESCE(src.metadata->>'source_mailbox','primary')='primary'
                   AND NOT (COALESCE(src.folder_path,'')=ANY(CAST(:excluded_paths AS text[])))
                   AND NOT (src.external_id=ANY(CAST(:tombstoned_ids AS text[])))
                   AND (NOT :whitelist OR (src.metadata->'raw'->>'parentFolderId')=ANY(CAST(:keep_folder_ids AS text[])))
                   AND (NOT :exclude_pathless OR COALESCE(src.folder_path,'')<>'')
            ), selected AS MATERIALIZED (
                SELECT src.snapshot_id AS source_snapshot_id,0 AS phase,to_jsonb(src) AS payload FROM emails src
                UNION ALL
                SELECT src.snapshot_id,1,to_jsonb(src)
                  FROM snapshot_items src JOIN source_choice p ON src.snapshot_id=p.id
                 WHERE src.item_type='EMAIL_ATTACHMENT'
                   AND (NOT :whitelist OR cardinality(CAST(:keep_folder_ids AS text[])) > 0) AND (
                     EXISTS (SELECT 1 FROM emails e WHERE e.external_id=COALESCE(src.parent_external_id,split_part(src.external_id,'::',1)))
                     OR EXISTS (SELECT 1 FROM snapshot_items e WHERE e.snapshot_id=:tsid AND e.item_type='EMAIL'
                                 AND e.external_id=COALESCE(src.parent_external_id,split_part(src.external_id,'::',1))))
            )
        """, "true")
    filters = {
        "CHAT_CARRY": "src.item_type IN " + _CHAT_TYPES,
        "FILE_VERSIONS": "src.item_type='FILE_VERSION' AND src.parent_external_id=ANY(CAST(:file_ids AS text[]))",
        "ARCHIVE_CARRY": "((src.item_type='ARCHIVE_ITEM' AND src.external_id=ANY(CAST(:external_ids AS text[]))) OR (src.item_type='ARCHIVE_ATTACHMENT' AND src.parent_external_id=ANY(CAST(:external_ids AS text[]))))",
    }
    return (prior + """, selected AS MATERIALIZED (
        SELECT src.snapshot_id AS source_snapshot_id,0 AS phase,to_jsonb(src) AS payload
          FROM snapshot_items src JOIN source_choice p ON src.snapshot_id=p.id
         WHERE """ + filters[kind] + ")", "true")


def _bindings(task, scope, params):
    return dict(params, task_id=task.id, workflow_id=task.workflow_id, kind=task.kind,
                 tid=scope["tenant_id"], rid=scope["resource_id"],
                 tsid=scope["target_snapshot_id"], jid=str(scope["job_id"]) if scope["job_id"] else None,
                 prior=params.get("prior_snapshot_id") or params.get("prior_snap_id"),
                 cid=params.get("chat_id"), cids=params.get("chat_ids", params.get("cids", [])), names=params.get("names", []),
                 legacy=bool(params.get("legacy_patterns_only", False)),
                 excluded_paths=list(params.get("excluded_paths") or []),
                 tombstoned_ids=list(params.get("tombstoned_ids") or []),
                 whitelist=params.get("keep_folder_ids") is not None,
                 keep_folder_ids=list(params.get("keep_folder_ids") or []),
                 exclude_pathless=bool(params.get("exclude_pathless", False)),
                 file_ids=list(params.get("file_ids") or []), external_ids=list(params.get("external_ids") or []))


async def pin_intent_sources(session, task) -> None:
    """Fix source scope before waiting; caller holds the workflow selection lock.

    P8 pins narrow candidate snapshot IDs without ranking provenance or copying
    payloads. Each intent is capped at COPY_INTENT_SOURCE_PIN_MAX (4096 default)
    and uses short statement/lock budgets. Exceeding a budget fails the required
    intent visibly; it never truncates sources. Full payloads freeze later.
    """
    if task.kind in {'CHAT_RENAME', 'CHAT_LABELS', 'CHAT_PRESENTATION'}:
        return
    scope, params = await _scope(session, task)
    p8 = task.kind == 'LEGACY_CHAT_ATTACHMENTS'
    if params.get('_copy_p8_sources_fixed' if p8 else '_copy_source_fixed'):
        return
    if p8 and not params.get('chat_id'):
        raise ValueError('legacy chat source intent requires chat_id')
    if task.kind in {'FILE_VERSIONS', 'ARCHIVE_CARRY'} and not (
            params.get('prior_snapshot_id') or params.get('prior_snap_id')):
        raise ValueError('copy source snapshot must be fixed explicitly')
    cap = int(os.getenv('COPY_INTENT_SOURCE_PIN_MAX', '4096'))
    statement_ms = int(os.getenv('COPY_INTENT_SOURCE_STATEMENT_MS', '5000'))
    lock_ms = int(os.getenv('COPY_INTENT_SOURCE_LOCK_MS', '1000'))
    if min(cap, statement_ms, lock_ms) < 1:
        raise ValueError('copy source pin limits and timeouts must be positive')
    await session.execute(text("SELECT set_config('statement_timeout',:statement,true),"
                               "set_config('lock_timeout',:lock,true)"),
                          {'statement': f'{statement_ms}ms', 'lock': f'{lock_ms}ms'})
    ctes, _ = _selection(task.kind, params)
    ids = ('SELECT DISTINCT snapshot_id AS id FROM candidates ORDER BY snapshot_id LIMIT :pin_probe'
           if p8 else 'SELECT id FROM source_choice WHERE id IS NOT NULL')
    fixed_valid = 'true' if p8 else """(CAST(:prior AS uuid) IS NULL OR EXISTS (SELECT 1 FROM source_choice))
        AND (NOT EXISTS (SELECT 1 FROM frozen_source WHERE id IS NOT NULL)
             OR EXISTS (SELECT 1 FROM source_choice))"""
    patch = ("jsonb_build_object('_copy_p8_sources_fixed',true,'_copy_p8_source_count',(SELECT count(*) FROM intent_source_ids))"
             if p8 else "jsonb_build_object('_copy_source_fixed',true,'_copy_source_implicit',CAST(:implicit AS boolean),"
             "'prior_snapshot_id',(SELECT id FROM source_choice))")
    # selected/ranked/provenance CTEs remain unreferenced and are not executed.
    row = (await session.execute(text(ctes + """,
        intent_source_ids AS MATERIALIZED (""" + ids + """),
        intent_locked_sources AS MATERIALIZED (
            SELECT s.id FROM snapshots s JOIN resources r ON r.id=s.resource_id
             WHERE s.id IN (SELECT id FROM intent_source_ids) AND r.tenant_id=:tid
               AND s.status::text <> 'PENDING_DELETION'
               AND (:allow_active OR (s.status::text IN ('COMPLETED','PARTIAL')
                    AND s.extra_data->>'cancelled_at' IS NULL
                    AND COALESCE(s.extra_data->>'copy_required_failed','false') <> 'true'))
               AND (SELECT count(*) FROM intent_source_ids) <= :pin_cap
             ORDER BY s.id FOR SHARE OF s
        ), intent_valid AS MATERIALIZED (
            SELECT (""" + fixed_valid + """
                AND (SELECT count(*) FROM intent_source_ids) <= :pin_cap
                AND NOT EXISTS (SELECT 1 FROM intent_source_ids ids WHERE NOT EXISTS (
                    SELECT 1 FROM intent_locked_sources locked WHERE locked.id=ids.id))) AS ok
        ), intent_pins AS (
            INSERT INTO copy_work_items(task_id,ordinal,payload,source_snapshot_id,phase)
            SELECT :task_id,-row_number() OVER(ORDER BY id),json_build_object('copy_source_pin',true),id,-1
              FROM intent_locked_sources WHERE (SELECT ok FROM intent_valid)
            ON CONFLICT(task_id,ordinal) DO NOTHING RETURNING ordinal
        ), fixed_intent AS (
            UPDATE copy_work_tasks SET params=(COALESCE(params::jsonb,'{}'::jsonb) || """ + patch + """)::json
             WHERE id=:task_id AND (SELECT ok FROM intent_valid) RETURNING params
        ) SELECT (SELECT ok FROM intent_valid) AS ok,(SELECT params FROM fixed_intent) AS params,
                 (SELECT count(*) FROM intent_pins) AS pins
    """), dict(_bindings(task, scope, params), pin_probe=cap + 1, pin_cap=cap,
               allow_active=p8, implicit=not bool(params.get('prior_snapshot_id') or params.get('prior_snap_id'))))).mappings().one()
    if not row['ok']:
        raise ValueError('copy source intent is unavailable, being deleted, or exceeds its pin limit')
    task.params = dict(row['params'])


async def prepare_manifest(session, task) -> None:
    """Freeze selections and predecessor atomically; never commit internally."""
    if task.kind not in KINDS:
        raise ValueError(f"unsupported copy kind: {task.kind}")
    scope, params = await _scope(session, task)
    if scope["prepared"]:
        task.prepared, task.params = True, params
        return
    if task.kind in {'CHAT_LABELS', 'CHAT_PRESENTATION'}:
        from shared.chat_presentation_work import prepare_presentation_manifest
        return await prepare_presentation_manifest(session, task, params)
    if task.kind in {"FILE_VERSIONS", "ARCHIVE_CARRY"} and not (
        params.get("prior_snapshot_id") or params.get("prior_snap_id")
    ):
        raise ValueError("copy source snapshot must be fixed explicitly")
    if task.kind == "LEGACY_CHAT_ATTACHMENTS" and not params.get("chat_id"):
        raise ValueError("legacy chat provenance requires chat_id")
    if task.kind == "CHAT_RENAME":
        cids, names = params.get("chat_ids", params.get("cids", [])), params.get("names", [])
        if len(cids) != len(names) or len(set(cids)) != len(cids) or any(not v for v in cids + names):
            raise ValueError("rename requires distinct chat ids and matching nonempty names")
    binds = _bindings(task, scope, params)
    ctes, provenance = _selection(task.kind, params)
    if task.kind not in {"LEGACY_CHAT_ATTACHMENTS", "CHAT_RENAME"}:
        provenance += " AND (NOT EXISTS (SELECT 1 FROM frozen_source WHERE id IS NOT NULL) OR EXISTS (SELECT 1 FROM source_choice))"
    # Pins use RESTRICT FKs: a concurrent delete cannot commit before both
    # manifest payloads and their source references have been made durable.
    statement = ctes + """,
        source_ids AS MATERIALIZED (
            SELECT source_snapshot_id AS id FROM selected WHERE source_snapshot_id IS NOT NULL
            UNION SELECT id FROM source_choice WHERE id IS NOT NULL
        ), pinned_sources AS MATERIALIZED (
            SELECT s.id FROM snapshots s JOIN resources r ON r.id=s.resource_id
             WHERE s.id IN (SELECT id FROM source_ids) AND r.tenant_id=:tid
               AND s.status::text <> 'PENDING_DELETION'
               AND (:allow_active OR (s.status::text IN ('COMPLETED','PARTIAL')
                    AND COALESCE(s.extra_data->>'copy_required_failed','false') <> 'true'))
             FOR SHARE OF s
        ),
        validity AS MATERIALIZED (
            SELECT (""" + provenance + """
                AND (CAST(:prior AS uuid) IS NULL OR EXISTS (SELECT 1 FROM source_choice))
                AND NOT EXISTS (SELECT 1 FROM source_ids ids
                    WHERE NOT EXISTS (SELECT 1 FROM pinned_sources pin WHERE pin.id=ids.id))
                AND NOT EXISTS (SELECT 1 FROM selected
                    WHERE payload->>'tenant_id' IS NOT NULL
                      AND payload->>'tenant_id' <> CAST(CAST(:tid AS uuid) AS text))) AS ok
        ), stored AS (
            INSERT INTO copy_work_items(task_id,ordinal,payload,source_snapshot_id,phase)
            SELECT :task_id,row_number() OVER (ORDER BY phase,(payload->>'external_id') COLLATE "C",(payload->>'item_type') COLLATE "C",(payload->>'id') COLLATE "C"),
                   payload::json,source_snapshot_id,phase
              FROM selected WHERE (SELECT ok FROM validity)
            RETURNING ordinal
        ), predecessor_pin AS (
            INSERT INTO copy_work_items(task_id,ordinal,payload,source_snapshot_id,phase)
            SELECT :task_id,-1,json_build_object('copy_source_pin',true),p.id,-1
              FROM source_choice p JOIN pinned_sources pin ON pin.id=p.id
             WHERE (SELECT ok FROM validity)
            ON CONFLICT(task_id,ordinal) DO NOTHING
        ), prepared_task AS (
            UPDATE copy_work_tasks SET prepared=true,
                   params=(COALESCE(params::jsonb,'{}'::jsonb) ||
                     CASE WHEN :has_prior THEN jsonb_build_object('prior_snapshot_id',(SELECT id FROM source_choice))
                          ELSE '{}'::jsonb END)::json
             WHERE id=:task_id AND (SELECT ok FROM validity)
            RETURNING params
        ) SELECT (SELECT ok FROM validity) AS ok,
                 (SELECT params FROM prepared_task) AS params,
                 (SELECT count(*) FROM stored) AS manifest_count
    """
    # Large selectors stay as durable rows and are aggregated on the server;
    # preparation never rebuilds every selector list in the worker heap.
    allowed={'file_ids','external_ids','excluded_paths','tombstoned_ids','keep_folder_ids','chat_ids','names'}
    for key in params.get('_copy_input_keys',[]):
        if key not in allowed: raise ValueError('unsupported durable copy selector')
        binding='cids' if key=='chat_ids' else key
        expression="(SELECT array_agg(inp.payload->>'value' ORDER BY inp.ordinal) FROM copy_work_inputs inp WHERE inp.task_id=:task_id AND inp.input_key='"+key+"')"
        statement=re.sub(r':'+binding+r'(?![A-Za-z0-9_])',expression,statement)
    result = (await session.execute(text(statement),dict(binds,has_prior=task.kind not in {'LEGACY_CHAT_ATTACHMENTS','CHAT_RENAME'},
        allow_active=task.kind in {'LEGACY_CHAT_ATTACHMENTS','CHAT_RENAME'}))).mappings().one()
    if not result["ok"]:
        if task.kind == "LEGACY_CHAT_ATTACHMENTS":
            raise LegacyChatProvenanceGap("legacy chat attachment provenance is ambiguous")
        raise ValueError("copy source snapshot or row tenant ownership is invalid")
    task.params, task.prepared = dict(result["params"] or {}), True


# Stored payload bytes per copy batch (always at least one row). Each batch statement must finish
# inside COPY_WORK_STATEMENT_TIMEOUT_MS (default 60 s).
BATCH_MAX_BYTES = int(os.getenv("COPY_BATCH_MAX_BYTES", str(8 * 1024 * 1024)))


async def copy_manifest_batch(session, task, *, limit: int) -> dict:
    """Apply one phase/keyset batch. Caller persists returned checkpoint in this transaction."""
    if task.kind not in KINDS or not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
        raise ValueError("supported copy kind and positive integer batch limit required")
    scope, params = await _scope(session, task)
    if not scope["prepared"]:
        raise ValueError("copy manifest is not prepared")
    if task.kind in {'CHAT_LABELS', 'CHAT_PRESENTATION'}:
        from shared.chat_presentation_work import apply_label_batch
        return await apply_label_batch(session, task, limit=limit)
    # Rows are also capped by stored payload bytes (always at least one row):
    # inline mail attachments make a 512-row batch tens of MB.
    prefix = """WITH batch AS MATERIALIZED (
        SELECT * FROM (SELECT w.*, sum(pg_column_size(w.payload)) OVER (ORDER BY w.ordinal) AS run_bytes
          FROM (SELECT * FROM copy_work_items WHERE task_id=:task_id AND ordinal>:checkpoint
          AND phase=(SELECT phase FROM copy_work_items WHERE task_id=:task_id AND ordinal>:checkpoint ORDER BY ordinal LIMIT 1)
         ORDER BY ordinal LIMIT :limit) w) sized
         WHERE run_bytes - pg_column_size(payload) < :max_bytes
    ), batch_parents AS MATERIALIZED (
        -- Read each stored json payload once. Correlated on the payload, a fresh-statistics plan
        -- re-parsed it for every target row it compared (10.5 s of a 10.7 s batch, locally).
        SELECT DISTINCT COALESCE(b.payload->>'parent_external_id',
                                 split_part(b.payload->>'external_id','::',1)) AS external_id
          FROM batch b WHERE b.payload->>'item_type'='EMAIL_ATTACHMENT'
    ), batch_validity AS MATERIALIZED (
        SELECT NOT EXISTS (
            SELECT 1 FROM batch_parents bp
             WHERE NOT EXISTS (SELECT 1 FROM snapshot_items parent
                  WHERE parent.snapshot_id=:tsid AND parent.item_type='EMAIL'
                    AND parent.external_id=bp.external_id)) AS ok
    ), changed AS (
    """
    if task.kind == "CHAT_RENAME":
        mutation = """UPDATE snapshot_items si
            SET folder_path='chats/' || (b.payload->>'name'),
                metadata=(COALESCE(si.metadata::jsonb,'{}'::jsonb) || jsonb_build_object('chatTopic',b.payload->>'name'))::json
            FROM batch b WHERE si.id=CAST(b.payload->>'id' AS uuid) AND si.snapshot_id=:tsid
              AND si.metadata->>'chatId'=b.payload->>'chat_id'
              AND si.folder_path <> 'chats/' || (b.payload->>'name')
              AND (NOT :legacy OR """ + _LEGACY_FOLDERS + ") RETURNING si.item_type,0::bigint AS content_size"
    else:
        mutation = """INSERT INTO snapshot_items(
            id,snapshot_id,tenant_id,item_type,external_id,parent_external_id,name,folder_path,
            content_hash,content_checksum,content_size,blob_path,encryption_key_id,backup_version,
            metadata,is_deleted,indexed_at,backend_id,chat_version_id,created_at)
            SELECT gen_random_uuid(),:tsid,COALESCE(p.tenant_id,:tid),p.item_type,p.external_id,p.parent_external_id,
                   p.name,p.folder_path,p.content_hash,p.content_checksum,p.content_size,p.blob_path,
                   p.encryption_key_id,COALESCE(p.backup_version,1),p.metadata,COALESCE(p.is_deleted,false),
                   p.indexed_at,p.backend_id,p.chat_version_id,NOW()
              FROM batch b CROSS JOIN LATERAL jsonb_populate_record(NULL::snapshot_items,b.payload::jsonb) p
             WHERE (SELECT ok FROM batch_validity)
             ORDER BY p.external_id COLLATE "C", p.item_type COLLATE "C", b.ordinal
             ON CONFLICT(snapshot_id,external_id,item_type) DO NOTHING
            RETURNING item_type,content_size
        """
    row = (await session.execute(text(prefix + mutation + """
        ) SELECT (SELECT ok FROM batch_validity) AS parents_valid,
            COALESCE((SELECT max(ordinal) FROM batch),:checkpoint) AS cursor,
            NOT EXISTS (SELECT 1 FROM copy_work_items WHERE task_id=:task_id
                AND ordinal>COALESCE((SELECT max(ordinal) FROM batch),:checkpoint)) AS done,
            (SELECT count(*) FROM changed WHERE NOT :archive OR item_type='ARCHIVE_ITEM') AS rows,
            (SELECT COALESCE(sum(content_size),0) FROM changed) AS bytes
    """), {"task_id": task.id, "checkpoint": int(task.checkpoint or 0), "limit": limit,
           "max_bytes": BATCH_MAX_BYTES, "tsid": scope["target_snapshot_id"], "tid": scope["tenant_id"],
           "archive": task.kind == "ARCHIVE_CARRY", "legacy": bool(params.get("legacy_patterns_only", False))})).mappings().one()
    if not row["parents_valid"]:
        raise RuntimeError("required mail attachment parent is missing from target")
    return {"done": bool(row["done"]), "cursor": int(row["cursor"]), "rows": int(row["rows"]), "bytes": int(row["bytes"])}
