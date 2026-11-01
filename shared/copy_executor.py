"""SQL-only copy adapters. The caller owns admission, fencing and transaction end.

Manifest preparation is one consistent SQL statement and freezes full payloads,
not mutable source IDs. Its scan/materialization cost still needs the caller's
measured statement budget. Batches never read source payloads again.
"""
from sqlalchemy import text

from shared.chat_attachment_copy import (
    LEGACY_CHAT_ATTACHMENT_SELECTION_CTES,
    LegacyChatProvenanceGap,
)


KINDS = frozenset({"CHAT_CARRY", "MAIL_CARRY", "FILE_VERSIONS",
                   "LEGACY_CHAT_ATTACHMENTS", "CHAT_RENAME", "ARCHIVE_CARRY"})
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
        return (
            LEGACY_CHAT_ATTACHMENT_SELECTION_CTES + """,
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
    source_where = "s.id=CAST(:prior AS uuid)" if explicit else "true"
    prior = """WITH frozen_source AS MATERIALIZED (
        SELECT params->>'prior_snapshot_id' AS id FROM copy_work_tasks
         WHERE workflow_id=:workflow_id AND kind=:kind AND id<>:task_id
           AND prepared AND (params::jsonb) ? 'prior_snapshot_id'
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
                 AND (cw.state IS DISTINCT FROM 'COMPLETED' OR EXISTS (
                     SELECT 1 FROM copy_work_tasks ct WHERE ct.workflow_id=cw.id
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


async def prepare_manifest(session, task) -> None:
    """Freeze selections and predecessor atomically; never commit internally."""
    if task.kind not in KINDS:
        raise ValueError(f"unsupported copy kind: {task.kind}")
    scope, params = await _scope(session, task)
    if scope["prepared"]:
        task.prepared, task.params = True, params
        return
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
    binds = dict(params, task_id=task.id, workflow_id=task.workflow_id, kind=task.kind,
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
    ctes, provenance = _selection(task.kind, params)
    if task.kind not in {"LEGACY_CHAT_ATTACHMENTS", "CHAT_RENAME"}:
        provenance += " AND (NOT EXISTS (SELECT 1 FROM frozen_source WHERE id IS NOT NULL) OR EXISTS (SELECT 1 FROM source_choice))"
    # Pins use RESTRICT FKs: a concurrent delete cannot commit before both
    # manifest payloads and their source references have been made durable.
    result = (await session.execute(text(ctes + """,
        validity AS MATERIALIZED (
            SELECT (""" + provenance + """
                AND (CAST(:prior AS uuid) IS NULL OR EXISTS (SELECT 1 FROM source_choice))
                AND NOT EXISTS (SELECT 1 FROM selected
                    WHERE payload->>'tenant_id' IS NOT NULL
                      AND payload->>'tenant_id' <> CAST(CAST(:tid AS uuid) AS text))) AS ok
        ), stored AS (
            INSERT INTO copy_work_items(task_id,ordinal,payload,source_snapshot_id,phase)
            SELECT :task_id,row_number() OVER (ORDER BY phase,payload->>'external_id',payload->>'item_type',payload->>'id'),
                   payload::json,source_snapshot_id,phase
              FROM selected WHERE (SELECT ok FROM validity)
            RETURNING ordinal
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
    """), dict(binds, has_prior=task.kind not in {"LEGACY_CHAT_ATTACHMENTS", "CHAT_RENAME"}))).mappings().one()
    if not result["ok"]:
        if task.kind == "LEGACY_CHAT_ATTACHMENTS":
            raise LegacyChatProvenanceGap("legacy chat attachment provenance is ambiguous")
        raise ValueError("copy source snapshot or row tenant ownership is invalid")
    task.params, task.prepared = dict(result["params"] or {}), True


async def copy_manifest_batch(session, task, *, limit: int) -> dict:
    """Apply one phase/keyset batch. Caller persists returned checkpoint in this transaction."""
    if task.kind not in KINDS or not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
        raise ValueError("supported copy kind and positive integer batch limit required")
    scope, params = await _scope(session, task)
    if not scope["prepared"]:
        raise ValueError("copy manifest is not prepared")
    prefix = """WITH batch AS MATERIALIZED (
        SELECT * FROM copy_work_items WHERE task_id=:task_id AND ordinal>:checkpoint
          AND phase=(SELECT phase FROM copy_work_items WHERE task_id=:task_id AND ordinal>:checkpoint ORDER BY ordinal LIMIT 1)
         ORDER BY ordinal LIMIT :limit
    ), batch_validity AS MATERIALIZED (
        SELECT NOT EXISTS (
            SELECT 1 FROM batch b WHERE b.payload->>'item_type'='EMAIL_ATTACHMENT'
              AND NOT EXISTS (SELECT 1 FROM snapshot_items parent
                  WHERE parent.snapshot_id=:tsid AND parent.item_type='EMAIL'
                    AND parent.external_id=COALESCE(b.payload->>'parent_external_id',
                        split_part(b.payload->>'external_id','::',1)))) AS ok
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
             ORDER BY b.ordinal ON CONFLICT(snapshot_id,external_id,item_type) DO NOTHING
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
           "tsid": scope["target_snapshot_id"], "tid": scope["tenant_id"],
           "archive": task.kind == "ARCHIVE_CARRY", "legacy": bool(params.get("legacy_patterns_only", False))})).mappings().one()
    if not row["parents_valid"]:
        raise RuntimeError("required mail attachment parent is missing from target")
    return {"done": bool(row["done"]), "cursor": int(row["cursor"]), "rows": int(row["rows"]), "bytes": int(row["bytes"])}
