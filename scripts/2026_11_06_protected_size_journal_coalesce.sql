-- Generated from shared.protected_size_schema.capture_function("public") (alembic 0023). Run in one transaction.
BEGIN;
SET LOCAL search_path TO public;
CREATE OR REPLACE FUNCTION protected_size_capture() RETURNS trigger LANGUAGE plpgsql SET search_path="public" AS $fn$
    DECLARE source_sql text; batch record; eid bigint; tail bigint; ids uuid[]; resources uuid[];
    BEGIN
      IF TG_ARGV[0]='snapshots' THEN
        IF TG_OP='INSERT' THEN source_sql := 'SELECT id,resource_id,status FROM new_rows';
        ELSIF TG_OP='DELETE' THEN source_sql := 'SELECT id,resource_id,status FROM old_rows';
        ELSE source_sql := 'SELECT n.id,n.resource_id,n.status FROM new_rows n JOIN old_rows o USING(id)
            WHERE n.resource_id IS DISTINCT FROM o.resource_id OR
              (n.status IN (''COMPLETED'',''PARTIAL'',''PENDING_DELETION'')) IS DISTINCT FROM
              (o.status IN (''COMPLETED'',''PARTIAL'',''PENDING_DELETION''))
            UNION ALL SELECT o.id,o.resource_id,o.status FROM old_rows o JOIN new_rows n USING(id)
            WHERE n.resource_id IS DISTINCT FROM o.resource_id'; END IF;
        IF TG_OP='DELETE' THEN
          INSERT INTO protected_size_snapshots(snapshot_id,resource_id,eligible,present,revision)
            SELECT id,resource_id,false,false,1 FROM old_rows
          ON CONFLICT(snapshot_id) DO UPDATE SET eligible=false,present=false,revision=protected_size_snapshots.revision+1;
        ELSE
          INSERT INTO protected_size_snapshots(snapshot_id,resource_id,eligible) SELECT id,resource_id,
            COALESCE(status IN ('COMPLETED','PARTIAL','PENDING_DELETION'),false) FROM new_rows
          ON CONFLICT(snapshot_id) DO UPDATE SET resource_id=EXCLUDED.resource_id,eligible=EXCLUDED.eligible,present=true,revision=protected_size_snapshots.revision+1;
        END IF;
      ELSE
        IF TG_OP='INSERT' THEN source_sql := 'SELECT id,snapshot_id FROM new_rows';
        ELSIF TG_OP='DELETE' THEN source_sql := 'SELECT id,snapshot_id FROM old_rows';
        ELSE source_sql := 'SELECT n.id,n.snapshot_id FROM new_rows n FULL JOIN old_rows o ON n.id=o.id AND n.snapshot_id=o.snapshot_id
            WHERE (n.id,n.snapshot_id,n.external_id,n.created_at,n.content_size,n.is_deleted)
            IS DISTINCT FROM (o.id,o.snapshot_id,o.external_id,o.created_at,o.content_size,o.is_deleted)
            AND n.id IS NOT NULL UNION SELECT o.id,o.snapshot_id FROM new_rows n FULL JOIN old_rows o ON n.id=o.id AND n.snapshot_id=o.snapshot_id
            WHERE (n.id,n.snapshot_id,n.external_id,n.created_at,n.content_size,n.is_deleted)
            IS DISTINCT FROM (o.id,o.snapshot_id,o.external_id,o.created_at,o.content_size,o.is_deleted) AND o.id IS NOT NULL'; END IF;
      END IF;
      FOR batch IN EXECUTE 'SELECT array_agg(id) ids,array_agg(' ||
          CASE WHEN TG_ARGV[0]='snapshots' THEN 'resource_id' ELSE 'snapshot_id' END || ') parents
          FROM (SELECT *,((row_number() OVER())-1)/1000 bucket FROM ('||source_sql||') changed) numbered GROUP BY bucket'
      LOOP
        IF TG_ARGV[0]='snapshots' THEN
          resources:=batch.parents; batch.parents:=batch.ids; batch.ids:='{}';
        ELSE
          SELECT array_agg(DISTINCT resource_id) INTO resources FROM snapshots WHERE id=ANY(batch.parents);
        END IF;
        -- Per-row statements (ORM executemany) fire once per row. This transaction's own
        -- journal row is uncommitted, so no worker has read it: extend it up to the cap
        -- instead of one event per row. GUC is transaction-local and reverts on savepoint rollback.
        eid:=NULL;
        tail:=NULLIF(current_setting('protected_size.tail_efa1f375d76194fa_'||TG_ARGV[0],true),'')::bigint;
        IF tail IS NOT NULL THEN
          UPDATE protected_size_journal SET snapshot_ids=snapshot_ids||batch.parents,item_ids=item_ids||batch.ids
            WHERE id=tail AND kind=TG_ARGV[0] AND cardinality(snapshot_ids)+cardinality(batch.parents)<=1000 RETURNING id INTO eid;
        END IF;
        IF eid IS NULL THEN
          INSERT INTO protected_size_journal(kind,snapshot_ids,item_ids) VALUES(TG_ARGV[0],batch.parents,batch.ids) RETURNING id INTO eid;
          PERFORM set_config('protected_size.tail_efa1f375d76194fa_'||TG_ARGV[0],eid::text,true);
        END IF;
        IF TG_ARGV[0]='items' THEN
          INSERT INTO protected_size_event_snapshots(snapshot_id,event_id)
            SELECT sid,eid FROM unnest(batch.parents) sid GROUP BY sid ON CONFLICT DO NOTHING;
        END IF;
        INSERT INTO protected_size_targets(event_id,resource_id)
          SELECT eid,r FROM unnest(resources) r WHERE r IS NOT NULL GROUP BY r ON CONFLICT DO NOTHING;
      END LOOP;
      RETURN NULL;
    END $fn$;
DO $fn$ DECLARE r text; BEGIN
  FOR r IN SELECT DISTINCT grantee FROM information_schema.role_table_grants
    WHERE table_schema=current_schema() AND table_name='protected_size_journal' AND privilege_type='INSERT'
  LOOP EXECUTE pg_catalog.format('GRANT UPDATE ON protected_size_journal TO %I',r); END LOOP;
END $fn$;
COMMIT;
