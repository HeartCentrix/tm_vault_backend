"""Explicit additive migration for the protected-size retained-reference ledger.

No history is read here. Transition-table capture is transactional and never
locks a resource counter. Event triggers keep direct leaf writes covered when
partitions are created/attached later; installing requires that DDL privilege.
"""
from sqlalchemy import text

DDL = (
    'ALTER TABLE resource_protected_bytes ALTER COLUMN latest_snapshot_id DROP NOT NULL',
    'ALTER TABLE resource_protected_bytes ADD COLUMN IF NOT EXISTS working_bytes bigint NOT NULL DEFAULT 0',
    'ALTER TABLE resource_protected_bytes ADD COLUMN IF NOT EXISTS ledger_published boolean NOT NULL DEFAULT false',
    'ALTER TABLE resource_protected_bytes ADD COLUMN IF NOT EXISTS last_turn timestamptz NOT NULL DEFAULT \'epoch\'',
    'ALTER TABLE resource_protected_bytes ADD COLUMN IF NOT EXISTS fence bigint NOT NULL DEFAULT 0',
    'ALTER TABLE resource_protected_bytes ADD COLUMN IF NOT EXISTS next_attempt timestamptz NOT NULL DEFAULT \'epoch\'',
    '''CREATE TABLE protected_size_control(singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
        discovery_cursor uuid, discovery_done boolean NOT NULL DEFAULT false,
        discovery_epoch bigint NOT NULL DEFAULT 0,instance_id uuid NOT NULL DEFAULT gen_random_uuid(),
        owner uuid, lease_until timestamptz, fence bigint NOT NULL DEFAULT 0)''',
    'INSERT INTO protected_size_control(singleton) VALUES(true)',
    '''CREATE TABLE protected_size_snapshots(snapshot_id uuid PRIMARY KEY,resource_id uuid NOT NULL,
        eligible boolean NOT NULL)''',
    'CREATE INDEX ON protected_size_snapshots(resource_id) WHERE eligible',
    '''CREATE TABLE protected_size_journal(id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        kind text NOT NULL CHECK(kind IN ('items','snapshots')),snapshot_ids uuid[] NOT NULL,
        item_ids uuid[] NOT NULL,created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        CHECK(cardinality(snapshot_ids)<=1000 AND cardinality(item_ids)<=1000))''',
    '''CREATE TABLE protected_size_targets(event_id bigint REFERENCES protected_size_journal(id),
        resource_id uuid NOT NULL,acknowledged_at timestamptz,processed_offset integer NOT NULL DEFAULT 0,PRIMARY KEY(resource_id,event_id))''',
    'CREATE INDEX ON protected_size_targets(event_id)',
    'CREATE INDEX ON protected_size_targets(acknowledged_at)',
    'CREATE INDEX ON protected_size_journal(created_at)',
    '''CREATE TABLE protected_size_orphans(resource_id uuid PRIMARY KEY)''',
    'CREATE INDEX ON protected_size_targets(resource_id,event_id) WHERE acknowledged_at IS NULL',
    '''CREATE TABLE protected_size_jobs(resource_id uuid NOT NULL,snapshot_id uuid NOT NULL,cursor uuid,
        phase text NOT NULL DEFAULT 'source',PRIMARY KEY(resource_id,snapshot_id))''',
    '''CREATE TABLE protected_size_refs(resource_id uuid NOT NULL,snapshot_id uuid NOT NULL,item_id uuid NOT NULL,
        identity text NOT NULL,created_at timestamp,size bigint NOT NULL,
        PRIMARY KEY(resource_id,snapshot_id,item_id))''',
    'CREATE INDEX protected_size_ref_winner ON protected_size_refs(resource_id,identity,created_at DESC NULLS FIRST,item_id DESC)',
    'CREATE INDEX protected_size_ref_item ON protected_size_refs(resource_id,item_id)',
    '''CREATE TABLE protected_size_tenant_turns(tenant_id uuid PRIMARY KEY,last_turn timestamptz NOT NULL)''',
    '''CREATE TABLE protected_size_winners(resource_id uuid NOT NULL,identity text NOT NULL,
        size bigint NOT NULL,PRIMARY KEY(resource_id,identity))''',
    'CREATE INDEX IF NOT EXISTS ix_protected_size_snapshot_items_identity ON snapshot_items(id)',
    'CREATE INDEX IF NOT EXISTS ix_protected_size_snapshot_items_page ON snapshot_items(snapshot_id,id)',
    'CREATE INDEX IF NOT EXISTS ix_protected_size_snapshots_page ON snapshots(id)',
)


def statements(schema):
    """SQL shared by Alembic and disposable integration fixtures."""
    q = '"' + schema.replace('"', '""') + '"'
    event_name = '"protected_size_' + schema.replace('"', '""') + '"'
    yield from DDL
    yield f'''CREATE FUNCTION protected_size_capture() RETURNS trigger LANGUAGE plpgsql SET search_path={q} AS $fn$
    DECLARE source_sql text; batch record; eid bigint; ids uuid[]; resources uuid[];
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
          DELETE FROM protected_size_snapshots WHERE snapshot_id IN (SELECT id FROM old_rows);
        ELSE
          INSERT INTO protected_size_snapshots SELECT id,resource_id,
            COALESCE(status IN ('COMPLETED','PARTIAL','PENDING_DELETION'),false) FROM new_rows
          ON CONFLICT(snapshot_id) DO UPDATE SET resource_id=EXCLUDED.resource_id,eligible=EXCLUDED.eligible;
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
          resources:=batch.parents;
          INSERT INTO protected_size_journal(kind,snapshot_ids,item_ids) VALUES('snapshots',batch.ids,'{{}}') RETURNING id INTO eid;
        ELSE
          SELECT array_agg(DISTINCT resource_id) INTO resources FROM snapshots WHERE id=ANY(batch.parents);
          INSERT INTO protected_size_journal(kind,snapshot_ids,item_ids) VALUES('items','{{}}',batch.ids) RETURNING id INTO eid;
        END IF;
        INSERT INTO protected_size_targets(event_id,resource_id)
          SELECT eid,r FROM unnest(resources) r WHERE r IS NOT NULL GROUP BY r ON CONFLICT DO NOTHING;
      END LOOP;
      RETURN NULL;
    END $fn$'''
    yield f'''CREATE FUNCTION protected_size_install_triggers() RETURNS void LANGUAGE plpgsql SET search_path={q} AS $fn$
    DECLARE relation record; operation text; transition text;
    BEGIN
      FOR relation IN
        SELECT c.oid,c.relname,n.nspname,CASE WHEN c.oid='snapshots'::regclass OR
          c.oid IN (SELECT relid FROM pg_partition_tree('snapshots'::regclass)) THEN 'snapshots' ELSE 'items' END kind
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE c.oid IN ('snapshots'::regclass,'snapshot_items'::regclass)
          OR c.oid IN (SELECT relid FROM pg_partition_tree('snapshots'::regclass))
          OR c.oid IN (SELECT relid FROM pg_partition_tree('snapshot_items'::regclass))
      LOOP
        FOREACH operation IN ARRAY ARRAY['INSERT','UPDATE','DELETE'] LOOP
          IF NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid=relation.oid AND tgname='protected_size_'||lower(operation)) THEN
            transition:=CASE operation WHEN 'INSERT' THEN 'NEW TABLE AS new_rows' WHEN 'DELETE' THEN 'OLD TABLE AS old_rows'
              ELSE 'OLD TABLE AS old_rows NEW TABLE AS new_rows' END;
            EXECUTE format('CREATE TRIGGER %I AFTER %s ON %I.%I REFERENCING %s FOR EACH STATEMENT EXECUTE FUNCTION {q}.protected_size_capture(%L)',
              'protected_size_'||lower(operation),operation,relation.nspname,relation.relname,transition,relation.kind);
          END IF;
        END LOOP;
      END LOOP;
    END $fn$'''
    yield 'SELECT protected_size_install_triggers()'
    yield f'''CREATE FUNCTION protected_size_resource_delete() RETURNS trigger LANGUAGE plpgsql SET search_path={q} AS $fn$
      BEGIN INSERT INTO protected_size_orphans SELECT id FROM resource_old ON CONFLICT DO NOTHING; RETURN NULL; END $fn$'''
    yield '''CREATE TRIGGER protected_size_resource_delete AFTER DELETE ON resources REFERENCING OLD TABLE AS resource_old
      FOR EACH STATEMENT EXECUTE FUNCTION protected_size_resource_delete()'''
    yield f'''CREATE FUNCTION protected_size_partition_ddl() RETURNS event_trigger LANGUAGE plpgsql SET search_path={q} AS $fn$
      BEGIN
        IF EXISTS(SELECT 1 FROM pg_event_trigger_ddl_commands() d
          WHERE d.classid='pg_class'::regclass AND
            (d.objid IN ('snapshots'::regclass,'snapshot_items'::regclass)
             OR EXISTS(SELECT 1 FROM pg_trigger t WHERE t.tgrelid=d.objid AND t.tgfoid='protected_size_capture()'::regprocedure)
             OR d.objid IN (SELECT relid FROM pg_partition_tree('snapshot_items'::regclass))
             OR d.objid IN (SELECT relid FROM pg_partition_tree('snapshots'::regclass)))) THEN
          PERFORM protected_size_install_triggers();
          -- DDL can attach prepopulated rows or detach uncaptured history.
          -- Restart compact snapshot discovery, never a synchronous item walk.
          UPDATE protected_size_control SET discovery_done=false,discovery_cursor=NULL,discovery_epoch=discovery_epoch+1;
        END IF;
      END $fn$'''
    yield f'''CREATE EVENT TRIGGER {event_name} ON ddl_command_end WHEN TAG IN ('CREATE TABLE','ALTER TABLE')
      EXECUTE FUNCTION protected_size_partition_ddl()'''


async def install(connection):
    schema = await connection.scalar(text('SELECT current_schema()'))
    for statement in statements(schema):
        await connection.execute(text(statement))
