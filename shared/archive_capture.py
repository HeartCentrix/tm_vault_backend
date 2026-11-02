"""Transactional archive census, exact item receipts and completion evidence.

All methods close transactions before returning to a provider or storage caller.
Folder identity is qualified by mailbox route, never by its display path.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
from urllib.parse import urlsplit, urlunsplit
from uuid import UUID, uuid4

from sqlalchemy import func, select, text, update
from sqlalchemy.dialects.postgresql import insert

from shared.models import ArchiveFolderWork as Folder, ArchiveItemWork as Item
from shared.models import Job, Resource, Tenant, Snapshot, SnapshotItem, SnapshotPartition, CopyWorkTask


class ArchiveConflict(RuntimeError):
    pass


class ArchivePageError(ValueError):
    pass


def digest(*parts):
    return hashlib.sha256(json.dumps(parts, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def canonical_base(url, *, expected=None):
    """Normalize host/scheme and API literals; preserve opaque ID case."""
    p = urlsplit(url)
    parts = p.path.strip('/').split('/')
    if (p.scheme.lower() != 'https' or p.hostname != 'graph.microsoft.com'
            or p.username or p.password or p.port or p.query or p.fragment
            or len(parts) != 6 or parts[0] not in ('beta', 'v1.0')
            or [v.lower() for v in parts[1:4]] != ['admin', 'exchange', 'mailboxes']
            or not parts[4] or parts[5].lower() != 'folders'):
        raise ArchivePageError('invalid archive mailbox route')
    result = f'https://graph.microsoft.com/{parts[0]}/admin/exchange/mailboxes/{parts[4]}/folders'
    if expected and urlsplit(expected).path.split('/')[1] != parts[0]:
        raise ArchivePageError('archive API version changed')
    return result


def page_base(page, fallback):
    context = page.get('@odata.context')
    if not context:
        return fallback
    if not isinstance(context, str) or '$metadata#' not in context:
        raise ArchivePageError('invalid archive context')
    root, _, fragment = context.partition('$metadata#')
    idx = fragment.lower().find('/folders')
    if idx < 0:
        raise ArchivePageError('invalid archive context')
    return canonical_base(root + fragment[:idx].strip('/') + '/folders', expected=fallback)


def folder_base(folder, fallback):
    explicit = folder.get('parentMailboxUrl')
    if not explicit:
        return fallback
    if not isinstance(explicit, str):
        raise ArchivePageError('invalid folder mailbox route')
    return canonical_base(explicit.rstrip('/') + '/folders', expected=fallback)


def validate_cursor(url, base, phase, folder_id):
    p = urlsplit(url)
    b = urlsplit(base)
    expected_path = b.path if phase == 'root' else f'{b.path}/{folder_id}/{"items" if phase == "items" else "childFolders"}'
    if (p.scheme != 'https' or p.netloc != b.netloc or p.path != expected_path
            or p.fragment or p.username or p.password):
        raise ArchivePageError('archive continuation changed scope')
    return url


@dataclass(frozen=True)
class ArchiveFence:
    snapshot_id: UUID
    job_id: UUID
    resource_id: UUID
    tenant_id: UUID
    snapshot_token: int
    job_token: int | None
    snapshot_owner: UUID | None = None
    partition_id: UUID | None = None
    partition_token: int | None = None
    partition_owner: UUID | None = None


class ArchiveCapture:
    def __init__(self, factory, fence, *, max_locations=8):
        self.factory, self.fence = factory, fence
        self.generation = None
        self.max_locations = max(1, min(8, max_locations))

    async def lock(self, db):
        f = self.fence
        job = await db.get(Job, f.job_id, with_for_update=True, populate_existing=True)
        snap = await db.get(Snapshot, f.snapshot_id, with_for_update=True, populate_existing=True)
        part = await db.get(SnapshotPartition, f.partition_id, with_for_update=True, populate_existing=True) if f.partition_id else None
        resource = await db.get(Resource, f.resource_id)
        tenant = await db.get(Tenant, f.tenant_id)
        if (not job or not snap or not resource or resource.tenant_id != f.tenant_id
                or job.tenant_id != f.tenant_id or snap.job_id != f.job_id
                or snap.resource_id != f.resource_id or (f.partition_id and (not part or part.snapshot_id != snap.id))):
            raise ArchiveConflict('archive parent identity changed')
        if (snap.lease_token != f.snapshot_token or snap.lease_owner_id != f.snapshot_owner
                or (f.job_token is not None and job.lease_token != f.job_token)
                or (part and (part.lease_token != f.partition_token or part.lease_owner_id != f.partition_owner))):
            raise ArchiveConflict('archive parent generation changed')
        if f.partition_id is None and f.job_token is None:
            raise ArchiveConflict('archive requires captured job generation')
        extra = snap.extra_data or {}
        if (not tenant or tenant.status.value == 'PENDING_DELETION' or resource.deletion_queued_at
                or job.status.value not in ('RUNNING', 'QUEUED', 'PENDING', 'RETRYING')
                or snap.status.value != 'IN_PROGRESS' or (part and part.status != 'IN_PROGRESS')
                or any(extra.get(k) for k in ('cancelled_at', 'pending_deletion', 'deletion_pending'))):
            raise ArchiveConflict('archive parent is not active')
        return snap

    def params(self, **extra):
        return dict(sid=self.fence.snapshot_id, gen=self.generation, **extra)

    async def begin(self, base, visibility):
        base = canonical_base(base)
        async with self.factory.begin() as db:
            snap = await self.lock(db)
            context = (snap.extra_data or {}).get('archive_capture')
            source = {'base': base, 'visibility': str(visibility), 'tenant_id': str(self.fence.tenant_id), 'resource_id': str(self.fence.resource_id)}
            if context:
                if context.get('version') != 1 or context.get('source') != source:
                    raise ArchiveConflict('archive capture source context changed')
                self.generation = UUID(context['generation'])
            else:
                self.generation = uuid4()
                snap.extra_data = {**(snap.extra_data or {}), 'archive_capture': {
                    'version': 1, 'generation': str(self.generation), 'source': source, 'phase': 'census'}}
            await self._location(db, base)
        await self.recover()

    async def recover(self):
        async with self.factory.begin() as db:
            await self.lock(db)
            await db.execute(update(Folder).where(Folder.snapshot_id == self.fence.snapshot_id,
                Folder.capture_generation == self.generation, Folder.state == 'reading').values(state='pending'))
            await db.execute(update(Item).where(Item.snapshot_id == self.fence.snapshot_id,
                Item.capture_generation == self.generation, Item.state == 'exporting').values(state='export_pending'))

    async def _location(self, db, base):
        key = digest(base, '', 'root')
        existing = await db.get(Folder, (self.fence.snapshot_id, self.generation, key))
        if existing:
            return
        count = await db.scalar(select(func.count()).select_from(Folder).where(Folder.snapshot_id == self.fence.snapshot_id,
            Folder.capture_generation == self.generation, Folder.phase == 'root', Folder.state != 'excluded'))
        over = count >= self.max_locations
        db.add(Folder(snapshot_id=self.fence.snapshot_id, capture_generation=self.generation, scope_key=key,
            phase='root', base_url=base, folder_id='', next_url=base,
            state='unresolved' if over else 'pending', gap_kind='location_cap' if over else None))
        await db.flush()

    async def add_location(self, base):
        async with self.factory.begin() as db:
            await self.lock(db)
            await self._location(db, canonical_base(base))

    async def claim_scope(self):
        async with self.factory.begin() as db:
            await self.lock(db)
            row = await db.scalar(select(Folder).where(Folder.snapshot_id == self.fence.snapshot_id,
                Folder.capture_generation == self.generation, Folder.state == 'pending',
                Folder.phase.in_(('root', 'items', 'children')))
                .order_by(Folder.phase.desc(), Folder.scope_key).limit(1).with_for_update(skip_locked=True))
            if row is None:
                return None
            row.state = 'reading'
            row.logical_attempts += 1
            return {c.name: getattr(row, c.key) for c in Folder.__table__.columns}

    async def _scope(self, db, scope):
        row = await db.get(Folder, (self.fence.snapshot_id, self.generation, scope['scope_key']), with_for_update=True)
        if row is None or row.scope_generation != scope['scope_generation']:
            raise ArchiveConflict('archive scope generation changed')
        return row

    async def page(self, scope, page, request_url):
        if not isinstance(page, dict) or not isinstance(page.get('value'), list):
            raise ArchivePageError('archive collection is malformed')
        if len(page['value']) > 10000:
            raise ArchivePageError('archive page descriptor limit exceeded')
        base = page_base(page, scope['base_url'])
        # Redirect context proves the route of this response; no display-name alias.
        nxt = page.get('@odata.nextLink')
        if nxt is not None:
            validate_cursor(nxt, base, scope['phase'], scope['folder_id'])
        fingerprint = digest(page)
        receipt_key = digest('page', scope['scope_key'], scope['scope_generation'], request_url)
        async with self.factory.begin() as db:
            await self.lock(db)
            row = await self._scope(db, scope)
            receipt = await db.get(Folder, (self.fence.snapshot_id, self.generation, receipt_key))
            if receipt:
                if receipt.evidence.get('digest') != fingerprint:
                    raise ArchiveConflict('archive page replay changed evidence')
                return
            if row.state != 'reading' or row.next_url != request_url:
                raise ArchiveConflict('archive page cursor moved')
            if nxt:
                cycle = await db.get(Folder, (self.fence.snapshot_id, self.generation,
                    digest('page', scope['scope_key'], scope['scope_generation'], nxt)))
                if nxt == request_url or cycle:
                    raise ArchivePageError('archive continuation cycle')
            await self._location(db, base)
            ordinal = row.page_ordinal + 1
            inserted_count = 0
            if row.phase == 'items':
                values = []
                for item in page['value']:
                    if not isinstance(item, dict) or not isinstance(item.get('id'), str) or not item['id']:
                        raise ArchivePageError('archive item identity missing')
                    values.append(dict(snapshot_id=self.fence.snapshot_id, capture_generation=self.generation,
                        scope_key=row.scope_key, scope_generation=row.scope_generation, external_id=item['id'],
                        location=base, folder_id=row.folder_id, change_key=item.get('changeKey'), item_type=item.get('type'),
                        size=item.get('size'), display_path=row.display_path, page_ordinal=ordinal, state='discovered', receipt={}))
                # Bound DB statements by page size, rather than one round trip per descriptor.
                unique = {}
                for value in values:
                    old = unique.get(value['external_id'])
                    if old and (old['change_key'], old['location']) != (value['change_key'], value['location']):
                        value['state'], value['gap_kind'] = 'failed', 'source_changed'
                    else:
                        value['gap_kind'] = None
                    unique[value['external_id']] = value
                for start in range(0, len(unique), 200):
                    stmt = insert(Item).values(list(unique.values())[start:start + 200])
                    # Same occurrence with changed source proof is never last-writer-wins.
                    changes = await db.execute(stmt.on_conflict_do_update(index_elements=['snapshot_id', 'capture_generation', 'scope_key', 'scope_generation', 'external_id'],
                        set_={'state': text("CASE WHEN EXCLUDED.gap_kind IS NOT NULL OR archive_item_work.change_key IS DISTINCT FROM EXCLUDED.change_key OR archive_item_work.location <> EXCLUDED.location THEN 'failed' ELSE archive_item_work.state END"),
                              'gap_kind': text("CASE WHEN EXCLUDED.gap_kind IS NOT NULL OR archive_item_work.change_key IS DISTINCT FROM EXCLUDED.change_key OR archive_item_work.location <> EXCLUDED.location THEN 'source_changed' ELSE archive_item_work.gap_kind END")}).returning(text('(xmax = 0) AS inserted')))
                    inserted_count += sum(bool(r[0]) for r in changes)
            else:
                for child in page['value']:
                    if not isinstance(child, dict) or not isinstance(child.get('id'), str) or not child['id']:
                        raise ArchivePageError('archive folder identity missing')
                    route = folder_base(child, base)
                    await self._location(db, route)
                    path = row.display_path + '/' + str(child.get('displayName') or child['id'])
                    declared = child.get('childFolderCount')
                    for phase in ('items', 'children'):
                        if phase == 'children' and declared == 0:
                            continue
                        key = digest(route, child['id'], phase)
                        old = await db.get(Folder, (self.fence.snapshot_id, self.generation, key))
                        if old is None:
                            db.add(Folder(snapshot_id=self.fence.snapshot_id, capture_generation=self.generation,
                                scope_key=key, phase=phase, base_url=route, folder_id=child['id'],
                                parent_scope_key=row.scope_key, parent_generation=row.scope_generation,
                                display_path=path, declared_count=declared if phase == 'children' else child.get('totalItemCount'),
                                next_url=f'{route}/{child["id"]}/{"items" if phase == "items" else "childFolders"}',
                                evidence={'request_base': row.base_url, 'response_base': base, 'explicit_route': bool(child.get('parentMailboxUrl'))}))
                        elif old.state == 'excluded':
                            old.state = 'pending'
                            old.scope_generation += 1
                            old.page_ordinal = old.observed_count = 0
                            old.terminal_page = False
                            old.next_url = f'{route}/{child["id"]}/{"items" if phase == "items" else "childFolders"}'
                            old.parent_generation = row.scope_generation
                            old.declared_count = declared if phase == 'children' else child.get('totalItemCount')
                        elif old.base_url != route or old.folder_id != child['id']:
                            raise ArchiveConflict('archive scope hash collision')
            db.add(Folder(snapshot_id=self.fence.snapshot_id, capture_generation=self.generation,
                scope_key=receipt_key, phase='receipt', base_url=base, folder_id=row.folder_id,
                parent_scope_key=row.scope_key, parent_generation=row.scope_generation, state='complete',
                terminal_page=True, evidence={'digest': fingerprint}, page_ordinal=ordinal))
            row.page_ordinal = ordinal
            row.next_url = nxt
            row.terminal_page = not nxt
            await db.flush()
            if row.phase == 'items':
                row.observed_count += inserted_count
            else:
                row.observed_count = await db.scalar(select(func.count()).select_from(Folder).where(
                    Folder.snapshot_id == self.fence.snapshot_id, Folder.capture_generation == self.generation,
                    Folder.parent_scope_key == row.scope_key, Folder.parent_generation == row.scope_generation,
                    Folder.phase == 'items', Folder.state != 'excluded'))
            if not nxt:
                row.state = 'complete'
                if row.declared_count is not None and row.observed_count < row.declared_count:
                    row.state, row.gap_kind = 'unresolved', 'count_shortfall'
            row.evidence = {**(row.evidence or {}), 'response_base': base}

    async def fail_scope(self, scope, error):
        async with self.factory.begin() as db:
            await self.lock(db)
            row = await self._scope(db, scope)
            row.state, row.gap_kind = 'unresolved', getattr(error, 'stage', 'enumeration_failed')[:64]
            row.error_count += 1
            row.evidence = {**(row.evidence or {}), 'error': {
                'type': type(error).__name__, 'status': getattr(error, 'status_code', getattr(getattr(error, 'response', None), 'status_code', None)),
                'code': getattr(error, 'code', None), 'request_id': getattr(error, 'request_id', None)}}
            delay = getattr(error, 'retry_after', None)
            if delay is not None:
                row.not_before = datetime.now(timezone.utc) + timedelta(seconds=max(0, delay))

    async def restart(self, scope):
        async with self.factory.begin() as db:
            await self.lock(db)
            row = await self._scope(db, scope)
            if row.restart_count >= 1:
                row.state, row.gap_kind = 'unresolved', 'cursor_restart_exhausted'
                return False
            # Descendants from an invalidated folder census cannot certify this generation.
            await db.execute(text('''WITH RECURSIVE descendants AS (
                SELECT scope_key FROM archive_folder_work WHERE snapshot_id=:sid AND capture_generation=:gen AND parent_scope_key=:key
                UNION ALL SELECT f.scope_key FROM archive_folder_work f JOIN descendants d ON f.parent_scope_key=d.scope_key
                WHERE f.snapshot_id=:sid AND f.capture_generation=:gen)
                UPDATE archive_folder_work SET state='excluded' WHERE snapshot_id=:sid AND capture_generation=:gen
                AND scope_key IN (SELECT scope_key FROM descendants)'''), self.params(key=row.scope_key))
            row.scope_generation += 1
            row.restart_count += 1
            row.page_ordinal = row.observed_count = 0
            row.terminal_page = False
            row.next_url = row.base_url if row.phase == 'root' else f'{row.base_url}/{row.folder_id}/{"items" if row.phase == "items" else "childFolders"}'
            row.state, row.gap_kind = 'pending', None
            return True

    async def repair(self):
        """One finite repair turn, respecting persisted provider cooldowns."""
        async with self.factory.begin() as db:
            await self.lock(db)
            rows = list((await db.scalars(select(Folder).where(Folder.snapshot_id == self.fence.snapshot_id,
                Folder.capture_generation == self.generation, Folder.state == 'unresolved',
                Folder.phase.in_(('root', 'items', 'children')), Folder.logical_attempts < 2,
                Folder.gap_kind != 'location_cap',
                (Folder.not_before.is_(None) | (Folder.not_before <= func.now()))).limit(100))).all())
            restart = []
            for scope in rows:
                if scope.next_url:
                    scope.state = 'pending'
                elif scope.restart_count < 1:
                    restart.append({'scope_key': scope.scope_key, 'scope_generation': scope.scope_generation})
        for scope in restart:
            await self.restart(scope)
        return len(rows)

    async def manifest_batch(self, *, after='', limit=128, state=None):
        clause = ' AND i.state=:state' if state else ''
        async with self.factory() as db:
            return [dict(r) for r in (await db.execute(text('''SELECT DISTINCT ON (i.external_id) i.*
                FROM archive_item_work i JOIN archive_folder_work f
                ON f.snapshot_id=i.snapshot_id AND f.capture_generation=i.capture_generation AND f.scope_key=i.scope_key AND f.scope_generation=i.scope_generation
                WHERE i.snapshot_id=:sid AND i.capture_generation=:gen AND f.state <> 'excluded'
                AND i.external_id > :after''' + clause + ' ORDER BY i.external_id,i.scope_key LIMIT :limit'),
                self.params(after=after, limit=min(limit, 256), state=state))).mappings()]

    async def baseline(self, days=30):
        async with self.factory.begin() as db:
            snap = await self.lock(db)
            context = dict(snap.extra_data['archive_capture'])
            if 'baseline_count' in context:
                return (UUID(context['prior_snapshot_id']) if context.get('prior_snapshot_id') else None, context['baseline_count'])
            result = (await db.execute(text('''SELECT s.id, s.item_count FROM snapshots s
                WHERE s.resource_id=:rid AND s.id<>:sid AND s.status IN ('COMPLETED','PARTIAL')
                AND EXISTS (SELECT 1 FROM snapshot_items i WHERE i.snapshot_id=s.id AND i.item_type='ARCHIVE_ITEM')
                ORDER BY (s.created_at > NOW()-make_interval(days=>:days)) DESC,s.item_count DESC NULLS LAST,s.created_at DESC LIMIT 1'''),
                {'rid': self.fence.resource_id, 'sid': self.fence.snapshot_id, 'days': days})).first()
            chosen = (result[0], int(result[1] or 0)) if result else (None, 0)
            context.update(prior_snapshot_id=str(chosen[0]) if chosen[0] else None, baseline_count=chosen[1])
            snap.extra_data = {**snap.extra_data, 'archive_capture': context}
            return chosen

    async def classify(self, prior_id, *, force_full=False, copy_collection=None):
        after = ''
        while batch := await self.manifest_batch(after=after):
            after = batch[-1]['external_id']
            ids = [r['external_id'] for r in batch]
            async with self.factory.begin() as db:
                await self.lock(db)
                await db.execute(text('''WITH current AS (
                    SELECT i.external_id, count(DISTINCT jsonb_build_array(i.location,i.folder_id,i.change_key)) AS proofs,
                        bool_or(i.gap_kind IS NOT NULL) AS failed
                    FROM archive_item_work i JOIN archive_folder_work f
                      ON f.snapshot_id=i.snapshot_id AND f.capture_generation=i.capture_generation AND f.scope_key=i.scope_key AND f.scope_generation=i.scope_generation
                    WHERE i.snapshot_id=:sid AND i.capture_generation=:gen AND f.state<>'excluded' AND i.external_id=ANY(:ids)
                    GROUP BY i.external_id)
                    UPDATE archive_item_work i SET state=CASE WHEN c.proofs>1 OR c.failed THEN 'failed'
                        WHEN NOT :full AND i.change_key IS NOT NULL AND EXISTS (
                          SELECT 1 FROM snapshot_items p WHERE p.snapshot_id=:prior AND p.item_type='ARCHIVE_ITEM'
                          AND p.external_id=i.external_id AND COALESCE(p.metadata->>'changeKey',p.metadata->>'change_key')=i.change_key
                          AND p.blob_path IS NOT NULL AND p.content_hash IS NOT NULL AND NOT COALESCE(p.is_deleted,false))
                          THEN 'copy_pending' ELSE 'export_pending' END,
                        gap_kind=CASE WHEN c.proofs>1 OR c.failed THEN 'identity_conflict' ELSE NULL END
                    FROM current c WHERE i.snapshot_id=:sid AND i.capture_generation=:gen AND i.external_id=c.external_id
                    AND i.state='discovered' '''), self.params(ids=ids, prior=prior_id, full=force_full))
                if prior_id:
                    await self._carry(db, prior_id, ids, copy_collection)

    async def fail_items(self, ids, kind):
        async with self.factory.begin() as db:
            await self.lock(db)
            await db.execute(update(Item).where(Item.snapshot_id == self.fence.snapshot_id,
                Item.capture_generation == self.generation, Item.external_id.in_(ids), Item.state.notin_(('stored', 'failed')))
                .values(state='failed', gap_kind=kind[:64]))

    async def commit_rows(self, rows):
        """The target row and per-item receipt commit together after upload."""
        stored = []
        async with self.factory.begin() as db:
            await self.lock(db)
            for row in rows:
                if (row['snapshot_id'], row['tenant_id']) != (self.fence.snapshot_id, self.fence.tenant_id):
                    raise ArchiveConflict('archive receipt target identity differs')
                iid, ck = row['external_id'], row['metadata'].get('changeKey')
                observations = list((await db.scalars(select(Item).join(Folder,
                    (Folder.snapshot_id == Item.snapshot_id) & (Folder.capture_generation == Item.capture_generation)
                    & (Folder.scope_key == Item.scope_key) & (Folder.scope_generation == Item.scope_generation)).where(
                    Item.snapshot_id == self.fence.snapshot_id, Item.capture_generation == self.generation,
                    Item.external_id == iid, Folder.state != 'excluded'))).all())
                if not observations or any(o.change_key != ck or o.gap_kind for o in observations):
                    for o in observations:
                        o.state, o.gap_kind = 'failed', 'source_changed'
                    continue
                prior = await db.scalar(select(SnapshotItem).where(SnapshotItem.snapshot_id == self.fence.snapshot_id,
                    SnapshotItem.item_type == 'ARCHIVE_ITEM', SnapshotItem.external_id == iid))
                if prior is None:
                    await db.execute(insert(SnapshotItem.__table__).values(row))
                    prior = await db.get(SnapshotItem, row['id'])
                expected = (ck, row['content_hash'], row['content_size'], row['blob_path'], row['backend_id'], row.get('encryption_key_id'))
                actual = ((prior.extra_data or {}).get('changeKey'), prior.content_hash, prior.content_size, prior.blob_path, prior.backend_id, prior.encryption_key_id)
                if expected != actual:
                    for o in observations:
                        o.state, o.gap_kind = 'failed', 'receipt_conflict'
                    continue
                for o in observations:
                    o.state, o.receipt_item_id = 'stored', prior.id
                    o.receipt = {'kind': 'export', 'change_key': ck, 'hash': prior.content_hash, 'size': prior.content_size,
                        'blob_path': prior.blob_path, 'backend_id': str(prior.backend_id),
                        'encryption_key_id': prior.encryption_key_id, 'backup_version': prior.backup_version}
                stored.append({'external_id': iid, 'item_id': str(prior.id), 'bytes': prior.content_size})
        return stored

    async def missing_samples(self, prior, limit=60):
        async with self.factory() as db:
            return list((await db.scalars(text('''SELECT p.external_id FROM snapshot_items p
                WHERE p.snapshot_id=:prior AND p.item_type='ARCHIVE_ITEM' AND NOT EXISTS (
                    SELECT 1 FROM archive_item_work i JOIN archive_folder_work f ON f.snapshot_id=i.snapshot_id AND f.capture_generation=i.capture_generation
                    AND f.scope_key=i.scope_key AND f.scope_generation=i.scope_generation
                    WHERE i.snapshot_id=:sid AND i.capture_generation=:gen AND i.external_id=p.external_id AND f.state<>'excluded')
                ORDER BY p.external_id LIMIT :limit'''), self.params(prior=prior, limit=min(limit, 60)))).all())

    async def summary(self, db=None):
        if db is None:
            async with self.factory() as session:
                return await self.summary(session)
        folder = (await db.execute(text('''SELECT count(*) FILTER (WHERE phase='root' AND logical_attempts>0) AS locations_walked,
            count(*) FILTER (WHERE phase='root') AS locations_planned,
            count(*) FILTER (WHERE state='complete' AND terminal_page) AS scopes_complete,
            count(*) FILTER (WHERE state IN ('pending','reading')) AS scopes_pending,
            count(*) FILTER (WHERE state='unresolved') AS scopes_unresolved,
            count(*) FILTER (WHERE not_before>NOW()) AS scopes_deferred,
            count(*) FILTER (WHERE state<>'complete' OR NOT terminal_page) AS folders_failed,
            count(*) AS scopes_planned FROM archive_folder_work WHERE snapshot_id=:sid AND capture_generation=:gen
            AND phase<>'receipt' AND state<>'excluded' '''), self.params())).mappings().one()
        counts = (await db.execute(text('''WITH current AS (
            SELECT i.external_id, bool_and(i.state='stored' AND s.id IS NOT NULL
              AND s.content_hash=i.receipt->>'hash' AND s.blob_path=i.receipt->>'blob_path'
              AND s.content_size=(i.receipt->>'size')::bigint AND s.backend_id::text=i.receipt->>'backend_id'
              AND s.encryption_key_id IS NOT DISTINCT FROM i.receipt->>'encryption_key_id'
              AND s.backup_version IS NOT DISTINCT FROM (i.receipt->>'backup_version')::integer
              AND COALESCE(s.metadata->>'changeKey',s.metadata->>'change_key') IS NOT DISTINCT FROM i.change_key) AS stored
            FROM archive_item_work i JOIN archive_folder_work f ON f.snapshot_id=i.snapshot_id AND f.capture_generation=i.capture_generation
              AND f.scope_key=i.scope_key AND f.scope_generation=i.scope_generation
            LEFT JOIN snapshot_items s ON s.id=i.receipt_item_id AND s.snapshot_id=i.snapshot_id AND s.external_id=i.external_id AND s.item_type='ARCHIVE_ITEM'
            WHERE i.snapshot_id=:sid AND i.capture_generation=:gen AND f.state<>'excluded' GROUP BY i.external_id)
            SELECT count(*) AS items_enumerated,count(*) FILTER (WHERE stored) AS files_succeeded,
              count(*) FILTER (WHERE NOT COALESCE(stored,false)) AS items_failed FROM current'''), self.params())).mappings().one()
        samples = (await db.execute(text('''SELECT phase, gap_kind AS kind, folder_id, display_path AS path, observed_count, declared_count
            FROM archive_folder_work WHERE snapshot_id=:sid AND capture_generation=:gen AND phase<>'receipt'
            AND state NOT IN ('complete','excluded') ORDER BY scope_key LIMIT 20'''), self.params())).mappings()
        result = {**dict(folder), **dict(counts), 'folder_failures': [dict(r) for r in samples]}
        totals = (await db.execute(text('SELECT COALESCE(sum(content_size),0) FROM snapshot_items WHERE snapshot_id=:sid'),
            {'sid': self.fence.snapshot_id})).scalar()
        exported = (await db.execute(text('''SELECT count(*),COALESCE(sum(size),0) FROM (
            SELECT DISTINCT i.external_id,(i.receipt->>'size')::bigint AS size FROM archive_item_work i
            JOIN archive_folder_work f ON f.snapshot_id=i.snapshot_id AND f.capture_generation=i.capture_generation
            AND f.scope_key=i.scope_key AND f.scope_generation=i.scope_generation WHERE i.snapshot_id=:sid
            AND i.capture_generation=:gen AND f.state<>'excluded' AND i.state='stored' AND i.receipt->>'kind'='export') v'''),
            self.params())).one()
        result.update(bytes_total=int(totals or 0),new_item_count=int(exported[0]),bytes_added=int(exported[1]))
        result['files_failed'] = result['folders_failed'] + result['items_failed']
        result['folder_failures_truncated'] = result['folders_failed'] > 20
        return result

    async def _carry(self, db, prior_id, ids, collection):
        pending = list((await db.scalars(select(Item).where(Item.snapshot_id == self.fence.snapshot_id,
            Item.capture_generation == self.generation, Item.external_id.in_(ids), Item.state == 'copy_pending',
            Item.copy_task_id.is_(None)))).all())
        if not pending:
            return
        prior = await db.get(Snapshot, prior_id, with_for_update={'read': True})
        if (not prior or prior.resource_id != self.fence.resource_id
                or prior.status.value not in ('COMPLETED', 'PARTIAL')
                or (prior.extra_data or {}).get('cancelled_at') or (prior.extra_data or {}).get('copy_required_failed')):
            raise ArchiveConflict('archive predecessor no longer retained')
        pending_ids = sorted({i.external_id for i in pending})
        source = {p.external_id: p for p in (await db.scalars(select(SnapshotItem).where(
            SnapshotItem.snapshot_id == prior_id, SnapshotItem.item_type == 'ARCHIVE_ITEM',
            SnapshotItem.external_id.in_(pending_ids)))).all()}
        for item in pending:
            row = source.get(item.external_id)
            if row is None:
                raise ArchiveConflict('archive predecessor content disappeared')
            item.receipt = {'kind': 'copy', 'change_key': item.change_key, 'hash': row.content_hash, 'size': row.content_size,
                'blob_path': row.blob_path, 'backend_id': str(row.backend_id),
                'encryption_key_id': row.encryption_key_id, 'backup_version': row.backup_version}
        if collection is not None:
            from shared.copy_work import CopyRequest, enqueue
            try:
                task = await enqueue(db, CopyRequest(kind='ARCHIVE_CARRY', lane='giant',
                    target_snapshot_id=self.fence.snapshot_id, partition_id=self.fence.partition_id,
                    params={'prior_snapshot_id': str(prior_id), 'external_ids': pending_ids,
                        'archive_capture_generation': str(self.generation)}))
                for item in pending:
                    item.copy_task_id = task.id
            except BaseException as exc:
                collection.error = exc
                raise
        else:
            # Legacy protocol still uses the same exact DB-only copy selection.
            columns = [c.name for c in SnapshotItem.__table__.columns]
            expressions = ['gen_random_uuid()' if c == 'id' else ':sid' if c == 'snapshot_id' else 'src.' + c for c in columns]
            await db.execute(text('INSERT INTO snapshot_items (' + ','.join(columns) + ') SELECT ' + ','.join(expressions) + '''
                FROM snapshot_items src WHERE src.snapshot_id=:prior AND ((src.item_type='ARCHIVE_ITEM' AND src.external_id=ANY(:ids))
                  OR (src.item_type='ARCHIVE_ATTACHMENT' AND src.parent_external_id=ANY(:ids)))
                ON CONFLICT (snapshot_id,external_id,item_type) DO NOTHING'''), {'sid': self.fence.snapshot_id, 'prior': prior_id, 'ids': pending_ids})
        await db.flush()
        await self.resolve_copies(db)

    async def resolve_copies(self, db):
        """A queued intent is not a receipt; validate completed tasks and bytes."""
        await db.execute(text('''UPDATE archive_item_work i SET state='stored',receipt_item_id=s.id
            FROM snapshot_items s WHERE i.snapshot_id=:sid AND i.capture_generation=:gen AND i.state='copy_pending'
            AND (i.copy_task_id IS NULL OR EXISTS (SELECT 1 FROM copy_work_tasks t WHERE t.id=i.copy_task_id AND t.state='COMPLETED'))
            AND s.snapshot_id=i.snapshot_id AND s.external_id=i.external_id AND s.item_type='ARCHIVE_ITEM'
            AND COALESCE(s.metadata->>'changeKey',s.metadata->>'change_key') IS NOT DISTINCT FROM i.change_key
            AND s.content_hash=i.receipt->>'hash' AND s.blob_path=i.receipt->>'blob_path'
            AND s.content_size=(i.receipt->>'size')::bigint AND s.backend_id::text=i.receipt->>'backend_id'
            AND s.encryption_key_id IS NOT DISTINCT FROM i.receipt->>'encryption_key_id'
            AND s.backup_version IS NOT DISTINCT FROM (i.receipt->>'backup_version')::integer'''), self.params())

    async def has_copies(self):
        async with self.factory() as db:
            return bool(await db.scalar(select(func.count()).select_from(Item).where(Item.snapshot_id == self.fence.snapshot_id,
                Item.capture_generation == self.generation, Item.copy_task_id.is_not(None))))

    async def export_batch(self, *, count=20, byte_budget=16 * 1024 * 1024):
        async with self.factory.begin() as db:
            await self.lock(db)
            # Serialize claims with the parent fence. A small ID batch is the only resident frontier.
            candidates = (await db.execute(text('''SELECT DISTINCT ON(i.external_id) i.* FROM archive_item_work i
                JOIN archive_folder_work f ON f.snapshot_id=i.snapshot_id AND f.capture_generation=i.capture_generation
                 AND f.scope_key=i.scope_key AND f.scope_generation=i.scope_generation
                WHERE i.snapshot_id=:sid AND i.capture_generation=:gen AND i.state='export_pending' AND f.state<>'excluded'
                ORDER BY i.external_id,i.scope_key LIMIT :count'''), self.params(count=min(count,20)))).mappings().all()
            chosen, size = [], 0
            for row in candidates:
                estimate = max(0, row['size'] or 0) * 4 // 3 + 1024
                if chosen and size + estimate > byte_budget:
                    break
                chosen.append(dict(row))
                size += estimate
            if chosen:
                await db.execute(update(Item).where(Item.snapshot_id == self.fence.snapshot_id,
                    Item.capture_generation == self.generation, Item.external_id.in_([r['external_id'] for r in chosen]),
                    Item.state == 'export_pending').values(state='exporting'))
            return chosen


async def archive_final_gate(db, snapshot, *, generation=None):
    """Shared inline/resume gate. Caller holds its parent or resume fence."""
    context = (snapshot.extra_data or {}).get('archive_capture')
    if not context:
        return None
    if context.get('version') != 1 or (generation and context['generation'] != str(generation)):
        raise ArchiveConflict('archive finalizer capture generation differs')
    resource = await db.get(Resource, snapshot.resource_id)
    ledger = ArchiveCapture(None, ArchiveFence(snapshot.id, snapshot.job_id, snapshot.resource_id,
        resource.tenant_id, snapshot.lease_token, None))
    ledger.generation = UUID(context['generation'])
    await ledger.resolve_copies(db)
    summary = await ledger.summary(db)
    snapshot.delta_tokens_json = {**(snapshot.delta_tokens_json or {}), **summary}
    return summary
