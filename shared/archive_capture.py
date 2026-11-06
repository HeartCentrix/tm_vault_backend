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


class ArchiveLocationCap(ArchivePageError):
    stage = 'location_cap'


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
            return existing.gap_kind != 'location_cap'
        count = await db.scalar(select(func.count()).select_from(Folder).where(Folder.snapshot_id == self.fence.snapshot_id,
            Folder.capture_generation == self.generation, Folder.phase == 'root', Folder.state != 'excluded'))
        over = count >= self.max_locations
        db.add(Folder(snapshot_id=self.fence.snapshot_id, capture_generation=self.generation, scope_key=key,
            phase='root', base_url=base, folder_id='', next_url=base,
            state='unresolved' if over else 'pending', gap_kind='location_cap' if over else None))
        await db.flush()
        return not over

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
            if not await self._location(db, base):
                row.state, row.gap_kind = 'unresolved', 'location_cap'
                return False
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
                memberships = {}
                for child in page['value']:
                    if not isinstance(child, dict) or not isinstance(child.get('id'), str) or not child['id']:
                        raise ArchivePageError('archive folder identity missing')
                    route = folder_base(child, base)
                    if not await self._location(db, route):
                        continue
                    target_key = digest(route, child['id'], 'items')
                    member_key = digest('membership', row.scope_key, row.scope_generation, target_key)
                    memberships[member_key] = dict(snapshot_id=self.fence.snapshot_id,
                        capture_generation=self.generation, scope_key=member_key, phase='membership',
                        base_url=route, folder_id=child['id'], parent_scope_key=row.scope_key,
                        parent_generation=row.scope_generation, state='complete', terminal_page=True,
                        evidence={'target_key': target_key, 'children_key':
                            digest(route, child['id'], 'children') if child.get('childFolderCount') != 0 else None})
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
                            old.parent_scope_key = row.scope_key
                            old.declared_count = declared if phase == 'children' else child.get('totalItemCount')
                        elif old.base_url != route or old.folder_id != child['id']:
                            raise ArchiveConflict('archive scope hash collision')
                values = list(memberships.values())
                for start in range(0, len(values), 200):
                    inserted = await db.execute(insert(Folder).values(values[start:start + 200])
                        .on_conflict_do_nothing().returning(Folder.scope_key))
                    inserted_count += len(inserted.all())
            db.add(Folder(snapshot_id=self.fence.snapshot_id, capture_generation=self.generation,
                scope_key=receipt_key, phase='receipt', base_url=base, folder_id=row.folder_id,
                parent_scope_key=row.scope_key, parent_generation=row.scope_generation, state='complete',
                terminal_page=True, evidence={'digest': fingerprint}, page_ordinal=ordinal))
            row.page_ordinal = ordinal
            row.next_url = nxt
            row.terminal_page = not nxt
            await db.flush()
            row.observed_count += inserted_count
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

    async def _refresh_count(self, scope, read_folder, *, after=False):
        """Read metadata outside the owner transaction, with durable finite evidence."""
        async with self.factory.begin() as db:
            await self.lock(db)
            row = await self._scope(db, scope)
            proof = dict((row.evidence or {}).get('count_reconciliation') or {})
            key = 'after_reads' if after else 'before_reads'
            done = 'after_done' if after else 'before_done'
            if proof.get(done):
                return
            if proof.get(key, 0) >= 2:
                row.state, row.gap_kind = 'unresolved', 'count_reconciliation_exhausted'
                proof[done], proof['stable'] = True, False
                row.evidence = {**row.evidence, 'count_reconciliation': proof}
                return
            proof[key] = proof.get(key, 0) + 1
            proof.setdefault('original_count', row.declared_count)
            row.evidence = {**(row.evidence or {}), 'count_reconciliation': proof}
            request = row.base_url + '/' + row.folder_id
        try:
            from shared.provider_http import response_byte_limit
            with response_byte_limit(65536):
                metadata = await read_folder(request)
            name = 'childFolderCount' if scope['phase'] == 'children' else 'totalItemCount'
            count = metadata.get(name) if isinstance(metadata, dict) else None
            if not isinstance(metadata, dict) or metadata.get('id') != scope['folder_id'] or type(count) is not int or count < 0:
                raise ArchivePageError('archive folder count proof is invalid')
            # A redirected folder response may identify its auxiliary mailbox
            # only through @odata.context. An explicit parent hint takes priority.
            route = (folder_base(metadata, scope['base_url']) if metadata.get('parentMailboxUrl')
                     else page_base(metadata, scope['base_url']))
        except Exception as error:
            from shared.provider_read import ProviderReadError
            if not isinstance(error, (ArchivePageError, ProviderReadError)) and type(error).__name__ not in (
                    'HTTPStatusError', 'ReadTimeout', 'ConnectError', 'GraphRetryExhaustedError'):
                raise
            async with self.factory.begin() as db:
                await self.lock(db)
                row = await self._scope(db, scope)
                proof = dict(row.evidence['count_reconciliation'])
                proof['metadata_unavailable'] = True
                proof[done] = True
                row.evidence = {**row.evidence, 'count_reconciliation': proof}
                row.state, row.gap_kind = 'unresolved', 'count_metadata_unavailable'
            return
        async with self.factory.begin() as db:
            await self.lock(db)
            row = await self._scope(db, scope)
            proof = dict(row.evidence['count_reconciliation'])
            proof[done] = True
            if after:
                proof['final_count'], proof['final_route'] = count, route
                proof['stable'] = (not proof.get('metadata_unavailable') and count == proof.get('refreshed_count')
                    and route == proof.get('refreshed_route') == row.evidence.get('response_base')
                    and row.observed_count == count and row.terminal_page)
                if not proof['stable']:
                    row.state, row.gap_kind = 'unresolved', 'count_changed_during_repair'
            else:
                proof['refreshed_count'], proof['refreshed_route'] = count, route
                row.declared_count = count
            row.evidence = {**row.evidence, 'count_reconciliation': proof}

    async def verify_repair_counts(self, read_folder):
        while True:
            async with self.factory() as db:
                rows = list((await db.scalars(select(Folder).where(Folder.snapshot_id == self.fence.snapshot_id,
                    Folder.capture_generation == self.generation, Folder.phase.in_(('items', 'children')),
                    Folder.restart_count > 0, Folder.state != 'excluded',
                    Folder.evidence['count_reconciliation']['before_reads'].as_integer() > 0,
                    Folder.evidence['count_reconciliation']['after_done'].as_boolean().is_not(True))
                    .order_by(Folder.scope_key).limit(100))).all())
                scopes = [{c.name: getattr(row, c.key) for c in Folder.__table__.columns} for row in rows
                    if (row.evidence or {}).get('count_reconciliation')]
            if not scopes:
                return
            for scope in scopes:
                proof = scope['evidence']['count_reconciliation']
                if proof.get('metadata_unavailable') or not proof.get('refreshed_route'):
                    async with self.factory.begin() as db:
                        await self.lock(db)
                        row = await self._scope(db, scope)
                        row.state, row.gap_kind = 'unresolved', 'count_metadata_unavailable'
                        row.evidence = {**row.evidence, 'count_reconciliation': {
                            **row.evidence['count_reconciliation'], 'after_done': True, 'stable': False}}
                else:
                    await self._refresh_count(scope, read_folder, after=True)

    async def _explain_hidden_children(self, read_folder):
        """A short childFolders listing is no loss when Graph's hidden system folders make up the
        difference (Contacts declares 7, lists 0, includeHiddenFolders=true returns all 7) or when
        it is Conversation History's one child Graph never lists (v1.0 and beta, live 2026-11-06).
        Hidden folders are counted, not backed up; any other shortfall stays a gap."""
        async with self.factory() as db:
            rows = list((await db.scalars(select(Folder).where(Folder.snapshot_id == self.fence.snapshot_id,
                Folder.capture_generation == self.generation, Folder.state == 'unresolved',
                Folder.phase == 'children', Folder.gap_kind == 'count_shortfall').limit(100))).all())
            scopes = [{c.name: getattr(r, c.key) for c in Folder.__table__.columns} for r in rows]
        for scope in scopes:
            url, ids = f"{scope['base_url']}/{scope['folder_id']}/childFolders?includeHiddenFolders=true&$top=200", set()
            try:
                for _ in range(50):
                    page = await read_folder(url)
                    ids.update(c['id'] for c in page.get('value', []) if isinstance(c, dict) and c.get('id'))
                    url = page.get('@odata.nextLink')
                    if not url:
                        break
            except Exception:
                continue  # unexplained: the normal repair path still re-reads it
            declared, observed = scope['declared_count'] or 0, scope['observed_count'] or 0
            # ponytail: matches the English folder name; a localized Conversation History stays a gap.
            unlisted = 1 if (declared - max(len(ids), observed) == 1
                             and (scope['display_path'] or '').rsplit('/', 1)[-1] == 'Conversation History') else 0
            if len(ids) + unlisted < declared:
                continue
            async with self.factory.begin() as db:
                await self.lock(db)
                row = await self._scope(db, scope)
                if (row.state, row.gap_kind) != ('unresolved', 'count_shortfall'):
                    continue
                row.state, row.gap_kind = 'complete', None
                extra = {'unlisted_system_children': unlisted} if unlisted else {'hidden_children': len(ids) - observed}
                row.evidence = {**(row.evidence or {}), **extra}

    async def repair(self, read_folder=None):
        """One finite repair turn, respecting persisted provider cooldowns."""
        if read_folder:
            await self._explain_hidden_children(read_folder)
        async with self.factory.begin() as db:
            await self.lock(db)
            rows = list((await db.scalars(select(Folder).where(Folder.snapshot_id == self.fence.snapshot_id,
                Folder.capture_generation == self.generation, Folder.state == 'unresolved',
                Folder.phase.in_(('root', 'items', 'children')), Folder.logical_attempts < 2,
                Folder.gap_kind != 'location_cap',
                (Folder.not_before.is_(None) | (Folder.not_before <= func.now()))).limit(100))).all())
            restart = []
            refresh = []
            for scope in rows:
                if scope.next_url:
                    scope.state = 'pending'
                elif scope.restart_count < 1:
                    descriptor = {c.name: getattr(scope, c.key) for c in Folder.__table__.columns}
                    restart.append(descriptor)
                    if read_folder and scope.phase != 'root' and scope.gap_kind == 'count_shortfall':
                        refresh.append(descriptor)
        for scope in refresh:
            await self._refresh_count(scope, read_folder)
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
            if chosen[0]:
                retained = await db.scalar(select(func.count(func.distinct(SnapshotItem.external_id))).where(
                    SnapshotItem.snapshot_id == chosen[0], SnapshotItem.item_type == 'ARCHIVE_ITEM'))
                chosen = (chosen[0], max(chosen[1], int(retained or 0)))
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
        if not prior_id or (not force_full and await self._point_at_prior(prior_id)):
            return
        after = ''
        while batch := await self.manifest_batch(after=after):
            after = batch[-1]['external_id']
            async with self.factory.begin() as db:
                await self.lock(db)
                await self._carry(db, prior_id, [r['external_id'] for r in batch], copy_collection)

    async def _point_at_prior(self, prior_id):
        """An unchanged archive - complete census, every item copy_pending (same changeKey as a stored
        prior row) and one-to-one with the prior's items - points at the prior instead of copying it:
        every incremental copied the whole archive (TM's largest: 1.1 M items). Items are stored through
        the pointer with a 'reuse' receipt (no copy to verify, nothing new). ARCHIVE_UNCHANGED_REUSE_ENABLED
        =false copies as before."""
        import os
        if os.getenv('ARCHIVE_UNCHANGED_REUSE_ENABLED', 'true').strip().lower() in ('0', 'false', 'no', 'off'):
            return False
        census = await self.summary()
        if census['scopes_pending'] or census['folders_failed'] or not census['items_enumerated']:
            return False
        from shared.snapshot_reuse import point_at
        async with self.factory.begin() as db:
            await self.lock(db)
            prior = await db.get(Snapshot, prior_id, with_for_update={'read': True})
            if (not prior or prior.resource_id != self.fence.resource_id or prior.status.value != 'COMPLETED'
                    or (prior.extra_data or {}).get('cancelled_at') or (prior.extra_data or {}).get('copy_required_failed')):
                return False
            counts = (await db.execute(text('''SELECT count(DISTINCT i.external_id) AS items,
                    count(*) FILTER (WHERE i.state<>'copy_pending') AS changed
                FROM archive_item_work i JOIN archive_folder_work f ON f.snapshot_id=i.snapshot_id
                  AND f.capture_generation=i.capture_generation AND f.scope_key=i.scope_key AND f.scope_generation=i.scope_generation
                WHERE i.snapshot_id=:sid AND i.capture_generation=:gen AND f.state<>'excluded' '''), self.params())).one()
            prior_items = await db.scalar(text('''SELECT count(*) FROM snapshot_items
                WHERE snapshot_id=:prior AND item_type='ARCHIVE_ITEM' '''), {'prior': prior_id})
            if counts.changed or counts.items != prior_items:
                return False
            if not await point_at(db, target_snapshot_id=self.fence.snapshot_id, prior_snapshot_id=prior_id,
                                  marker='archive_reused_prior'):
                return False
            await db.execute(text('''UPDATE archive_item_work i SET state='stored', receipt_item_id=s.id,
                receipt=json_build_object('kind','reuse','change_key',i.change_key,'hash',s.content_hash,'size',s.content_size,
                    'blob_path',s.blob_path,'backend_id',s.backend_id::text,'encryption_key_id',s.encryption_key_id,
                    'backup_version',s.backup_version,'reused_snapshot_id',CAST(:prior AS text))
                FROM snapshot_items s WHERE i.snapshot_id=:sid AND i.capture_generation=:gen AND i.state='copy_pending'
                  AND s.snapshot_id=:prior AND s.item_type='ARCHIVE_ITEM' AND s.external_id=i.external_id'''),
                self.params(prior=prior_id))
        return True

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
                row = {'backup_version': 1, **row}
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
                expected = (ck, row['content_hash'], row['content_size'], row['blob_path'], row['backend_id'], row.get('encryption_key_id'), row.get('backup_version'))
                actual = ((prior.extra_data or {}).get('changeKey'), prior.content_hash, prior.content_size, prior.blob_path, prior.backend_id, prior.encryption_key_id, prior.backup_version)
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
            return list((await db.scalars(text('''SELECT external_id FROM (SELECT p.external_id, p.folder_path, row_number() OVER (PARTITION BY p.folder_path ORDER BY p.external_id) AS ordinal FROM snapshot_items p
                WHERE p.snapshot_id=:prior AND p.item_type='ARCHIVE_ITEM' AND NOT EXISTS (
                    SELECT 1 FROM archive_item_work i JOIN archive_folder_work f ON f.snapshot_id=i.snapshot_id AND f.capture_generation=i.capture_generation
                    AND f.scope_key=i.scope_key AND f.scope_generation=i.scope_generation
                    WHERE i.snapshot_id=:sid AND i.capture_generation=:gen AND i.external_id=p.external_id AND f.state<>'excluded')
                 ) candidates ORDER BY ordinal, folder_path, external_id LIMIT :limit'''), self.params(prior=prior, limit=min(limit, 60)))).all())

    async def summary(self, db=None):
        if db is None:
            async with self.factory() as session:
                return await self.summary(session)
        folder = (await db.execute(text('''SELECT count(*) FILTER (WHERE phase='root' AND logical_attempts>0) AS locations_walked,
            count(*) FILTER (WHERE phase='root') AS locations_planned,
            count(*) FILTER (WHERE state='complete' AND terminal_page AND
                (evidence->'count_reconciliation' IS NULL OR evidence->'count_reconciliation'->>'stable'='true')) AS scopes_complete,
            count(*) FILTER (WHERE state IN ('pending','reading')) AS scopes_pending,
            count(*) FILTER (WHERE state='unresolved' OR (state='complete' AND
                evidence->'count_reconciliation' IS NOT NULL AND
                COALESCE(evidence->'count_reconciliation'->>'stable','false')<>'true')) AS scopes_unresolved,
            count(*) FILTER (WHERE not_before>NOW()) AS scopes_deferred,
            count(*) FILTER (WHERE state<>'complete' OR NOT terminal_page OR
                (evidence->'count_reconciliation' IS NOT NULL AND
                    COALESCE(evidence->'count_reconciliation'->>'stable','false')<>'true')) AS folders_failed,
            count(*) AS scopes_planned FROM archive_folder_work WHERE snapshot_id=:sid AND capture_generation=:gen
            AND phase NOT IN ('receipt','membership') AND state<>'excluded' '''), self.params())).mappings().one()
        counts = (await db.execute(text('''WITH current AS (
            SELECT i.external_id, bool_and(i.state='stored' AND s.id IS NOT NULL
              AND s.content_hash=i.receipt->>'hash' AND s.blob_path=i.receipt->>'blob_path'
              AND s.content_size=(i.receipt->>'size')::bigint AND s.backend_id::text=i.receipt->>'backend_id'
              AND s.encryption_key_id IS NOT DISTINCT FROM i.receipt->>'encryption_key_id'
              AND s.backup_version IS NOT DISTINCT FROM (i.receipt->>'backup_version')::integer
              AND COALESCE(s.metadata->>'changeKey',s.metadata->>'change_key') IS NOT DISTINCT FROM i.change_key) AS stored
            FROM archive_item_work i JOIN archive_folder_work f ON f.snapshot_id=i.snapshot_id AND f.capture_generation=i.capture_generation
              AND f.scope_key=i.scope_key AND f.scope_generation=i.scope_generation
            LEFT JOIN snapshot_items s ON s.id=i.receipt_item_id AND s.snapshot_id=COALESCE((SELECT x.reuse_chain_root_id FROM snapshots x WHERE x.id=i.snapshot_id), i.snapshot_id) AND s.external_id=i.external_id AND s.item_type='ARCHIVE_ITEM'
            WHERE i.snapshot_id=:sid AND i.capture_generation=:gen AND f.state<>'excluded' GROUP BY i.external_id)
            SELECT count(*) AS items_enumerated,count(*) FILTER (WHERE stored) AS files_succeeded,
              count(*) FILTER (WHERE NOT COALESCE(stored,false)) AS items_failed FROM current'''), self.params())).mappings().one()
        samples = (await db.execute(text('''SELECT phase, COALESCE(gap_kind,'count_reconciliation_pending') AS kind, folder_id, display_path AS path, observed_count, declared_count
            FROM archive_folder_work WHERE snapshot_id=:sid AND capture_generation=:gen AND phase NOT IN ('receipt','membership')
            AND (state NOT IN ('complete','excluded') OR (state='complete' AND evidence->'count_reconciliation' IS NOT NULL
                AND COALESCE(evidence->'count_reconciliation'->>'stable','false')<>'true')) ORDER BY scope_key LIMIT 20'''), self.params())).mappings()
        result = {**dict(folder), **dict(counts), 'folder_failures': [dict(r) for r in samples]}
        # Shared child work can have observations from multiple parent lists.
        # A restarted parent must not silently erase another list's required child.
        missing_members = await db.scalar(text('''SELECT count(DISTINCT required.key)
            FROM archive_folder_work m JOIN archive_folder_work p
              ON p.snapshot_id=m.snapshot_id AND p.capture_generation=m.capture_generation
              AND p.scope_key=m.parent_scope_key AND p.scope_generation=m.parent_generation
            CROSS JOIN LATERAL (VALUES (m.evidence->>'target_key'),(m.evidence->>'children_key')) required(key)
            LEFT JOIN archive_folder_work target ON target.snapshot_id=m.snapshot_id
              AND target.capture_generation=m.capture_generation AND target.scope_key=required.key
            WHERE m.snapshot_id=:sid AND m.capture_generation=:gen AND m.phase='membership'
              AND m.state<>'excluded' AND p.state<>'excluded' AND required.key IS NOT NULL
              AND (target.scope_key IS NULL OR target.state='excluded')'''), self.params())
        if missing_members:
            result['folders_failed'] += missing_members
            result['folder_failures'].append({'phase': 'membership', 'kind': 'required_folder_not_covered',
                'count': missing_members})
        totals = (await db.execute(text('''SELECT COALESCE(sum(content_size),0) FROM snapshot_items WHERE snapshot_id=
            COALESCE((SELECT reuse_chain_root_id FROM snapshots WHERE id=:sid), :sid)'''),
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
        attachment_receipts = await self._attachment_receipts(db, prior_id, pending_ids, normalize_version=collection is not None)
        for item in pending:
            row = source.get(item.external_id)
            if row is None:
                raise ArchiveConflict('archive predecessor content disappeared')
            item.receipt = {'kind': 'copy', 'change_key': item.change_key, 'hash': row.content_hash, 'size': row.content_size,
                'blob_path': row.blob_path, 'backend_id': str(row.backend_id),
                'encryption_key_id': row.encryption_key_id, 'backup_version': (row.backup_version or 1) if collection is not None else row.backup_version,
                'attachments': attachment_receipts[item.external_id]}
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
        await self.resolve_copies(db, ids=pending_ids)

    async def _attachment_receipts(self, db, snapshot_id, ids, *, normalize_version=False):
        evidence = {iid: [hashlib.sha256(), 0] for iid in ids}
        rows = await db.stream(select(SnapshotItem.parent_external_id, SnapshotItem.external_id,
            SnapshotItem.content_hash, SnapshotItem.blob_path, SnapshotItem.content_size,
            SnapshotItem.backend_id, SnapshotItem.encryption_key_id, SnapshotItem.backup_version)
            .where(SnapshotItem.snapshot_id == snapshot_id, SnapshotItem.item_type == 'ARCHIVE_ATTACHMENT',
                SnapshotItem.parent_external_id.in_(ids)).order_by(SnapshotItem.parent_external_id,
                SnapshotItem.external_id).execution_options(yield_per=128))
        async for row in rows:
            parent, *values = row
            if normalize_version:
                values[-1] = values[-1] or 1
            evidence[parent][0].update(json.dumps(values, default=str, separators=(',', ':')).encode() + b'\n')
            evidence[parent][1] += 1
        return {iid: {'sha256': value[0].hexdigest(), 'count': value[1]} for iid, value in evidence.items()}

    async def resolve_copies(self, db, *, ids=None):
        """A queued intent is not a receipt; validate completed tasks and bytes."""
        after = ''
        while True:
            query = select(Item).outerjoin(CopyWorkTask, CopyWorkTask.id == Item.copy_task_id).where(
                Item.snapshot_id == self.fence.snapshot_id, Item.capture_generation == self.generation,
                Item.state.in_(('copy_pending', 'stored')), Item.receipt['kind'].as_string() == 'copy',
                Item.external_id > after,
                (Item.copy_task_id.is_(None) | (CopyWorkTask.state == 'COMPLETED')))
            if ids is not None:
                query = query.where(Item.external_id.in_(ids))
            ready = (await db.scalars(query.order_by(Item.external_id, Item.scope_key).limit(128))).all()
            if not ready:
                break
            after = ready[-1].external_id
            actual = await self._attachment_receipts(db, self.fence.snapshot_id, [item.external_id for item in ready])
            for item in ready:
                if (item.receipt or {}).get('attachments') != actual[item.external_id]:
                    item.state, item.gap_kind = 'failed', 'attachment_receipt_conflict'
            await db.flush()
        await db.execute(text('''UPDATE archive_item_work i SET state='stored',receipt_item_id=s.id
            FROM snapshot_items s WHERE i.snapshot_id=:sid AND i.capture_generation=:gen AND i.state='copy_pending'
            AND (i.copy_task_id IS NULL OR EXISTS (SELECT 1 FROM copy_work_tasks t WHERE t.id=i.copy_task_id AND t.state='COMPLETED'))
            AND s.snapshot_id=i.snapshot_id AND s.external_id=i.external_id AND s.item_type='ARCHIVE_ITEM'
            AND COALESCE(s.metadata->>'changeKey',s.metadata->>'change_key') IS NOT DISTINCT FROM i.change_key
            AND s.content_hash=i.receipt->>'hash' AND s.blob_path=i.receipt->>'blob_path'
            AND s.content_size=(i.receipt->>'size')::bigint AND s.backend_id::text=i.receipt->>'backend_id'
            AND s.encryption_key_id IS NOT DISTINCT FROM i.receipt->>'encryption_key_id'
            AND s.backup_version IS NOT DISTINCT FROM (i.receipt->>'backup_version')::integer
            AND (CAST(:ids AS text[]) IS NULL OR i.external_id=ANY(CAST(:ids AS text[])))'''), self.params(ids=ids))

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
