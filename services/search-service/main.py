"""Full-Text Search Service - Search across backup snapshot items
Port: 8013

Responsibilities:
- Index snapshot items for full-text search
- Search across emails, files, Teams messages, etc.
- Support filtering by workload type, date range, tenant
- Return ranked search results with highlights
"""
import re
import uuid
from datetime import datetime
from typing import Optional, List, Dict, Any
from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy import select, and_, func, or_, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from shared.database import async_session_factory, init_db, close_db
from shared.models import SnapshotItem, Snapshot, Resource, Tenant

from shared.audit_middleware import AuditMiddleware  # Ops/Data Check 3: mutation-audit backstop
app = FastAPI(title="Full-Text Search Service", version="1.0.0")
app.add_middleware(AuditMiddleware, service_name="search-service")
from shared.auth_enforcement import AuthMiddleware, is_admin, verified_user  # Ops/Data Check 17: verified principal required
app.add_middleware(AuthMiddleware, service_name="search-service", enforce_tenant_scope=True)


# Dependency - must be before route decorators
async def get_db():
    async with async_session_factory() as session:
        yield session


@app.on_event("startup")
async def startup():
    """Initialize database"""
    from shared import core_metrics
    core_metrics.init()
    await init_db()
    # NO search DDL here. The former create_search_index() ran a plain
    # (non-CONCURRENT) CREATE INDEX + ALTER TABLE ... ADD COLUMN IF NOT EXISTS
    # (ACCESS EXCLUSIVE even as a no-op) + CREATE INDEX on snapshot_items in
    # ONE transaction at every pod start, and the liveness probe SIGKILLed it
    # mid-DDL. idx_snapshot_items_metadata_gin, the search_vector column and
    # idx_snapshot_items_search_vector already exist in prod. Any future
    # search index MUST ship as an alembic migration using
    # CREATE INDEX CONCURRENTLY — never from a service startup hook.


@app.on_event("shutdown")
async def shutdown():
    await close_db()


@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "search"}


@app.get("/api/v1/search")
async def search_snapshots(
    q: str = Query(..., min_length=1, description="Search query"),
    tenantId: Optional[str] = Query(None),
    workloadType: Optional[str] = Query(None),
    itemType: Optional[str] = Query(None),
    dateFrom: Optional[str] = Query(None),
    dateTo: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    size: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    """
    Full-text search across all backup snapshot items.

    Searches in:
    - Item names
    - Email subjects, bodies, senders, recipients
    - File names and content
    - Teams message content
    - Metadata fields

    Supports filtering by:
    - tenantId: Filter by tenant
    - workloadType: Filter by workload (exchange, onedrive, sharepoint, teams, entra)
    - itemType: Filter by specific item type (EMAIL, FILE, TEAMS_MESSAGE, etc.)
    - dateFrom/dateTo: Filter by backup date range (ISO 8601)
    """
    # Build search conditions
    filters = []

    if tenantId:
        filters.append(SnapshotItem.tenant_id == uuid.UUID(tenantId))

    if itemType:
        filters.append(SnapshotItem.item_type == itemType.upper())

    if workloadType:
        # Map workload types to item type patterns
        workload_map = {
            "exchange": ["EMAIL", "CALENDAR", "CONTACT"],
            "onedrive": ["FILE", "ONEDRIVE_FILE"],
            "sharepoint": ["SHAREPOINT_FILE", "SHAREPOINT_LIST_ITEM"],
            "teams": ["TEAMS_MESSAGE", "TEAMS_MESSAGE_REPLY", "TEAMS_CHAT_MESSAGE"],
            "entra": ["ENTRA_USER_PROFILE", "ENTRA_GROUP_META", "ENTRA_RELATIONSHIP"],
        }
        item_types = workload_map.get(workloadType.lower(), [])
        if item_types:
            filters.append(SnapshotItem.item_type.in_(item_types))

    if dateFrom:
        filters.append(SnapshotItem.created_at >= datetime.fromisoformat(dateFrom))

    if dateTo:
        filters.append(SnapshotItem.created_at <= datetime.fromisoformat(dateTo))

    # Build search query
    search_terms = extract_search_terms(q)

    # Use PostgreSQL full-text search with JSONB containment
    stmt = (
        select(SnapshotItem, Snapshot, Resource, Tenant)
        .join(Snapshot, SnapshotItem.snapshot_id == Snapshot.id)
        .join(Resource, Snapshot.resource_id == Resource.id)
        .join(Tenant, Resource.tenant_id == Tenant.id)
    )

    if filters:
        stmt = stmt.where(and_(*filters))

    # Apply text search
    if search_terms:
        text_filters = build_text_search_filters(search_terms)
        stmt = stmt.where(and_(*text_filters))

    # Order by relevance (using created_at as proxy for now)
    stmt = stmt.order_by(SnapshotItem.created_at.desc())

    # Get total count
    count_stmt = select(func.count()).select_from(stmt.subquery())
    total_result = await db.execute(count_stmt)
    total = total_result.scalar() or 0

    # Paginate
    stmt = stmt.offset((page - 1) * size).limit(size)
    result = await db.execute(stmt)
    rows = result.all()

    # Format results
    items = []
    for snapshot_item, snapshot, resource, tenant in rows:
        items.append(format_search_result(snapshot_item, snapshot, resource, tenant, q))

    return {
        "query": q,
        "results": items,
        "totalResults": total,
        "page": page,
        "pageSize": size,
        "totalPages": max(1, (total + size - 1) // size),
        "filters": {
            "tenantId": tenantId,
            "workloadType": workloadType,
            "itemType": itemType,
            "dateFrom": dateFrom,
            "dateTo": dateTo,
        }
    }


def _suggestion_tenant_scope(user: dict, tenant_id: Optional[str]) -> List[uuid.UUID]:
    """Tenant UUIDs the suggestions query may touch — never empty.

    Same principal/tenant mechanism as /api/v1/search: the verified principal
    (AuthMiddleware, enforce_tenant_scope=True) carries the caller's entitled
    `tenant_ids`; a caller-supplied `tenantId` is honoured only if entitled.
    Admin / internal principals carry no tenant claims, so they must name one
    — the query is never run without a tenant_id predicate.
    """
    privileged = is_admin(user) or user.get("_principal") == "internal"
    entitled: List[uuid.UUID] = []
    for raw in user.get("tenant_ids") or []:
        try:
            entitled.append(uuid.UUID(str(raw)))
        except (TypeError, ValueError):
            continue
    if tenant_id:
        try:
            requested = uuid.UUID(tenant_id)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid tenantId")
        if not privileged and requested not in entitled:
            raise HTTPException(status_code=403, detail="Not entitled to the requested tenant")
        return [requested]
    if entitled:
        return entitled
    if privileged:
        raise HTTPException(status_code=400, detail="tenantId is required")
    raise HTTPException(status_code=403, detail="No tenant access")


@app.get("/api/v1/search/suggestions")
async def search_suggestions(
    q: str = Query(..., min_length=2),
    limit: int = Query(10, ge=1, le=50),
    tenantId: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(verified_user),
):
    """Get search suggestions from indexed content (tenant-scoped)"""
    # An unscoped `name ILIKE '%q%'` per keystroke plans a full seq scan of
    # snapshot_items (the hottest table). Always constrain to tenant_id.
    tenant_scope = _suggestion_tenant_scope(user, tenantId)
    stmt = (
        select(SnapshotItem.name, SnapshotItem.item_type, func.count().label('count'))
        .where(SnapshotItem.tenant_id.in_(tenant_scope))
        .where(SnapshotItem.name.ilike(f"%{q}%"))
        .group_by(SnapshotItem.name, SnapshotItem.item_type)
        .order_by(func.count().desc())
        .limit(limit)
    )

    result = await db.execute(stmt)
    rows = result.all()

    suggestions = []
    for name, item_type, count in rows:
        suggestions.append({
            "text": name,
            "type": item_type,
            "count": count,
        })

    return {"suggestions": suggestions, "query": q}


@app.post("/api/v1/search/reindex")
async def reindex_snapshots(
    snapshot_ids: List[str] = None,
    limit: int = Query(1000, ge=1, le=10000),
    db: AsyncSession = Depends(get_db),
):
    """
    Reindex snapshot items for full-text search.
    Can be used to update search index after backup completion.

    Snapshot-scoped and bounded: `snapshot_ids` are snapshots.id values (the
    old code matched them against snapshot_items.id, so it never indexed a
    snapshot's items — and with no ids it selected the whole table). At most
    `limit` items are indexed per call.
    """
    if not snapshot_ids:
        raise HTTPException(status_code=400, detail="snapshot_ids is required")
    try:
        snapshot_uuids = [uuid.UUID(sid) for sid in snapshot_ids]
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="Invalid snapshot id")

    stmt = (
        select(SnapshotItem)
        .where(SnapshotItem.snapshot_id.in_(snapshot_uuids))
        .limit(limit)
    )

    result = await db.execute(stmt)
    items = result.scalars().all()

    indexed_count = 0
    for item in items:
        await index_snapshot_item(item, db)
        indexed_count += 1

    # The per-item search_vector UPDATEs ran on the request session; without
    # an explicit commit they are rolled back when get_db() closes it.
    await db.commit()

    return {"indexed": indexed_count, "limit": limit}


async def index_snapshot_item(item: SnapshotItem, db: AsyncSession):
    """Index a single snapshot item for full-text search"""
    try:
        # SnapshotItem ORM aliases the JSON column to `extra_data` (the DB
        # column is named ``metadata``); use the attribute the ORM exposes.
        metadata = getattr(item, "extra_data", None) or {}
        raw_data = metadata.get("raw", {}) if isinstance(metadata, dict) else {}
        structured = metadata.get("structured", {}) if isinstance(metadata, dict) else {}

        # Chat messages have an empty extra_data on snapshot_items since
        # the 2026-05-13 Level 2 refactor; the body / sender / payload
        # live in chat_thread_messages. Hydrate them so search continues
        # to index chat content.
        if item.item_type in ("TEAMS_CHAT_MESSAGE",) and not raw_data:
            try:
                row = (await db.execute(text(
                    "SELECT ctm.body_content, ctm.from_display_name, "
                    "       ctm.metadata_raw "
                    "  FROM chat_thread_messages ctm "
                    "  JOIN chat_threads ct ON ct.id = ctm.chat_thread_id "
                    " WHERE ct.tenant_id = :tid "
                    "   AND ct.chat_id = :cid "
                    "   AND ctm.message_external_id = :ext "
                    "   AND ct.archived_at IS NULL "
                    "   AND ctm.archived_at IS NULL "
                    " LIMIT 1"
                ), {
                    "tid": str(item.tenant_id),
                    "cid": item.parent_external_id,
                    "ext": item.external_id,
                })).first()
                if row is not None:
                    raw_data = row.metadata_raw if isinstance(row.metadata_raw, dict) else {}
                    raw_data.setdefault("body", {})
                    if row.body_content:
                        raw_data["body"]["content"] = row.body_content
                    if row.from_display_name:
                        raw_data.setdefault("from", {}).setdefault("user", {})[
                            "displayName"
                        ] = row.from_display_name
            except Exception as _e:
                # Search index is best-effort — a JOIN miss falls back to
                # indexing whatever's already in raw_data (name/folder_path).
                print(f"[SEARCH] chat-body JOIN failed for item {item.id}: {_e}")

        # Build searchable text content
        search_text = build_search_text(item, raw_data, structured)

        # Update search vector (PostgreSQL tsvector)
        # This enables fast full-text search
        await db.execute(text("""
            UPDATE snapshot_items
            SET search_vector = to_tsvector('english', :search_text)
            WHERE id = :item_id
        """), {"search_text": search_text, "item_id": str(item.id)})

    except Exception as e:
        print(f"[SEARCH] Failed to index item {item.id}: {e}")


def build_search_text(item: SnapshotItem, raw_data: Dict, structured: Dict) -> str:
    """Build searchable text from snapshot item data"""
    parts = []

    # Add item name
    if item.name:
        parts.append(item.name)

    # Add item type
    parts.append(item.item_type)

    # Add email-specific fields
    if item.item_type == "EMAIL":
        parts.append(raw_data.get("subject", ""))
        parts.append(raw_data.get("bodyPreview", ""))
        body = raw_data.get("body", {})
        parts.append(body.get("content", ""))
        from_addr = raw_data.get("from", {}).get("emailAddress", {})
        parts.append(from_addr.get("name", ""))
        parts.append(from_addr.get("address", ""))
        for recipient in raw_data.get("toRecipients", []):
            addr = recipient.get("emailAddress", {})
            parts.append(addr.get("name", ""))
            parts.append(addr.get("address", ""))

    # Add Teams message content
    if item.item_type in ("TEAMS_MESSAGE", "TEAMS_MESSAGE_REPLY", "TEAMS_CHAT_MESSAGE"):
        body = raw_data.get("body", {})
        parts.append(body.get("content", ""))
        parts.append(body.get("contentPreview", ""))
        parts.append(raw_data.get("subject", ""))
        parts.append(raw_data.get("summary", ""))

    # Add file content
    if item.item_type in ("FILE", "ONEDRIVE_FILE", "SHAREPOINT_FILE"):
        parts.append(raw_data.get("name", ""))
        parts.append(raw_data.get("description", ""))
        # File content might be in structured metadata
        parts.append(str(structured.get("content_preview", "")))

    # Add Entra ID fields
    if item.item_type.startswith("ENTRA_"):
        parts.append(raw_data.get("displayName", ""))
        parts.append(raw_data.get("description", ""))
        parts.append(raw_data.get("mail", ""))
        parts.append(raw_data.get("userPrincipalName", ""))
        parts.append(raw_data.get("jobTitle", ""))
        parts.append(raw_data.get("department", ""))

    # Add structured metadata fields
    if structured:
        if "permissions" in structured:
            for perm in structured.get("permissions", []):
                parts.append(str(perm.get("display_name", "")))

    # Clean and normalize
    search_text = " ".join(str(p) for p in parts if p)
    # Remove HTML tags
    search_text = re.sub(r'<[^>]+>', ' ', search_text)
    # Remove extra whitespace
    search_text = re.sub(r'\s+', ' ', search_text).strip()

    return search_text


def extract_search_terms(query: str) -> List[str]:
    """Extract search terms from query string"""
    # Split on whitespace and filter out empty strings
    terms = [t.strip() for t in query.split() if t.strip()]
    return terms


def build_text_search_filters(search_terms: List[str]) -> list:
    """Build PostgreSQL text search filters"""
    filters = []

    for term in search_terms:
        # Search in name
        name_filter = SnapshotItem.name.ilike(f"%{term}%")

        # Search in metadata JSONB
        # PostgreSQL JSONB containment operator
        metadata_filter = text(
            "snapshot_items.metadata::text ILIKE :term"
        ).bindparams(term=f"%{term}%")

        # Combine with OR (match name OR metadata)
        filters.append(or_(name_filter, metadata_filter))

    # All terms must match (AND)
    return filters


def format_search_result(
    item: SnapshotItem,
    snapshot: Snapshot,
    resource: Resource,
    tenant: Tenant,
    query: str,
) -> Dict[str, Any]:
    """Format a search result for API response"""
    # SnapshotItem maps the DB column ``metadata`` to ``extra_data``;
    # ``item.metadata`` is the declarative Base's MetaData object.
    metadata = item.extra_data or {}
    raw_data = metadata.get("raw", {}) if isinstance(metadata, dict) else {}

    # Build preview/snippet
    preview = build_preview(item, raw_data, query)

    return {
        "id": str(item.id),
        "snapshotId": str(item.snapshot_id),
        "itemType": item.item_type,
        "name": item.name,
        "externalId": item.external_id,
        "contentSize": item.content_size,
        "createdAt": item.created_at.isoformat() if item.created_at else None,
        "preview": preview,
        "source": {
            "resourceId": str(resource.id),
            "resourceName": resource.display_name,
            "resourceType": resource.type.value if hasattr(resource.type, 'value') else resource.type,
            "tenantId": str(tenant.id),
            "tenantName": tenant.display_name,
        },
        "snapshot": {
            "id": str(snapshot.id),
            "type": snapshot.type.value if hasattr(snapshot.type, 'value') else snapshot.type,
            "label": snapshot.snapshot_label,
            "createdAt": snapshot.started_at.isoformat() if snapshot.started_at else None,
        },
        "blobPath": item.blob_path,
        "downloadUrl": f"/api/v1/snapshots/{item.snapshot_id}/items/{item.id}/download",
    }


def build_preview(item: SnapshotItem, raw_data: Dict, query: str) -> str:
    """Build a preview snippet with highlighted query terms"""
    preview_text = ""

    if item.item_type == "EMAIL":
        subject = raw_data.get("subject", "")
        body_preview = raw_data.get("bodyPreview", "") or raw_data.get("body", {}).get("content", "")
        # Strip HTML
        body_preview = re.sub(r'<[^>]+>', ' ', body_preview)
        preview_text = f"Subject: {subject}\n{body_preview[:300]}"

    elif item.item_type in ("TEAMS_MESSAGE", "TEAMS_MESSAGE_REPLY", "TEAMS_CHAT_MESSAGE"):
        body = raw_data.get("body", {})
        content = body.get("content", "") or body.get("contentPreview", "")
        content = re.sub(r'<[^>]+>', ' ', content)
        preview_text = content[:300]

    elif item.item_type in ("FILE", "ONEDRIVE_FILE", "SHAREPOINT_FILE"):
        preview_text = raw_data.get("description", "") or f"File: {item.name}"

    else:
        preview_text = f"{item.item_type}: {item.name}"

    # Truncate
    if len(preview_text) > 300:
        preview_text = preview_text[:297] + "..."

    return preview_text
