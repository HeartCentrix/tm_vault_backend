"""Authorization for export artifacts — stated once, used by every route.

Every route that creates, describes or serves an export artifact enforces the
same rules from here: the chat-export API (`chat_export.py`) and, in
`main.py`, `POST /api/v1/jobs/export`, `POST /api/v1/jobs/restore*`,
`POST /api/v1/resources/{id}/export-or-restore`,
`POST /api/v1/sharepoint/{id}/download`,
`GET /api/v1/jobs/export/{job_id}/status`, `.../download` and
`GET /api/v1/jobs/restore/{job_id}/status`. Keeping the rules in one module is
the point: the 2026-07-31 breach existed because the chat trigger path had a
(vacuous) check, the download path had none at all, and the generic export
trigger — a second door into the same pipeline — had neither.

THE MODEL
---------
1. A verified JWT subject is required. No subject -> 401.
2. The tenant that owns every resource in the export's scope must be one the
   caller is entitled to act in (`shared.tenant_entitlement`). Not entitled
   -> 403. An entitlement we cannot establish is a DENY, never a skip.
3. Ordinary users may act only on resources belonging to their own M365
   identity: the caller's token email must match the resource's (or its parent
   ENTRA_USER's) UPN. A resource no ordinary user's UPN can match — a
   SharePoint site, a Teams channel, an M365 group — is therefore
   operator-only. There is no exemption for "generic" (Recovery / mailbox /
   OneDrive / SharePoint) export jobs: `restore-worker` writes Teams message
   bodies into the generic EXPORT_ZIP artifact as well
   (workers/restore-worker/main.py, TEAMS_MESSAGE* branch), so an exemption
   there was a second door into exactly the data rule 3 exists to protect.
4. Holders of an operator role bypass rule 3 *inside their entitled
   tenants* — this is the IT/helpdesk path (exporting a departing employee's
   chats, restoring a shared SharePoint site), the product's primary workflow.
5. SUPER_ADMIN additionally bypasses rule 2 as a logged break-glass.

BOTH ENDS OF A RESTORE ARE AUTHORIZED (source scope AND destination)
--------------------------------------------------------------------
A restore does not only read backed-up data, it WRITES it somewhere. The
destination therefore gets the same rules 1-5 as the source scope: rule 3 means
an ordinary user can only restore into their own mailbox/OneDrive, and a
cross-user restore (the helpdesk workflow) needs the operator grant.

Which spec keys are destinations is not a judgement call made per call site —
it is the explicit classification below, because the round-3 blocker was
exactly a call site that authorized one spelling while the worker read
another: `trigger_restore` authorized `targetResourceId`, but
`workers/restore-worker/main.py:1507` reads
``spec.get("targetUserId") or spec.get("targetResourceId")`` — targetUserId
WINS — and `_resolve_onedrive_target_user` (:3070) reads `targetUserId` only.
Renaming one JSON key put the destination past the guard, which let an ordinary
user write their own backed-up files into a colleague's OneDrive.

  * DESTINATION_RESOURCE_KEYS name a `Resource` row: resolved by
    `Resource.id` and authorized as a write target. The legacy `external_id`
    spelling that `restore_cross_user` also accepts is refused with 400 here,
    so the row we authorize is provably the row the worker resolves.
  * DESTINATION_OPERATOR_KEYS name something that is not in `resources` at all
    (a Power Platform environment, a SharePoint site that does not exist yet,
    an Azure target RG/server/disk). There is no row, so there is no owner to
    match: setting one requires an operator role. In practice this costs
    nothing — every workload those keys apply to (POWER_*, SHAREPOINT_SITE,
    AZURE_*) has no owning UPN and is already operator-only by rule 3.
  * NON_DESTINATION_SPEC_KEYS is the rest, enumerated rather than assumed.
    `targetFolder` lives here on purpose: it is a folder root INSIDE the
    already-authorized target, not a principal boundary.

`SPEC_KEYS` — the allowlist `main.py` applies to the request body — is the
union of the three sets, so a new spec key cannot reach the worker without
being classified. Anything destination-shaped that is somehow still
unclassified (a second, hand-built spec on some future write path) aborts the
request in `assert_destinations_classified`: fail closed, not a comment.

WHERE THE DECISION GETS ITS FACTS
---------------------------------
Never from the job spec. `main.trigger_export` had no auth dependency and
merged the caller's raw body into the row (`spec={..., **request}`), so a
caller controlled `spec.kind` and `spec.userId` — the exact fields an earlier
version of `ensure_job_access` read. An "owner" recorded that way is forgeable,
so it is not an owner.
Every rule above is therefore evaluated against `Resource` rows loaded from
the database. Ids that arrive on a request body, or that were persisted into a
`Job.spec`, are used only as lookup keys: a forged id can ADD a target to the
check but can never remove one, so an export of N snapshots is allowed only if
the caller is authorized on every resource those N snapshots belong to. That
also covers scope smuggling — listing your own snapshot (which is what stamps
`Job.resource_id`) while passing someone else's item ids alongside it.

WHY NOT PURE TENANT SCOPING
---------------------------
Tenant entitlement alone does not close the reported breach in this
deployment. `auth-service.oauth_callback` has no allowlist: anyone who can
complete Entra SSO gets a `PlatformUser` attached to `select(Organization)
.limit(1)` with role USER, and entitlement is derived from org membership.
One org + no login gate means "tenant-scoped" == "everyone who can log in",
so dropping rule 3 would leave every employee able to download any
colleague's Teams chat history. Rule 3 stays; rule 4 is what makes the
product usable.

GRANTING AN OPERATOR ROLE (operator action, no schema change)
-------------------------------------------------------------
Nothing in the product writes `user_roles` except `oauth_callback`, which
inserts `USER`. The table, the enum values and the read path
(`auth-service._load_token_claims_from_db` -> `roles` claim) all already
exist — only the row is missing. Grant the least-privileged role that covers
"export another user's backed-up data" with one statement, then have the
operator sign out and back in (roles are read at token mint):

    -- psql tmvault_prod
    INSERT INTO user_roles (user_id, role)
    SELECT id, 'RESTORE_OPERATOR' FROM platform_users
     WHERE lower(email) = lower('operator@taylormorrison.com')
    ON CONFLICT DO NOTHING;

    -- verify
    SELECT u.email, r.role FROM platform_users u
      JOIN user_roles r ON r.user_id = u.id
     WHERE lower(u.email) = lower('operator@taylormorrison.com');

    -- revoke
    DELETE FROM user_roles r USING platform_users u
     WHERE r.user_id = u.id AND r.role = 'RESTORE_OPERATOR'
       AND lower(u.email) = lower('operator@taylormorrison.com');

`role` is a VARCHAR on installs that predate the userrole-enum migration and
a `userrole` enum after it; the literal above works on both. The spelling must
match the `UserRole` enum member exactly — `role` has no CHECK constraint but
the ORM coerces it (`shared/models.py`, `SAEnum(UserRole)`), so a typo makes
every subsequent token mint for that user raise. Run the verify SELECT and
read the value back before the grantee signs in again.

Until the grant is run, export is self-service only: each user can export
their own mailbox / OneDrive / chats and nothing else — deliberately
fail-closed for a product that holds other people's private conversations.
Resources with no owning UPN (SharePoint sites, Teams channels, M365 groups,
Azure workloads) need the grant.

The same grant is what a CROSS-USER RESTORE needs: restoring a departed
employee's files into a colleague's OneDrive/mailbox is a write into a resource
the requester does not own, and so is "recover to a new SharePoint site",
restoring a flow into a different Power Platform environment, or naming an Azure
target RG. Those all return 403 for an ordinary user by design (see the
destination rules below) — an operator role, not a request-body flag, is what
opens them.

ROLLOUT ORDER: alert-service, then job-service, then auth-service
----------------------------------------------------------------
auth-service is what populates the `tenantIds` claim, so it goes LAST. Its two
consumers fail closed while the claim is empty, which means shipping them
early cannot open anything, and shipping auth-service early makes the claim
live against an old alert-service that has no admin guard on
`DELETE /api/v1/access-groups/{id}`.
This module is order-safe in both directions by construction: `entitled_tenants`
re-derives entitlement from the database when the claim is absent (pinned by
tests/services/test_chat_export_authz.py::
test_empty_claim_falls_back_to_db_org_membership), and the `roles` claim it
reads for rule 4 predates the claim fix — auth-service has always minted it
from `user_roles`. So an operator grant takes effect as soon as the grantee
signs in again, whether or not auth-service has been rolled yet.

WHAT THIS GATE DOES NOT COVER
-----------------------------
Reading backed-up content directly. `services/snapshot-service` and
`services/resource-service` have no authentication at all, and api-gateway
proxies their content routes without validating a token — including
`GET /api/v1/resources/snapshots/{id}/chats`, `.../chats/groups`,
`.../messages` and `.../items/{item_id}/content`. That is a separate,
pre-existing architectural gap (the data tier trusts the gateway; the gateway
forwards cookies without verifying them) and needs its own change. This gate
is still worth having: it is what stops an authenticated user from
manufacturing a durable, downloadable ZIP of someone else's mailbox or chat
history, and it is what puts a name in the audit trail when one is built.
"""
import hashlib
import logging
import re
import uuid
from typing import Any, Dict, Iterable, Optional, Set

from fastapi import HTTPException
from sqlalchemy import select

from shared.models import Job, Resource, Snapshot, SnapshotItem
from shared.tenant_entitlement import resolve_entitled_tenant_ids

log = logging.getLogger("export.authz")

# `chat_export.trigger` tags every chat-export job spec with this kind. It is
# the marker for "this job row carries an owner we can enforce".
CHAT_EXPORT_KIND = "chat_export_thread"

# Elevated roles. "tenant_admin" (lowercase) is a legacy claim spelling kept
# for tokens minted by older builds.
ADMIN_ROLES = {"SUPER_ADMIN", "ORG_ADMIN", "TENANT_ADMIN", "tenant_admin"}
# Roles allowed to export/download another user's backed-up data inside their
# entitled tenants. RESTORE_OPERATOR / BACKUP_OPERATOR are the least
# privileged of these and are the documented grant (see module docstring).
EXPORT_OPERATOR_ROLES = ADMIN_ROLES | {"BACKUP_OPERATOR", "RESTORE_OPERATOR"}


# ----------------------------------------------------------------- destinations
#
# Every request-body key the export/restore pipeline consumes, classified by
# what it can do to the DESTINATION of the write. Derived by grepping every
# top-level `spec.get(...)` in workers/restore-worker/*.py and
# workers/azure-workload-worker/*.py; pinned by
# tests/services/test_restore_destination_authz.py::
# test_every_destination_shaped_key_the_workers_read_is_classified.

# Destinations that name a `Resource` row. Authorized as a write target under
# rules 1-5, by `Resource.id` only.
#   targetUserId     -> restore_cross_user (main.py:1507, preferred over
#                       targetResourceId) and _resolve_onedrive_target_user
#                       (:3070, the ONLY key that engine reads)
#   targetResourceId -> restore_cross_resource (:1705) and
#                       _resolve_sharepoint_target_site (:3196)
DESTINATION_RESOURCE_KEYS = ("targetUserId", "targetResourceId")

# Destinations with no `Resource` row to own -> operator-only.
#   targetEnvironmentId -> Power Platform environment a flow/app is restored
#                          into (main.py:1181 -> :3887/:3957)
#   newSiteName/Alias/OwnerEmail -> provisions a NEW SharePoint site and hands
#                          ownership to newSiteOwnerEmail (:3186-3195)
#   azureRestoreParams  -> target RG / subscription / server / db / disk names
#                          (workers/azure-workload-worker/main.py:151)
DESTINATION_OPERATOR_KEYS = (
    "targetEnvironmentId", "newSiteName", "newSiteAlias", "newSiteOwnerEmail",
    "azureRestoreParams",
)

# Not destinations. `targetFolder` is a folder root inside the target resource
# that has already been authorized, and `conflictMode`/`overwrite` pick
# in-place vs `Restored by TM/{date}/…` within it — no principal boundary is
# crossed by any of them.
NON_DESTINATION_SPEC_KEYS = (
    "exportFormat", "includeAttachments", "preserveTree", "scope",
    "folderPaths", "excludedItemIds", "contactFolders", "workloads",
    "format", "pstGranularity", "pstIncludeTypes", "includeNestedDetail",
    "entraSections", "includeGroupMembership", "includeAuMembership",
    "conflictMode", "targetFolder", "overwrite", "recoverMode",
    "azureRestoreMode",
)

# The allowlist main.py applies to the request body. Being the union means the
# allowlist and the destination classification cannot drift apart: a key added
# to the pipeline has to land in one of the three sets above.
SPEC_KEYS = (
    *NON_DESTINATION_SPEC_KEYS,
    *DESTINATION_RESOURCE_KEYS,
    *DESTINATION_OPERATOR_KEYS,
)

_CLASSIFIED_SPEC_KEYS = frozenset(SPEC_KEYS)

# Names that read like "this is where the data goes". Used only to catch keys
# nothing has classified — an explicitly classified key (`targetFolder`) is
# never second-guessed by a regex.
_DESTINATION_SHAPED = re.compile(
    r"^(target|destination|restore_?to|new_?site|new_?owner)", re.IGNORECASE
)


def is_destination_shaped(key: str) -> bool:
    """Does this spec key name look like it decides where a restore writes?"""
    return bool(_DESTINATION_SHAPED.match(str(key)))


def _roles(user: dict) -> list:
    return list(user.get("roles") or [])


def is_admin(user: dict) -> bool:
    return any(r in ADMIN_ROLES for r in _roles(user))


def is_export_operator(user: dict) -> bool:
    return any(r in EXPORT_OPERATOR_ROLES for r in _roles(user))


def is_super_admin(user: dict) -> bool:
    return "SUPER_ADMIN" in _roles(user)


def subject_uuid(user: dict) -> Optional[uuid.UUID]:
    try:
        return uuid.UUID(str(user.get("id")))
    except (ValueError, TypeError):
        return None


def auth_denied(reason: str, user: dict, **fields) -> HTTPException:
    log.warning(
        "auth_denied reason=%s user_id=%s user_tenants=%s %s",
        reason, user.get("id"), user.get("tenant_ids"),
        " ".join(f"{k}={v}" for k, v in fields.items()),
    )
    return HTTPException(403, detail={"error": "AUTH_DENIED"})


async def entitled_tenants(sess: Any, user: dict) -> set:
    """Tenants the caller may act in. Empty set means DENY — never "skip".

    The JWT claim is authoritative when present. When it is absent we
    re-derive entitlement from the database instead of waving the request
    through: access tokens minted before the `tenantIds` claim was fixed are
    still in flight for up to JWT_EXPIRATION_HOURS, and an absent claim must
    not read as unlimited access.
    """
    claimed = set()
    for raw in user.get("tenant_ids") or []:
        try:
            claimed.add(raw if isinstance(raw, uuid.UUID) else uuid.UUID(str(raw)))
        except (ValueError, TypeError):
            continue
    if claimed:
        return claimed
    return set(await resolve_entitled_tenant_ids(sess, subject_uuid(user)))


async def owns_resource(sess: Any, user: dict, res: Resource) -> bool:
    """Does the caller own the M365 identity this resource belongs to?

    The frontend passes either the parent ENTRA_USER id or one of its Tier-2
    children (USER_CHATS), so both rows are candidates. The only owner signal
    shared by the token and discovery data is the UPN/email: the token's
    `email` comes from Graph /me at login, the resource's from Graph
    discovery. Rows with no recorded email are not attributable and therefore
    not owned by anyone.
    """
    email = (user.get("email") or "").strip().lower()
    if not email:
        return False
    candidates = [res]
    if res.parent_resource_id:
        parent = (
            await sess.execute(select(Resource).where(Resource.id == res.parent_resource_id))
        ).scalar_one_or_none()
        if parent is not None:
            candidates.append(parent)
    for row in candidates:
        meta = row.extra_data or {}
        for value in (row.email, meta.get("user_principal_name"), meta.get("userPrincipalName")):
            if value and str(value).strip().lower() == email:
                return True
    return False


async def _ensure_tenant(
    sess: Any, user: dict, tenant_id, *, what: str, ident, entitled: Optional[Set] = None
) -> bool:
    """Rules 1+2+5. Returns True when the caller cleared the gate via the
    SUPER_ADMIN break-glass (i.e. per-resource rules must be skipped).

    `entitled` lets a caller that checks several resources in one decision
    resolve entitlement once instead of per resource.
    """
    if not user.get("id"):
        raise HTTPException(401, detail={"error": "AUTH_REQUIRED"})
    if entitled is None:
        entitled = await entitled_tenants(sess, user)
    if tenant_id in entitled:
        return False
    if not is_super_admin(user):
        raise auth_denied("tenant_not_entitled", user, **{f"{what}_tenant": tenant_id})
    log.warning(
        "super_admin_cross_tenant user_id=%s %s_tenant=%s %s_id=%s",
        user.get("id"), what, str(tenant_id), what, str(ident),
    )
    return True


def _uuid_or_400(value: Any, kind: str) -> uuid.UUID:
    """Parse an id that came from outside. Malformed -> 400, never a 500."""
    try:
        return value if isinstance(value, uuid.UUID) else uuid.UUID(str(value))
    except (ValueError, TypeError, AttributeError):
        raise HTTPException(400, detail={"error": f"INVALID_{kind}"})


def spec_scope_ids(spec: Optional[dict], *names: str) -> list:
    """Ids as a spec happens to spell them.

    `main.trigger_export` / `trigger_restore` write snake_case
    (`snapshot_ids`), the chat router writes the request model's camelCase
    (`snapshotIds`), and rows written by the pre-fix build carry both because
    the raw body was merged in. Read every spelling — missing one would mean
    silently authorizing a narrower scope than the worker packaged.
    """
    out: list = []
    for name in names:
        for raw in (spec or {}).get(name) or []:
            if raw not in out:
                out.append(raw)
    return out


async def resolve_export_targets(
    sess: Any,
    user: dict,
    *,
    resource_ids: Iterable = (),
    snapshot_ids: Iterable = (),
    item_ids: Iterable = (),
    strict: bool = True,
) -> Dict[uuid.UUID, Resource]:
    """Every `Resource` an export scope touches, read from the database.

    This is the "server-verified state" half of the model: the caller supplies
    ids, the database supplies what they mean.

    `strict=True` is for request time, where an id that resolves to nothing
    means the caller sent junk and the request should fail loudly rather than
    queue an unattributable job. Job-scoped checks pass `strict=False`:
    retention prunes snapshot rows out from under a finished artifact, and
    that must not read as junk — the caller simply loses the ability to prove
    ownership that way (see `ensure_job_access`).

    A target that resolves to a `Resource` row we cannot read is always a hard
    deny, in both modes. We do not authorize what we cannot see.
    """
    want: Set[uuid.UUID] = {_uuid_or_400(r, "RESOURCE_ID") for r in (resource_ids or ())}
    snap_uuids: Set[uuid.UUID] = {_uuid_or_400(s, "SNAPSHOT_ID") for s in (snapshot_ids or ())}
    item_uuids: Set[uuid.UUID] = {_uuid_or_400(i, "ITEM_ID") for i in (item_ids or ())}

    if item_uuids:
        # Items can name a snapshot the caller never listed — that is the
        # smuggling case this resolution exists to catch. Same query shape
        # main.trigger_export already runs for its byte estimate, so no new
        # access pattern on snapshot_items.
        rows = (
            await sess.execute(
                select(SnapshotItem.snapshot_id)
                .where(SnapshotItem.id.in_(item_uuids))
                .distinct()
            )
        ).scalars().all()
        snap_uuids.update(r for r in rows if r)

    if snap_uuids:
        snaps = (
            await sess.execute(select(Snapshot).where(Snapshot.id.in_(snap_uuids)))
        ).scalars().all()
        if strict and {s.id for s in snaps} != snap_uuids:
            raise HTTPException(404, detail={"error": "SNAPSHOT_NOT_FOUND"})
        want.update(s.resource_id for s in snaps if s.resource_id)

    if not want:
        return {}
    rows = (await sess.execute(select(Resource).where(Resource.id.in_(want)))).scalars().all()
    targets = {r.id: r for r in rows}
    missing = want - set(targets)
    if missing:
        if strict and not snap_uuids and not item_uuids:
            # A caller naming a resource that does not exist gets the plain
            # 404 the routes have always returned.
            raise HTTPException(404, detail={"error": "RESOURCE_NOT_FOUND"})
        raise auth_denied(
            "target_resource_unreadable", user,
            resources=",".join(sorted(str(m) for m in missing)),
        )
    return targets


async def _authorize_targets(
    sess: Any, user: dict, targets: Dict[uuid.UUID, Resource], *, entitled: Optional[Set] = None
) -> None:
    """Rules 1-5 applied to every resource in the scope. One deny denies all."""
    if not user.get("id"):
        raise HTTPException(401, detail={"error": "AUTH_REQUIRED"})
    if entitled is None:
        entitled = await entitled_tenants(sess, user)
    operator = is_export_operator(user)
    for res in targets.values():
        if await _ensure_tenant(
            sess, user, res.tenant_id, what="resource", ident=res.id, entitled=entitled
        ):
            continue  # SUPER_ADMIN break-glass, logged
        if operator:
            continue
        if not await owns_resource(sess, user, res):
            raise auth_denied("not_resource_owner", user, resource_id=res.id)


async def authorize_export_scope(
    sess: Any,
    user: dict,
    *,
    resource_ids: Iterable = (),
    snapshot_ids: Iterable = (),
    item_ids: Iterable = (),
) -> Dict[uuid.UUID, Resource]:
    """THE gate for every route that creates an export/restore artifact.

    Returns the authorized `Resource` rows keyed by id so callers can reuse
    them (e.g. to stamp the job's tenant) without a second read.
    """
    if not user.get("id"):
        raise HTTPException(401, detail={"error": "AUTH_REQUIRED"})
    targets = await resolve_export_targets(
        sess, user,
        resource_ids=resource_ids, snapshot_ids=snapshot_ids, item_ids=item_ids,
        strict=True,
    )
    if not targets:
        # Nothing to authorize means nothing to export. Refusing here keeps
        # unattributable jobs (NULL tenant_id / resource_id) out of the queue.
        raise HTTPException(400, detail={"error": "SCOPE_EMPTY"})
    await _authorize_targets(sess, user, targets)
    return targets


def _is_set(value: Any) -> bool:
    """Is this destination actually requested? The SPA sends the whole restore
    body with the unused keys nulled out, and `azureRestoreParams` defaults to
    `{}`, so only a non-empty value is a destination."""
    if value is None:
        return False
    if isinstance(value, (str, bytes, dict, list, tuple, set)):
        return len(value) > 0
    return True


def assert_destinations_classified(keys: Iterable[str]) -> None:
    """Rule (b): a destination-shaped spec key nobody classified fails CLOSED.

    `SPEC_KEYS` already makes drift impossible for the body allowlist, so this
    is the backstop for a hand-built spec (`trigger_restore` builds its own
    dict) — if someone adds `targetMailbox` / `targetSiteId` there and teaches
    the worker to read it, every restore aborts loudly instead of shipping an
    unauthorized destination.
    """
    unknown = sorted(
        str(k) for k in keys
        if str(k) not in _CLASSIFIED_SPEC_KEYS and is_destination_shaped(k)
    )
    if unknown:
        log.error("unclassified_destination_keys keys=%s", ",".join(unknown))
        raise HTTPException(
            500, detail={"error": "DESTINATION_KEY_UNCLASSIFIED", "keys": unknown}
        )


async def authorize_restore_destinations(
    sess: Any, user: dict, spec: Dict, *, entitled: Optional[Set] = None
) -> Dict[str, Resource]:
    """Rule (a): every caller-supplied destination must resolve to something
    the caller may write into.

    Returns `{spec key: authorized Resource}` so the caller can rewrite the
    spec to the canonical `Resource.id` it authorized — the worker then cannot
    resolve the key to a different row than the one that cleared the gate.
    """
    assert_destinations_classified((spec or {}).keys())
    if not user.get("id"):
        raise HTTPException(401, detail={"error": "AUTH_REQUIRED"})

    unownable = [k for k in DESTINATION_OPERATOR_KEYS if _is_set((spec or {}).get(k))]
    if unownable and not is_export_operator(user):
        raise auth_denied("destination_requires_operator", user, keys=",".join(unownable))

    wanted: Dict[str, uuid.UUID] = {}
    for key in DESTINATION_RESOURCE_KEYS:
        raw = (spec or {}).get(key)
        if not _is_set(raw):
            continue
        # A non-UUID here is the legacy external_id spelling
        # `restore_cross_user` still accepts. We refuse it rather than let the
        # worker resolve a row we never authorized.
        wanted[key] = _uuid_or_400(raw, "TARGET_ID")
    if not wanted:
        return {}

    rows = (
        await sess.execute(select(Resource).where(Resource.id.in_(set(wanted.values()))))
    ).scalars().all()
    found = {r.id: r for r in rows}
    missing = sorted(str(v) for v in wanted.values() if v not in found)
    if missing:
        raise auth_denied("destination_unresolvable", user, targets=",".join(missing))
    if entitled is None:
        entitled = await entitled_tenants(sess, user)
    await _authorize_targets(sess, user, found, entitled=entitled)
    return {key: found[rid] for key, rid in wanted.items()}


# ------------------------------------------- the decision, recorded once per job
#
# `ensure_job_access` used to re-derive the whole scope on EVERY /status poll,
# SSE tick and download: `SnapshotItem.id.in_(...)` against snapshot_items (a
# plain 87 GB table — the partitioning migration was never applied, so this is
# a PK probe, but with export-sized IN lists on a hot polled path). The
# authorization decision for a job is stable once made, so the create-time gate
# records WHICH resources it resolved and authorized, and the serve path
# authorizes those rows instead of walking items -> snapshots -> resources
# again.
#
# The stamp is an optimization and never a shrink:
#   * it is written server-side from the rows the create-time gate authorized,
#     and its key is not in SPEC_KEYS, so a caller cannot write it;
#   * it is bound to the spec's scope ids by `scope_sha` — if those ids are not
#     the ones that were authorized, the stamp is ignored and the full walk
#     runs (so a smuggled id can still only ADD a target);
#   * `Job.resource_id` (a server-derived column) is unioned in at check time;
#   * anything unexpected — missing/garbled stamp, an id that no longer resolves
#     to a Resource row — falls back to the full walk.
AUTHORIZED_SCOPE_KEY = "authorized_scope"
_AUTHORIZED_SCOPE_VERSION = 1


def _scope_sha(spec: Optional[dict], resource_id: Any) -> str:
    """Fingerprint of the ids `ensure_job_access` would otherwise re-resolve."""
    parts = [
        ",".join(sorted(str(x).lower() for x in spec_scope_ids(spec, "snapshot_ids", "snapshotIds"))),
        ",".join(sorted(str(x).lower() for x in spec_scope_ids(spec, "item_ids", "itemIds"))),
        str(resource_id or "").lower(),
    ]
    return hashlib.sha256("\x1f".join(parts).encode()).hexdigest()


def authorized_scope_stamp(
    spec: Dict, *, resource_id: Any, resource_ids: Iterable
) -> Dict[str, Any]:
    """Build the stamp for a job whose scope has just been authorized."""
    return {
        "v": _AUTHORIZED_SCOPE_VERSION,
        "resource_ids": sorted({str(r).lower() for r in resource_ids if r}),
        "scope_sha": _scope_sha(spec, resource_id),
    }


def _stamped_scope(spec: Optional[dict], job: Job) -> Optional[Set[uuid.UUID]]:
    """The stamped resource set, or None when the full walk must run."""
    stamp = (spec or {}).get(AUTHORIZED_SCOPE_KEY)
    if not isinstance(stamp, dict) or stamp.get("v") != _AUTHORIZED_SCOPE_VERSION:
        return None
    if stamp.get("scope_sha") != _scope_sha(spec, job.resource_id):
        log.info("authorized_scope_stamp_stale job_id=%s", job.id)
        return None
    ids: Set[uuid.UUID] = set()
    for raw in stamp.get("resource_ids") or []:
        try:
            ids.add(raw if isinstance(raw, uuid.UUID) else uuid.UUID(str(raw)))
        except (ValueError, TypeError, AttributeError):
            return None
    if not ids:
        return None
    if job.resource_id:
        ids.add(job.resource_id)
    return ids


async def _targets_from_stamp(sess: Any, spec: Optional[dict], job: Job) -> Optional[Dict[uuid.UUID, Resource]]:
    wanted = _stamped_scope(spec, job)
    if not wanted:
        return None
    rows = (await sess.execute(select(Resource).where(Resource.id.in_(wanted)))).scalars().all()
    targets = {r.id: r for r in rows}
    if len(targets) != len(wanted):
        # A resource the create-time decision covered is no longer readable.
        # Do not authorize a narrower set — redo the derivation and let the
        # normal unresolvable-scope rules apply.
        return None
    return targets


async def ensure_resource_access(sess: Any, user: dict, resource_id: str) -> Resource:
    """Gate for resource-scoped export routes. Fails closed on anything we
    cannot prove."""
    rid = _uuid_or_400(resource_id, "RESOURCE_ID")
    targets = await authorize_export_scope(sess, user, resource_ids=[rid])
    return targets[rid]


async def ensure_job_access(sess: Any, user: dict, job: Job) -> None:
    """Gate for job-scoped export routes: the chat-export status snapshot, the
    artifact download/status endpoints and the restore-status route all hand
    back (or stream) the export itself, so a weak check here is the
    exfiltration path.

    Nothing here reads an authorization decision out of `job.spec`. The rules
    are applied to `Resource` rows read from the database — the same decision
    `authorize_export_scope` makes at create time. `spec.kind` and
    `spec.userId` are ignored entirely: they were caller-writable.

    WHICH resources are in scope comes from the create-time stamp when it is
    intact (see AUTHORIZED_SCOPE_KEY — this route is polled, and re-walking
    snapshot_items per tick was the load problem), and from re-resolving the
    spec's ids otherwise. Either way the rules are re-evaluated per request
    against live rows, so a revoked entitlement takes effect immediately.
    """
    entitled = await entitled_tenants(sess, user)
    if await _ensure_tenant(
        sess, user, job.tenant_id, what="job", ident=job.id, entitled=entitled
    ):
        return  # SUPER_ADMIN break-glass, logged
    spec = job.spec or {}
    targets = await _targets_from_stamp(sess, spec, job)
    if targets is None:
        targets = await resolve_export_targets(
            sess, user,
            # Job.resource_id is a server-derived column (both triggers walk
            # snapshot -> resource to set it); the spec ids are re-resolved.
            resource_ids=[job.resource_id] if job.resource_id else (),
            snapshot_ids=spec_scope_ids(spec, "snapshot_ids", "snapshotIds"),
            item_ids=spec_scope_ids(spec, "item_ids", "itemIds"),
            strict=False,
        )
    if not targets:
        # Retention pruned the rows this artifact was built from, or the job
        # predates resource stamping. Tenant entitlement is established above,
        # but an ordinary user has no way left to prove ownership — so the
        # artifact is operator-only rather than readable by any tenant member.
        if is_export_operator(user):
            return
        raise auth_denied("job_scope_unresolvable", user, job_id=job.id)
    await _authorize_targets(sess, user, targets, entitled=entitled)
