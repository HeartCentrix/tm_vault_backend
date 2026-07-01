"""Retention safety kill-switch (P0).

Root-cause context: a GFS retention pass deleted the base FULL snapshot (136 GB
OneDrive) because the selector kept newest-per-day, and _delete_snapshots then
dropped the rows while the incremental chain is sparse — orphaning the blobs.
Until the durable synthetic-full + ref-counted-GC model is validated, retention
must NOT perform destructive deletes. `_retention_deletes_permitted` is the
global gate: deletes only when RETENTION_DELETE_ENABLED is on AND there is
something to delete; otherwise retention is a dry-run (audits, deletes nothing).

Default is OFF (safe) so a mis-tuned selector or an un-migrated resource can
never silently destroy a base full again.
"""
from __future__ import annotations

import os
import uuid

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")

from shared.retention_cleanup import _retention_deletes_permitted


def test_frozen_by_default_no_delete_even_with_candidates():
    # kill-switch OFF -> never delete, even when the selector marked snaps.
    assert _retention_deletes_permitted(False, {uuid.uuid4(), uuid.uuid4()}) is False


def test_deletes_permitted_only_when_enabled():
    assert _retention_deletes_permitted(True, {uuid.uuid4()}) is True


def test_noop_when_nothing_to_delete():
    # enabled but empty candidate set -> nothing to do.
    assert _retention_deletes_permitted(True, set()) is False
    assert _retention_deletes_permitted(False, set()) is False
