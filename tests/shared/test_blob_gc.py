"""Ref-counted blob GC primitives (P2).

Two structural bugs in the only place blobs get physically deleted today
(backup-scheduler `_sweep_cancelled_snapshots`), which a future retention GC
would inherit:

  1. Container mismatch — the purge derives the container from `resource_type`
     ("backup-onedrive-<t>"), but blobs were WRITTEN under the workload suffix
     (ONEDRIVE -> "files" -> "backup-files-<t>"). Deleting against the wrong
     container silently orphans the blob (leak), and — with a wrong-but-existing
     container — risks nuking an unrelated bucket.
  2. No ref-counting — it deletes every blob_path of the snapshot without
     checking whether a SURVIVING snapshot_item still references the same
     (content-addressed / carried-forward) blob. In the sparse capture model
     blob_paths are shared across snapshots, so this can delete a blob a live
     snapshot still needs -> data loss.

`blobs_safe_to_delete` is the ref-count core: a blob is deletable ONLY when no
surviving snapshot_item references it. `container_candidates` reproduces the
write-side container derivation (workload suffix first, legacy raw type last)
so the purge targets where the bytes actually live.
"""
from __future__ import annotations

import os

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")

from shared.blob_gc import blobs_safe_to_delete, container_candidates


def test_shared_blob_is_not_deleted_while_a_survivor_references_it():
    # snapshot being retired references A and B; a surviving snapshot still
    # references B (carried-forward / content-addressed reuse) -> only A is safe.
    doomed = ["tenant/res/snapA/ts/itemA", "tenant/res/shared/ts/itemB"]
    still_referenced = ["tenant/res/shared/ts/itemB"]
    assert blobs_safe_to_delete(doomed, still_referenced) == {
        "tenant/res/snapA/ts/itemA"
    }


def test_nothing_safe_when_all_blobs_still_referenced():
    doomed = ["p1", "p2"]
    assert blobs_safe_to_delete(doomed, ["p1", "p2", "p3"]) == set()


def test_all_safe_when_none_referenced():
    assert blobs_safe_to_delete(["p1", "p2"], []) == {"p1", "p2"}


def test_blobs_safe_to_delete_is_none_and_empty_safe():
    assert blobs_safe_to_delete(None, None) == set()
    assert blobs_safe_to_delete([], ["p1"]) == set()
    # falsy blob paths are never emitted as deletable
    assert blobs_safe_to_delete(["", None, "p1"], []) == {"p1"}


def test_container_candidates_prefers_workload_bucket_over_legacy_type():
    # ONEDRIVE blobs live in the "files" workload bucket, NOT "backup-onedrive-*".
    cands = container_candidates("ONEDRIVE", "abcd1234-0000-0000-0000-000000000000",
                                 workloads=("files",))
    assert cands[0] == "backup-files-abcd1234"
    # legacy raw-type container is offered LAST as a fallback, never first.
    assert "backup-onedrive-abcd1234" in cands
    assert cands.index("backup-files-abcd1234") < cands.index("backup-onedrive-abcd1234")


def test_container_candidates_dedups_and_normalizes_underscores():
    # USER_MAIL -> ("email","mailbox"); underscores in a workload normalize to '-'.
    cands = container_candidates("USER_MAIL", "abcd1234-ffff",
                                 workloads=("email", "mailbox"))
    assert cands[0] == "backup-email-abcd1234"
    assert "backup-mailbox-abcd1234" in cands
    # no duplicates
    assert len(cands) == len(set(cands))
