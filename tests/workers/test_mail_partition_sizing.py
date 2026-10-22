"""Unit tests for mail partition sizing without folder ``sizeInBytes``.

Context (2026-08-03): folder ``sizeInBytes`` is a BETA-only Graph
property, so v1.0 folder trees carry ``sizeInBytes: 0`` (see
get_mail_folder_tree). Two consumers had to adapt:

  * ``_bin_pack_mail_folders_by_size`` ran LPT over all-zero weights —
    every iteration ties, ``index(min(...))`` answers shard 0, and the
    ENTIRE mailbox landed in one shard (the serialization partitioning
    exists to avoid). All-zero weights must take the round-robin path,
    and ``totalItemCount`` acts as the weight when byte sizes are gone.
  * ``_should_partition_mail``'s byte threshold went permanently
    false for deep-but-narrow mailboxes; ``MAIL_PARTITION_MIN_ITEMS``
    (totalItemCount sum) is the live proxy.
"""
from __future__ import annotations

import importlib.util
import pathlib
import sys

import pytest


_MAIN_PATH = (
    pathlib.Path(__file__).resolve().parents[2]
    / "workers" / "backup-worker" / "main.py"
)
_spec = importlib.util.spec_from_file_location("bw_main_partition_test", _MAIN_PATH)
_bw = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["bw_main_partition_test"] = _bw
try:
    _spec.loader.exec_module(_bw)  # type: ignore[union-attr]
except Exception as exc:
    pytest.skip(
        f"backup-worker module failed to import: {exc}",
        allow_module_level=True,
    )


_bin_pack = _bw.BackupWorker._bin_pack_mail_folders_by_size
_should = _bw.BackupWorker._should_partition_mail


def _tree(counts):
    return {
        fid: {
            "path": f"/{fid}",
            "totalItemCount": n,
            "unreadItemCount": 0,
            "sizeInBytes": 0,
        }
        for fid, n in counts.items()
    }


def test_all_zero_weights_round_robin_not_one_shard():
    fids = [f"f{i}" for i in range(8)]
    tree = _tree({f: 0 for f in fids})
    shards = _bin_pack(None, fids, tree, 4)
    assert all(len(s) == 2 for s in shards), (
        f"all-zero weights must spread round-robin, got {[len(s) for s in shards]}"
    )


def test_item_counts_are_the_lpt_weight_when_bytes_are_zero():
    tree = _tree({"big": 10_000, "mid": 5_000, "s1": 100, "s2": 100})
    shards = _bin_pack(None, ["big", "mid", "s1", "s2"], tree, 2)
    by_folder = {f: i for i, shard in enumerate(shards) for f in shard}
    assert by_folder["big"] != by_folder["mid"], (
        "the two heavy folders must land on different shards (LPT by "
        "totalItemCount when sizeInBytes is zeroed)"
    )


def test_lpt_weights_share_one_unit_across_enriched_and_nested():
    """Beta enrichment gives TOP-LEVEL folders real byte sizes while
    nested folders only have counts. Raw counts vs raw bytes are
    incomparable — a 50KB enriched folder must NOT outweigh a
    40k-message nested folder (count-estimated ~4GB)."""
    tree = {
        "tiny-toplevel": {"path": "/Tiny", "totalItemCount": 3,
                          "unreadItemCount": 0, "sizeInBytes": 50 * 1024},
        "huge-nested": {"path": "/Archive/2019", "totalItemCount": 40_000,
                        "unreadItemCount": 0, "sizeInBytes": 0},
        "s1": {"path": "/A", "totalItemCount": 10, "unreadItemCount": 0, "sizeInBytes": 0},
        "s2": {"path": "/B", "totalItemCount": 10, "unreadItemCount": 0, "sizeInBytes": 0},
    }
    shards = _bin_pack(None, list(tree), tree, 2)
    by_folder = {f: i for i, shard in enumerate(shards) for f in shard}
    assert by_folder["huge-nested"] != by_folder["s1"] or by_folder["huge-nested"] != by_folder["s2"], (
        "the 40k-message nested folder must be treated as the heavyweight"
    )
    # LPT places the heaviest first, alone on its shard; the enriched-but-tiny
    # top-level folder must NOT be treated as heavier than the nested bulk.
    heaviest_shard = by_folder["huge-nested"]
    assert by_folder["tiny-toplevel"] != heaviest_shard, (
        "mixed units: 50KB real size must weigh less than ~4GB estimated"
    )


def test_missing_tree_still_round_robins():
    fids = ["a", "b", "c"]
    shards = _bin_pack(None, fids, None, 2)
    assert sorted(f for s in shards for f in s) == fids


class _Settings:
    MAIL_PARTITION_ENABLED = True
    MAIL_PARTITION_MIN_FOLDERS = 20
    MAIL_PARTITION_MIN_BYTES = 2 * 1024**3
    MAIL_PARTITION_MIN_ITEMS = 20_000


def test_deep_narrow_mailbox_partitions_via_item_count(monkeypatch):
    monkeypatch.setattr(_bw, "settings", _Settings())
    fids = [f"f{i}" for i in range(4)]  # far below MIN_FOLDERS
    tree = _tree({f: 30_000 for f in fids})  # 120k items, zero bytes
    assert _should(None, fids, tree) is True, (
        "totalItemCount sum >= MAIL_PARTITION_MIN_ITEMS must trigger the "
        "split now that sizeInBytes is always 0"
    )


def test_small_mailbox_still_does_not_partition(monkeypatch):
    monkeypatch.setattr(_bw, "settings", _Settings())
    fids = [f"f{i}" for i in range(4)]
    tree = _tree({f: 100 for f in fids})
    assert _should(None, fids, tree) is False


def test_thresholds_scope_to_the_drain_set_not_the_whole_tree(monkeypatch):
    """folder_ids is the post-fingerprint DRAIN set. A converged 82k-item
    mailbox whose incremental run drains two small folders must NOT pay
    partition fanout on every run — the sums measure this run's work."""
    monkeypatch.setattr(_bw, "settings", _Settings())
    tree = _tree({"inbox": 60_000, "archive1": 22_000, "sent": 400, "drafts": 12})
    assert _should(None, ["sent", "drafts"], tree) is False, (
        "two tiny drain folders must not partition just because the WHOLE "
        "tree holds 82k items"
    )
    assert _should(None, ["inbox"], tree) is True, (
        "a genuinely heavy drain folder must still partition"
    )


def test_degraded_pathonly_tree_is_safe_everywhere():
    """Degraded trees carry path STRINGS (see get_mail_folder_tree).
    Every partition-path consumer must isinstance-guard before .get() —
    _fanout_mail_partitions crashed with AttributeError on the exact
    contingency the degraded mode exists for."""
    degraded = {f"f{i}": f"/Folder {i}" for i in range(25)}

    shards = _bin_pack(None, list(degraded), degraded, 3)
    assert sorted(f for s in shards for f in s) == sorted(degraded), (
        "bin-pack must tolerate path-only entries"
    )

    import inspect
    src = inspect.getsource(_bw.BackupWorker._fanout_mail_partitions)
    assert "isinstance(entry, dict)" in src, (
        "_fanout_mail_partitions byte sums must isinstance-guard tree "
        "entries — degraded trees hold strings and str.get() raises"
    )
