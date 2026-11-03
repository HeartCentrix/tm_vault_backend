"""`snapshot_partitions.total_files` never held files.

Verified 2026-08-11: 140 of the 147 partitions enqueued in the preceding
24h read files_uploaded > total_files, because every fanout helper writes
the length of the id list it just bin-packed — chats, mail folders,
SharePoint drives, channels, Entra categories — while files_uploaded
counts snapshot_items. The four SHAREPOINT "IT OPS NOTES" shards each
carry total_files=1 (one document library, extra_data
partition_total_drives=4) against files_uploaded of 1,270 / 1,271 / 1,273
/ 11,142; a MAIL shard carries total_files=1 with
partition_total_folders=37.

Nothing consumes it as a denominator (no UI, no API, no completion /
requeue / rollup predicate), so this is a naming fix, NOT a migration: the
attribute is `shard_unit_count`, mapped to the SAME `total_files` column,
with `total_files` retained as an ORM synonym so a reader outside this
repo's search path cannot become a production AttributeError.

The tests below pin (a) both names resolving to one column, and (b) every
writer in the backup-worker using the honest name — a missed call site
would either write nothing or raise.
"""
from __future__ import annotations

import ast
import os
import pathlib
import sys

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from shared.models import SnapshotPartition  # noqa: E402

_WORKER_SRC = (
    _REPO_ROOT / "workers" / "backup-worker" / "main.py"
).read_text()


# ─── the mapping ─────────────────────────────────────────────────────────


def test_the_db_column_is_unchanged():
    """No migration: a renamed column would break every deployed worker
    mid-rollout, and the reconciler/scheduler SQL that names it."""
    assert "total_files" in SnapshotPartition.__table__.c
    assert SnapshotPartition.__table__.c.total_files.name == "total_files"


def test_the_honest_attribute_writes_that_column():
    row = SnapshotPartition(partition_index=0, shard_unit_count=184)
    assert row.shard_unit_count == 184
    assert str(SnapshotPartition.shard_unit_count == 1).endswith(
        "snapshot_partitions.total_files = :total_files_1"
    )


def test_the_old_name_still_resolves_both_ways():
    """Backwards-compatible alias — a reader we did not find must not hit
    an AttributeError in production."""
    row = SnapshotPartition(partition_index=0, total_files=184)
    assert row.shard_unit_count == 184
    assert row.total_files == 184
    row.shard_unit_count = 7
    assert row.total_files == 7


def test_the_unit_per_partition_type_is_documented():
    """The column lies unless the reader knows what it counts; the doc is
    the only thing that stops the next person comparing it to
    files_uploaded again."""
    doc = (_REPO_ROOT / "shared" / "models.py").read_text()
    start = doc.find("shard_unit_count = Column(")
    assert start != -1
    preamble = doc[max(0, start - 1800):start]
    for partition_type in (
        "ONEDRIVE_FILES", "MAIL_FOLDERS", "CHATS",
        "SHAREPOINT_DRIVES", "GROUPS_CHANNELS", "ENTRA_CATEGORIES",
    ):
        assert partition_type in preamble, partition_type


# ─── every writer / reader in the worker ─────────────────────────────────


def _fanout_functions():
    tree = ast.parse(_WORKER_SRC)
    return [
        n for n in ast.walk(tree)
        if isinstance(n, (ast.AsyncFunctionDef, ast.FunctionDef))
        and n.name.startswith("_fanout_") and n.name.endswith("_partitions")
    ]


def test_all_six_fanout_helpers_write_the_honest_name():
    fns = _fanout_functions()
    names = sorted(f.name for f in fns)
    assert names == [
        "_fanout_chats_partitions",
        "_fanout_entra_partitions",
        "_fanout_groups_partitions",
        "_fanout_mail_partitions",
        "_fanout_onedrive_partitions",
        "_fanout_sharepoint_partitions",
    ], names
    for fn in fns:
        src = ast.get_source_segment(_WORKER_SRC, fn) or ""
        assert "shard_unit_count=" in src, fn.name
        assert "total_files=" not in src, (
            f"{fn.name} still writes the old kwarg — one of the two names "
            f"will silently stop being maintained"
        )


def test_no_orm_reader_of_the_old_attribute_survives_in_the_worker():
    """The synonym keeps the old name working, but inside this repo there
    must be exactly one name so the two cannot drift."""
    assert 'claim.get("total_files")' not in _WORKER_SRC
    assert ".total_files" not in _WORKER_SRC


def test_the_claim_returns_the_honest_key():
    """`_claim_partition` returns dict(row._mapping), so the SQL alias IS
    the dict key every shard handler reads."""
    assert "total_files AS shard_unit_count" in _WORKER_SRC
    assert _WORKER_SRC.count('claim.get("shard_unit_count")') == 6
    # The row-index fallback (drivers without ._mapping) must agree.
    assert '"shard_unit_count": row[8]' in _WORKER_SRC
