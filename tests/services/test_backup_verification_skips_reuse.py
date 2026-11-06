"""The integrity sampler picks a snapshot then one of its own blob rows. An unchanged mailbox's
reuse snapshot owns no rows (its data is the full snapshot it names, sampled on its own), so it
was counted as a missing blob."""
import pathlib
import re


def test_sampler_skips_reuse_snapshots():
    src = (pathlib.Path(__file__).resolve().parents[2] / "services" / "backup-scheduler" / "main.py").read_text()
    start = src.index("snap_stmt = (")
    block = re.sub(r"\s+", " ", src[start:src.index(".limit(BACKUP_VERIFY_SAMPLE_SIZE)", start)])
    assert "Snapshot.reuse_of_snapshot_id.is_(None)" in block
