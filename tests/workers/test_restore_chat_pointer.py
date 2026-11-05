"""A chat backup whose every chat was unchanged owns no rows; restoring it must restore the backup it reuses."""
import pathlib


def test_catalogue_restore_point_follows_the_reused_backup():
    src = (pathlib.Path(__file__).resolve().parents[2] / "workers" / "restore-worker" / "main.py").read_text()
    assert "catalogue_ids.add(picked.reuse_chain_root_id or picked.id)" in src
