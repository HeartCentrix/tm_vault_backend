"""SeaweedFS 3.71 grows a collection only once EVERY writable volume is past
master.volume_growth.threshold (default 0.9), and then by copy_1 (default 7) volumes.
With 5 GB volumes that left ~3.5 GB of headroom — ~20 s at backup write rates — while
growth took ~60 s, so all uploads hit "No more writable volumes!" (Railway 2026-11-06
05:15:18→05:16:19, 9 OneDrive files failed). The coordinator must start the master with
earlier, larger growth."""
import pathlib
import re
import subprocess

ENTRY = pathlib.Path(__file__).resolve().parents[2] / "deploy/railway/seaweedfs/entrypoint.sh"


def _rendered_master_toml(tmp_path, env=None):
    script = ENTRY.read_text()
    block = re.search(r"(GROW_COPY_1=.*?\nTOML\n)", script, re.S)
    assert block, "entrypoint must render master.toml"
    body = block.group(1).replace("/etc/seaweedfs/master.toml", str(tmp_path / "master.toml"))
    subprocess.run(["sh", "-c", "set -eu\n" + body], check=True, env=env or {})
    return (tmp_path / "master.toml").read_text()


def test_master_grows_volumes_well_before_they_fill(tmp_path):
    toml = _rendered_master_toml(tmp_path)
    assert "[master.volume_growth]" in toml
    copy_1 = int(re.search(r"copy_1\s*=\s*(\d+)", toml).group(1))
    threshold = float(re.search(r"threshold\s*=\s*([\d.]+)", toml).group(1))
    headroom_gb = copy_1 * 5 * (1 - threshold)  # 5 GB volumes on Railway
    assert headroom_gb >= 30, (copy_1, threshold)


def test_growth_knobs_stay_tunable(tmp_path):
    toml = _rendered_master_toml(tmp_path, {"SEAWEED_GROWTH_COPY_1": "9", "SEAWEED_GROWTH_THRESHOLD": "0.7"})
    assert re.search(r"copy_1\s*=\s*9\b", toml) and re.search(r"threshold\s*=\s*0\.7\b", toml)


def test_master_is_started_after_the_config_is_written():
    script = ENTRY.read_text()
    assert script.index("master.toml") < script.index("/usr/bin/weed master")
