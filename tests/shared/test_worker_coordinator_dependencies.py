"""Standalone worker images must install their required Redis coordinator client."""
from pathlib import Path

import pytest
from packaging.requirements import Requirement

ROOT = Path(__file__).resolve().parents[2]
WORKERS = sorted(
    main.parent for main in (ROOT / "workers").glob("*/main.py")
    if "await graph_rate_limiter.maybe_init_redis()" in main.read_text()
)


@pytest.mark.parametrize("worker", WORKERS, ids=lambda path: path.name)
def test_coordinator_worker_installs_redis(worker):
    requirements = {
        Requirement(line.split("#", 1)[0].strip()).name
        for line in (worker / "requirements.txt").read_text().splitlines()
        if line.split("#", 1)[0].strip()
    }
    assert "redis" in requirements, f"{worker.name} starts the Redis admission coordinator"
