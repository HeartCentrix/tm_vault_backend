"""#34: services run `uvicorn --workers 2`; both workers call
core_metrics.init() and race for :9103. With PROMETHEUS_MULTIPROC_DIR set,
the worker that wins the port must serve the samples of every worker, and
the loser must keep counting instead of crashing."""
from __future__ import annotations

import ast
import os
import pathlib
import re
import socket
import subprocess
import sys
import urllib.request

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]

_SCRIPT = """
import sys
from shared import core_metrics
print("bound" if core_metrics.init() else "lost", flush=True)
core_metrics.inc_job("USER_MAIL", "completed")
if sys.argv[1] == "serve":
    sys.stdin.readline()
"""


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_port_winner_serves_every_workers_samples(tmp_path):
    pytest.importorskip("prometheus_client")
    port = _free_port()
    env = {**os.environ, "PYTHONPATH": str(ROOT),
           "PROMETHEUS_MULTIPROC_DIR": str(tmp_path), "CORE_METRICS_PORT": str(port)}
    server = subprocess.Popen([sys.executable, "-c", _SCRIPT, "serve"], env=env,
                              stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    try:
        assert server.stdout.readline().strip() == "bound"
        loser = subprocess.run([sys.executable, "-c", _SCRIPT, "once"], env=env,
                               capture_output=True, text=True, timeout=60)
        assert loser.returncode == 0, loser.stderr
        assert loser.stdout.strip() == "lost"
        body = urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=10).read().decode()
        assert re.search(r'tmvault_jobs_total\{status="completed",type="USER_MAIL"\} 2\.0', body), body
    finally:
        server.kill()
        server.wait()


def test_multi_worker_dockerfiles_enable_multiprocess_metrics():
    offenders = []
    for dockerfile in ROOT.glob("**/Dockerfile"):
        if ".worktrees" in dockerfile.parts:
            continue
        text = dockerfile.read_text()
        m = re.search(r"--workers\s+(\d+)", text)
        main_py = dockerfile.parent / "main.py"
        uses_metrics = main_py.exists() and "core_metrics.init()" in main_py.read_text()
        if m and int(m.group(1)) > 1 and uses_metrics and "PROMETHEUS_MULTIPROC_DIR" not in text:
            offenders.append(str(dockerfile.relative_to(ROOT)))
    assert offenders == []


def test_prometheus_client_in_base_requirements():
    assert re.search(r"^prometheus[-_]client\b", (ROOT / "requirements.base.txt").read_text(), re.M)


def test_chat_export_worker_survives_metrics_port_conflict():
    tree = ast.parse((ROOT / "workers/chat-export-worker/main.py").read_text())
    guarded = [
        node for node in ast.walk(tree) if isinstance(node, ast.Try)
        and any(isinstance(h.type, ast.Name) and h.type.id == "OSError" for h in node.handlers)
        and "start_http_server" in ast.unparse(node.body[0])
    ]
    assert guarded, "start_http_server(9102) must be wrapped in try/except OSError"
