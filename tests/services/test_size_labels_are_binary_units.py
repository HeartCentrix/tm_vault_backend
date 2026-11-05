"""Sizes divided by 1024 are labelled MiB/GiB, like the activity feed (Railway 2026-11-05: the
dashboard showed "146.0 GB" for 146.0 GiB while the activity said "155.5 GiB backed up")."""
import ast
import pathlib

_ROOT = pathlib.Path(__file__).resolve().parents[2]


def _format_bytes(path):
    tree = ast.parse((_ROOT / path).read_text())
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "format_bytes")
    ns: dict = {}
    exec(compile(ast.Module(body=[node], type_ignores=[]), path, "exec"), ns)
    return ns["format_bytes"]


def test_dashboard_and_resource_sizes_use_binary_unit_labels():
    for path in ("services/dashboard-service/main.py", "services/resource-service/main.py"):
        fmt = _format_bytes(path)
        assert fmt(158432679512) == "147.6 GiB"
        assert fmt(83376820) == "79.5 MiB"
