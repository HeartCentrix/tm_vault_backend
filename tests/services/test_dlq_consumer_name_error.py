"""backup-scheduler DLQ consumer: the NameError that ate every poison message.

Bug (prod, discovered 2026-08-10): `consume_backup_dlq` in
services/backup-scheduler/main.py called `json.loads(msg.body.decode())`,
but `json` was never imported at module scope. Every dead-lettered backup
message therefore raised::

    NameError: name 'json' is not defined

which landed in the function's blanket `except Exception` — whose handler
acks the message anyway ("re-rejecting would just re-queue to DLQ
infinitely"). Net effect: every poison message was destroyed without an
Alert, without its body being recorded, and the `alerts` table had **zero
rows in its entire history**. 9 messages were destroyed in the 72h window
we could still observe.

The bug is invisible to import-time checks and to any test that does not
execute that specific line, so the guards here are static:

  1. every name used as `<mod>.<attr>` in a service/worker entrypoint that
     comes from the stdlib set we care about must actually be imported;
  2. the DLQ consumer must log the raw body BEFORE the first `await` that
     can fail, since every path ends in ack();
  3. the consumer must not read the `workload` key, which no producer has
     ever emitted.
"""
from __future__ import annotations

import ast
import pathlib

import pytest

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
_SCHED = _REPO_ROOT / "services" / "backup-scheduler" / "main.py"

# Modules whose absence produces a silent NameError inside a broad
# `except Exception` rather than a startup crash.
_WATCHED = {"json", "uuid", "os", "asyncio", "re", "base64", "hashlib", "time"}


def _module_scope_imports(tree: ast.Module) -> set[str]:
    names: set[str] = set()
    for node in tree.body:  # module scope only
        if isinstance(node, ast.Import):
            for a in node.names:
                names.add((a.asname or a.name).split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            for a in node.names:
                names.add(a.asname or a.name)
    return names


def _function_local_imports(tree: ast.Module) -> set[str]:
    """Imports inside any function body — these also satisfy a reference."""
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)) and node not in tree.body:
            for a in node.names:
                nm = a.asname or a.name
                names.add(nm.split(".")[0])
    return names


def _attribute_roots(tree: ast.Module) -> set[str]:
    """Names used as `<name>.<attr>`."""
    roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
            roots.add(node.value.id)
    return roots


_ENTRYPOINTS = sorted(
    [p for p in (_REPO_ROOT / "services").glob("*/main.py")]
    + [p for p in (_REPO_ROOT / "workers").glob("*/main.py")]
)


def test_entrypoints_exist():
    assert _ENTRYPOINTS, "no service/worker entrypoints found — path wrong?"


@pytest.mark.parametrize("path", _ENTRYPOINTS, ids=lambda p: p.parent.name)
def test_watched_stdlib_modules_are_imported_where_used(path: pathlib.Path):
    """The regression guard: using `json.loads` without importing json is a
    latent NameError that only fires on the rare path that runs it."""
    tree = ast.parse(path.read_text())
    available = _module_scope_imports(tree) | _function_local_imports(tree)
    used = _attribute_roots(tree) & _WATCHED
    missing = sorted(used - available)
    assert not missing, (
        f"{path.relative_to(_REPO_ROOT)} references {missing} as a module "
        f"but never imports it — a NameError waiting for the right code path"
    )


# ─── DLQ-consumer-specific shape guards ──────────────────────────────────

def _consume_backup_dlq_src(*, strip_comments: bool = True) -> str:
    """Source of consume_backup_dlq. Comments are stripped by default: these
    guards assert on CODE, and the code carries comments that legitimately
    quote the very patterns being banned (e.g. explaining why "workload" is
    wrong), which would otherwise match and fail spuriously."""
    tree = ast.parse(_SCHED.read_text())
    lines = _SCHED.read_text().splitlines()
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "consume_backup_dlq":
            body = lines[node.lineno - 1: node.end_lineno]
            if strip_comments:
                body = [l for l in body if not l.lstrip().startswith("#")]
            return "\n".join(body)
    pytest.fail("consume_backup_dlq not found in backup-scheduler/main.py")


def test_dlq_consumer_logs_raw_body_before_parsing():
    """Every path in the consumer ends in ack(), so the raw body must be
    recorded before the first thing that can throw. Without this the
    message is destroyed with no trace anywhere."""
    src = _consume_backup_dlq_src()
    i_raw = src.find("raw = msg.body.decode")
    i_parse = src.find("json.loads(")
    assert i_raw != -1, "consumer no longer captures the raw body"
    assert i_parse != -1, "consumer no longer parses the body"
    assert i_raw < i_parse, (
        "raw body must be captured (and logged) BEFORE json.loads — "
        "otherwise a parse failure acks the message with no record"
    )
    # and it must actually be printed, not just bound
    assert "body={raw[" in src or "body={raw}" in src, (
        "the raw body is captured but never logged"
    )


def test_dlq_consumer_does_not_read_the_nonexistent_workload_key():
    """create_backup_message emits "type", never "workload" — the old read
    made every alert say "(None)"."""
    src = _consume_backup_dlq_src()
    # The banned pattern is the READ. `"workload": workload` as an alert
    # details key is fine — it labels the value, it does not fetch it.
    assert 'body.get("workload")' not in src, (
        'consumer reads body["workload"], which no producer emits; use "type"'
    )
    assert 'body.get("type")' in src, "consumer should read the real key, \"type\""


def test_dlq_consumer_alert_carries_the_raw_body():
    """So a destroyed message is recoverable from the DB, not only logs."""
    src = _consume_backup_dlq_src()
    assert "raw_body" in src, "Alert.details should carry the raw body"


def test_dlq_consumer_tolerates_an_unparseable_resource_id():
    """A malformed id must not abort the Alert and drop into ack-anyway."""
    src = _consume_backup_dlq_src()
    assert "uuid.UUID(resource_id) if resource_id else None" not in src, (
        "unguarded uuid.UUID(resource_id) inside session.add can raise and "
        "route the message to the ack-anyway path"
    )
