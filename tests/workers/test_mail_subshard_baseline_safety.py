"""Execute the real nested mail drain with fake Graph/DB seams.

The census covers dates a receivedDateTime bucket cannot see; replay keeps
changes during parallel body reads. No mirrored drain implementation here.
"""
from __future__ import annotations

import ast
import asyncio
import importlib.util
import pathlib
import sys
import uuid
from types import SimpleNamespace

import pytest

_PATH = pathlib.Path(__file__).resolve().parents[2] / "workers" / "backup-worker" / "main.py"
_spec = importlib.util.spec_from_file_location("mail_baseline_safety_worker", _PATH)
_bw = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = _bw
_spec.loader.exec_module(_bw)
_TREE = ast.parse(_PATH.read_text())


def _message(mid, subject=None):
    return {"id": mid, "subject": subject or mid, "body": {"content": "body"},
            "parentFolderId": "folder"}


class Graph:
    GRAPH_URL = "https://graph.example/v1.0"

    def __init__(self, *, fail_bucket=False, fail_get=False, expired=False):
        self.fail_bucket, self.fail_get, self.expired = fail_bucket, fail_get, expired
        self.calls, self.gets = [], []
        self.bucket_calls = 0

    async def _iter_pages(self, url, params=None, headers=None):
        self.calls.append((url, params, headers))
        if url == "saved" and self.expired:
            raise RuntimeError("410 resyncRequired")
        if url in ("baseline", "saved"):
            yield {"value": [{"id": "historical"}, _message("bucket", "changed bucket"),
                             {"id": "deleted", "deleted": {"state": "deleted"}},
                             {"id": "doomed", "@removed": {}}],
                   "@odata.deltaLink": "final"}
        elif url.endswith("/delta"):
            yield {"value": [{"id": mid} for mid in ("bucket", "draft", "historical", "doomed")],
                   "@odata.deltaLink": "baseline"}
        else:
            self.bucket_calls += 1
            n = self.bucket_calls
            await asyncio.sleep(0)
            if self.fail_bucket and n == 1:
                raise RuntimeError("bucket interrupted")
            yield {"value": [_message("bucket"), _message("doomed")]}

    async def _get(self, url, params=None):
        mid = url.rsplit("/", 1)[-1]
        self.gets.append(mid)
        if self.fail_get and mid == "draft":
            raise RuntimeError("GET failed")
        return _message(mid, "updated " + mid)


async def _run(graph, *, saved=None, fail_persist=False, fail_cleanup=False, fail_replay=False):
    worker = _bw.BackupWorker.__new__(_bw.BackupWorker)
    worker.worker_id = "test"
    checkpoints, writes, deletes, stored = [], [], [], {}

    async def checkpoint(*args):
        checkpoints.append(args)

    worker._upsert_mail_folder_delta = checkpoint

    async def persist(messages, folder, *, replace_existing=False):
        await asyncio.sleep(0)
        if fail_persist or (fail_replay and replace_existing):
            raise RuntimeError("persist failed")
        writes.extend(m["id"] for m in messages)
        for m in messages:
            if replace_existing or m["id"] not in stored:
                stored[m["id"]] = m
        return len(messages), len(messages) * 100

    resource_row = SimpleNamespace(extra_data={})

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def get(self, model, pk):
            return resource_row if model is _bw.Resource else None

        async def commit(self):
            pass

        async def execute(self, statement, params):
            if fail_cleanup:
                raise RuntimeError("tombstone cleanup failed")
            deletes.append((str(statement), params))
            for mid in params["ids"]:
                stored.pop(mid, None)

    env = dict(vars(_bw))
    env.update({
        "self": worker, "graph_client": graph, "user_id": "user", "mail_select": "id,subject,body",
        "mail_existing_tokens": {"folder": saved} if saved else {}, "mail_new_tokens": {},
        "folder_tree": {"folder": "/Inbox"},
        "folder_tree_full": {"folder": {"sizeInBytes": 300}},
        "mail_sem": asyncio.Semaphore(3), "_mail_streaming_enabled": True,
        "_stream_persist_folder_items": persist, "async_session_factory": Session,
        "_mail_persist_db_gate": lambda: asyncio.Semaphore(1),
        "test_stored": stored, "test_deletes": deletes,
        "resource": SimpleNamespace(id=uuid.uuid4()), "snapshot": SimpleNamespace(id=uuid.uuid4()),
        "mail_progress": {"folders_done": 0, "msgs": 0}, "mail_folder_ids": ["folder"],
        "user_label_mail": "user", "_mail_folders_drained_ok": set(),
        "_mail_folders_token_evicted": set(), "mail_tombstoned_ids": set(),
        "settings": SimpleNamespace(MAIL_INTRA_FOLDER_SUBSHARD_ENABLED=True,
            MAIL_INTRA_FOLDER_MIN_BYTES=128, MAIL_INTRA_FOLDER_INCREMENTAL_JUMP_BYTES=128,
            MAIL_INTRA_FOLDER_TARGET_BYTES=200, MAIL_INTRA_FOLDER_MAX_SUBSHARDS=4),
    })
    names = {"_rehydrate_bare_mail", "_drain_one_folder"}
    functions = [n for n in ast.walk(_TREE) if isinstance(n, ast.AsyncFunctionDef) and n.name in names]
    wrapper = ast.parse("def build():\n    _mail_persisted_total = 0\n    _mail_persisted_bytes = 0\n").body[0]
    wrapper.body.extend(functions)
    wrapper.body.append(ast.Return(value=ast.Name(id="_drain_one_folder", ctx=ast.Load())))
    module = ast.fix_missing_locations(ast.Module(body=[wrapper], type_ignores=[]))
    exec(compile(module, str(_PATH), "exec"), env)
    rows = await env["build"]()("folder")
    return rows, env, checkpoints, writes


def test_full_subshards_reconcile_census_and_replay_changes():
    graph = Graph()
    rows, env, checkpoints, writes = asyncio.run(_run(graph))
    assert {r[2] for r in rows} == {"bucket", "draft", "historical"}
    assert next(r for r in rows if r[2] == "historical")[1] == "updated historical"
    assert graph.bucket_calls == 2
    assert {"draft", "historical"}.issubset(graph.gets)
    assert env["mail_tombstoned_ids"] == {"deleted", "doomed"}
    assert env["mail_new_tokens"] == {"folder": "final"}
    assert env["_mail_folders_drained_ok"] == {"folder"}
    assert checkpoints[0][-1] == "final"
    assert writes.count("bucket") == 2, "persist once for the buckets, once for the newer delta"
    assert "deleted" not in writes
    assert "deleted" not in graph.gets and "doomed" not in graph.gets
    assert set(env["test_stored"]) == {"bucket", "draft", "historical"}
    assert env["test_stored"]["bucket"]["subject"] == "changed bucket"
    sql, params = env["test_deletes"][0]
    assert "snapshot_id = :sid" in sql and "EMAIL_ATTACHMENT" in sql
    assert params == {"sid": env["snapshot"].id, "ids": ["deleted", "doomed"]}
    assert graph.calls[0][0].endswith("/delta")
    assert graph.calls[-1][0] == "baseline"
    for url, params, headers in graph.calls:
        if url.endswith("/delta") or url in ("baseline", "saved"):
            assert "$top" not in (params or {})
        assert headers == _bw.mail_delta_request_headers()


@pytest.mark.parametrize("failure", ["bucket", "get", "persist"])
def test_failed_subshard_never_advances_cursor_or_fingerprint(failure):
    graph = Graph(fail_bucket=failure == "bucket", fail_get=failure == "get")
    _, env, checkpoints, _ = asyncio.run(_run(graph, fail_persist=failure == "persist"))
    assert not env["mail_new_tokens"]
    assert not checkpoints
    assert not env["_mail_folders_drained_ok"]
    assert not any(url == "baseline" for url, _, _ in graph.calls)


def test_incremental_large_folder_resumes_saved_delta():
    graph = Graph()
    rows, env, checkpoints, writes = asyncio.run(_run(graph, saved="saved"))
    assert [url for url, _, _ in graph.calls] == ["saved"]
    assert [r[2] for r in rows] == ["historical", "bucket"]
    assert env["mail_tombstoned_ids"] == {"deleted", "doomed"}
    assert checkpoints[0][-1] == "final"


def test_expired_cursor_reboots_once_with_complete_subshards():
    graph = Graph(expired=True)
    rows, env, checkpoints, _ = asyncio.run(_run(graph, saved="saved"))
    assert env["_mail_folders_token_evicted"] == {"folder"}
    assert [url for url, _, _ in graph.calls].count("saved") == 1
    assert {r[2] for r in rows} == {"bucket", "draft", "historical"}
    assert checkpoints[0][-1] == "final"


@pytest.mark.parametrize("failure", ["cleanup", "replay"])
def test_failed_final_replay_or_tombstone_cleanup_keeps_old_cursor(failure):
    _, env, checkpoints, _ = asyncio.run(_run(
        Graph(), fail_cleanup=failure == "cleanup", fail_replay=failure == "replay",
    ))
    assert not checkpoints
    assert not env["mail_new_tokens"]
    assert not env["_mail_folders_drained_ok"]


@pytest.mark.parametrize("message, expected", [
    ({"type": "FULL"}, True),
    ({"forceFullBackup": True}, True),
    ({"type": "FULL", "partitionFilter": {"folderIds": ["folder"]}}, False),
    ({"forceFullBackup": True, "partitionFilter": {"folderIds": ["folder"]}}, False),
    ({"type": "INCREMENTAL"}, False),
])
def test_only_coordinator_clears_full_backup_cursors(message, expected):
    # Evaluate the actual guard around the USER_MAIL clear, not a copied condition.
    guard = next(n for n in ast.walk(_TREE) if isinstance(n, ast.If)
                 and any(isinstance(c, ast.Call) and isinstance(c.func, ast.Attribute)
                         and c.func.attr == "_should_clear_mail_deltas"
                         for c in ast.walk(n.test)))
    expression = ast.Expression(body=guard.test)
    assert bool(eval(compile(expression, str(_PATH), "eval"), {
        "message": message, "self": _bw.BackupWorker.__new__(_bw.BackupWorker),
    })) is expected


def test_replay_replaces_only_current_email_rows_in_same_transaction():
    calls = []

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def execute(self, statement, params):
            calls.append(("delete", self, str(statement), params))

    async def bulk(session, rows):
        calls.append(("insert", session, rows))

    async def bodies(*args):
        pass

    env = dict(vars(_bw))
    sid = uuid.uuid4()
    env.update({"snapshot": SimpleNamespace(id=sid), "tenant": SimpleNamespace(id=uuid.uuid4()),
                "user_id": "user", "async_session_factory": Session,
                "_mail_persist_db_gate": lambda: asyncio.Semaphore(1),
                "_bulk_upsert_snapshot_items": bulk, "_bulk_upsert_mail_message_bodies": bodies})
    fn = next(n for n in ast.walk(_TREE) if isinstance(n, ast.AsyncFunctionDef)
              and n.name == "_stream_persist_folder_items")
    exec(compile(ast.Module(body=[fn], type_ignores=[]), str(_PATH), "exec"), env)
    asyncio.run(env["_stream_persist_folder_items"]([_message("changed")], "/Inbox", replace_existing=True))
    assert [c[0] for c in calls] == ["delete", "insert"]
    assert calls[0][1] is calls[1][1]
    assert "snapshot_id = :sid" in calls[0][2] and "item_type = 'EMAIL'" in calls[0][2]
    assert calls[0][3] == {"sid": sid, "ids": ["changed"]}
