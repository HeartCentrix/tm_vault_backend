"""Optional mail dedup writes must not duplicate unbounded payloads in PostgreSQL."""
import ast
import hashlib
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional
from unittest.mock import AsyncMock
import uuid
import pytest


def helper():
    tree = ast.parse((Path(__file__).resolve().parents[2] / 'workers/backup-worker/main.py').read_text())
    names = {'_mail_message_fingerprint', '_parse_graph_iso_dt', '_bulk_upsert_mail_message_bodies'}
    ns = dict(os=os, uuid=uuid, hashlib=hashlib, datetime=datetime, Dict=Dict, List=List, Any=Any,
              Optional=Optional, _BULK_INSERT_CHUNK=2000)
    exec(compile(ast.Module(body=[n for n in tree.body if getattr(n, 'name', None) in names], type_ignores=[]), '<mail-body>', 'exec'), ns)
    return ns['_bulk_upsert_mail_message_bodies']


async def test_optional_duplicate_write_is_off_without_affecting_source(monkeypatch):
    monkeypatch.delenv('MAIL_BODY_CATALOGUE_ENABLED', raising=False)
    session = AsyncMock()
    message = {'id': 'm', 'subject': 'original', 'body': {'content': 'keep this'}}
    assert await helper()(session, uuid.uuid4(), 'user', uuid.uuid4(), [message]) == 0
    session.execute.assert_not_called()
    assert message['body']['content'] == 'keep this'


async def test_enabled_writes_are_bounded_and_skip_only_optional_oversized_copy(monkeypatch):
    monkeypatch.setenv('MAIL_BODY_CATALOGUE_ENABLED', 'true')
    session = AsyncMock()
    messages = [{'subject': str(i), 'body': {'content': 'x' * 20000}} for i in range(100)]
    messages.append({'subject': 'large', 'body': {'content': 'x' * 2_000_000}})
    assert await helper()(session, uuid.uuid4(), 'user', uuid.uuid4(), messages) == 100
    assert session.execute.await_count > 1
    for call in session.execute.await_args_list:
        stmt = call.args[0]
        params = stmt.compile().params
        # Each body appears as a dedicated column AND in raw metadata.
        payload = sum(len(v.encode()) for k,v in params.items() if k.startswith('body_content_m') and isinstance(v, str))
        assert payload < 512 * 1024
    assert messages[-1]['body']['content'] == 'x' * 2_000_000
