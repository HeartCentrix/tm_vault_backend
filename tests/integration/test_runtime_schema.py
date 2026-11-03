"""Runtime boot checks schema without changing it."""
import pytest
from test_chat_catalogue_postgres import store

pytestmark = pytest.mark.integration


async def test_runtime_schema_verification_is_read_only(store, monkeypatch):
    from shared import database
    from sqlalchemy import event
    monkeypatch.setattr(database, 'engine', store.engine)
    monkeypatch.setattr(database.settings, 'DB_SCHEMA', store.schema)
    statements = []
    def capture(conn, cursor, statement, parameters, context, executemany):
        statements.append(statement.lstrip().upper())
    event.listen(store.engine.sync_engine, 'before_cursor_execute', capture)
    try:
        assert await database.verify_runtime_schema()
    finally:
        event.remove(store.engine.sync_engine, 'before_cursor_execute', capture)
    assert statements and all(s.startswith(('SELECT', 'SET LOCAL')) for s in statements)
