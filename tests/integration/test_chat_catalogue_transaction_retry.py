"""Fresh catalogue transaction recovery after actual PostgreSQL aborts."""
from __future__ import annotations

import importlib
import os
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def database():
    url = os.environ.get("TMVAULT_TEST_DATABASE_URL")
    if not url:
        pytest.skip("requires explicitly configured disposable PostgreSQL")
    schema = "chat_retry_test_" + uuid4().hex
    engine = create_async_engine(url, connect_args={"server_settings": {"search_path": schema}, "statement_cache_size": 0})
    async with engine.begin() as conn:
        await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        await conn.execute(text("CREATE TABLE results (key text PRIMARY KEY, attempt integer NOT NULL)"))
    try:
        yield async_sessionmaker(engine, expire_on_commit=False)
    finally:
        async with engine.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        await engine.dispose()


@pytest.mark.parametrize("sqlstate", ["40P01", "40001"])
async def test_retry_rolls_back_failed_writes_and_uses_fresh_session_and_transaction(database, sqlstate):
    retry = importlib.import_module("shared.db_retry").run_catalogue_transaction
    sessions, transactions = [], []
    async def operation(session):
        sessions.append(session)
        transactions.append(await session.scalar(text("SELECT txid_current()")))
        await session.execute(text("INSERT INTO results VALUES ('one-logical-result',:attempt)"), {"attempt": len(sessions)})
        if len(sessions) == 1:
            try:
                await session.execute(text(f"DO $$ BEGIN RAISE EXCEPTION 'synthetic transaction failure' USING ERRCODE='{sqlstate}'; END $$"))
            except DBAPIError as original:
                with pytest.raises(DBAPIError) as aborted:
                    await session.execute(text("SELECT 1"))
                assert aborted.value.orig.sqlstate == "25P02"
                raise original
        return "published-once"
    assert await retry(database, operation) == "published-once"
    assert len(sessions) == 2 and sessions[0] is not sessions[1]
    assert len(set(transactions)) == 2
    async with database() as session:
        assert (await session.execute(text("SELECT key,attempt FROM results"))).all() == [("one-logical-result", 2)]


async def test_non_retryable_database_error_runs_once_and_leaves_no_writes(database):
    retry = importlib.import_module("shared.db_retry").run_catalogue_transaction
    calls = []
    async def operation(session):
        calls.append(session)
        await session.execute(text("INSERT INTO results VALUES ('same-key',1)"))
        await session.execute(text("INSERT INTO results VALUES ('same-key',2)"))
    with pytest.raises(DBAPIError):
        await retry(database, operation)
    assert len(calls) == 1
    async with database() as session:
        assert await session.scalar(text("SELECT count(*) FROM results")) == 0


async def test_retries_are_bounded_and_last_database_failure_is_preserved(database):
    retry = importlib.import_module("shared.db_retry").run_catalogue_transaction
    calls = []
    async def operation(session):
        calls.append(session)
        await session.execute(text("INSERT INTO results VALUES ('never-committed',1)"))
        await session.execute(text("DO $$ BEGIN RAISE EXCEPTION 'still moving' USING ERRCODE='40001'; END $$"))
    with pytest.raises(DBAPIError) as failure:
        await retry(database, operation, attempts=2)
    assert failure.value.orig.sqlstate == "40001" and len(calls) == 2
    assert calls[0] is not calls[1]
    async with database() as session:
        assert await session.scalar(text("SELECT count(*) FROM results")) == 0
