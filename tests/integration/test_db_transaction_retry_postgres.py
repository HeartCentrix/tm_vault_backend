"""Owned DB-only retries use a new transaction after complete rollback."""
import importlib
import os

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from test_chat_catalogue_transaction_retry import database

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("sqlstate", ["40P01", "40001"])
async def test_owned_retry_rolls_back_and_releases_connection_before_jitter(database, monkeypatch, sqlstate):
    retry_module = importlib.import_module("shared.db_retry")
    sessions, transactions, delays = [], [], []
    async def operation(session):
        sessions.append(session)
        transactions.append(await session.scalar(text("SELECT txid_current()")))
        await session.execute(text("INSERT INTO results VALUES ('logical-write',:attempt)"),
                              {"attempt": len(sessions)})
        if len(sessions) < 3:
            await session.execute(text(f"DO $$ BEGIN RAISE EXCEPTION USING ERRCODE='{sqlstate}'; END $$"))
        return "committed"
    async def no_wait(delay):
        delays.append(delay)
        assert not sessions[-1].in_transaction()
        async with database() as observer:
            assert await observer.scalar(text("SELECT count(*) FROM results")) == 0
    monkeypatch.setattr(retry_module.asyncio, "sleep", no_wait)
    assert await retry_module.run_db_transaction(database, operation) == "committed"
    assert len({id(session) for session in sessions}) == len(set(transactions)) == 3
    assert len(delays) == 2 and all(0.05 <= delay <= 0.5 for delay in delays)
    async with database() as observer:
        assert (await observer.execute(text("SELECT key,attempt FROM results"))).all() == [("logical-write", 3)]


async def test_owned_retry_caps_requested_attempts_and_preserves_last_error(database, monkeypatch):
    retry_module = importlib.import_module("shared.db_retry")
    sessions = []
    async def operation(session):
        sessions.append(session)
        await session.execute(text("INSERT INTO results VALUES ('never-committed',1)"))
        await session.execute(text("DO $$ BEGIN RAISE EXCEPTION USING ERRCODE='40001'; END $$"))
    async def no_wait(delay):
        pass
    monkeypatch.setattr(retry_module.asyncio, "sleep", no_wait)
    with pytest.raises(DBAPIError) as failure:
        await retry_module.run_db_transaction(database, operation, attempts=9)
    assert failure.value.orig.sqlstate == "40001"
    assert len(sessions) == 3
    async with database() as observer:
        assert await observer.scalar(text("SELECT count(*) FROM results")) == 0


@pytest.mark.parametrize("sqlstate", ["25P02", "23503", "57014", "08006"])
async def test_owned_retry_does_not_replay_other_database_errors(database, sqlstate):
    retry_module = importlib.import_module("shared.db_retry")
    sessions = []
    async def operation(session):
        sessions.append(session)
        await session.execute(text("INSERT INTO results VALUES ('never-committed',1)"))
        await session.execute(text(f"DO $$ BEGIN RAISE EXCEPTION USING ERRCODE='{sqlstate}'; END $$"))
    with pytest.raises(DBAPIError) as failure:
        await retry_module.run_db_transaction(database, operation)
    assert failure.value.orig.sqlstate == sqlstate
    assert len(sessions) == 1
    async with database() as observer:
        assert await observer.scalar(text("SELECT count(*) FROM results")) == 0


@pytest.mark.parametrize("sqlstate", ["40P01", "40001"])
async def test_owned_operation_retries_complete_existing_transaction_context(database, monkeypatch, sqlstate):
    retry_module = importlib.import_module("shared.db_retry")
    sessions, delays = [], []
    async def operation():
        async with database.begin() as session:
            sessions.append(session)
            await session.execute(text("INSERT INTO results VALUES ('legacy-owned',:attempt)"),
                                  {"attempt": len(sessions)})
            if len(sessions) == 1:
                await session.execute(text(f"DO $$ BEGIN RAISE EXCEPTION USING ERRCODE='{sqlstate}'; END $$"))
        return "committed"
    async def no_wait(delay):
        delays.append(delay)
        assert not sessions[-1].in_transaction()
        async with database() as observer:
            assert await observer.scalar(text("SELECT count(*) FROM results")) == 0
    monkeypatch.setattr(retry_module.asyncio, "sleep", no_wait)
    assert await retry_module.run_owned_db_operation(operation) == "committed"
    assert len(sessions) == 2 and sessions[0] is not sessions[1]
    assert len(delays) == 1 and 0.05 <= delays[0] <= 0.5
    async with database() as observer:
        assert (await observer.execute(text("SELECT key,attempt FROM results"))).all() == [("legacy-owned", 2)]
