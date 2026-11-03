"""A busy scheduler must defer polls instead of queuing fleet-wide row locks."""
from uuid import uuid4

import pytest
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert

from test_chat_catalogue_postgres import store
from shared import copy_work, models, provider_work

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("control,claim", [
    (models.CopyWorkControl, copy_work.claim),
    (models.ProviderWorkControl, provider_work.claim_due_work),
])
async def test_busy_scheduler_defers_without_waiting_for_control_lock(store, control, claim):
    async with store.factory.begin() as session:
        await session.execute(insert(control).values(id='global', enabled=True).on_conflict_do_nothing())
    async with store.factory.begin() as holder:
        await holder.scalar(select(control).where(control.id == 'global').with_for_update())
        async with store.factory.begin() as contender:
            await contender.execute(text("SET LOCAL lock_timeout = '100ms'"))
            assert await claim(contender, owner_id=uuid4()) is None
            # A deferred scheduler leaves its transaction usable.
            assert await contender.scalar(select(1)) == 1
    async with store.factory.begin() as session:
        assert await claim(session, owner_id=uuid4()) is None
