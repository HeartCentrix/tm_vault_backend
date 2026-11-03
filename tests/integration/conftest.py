"""Keep process-lifetime activation latches local to each disposable database."""
import pytest


@pytest.fixture(autouse=True)
def restore_copy_guard_activation():
    # A deployed process uses one database and deliberately never unlatches its
    # guards. Integration tests switch schemas/databases within that process;
    # activation in one schema must not change a later test's protocol.
    from shared import copy_workflow
    before = copy_workflow._COPY_GUARDS_ACTIVE
    yield
    copy_workflow._COPY_GUARDS_ACTIVE = before
