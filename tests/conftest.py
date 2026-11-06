"""Suite-wide defaults.

Production now defaults the provider-work, chat catalogue, copy-work, batch-row and chat
presentation paths ON (TM's k8s config never set them, so it ran the legacy paths).
Most tests here exercise the legacy paths and relied on the old OFF defaults; they keep
that unless a test opts in. tests/shared/test_production_defaults.py locks the ON defaults.
"""
import os
import pytest

LEGACY_PATH_FLAGS = ("PROVIDER_WORK_ENABLED", "CHAT_CATALOGUE_WRITER_ENABLED", "COPY_WORK_ENABLED",
                     "BATCH_ROW_REDESIGN_ENABLED", "CHAT_PRESENTATION_ENABLED")
for _flag in LEGACY_PATH_FLAGS:
    os.environ.setdefault(_flag, "false")


@pytest.fixture(autouse=True)
def isolated_unit_graph_admission(request, monkeypatch):
    """Unit tests must not inherit Redis connections owned by another event loop.

    Real coordinator coverage uses explicitly marked integration tests and their
    owned Redis fixtures. Keep the real local token bucket for ordinary unit calls.
    """
    if request.node.get_closest_marker("integration"):
        return
    from shared import graph_rate_limiter as admission
    limiter = admission.GraphRateLimiter()

    async def local_only():
        pass

    monkeypatch.setattr(limiter, "maybe_init_redis", local_only)
    monkeypatch.setattr(admission, "graph_rate_limiter", limiter)
