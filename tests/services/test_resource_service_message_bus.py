"""SLA assignment enqueues Tier-2 discovery through message_bus.publish, which is a
silent no-op until message_bus.connect() has run. resource-service never connected
the bus (observed 2026-11-04: ten "Tier-2 prep enqueued" prints, zero messages on
discovery.tier2, no child resources), so the service must connect at startup."""
import asyncio
import importlib.util
import os
import pathlib
import sys

os.environ.setdefault("ALLOW_DEV_JWT_SECRETS", "true")
os.environ.setdefault("JWT_SECRET", "unit-test-only-not-a-real-secret")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://fake:fake@localhost/fake")

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
_SVC_DIR = _REPO_ROOT / "services" / "resource-service"
if str(_SVC_DIR) not in sys.path:
    sys.path.insert(0, str(_SVC_DIR))
_spec = importlib.util.spec_from_file_location("rs_main_bus_under_test", _SVC_DIR / "main.py")
_rs = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["rs_main_bus_under_test"] = _rs
assert _spec and _spec.loader
_spec.loader.exec_module(_rs)  # type: ignore[union-attr]


def test_lifespan_connects_and_closes_the_message_bus(monkeypatch):
    calls = []

    async def connect(*args, **kwargs):
        calls.append("connect")

    async def close(*args, **kwargs):
        calls.append("close")

    async def noop(*args, **kwargs):
        pass

    monkeypatch.setattr(_rs.message_bus, "connect", connect)
    monkeypatch.setattr(_rs.message_bus, "disconnect", close)
    monkeypatch.setattr(_rs, "init_db", noop)
    monkeypatch.setattr(_rs, "close_db", noop)
    import shared.core_metrics as core_metrics
    monkeypatch.setattr(core_metrics, "init", lambda *a, **k: None)

    async def run():
        async with _rs.lifespan(_rs.app):
            assert calls == ["connect"]

    asyncio.run(run())
    assert calls == ["connect", "close"]
