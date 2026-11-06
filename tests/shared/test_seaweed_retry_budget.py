"""SeaweedFS answers every write with InternalError ("No more writable volumes!")
while the master grows new volumes — 61 s on Railway 2026-11-06 05:15:18→05:16:19,
which failed 9 OneDrive files whose 3 retries waited only ~3.5 s in total. The default
retry budget must ride out a gap of that size (botocore standard mode:
wait_i = rand*min(2**(i-1), 20) s, i = 1..max_attempts retries)."""
import importlib
import os


def test_default_retry_budget_rides_out_a_one_minute_seaweed_gap(monkeypatch):
    monkeypatch.delenv("ONPREM_S3_MAX_ATTEMPTS", raising=False)
    import shared.storage.seaweedfs as sw
    sw = importlib.reload(sw)
    retries = sw._SEAWEED_BOTO_CONFIG.retries
    assert retries["mode"] == "standard"
    mean_wait = sum(min(2 ** (i - 1), 20) / 2 for i in range(1, retries["max_attempts"] + 1))
    assert mean_wait >= 80, (retries, mean_wait)


def test_retry_budget_still_overridable(monkeypatch):
    monkeypatch.setenv("ONPREM_S3_MAX_ATTEMPTS", "4")
    import shared.storage.seaweedfs as sw
    assert importlib.reload(sw)._SEAWEED_BOTO_CONFIG.retries["max_attempts"] == 4
    monkeypatch.delenv("ONPREM_S3_MAX_ATTEMPTS")
    importlib.reload(sw)
