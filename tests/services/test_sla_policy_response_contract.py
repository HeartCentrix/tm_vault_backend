from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_resource_service_main(monkeypatch):
    monkeypatch.setenv("ALLOW_DEV_JWT_SECRETS", "true")
    path = Path(__file__).resolve().parents[2] / "services" / "resource-service" / "main.py"
    spec = importlib.util.spec_from_file_location("resource_service_main_contract", path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def test_policy_detail_routes_return_camel_case_fields(monkeypatch):
    module = _load_resource_service_main(monkeypatch)
    policy_routes = {
        (next(iter(route.methods)), route.path): route
        for route in module.app.routes
        if getattr(route, "path", "") == "/api/v1/policies/{policy_id}"
    }

    assert policy_routes[("GET", "/api/v1/policies/{policy_id}")].response_model_by_alias is False
    assert policy_routes[("PUT", "/api/v1/policies/{policy_id}")].response_model_by_alias is False
