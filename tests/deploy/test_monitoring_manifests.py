"""The TM Vault Prometheus manifests must match the code and the other manifests.

deploy/taylor-morrison/k8s/monitoring/ holds PodMonitors, a NetworkPolicy, a
PrometheusRule and a blackbox Probe. None of them is exercised by any test or
script until a human applies them, and every one of them encodes a fact that
lives somewhere else:

  * the exporter PORTS come from shared/core_metrics.py (CORE_METRICS_PORT),
    shared/sla_metrics.py (SLA_METRICS_PORT), shared/pst_metrics.py
    (PST_METRICS_PORT) and the literal in workers/chat-export-worker/main.py;
  * the SELECTORS name `app` labels of Deployments in 20-rabbitmq.yaml,
    40-services.yaml and 50-workers.yaml;
  * no TM Vault container declares its metrics port, so the PodMonitors must
    rewrite __address__ instead of using port/portNumber (which would select
    zero targets);
  * itops is default-deny ingress and the cluster's catch-all scrape policy
    only opens 9090/9100/8080/8443, so the NetworkPolicy must open EVERY port
    the PodMonitors scrape, or every target is silently down;
  * the alert expressions reference metric names defined in shared/*_metrics.py,
    SnapshotStatus values from shared/models.py, `job` values that only exist
    because the PodMonitors set jobLabel: app, and container names from the
    Deployment manifests;
  * Alertmanager routes only severity=warning|critical, and every runbook_url
    points at a heading in deploy/taylor-morrison/RUNBOOK.md.

Each of those drifts silently in production (a scrape that returns nothing is
not an error), so they are pinned here.
"""
from __future__ import annotations

import re
from pathlib import Path

import yaml

_ROOT = Path(__file__).resolve().parents[2]
_DEPLOY = _ROOT / "deploy" / "taylor-morrison"
_K8S = _DEPLOY / "k8s"
_MON = _K8S / "monitoring"
_RUNBOOK = _DEPLOY / "RUNBOOK.md"
_RUNBOOK_URL = (
    "https://github.com/taylormorrison/tm_vault_backend/blob/main/"
    "deploy/taylor-morrison/RUNBOOK.md"
)

_STACK_MANIFESTS = (
    "20-rabbitmq.yaml",
    "21-redis.yaml",
    "40-services.yaml",
    "50-workers.yaml",
    "60-frontend.yaml",
)
_PROMETHEUS_SELECTOR = {"release": "kube-prometheus-stack"}
_ROUTED_SEVERITIES = {"warning", "critical"}
# Modules defined in the cluster's blackbox-exporter ConfigMap (monitoring ns, 2026-10-14).
_BLACKBOX_MODULES = {"http_2xx", "http_2xx_tls", "http_ingress_vip", "tcp_connect"}
# Metric families that come from kube-prometheus-stack itself, not from this app.
_CLUSTER_METRIC_PREFIXES = ("kube_", "container_", "probe_", "rabbitmq_")
_RABBITMQ_PROMETHEUS_PORT = 15692
# tmvault containers that have a memory limit but are deliberately not in the
# memory alerts (generic container names that other itops apps also use).
_MEMORY_RULE_EXEMPT = {"frontend", "rabbitmq", "redis"}


# --------------------------------------------------------------------------- helpers
def _docs(path: Path) -> list[dict]:
    return [d for d in yaml.safe_load_all(path.read_text()) if d]


def _monitoring_docs() -> list[tuple[str, dict]]:
    out = []
    for p in sorted(_MON.glob("*.yaml")):
        out.extend((p.name, d) for d in _docs(p))
    return out


def _of_kind(kind: str) -> list[dict]:
    return [d for _, d in _monitoring_docs() if d.get("kind") == kind]


def _stack_deployments() -> dict[str, dict]:
    """{app label: Deployment doc} for every Deployment labelled stack=tmvault."""
    out = {}
    for name in _STACK_MANIFESTS:
        for d in _docs(_K8S / name):
            if d.get("kind") != "Deployment":
                continue
            labels = d["spec"]["template"]["metadata"].get("labels") or {}
            if labels.get("stack") == "tmvault":
                out[labels["app"]] = d
    return out


def _containers(dep: dict) -> list[dict]:
    return dep["spec"]["template"]["spec"]["containers"]


def _selected_apps(pm: dict) -> set[str]:
    sel = pm["spec"]["selector"]
    apps = set()
    if "app" in (sel.get("matchLabels") or {}):
        apps.add(sel["matchLabels"]["app"])
    for expr in sel.get("matchExpressions") or []:
        if expr["key"] == "app":
            assert expr["operator"] == "In", pm["metadata"]["name"]
            apps.update(expr["values"])
    return apps


def _endpoint_port(ep: dict, owner: str) -> int:
    """The port an endpoint scrapes, from its __address__ rewrite."""
    rewrites = [
        r for r in ep.get("relabelings") or [] if r.get("targetLabel") == "__address__"
    ]
    assert len(rewrites) == 1, f"{owner}: endpoint must rewrite __address__ exactly once"
    m = re.fullmatch(r"\$\{1\}:(\d+)", rewrites[0]["replacement"])
    assert m, f"{owner}: __address__ replacement must be ${{1}}:<port>"
    assert rewrites[0]["sourceLabels"] == ["__meta_kubernetes_pod_ip"], owner
    return int(m.group(1))


def _code_port(rel: str, env_name: str) -> int:
    src = (_ROOT / rel).read_text()
    m = re.search(r'os\.environ\.get\("%s",\s*"(\d+)"\)' % re.escape(env_name), src)
    assert m, f"{rel} no longer resolves {env_name} with a literal default"
    return int(m.group(1))


def _expected_ports() -> dict[str, int]:
    core = _code_port("shared/core_metrics.py", "CORE_METRICS_PORT")
    sla = _code_port("shared/sla_metrics.py", "SLA_METRICS_PORT")
    pst = _code_port("shared/pst_metrics.py", "PST_METRICS_PORT")
    chat_src = (_ROOT / "workers" / "chat-export-worker" / "main.py").read_text()
    m = re.search(r"start_http_server\((\d+)\)", chat_src)
    assert m, "chat-export-worker no longer starts a metrics server with a literal port"
    chat = int(m.group(1))
    core_apps = (
        "backup-worker", "backup-worker-heavy", "discovery-worker", "azure-workload-worker",
        "auth-service", "tenant-service", "resource-service", "job-service",
        "snapshot-service", "dashboard-service", "alert-service", "delta-token",
        "audit-service",
    )
    out = {app: core for app in core_apps}
    out["backup-scheduler"] = sla
    out["restore-worker"] = pst
    out["chat-export-worker"] = chat
    out["tmvault-rabbitmq"] = _RABBITMQ_PROMETHEUS_PORT
    return out


def _defined_app_metrics() -> set[str]:
    names = set()
    for rel in ("shared/core_metrics.py", "shared/sla_metrics.py", "shared/pst_metrics.py"):
        src = (_ROOT / rel).read_text()
        names.update(re.findall(r'(?:Counter|Gauge|Histogram)\(\s*"([a-z_]+)"', src))
    return names


def _rules() -> list[dict]:
    out = []
    for pr in _of_kind("PrometheusRule"):
        for group in pr["spec"]["groups"]:
            out.extend(r for r in group["rules"] if "alert" in r)
    return out


def _runbook_anchors() -> set[str]:
    anchors = set()
    for line in _RUNBOOK.read_text().splitlines():
        if not line.startswith("#"):
            continue
        text = line.lstrip("#").strip().lower()
        text = re.sub(r"[^\w\- ]", "", text)
        anchors.add(text.replace(" ", "-"))
    return anchors


# --------------------------------------------------------------------------- tests
def test_monitoring_folder_has_the_four_kinds():
    kinds = {d["kind"] for _, d in _monitoring_docs()}
    assert kinds == {"PodMonitor", "NetworkPolicy", "PrometheusRule", "Probe"}


def test_prometheus_selects_every_monitoring_object():
    """Prometheus only picks up objects labelled release=kube-prometheus-stack."""
    for _, d in _monitoring_docs():
        meta = d["metadata"]
        assert meta["namespace"] == "itops", meta["name"]
        if d["kind"] == "NetworkPolicy":
            continue
        labels = meta.get("labels") or {}
        for k, v in _PROMETHEUS_SELECTOR.items():
            assert labels.get(k) == v, f"{d['kind']}/{meta['name']} missing {k}={v}"


def test_podmonitors_scrape_the_port_each_role_opens_in_code():
    expected = _expected_ports()
    seen: dict[str, str] = {}
    for pm in _of_kind("PodMonitor"):
        name = pm["metadata"]["name"]
        apps = _selected_apps(pm)
        assert apps, f"{name} selects no app"
        for ep in pm["spec"]["podMetricsEndpoints"]:
            port = _endpoint_port(ep, name)
            for app in apps:
                assert app in expected, f"{name} scrapes {app}, which has no known exporter"
                assert port == expected[app], (
                    f"{name} scrapes {app} on {port} but the code opens {expected[app]}"
                )
        for app in apps:
            assert app not in seen, f"{app} selected by both {seen[app]} and {name}"
            seen[app] = name
    assert set(seen) == set(expected), (
        f"roles with an exporter but no PodMonitor: {set(expected) - set(seen)}"
    )


def test_podmonitors_select_only_stack_deployments_and_set_job_label():
    deployments = _stack_deployments()
    for pm in _of_kind("PodMonitor"):
        name = pm["metadata"]["name"]
        spec = pm["spec"]
        assert spec["namespaceSelector"] == {"matchNames": ["itops"]}, name
        assert spec["selector"].get("matchLabels", {}).get("stack") == "tmvault", name
        assert spec["jobLabel"] == "app", f"{name}: rules key on job == app label"
        for app in _selected_apps(pm):
            assert app in deployments, f"{name} selects app={app}, no such tmvault Deployment"


def test_podmonitors_rewrite_address_because_no_container_declares_a_metrics_port():
    """port:/portNumber: are keep-relabels on DECLARED container ports; none exist."""
    expected = _expected_ports()
    deployments = _stack_deployments()
    for app, port in expected.items():
        declared = {
            p["containerPort"]
            for c in _containers(deployments[app])
            for p in c.get("ports") or []
        }
        assert port not in declared, (
            f"{app} now declares containerPort {port}; the PodMonitor could use portNumber"
        )
    for pm in _of_kind("PodMonitor"):
        for ep in pm["spec"]["podMetricsEndpoints"]:
            for forbidden in ("port", "portNumber", "targetPort"):
                assert forbidden not in ep, (
                    f"{pm['metadata']['name']}: {forbidden} selects zero targets here"
                )
            assert ep.get("path") in ("/metrics", "/metrics/detailed")
            assert ep.get("scrapeTimeout") and ep.get("interval")


def test_rabbitmq_detailed_endpoint_requests_the_queue_families_the_rules_use():
    (pm,) = [p for p in _of_kind("PodMonitor") if _selected_apps(p) == {"tmvault-rabbitmq"}]
    detailed = [e for e in pm["spec"]["podMetricsEndpoints"] if e["path"] == "/metrics/detailed"]
    assert len(detailed) == 1
    assert set(detailed[0]["params"]["family"]) >= {"queue_coarse_metrics", "queue_metrics"}
    # The pod declares 5672 + 15672: without the keep, each endpoint gets two targets.
    for ep in pm["spec"]["podMetricsEndpoints"]:
        keeps = [r for r in ep["relabelings"] if r.get("action") == "keep"]
        assert len(keeps) == 1 and keeps[0]["regex"] == "15672", pm["metadata"]["name"]


def test_netpol_opens_every_scraped_port_from_the_monitoring_namespace():
    (np,) = _of_kind("NetworkPolicy")
    spec = np["spec"]
    assert spec["podSelector"] == {"matchLabels": {"stack": "tmvault"}}
    assert spec["policyTypes"] == ["Ingress"]
    (rule,) = spec["ingress"]
    assert rule["from"] == [
        {"namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "monitoring"}}}
    ]
    opened = {(p["protocol"], p["port"]) for p in rule["ports"]}
    scraped = {
        ("TCP", _endpoint_port(ep, pm["metadata"]["name"]))
        for pm in _of_kind("PodMonitor")
        for ep in pm["spec"]["podMetricsEndpoints"]
    }
    assert scraped <= opened, f"scraped but not opened: {scraped - opened}"
    assert opened <= scraped, f"opened but never scraped: {opened - scraped}"


def test_existing_stack_netpol_does_not_already_admit_prometheus():
    """06-netpol-ingress-stack.yaml admits only other tmvault pods — the new policy is needed."""
    (np,) = [d for d in _docs(_K8S / "06-netpol-ingress-stack.yaml") if d["kind"] == "NetworkPolicy"]
    froms = [f for rule in np["spec"]["ingress"] for f in rule["from"]]
    assert all("namespaceSelector" not in f for f in froms)


def test_rules_reference_only_metrics_the_app_defines_or_the_cluster_ships():
    defined = _defined_app_metrics()
    assert "tmvault_snapshots_total" in defined and "sla_audit_publish_failed_total" in defined
    for rule in _rules():
        names = set(re.findall(r"\b([a-z][a-z0-9_]*)\s*\{", rule["expr"]))
        names |= set(re.findall(r"\b(up)\b", rule["expr"]))
        assert names, rule["alert"]
        for n in names:
            if n == "up" or n.startswith(_CLUSTER_METRIC_PREFIXES):
                continue
            base = re.sub(r"_(bucket|count|sum|created)$", "", n)
            assert base in defined, f"{rule['alert']} uses undefined metric {n}"


def test_snapshot_status_matchers_are_snapshotstatus_members():
    models = (_ROOT / "shared" / "models.py").read_text()
    enum_body = models.split("class SnapshotStatus(str, enum.Enum):", 1)[1].split("\n\n", 1)[0]
    statuses = set(re.findall(r'=\s*"([A-Z_]+)"', enum_body))
    assert {"COMPLETED", "FAILED", "PARTIAL"} <= statuses
    checked = 0
    for rule in _rules():
        for sel in re.findall(r"tmvault_snapshots_total\{([^}]*)\}", rule["expr"]):
            for value in re.findall(r'status\s*=~?\s*"([^"]+)"', sel):
                for status in value.split("|"):
                    assert status in statuses, f"{rule['alert']}: status {status!r}"
                    checked += 1
    assert checked, "no rule matches on tmvault_snapshots_total{status}"


def test_up_job_matchers_match_podmonitor_app_labels_or_probe_job_name():
    jobs = set()
    for pm in _of_kind("PodMonitor"):
        jobs |= _selected_apps(pm)
    for probe in _of_kind("Probe"):
        jobs.add(probe["spec"]["jobName"])
    checked = 0
    for rule in _rules():
        for value in re.findall(r'\bjob\s*=~?\s*"([^"]+)"', rule["expr"]):
            for job in value.split("|"):
                assert job in jobs, f"{rule['alert']}: job {job!r} is never scraped"
                checked += 1
    assert checked >= 5


def test_memory_rules_name_real_tmvault_containers_with_limits():
    deployments = _stack_deployments()
    limited = {
        c["name"]
        for dep in deployments.values()
        for c in _containers(dep)
        if ((c.get("resources") or {}).get("limits") or {}).get("memory")
    }
    assert "backup-worker" in limited and "backup-scheduler" in limited
    memory_rules = [r for r in _rules() if "container_memory_working_set_bytes" in r["expr"]
                    or "kube_pod_container_status_restarts_total" in r["expr"]]
    assert len(memory_rules) == 3
    for rule in memory_rules:
        regexes = set(re.findall(r'container\s*=~\s*"([^"]+)"', rule["expr"]))
        assert len(regexes) == 1, f"{rule['alert']} uses inconsistent container regexes"
        named = set(regexes.pop().split("|"))
        assert named <= limited, f"{rule['alert']} names unknown containers {named - limited}"
        assert limited - _MEMORY_RULE_EXEMPT <= named, (
            f"{rule['alert']} misses limited containers {limited - _MEMORY_RULE_EXEMPT - named}"
        )


def test_every_alert_is_routable_and_has_a_runbook_heading():
    anchors = _runbook_anchors()
    assert "prometheus-alerts" in anchors
    names = set()
    for rule in _rules():
        alert = rule["alert"]
        assert alert.startswith("TMVault"), alert
        assert alert not in names, f"duplicate alert {alert}"
        names.add(alert)
        labels = rule["labels"]
        assert labels["severity"] in _ROUTED_SEVERITIES, f"{alert}: {labels['severity']} is unrouted"
        assert labels.get("service") == "tmvault" and labels.get("team"), alert
        ann = rule["annotations"]
        for key in ("summary", "description", "runbook_url"):
            assert ann.get(key), f"{alert} lacks annotation {key}"
        base, _, anchor = ann["runbook_url"].partition("#")
        assert base == _RUNBOOK_URL, f"{alert}: runbook_url must point at RUNBOOK.md"
        assert anchor in anchors, f"{alert}: RUNBOOK.md has no heading for #{anchor}"
    assert len(names) >= 15


def test_probe_uses_an_existing_blackbox_module_against_the_ingress_host():
    (probe,) = _of_kind("Probe")
    spec = probe["spec"]
    assert spec["module"] in _BLACKBOX_MODULES
    # The host serves the internal wildcard cert: only the skip-verify module can pass.
    assert spec["module"] == "http_2xx_tls"
    assert spec["prober"]["url"].endswith(".monitoring.svc:9115")
    (ingress,) = [d for d in _docs(_K8S / "60-frontend.yaml") if d["kind"] == "Ingress"]
    hosts = {r["host"] for r in ingress["spec"]["rules"]}
    targets = spec["targets"]["staticConfig"]["static"]
    assert targets == [f"https://{h}/" for h in sorted(hosts)]
    assert spec["jobName"] == "tmvault-ingress"
