"""INTERNAL_API_KEY must actually reach every pod that touches the audit trail.

audit-service gates POST /api/v1/audit/log on the internal-services shared
secret (services/audit-service/main.py ``require_internal_api_key``). The two
halves of that gate live in different pods:

  * the VALIDATOR (audit-service) reads ``settings.INTERNAL_API_KEY``;
  * every EMITTER reads ``os.getenv("INTERNAL_API_KEY")`` via
    ``shared.audit.internal_audit_headers``.

The k8s manifests plumb secrets two different ways. ``50-workers.yaml`` uses
``envFrom: [{secretRef: tmvault-secrets}]``, so a worker picks up every key in
the Secret automatically. ``40-services.yaml`` enumerates individual
``secretKeyRef`` entries, so a key that is not enumerated NEVER reaches a
service pod no matter what is in the Secret.

That asymmetry is the defect these tests pin:

  * add the key to the Secret only  -> all six workers get it, ZERO services
    do, including audit-service itself. The validator stays UNCONFIGURED and
    fails OPEN while looking closed.
  * add it to audit-service only    -> the validator turns on while
    job-service / snapshot-service / backup-scheduler still have no key, so
    ``internal_audit_headers()`` returns ``{}`` and their raw POSTs 401. httpx
    does not raise on 4xx and those call sites swallow exceptions, so those are
    SILENT audit losses on a compliance product.

So the key has to be enumerated for services, exactly the way JWT_SECRET and
ENCRYPTION_KEY already are, and it has to be in the generator too or the next
``gen-services.sh`` run silently drops it again.
"""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

_K8S = Path(__file__).resolve().parents[2] / "deploy" / "taylor-morrison" / "k8s"
_SERVICES = _K8S / "40-services.yaml"
_WORKERS = _K8S / "50-workers.yaml"
_GEN = _K8S / "gen-services.sh"
_MIGRATE = _K8S / "30-migrate-job.yaml"

_SECRET_NAME = "tmvault-secrets"

# Services that emit audit events, and therefore MUST carry the key, plus
# audit-service which validates it. Derived from
# `grep -rn 'internal_audit_headers\|shared.audit import' services/`:
#   job-service        (raw POSTs + emit_backup_triggered)
#   snapshot-service   (raw POSTs)
#   backup-scheduler   (emit_backup_triggered)
#   audit-service      (validator)
# job-service/audit-service deploy on the archive lineage, snapshot-service and
# backup-scheduler on the perf lineage -- which is why this manifest change has
# to land in BOTH worktrees.
_AUDIT_PODS = (
    "audit-service",
    "job-service",
    "snapshot-service",
    "backup-scheduler",
)


def _deployments(path: Path) -> dict:
    docs = [d for d in yaml.safe_load_all(path.read_text()) if d]
    out = {}
    for d in docs:
        if d.get("kind") != "Deployment":
            continue
        for c in d["spec"]["template"]["spec"]["containers"]:
            out[c["name"]] = c
    return out


def _secret_env(container: dict) -> dict:
    """{ENV_NAME: (secret_name, secret_key)} for enumerated secretKeyRef vars."""
    out = {}
    for e in container.get("env") or []:
        ref = ((e.get("valueFrom") or {}).get("secretKeyRef")) or {}
        if ref:
            out[e["name"]] = (ref.get("name"), ref.get("key"))
    return out


def _envfrom_secrets(container: dict) -> set:
    return {
        (f.get("secretRef") or {}).get("name")
        for f in container.get("envFrom") or []
        if f.get("secretRef")
    }


def test_service_manifest_has_no_envfrom_secretref():
    """Pins the premise. If services ever gain envFrom/secretRef this whole
    test file can be simplified -- but until then, enumeration is mandatory."""
    for name, c in _deployments(_SERVICES).items():
        assert not _envfrom_secrets(c), (
            f"{name} now uses envFrom/secretRef; revisit this test file"
        )


@pytest.mark.parametrize("name", _AUDIT_PODS)
def test_audit_pods_get_internal_api_key(name):
    """The validator and every emitting service must enumerate the key."""
    deps = _deployments(_SERVICES)
    assert name in deps, f"{name} missing from {_SERVICES.name}"
    env = _secret_env(deps[name])
    assert "INTERNAL_API_KEY" in env, (
        f"{name} has no INTERNAL_API_KEY env entry, so adding the key to Secret "
        f"{_SECRET_NAME} cannot reach it (services enumerate secretKeyRef; only "
        "workers use envFrom/secretRef)"
    )
    assert env["INTERNAL_API_KEY"] == (_SECRET_NAME, "INTERNAL_API_KEY"), env[
        "INTERNAL_API_KEY"
    ]


def test_every_service_gets_the_key_like_jwt_secret():
    """Plumb it uniformly, mirroring JWT_SECRET/ENCRYPTION_KEY.

    A per-service allowlist rots the moment a service starts emitting audit
    events (it would then drop rows silently). Every service already gets
    JWT_SECRET whether it uses it or not; the audit key gets the same
    treatment so the gate can never be half-configured.
    """
    for name, c in _deployments(_SERVICES).items():
        env = _secret_env(c)
        assert "JWT_SECRET" in env, f"{name} lost JWT_SECRET"
        assert "INTERNAL_API_KEY" in env, (
            f"{name} enumerates JWT_SECRET but not INTERNAL_API_KEY"
        )
        assert env["INTERNAL_API_KEY"] == (_SECRET_NAME, "INTERNAL_API_KEY")


def test_generator_emits_the_key():
    """gen-services.sh regenerates 40-services.yaml wholesale; if the key is
    only hand-patched into the YAML the next regen deletes it."""
    gen = _GEN.read_text()
    assert "INTERNAL_API_KEY" in gen, "gen-services.sh would drop the key on regen"
    assert (
        "secretKeyRef: { name: tmvault-secrets, key: INTERNAL_API_KEY }" in gen
    ), "generator must source the key from Secret tmvault-secrets"


def test_workers_still_inherit_the_whole_secret():
    """Regression guard: workers must keep envFrom/secretRef so the six
    worker emitters pick the key up without per-worker edits."""
    deps = _deployments(_WORKERS)
    assert deps, "no worker Deployments parsed"
    for name, c in deps.items():
        assert _SECRET_NAME in _envfrom_secrets(c), (
            f"worker {name} no longer inherits Secret {_SECRET_NAME}"
        )


def test_configmap_does_not_carry_the_key():
    """It is a shared secret, not configuration -- it must not land in a
    ConfigMap where it is readable by anything that can read the namespace."""
    cm = (_K8S / "10-configmap.yaml").read_text()
    assert "INTERNAL_API_KEY" not in cm, (
        "INTERNAL_API_KEY must live in Secret tmvault-secrets, not the ConfigMap"
    )


def test_migrate_job_untouched_by_the_audit_key():
    """The migrate Job emits no audit events; it should not grow the key."""
    assert "INTERNAL_API_KEY" not in _MIGRATE.read_text()
