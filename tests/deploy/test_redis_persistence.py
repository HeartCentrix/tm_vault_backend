"""tmvault-redis must survive a restart without emptying the revocation denylist.

``shared/security.py`` keeps the refresh-token revocation denylist in Redis
(``jwt_revoked:<jti>`` keys, TTL = remaining refresh lifetime, up to
``JWT_REFRESH_EXPIRATION_DAYS`` = 7 days). ``revoke_refresh_token`` is the
SET NX compare-and-swap that stops a rotated refresh token being replayed.

``21-redis.yaml`` used to run ``--save "" --appendonly no`` with no volume:
every pod restart (node drain, image roll, OOM kill, Kyverno re-mutation)
started Redis empty, so every refresh token rotated or logged out in the
previous ~7 days became replayable again until its natural expiry.

These tests pin the durable shape:

  * AOF on, fsync every second, written to a PersistentVolumeClaim mounted
    at the official image's ``dir`` (``/data``);
  * ``Recreate`` strategy, because the claim is ReadWriteOnce and a
    RollingUpdate with replicas=1 surges a second pod that cannot attach
    the RBD image while the old pod still holds it on another node;
  * explicit resources, so the itops LimitRange default (1 CPU / 1Gi) is
    not what sizes the pod;
  * ``noeviction`` stated explicitly (the denylist must error, never evict);
  * requirepass kept, and the password never written into the pod spec.
"""
from __future__ import annotations

from pathlib import Path

import yaml

_ROOT = Path(__file__).resolve().parents[2]
_K8S = _ROOT / "deploy" / "taylor-morrison" / "k8s"
_REDIS = _K8S / "21-redis.yaml"

_PVC_NAME = "tmvault-redis-data"
_DATA_DIR = "/data"  # WORKDIR / `dir` of the official redis image


def _docs() -> list:
    return [d for d in yaml.safe_load_all(_REDIS.read_text()) if d]


def _by_kind(kind: str) -> list:
    return [d for d in _docs() if d.get("kind") == kind]


def _deployment() -> dict:
    deps = _by_kind("Deployment")
    assert len(deps) == 1, [d.get("kind") for d in _docs()]
    return deps[0]


def _pod_spec() -> dict:
    return _deployment()["spec"]["template"]["spec"]


def _redis_container() -> dict:
    containers = _pod_spec()["containers"]
    assert len(containers) == 1, [c["name"] for c in containers]
    assert containers[0]["name"] == "redis"
    return containers[0]


def _arg_value(args: list, flag: str) -> str:
    """Value following ``flag`` in a redis-server argv (``--flag value``)."""
    assert args.count(flag) == 1, (flag, args)
    i = args.index(flag)
    assert i + 1 < len(args), (flag, args)
    return args[i + 1]


def test_pvc_is_declared_for_the_data_dir():
    pvcs = _by_kind("PersistentVolumeClaim")
    assert len(pvcs) == 1, "exactly one PVC expected in 21-redis.yaml"
    pvc = pvcs[0]
    assert pvc["metadata"]["name"] == _PVC_NAME
    assert pvc["metadata"]["namespace"] == "itops"
    assert pvc["metadata"]["labels"]["app"] == "tmvault-redis"
    assert pvc["spec"]["accessModes"] == ["ReadWriteOnce"]
    # exact class name verified with `kubectl get sc` (the cluster default)
    assert pvc["spec"]["storageClassName"] == "rook-ceph-block"
    assert pvc["spec"]["resources"]["requests"]["storage"] == "1Gi"


def test_pvc_is_mounted_at_the_redis_dir():
    c = _redis_container()
    mounts = {m["mountPath"]: m["name"] for m in c.get("volumeMounts") or []}
    assert _DATA_DIR in mounts, f"no volumeMount at {_DATA_DIR}: {mounts}"
    vol_name = mounts[_DATA_DIR]
    volumes = {v["name"]: v for v in _pod_spec().get("volumes") or []}
    assert vol_name in volumes, volumes
    claim = (volumes[vol_name].get("persistentVolumeClaim") or {}).get("claimName")
    assert claim == _PVC_NAME, (
        f"{_DATA_DIR} must be backed by PVC {_PVC_NAME}, not {volumes[vol_name]}"
    )
    assert "emptyDir" not in volumes[vol_name]


def test_aof_persistence_is_on():
    args = _redis_container()["args"]
    assert _arg_value(args, "--appendonly") == "yes", args
    assert _arg_value(args, "--appendfsync") == "everysec", args


def test_rdb_snapshots_stay_off_because_aof_is_the_durability_path():
    """AOF+everysec is the durability mechanism; a periodic BGSAVE would only
    add a fork/copy-on-write spike inside a 512Mi limit for a keyspace of a
    few thousand small keys. Redis 7 AOF rewrites already use an RDB-preamble
    base file, so compaction is covered. Change this deliberately, not by
    accident."""
    args = _redis_container()["args"]
    assert _arg_value(args, "--save") == "", args


def test_noeviction_is_explicit():
    args = _redis_container()["args"]
    assert _arg_value(args, "--maxmemory-policy") == "noeviction", args


def test_requirepass_is_kept_and_sourced_from_the_secret():
    c = _redis_container()
    args = c["args"]
    assert _arg_value(args, "--requirepass") == "$(REDIS_PASSWORD)", args
    env = {e["name"]: e for e in c.get("env") or []}
    ref = env["REDIS_PASSWORD"]["valueFrom"]["secretKeyRef"]
    assert (ref["name"], ref["key"]) == ("tmvault-secrets", "REDIS_PASSWORD")
    assert "value" not in env["REDIS_PASSWORD"], "password must not be inlined"


def test_readiness_probe_authenticates_without_leaking_the_password():
    c = _redis_container()
    cmd = c["readinessProbe"]["exec"]["command"]
    assert cmd[:2] == ["sh", "-c"], cmd
    script = " ".join(cmd[2:])
    assert "redis-cli" in script and "ping" in script, script
    # authenticates via the env var expanded inside the container, so the
    # pod spec never carries the literal password
    assert "$REDIS_PASSWORD" in script, script
    assert "-a" in script, script


def test_strategy_is_recreate_for_the_rwo_claim():
    dep = _deployment()
    assert dep["spec"]["replicas"] == 1
    assert dep["spec"].get("strategy") == {"type": "Recreate"}, dep["spec"].get(
        "strategy"
    )


def test_resources_are_explicit_not_limitrange_defaults():
    res = _redis_container().get("resources") or {}
    assert res.get("requests") == {"cpu": "100m", "memory": "128Mi"}, res
    assert res.get("limits") == {"cpu": "500m", "memory": "512Mi"}, res


def test_pod_can_write_the_claim_as_the_redis_user():
    """fsGroup is what makes a freshly provisioned RBD volume writable by the
    non-root redis uid; without it AOF writes fail with EACCES and the pod
    crash-loops on first start."""
    sc = _pod_spec()["securityContext"]
    assert sc["runAsUser"] == 999
    assert sc["fsGroup"] == 999
    assert sc["runAsNonRoot"] is True


def test_compose_stack_already_runs_aof():
    """Premise guard for 'nothing relies on persistence being off': the dev
    compose stack has run `--appendonly yes` all along."""
    compose = (_ROOT / "docker-compose.yml").read_text()
    assert "--appendonly yes" in compose
