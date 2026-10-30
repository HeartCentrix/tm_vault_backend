"""The migrate Job must never STAMP a populated database.

``tmvault-migrate`` was written as a fresh-install bootstrap: ``init_db()``
builds the schema with ``create_all`` and then ``alembic stamp head`` records
every revision as applied WITHOUT running it. That is correct for an empty
database and actively destructive for prod.

Prod's ``alembic_version`` is ``0006`` with four revisions pending
(0007 resource_protected_bytes, 0008 resource_content_rollup,
0009 uq_tier2_child_per_parent, 0010 blob_reference_guard_indexes). Running
``alembic stamp head`` there jumps the row straight to ``0010`` and the Tier-2
unique index and the blob-guard indexes are never created — while every
subsequent ``alembic current`` reports the database fully migrated. Silent,
unrecoverable-without-noticing, and exactly the outcome the duplicate-``0007``
blocker was about.

So the Job has to branch on whether the database is already stamped, and the
populated branch must be ``alembic upgrade`` only.
"""
from __future__ import annotations

import re
from pathlib import Path

import yaml

_K8S = Path(__file__).resolve().parents[2] / "deploy" / "taylor-morrison" / "k8s"
_MIGRATE = _K8S / "30-migrate-job.yaml"


def _job() -> dict:
    docs = [d for d in yaml.safe_load_all(_MIGRATE.read_text()) if d]
    jobs = [d for d in docs if d.get("kind") == "Job"]
    assert len(jobs) == 1, [d.get("kind") for d in docs]
    return jobs[0]


def _command_text() -> str:
    c = _job()["spec"]["template"]["spec"]["containers"][0]
    return "\n".join(c.get("command") or [])


def _arms() -> dict:
    """{'FRESH': <script text>, 'EXISTING': <script text>} from the case arms.

    Splitting on the `case` labels is what proves the two dangerous
    operations are in DIFFERENT branches rather than merely both present.
    """
    cmd = _command_text()
    marks = [
        (m.start(), m.group(1))
        for m in re.finditer(r"^\s*(FRESH|EXISTING|\*)\)\s*$", cmd, re.M)
    ]
    assert [n for _s, n in marks] == ["FRESH", "EXISTING", "*"], marks
    bounds = [s for s, _n in marks] + [len(cmd)]
    return {
        name: cmd[bounds[i] : bounds[i + 1]]
        for i, (_s, name) in enumerate(marks)
        if name != "*"
    }


def test_stamp_head_only_on_the_fresh_path():
    """`alembic stamp head` must be reachable only on the fresh-install path."""
    cmd = _command_text()
    assert "stamp head" in cmd, "fresh-install bootstrap should still stamp"
    arms = _arms()
    assert "stamp head" in arms["FRESH"]
    assert "stamp" not in arms["EXISTING"], (
        "stamping a populated database would falsely mark revisions 0007-0010 "
        f"applied against prod's 0006: {arms['EXISTING']!r}"
    )
    # And nothing outside the two arms may stamp.
    preamble = cmd[: cmd.index("FRESH)")]
    assert "stamp" not in preamble, preamble


def test_populated_database_path_runs_upgrade():
    cmd = _command_text()
    assert "alembic upgrade head" in cmd, (
        "no `alembic upgrade head` anywhere in the migrate Job — an existing "
        "database would never get 0007-0010 applied"
    )
    assert "alembic upgrade head" in _arms()["EXISTING"]


def test_init_db_is_not_run_against_a_populated_database():
    """init_db() is the boot-time create_all/index batch. On a populated
    ~87GB database its DDL sweep is what froze prod for ~5 minutes; the
    upgrade path must not call it."""
    assert "init_db" not in _arms()["EXISTING"], (
        "init_db() on the upgrade path does heavy DDL against the live schema"
    )
    assert "init_db" in _arms()["FRESH"]


def test_job_detects_existing_alembic_version():
    """The branch has to be decided from the database, not from an operator
    remembering which flavour of cluster this is."""
    cmd = _command_text()
    assert "alembic_version" in cmd, (
        "the Job does not look at alembic_version, so it cannot tell a fresh "
        "install from prod"
    )


def test_job_fails_loudly_when_it_cannot_decide():
    """Neither branch is safe to guess at: an undecidable state must exit
    non-zero rather than defaulting to stamp (silent) or upgrade (surprise)."""
    cmd = _command_text()
    assert "set -e" in cmd, "no `set -e`; a failed probe would fall through"
    assert "exit 1" in cmd, "no non-zero exit for the undecidable case"


def test_job_still_restart_never_and_bounded():
    job = _job()
    assert job["spec"]["template"]["spec"]["restartPolicy"] == "Never"
    assert job["spec"]["backoffLimit"] <= 2


def test_job_image_is_digest_pinned():
    c = _job()["spec"]["template"]["spec"]["containers"][0]
    assert "@sha256:" in c["image"], c["image"]
