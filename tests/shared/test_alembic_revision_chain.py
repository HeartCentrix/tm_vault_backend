"""One database, two code lineages, therefore ONE alembic chain.

Production runs a single PostgreSQL 17.10 database whose ``alembic_version``
is ``0006``. The services that reach it are built from two forked integration
branches (``integrate/archive`` -> backup/discovery/tenant/chat-export/audit/
auth/job/alert/api-gateway/restore/azure-workload; ``integrate/perf`` ->
backup-scheduler, snapshot-service), and ``alembic/versions/`` is co-owned by
both. Each fork independently grabbed the next bare sequence number, so both
trees shipped a revision literally identified as ``0007`` with a different
body:

    perf     0007_resource_protected_bytes        revision="0007" down="0006"
    archive  0007_uq_tier2_child_per_parent       revision="0007" down="0006"

Neither tree can detect that on its own -- inside each tree the chain looks
single-headed and valid. The failure only appears against the shared database:
merge the trees into one image and Alembic refuses on the duplicate
identifier so NOTHING applies; ship them separately and whichever runs first
stamps ``alembic_version`` to a revision the other tree cannot resolve, after
which ``alembic upgrade head`` on the other lineage dies with "Can't locate
revision" and neither the Tier-2 unique index nor the blob-guard indexes are
ever created.

The fix is one chain, present in full in BOTH trees. ``EXPECTED_CHAIN`` below
is that contract and is byte-identical in both worktrees: if either lineage
adds, renumbers or re-parents a revision, BOTH trees go red until the other
lineage is reconciled. That is the cross-lineage gate no per-branch review
could provide.

Deliberately DB-free -- it parses the version files with ``ast`` (alembic is
not importable in this environment, and a chain check should not need a
database anyway).
"""
from __future__ import annotations

import ast
import builtins
from pathlib import Path

import pytest

_VERSIONS = Path(__file__).resolve().parents[2] / "alembic" / "versions"

# The prod DB's current alembic_version (verified 2026-07-31).
PROD_REVISION = "0006"

# (revision, down_revision) for the ENTIRE chain, oldest first. Both lineages
# must carry all of it. 0007..0010 are the revisions pending against prod.
EXPECTED_CHAIN = [
    ("0001", None),
    ("0002", "0001"),
    ("0003", "0002"),
    ("20260616_0004", "0003"),
    ("0005", "20260616_0004"),
    ("0006", "0005"),
    ("0007", "0006"),            # resource_protected_bytes      (perf origin)
    ("0008", "0007"),            # resource_content_rollup       (perf origin)
    ("0009", "0008"),            # uq_tier2_child_per_parent     (archive origin)
    ("0010", "0009"),            # blob_reference_guard_indexes  (archive origin)
    ("0011", "0010"),            # immutable chat catalogue
]

# Substring that must appear in the module docstring of each pending revision,
# so a renumber cannot accidentally point an id at the wrong migration body.
PENDING_SUBJECTS = {
    "0007": "resource_protected_bytes",
    "0008": "resource_content_rollup",
    "0009": "Tier-2 child",
    "0010": "blob reference guard",
    "0011": "chat catalogue",
}


def _literal(tree: ast.Module, name: str):
    """Module-level string/None literal assigned to `name` (handles both
    `revision = "x"` and `revision: str = "x"`)."""
    for node in tree.body:
        targets = []
        if isinstance(node, ast.Assign):
            targets = node.targets
        elif isinstance(node, ast.AnnAssign):
            targets = [node.target]
        else:
            continue
        for t in targets:
            if isinstance(t, ast.Name) and t.id == name:
                try:
                    return ast.literal_eval(node.value)
                except ValueError:
                    return None
    raise AssertionError(f"no module-level `{name}` assignment")


def _revisions() -> dict:
    """{revision_id: (path, down_revision, tree)} across all version files."""
    out = {}
    for p in sorted(_VERSIONS.glob("*.py")):
        tree = ast.parse(p.read_text(), filename=str(p))
        rev = _literal(tree, "revision")
        down = _literal(tree, "down_revision")
        assert rev not in out, (
            f"DUPLICATE revision id {rev!r}: {out[rev][0].name} and {p.name}. "
            "Two revisions with one id means Alembic can neither apply both "
            "nor resolve the chain against the shared prod database."
        )
        out[rev] = (p, down, tree)
    return out


def _seq(rev: str) -> int:
    """Numeric ordering key: the trailing NNNN of `0007` / `20260616_0004`."""
    return int(rev.rsplit("_", 1)[-1])


def test_no_duplicate_revision_ids():
    """_revisions() asserts on collision; this names the invariant."""
    revs = _revisions()
    assert len(revs) == len(EXPECTED_CHAIN), sorted(revs)


def test_exactly_one_head():
    revs = _revisions()
    parents = {down for _p, down, _t in revs.values() if down}
    heads = sorted(set(revs) - parents)
    assert len(heads) == 1, f"expected one head, found {heads}"
    assert heads[0] == EXPECTED_CHAIN[-1][0]


def test_chain_is_exactly_the_expected_linear_chain():
    """The whole point: BOTH lineages carry the SAME complete chain."""
    revs = _revisions()
    actual = [(r, revs[r][1]) for r in sorted(revs, key=_seq)]
    assert actual == EXPECTED_CHAIN, (
        "alembic chain diverged from the cross-lineage contract.\n"
        f"  actual:   {actual}\n"
        f"  expected: {EXPECTED_CHAIN}\n"
        "If this is an intentional new revision, add it here AND copy the "
        "revision file into the other integration worktree."
    )


def test_chain_resolves_from_prod_revision_forward():
    """Walk down from head; it must reach prod's 0006 with no gaps, and every
    down_revision must name a revision that exists in THIS tree."""
    revs = _revisions()
    head = EXPECTED_CHAIN[-1][0]
    seen, cur = [], head
    while cur is not None:
        assert cur in revs, (
            f"down_revision {cur!r} does not exist in this tree -- "
            "`alembic upgrade head` would fail with 'Can't locate revision'"
        )
        seen.append(cur)
        assert len(seen) <= len(revs) + 1, f"cycle in chain: {seen}"
        cur = revs[cur][1]
    assert PROD_REVISION in seen, (
        f"chain from head {head} never reaches prod revision {PROD_REVISION}"
    )
    pending = seen[: seen.index(PROD_REVISION)]
    assert sorted(pending, key=_seq) == sorted(PENDING_SUBJECTS, key=_seq), (
        f"pending set against prod is {sorted(pending, key=_seq)}, "
        f"expected {sorted(PENDING_SUBJECTS, key=_seq)}"
    )


def test_revision_ids_increase_along_the_chain():
    """An ordered identifier: each revision's sequence number is strictly
    greater than its parent's. `20260731_0007` chained behind `0007` violated
    this, which is what made the collision invisible."""
    revs = _revisions()
    for rev, (path, down, _t) in revs.items():
        if down is None:
            continue
        assert _seq(rev) > _seq(down), (
            f"{path.name}: revision {rev!r} does not sort after its parent "
            f"{down!r}; ids must be globally unique AND ordered"
        )


@pytest.mark.parametrize("rev", sorted(PENDING_SUBJECTS, key=_seq))
def test_pending_revision_id_matches_its_body(rev):
    """Guards the renumber: id 0009 must still be the Tier-2 migration."""
    revs = _revisions()
    assert rev in revs, f"{rev} missing from this tree"
    path, _down, tree = revs[rev]
    doc = ast.get_docstring(tree) or ""
    assert PENDING_SUBJECTS[rev] in doc, (
        f"{path.name} (revision {rev}) docstring does not mention "
        f"{PENDING_SUBJECTS[rev]!r} -- id and body may have been mismatched "
        "during a renumber"
    )
    assert path.name.startswith(rev + "_"), (
        f"{path.name} should be named after its revision id {rev}"
    )


def _unresolved_globals(tree: ast.Module) -> set:
    bound = set(dir(builtins))
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store):
            bound.add(node.id)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            bound.add(node.name)
            a = node.args
            for arg in [*a.posonlyargs, *a.args, *a.kwonlyargs]:
                bound.add(arg.arg)
            if a.vararg:
                bound.add(a.vararg.arg)
            if a.kwarg:
                bound.add(a.kwarg.arg)
        elif isinstance(node, ast.ClassDef):
            bound.add(node.name)
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            for al in node.names:
                bound.add((al.asname or al.name).split(".")[0])
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            bound.add(node.target.id)
        elif isinstance(node, ast.ExceptHandler) and node.name:
            bound.add(node.name)
    used = {
        n.id
        for n in ast.walk(tree)
        if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)
    }
    return used - bound


@pytest.mark.parametrize("rev", sorted(PENDING_SUBJECTS, key=_seq))
def test_pending_revision_references_no_undefined_name(rev):
    """A NameError inside downgrade() is a rollback footgun on a shared prod
    DB: the operator finds out mid-rollback, with the schema half-reverted.
    (The blob-guard revision kept a `SNAPSHOT_ITEMS_PARTITIONS` loop after the
    partitioning premise was removed -- there are no partitions in prod.)"""
    path, _down, tree = _revisions()[rev]
    missing = _unresolved_globals(tree)
    assert not missing, f"{path.name} references undefined name(s): {sorted(missing)}"
