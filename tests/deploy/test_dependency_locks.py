"""Images must build from the resolved .lock files on a pinned base image.

2026-10-14: workers crash-looped with ModuleNotFoundError greenlet because
open requirement ranges re-resolved at build time. Regenerate locks with
scripts/relock.sh after editing any requirements*.txt.
"""
import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parents[2]
DOCKERFILES = [p for p in ROOT.glob("**/Dockerfile") if ".worktrees" not in p.parts]
LOCKS = [p for p in ROOT.glob("**/requirements*.lock") if ".worktrees" not in p.parts]


def test_dockerfiles_install_from_lock_on_pinned_base():
    bad = []
    for df in DOCKERFILES:
        text = df.read_text()
        if re.search(r"requirements[\w.-]*\.txt", text):
            bad.append(f"{df.relative_to(ROOT)}: installs from .txt")
        for img in re.findall(r"^FROM\s+(python:\S+|debian:\S+)", text, re.M):
            if "@sha256:" not in img:
                bad.append(f"{df.relative_to(ROOT)}: unpinned {img}")
    assert bad == []


def test_every_requirements_txt_has_lock_with_greenlet_when_sqlalchemy():
    bad = []
    for txt in ROOT.glob("**/requirements*.txt"):
        if ".worktrees" in txt.parts:
            continue
        lock = txt.with_suffix(".lock")
        if not lock.exists():
            bad.append(f"{lock.relative_to(ROOT)} missing")
            continue
        body = lock.read_text()
        if re.search(r"^sqlalchemy==", body, re.M) and not re.search(r"^greenlet==", body, re.M):
            bad.append(f"{lock.relative_to(ROOT)} lacks greenlet")
    assert LOCKS and bad == []
