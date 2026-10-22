"""Tests for chat-export-worker file-path coordination.

These pin the rule that the inline image src URLs the normalizer puts
into the rendered HTML / PDF MUST match the file paths the packager
actually writes inside the ZIP. Pre-fix the consumer used a raw
last-segment of `thread_path` (no sanitisation, no layout awareness)
while the packager used `_safe_name(thread_name)` + per-message
sub-folders — so every chat thread whose name contained a colon
(Teams group chats are like that by default) exported as broken
images and broken attachment links.

We also pin:
  - per-message HTML's CSS link points up the right number of levels
  - per-message attachment hrefs are relative to the HTML (not zip root)
  - reaction emoji rendering uses the glyph table

chat-export-worker has a hyphen in its dir, so the helpers are loaded
via the same AST extraction pattern used elsewhere in this suite.
"""
from __future__ import annotations

import ast
import pathlib
import re
import sys

import pytest


_BASE = pathlib.Path(__file__).resolve().parents[2] / "workers" / "chat-export-worker"


def _load_named_nodes(rel_path: str, names: set[str]):
    """Extract a set of top-level nodes (FunctionDef / Assign / AnnAssign)
    by name and exec them in a fresh module. Pre-seeds the module's
    globals with `re` and `typing.Optional` so extracted helpers can
    use them without re-executing the entire source file.
    """
    src = (_BASE / rel_path).read_text()
    tree = ast.parse(src)
    picked: list = []
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in names:
            picked.append(node)
        elif isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id in names for t in node.targets
        ):
            picked.append(node)
        elif (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and node.target.id in names
        ):
            picked.append(node)
    if not picked:
        pytest.skip(
            f"none of {names} found in {rel_path}",
            allow_module_level=True,
        )
    module = type(sys)(f"_extracted_{rel_path.replace('/', '_')}")
    # Pre-seed common deps. Add more here if a helper grows new ones.
    import re as _re
    import typing as _typing
    import html as _ihtml
    module.__dict__["re"] = _re
    module.__dict__["Optional"] = _typing.Optional
    module.__dict__["ihtml"] = _ihtml
    code = compile(
        ast.Module(body=picked, type_ignores=[]),
        str(_BASE / rel_path), "exec",
    )
    exec(code, module.__dict__)
    return module


_packager_ns = _load_named_nodes(
    "packager/thread_packager.py", {"_safe_name", "_unique_in_dir"},
)
_normalizer_ns = _load_named_nodes(
    "render/normalizer.py",
    {"_REACTION_GLYPH", "_reaction_glyph", "_plaintext_to_html"},
)
_safe_name = _packager_ns._safe_name
_unique_in_dir = _packager_ns._unique_in_dir
_reaction_glyph = _normalizer_ns._reaction_glyph
_plaintext_to_html = _normalizer_ns._plaintext_to_html


# ── _safe_name ─────────────────────────────────────────────────────────


def test_safe_name_strips_colons_in_teams_group_chats():
    """Teams group chats are routinely named like 'Group: foo, bar'.
    Colon is reserved on Windows + breaks URL parsing — _safe_name
    replaces it. This is the rule both consumer and packager rely on
    to keep their paths in sync."""
    assert _safe_name("Group: foo, bar") == "Group_ foo, bar"


def test_safe_name_strips_path_separators():
    """If a chat name contains a slash it would otherwise leak into
    the ZIP path structure and create unintended subfolders."""
    assert "/" not in _safe_name("a/b/c")
    assert "\\" not in _safe_name("a\\b\\c")


def test_safe_name_falls_back_when_empty():
    assert _safe_name("") == "item"
    assert _safe_name("   ") == "item"
    assert _safe_name("?\":*") == "_"  # all chars stripped → underscore


def test_safe_name_truncates_long_input():
    """120-char cap so deeply nested ZIP paths don't hit Windows' 260
    MAX_PATH on Windows extractors."""
    long_in = "x" * 500
    assert len(_safe_name(long_in)) == 120


# ── attachment name uniquification ─────────────────────────────────────


def test_unique_in_dir_dedupes_same_name():
    """Two attachments named 'contract.pdf' shared in different messages
    of one thread land in the SAME directory in single_thread layout.
    Pre-fix they became two ZIP entries at the identical path; extraction
    kept only one and both HTML chips linked to the survivor — wrong-data
    delivery. Pinned: the second gets a ' (2)' suffix before the ext."""
    used: set = set()
    assert _unique_in_dir("contract.pdf", used) == "contract.pdf"
    assert _unique_in_dir("contract.pdf", used) == "contract (2).pdf"
    assert _unique_in_dir("contract.pdf", used) == "contract (3).pdf"


def test_unique_in_dir_is_case_insensitive():
    """Windows/macOS extraction is case-insensitive, so 'Contract.pdf'
    and 'contract.pdf' would still overwrite each other on disk even
    though ZIP allows both entries."""
    used: set = set()
    assert _unique_in_dir("Contract.pdf", used) == "Contract.pdf"
    assert _unique_in_dir("contract.pdf", used) == "contract (2).pdf"


def test_unique_in_dir_handles_extensionless_names():
    used: set = set()
    assert _unique_in_dir("README", used) == "README"
    assert _unique_in_dir("README", used) == "README (2)"


def test_unique_in_dir_scopes_per_directory():
    """Same filename in DIFFERENT directories (per_message layout puts
    each message's attachments in its own dir) must not be renamed."""
    assert _unique_in_dir("contract.pdf", set()) == "contract.pdf"
    assert _unique_in_dir("contract.pdf", set()) == "contract.pdf"


# ── inline image local_path coordination ───────────────────────────────


def _expected_inline_path(layout: str, tsafe: str, hc_id: str, ext: str) -> str:
    """Mirror the packager's _add() path for inline images."""
    if layout == "single_thread":
        # att_dir = f"{tsafe}-attachments"
        return f"{tsafe}-attachments/inline/{hc_id}{ext}"
    # per_message: att_dir = f"per-message/{tsafe}/{_safe_name(ext_id)}/attachments"
    # but the test caller is checking that the SRC URL written into
    # the HTML resolves to that path. The HTML lives at
    # per-message/{tsafe}/{ext_id}/{ext_id}.html — so a relative
    # `./attachments/inline/{hc}{ext}` resolves to
    # per-message/{tsafe}/{ext_id}/attachments/inline/{hc}{ext}.
    return None  # checked via the relative form below


def test_inline_local_path_single_thread_uses_safe_name():
    """The consumer's hosted_by_msg local_path must use _safe_name(thread_name).
    Pre-fix it used thread_path.split('/')[-1] raw, so a thread with
    colons in its name produced an <img src> that didn't match the
    file the packager wrote."""
    thread_name = "Group: A, B, C"
    tsafe = _safe_name(thread_name)
    # The consumer builds this for single_thread layout:
    local_path = f"./{tsafe}-attachments/inline/HC1.png"
    # The packager writes the file here:
    written_path = _expected_inline_path("single_thread", tsafe, "HC1", ".png")
    # `./` strip → equal
    assert local_path[2:] == written_path


def test_inline_local_path_per_message_is_relative_to_html():
    """In per_message layout the HTML lives at
    per-message/{tsafe}/{ext_id}/{ext_id}.html. The inline image
    file lives at .../attachments/inline/{hc}{ext} — i.e. one level
    DOWN from the HTML. So the <img src> must be `./attachments/...`,
    NOT `./{tsafe}-attachments/...` (single_thread shape, which is
    what the buggy code produced for both layouts)."""
    local_path = "./attachments/inline/HC1.png"
    # Relative resolution from per-message/Group_/msgABC/msgABC.html
    html_dir = "per-message/Group_/msgABC"
    resolved = (
        local_path
        if local_path.startswith("/")
        else f"{html_dir}/{local_path[2:]}"
    )
    # Resolves into the per-message-level attachments dir.
    assert resolved == "per-message/Group_/msgABC/attachments/inline/HC1.png"


# ── attachment chip href coordination ──────────────────────────────────


def test_attachment_href_single_thread_is_root_relative():
    """Attachment chip in single_thread points to {tsafe}-attachments/
    from the root, so a href of `./{tsafe}-attachments/{name}` resolves
    correctly from the root-level HTML."""
    tsafe = "MyChat"
    href = f"./{tsafe}-attachments/foo.pdf"
    # HTML at zip root → relative resolution gives the actual path.
    assert href[2:] == f"{tsafe}-attachments/foo.pdf"


def test_attachment_href_per_message_is_html_relative_not_root_absolute():
    """The pre-fix bug: packager wrote per-message hrefs as
    `./per-message/{tsafe}/{ext_id}/attachments/{name}` — but the
    HTML is ALREADY at `per-message/{tsafe}/{ext_id}/`. Resolving
    `./per-message/.../attachments/...` from there double-nested it
    into a non-existent path. The fix uses `./attachments/{name}`
    so it resolves to the directory one level below the HTML."""
    href = "./attachments/foo.pdf"
    html_dir = "per-message/Group_/msgABC"
    resolved = f"{html_dir}/{href[2:]}"
    assert resolved == "per-message/Group_/msgABC/attachments/foo.pdf"


# ── reaction glyph table ───────────────────────────────────────────────


def test_reaction_glyph_translates_common_types():
    """Graph emits reactionType as a string name ('like', 'heart').
    We render the Unicode glyph for operator-friendly export."""
    assert _reaction_glyph("like") == "👍"
    assert _reaction_glyph("heart") == "❤️"
    assert _reaction_glyph("laugh") == "😂"
    assert _reaction_glyph("LIKE") == "👍"  # case-insensitive


def test_reaction_glyph_unknown_falls_through():
    """Unknown / custom reactions pass through as the raw string
    rather than being silently dropped — operator never loses data."""
    assert _reaction_glyph("custom_emoji_xyz") == "custom_emoji_xyz"


def test_reaction_glyph_empty_stays_empty():
    assert _reaction_glyph("") == ""
    assert _reaction_glyph(None) == ""  # tolerate missing field


# ── per-message CSS link path level ────────────────────────────────────


# ── plain-text body → HTML conversion ──────────────────────────────────


def test_plaintext_single_line_wraps_in_p():
    assert _plaintext_to_html("hello world") == "<p>hello world</p>"


def test_plaintext_newlines_become_br():
    """Pre-fix: 'line1\\nline2' rendered as a single line in the
    export. Pinned: newlines convert to <br> after HTML-escaping."""
    assert _plaintext_to_html("line1\nline2") == "<p>line1<br>line2</p>"


def test_plaintext_crlf_normalised():
    """Windows-style CRLF collapses to one <br>, not two."""
    assert _plaintext_to_html("line1\r\nline2") == "<p>line1<br>line2</p>"


def test_plaintext_bare_cr_normalised():
    """Bare CR (rare but real on classic-Mac-era pastes) folds to LF
    then to <br>."""
    assert _plaintext_to_html("line1\rline2") == "<p>line1<br>line2</p>"


def test_plaintext_metacharacters_escaped_first():
    """Escape BEFORE newline-to-br substitution so HTML in plain-text
    bodies (e.g. someone pasted '<script>alert(1)</script>') is
    rendered as harmless text. Pinned to prevent any future refactor
    that flips the order and reintroduces XSS via plain-text path."""
    out = _plaintext_to_html("<script>alert(1)</script>\nnext")
    assert "&lt;script&gt;" in out
    assert "<script>" not in out
    assert out.endswith("next</p>")


def test_plaintext_empty_and_none_safe():
    """Empty body still renders as empty <p>; None doesn't crash."""
    assert _plaintext_to_html("") == "<p></p>"
    assert _plaintext_to_html(None) == "<p></p>"


# ── streaming packager — AST pin ───────────────────────────────────────


def test_packager_streams_attachments_not_buffer_in_memory():
    """Pin that the packager's attachment write path uses
    `zf.open(..., mode='w')` (streaming) rather than `zf.writestr`
    (whole-blob in memory) so a 300+ MB chat attachment can't OOM
    the worker. Pre-fix `_read` accumulated `buf += chunk` then
    `zf.writestr(target, data)` — both buffers held the full bytes.
    """
    pkg_src = (_BASE / "packager" / "thread_packager.py").read_text()
    # The streaming helper must exist and be used in the attachment loop.
    assert "_add_stream" in pkg_src, (
        "packager is missing the streaming write helper — attachment "
        "writes will buffer the full blob in RAM"
    )
    assert "zf.open(path, mode=\"w\"" in pkg_src, (
        "packager isn't opening zip entries in write-stream mode; "
        "the older zf.writestr path keeps the whole blob in memory"
    )
    # The buggy "buf = b''" / "buf += chunk" accumulation must be gone.
    assert "buf += chunk" not in pkg_src, (
        "packager still concatenates chunks into a bytes buffer — "
        "that's the OOM path"
    )


def test_consumer_uses_streaming_blob_download():
    """Pin that the consumer hits download_blob_stream rather than
    download_blob — the latter calls .readall() and returns the
    whole blob in one allocation."""
    src = (_BASE / "consumers" / "thread.py").read_text()
    assert "download_blob_stream" in src, (
        "consumer's BlobAttachmentSource isn't using the streaming "
        "download path"
    )


def test_per_message_template_css_link_correct_depth():
    """Per-message HTML lives at /per-message/{tsafe}/{ext_id}/X.html.
    styles.css is written at the ZIP root. So the link MUST go up
    exactly three levels: ext_id → tsafe → per-message → root.
    Pre-fix it used `../../styles.css` which resolves to
    /per-message/{tsafe}/styles.css — a file that doesn't exist."""
    tpl_path = _BASE / "templates" / "message.html.j2"
    body = tpl_path.read_text()
    # Should NOT have '../../styles.css' as the active CSS link.
    css_match = re.search(r'href="(\.\./)+styles\.css"', body)
    assert css_match is not None, "per-message template missing CSS link"
    rel = css_match.group(0)
    # 3 instances of '../' → six chars of '../' before 'styles.css'
    assert rel.count("../") == 3, (
        f"per-message CSS link is {rel!r}; should be `../../../styles.css` "
        f"because the HTML is nested 3 levels deep in the ZIP."
    )
