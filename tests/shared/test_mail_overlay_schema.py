"""The overlay DDL has one source (shared.database.MAIL_OVERLAY_DDL): bootstrap, alembic 0021 and
the manual Railway script must all carry exactly it."""
from pathlib import Path

from shared.database import MAIL_OVERLAY_DDL

ROOT = Path(__file__).resolve().parents[2]


def test_manual_railway_script_is_the_shared_ddl():
    script = (ROOT / "scripts" / "2026_11_06_mail_overlay_snapshots.sql").read_text()
    assert ";\n\n".join(MAIL_OVERLAY_DDL) + ";\n" in script
    assert script.rstrip().endswith("COMMIT;") and "\nBEGIN;\n" in script


def test_migration_and_bootstrap_apply_the_shared_ddl():
    migration = (ROOT / "alembic" / "versions" / "0021_mail_overlay_snapshots.py").read_text()
    assert "for ddl in MAIL_OVERLAY_DDL:" in migration
    bootstrap = (ROOT / "shared" / "database.py").read_text()
    assert "MAIL_OVERLAY_DDL[0]" in bootstrap and "*MAIL_OVERLAY_DDL," in bootstrap


def test_chat_overlay_script_and_migration_carry_the_shared_function():
    from shared.database import SNAPSHOT_ITEMS_EFFECTIVE_FUNCTION
    script = (ROOT / "scripts" / "2026_11_07_chat_overlay_snapshots.sql").read_text()
    assert SNAPSHOT_ITEMS_EFFECTIVE_FUNCTION + ";\n" in script and "'CHAT_HOSTED_CONTENT'" in script
    assert "op.execute(SNAPSHOT_ITEMS_EFFECTIVE_FUNCTION)" in (
        ROOT / "alembic" / "versions" / "0025_chat_overlay_snapshots.py").read_text()


def test_sharepoint_overlay_script_and_migration_carry_the_shared_function():
    from shared.database import SNAPSHOT_ITEMS_EFFECTIVE_FUNCTION
    from shared.sp_overlay import SP_OVERLAY_TYPES
    script = (ROOT / "scripts" / "2026_11_08_sharepoint_overlay_snapshots.sql").read_text()
    assert SNAPSHOT_ITEMS_EFFECTIVE_FUNCTION + ";\n" in script and script.rstrip().endswith("COMMIT;")
    assert all(f"'{t}'" in SNAPSHOT_ITEMS_EFFECTIVE_FUNCTION for t in SP_OVERLAY_TYPES)
    assert "op.execute(SNAPSHOT_ITEMS_EFFECTIVE_FUNCTION)" in (
        ROOT / "alembic" / "versions" / "0028_sharepoint_overlay_snapshots.py").read_text()
