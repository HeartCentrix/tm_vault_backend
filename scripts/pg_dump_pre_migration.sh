#!/usr/bin/env bash
# Take a fenced pg_dump before applying alembic 0002 (partitioning).
#
# Reads $DATABASE_URL (or DB_HOST/DB_PORT/DB_USERNAME/DB_PASSWORD/DB_NAME)
# the same way the app does. Writes a custom-format dump to
# $BACKUP_DIR/tmvault-pre-0002-<timestamp>.dump, then exports
# LAST_PG_DUMP_PATH so the pre-flight check accepts the run.
#
# Usage:
#   BACKUP_DIR=/mnt/backups ./scripts/pg_dump_pre_migration.sh
#   # then:
#   python3 scripts/pre_migration_check.py
#   # if it passes:
#   alembic upgrade head

set -euo pipefail

BACKUP_DIR="${BACKUP_DIR:-./backups}"
mkdir -p "$BACKUP_DIR"

STAMP=$(date -u +"%Y%m%dT%H%M%SZ")
OUT="$BACKUP_DIR/tmvault-pre-0002-$STAMP.dump"

if [[ -n "${DATABASE_URL:-}" ]]; then
    DSN="$DATABASE_URL"
else
    DSN="postgresql://${DB_USERNAME:?DB_USERNAME required}:${DB_PASSWORD:?DB_PASSWORD required}@${DB_HOST:?DB_HOST required}:${DB_PORT:-5432}/${DB_NAME:?DB_NAME required}"
fi

# Convert SQLAlchemy DSN to one pg_dump accepts (drop the +asyncpg driver suffix).
DSN="${DSN//postgresql+asyncpg/postgresql}"

echo "[pre-migration-backup] starting pg_dump → $OUT"
echo "[pre-migration-backup] schema = ${DB_SCHEMA:-tm_vault}"

# -Fc  : custom format (smaller, parallel-restorable)
# -Z9  : max compression (CPU-heavy but disk-conservative on Railway)
# -n   : restrict to the app schema only (skip postgres internals)
pg_dump \
    -Fc -Z9 \
    -n "${DB_SCHEMA:-tm_vault}" \
    --no-owner --no-privileges \
    --file="$OUT" \
    "$DSN"

SIZE=$(du -h "$OUT" | awk '{print $1}')
echo "[pre-migration-backup] done: $OUT ($SIZE)"
echo "[pre-migration-backup] verifying..."
pg_restore --list "$OUT" > /dev/null
echo "[pre-migration-backup] verified — dump is restorable"

# Export so pre_migration_check.py accepts the freshness.
export LAST_PG_DUMP_PATH="$OUT"
echo ""
echo "NEXT STEPS:"
echo "  export LAST_PG_DUMP_PATH=\"$OUT\""
echo "  python3 scripts/pre_migration_check.py"
echo "  # if check passes:"
echo "  alembic upgrade head"
echo ""
echo "ROLLBACK (if migration goes wrong):"
echo "  alembic downgrade -1     # in-place reversal, may take hours"
echo "  # OR — clean slate restore:"
echo "  dropdb \$DB_NAME && createdb \$DB_NAME"
echo "  pg_restore -d \$DB_NAME --schema=${DB_SCHEMA:-tm_vault} $OUT"
