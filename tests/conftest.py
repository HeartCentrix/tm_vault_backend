"""Suite-wide defaults.

Production now defaults the provider-work, chat catalogue, copy-work, batch-row and chat
presentation paths ON (TM's k8s config never set them, so it ran the legacy paths).
Most tests here exercise the legacy paths and relied on the old OFF defaults; they keep
that unless a test opts in. tests/shared/test_production_defaults.py locks the ON defaults.
"""
import os

LEGACY_PATH_FLAGS = ("PROVIDER_WORK_ENABLED", "CHAT_CATALOGUE_WRITER_ENABLED", "COPY_WORK_ENABLED",
                     "BATCH_ROW_REDESIGN_ENABLED", "CHAT_PRESENTATION_ENABLED")
for _flag in LEGACY_PATH_FLAGS:
    os.environ.setdefault(_flag, "false")
