"""Deployments that never set these flags (TM's k8s configmap) must run the validated paths:
the chat catalogue (no P8 history copy / 4-hour bucket wait), provider work, copy work,
batch-row finalization and chat presentation."""

LEGACY_PATH_FLAGS = ("PROVIDER_WORK_ENABLED", "CHAT_CATALOGUE_WRITER_ENABLED", "COPY_WORK_ENABLED",
                     "BATCH_ROW_REDESIGN_ENABLED", "CHAT_PRESENTATION_ENABLED")


def test_validated_paths_are_on_when_unset(monkeypatch):
    for flag in LEGACY_PATH_FLAGS:
        monkeypatch.delenv(flag, raising=False)
    from shared import chat_presentation, copy_workflow, provider_work
    from shared import config
    fresh = config.Settings()
    assert provider_work.enabled() and copy_workflow.copy_work_enabled()
    assert chat_presentation.chat_presentation_enabled()
    assert fresh.CHAT_CATALOGUE_WRITER_ENABLED and fresh.BATCH_ROW_REDESIGN_ENABLED


def test_explicit_false_still_turns_a_path_off(monkeypatch):
    monkeypatch.setenv("PROVIDER_WORK_ENABLED", "false")
    monkeypatch.setenv("CHAT_CATALOGUE_WRITER_ENABLED", "false")
    from shared import provider_work, config
    assert not provider_work.enabled() and not config.Settings().CHAT_CATALOGUE_WRITER_ENABLED
