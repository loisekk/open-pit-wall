import json

import pytest

from open_pit_wall import data_loader


def test_get_session_cache_filename_uses_json_extension():
    filename = data_loader.get_session_cache_filename(2024, 1, "Monaco Grand Prix", "R")

    assert filename.endswith(".json")


def test_app_directories_honor_environment_override(monkeypatch, tmp_path):
    monkeypatch.setenv(data_loader.APP_HOME_ENV_VAR, str(tmp_path))

    assert data_loader.get_app_data_directory() == tmp_path
    assert data_loader.get_fastf1_cache_directory() == tmp_path / ".fastf1-cache"
    assert data_loader.get_computed_data_directory() == tmp_path / "computed_data"


def test_load_cached_session_data_reads_json_cache(monkeypatch, tmp_path):
    monkeypatch.setenv(data_loader.APP_HOME_ENV_VAR, str(tmp_path))
    cache_path = data_loader.get_computed_data_directory() / data_loader.get_session_cache_filename(
        2024, 1, "Monaco Grand Prix", "R"
    )
    payload = {
        "schema_version": data_loader.REPLAY_SCHEMA_VERSION,
        "replay_start_utc": "2024-01-01T12:00:00Z",
        "frames": [{"t": 0.0, "lap": 1, "event": "telemetry.drivers", "payload": []}],
        "race_control_messages": [],
    }
    cache_path.write_text(json.dumps(payload), encoding="utf-8")

    loaded = data_loader.load_cached_session_data(2024, 1, "Monaco Grand Prix", "R")

    assert loaded["schema_version"] == data_loader.REPLAY_SCHEMA_VERSION
    assert loaded["frames"][0]["event"] == "telemetry.drivers"


def test_load_cached_session_data_rejects_legacy_pickle_cache(monkeypatch, tmp_path):
    monkeypatch.setenv(data_loader.APP_HOME_ENV_VAR, str(tmp_path))
    legacy_cache_path = (
        data_loader.get_computed_data_directory()
        / data_loader.get_session_cache_filename(2024, 1, "Monaco Grand Prix", "R").replace(
            ".json", ".pkl"
        )
    )
    legacy_cache_path.write_bytes(b"legacy pickle")

    with pytest.raises(ValueError, match="Legacy pickle cache files"):
        data_loader.load_cached_session_data(2024, 1, "Monaco Grand Prix", "R")
