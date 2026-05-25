import sys
import types

import pytest

from open_pit_wall import main as app_main


def test_default_command_launches_menu(monkeypatch):
    launched = []

    monkeypatch.setitem(
        sys.modules,
        "open_pit_wall.cli_menu",
        types.SimpleNamespace(run_cli_menu=lambda: launched.append("menu")),
    )

    with pytest.raises(SystemExit) as excinfo:
        app_main.main([])

    assert excinfo.value.code == 0
    assert launched == ["menu"]


def test_top_level_help_prints_usage(capsys):
    with pytest.raises(SystemExit) as excinfo:
        app_main.main(["--help"])

    assert excinfo.value.code == 0
    assert "open-pit-wall" in capsys.readouterr().out


def test_replay_command_delegates_to_broadcaster(monkeypatch):
    delegated = []

    monkeypatch.setitem(
        sys.modules,
        "open_pit_wall.telemetry_broadcaster",
        types.SimpleNamespace(main=lambda argv: delegated.append(argv)),
    )

    app_main.main(["replay", "--help"])

    assert delegated == [["--help"]]
