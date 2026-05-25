import importlib
import pytest
import sys
import types

from open_pit_wall import telemetry_broadcaster


class _PromptStub:
    def ask(self):
        raise KeyboardInterrupt


def _load_cli_menu_with_stubs(monkeypatch):
    questionary = types.ModuleType("questionary")
    questionary.Choice = lambda *args, **kwargs: None
    questionary.Style = lambda *args, **kwargs: None
    questionary.select = lambda *args, **kwargs: None
    questionary.text = lambda *args, **kwargs: None

    data_loader = types.ModuleType("open_pit_wall.data_loader")
    data_loader.download_race_session = lambda *args, **kwargs: None
    data_loader.find_session_cache_path = lambda *args, **kwargs: None
    data_loader.get_available_years = lambda: []
    data_loader.get_computed_data_directory = lambda: "/tmp"
    data_loader.get_race_weekends_by_year = lambda year: []
    data_loader.get_session_options_for_weekend = lambda weekend: []
    data_loader.is_session_data_downloaded = lambda *args, **kwargs: False

    class _Console:
        def print(self, *args, **kwargs):
            return None

    rich_package = types.ModuleType("rich")
    rich_package.__path__ = []

    rich_console = types.ModuleType("rich.console")
    rich_console.Console = _Console

    rich_markdown = types.ModuleType("rich.markdown")
    rich_markdown.Markdown = lambda value: value

    rich_panel = types.ModuleType("rich.panel")

    class _Panel:
        @staticmethod
        def fit(*args, **kwargs):
            return None

    rich_panel.Panel = _Panel

    rich_progress = types.ModuleType("rich.progress")
    rich_progress.Progress = object
    rich_progress.SpinnerColumn = lambda *args, **kwargs: None
    rich_progress.TextColumn = lambda *args, **kwargs: None

    monkeypatch.setitem(sys.modules, "questionary", questionary)
    monkeypatch.setitem(sys.modules, "open_pit_wall.data_loader", data_loader)
    monkeypatch.setitem(sys.modules, "rich", rich_package)
    monkeypatch.setitem(sys.modules, "rich.console", rich_console)
    monkeypatch.setitem(sys.modules, "rich.markdown", rich_markdown)
    monkeypatch.setitem(sys.modules, "rich.panel", rich_panel)
    monkeypatch.setitem(sys.modules, "rich.progress", rich_progress)
    sys.modules.pop("open_pit_wall.cli_menu", None)
    return importlib.import_module("open_pit_wall.cli_menu")


def test_prompt_choice_re_raises_keyboard_interrupt(monkeypatch):
    cli_menu = _load_cli_menu_with_stubs(monkeypatch)
    interrupted = []

    monkeypatch.setattr(cli_menu, "select", lambda *args, **kwargs: _PromptStub())
    monkeypatch.setattr(
        cli_menu._thread, "interrupt_main", lambda: interrupted.append(True)
    )

    with pytest.raises(KeyboardInterrupt):
        cli_menu._prompt_choice("Choose", [{"label": "Option", "value": "option"}])

    assert interrupted == [True]


def test_prompt_float_re_raises_keyboard_interrupt(monkeypatch):
    cli_menu = _load_cli_menu_with_stubs(monkeypatch)
    interrupted = []

    monkeypatch.setattr(cli_menu, "text", lambda *args, **kwargs: _PromptStub())
    monkeypatch.setattr(
        cli_menu._thread, "interrupt_main", lambda: interrupted.append(True)
    )

    with pytest.raises(KeyboardInterrupt):
        cli_menu._prompt_float("Speed", default=1.0)

    assert interrupted == [True]


def test_broadcaster_main_exits_with_ctrl_c(monkeypatch, capsys):
    def fake_run(coroutine):
        coroutine.close()
        raise KeyboardInterrupt

    monkeypatch.setattr(telemetry_broadcaster.asyncio, "run", fake_run)

    with pytest.raises(SystemExit) as excinfo:
        telemetry_broadcaster.main([])

    assert excinfo.value.code == 130
    assert "Telemetry broadcaster stopped." in capsys.readouterr().out
