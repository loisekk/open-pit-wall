"""CLI menu flow for selecting, downloading, and loading race telemetry."""

import _thread

from questionary import Choice, Style, select, text
from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn

from open_pit_wall.data_loader import (
    download_race_session,
    find_session_cache_path,
    get_available_years,
    get_computed_data_directory,
    get_race_weekends_by_year,
    get_session_options_for_weekend,
    is_session_data_downloaded,
)
from open_pit_wall.telemetry_broadcaster import main as run_telemetry_broadcaster

BACK = "__back__"
EXIT = "__exit__"
DEFAULT_REPLAY_SPEED = 1.0
CONSOLE = Console()
MENU_STYLE = Style(
    [
        ("pointer", "fg:#e10600 bold"),
        ("selected", "fg:#64eb34 bold"),
        ("highlighted", "fg:#e10600 bold"),
        ("answer", "fg:#64eb34 bold"),
        ("question", "bold"),
    ]
)


def _render_banner():
    CONSOLE.print(Markdown("# Open Pit Wall 🏎️"))
    CONSOLE.print(
        Panel.fit(
            "[bold white]Race replay loader[/bold white]\n"
            f"[green]Saved race data:[/green] {get_computed_data_directory()}",
            border_style="red",
        )
    )


def _render_context(title, body=None):
    if body:
        CONSOLE.print(Panel(body, title=title, border_style="red"))
    else:
        CONSOLE.print(f"\n[bold red]{title}[/bold red]")


def _prompt_choice(title, options, allow_back=False, allow_exit=True):
    """Prompt for an option using a styled interactive selector."""
    choices = [Choice(title=option["label"], value=option["value"]) for option in options]
    if allow_back:
        choices.append(Choice(title="← Back", value=BACK))
    if allow_exit:
        choices.append(Choice(title="✕ Exit", value=EXIT))

    try:
        answer = select(
            title,
            choices=choices,
            qmark="🏁",
            style=MENU_STYLE,
        ).ask()
    except EOFError:
        return EXIT
    except KeyboardInterrupt:
        _thread.interrupt_main()
        raise
    return EXIT if answer is None else answer


def _prompt_float(prompt, *, minimum=None, default=None):
    """Prompt for a numeric float value."""
    while True:
        try:
            raw_value = text(
                prompt,
                qmark="⚡",
                default="" if default is None else str(default),
                style=MENU_STYLE,
            ).ask()
        except EOFError:
            return default
        except KeyboardInterrupt:
            _thread.interrupt_main()
            raise
        if raw_value is None:
            return default

        raw_value = raw_value.strip()
        if not raw_value and default is not None:
            return default

        try:
            value = float(raw_value)
        except ValueError:
            CONSOLE.print("[bold red]Invalid value. Please enter a number.[/bold red]")
            continue

        if minimum is not None and value < minimum:
            CONSOLE.print(
                f"[bold red]Value must be at least {minimum}.[/bold red]"
            )
            continue

        return value


def _build_year_options():
    return [{"label": str(year), "value": year} for year in get_available_years()]


def _build_weekend_options(weekends):
    options = []
    for weekend in weekends:
        has_sprint_session = any(
            session["code"] == "S" for session in get_session_options_for_weekend(weekend)
        )
        weekend_type = "Sprint weekend" if has_sprint_session else "Race weekend"
        label = (
            f"Round {weekend['round_number']:02d} - {weekend['event_name']} "
            f"({weekend['date']}, {weekend_type})"
        )
        options.append({"label": label, "value": weekend})
    return options


def _build_session_options(year, weekend):
    options = []
    for session in get_session_options_for_weekend(weekend):
        downloaded = is_session_data_downloaded(
            year, weekend["round_number"], weekend["event_name"], session["code"]
        )
        status = "Downloaded" if downloaded else "Not downloaded"
        options.append(
            {
                "label": f"{session['label']} [{status}]",
                "value": session,
            }
        )
    return options


def _choose_year():
    return _prompt_choice("Choose a race year", _build_year_options(), allow_exit=True)


def _choose_weekend(year):
    with Progress(
        SpinnerColumn(style="bold red"),
        TextColumn("[bold]Loading race weekends…"),
        console=CONSOLE,
        transient=True,
    ) as progress:
        progress.add_task("load", total=None)
        weekends = get_race_weekends_by_year(year)
    if not weekends:
        CONSOLE.print(
            f"[bold red]No race weekends were found for {year}.[/bold red]"
        )
        return BACK

    return _prompt_choice(
        f"Choose a race weekend for {year}",
        _build_weekend_options(weekends),
        allow_back=True,
        allow_exit=True,
    )


def _choose_session(year, weekend):
    return _prompt_choice(
        f"Choose a session for {weekend['event_name']} ({year})",
        _build_session_options(year, weekend),
        allow_back=True,
        allow_exit=True,
    )


def _session_action_menu(year, weekend, session, replay_speed):
    session_downloaded = is_session_data_downloaded(
        year, weekend["round_number"], weekend["event_name"], session["code"]
    )
    cache_path = find_session_cache_path(
        year, weekend["round_number"], weekend["event_name"], session["code"]
    )

    status_line = "Downloaded" if session_downloaded else "Not downloaded"
    if cache_path is not None:
        status_line = f"{status_line} - {cache_path}"

    options = []
    if session_downloaded:
        options.append({"label": "Play saved data", "value": "play"})
        options.append({"label": "Re-download saved data", "value": "redownload"})
    else:
        options.append({"label": "Download this session", "value": "download"})
    options.append(
        {
            "label": f"Set replay speed (currently {replay_speed:.2f}x)",
            "value": "set_speed",
        }
    )

    options.append({"label": "Back to session selection", "value": BACK})
    options.append({"label": "Exit", "value": EXIT})

    _render_context(
        f"{weekend['event_name']} • {session['label']}",
        (
            f"[green]Status:[/green] {status_line}\n"
            f"[green]Replay speed:[/green] {replay_speed:.2f}x\n"
            f"[green]Saved data directory:[/green] {get_computed_data_directory()}"
        ),
    )
    return _prompt_choice("Choose an action", options, allow_exit=False)


def _download_session(year, weekend, session, force_refresh=False):
    CONSOLE.print()
    CONSOLE.print(
        f"[bold red]Downloading[/bold red] {session['label']} for "
        f"{weekend['event_name']} {year}. This may take a while."
    )
    download_race_session(
        year,
        weekend["round_number"],
        weekend["event_name"],
        session["code"],
        force_refresh=force_refresh,
    )
    CONSOLE.print(
        "[bold green]Download complete.[/bold green] Returning to the session selection menu."
    )


def _play_selected_session(year, weekend, session, replay_speed):
    cache_path = find_session_cache_path(
        year, weekend["round_number"], weekend["event_name"], session["code"]
    )
    if cache_path is None:
        raise FileNotFoundError(
            f"No saved data exists for {weekend['event_name']} ({session['label']})."
        )

    CONSOLE.print()
    CONSOLE.print(
        Panel.fit(
            f"[bold white]{weekend['event_name']} — {session['label']}[/bold white]\n"
            f"[green]Replay file:[/green] {cache_path}\n"
            f"[green]Replay speed:[/green] {replay_speed:.2f}x\n"
            "[yellow]The broadcaster starts paused so you can connect a WebSocket client first.[/yellow]\n"
            "[cyan]Replay prompt commands:[/cyan] play, pause, ff, rw, restart, "
            "speed <value>, faster, slower, status, help, quit",
            border_style="red",
        )
    )
    run_telemetry_broadcaster(
        ["--data-file", str(cache_path), "--speed", str(replay_speed)]
    )
    CONSOLE.print("[bold green]Replay stopped.[/bold green] Returning to the menu.")


def run_cli_menu():
    """Run the interactive menu and handle replay launch actions."""
    try:
        _render_banner()
        replay_speed = DEFAULT_REPLAY_SPEED

        while True:
            selected_year = _choose_year()
            if selected_year == EXIT:
                return None

            while True:
                selected_weekend = _choose_weekend(selected_year)
                if selected_weekend == EXIT:
                    return None
                if selected_weekend == BACK:
                    break

                while True:
                    selected_session = _choose_session(selected_year, selected_weekend)
                    if selected_session == EXIT:
                        return None
                    if selected_session == BACK:
                        break

                    action = _session_action_menu(
                        selected_year, selected_weekend, selected_session, replay_speed
                    )
                    if action == EXIT:
                        return None
                    if action == BACK:
                        continue
                    if action == "set_speed":
                        replay_speed = _prompt_float(
                            "Enter replay speed multiplier (for example 0.5, 1, 2): ",
                            minimum=0.1,
                            default=replay_speed,
                        )
                        CONSOLE.print(
                            f"[bold green]Replay speed set to {replay_speed:.2f}x.[/bold green]"
                        )
                        continue
                    if action == "download":
                        _download_session(selected_year, selected_weekend, selected_session)
                        continue
                    if action == "redownload":
                        _download_session(
                            selected_year,
                            selected_weekend,
                            selected_session,
                            force_refresh=True,
                        )
                        continue
                    if action == "play":
                        _play_selected_session(
                            selected_year,
                            selected_weekend,
                            selected_session,
                            replay_speed,
                        )
                        continue
    except (KeyboardInterrupt, EOFError):
        CONSOLE.print("\n[bold yellow]Exiting Open Pit Wall.[/bold yellow]")
        return None
