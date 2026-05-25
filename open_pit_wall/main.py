import argparse
import sys

def _build_parser():
    parser = argparse.ArgumentParser(
        prog="open-pit-wall",
        description=(
            "Replay cached Formula 1 telemetry through an interactive terminal UI "
            "or by running the standalone WebSocket broadcaster."
        ),
        epilog=(
            "Examples:\n"
            "  open-pit-wall\n"
            "  open-pit-wall replay --help\n"
            "  open-pit-wall replay --data-file /path/to/session.json"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "command",
        nargs="?",
        choices=("interactive",),
        default="interactive",
        help="Launch the interactive session picker (default).",
    )
    return parser


def main(argv=None):
    args = list(sys.argv[1:] if argv is None else argv)
    if args and args[0] == "replay":
        from open_pit_wall.telemetry_broadcaster import (
            main as run_telemetry_broadcaster,
        )

        run_telemetry_broadcaster(args[1:])
        return

    parser = _build_parser()
    parser.parse_args(args)
    from open_pit_wall.cli_menu import run_cli_menu

    run_cli_menu()
    raise SystemExit(0)
