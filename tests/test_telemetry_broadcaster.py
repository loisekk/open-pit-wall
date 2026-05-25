import asyncio
import json
from pathlib import Path

import pytest

from open_pit_wall.telemetry_broadcaster import (
    ClientState,
    ReplaySession,
    TelemetryBroadcaster,
    load_replay_session,
)


def make_replay_session(*, race_control_messages):
    return ReplaySession(
        path=Path("replay.json"),
        schema_version=2,
        replay_start_utc="2024-01-01T12:00:00Z",
        frames=[
            {
                "t": 0.0,
                "lap": 1,
                "timestamp": "2024-01-01T12:00:00Z",
                "event": "telemetry.drivers",
                "payload": [],
            },
            {
                "t": 1.0,
                "lap": 1,
                "timestamp": "2024-01-01T12:00:01Z",
                "event": "telemetry.drivers",
                "payload": [],
            },
        ],
        driver_colors={},
        race_control_messages=race_control_messages,
        total_laps=1,
        max_tyre_life={},
    )


def test_logs_race_control_messages_due_before_replay_output(capsys):
    replay_session = make_replay_session(
        race_control_messages=[
            {
                "timestamp": "2024-01-01T12:00:00Z",
                "event": "race_control",
                "payload": {
                    "message": "TRACK CLEAR",
                    "flag": "GREEN",
                    "scope": "Track",
                    "sector": 0,
                    "current_lap": 1,
                },
            },
            {
                "timestamp": "2024-01-01T12:00:05Z",
                "event": "race_control",
                "payload": {
                    "message": "SAFETY CAR DEPLOYED",
                    "flag": "YELLOW",
                    "scope": "Track",
                    "sector": 0,
                    "current_lap": 1,
                },
            },
        ]
    )
    broadcaster = TelemetryBroadcaster(replay_session, enable_terminal_controls=False)

    broadcaster._log_due_race_control_messages(0.0)

    captured = capsys.readouterr()
    assert "Race control messages queued before replay output:" in captured.out
    assert "TRACK CLEAR | flag=GREEN | scope=Track | lap=1" in captured.out
    assert "SAFETY CAR DEPLOYED" not in captured.out


def test_formats_race_control_console_message_with_optional_fields():
    formatted = TelemetryBroadcaster._format_race_control_console_message(
        {
            "timestamp": "2024-01-01T12:00:00Z",
            "payload": {
                "message": "YELLOW FLAG",
                "flag": "YELLOW",
                "scope": "Sector",
                "sector": 2,
                "driver_number": "16",
                "current_lap": 4,
            },
        }
    )

    assert (
        formatted
        == "2024-01-01T12:00:00Z | YELLOW FLAG | flag=YELLOW | scope=Sector | "
        "sector=2 | driver=16 | lap=4"
    )


def test_formats_runtime_status_with_connections_and_subscriptions():
    replay_session = make_replay_session(race_control_messages=[])
    broadcaster = TelemetryBroadcaster(replay_session, enable_terminal_controls=False)
    broadcaster._clients = {
        object(): ClientState(
            client_id=1, subscriptions={"leaderboard", "telemetry.drivers"}
        ),
        object(): ClientState(client_id=2, subscriptions={"telemetry.weather"}),
    }
    broadcaster._replay_status = "started"
    broadcaster._paused = False
    broadcaster._set_last_broadcast_frame(replay_session.frames[1])
    broadcaster._refresh_runtime_status_snapshot()

    formatted = broadcaster._format_runtime_status_line()

    assert "connections=2" in formatted
    assert "active=2" in formatted
    assert "c1=leaderboard,telemetry.drivers" in formatted
    assert "c2=telemetry.weather" in formatted
    assert "timestamp=2024-01-01T12:00:01Z" in formatted


def test_status_message_includes_connection_count_and_timestamp():
    replay_session = make_replay_session(race_control_messages=[])
    broadcaster = TelemetryBroadcaster(replay_session, enable_terminal_controls=False)
    websocket = object()
    broadcaster._clients = {
        websocket: ClientState(client_id=1, subscriptions={"leaderboard"})
    }

    status_message = asyncio.run(broadcaster._status_message(websocket))

    assert status_message["current_timestamp"] == "2024-01-01T12:00:00Z"
    assert status_message["total_connections"] == 1
    assert status_message["active_subscribers"] == 1
    assert status_message["subscriptions"] == ["leaderboard"]


def test_broadcasts_race_control_messages_to_race_control_channel():
    replay_session = make_replay_session(
        race_control_messages=[
            {
                "timestamp": "2024-01-01T12:00:00Z",
                "event": "race_control",
                "payload": {"message": "TRACK CLEAR"},
            }
        ]
    )
    broadcaster = TelemetryBroadcaster(replay_session, enable_terminal_controls=False)
    broadcast_calls = []

    async def capture_broadcast(channel, message):
        broadcast_calls.append((channel, message))

    broadcaster._broadcast_channel_message = capture_broadcast

    asyncio.run(broadcaster._broadcast_due_race_control_messages(0.0))

    assert broadcast_calls == [("race_control", replay_session.race_control_messages[0])]


def test_load_replay_session_reads_json_file(tmp_path):
    replay_path = tmp_path / "replay.json"
    replay_path.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "replay_start_utc": "2024-01-01T12:00:00Z",
                "frames": [
                    {
                        "t": 0.0,
                        "lap": 1,
                        "timestamp": "2024-01-01T12:00:00Z",
                        "event": "telemetry.drivers",
                        "payload": [],
                    }
                ],
                "driver_colors": {},
                "race_control_messages": [],
                "total_laps": 1,
                "max_tyre_life": {"1": 20},
            }
        ),
        encoding="utf-8",
    )

    replay_session = load_replay_session(replay_path)

    assert replay_session.path == replay_path.resolve()
    assert replay_session.total_frames == 1


def test_load_replay_session_rejects_pickle_files(tmp_path):
    replay_path = tmp_path / "replay.pkl"
    replay_path.write_bytes(b"legacy pickle")

    with pytest.raises(ValueError, match="Legacy pickle replay files"):
        load_replay_session(replay_path)
