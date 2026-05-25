"""WebSocket telemetry replay service for cached race data."""

from __future__ import annotations

import _thread
import argparse
import asyncio
from bisect import bisect_left
from datetime import datetime
import json
from dataclasses import dataclass, field
from pathlib import Path
from threading import Event as ThreadEvent, Lock, Thread
from typing import Any

import numpy as np
from websockets.asyncio.server import ServerConnection, serve
from websockets.exceptions import ConnectionClosed

from open_pit_wall.data_loader import get_computed_data_directory

DEFAULT_DATA_DIRECTORY = get_computed_data_directory()
DEFAULT_REPLAY_FILE = DEFAULT_DATA_DIRECTORY / "replay.json"

CHANNELS = {
    "telemetry.drivers": "Broadcast per-driver telemetry for each frame.",
    "leaderboard": "Broadcast leaderboard telemetry for each frame.",
    "race_control": "Broadcast merged race control and track status messages.",
    "telemetry.weather": "Broadcast weather snapshots when present on a frame.",
    "telemetry.lap": "Broadcast the leader lap and replay timestamp for each frame.",
}
DEFAULT_CHANNEL = "telemetry.drivers"
DRIVER_CHANNEL_PREFIX = "telemetry.drivers."
DEFAULT_SEEK_SECONDS = 10.0
SPEED_STEP = 0.5
REPLAY_SCHEMA_VERSION = 2


@dataclass(slots=True)
class ReplaySession:
    """Loaded replay data and derived summary fields."""

    path: Path
    schema_version: int
    replay_start_utc: str
    frames: list[dict[str, Any]]
    driver_colors: dict[str, Any]
    race_control_messages: list[dict[str, Any]]
    total_laps: int
    max_tyre_life: dict[str, Any]
    duration_seconds: float = field(init=False)
    driver_codes: list[str] = field(init=False)
    race_control_schedule: list[tuple[float, dict[str, Any]]] = field(init=False)

    def __post_init__(self) -> None:
        self.duration_seconds = float(self.frames[-1]["t"]) if self.frames else 0.0
        first_frame = self.frames[0] if self.frames else {}
        first_payload = first_frame.get("payload", [])
        self.driver_codes = sorted(
            payload["driver_code"]
            for payload in first_payload
            if isinstance(payload, dict) and "driver_code" in payload
        )
        replay_start = _parse_timestamp(self.replay_start_utc)
        self.race_control_schedule = []
        for message in self.race_control_messages:
            timestamp = _parse_timestamp(message.get("timestamp", ""))
            if replay_start is None or timestamp is None:
                continue
            elapsed_seconds = max((timestamp - replay_start).total_seconds(), 0.0)
            self.race_control_schedule.append((elapsed_seconds, message))
        self.race_control_schedule.sort(key=lambda item: item[0])

    @property
    def total_frames(self) -> int:
        return len(self.frames)


def make_json_safe(value: Any) -> Any:
    """Convert numpy-backed replay payloads into JSON-safe Python values."""

    if isinstance(value, dict):
        return {str(key): make_json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [make_json_safe(item) for item in value]
    if isinstance(value, np.ndarray):
        return [make_json_safe(item) for item in value.tolist()]
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, Path):
        return str(value)
    return value


def _parse_timestamp(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def load_replay_session(data_file: Path) -> ReplaySession:
    """Load a cached telemetry JSON file and validate its structure."""

    resolved_path = data_file.expanduser().resolve()
    if not resolved_path.exists():
        raise FileNotFoundError(f"Replay file does not exist: {resolved_path}")
    if resolved_path.suffix == ".pkl":
        raise ValueError(
            "Legacy pickle replay files are no longer supported. Re-download the "
            f"session to regenerate it as JSON: {resolved_path}"
        )

    with resolved_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    if not isinstance(payload, dict):
        raise TypeError(f"Replay file must contain a dict payload: {resolved_path}")

    schema_version = int(payload.get("schema_version", 0))
    if schema_version != REPLAY_SCHEMA_VERSION:
        raise ValueError(
            "Replay file schema is incompatible. Re-download the session data "
            f"to regenerate it with schema version {REPLAY_SCHEMA_VERSION}: {resolved_path}"
        )

    frames = payload.get("frames")
    if not isinstance(frames, list) or not frames:
        raise ValueError(f"Replay file does not contain any frames: {resolved_path}")

    return ReplaySession(
        path=resolved_path,
        schema_version=schema_version,
        replay_start_utc=str(payload.get("replay_start_utc", "")),
        frames=frames,
        driver_colors=payload.get("driver_colors", {}),
        race_control_messages=payload.get("race_control_messages", []),
        total_laps=int(payload.get("total_laps", 0)),
        max_tyre_life=payload.get("max_tyre_life", {}),
    )


@dataclass(slots=True)
class ClientState:
    """Per-connection subscription state."""

    client_id: int
    subscriptions: set[str] = field(default_factory=set)


@dataclass(slots=True)
class RuntimeStatusSnapshot:
    """Thread-safe snapshot of broadcaster runtime state for the terminal UI."""

    replay_status: str
    paused: bool
    frame_index: int
    total_frames: int
    replay_speed: float
    total_connections: int
    active_subscribers: int
    current_time: float
    current_lap: int
    current_timestamp: str
    connection_subscriptions: tuple[tuple[int, tuple[str, ...]], ...]


class TelemetryBroadcaster:
    """Replay cached telemetry over a shared WebSocket feed."""

    def __init__(
        self,
        replay_session: ReplaySession,
        *,
        host: str = "127.0.0.1",
        port: int = 8765,
        replay_speed: float = 1.0,
        loop_forever: bool = False,
        autoplay: bool = False,
        seek_seconds: float = DEFAULT_SEEK_SECONDS,
        enable_terminal_controls: bool = True,
    ) -> None:
        if replay_speed <= 0:
            raise ValueError("Replay speed must be greater than zero.")
        if seek_seconds <= 0:
            raise ValueError("Seek seconds must be greater than zero.")

        self.replay_session = replay_session
        self.host = host
        self.port = port
        self.replay_speed = replay_speed
        self.loop_forever = loop_forever
        self.autoplay = autoplay
        self.seek_seconds = seek_seconds
        self.enable_terminal_controls = enable_terminal_controls

        self._clients: dict[ServerConnection, ClientState] = {}
        self._clients_changed = asyncio.Condition()
        self._frame_index = 0
        self._race_control_index = 0
        self._replay_status = "ready"
        self._replay_task: asyncio.Task[None] | None = None
        self._frame_times = [float(frame["t"]) for frame in self.replay_session.frames]
        self._race_control_times = [
            elapsed for elapsed, _ in self.replay_session.race_control_schedule
        ]
        self._paused = not autoplay
        self._control_changed: asyncio.Event | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._stop_future: asyncio.Future[None] | None = None
        self._terminal_shutdown = ThreadEvent()
        self._terminal_thread: Thread | None = None
        self._next_client_id = 1
        self._status_lock = Lock()
        self._last_broadcast_time = 0.0
        self._last_broadcast_lap = 0
        self._last_broadcast_timestamp = ""
        self._set_last_broadcast_frame(self.replay_session.frames[self._current_frame_index()])
        self._runtime_status = self._build_runtime_status_snapshot()

    async def run(self) -> None:
        """Start the WebSocket server and shared replay loop."""

        self._loop = asyncio.get_running_loop()
        self._control_changed = asyncio.Event()
        self._stop_future = self._loop.create_future()
        self._replay_task = asyncio.create_task(self._run_replay_loop(), name="replay-loop")
        if self.enable_terminal_controls:
            self._start_terminal_controls()
        try:
            async with serve(
                self._handle_client,
                self.host,
                self.port,
                ping_interval=20,
                ping_timeout=20,
                max_size=None,
            ):
                print(
                    "Telemetry broadcaster listening on "
                    f"ws://{self.host}:{self.port} using {self.replay_session.path}"
                )
                if self.enable_terminal_controls:
                    self._print_terminal_controls()
                if self._paused:
                    print("Replay is paused. Connect a client, then use 'play' to begin.")
                await self._stop_future
        finally:
            self._terminal_shutdown.set()
            if self._replay_task is not None:
                self._replay_task.cancel()
                try:
                    await self._replay_task
                except asyncio.CancelledError:
                    pass

    async def _handle_client(self, websocket: ServerConnection) -> None:
        await self._register_client(websocket)
        try:
            await websocket.send(self._serialize_message(self._welcome_message()))
            async for raw_message in websocket:
                await self._handle_client_message(websocket, raw_message)
        except ConnectionClosed:
            pass
        finally:
            await self._remove_client(websocket)

    async def _register_client(self, websocket: ServerConnection) -> None:
        async with self._clients_changed:
            self._clients[websocket] = ClientState(client_id=self._next_client_id)
            self._next_client_id += 1
            self._refresh_runtime_status_snapshot_locked()
        self._notify_control_changed()

    async def _remove_client(self, websocket: ServerConnection) -> None:
        async with self._clients_changed:
            removed = self._clients.pop(websocket, None)
            if removed and removed.subscriptions:
                self._clients_changed.notify_all()
            self._refresh_runtime_status_snapshot_locked()
        self._notify_control_changed()

    async def _handle_client_message(
        self, websocket: ServerConnection, raw_message: str
    ) -> None:
        try:
            message = json.loads(raw_message)
        except json.JSONDecodeError:
            await self._send_error(websocket, "Messages must be valid JSON.")
            return

        if not isinstance(message, dict):
            await self._send_error(websocket, "Messages must be JSON objects.")
            return

        action = str(message.get("action", "")).strip().lower()
        if action == "subscribe":
            await self._subscribe_client(websocket, message)
            return
        if action == "unsubscribe":
            await self._unsubscribe_client(websocket, message)
            return
        if action == "list_channels":
            await websocket.send(self._serialize_message(self._channels_message()))
            return
        if action == "status":
            await websocket.send(self._serialize_message(await self._status_message(websocket)))
            return

        await self._send_error(
            websocket,
            "Unsupported action. Use subscribe, unsubscribe, list_channels, or status.",
        )

    async def _subscribe_client(
        self, websocket: ServerConnection, message: dict[str, Any]
    ) -> None:
        requested_channels = self._coerce_channels(message.get("channels"))
        if not requested_channels:
            requested_channels = [DEFAULT_CHANNEL]

        invalid_channels = sorted(
            channel for channel in requested_channels if not self._is_valid_channel(channel)
        )
        if invalid_channels:
            await self._send_error(
                websocket,
                "Unknown channel(s): " + ", ".join(invalid_channels),
            )
            return

        async with self._clients_changed:
            client_state = self._clients.get(websocket)
            if client_state is None:
                return

            was_inactive = not self._has_active_subscribers_locked()
            client_state.subscriptions.update(requested_channels)

            if was_inactive and self._frame_index >= self.replay_session.total_frames:
                self._frame_index = 0
                self._race_control_index = 0

            self._clients_changed.notify_all()
            self._refresh_runtime_status_snapshot_locked()
        self._notify_control_changed()

        await websocket.send(
            self._serialize_message(
                {
                    "type": "subscribed",
                    "channels": sorted(self._clients[websocket].subscriptions),
                }
            )
        )

    async def _unsubscribe_client(
        self, websocket: ServerConnection, message: dict[str, Any]
    ) -> None:
        requested_channels = self._coerce_channels(message.get("channels"))
        async with self._clients_changed:
            client_state = self._clients.get(websocket)
            if client_state is None:
                return

            if not requested_channels:
                client_state.subscriptions.clear()
            else:
                client_state.subscriptions.difference_update(requested_channels)

            self._clients_changed.notify_all()
            subscriptions = sorted(client_state.subscriptions)
            self._refresh_runtime_status_snapshot_locked()

        self._notify_control_changed()
        await websocket.send(
            self._serialize_message({"type": "unsubscribed", "channels": subscriptions})
        )

    async def _run_replay_loop(self) -> None:
        while True:
            await self._wait_until_ready()
            status = "started" if self._frame_index == 0 else "resumed"
            await self._set_replay_status(status)
            self._log_due_race_control_messages(self._current_time_seconds())

            while self._frame_index < self.replay_session.total_frames:
                if self._paused:
                    await self._set_replay_status("paused")
                    break
                if not await self._has_active_subscribers():
                    await self._set_replay_status("waiting")
                    break

                current_index = self._frame_index
                frame = self.replay_session.frames[current_index]
                await self._broadcast_due_race_control_messages(float(frame["t"]))
                await self._broadcast_frame(current_index, frame)

                if current_index >= self.replay_session.total_frames - 1:
                    self._frame_index = self.replay_session.total_frames
                    await self._set_replay_status("completed")
                    if self.loop_forever:
                        self._frame_index = 0
                        self._race_control_index = 0
                        await self._set_replay_status("looping")
                    else:
                        self._paused = True
                    break

                current_time = float(frame["t"])
                next_time = float(self.replay_session.frames[current_index + 1]["t"])
                self._frame_index = current_index + 1

                frame_delay = max((next_time - current_time) / self.replay_speed, 0.0)
                await self._wait_for_control_change(frame_delay)

    async def _wait_until_ready(self) -> None:
        while True:
            if self.loop_forever and self._frame_index >= self.replay_session.total_frames:
                self._frame_index = 0

            if (
                self._frame_index < self.replay_session.total_frames
                and not self._paused
                and await self._has_active_subscribers()
            ):
                return

            await self._wait_for_control_change()

    async def _wait_for_control_change(self, timeout: float | None = None) -> bool:
        if self._control_changed is None:
            if timeout:
                await asyncio.sleep(timeout)
            return False

        try:
            if timeout is None:
                await self._control_changed.wait()
                return True
            await asyncio.wait_for(self._control_changed.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False
        finally:
            self._control_changed.clear()

    async def _has_active_subscribers(self) -> bool:
        async with self._clients_changed:
            return self._has_active_subscribers_locked()

    def _has_active_subscribers_locked(self) -> bool:
        return any(client.subscriptions for client in self._clients.values())

    async def _set_replay_status(self, status: str, **extra: Any) -> None:
        self._replay_status = status
        self._refresh_runtime_status_snapshot()
        await self._broadcast_control_message(
            {
                "type": "replay_status",
                "status": status,
                "frame_index": self._frame_index,
                "current_time": self._current_time_seconds(),
                "replay_speed": self.replay_speed,
                **extra,
            }
        )

    def _notify_control_changed(self) -> None:
        if self._control_changed is not None:
            self._control_changed.set()

    def _current_frame_index(self) -> int:
        if self.replay_session.total_frames <= 0:
            return 0
        return min(max(self._frame_index, 0), self.replay_session.total_frames - 1)

    def _current_time_seconds(self) -> float:
        if not self.replay_session.frames:
            return 0.0
        return float(self.replay_session.frames[self._current_frame_index()]["t"])

    def _sync_race_control_index(self) -> None:
        current_time = self._current_time_seconds()
        self._race_control_index = bisect_left(self._race_control_times, current_time)

    def _seek_to_time(self, target_time: float) -> int:
        clamped_time = min(max(target_time, 0.0), self.replay_session.duration_seconds)
        return min(
            bisect_left(self._frame_times, clamped_time),
            self.replay_session.total_frames - 1,
        )

    def _schedule_status_broadcast(self, **extra: Any) -> None:
        if self._loop is None:
            return
        asyncio.create_task(
            self._set_replay_status(
                self._replay_status,
                **extra,
            )
        )

    async def _broadcast_due_race_control_messages(self, current_time: float) -> None:
        while self._race_control_index < len(self.replay_session.race_control_schedule):
            scheduled_time, message = self.replay_session.race_control_schedule[
                self._race_control_index
            ]
            if scheduled_time > current_time:
                break
            await self._broadcast_channel_message("race_control", message)
            self._race_control_index += 1

    def _log_due_race_control_messages(self, current_time: float) -> None:
        pending_messages = []
        next_index = self._race_control_index
        while next_index < len(self.replay_session.race_control_schedule):
            scheduled_time, message = self.replay_session.race_control_schedule[next_index]
            if scheduled_time > current_time:
                break
            pending_messages.append(message)
            next_index += 1

        if not pending_messages:
            return

        print("Race control messages queued before replay output:")
        for message in pending_messages:
            print(f"  {self._format_race_control_console_message(message)}")

    @staticmethod
    def _format_race_control_console_message(message: dict[str, Any]) -> str:
        payload = message.get("payload", {})
        timestamp = str(message.get("timestamp", "")).strip()
        message_text = str(payload.get("message", "")).strip() or "RACE CONTROL UPDATE"
        details = [message_text]

        flag = str(payload.get("flag", "")).strip()
        if flag:
            details.append(f"flag={flag}")

        scope = str(payload.get("scope", "")).strip()
        if scope:
            details.append(f"scope={scope}")

        sector = payload.get("sector")
        if isinstance(sector, (int, float)) and int(sector) > 0:
            details.append(f"sector={int(sector)}")

        driver_number = str(payload.get("driver_number", "")).strip()
        if driver_number:
            details.append(f"driver={driver_number}")

        current_lap = payload.get("current_lap")
        if isinstance(current_lap, (int, float)) and int(current_lap) > 0:
            details.append(f"lap={int(current_lap)}")

        detail_text = " | ".join(details)
        return f"{timestamp} | {detail_text}" if timestamp else detail_text

    def _start_terminal_controls(self) -> None:
        self._terminal_thread = Thread(
            target=self._terminal_control_loop,
            name="telemetry-broadcaster-controls",
            daemon=True,
        )
        self._terminal_thread.start()

    def _print_terminal_controls(self) -> None:
        print(
            "Replay controls: play, pause, ff, rw, restart, speed <value>, "
            "faster, slower, status, help, quit"
        )

    def _terminal_control_loop(self) -> None:
        while not self._terminal_shutdown.is_set():
            try:
                raw_command = input("replay> ")
            except EOFError:
                return
            except KeyboardInterrupt:
                self._terminal_shutdown.set()
                _thread.interrupt_main()
                return

            command = raw_command.strip().lower()
            if not command:
                continue
            if self._loop is None:
                continue

            self._loop.call_soon_threadsafe(self._apply_terminal_command, command)
            if command in {"q", "quit", "exit"}:
                return

    def _apply_terminal_command(self, command: str) -> None:
        normalized = command.strip().lower()
        if normalized.startswith("speed "):
            self._set_speed_from_command(command)
            return
        if normalized in {"p", "play", "resume"}:
            self._resume_replay()
            return
        if normalized in {"pause", "hold"}:
            self._pause_replay()
            return
        if normalized in {"faster", "speed+", "speedup"}:
            self._change_speed(SPEED_STEP)
            return
        if normalized in {"slower", "speed-", "slowdown"}:
            self._change_speed(-SPEED_STEP)
            return
        if normalized in {"ff", "fast", "fast-forward", "fastforward"}:
            self._seek_relative(self.seek_seconds, action="fast_forward")
            return
        if normalized in {"rw", "rew", "rewind"}:
            self._seek_relative(-self.seek_seconds, action="rewind")
            return
        if normalized in {"restart", "startover"}:
            self._restart_replay()
            return
        if normalized in {"status", "info"}:
            self._print_runtime_status()
            return
        if normalized in {"help", "h", "?"}:
            self._print_terminal_controls()
            return
        if normalized in {"q", "quit", "exit"}:
            self._stop_replay()
            return

        print(f"Unknown replay command: {command}")
        self._print_terminal_controls()

    def _set_speed_from_command(self, command: str) -> None:
        _, _, raw_speed = command.partition(" ")
        try:
            new_speed = float(raw_speed.strip())
        except ValueError:
            print(f"Invalid speed value: {raw_speed.strip()}")
            return
        self._set_replay_speed(new_speed)

    def _change_speed(self, delta: float) -> None:
        self._set_replay_speed(self.replay_speed + delta)

    def _set_replay_speed(self, new_speed: float) -> None:
        if new_speed <= 0:
            print("Replay speed must be greater than zero.")
            return

        self.replay_speed = new_speed
        self._refresh_runtime_status_snapshot()
        self._notify_control_changed()
        self._schedule_status_broadcast(action="speed_change")
        print(f"Replay speed set to {self.replay_speed:.2f}x.")

    def _resume_replay(self) -> None:
        if self._frame_index >= self.replay_session.total_frames:
            print("Replay has ended. Use 'restart' before playing again.")
            return
        if not self._paused:
            print("Replay is already playing.")
            return

        self._paused = False
        self._refresh_runtime_status_snapshot()
        print("Replay set to play.")
        if self._loop is not None:
            self._notify_control_changed()

    def _pause_replay(self) -> None:
        if self._paused:
            print("Replay is already paused.")
            return

        self._paused = True
        self._replay_status = "paused"
        self._refresh_runtime_status_snapshot()
        self._notify_control_changed()
        self._schedule_status_broadcast(action="pause")
        print("Replay paused.")

    def _seek_relative(self, delta_seconds: float, *, action: str) -> None:
        current_time = self._current_time_seconds()
        target_index = self._seek_to_time(current_time + delta_seconds)
        self._frame_index = target_index
        self._sync_race_control_index()
        self._set_last_broadcast_frame(self.replay_session.frames[self._current_frame_index()])
        self._refresh_runtime_status_snapshot()
        self._notify_control_changed()
        self._schedule_status_broadcast(action=action)
        direction = "Fast-forwarded" if delta_seconds > 0 else "Rewound"
        print(
            f"{direction} to t={self._current_time_seconds():.1f}s "
            f"(frame {self._current_frame_index() + 1}/{self.replay_session.total_frames})."
        )

    def _restart_replay(self) -> None:
        self._frame_index = 0
        self._race_control_index = 0
        self._set_last_broadcast_frame(self.replay_session.frames[self._current_frame_index()])
        self._refresh_runtime_status_snapshot()
        self._notify_control_changed()
        self._schedule_status_broadcast(action="restart")
        print("Replay reset to the beginning.")

    def _stop_replay(self) -> None:
        self._terminal_shutdown.set()
        self._notify_control_changed()
        if self._stop_future is not None and not self._stop_future.done():
            self._stop_future.set_result(None)
        print("Stopping replay broadcaster.")

    def _print_runtime_status(self) -> None:
        print(self._format_runtime_status_line())

    async def _broadcast_frame(self, frame_index: int, frame: dict[str, Any]) -> None:
        self._set_last_broadcast_frame(frame)
        self._refresh_runtime_status_snapshot()
        async with self._clients_changed:
            active_channels = sorted(
                {
                    subscription
                    for client in self._clients.values()
                    for subscription in client.subscriptions
                }
            )
            channel_targets = {
                channel: [
                    websocket
                    for websocket, client in self._clients.items()
                    if channel in client.subscriptions
                ]
                for channel in active_channels
            }

        for channel, targets in channel_targets.items():
            if not targets:
                continue

            payload = self._build_channel_payload(channel, frame)
            if payload is None:
                continue

            await self._send_to_targets(payload, targets)

    def _build_channel_payload(
        self, channel: str, frame: dict[str, Any]
    ) -> dict[str, Any] | None:
        if channel == "telemetry.drivers":
            return {
                "timestamp": frame.get("timestamp", ""),
                "event": frame.get("event", "telemetry.drivers"),
                "payload": frame.get("payload", []),
            }
        if channel == "leaderboard":
            return self._build_leaderboard_payload(frame)
        if channel == "telemetry.weather":
            weather = frame.get("weather")
            if weather is None:
                return None
            return {
                "timestamp": frame.get("timestamp", ""),
                "event": "telemetry.weather",
                "payload": weather,
            }
        if channel == "telemetry.lap":
            return {
                "timestamp": frame.get("timestamp", ""),
                "event": "telemetry.lap",
                "payload": {"current_lap": frame["lap"], "elapsed_seconds": frame["t"]},
            }
        if channel.startswith(DRIVER_CHANNEL_PREFIX):
            driver_code = channel.removeprefix(DRIVER_CHANNEL_PREFIX)
            driver_data = next(
                (
                    payload
                    for payload in frame.get("payload", [])
                    if payload.get("driver_code") == driver_code
                ),
                None,
            )
            if driver_data is None:
                return None
            return {
                "timestamp": frame.get("timestamp", ""),
                "event": channel,
                "payload": driver_data,
            }
        return None

    @staticmethod
    def _estimate_track_length(drivers: list[dict[str, Any]]) -> float | None:
        estimates = []
        for driver in drivers:
            position = driver.get("position", {})
            if not isinstance(position, dict):
                continue
            lap_distance = position.get("dist_metres_around_track")
            lap_percentage = position.get("dist_percentage_around_track")
            if not isinstance(lap_distance, (int, float)) or not isinstance(
                lap_percentage, (int, float)
            ):
                continue
            if lap_percentage <= 0:
                continue
            estimates.append(float(lap_distance) / float(lap_percentage))
        if not estimates:
            return None
        return float(np.median(estimates))

    @staticmethod
    def _build_leaderboard_payload(frame: dict[str, Any]) -> dict[str, Any]:
        drivers = frame.get("payload", [])
        if not isinstance(drivers, list):
            drivers = []

        track_length = TelemetryBroadcaster._estimate_track_length(drivers)
        leader_distance = 0.0
        if drivers:
            leader_position = drivers[0].get("position", {})
            if isinstance(leader_position, dict):
                leader_distance = float(
                    leader_position.get("dist_metres_around_track", 0.0) or 0.0
                )

        leaderboard_drivers = []
        for index, driver in enumerate(drivers, start=1):
            position = driver.get("position", {})
            if not isinstance(position, dict):
                continue

            driver_distance = float(
                position.get("dist_metres_around_track", 0.0) or 0.0
            )
            gap_to_leader = max(leader_distance - driver_distance, 0.0)
            if track_length is not None and driver_distance > leader_distance:
                gap_to_leader = max(
                    leader_distance + track_length - driver_distance, 0.0
                )

            leaderboard_drivers.append(
                {
                    "position": index,
                    "driver_code": str(driver.get("driver_code", "")),
                    "dist_metres_from_leader": round(gap_to_leader, 3),
                }
            )

        return {
            "timestamp": frame.get("timestamp", ""),
            "event": "leaderboard",
            "payload": {"drivers": leaderboard_drivers},
        }

    async def _broadcast_control_message(self, message: dict[str, Any]) -> None:
        async with self._clients_changed:
            targets = [
                websocket
                for websocket, client in self._clients.items()
                if client.subscriptions
            ]
        if targets:
            await self._send_to_targets(message, targets)

    async def _broadcast_channel_message(
        self, channel: str, message: dict[str, Any]
    ) -> None:
        async with self._clients_changed:
            targets = [
                websocket
                for websocket, client in self._clients.items()
                if channel in client.subscriptions
            ]
        if targets:
            await self._send_to_targets(message, targets)

    async def _send_to_targets(
        self, message: dict[str, Any], targets: list[ServerConnection]
    ) -> None:
        encoded_message = self._serialize_message(message)
        disconnected: list[ServerConnection] = []
        for websocket in targets:
            try:
                await websocket.send(encoded_message)
            except ConnectionClosed:
                disconnected.append(websocket)

        if disconnected:
            async with self._clients_changed:
                for websocket in disconnected:
                    self._clients.pop(websocket, None)
                self._clients_changed.notify_all()
                self._refresh_runtime_status_snapshot_locked()

    async def _send_error(self, websocket: ServerConnection, message: str) -> None:
        try:
            await websocket.send(
                self._serialize_message({"type": "error", "message": message})
            )
        except ConnectionClosed:
            await self._remove_client(websocket)

    def _welcome_message(self) -> dict[str, Any]:
        return {
            "type": "welcome",
            "default_channel": DEFAULT_CHANNEL,
            "channels": CHANNELS,
            "driver_channel_pattern": f"{DRIVER_CHANNEL_PREFIX}{{DRIVER_CODE}}",
            "session": {
                "file": str(self.replay_session.path),
                "total_frames": self.replay_session.total_frames,
                "duration_seconds": self.replay_session.duration_seconds,
                "total_laps": self.replay_session.total_laps,
                "driver_count": len(self.replay_session.driver_codes),
                "driver_codes": self.replay_session.driver_codes,
                "driver_channels": [
                    f"{DRIVER_CHANNEL_PREFIX}{driver_code}"
                    for driver_code in self.replay_session.driver_codes
                ],
                "replay_speed": self.replay_speed,
                "loop_forever": self.loop_forever,
            },
        }

    def _channels_message(self) -> dict[str, Any]:
        return {
            "type": "channels",
            "channels": CHANNELS,
            "default_channel": DEFAULT_CHANNEL,
            "driver_channel_pattern": f"{DRIVER_CHANNEL_PREFIX}{{DRIVER_CODE}}",
            "driver_channels": [
                f"{DRIVER_CHANNEL_PREFIX}{driver_code}"
                for driver_code in self.replay_session.driver_codes
            ],
        }

    async def _status_message(self, websocket: ServerConnection) -> dict[str, Any]:
        async with self._clients_changed:
            client_state = self._clients.get(websocket, ClientState(client_id=0))
            active_subscribers = sum(
                1 for client in self._clients.values() if client.subscriptions
            )
            total_connections = len(self._clients)

        current_index = min(self._frame_index, self.replay_session.total_frames - 1)
        current_frame = self.replay_session.frames[current_index]
        return {
            "type": "status",
            "status": self._replay_status,
            "frame_index": self._frame_index,
            "current_time": float(current_frame["t"]),
            "current_lap": int(current_frame["lap"]),
            "current_timestamp": str(current_frame.get("timestamp", "")),
            "replay_speed": self.replay_speed,
            "total_connections": total_connections,
            "active_subscribers": active_subscribers,
            "subscriptions": sorted(client_state.subscriptions),
        }

    def _set_last_broadcast_frame(self, frame: dict[str, Any]) -> None:
        self._last_broadcast_time = float(frame.get("t", 0.0) or 0.0)
        self._last_broadcast_lap = int(frame.get("lap", 0) or 0)
        self._last_broadcast_timestamp = str(frame.get("timestamp", ""))

    def _build_runtime_status_snapshot(self) -> RuntimeStatusSnapshot:
        connection_subscriptions = tuple(
            sorted(
                (
                    client.client_id,
                    tuple(sorted(client.subscriptions)),
                )
                for client in self._clients.values()
            )
        )
        return RuntimeStatusSnapshot(
            replay_status=self._replay_status,
            paused=self._paused,
            frame_index=self._current_frame_index(),
            total_frames=self.replay_session.total_frames,
            replay_speed=self.replay_speed,
            total_connections=len(self._clients),
            active_subscribers=sum(
                1 for client in self._clients.values() if client.subscriptions
            ),
            current_time=self._last_broadcast_time,
            current_lap=self._last_broadcast_lap,
            current_timestamp=self._last_broadcast_timestamp,
            connection_subscriptions=connection_subscriptions,
        )

    def _refresh_runtime_status_snapshot(self) -> None:
        with self._status_lock:
            self._runtime_status = self._build_runtime_status_snapshot()

    def _refresh_runtime_status_snapshot_locked(self) -> None:
        with self._status_lock:
            self._runtime_status = self._build_runtime_status_snapshot()

    def _get_runtime_status_snapshot(self) -> RuntimeStatusSnapshot:
        with self._status_lock:
            return self._runtime_status

    def _format_connection_subscriptions(
        self, snapshot: RuntimeStatusSnapshot, *, max_width: int = 80
    ) -> str:
        if not snapshot.connection_subscriptions:
            return "none"

        subscriptions = []
        for client_id, channels in snapshot.connection_subscriptions:
            channel_summary = ",".join(channels) if channels else "-"
            subscriptions.append(f"c{client_id}={channel_summary}")

        summary = "; ".join(subscriptions)
        if len(summary) <= max_width:
            return summary
        return summary[: max_width - 3].rstrip() + "..."

    def _format_runtime_status_line(self) -> str:
        snapshot = self._get_runtime_status_snapshot()
        timestamp = snapshot.current_timestamp or f"t={snapshot.current_time:.1f}s"
        return (
            "Replay status: "
            f"{snapshot.replay_status}, paused={snapshot.paused}, "
            f"connections={snapshot.total_connections}, active={snapshot.active_subscribers}, "
            f"subscriptions={self._format_connection_subscriptions(snapshot)}, "
            f"timestamp={timestamp}, lap={snapshot.current_lap}, "
            f"frame={snapshot.frame_index + 1}/{snapshot.total_frames}, "
            f"speed={snapshot.replay_speed:.2f}x"
        )

    def _serialize_message(self, message: dict[str, Any]) -> str:
        return json.dumps(make_json_safe(message), separators=(",", ":"))

    @staticmethod
    def _coerce_channels(raw_channels: Any) -> list[str]:
        if raw_channels is None:
            return []
        if isinstance(raw_channels, str):
            return [raw_channels]
        if isinstance(raw_channels, list):
            return [channel for channel in raw_channels if isinstance(channel, str)]
        return []

    def _is_valid_channel(self, channel: str) -> bool:
        if channel in CHANNELS:
            return True
        if not channel.startswith(DRIVER_CHANNEL_PREFIX):
            return False

        driver_code = channel.removeprefix(DRIVER_CHANNEL_PREFIX)
        return driver_code in self.replay_session.driver_codes


def discover_default_replay_file() -> Path:
    """Resolve the default replay file for the broadcaster CLI."""

    if DEFAULT_REPLAY_FILE.exists():
        return DEFAULT_REPLAY_FILE

    available_files = sorted(DEFAULT_DATA_DIRECTORY.glob("*.json"))
    if available_files:
        return available_files[0]

    return DEFAULT_REPLAY_FILE


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for the broadcaster service."""

    parser = argparse.ArgumentParser(
        prog="open-pit-wall replay",
        description="Replay cached telemetry over a WebSocket feed."
    )
    parser.add_argument(
        "--data-file",
        type=Path,
        default=discover_default_replay_file(),
        help="Path to the cached telemetry .json file to replay.",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host interface for the WebSocket server.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8765,
        help="TCP port for the WebSocket server.",
    )
    parser.add_argument(
        "--speed",
        type=float,
        default=1.0,
        help="Replay speed multiplier. Use 2.0 for double speed, 0.5 for half speed.",
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Restart the replay from the beginning when the final frame is reached.",
    )
    parser.add_argument(
        "--autoplay",
        action="store_true",
        help="Start playback automatically once a subscriber is connected.",
    )
    parser.add_argument(
        "--seek-seconds",
        type=float,
        default=DEFAULT_SEEK_SECONDS,
        help="How many seconds fast forward and rewind should jump per command.",
    )
    return parser.parse_args(argv)


async def run_broadcaster(
    data_file: Path,
    *,
    host: str = "127.0.0.1",
    port: int = 8765,
    replay_speed: float = 1.0,
    loop_forever: bool = False,
    autoplay: bool = False,
    seek_seconds: float = DEFAULT_SEEK_SECONDS,
    enable_terminal_controls: bool = True,
) -> None:
    """Load a replay file and run the broadcaster until interrupted."""

    replay_session = load_replay_session(data_file)
    broadcaster = TelemetryBroadcaster(
        replay_session,
        host=host,
        port=port,
        replay_speed=replay_speed,
        loop_forever=loop_forever,
        autoplay=autoplay,
        seek_seconds=seek_seconds,
        enable_terminal_controls=enable_terminal_controls,
    )
    await broadcaster.run()


async def async_main(argv: list[str] | None = None) -> None:
    """CLI entry point for running the telemetry broadcaster."""

    args = parse_args(argv)
    await run_broadcaster(
        args.data_file,
        host=args.host,
        port=args.port,
        replay_speed=args.speed,
        loop_forever=args.loop,
        autoplay=args.autoplay,
        seek_seconds=args.seek_seconds,
    )


def main(argv: list[str] | None = None) -> None:
    """Synchronous wrapper for the asyncio CLI entry point."""

    try:
        asyncio.run(async_main(argv))
    except KeyboardInterrupt:
        print("\nTelemetry broadcaster stopped.")
        raise SystemExit(130)


if __name__ == "__main__":
    main()