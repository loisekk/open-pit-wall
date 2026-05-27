"""Live driver telemetry chart for the Open Pit Wall websocket replay feed."""

from __future__ import annotations

import argparse
import asyncio
from collections import deque
from dataclasses import dataclass
import json
from pathlib import Path
import threading
from typing import Any

import matplotlib.animation as animation
import matplotlib.pyplot as plt
from websockets.asyncio.client import connect

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8765
DEFAULT_MAX_POINTS = 600
DISPLAY_WINDOW_SECONDS = 30.0
PLOT_UPDATE_INTERVAL_MS = 100
CONNECT_TIMEOUT_SECONDS = 5.0
BAND_HEIGHT = 100.0
BAND_GAP = 24.0


@dataclass(frozen=True, slots=True)
class MetricSpec:
    name: str
    source_key: str
    minimum: float
    maximum: float
    color: str
    unit: str
    band_index: int

    @property
    def offset(self) -> float:
        return self.band_index * (BAND_HEIGHT + BAND_GAP)

    @property
    def center(self) -> float:
        return self.offset + (BAND_HEIGHT / 2.0)


METRIC_SPECS = (
    MetricSpec("Speed", "speed", 0.0, 400.0, "#ff5a5f", "km/h", 3),
    MetricSpec("Gear", "gear", 0.0, 8.0, "#2ec4b6", "", 2),
    MetricSpec("Braking", "brake", 0.0, 100.0, "#ff9f1c", "%", 1),
    MetricSpec("Throttle", "throttle", 0.0, 100.0, "#3a86ff", "%", 0),
)


@dataclass(frozen=True, slots=True)
class TelemetrySample:
    timestamp: str
    elapsed_seconds: float
    speed: float
    gear: float
    brake: float
    throttle: float


@dataclass(slots=True)
class PendingSample:
    elapsed_seconds: float | None = None
    driver_payload: dict[str, Any] | None = None


class TelemetryAssembler:
    """Align lap timing and single-driver telemetry messages by timestamp."""

    def __init__(self, driver_code: str) -> None:
        self.driver_code = normalize_driver_code(driver_code)
        self._pending: dict[str, PendingSample] = {}

    def ingest(self, message: dict[str, Any]) -> TelemetrySample | None:
        event = str(message.get("event", ""))
        timestamp = str(message.get("timestamp", "")).strip()
        if not timestamp:
            return None

        pending = self._pending.setdefault(timestamp, PendingSample())
        if event == "telemetry.lap":
            payload = message.get("payload", {})
            if not isinstance(payload, dict):
                return None
            elapsed_seconds = payload.get("elapsed_seconds")
            if not isinstance(elapsed_seconds, (int, float)):
                return None
            pending.elapsed_seconds = float(elapsed_seconds)
        elif event == f"telemetry.drivers.{self.driver_code}":
            payload = message.get("payload", {})
            if not isinstance(payload, dict):
                return None
            if normalize_driver_code(str(payload.get("driver_code", self.driver_code))) != self.driver_code:
                return None
            pending.driver_payload = payload
        else:
            return None

        if pending.elapsed_seconds is None or pending.driver_payload is None:
            self._trim_pending()
            return None

        sample = build_sample(timestamp, pending.elapsed_seconds, pending.driver_payload)
        del self._pending[timestamp]
        self._trim_pending()
        return sample

    def _trim_pending(self) -> None:
        while len(self._pending) > 25:
            oldest_timestamp = next(iter(self._pending))
            del self._pending[oldest_timestamp]


class TelemetryBuffer:
    """Thread-safe rolling telemetry history for the live plot."""

    def __init__(self, max_points: int) -> None:
        self.max_points = max_points
        self._samples: deque[TelemetrySample] = deque(maxlen=max_points)
        self._lock = threading.Lock()

    def append(self, sample: TelemetrySample) -> None:
        with self._lock:
            if self._samples and sample.timestamp == self._samples[-1].timestamp:
                self._samples[-1] = sample
            else:
                self._samples.append(sample)

    def snapshot(self) -> list[TelemetrySample]:
        with self._lock:
            return list(self._samples)


class ConnectionState:
    """Shared connection lifecycle state between the websocket thread and plot."""

    def __init__(self) -> None:
        self.connected = threading.Event()
        self.stopped = threading.Event()
        self.error: Exception | None = None
        self.driver_codes: list[str] = []

    def set_error(self, error: Exception) -> None:
        self.error = error
        self.connected.set()
        self.stopped.set()


def normalize_driver_code(value: str) -> str:
    driver_code = value.strip().upper()
    if not driver_code:
        raise ValueError("Driver code must not be empty.")
    return driver_code


def coerce_brake_percentage(value: Any) -> float:
    if not isinstance(value, (int, float)):
        raise ValueError("Brake telemetry value must be numeric.")
    brake_value = float(value)
    if brake_value <= 1.0:
        brake_value *= 100.0
    return max(0.0, min(brake_value, 100.0))


def clamp_metric_value(spec: MetricSpec, value: Any) -> float:
    if not isinstance(value, (int, float)):
        raise ValueError(f"{spec.name} telemetry value must be numeric.")
    numeric_value = float(value)
    return max(spec.minimum, min(numeric_value, spec.maximum))


def build_sample(
    timestamp: str, elapsed_seconds: float, driver_payload: dict[str, Any]
) -> TelemetrySample:
    return TelemetrySample(
        timestamp=timestamp,
        elapsed_seconds=float(elapsed_seconds),
        speed=clamp_metric_value(metric_spec_by_name("Speed"), driver_payload.get("speed", 0.0)),
        gear=clamp_metric_value(metric_spec_by_name("Gear"), driver_payload.get("gear", 0.0)),
        brake=coerce_brake_percentage(driver_payload.get("brake", 0.0)),
        throttle=clamp_metric_value(
            metric_spec_by_name("Throttle"), driver_payload.get("throttle", 0.0)
        ),
    )


def metric_spec_by_name(name: str) -> MetricSpec:
    for spec in METRIC_SPECS:
        if spec.name == name:
            return spec
    raise KeyError(name)


def metric_value(sample: TelemetrySample, spec: MetricSpec) -> float:
    return float(getattr(sample, spec.source_key))


def metric_band_value(sample: TelemetrySample, spec: MetricSpec) -> float:
    raw_value = metric_value(sample, spec)
    span = spec.maximum - spec.minimum
    if span <= 0:
        return spec.center
    normalized = (raw_value - spec.minimum) / span
    return spec.offset + (normalized * BAND_HEIGHT)


def display_window_bounds(latest_elapsed_seconds: float) -> tuple[float, float]:
    window_end = max(DISPLAY_WINDOW_SECONDS, float(latest_elapsed_seconds))
    return max(0.0, window_end - DISPLAY_WINDOW_SECONDS), window_end


def display_samples(samples: list[TelemetrySample]) -> list[TelemetrySample]:
    if not samples:
        return []
    window_start, _ = display_window_bounds(samples[-1].elapsed_seconds)
    return [sample for sample in samples if sample.elapsed_seconds >= window_start]


def build_plot_title(driver_code: str, websocket_url: str) -> str:
    return f"Open Pit Wall telemetry - {driver_code} ({websocket_url})"


def websocket_url(host: str, port: int) -> str:
    return f"ws://{host}:{port}"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot live telemetry for a single driver from the Open Pit Wall websocket."
    )
    parser.add_argument(
        "--driver",
        required=True,
        help="Driver code to subscribe to, for example VER.",
    )
    parser.add_argument(
        "--host",
        default=DEFAULT_HOST,
        help="WebSocket host running the replay broadcaster.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_PORT,
        help="WebSocket port running the replay broadcaster.",
    )
    parser.add_argument(
        "--max-points",
        type=int,
        default=DEFAULT_MAX_POINTS,
        help="Maximum telemetry samples to keep visible on the chart.",
    )
    args = parser.parse_args(argv)
    args.driver = normalize_driver_code(args.driver)
    if args.max_points < 10:
        parser.error("--max-points must be at least 10.")
    return args


async def stream_telemetry(
    *,
    host: str,
    port: int,
    driver_code: str,
    buffer: TelemetryBuffer,
    state: ConnectionState,
) -> None:
    url = websocket_url(host, port)
    async with connect(url, ping_interval=20, ping_timeout=20, max_size=None) as websocket:
        assembler = TelemetryAssembler(driver_code)
        welcome_raw = await asyncio.wait_for(websocket.recv(), timeout=CONNECT_TIMEOUT_SECONDS)
        welcome = json.loads(welcome_raw)
        validate_welcome_message(welcome, driver_code, state)
        await websocket.send(
            json.dumps(
                {
                    "action": "subscribe",
                    "channels": [f"telemetry.drivers.{driver_code}", "telemetry.lap"],
                }
            )
        )

        async for raw_message in websocket:
            message = json.loads(raw_message)
            message_type = str(message.get("type", ""))
            if message_type == "error":
                raise RuntimeError(str(message.get("message", "Websocket subscription failed.")))
            if message_type == "subscribed":
                state.connected.set()
                continue
            sample = assembler.ingest(message)
            if sample is not None:
                buffer.append(sample)
                state.connected.set()


def validate_welcome_message(
    welcome: dict[str, Any], driver_code: str, state: ConnectionState
) -> None:
    session_info = welcome.get("session", {})
    if not isinstance(session_info, dict):
        return
    driver_codes = session_info.get("driver_codes", [])
    if not isinstance(driver_codes, list):
        return
    normalized_codes = sorted(
        normalize_driver_code(code)
        for code in driver_codes
        if isinstance(code, str) and code.strip()
    )
    state.driver_codes = normalized_codes
    if normalized_codes and driver_code not in normalized_codes:
        joined_codes = ", ".join(normalized_codes)
        raise ValueError(f"Driver '{driver_code}' is not available. Choose one of: {joined_codes}")


def run_websocket_client(
    *,
    host: str,
    port: int,
    driver_code: str,
    buffer: TelemetryBuffer,
    state: ConnectionState,
) -> None:
    try:
        asyncio.run(
            stream_telemetry(
                host=host,
                port=port,
                driver_code=driver_code,
                buffer=buffer,
                state=state,
            )
        )
    except Exception as error:  # noqa: BLE001 - surfaced back to the CLI immediately.
        state.set_error(error)
    finally:
        state.stopped.set()


def configure_axes(ax: plt.Axes, driver_code: str, url: str) -> None:
    band_minimum = min(spec.offset for spec in METRIC_SPECS)
    band_maximum = max(spec.offset + BAND_HEIGHT for spec in METRIC_SPECS)
    lower_limit = band_minimum - BAND_GAP
    upper_limit = band_maximum + BAND_GAP
    ax.set_ylim(lower_limit, upper_limit + BAND_GAP)
    ax.set_yticks([spec.center for spec in METRIC_SPECS])
    ax.set_yticklabels(
        [f"{spec.name} ({int(spec.minimum)}-{int(spec.maximum)}{spec.unit})" for spec in METRIC_SPECS]
    )
    ax.set_xlabel("Seconds")
    ax.set_ylabel("Stacked telemetry bands")
    ax.set_title(build_plot_title(driver_code, url))
    ax.grid(axis="x", alpha=0.3)
    ax.set_xlim(0.0, DISPLAY_WINDOW_SECONDS)
    for spec in METRIC_SPECS:
        ax.axhline(spec.offset, color="#d9d9d9", linewidth=0.8, linestyle="--", zorder=0)
        ax.axhline(
            spec.offset + BAND_HEIGHT,
            color="#efefef",
            linewidth=0.6,
            linestyle=":",
            zorder=0,
        )


def plot_live_telemetry(
    *,
    driver_code: str,
    host: str,
    port: int,
    buffer: TelemetryBuffer,
    state: ConnectionState,
) -> None:
    url = websocket_url(host, port)
    figure, axis = plt.subplots(figsize=(13, 7))
    lines = {
        spec.name: axis.plot([], [], color=spec.color, linewidth=2.0, label=spec.name)[0]
        for spec in METRIC_SPECS
    }
    latest_text = axis.text(
        0.01,
        0.99,
        "Connecting...",
        transform=axis.transAxes,
        ha="left",
        va="top",
        fontsize=10,
        bbox={"facecolor": "white", "alpha": 0.85, "edgecolor": "#cccccc"},
    )
    configure_axes(axis, driver_code, url)
    axis.legend(loc="upper right")

    def on_close(_: Any) -> None:
        state.stopped.set()

    figure.canvas.mpl_connect("close_event", on_close)

    def update(_: int) -> list[Any]:
        if state.error is not None:
            latest_text.set_text(f"Connection error: {state.error}")
            return [*lines.values(), latest_text]

        samples = display_samples(buffer.snapshot())
        if not samples:
            if state.connected.is_set():
                latest_text.set_text("Connected. Waiting for telemetry frames...")
            return [*lines.values(), latest_text]

        x_values = [sample.elapsed_seconds for sample in samples]
        for spec in METRIC_SPECS:
            lines[spec.name].set_data(
                x_values,
                [metric_band_value(sample, spec) for sample in samples],
            )

        latest_sample = samples[-1]
        latest_text.set_text(
            " | ".join(
                (
                    f"t={latest_sample.elapsed_seconds:.1f}s",
                    f"Speed={latest_sample.speed:.0f} km/h",
                    f"Gear={latest_sample.gear:.0f}",
                    f"Brake={latest_sample.brake:.0f}%",
                    f"Throttle={latest_sample.throttle:.0f}%",
                )
            )
        )

        window_start, window_end = display_window_bounds(latest_sample.elapsed_seconds)
        axis.set_xlim(window_start, window_end)
        return [*lines.values(), latest_text]

    live_animation = animation.FuncAnimation(
        figure, update, interval=PLOT_UPDATE_INTERVAL_MS, blit=False
    )
    setattr(figure, "_telemetry_animation", live_animation)
    plt.tight_layout()
    plt.show()


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    buffer = TelemetryBuffer(max_points=args.max_points)
    state = ConnectionState()

    client_thread = threading.Thread(
        target=run_websocket_client,
        kwargs={
            "host": args.host,
            "port": args.port,
            "driver_code": args.driver,
            "buffer": buffer,
            "state": state,
        },
        daemon=True,
    )
    client_thread.start()

    state.connected.wait(timeout=CONNECT_TIMEOUT_SECONDS)
    if state.error is not None:
        raise SystemExit(str(state.error))

    try:
        plot_live_telemetry(
            driver_code=args.driver,
            host=args.host,
            port=args.port,
            buffer=buffer,
            state=state,
        )
    finally:
        state.stopped.set()
        client_thread.join(timeout=1.0)
        if state.error is not None:
            raise SystemExit(str(state.error))


if __name__ == "__main__":
    main()
