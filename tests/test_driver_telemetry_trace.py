from __future__ import annotations

import importlib.util
from pathlib import Path
import sys

import pytest


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "examples"
    / "driver-telemetry-trace"
    / "main.py"
)
SPEC = importlib.util.spec_from_file_location("driver_telemetry_trace_main", MODULE_PATH)
driver_telemetry_trace = importlib.util.module_from_spec(SPEC)
assert SPEC is not None and SPEC.loader is not None
sys.modules[SPEC.name] = driver_telemetry_trace
SPEC.loader.exec_module(driver_telemetry_trace)


def test_parse_args_requires_driver_and_normalizes_code():
    args = driver_telemetry_trace.parse_args(["--driver", "ver"])

    assert args.driver == "VER"
    assert args.host == driver_telemetry_trace.DEFAULT_HOST
    assert args.port == driver_telemetry_trace.DEFAULT_PORT


def test_coerce_brake_percentage_handles_boolean_style_values():
    assert driver_telemetry_trace.coerce_brake_percentage(0.0) == 0.0
    assert driver_telemetry_trace.coerce_brake_percentage(1.0) == 100.0
    assert driver_telemetry_trace.coerce_brake_percentage(0.62) == 62.0
    assert driver_telemetry_trace.coerce_brake_percentage(72.5) == 72.5


def test_telemetry_assembler_builds_sample_when_lap_and_driver_match():
    assembler = driver_telemetry_trace.TelemetryAssembler("VER")

    assert (
        assembler.ingest(
            {
                "timestamp": "2024-01-01T12:00:00Z",
                "event": "telemetry.drivers.VER",
                "payload": {
                    "driver_code": "VER",
                    "speed": 301.0,
                    "gear": 8,
                    "brake": 0.8,
                    "throttle": 96.0,
                },
            }
        )
        is None
    )

    sample = assembler.ingest(
        {
            "timestamp": "2024-01-01T12:00:00Z",
            "event": "telemetry.lap",
            "payload": {"current_lap": 3, "elapsed_seconds": 12.5},
        }
    )

    assert sample == driver_telemetry_trace.TelemetrySample(
        timestamp="2024-01-01T12:00:00Z",
        elapsed_seconds=12.5,
        speed=301.0,
        gear=8.0,
        brake=80.0,
        throttle=96.0,
    )


def test_metric_band_value_offsets_each_metric_band():
    sample = driver_telemetry_trace.TelemetrySample(
        timestamp="2024-01-01T12:00:01Z",
        elapsed_seconds=10.0,
        speed=200.0,
        gear=4.0,
        brake=50.0,
        throttle=25.0,
    )

    speed_spec = driver_telemetry_trace.metric_spec_by_name("Speed")
    throttle_spec = driver_telemetry_trace.metric_spec_by_name("Throttle")

    assert driver_telemetry_trace.metric_band_value(sample, speed_spec) == pytest.approx(
        speed_spec.offset + 50.0
    )
    assert driver_telemetry_trace.metric_band_value(sample, throttle_spec) == pytest.approx(
        throttle_spec.offset + 25.0
    )


def test_display_window_bounds_lock_chart_to_last_thirty_seconds():
    assert driver_telemetry_trace.display_window_bounds(12.5) == (0.0, 30.0)
    assert driver_telemetry_trace.display_window_bounds(45.0) == (15.0, 45.0)


def test_display_samples_filters_out_points_older_than_display_window():
    samples = [
        driver_telemetry_trace.TelemetrySample(
            timestamp="2024-01-01T12:00:00Z",
            elapsed_seconds=10.0,
            speed=100.0,
            gear=5.0,
            brake=0.0,
            throttle=70.0,
        ),
        driver_telemetry_trace.TelemetrySample(
            timestamp="2024-01-01T12:00:20Z",
            elapsed_seconds=20.0,
            speed=120.0,
            gear=6.0,
            brake=5.0,
            throttle=75.0,
        ),
        driver_telemetry_trace.TelemetrySample(
            timestamp="2024-01-01T12:00:45Z",
            elapsed_seconds=45.0,
            speed=150.0,
            gear=7.0,
            brake=10.0,
            throttle=80.0,
        ),
    ]

    visible_samples = driver_telemetry_trace.display_samples(samples)

    assert [sample.elapsed_seconds for sample in visible_samples] == [20.0, 45.0]


def test_plot_live_telemetry_keeps_animation_attached_to_figure(monkeypatch):
    created: dict[str, object] = {}

    def fake_animation(*args, **kwargs):
        created["figure"] = args[0]
        created["animation"] = object()
        return created["animation"]

    monkeypatch.setattr(driver_telemetry_trace.animation, "FuncAnimation", fake_animation)
    monkeypatch.setattr(driver_telemetry_trace.plt, "tight_layout", lambda: None)
    monkeypatch.setattr(driver_telemetry_trace.plt, "show", lambda: None)

    buffer = driver_telemetry_trace.TelemetryBuffer(max_points=10)
    state = driver_telemetry_trace.ConnectionState()

    driver_telemetry_trace.plot_live_telemetry(
        driver_code="VER",
        host=driver_telemetry_trace.DEFAULT_HOST,
        port=driver_telemetry_trace.DEFAULT_PORT,
        buffer=buffer,
        state=state,
    )

    figure = created["figure"]
    assert getattr(figure, "_telemetry_animation") is created["animation"]
    driver_telemetry_trace.plt.close(figure)
