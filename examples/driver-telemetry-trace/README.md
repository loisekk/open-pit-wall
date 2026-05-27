# Driver telemetry trace example

This example subscribes to the Open Pit Wall websocket for a single driver and plots the last 30 seconds of live `Speed`, `Gear`, `Braking`, and `Throttle` telemetry on one chart with vertically stacked bands.

## Run it

1. Start the replay broadcaster from the repository root.
2. Change into this directory.
3. Run the example with the driver code you want.

```bash
open-pit-wall replay --data-file /path/to/session.json --autoplay
cd examples/driver-telemetry-trace
python3 main.py --driver VER
```

Optional connection arguments:

```bash
python3 main.py --driver VER --host 127.0.0.1 --port 8765 --max-points 900
```
