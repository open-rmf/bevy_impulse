# Traffic app example

This is an example that enables users to build workflows via the diagram editor
and watch how their node connections result in different behaviors in a simple
traffic simulator. It is designed to support and demonstrate various `crossflow`
operations via the diagram editor.

The simulator is a kinematic simulation of a vehicle driving down a road. The
compiled nodes are intentionally minimal: inputs resemble vehicle controls
(throttle and steering) and outputs resemble sensor data (obstacle locations,
traffic signals, dashboard readings). The actual decision-making logic lives in
the diagram itself, typically inside a Python script operation that reads
sensor data out of [Buffers](https://open-rmf.github.io/crossflow-handbook/buffers.html)
and streams control commands back to the vehicle.

The simulation uses metric units throughout: positions and distances are in
meters, wheel angles are in degrees, and speeds are shown in km/h as the
user-facing unit.

## Vehicle controls

Nodes that command the vehicle:

| Node   | Use case    | Input   | Output   |
| ------ |------------ | ------- | -------- |
| `set_throttle` | Sets the target speed of the vehicle. Pass in a number for the target speed in km/h, or a dict to set both `target_speed` (km/h) and `max_acceleration` (km/h per second). The vehicle accelerates toward the target speed within its acceleration limit. | `f32` or `ThrottleCommand` | `Result<(), String>` |
| `steer` | Sets the target angle of the front wheels. Pass in a number for the target turn angle in degrees (positive angles steer left), or a dict to set both `target_turn_angle` and `max_steer_speed` (degrees per second). | `f32` or `SteeringCommand` | `Result<(), String>` |

## Sensors

Continuous service nodes that stream out data about the vehicle and its
surroundings. Their streams are typically connected to a
[Buffer](https://open-rmf.github.io/crossflow-handbook/buffers.html) so that a
script can fetch the newest value on its own schedule:

| Node   | Use case    | Streams   |
| ------ |------------ | --------- |
| `dashboard` | Streams the vehicle's dashboard instruments every update. | `speed` (km/h), `steering_wheel` (degrees) |
| `detect_traffic_signal` | Monitors the upcoming traffic signal via events and streams out changes. | `traffic_signal` (`red`/`yellow`/`green`/`empty`) |
| `detect_speed_limit` | Streams the speed limit posted by the last road sign the vehicle passed. The limit stays in effect until the next sign. | `speed_limit` (km/h) |
| `detect_obstacles` | Monitors obstacles ahead of the vehicle via query and streams out their positions relative to the vehicle, in meters. | `obstacles` (list of `{x, y}`) |
| `detect_lane_position` | Streams the vehicle's current x position within the lane, in meters. | `position` (meters) |
| `detect_stop_request` | A "user cancellation sensor" that emits each time the "Send stop request" button in the simulator UI is pressed. Use this to let the user end an active workflow early. | `stop` (elapsed seconds) |

## Controllers

| Node   | Use case    | Input   | Streams   |
| ------ |------------ | ------- | --------- |
| `lane_controller` | Continuously steers the vehicle toward a target x position within the lane. The target is read from a `ScriptMessage` buffer via [buffer access](https://open-rmf.github.io/crossflow-handbook/buffer-access.html), so a script can update the target while the controller runs. Its steering commands are streamed out and typically connected to the `steer` node. Optionally configure the controller gains (`err_gain`, `dir_gain`, `max_yaw`). | `((), BufferKey<ScriptMessage>)` | `steer` (degrees) |

## Example workflows

Ready-made JSON workflows live in `traffic_app/diagrams/`. Each one carries a
description and input examples, and they are worth exploring in this order:

| Workflow | What it demonstrates | Input |
| -------- | -------------------- | ----- |
| `drive.json` | The simplest possible workflow: set the throttle and terminate. The vehicle keeps driving at the target speed. | Target speed in km/h, e.g. `10` |
| `donuts.json` | [Split](https://open-rmf.github.io/crossflow-handbook/parallelism.html#split) and [Join](https://open-rmf.github.io/crossflow-handbook/join.html) operations routing one input to both vehicle controls, which makes the vehicle spin in circles. | e.g. `{"throttle": 20, "steer": -45}` |
| `stoplight.json` | A Python script control loop that fetches the latest traffic signal from a buffer and stops the vehicle at red lights. | Duration in seconds, e.g. `30` |
| `stoplight_and_obstacles.json` | The same control loop extended to also brake for obstacles ahead of the vehicle. | Duration in seconds, e.g. `30` |
| `speed_limit.json` | A control loop that follows the speed limit posted on road signs as they pass by. | Duration in seconds, e.g. `60` |
| `change_lane.json` | Splitting responsibilities between the diagram and compiled nodes: a script decides which lane to drive in and streams the target into a buffer, while the `lane_controller` node steers toward it. | Duration in seconds, e.g. `60` |

All of the timed workflows also connect a `detect_stop_request` sensor, so you
can press the "Send stop request" button in the simulator's user panel to end
the trip early. Workflows like `drive.json` and `donuts.json` terminate right
after sending their commands, so there is no workflow left to stop; use the
"Halt vehicle" button instead, which resets the throttle and steering directly.

Try experimenting with the various settings, such as buffer sizes and fetch
types (clone vs. pull), or edit the scripts and controller gains to see how
they affect the vehicle's behavior.

## Try it out!

From the current directory, run

```bash
cargo run -- serve
```

The first build downloads the app's sprite and font assets from
[Gazebo Fuel](https://app.gazebosim.org/Open-RMF/fuel/models/crossflow_traffic_app_assets)
into `assets/`, so it needs an internet connection; later builds reuse the
downloaded files.

Then open http://localhost:3000 to run the diagram editor app from your web
browser. Load one of the workflows from `traffic_app/diagrams/`, click
`Run Workflow`, enter an input (each workflow's input examples are listed in
its side panel), and watch the vehicle react in the simulator window.
