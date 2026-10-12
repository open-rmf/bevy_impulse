/*
 * Copyright (C) 2026 Open Source Robotics Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
*/

use bevy::prelude::*;
use crossflow::{ConfigExample, Node, NodeBuilderOptions, ScriptMessage, prelude::*};
use crossflow_diagram_editor::basic_executor::BasicExecutorSetup;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub mod movement;
pub use movement::*;

pub mod pedestrian;
pub use pedestrian::*;

pub mod spawn_world;
pub use spawn_world::*;

pub mod speed_limit;
pub use speed_limit::*;

pub mod traffic;
pub use traffic::*;

pub mod traffic_signal;
pub use traffic_signal::*;

pub mod user_panel;
pub use user_panel::*;

pub mod vehicle;
pub use vehicle::*;

#[derive(StreamPack)]
struct TrafficSignalStreams {
    traffic_signal: TrafficSignal,
}

#[derive(StreamPack)]
struct SpeedLimitStreams {
    speed_limit: SpeedLimit,
}

#[derive(StreamPack)]
struct TrafficObstacleStreams {
    obstacles: Vec<JsonVec2>,
}

#[derive(StreamPack)]
struct StopRequestedStreams {
    stop: f32,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, JsonSchema)]
struct JsonVec2 {
    x: f32,
    y: f32,
}

#[derive(StreamPack)]
struct DashboardStreams {
    speed: f32,
    steering_wheel: f32,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ChangeLaneConfig {
    /// Heading limit in radians. When the vehicle yaw exceeds this, the
    /// controller stops steering further into the turn.
    pub max_yaw: f32,
    /// Steering command (degrees) per meter of lateral error.
    pub err_gain: f32,
    /// Damping term: steering command (degrees) per radian of vehicle yaw.
    pub dir_gain: f32,
}

impl Default for ChangeLaneConfig {
    fn default() -> Self {
        // Approximately critically damped for the kinematic bicycle model
        // at 20 km/h with a 5 m wheelbase
        ChangeLaneConfig {
            max_yaw: 0.35,
            err_gain: 2.0,
            dir_gain: 48.0,
        }
    }
}

#[derive(StreamPack)]
pub struct ChangeLaneStreams {
    pub steer: f32,
}

#[derive(StreamPack)]
pub struct LanePositionStreams {
    pub position: f32,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ThrottleConfig {
    #[serde(default)]
    pub max_acceleration: Option<f32>,
}

pub fn register(setup: &mut BasicExecutorSetup) {
    let registry = &mut setup.registry;
    let app = &mut setup.app;

    // =========================================================================
    fn dashboard(
        srv: ContinuousService<(), (), DashboardStreams>,
        mut orders: ContinuousQuery<(), (), DashboardStreams>,
        main_vehicle: Query<&VehicleDynamics, With<MainVehicle>>,
    ) {
        let Some(mut orders) = orders.get_mut(&srv.key) else {
            return;
        };

        let Ok(dynamics) = main_vehicle.single() else {
            return;
        };

        orders.for_each(|order| {
            order.streams().speed.send(dynamics.speed);
            order.streams().steering_wheel.send(dynamics.wheel_rotation);
        });
    }

    let dashboard_service = app.spawn_continuous_service(Last, dashboard);
    registry.register_node_builder(
        NodeBuilderOptions::new("dashboard")
            .with_default_display_text("Dashboard")
            .with_description(
                "Get information from the vehicle's dashboard instruments: \
                speed (km/h) and steering wheel angle (degrees)",
            ),
        move |builder, _: ()| builder.create_node(dashboard_service),
    );

    // =========================================================================
    let set_throttle_description = "Pass in a number to set the target speed \
        of the vehicle in km/h. Pass in a dict to set both the target_speed \
        (km/h) and the max_acceleration (km/h per second) fields.";
    let set_throttle_config_examples = [
        ConfigExample::new(
            "Use the built-in default for max acceleration.",
            JsonMessage::Null,
        ),
        ConfigExample::new(
            "Specify a custom default max acceleration. \
            This will be ignored if the incoming request contains a max_acceleration field.",
            ThrottleConfig {
                max_acceleration: Some(5.0),
            },
        ),
    ];

    registry.register_node_builder(
        NodeBuilderOptions::new("set_throttle")
            .with_default_display_text("Throttle")
            .with_description(set_throttle_description)
            .with_config_examples(set_throttle_config_examples),
        |builder, config: Option<ThrottleConfig>| {
            let f = move |
                srv: Blocking<JsonMessage>,
                mut main_vehicle: Query<&mut ThrottleCommand, With<MainVehicle>>,
            | {
                let speed_value = srv.request.as_number().and_then(|n| n.as_f64().map(|n| n as f32));
                let mut cmd = if let Some(target_speed) = speed_value {
                    ThrottleCommand {
                        target_speed,
                        max_acceleration: None,
                    }
                } else {
                    serde_json::from_value(srv.request).map_err(|err| err.to_string())?
                };

                cmd.max_acceleration = cmd.max_acceleration.or_else(|| config.and_then(|c| c.max_acceleration));
                let mut cmd_mut = main_vehicle.single_mut().map_err(|err| err.to_string())?;
                *cmd_mut = cmd;

                Ok::<_, String>(())
            };

            builder.create_node(f.into_callback())
        },
    )
        .with_result();

    // =========================================================================
    let set_steering_description = "Pass in a number to set the target turn angle \
        of the front wheels in degrees (positive angles steer left). Pass in a \
        struct to set both target_turn_angle and max_steer_speed. Use \
        max_steer_speed (degrees per second) to limit how fast the turn angle \
        can change.";

    registry
        .register_node_builder(
            NodeBuilderOptions::new("steer")
                .with_default_display_text("Steer")
                .with_description(set_steering_description),
            |builder, _: ()| {
                let f = move |
                srv: Blocking<JsonMessage>,
                mut main_vehicle: Query<&mut SteeringCommand, With<MainVehicle>>,
            | {
                let turn_value = srv.request.as_number().and_then(|n| n.as_f64().map(|n| n as f32));
                let cmd = if let Some(target_turn_angle) = turn_value {
                    SteeringCommand {
                        target_turn_angle,
                        max_steer_speed: None,
                    }
                } else {
                    serde_json::from_value(srv.request).map_err(|err| err.to_string())?
                };

                let mut cmd_mut = main_vehicle.single_mut().map_err(|err| err.to_string())?;
                *cmd_mut = cmd;

                Ok::<_, String>(())
            };

                builder.create_node(f.into_callback())
            },
        )
        .with_result();

    // =========================================================================
    let detect_traffic_signal_description = "Detects traffic signal updates via events";
    fn detect_traffic_signal(
        srv: ContinuousService<(), (), TrafficSignalStreams>,
        mut orders: ContinuousQuery<(), (), TrafficSignalStreams>,
        mut upcoming_signal: EventReader<UpcomingTrafficSignal>,
    ) {
        let Some(mut orders) = orders.get_mut(&srv.key) else {
            return;
        };

        if orders.is_empty() {
            return;
        }

        for signal in upcoming_signal.read() {
            orders.for_each(|order| order.streams().traffic_signal.send(signal.0.clone()));
        }
    }

    let detect_traffic_signal_service =
        app.spawn_continuous_service(PostUpdate, detect_traffic_signal);
    registry.register_node_builder(
        NodeBuilderOptions::new("detect_traffic_signal".to_string())
            .with_description(detect_traffic_signal_description),
        move |builder, _config: ()| builder.create_node(detect_traffic_signal_service),
    );

    // =========================================================================
    fn detect_speed_limit(
        srv: ContinuousService<(), (), SpeedLimitStreams>,
        mut orders: ContinuousQuery<(), (), SpeedLimitStreams>,
        current: Res<CurrentSpeedLimit>,
    ) {
        let Some(mut orders) = orders.get_mut(&srv.key) else {
            return;
        };

        orders.for_each(|order| order.streams().speed_limit.send(current.0.clone()));
    }

    let detect_speed_limit_service = app.spawn_continuous_service(PostUpdate, detect_speed_limit);
    registry.register_node_builder(
        NodeBuilderOptions::new("detect_speed_limit")
            .with_default_display_text("Detect Speed Limit")
            .with_description(
                "Streams the speed limit (km/h) posted by the last road sign \
                the vehicle passed",
            ),
        move |builder, _: ()| builder.create_node(detect_speed_limit_service),
    );

    // =========================================================================
    let detect_obstacles_description = "Detects obstacles in range via query";
    fn detect_obstacles(
        srv: ContinuousService<(), (), TrafficObstacleStreams>,
        mut orders: ContinuousQuery<(), (), TrafficObstacleStreams>,
        main_vehicle: Query<&Transform, (With<MainVehicle>, Without<Obstacle>)>,
        obstacles: Query<&Transform, (With<Obstacle>, Without<MainVehicle>)>,
        world_limits: Res<WorldLimits>,
    ) {
        let Some(mut orders) = orders.get_mut(&srv.key) else {
            return;
        };
        if orders.is_empty() {
            return;
        }

        let Ok(vehicle) = main_vehicle.single() else {
            return;
        };

        let scale = world_limits.convert_m_to_px;
        let obstacles: Vec<JsonVec2> = obstacles
            .iter()
            .filter(|ob| {
                let diff = ob.translation.y - vehicle.translation.y;
                // Ignore obstacles behind the main vehicle
                if diff < world_limits.vehicle_size.1 {
                    return false;
                }
                // Ignore obstacles off screen
                if diff > 0.5 * world_limits.window.1 {
                    return false;
                }
                true
            })
            .map(|t| JsonVec2 {
                x: (t.translation.x - vehicle.translation.x) / scale,
                y: (t.translation.y - vehicle.translation.y) / scale,
            })
            .collect();

        if obstacles.is_empty() {
            return;
        }

        orders.for_each(|order| order.streams().obstacles.send(obstacles.clone()));
    }
    let detect_obstacles_service = app.spawn_continuous_service(PostUpdate, detect_obstacles);
    registry.register_node_builder(
        NodeBuilderOptions::new("detect_obstacles").with_description(detect_obstacles_description),
        move |builder, _config: ()| builder.create_node(detect_obstacles_service),
    );

    // =========================================================================
    fn detect_stop_request(
        srv: ContinuousService<(), (), StopRequestedStreams>,
        mut orders: ContinuousQuery<(), (), StopRequestedStreams>,
        mut stop_requested: EventReader<StopRequested>,
        time: Res<Time>,
    ) {
        let Some(mut orders) = orders.get_mut(&srv.key) else {
            return;
        };

        if stop_requested.read().last().is_none() {
            return;
        }

        orders.for_each(|order| {
            order.streams().stop.send(time.elapsed_secs());
        });
    }
    let detect_stop_request_service = app.spawn_continuous_service(PostUpdate, detect_stop_request);
    registry.register_node_builder(
        NodeBuilderOptions::new("detect_stop_request")
            .with_description("Sends out a signal each time a stop is requested")
            .with_default_display_text("Detect Stop Request"),
        move |builder, _: ()| builder.create_node(detect_stop_request_service),
    );

    // =========================================================================
    fn lane_controller(
        srv: ContinuousService<(ChangeLaneConfig, BufferKey<ScriptMessage>), (), ChangeLaneStreams>,
        mut orders: ContinuousQuery<
            (ChangeLaneConfig, BufferKey<ScriptMessage>),
            (),
            ChangeLaneStreams,
        >,
        mut target: BufferAccess<ScriptMessage>,
        query: Query<&Position, With<MainVehicle>>,
    ) {
        let Some(mut orders) = orders.get_mut(&srv.key) else {
            return;
        };

        let Ok(position) = query.single() else {
            return;
        };

        orders.for_each(|order| {
            let config = &order.request().0;
            let target_key = &order.request().1;
            let Some(target) = target
                .get(order.id(), target_key)
                .ok()
                .and_then(|t| t.newest())
            else {
                return;
            };
            let Some(target) = target.data.as_number().and_then(|n| n.as_f64()) else {
                return;
            };
            let target = target as f32;

            let x = position.translation.x;
            let yaw = position.yaw;
            let dx = target - x;
            let mut steering = -config.err_gain * dx - config.dir_gain * yaw;
            if yaw.abs() > config.max_yaw {
                if steering.signum() * yaw.signum() > 0.0 {
                    steering = 0.0;
                }
            }

            order.streams().steer.send(steering);
        });
    }
    let lane_controller_service = app.spawn_continuous_service(PostUpdate, lane_controller);
    registry
        .opt_out()
        .no_serializing()
        .no_deserializing()
        .register_node_builder(
            NodeBuilderOptions::new("lane_controller")
                .with_description("Steer the robot to a certain x position with the lane")
                .with_default_display_text("Lane Controller"),
            move |builder, config: Option<ChangeLaneConfig>| {
                let config = config.unwrap_or_default();
                let insert_config =
                    builder.create_map_block(move |(_, key): ((), BufferKey<ScriptMessage>)| {
                        (config, key)
                    });

                let node = builder.create_node(lane_controller_service);
                builder.connect(insert_config.output, node.input);
                Node::<_, _, ChangeLaneStreams> {
                    input: insert_config.input,
                    output: node.output,
                    streams: node.streams,
                }
            },
        )
        .with_buffer_access();

    fn detect_lane_position(
        srv: ContinuousService<(), (), LanePositionStreams>,
        mut orders: ContinuousQuery<(), (), LanePositionStreams>,
        query: Query<&Position, With<MainVehicle>>,
    ) {
        let Some(mut orders) = orders.get_mut(&srv.key) else {
            return;
        };

        let Ok(position) = query.single() else {
            return;
        };

        orders.for_each(|order| {
            order.streams().position.send(position.translation.x);
        });
    }
    let detect_lane_position_service =
        app.spawn_continuous_service(PostUpdate, detect_lane_position);
    registry.register_node_builder(
        NodeBuilderOptions::new("detect_lane_position")
            .with_description("Detect the current position within the lane")
            .with_default_display_text("Detect Lane Position"),
        move |builder, _: ()| builder.create_node(detect_lane_position_service),
    );
}
