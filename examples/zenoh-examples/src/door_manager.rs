/*
 * Copyright (C) 2025 Open Source Robotics Foundation
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

use bevy_app::{App, Update};
use bevy_ecs::prelude::Res;
use bevy_impulse::prelude::*;
use bevy_time::{Time, TimePlugin};
use clap::Parser;
use zenoh_examples::protos;
use std::collections::HashSet;
use serde_json::json;
use tracing::error;

use zenoh_examples::{
    zenoh_publisher_node, zenoh_subscription_node,
    ZenohSubscriptionConfig, ArcError,
};

fn main() {
    let args = Args::parse();

    let mut app = App::new();
    app.add_plugins((
        ImpulseAppPlugin::default(),
        TimePlugin::default(),
    ));
    let mut registry = DiagramElementRegistry::new();

    let process_door_request = app.world.spawn_service(process_request);
    registry
        .opt_out()
        .no_serializing()
        .no_deserializing()
        .register_node_builder(
            NodeBuilderOptions::new("process_door_request"),
            move |builder, _config: ()| {
                builder.create_node(process_door_request)
            },
        )
        .with_buffer_access();

    let door_controller = app.spawn_continuous_service(Update, door_controller);
    registry
        .opt_out()
        .no_serializing()
        .no_deserializing()
        .register_node_builder(
            NodeBuilderOptions::new("door_controller"),
            move |builder, _config: ()| {
                builder.create_node(door_controller)
            },
        );

    registry
        .opt_out()
        .no_serializing()
        .no_deserializing()
        .register_node_builder(
            NodeBuilderOptions::new("door_request_subscription"),
            |builder, config: ZenohSubscriptionConfig| {
                zenoh_subscription_node::<protos::DoorRequest>(config.topic_name, builder)
            }
        );

    registry
        .opt_out()
        .no_serializing()
        .no_deserializing()
        .register_message::<protos::DoorRequest>();

    let door_state_notifier = app.world.spawn_service(door_state_notifier.into_blocking_service());
    registry
        .opt_out()
        .no_serializing()
        .no_deserializing()
        .register_node_builder(
            NodeBuilderOptions::new("door_state_notifier"),
            move |builder, _: ()| {
                builder.create_node(door_state_notifier)
            }
        )
        .with_listen();

    registry
        .opt_out()
        .no_serializing()
        .no_deserializing()
        .register_node_builder(
            NodeBuilderOptions::new("door_state_publisher"),
            |builder, config: ZenohSubscriptionConfig| {
                zenoh_publisher_node::<protos::DoorState>(config.topic_name, builder)
            }
        );

    for door_name in args.names {
        let request_topic_name = format!("door_request/{door_name}");
        let state_topic_name = format!("door_state/{door_name}");
        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "startup",
            "ops": {
                "start": {
                    "type": "fork_clone",
                    "next": [
                        "controller_access",
                        "receive_requests"
                    ]
                },
                "controller_access": {
                    "type": "buffer_access",
                    "buffers": {
                        "command": "command_buffer",
                        "position": "position_buffer"
                    },
                    "next": "controller"
                },
                "controller": {
                    "type": "node",
                    "builder": "door_controller",
                    "next": { "builtin": "dispose" }
                },
                "receive_requests": {
                    "type": "node",
                    "builder": "door_request_subscription",
                    "config": {
                        "topic_name": request_topic_name
                    },
                    "next": { "builtin": "terminate" },
                    "stream_out": {
                        "sample": "process_requests_access"
                    }
                },
                "process_requests_access": {
                    "type": "buffer_access",
                    "buffers": {
                        "sessions": "session_buffer"
                    },
                    "next": "process_requests"
                },
                "process_requests": {
                    "type": "node",
                    "builder": "process_door_request",
                    "next": "command_buffer"
                },
                "command_buffer": { "type": "buffer" },
                "position_buffer": { "type": "buffer" },
                "session_buffer": { "type": "buffer" },
                "state_notifier": {
                    "type": "node",
                    "builder": "door_state_notifier",
                    "next": "door_state_publisher"
                },
                "publish_states": {
                    "type": "node",
                    "builder": "door_state_publisher",
                    "config": {
                        "topic_name": state_topic_name
                    },
                    "next": { "builtin": "dispose" }
                }
            }
        }))
        .unwrap();

        app.world.command(|commands| {
            let workflow = diagram.spawn_io_workflow::<(), Result<(), ArcError>>(commands, &mut registry).unwrap();
            let _ = commands.request((), workflow).detach();
        });
    }

    app.run();
}

#[derive(Parser, Debug)]
struct Args {
    /// The names of all doors that are being managed by this door manager
    #[arg(short, long, required = true)]
    names: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum DoorCommand {
    #[default]
    Close,
    Open,
}

#[derive(Clone, Copy, Debug)]
struct DoorPosition {
    position: f32,
    nominal_speed: f32,
}

impl Default for DoorPosition {
    fn default() -> Self {
        DoorPosition {
            position: 1.0,
            nominal_speed: 0.2
        }
    }
}

#[derive(StreamPack)]
struct DoorControlStream {
    status: protos::door_state::Status,
}

#[derive(Clone, Debug, Default)]
struct DoorSessions {
    names: HashSet<String>,
}

#[derive(Clone, Accessor)]
struct ProcessRequestBuffers {
    sessions: BufferKey<DoorSessions>,
}

fn process_request(
    In(service): BlockingServiceInput<(protos::DoorRequest, ProcessRequestBuffers)>,
    mut session_access: BufferAccessMut<DoorSessions>,
) -> DoorCommand {
    let (request, keys) = service.request;
    let Ok(mut sessions) = session_access.get_mut(&keys.sessions) else {
        error!("Session buffer is broken");
        return DoorCommand::Close;
    };

    let sessions = sessions.newest_mut_or_default().unwrap();

    match request.mode() {
        protos::door_request::Mode::Open => {
            sessions.names.insert(request.session);
        }
        protos::door_request::Mode::Release => {
            sessions.names.remove(&request.session);
        }
    }

    if sessions.names.is_empty() {
        DoorCommand::Close
    } else {
        DoorCommand::Open
    }
}

#[derive(Clone, Accessor)]
struct DoorControlBuffers {
    position: BufferKey<DoorPosition>,
    command: BufferKey<DoorCommand>,
}

fn door_controller(
    In(input): ContinuousServiceInput<((), DoorControlBuffers), (), DoorControlStream>,
    mut requests: ContinuousQuery<((), DoorControlBuffers), (), DoorControlStream>,
    mut position_access: BufferAccessMut<DoorPosition>,
    command_access: BufferAccess<DoorCommand>,
    time: Res<Time>,
) {
    let mut orders = requests.get_mut(&input.key).unwrap();
    orders.for_each(|order| {
        let keys = order.request().1.clone();
        let Ok(mut position_buffer) = position_access.get_mut(&keys.position) else {
            error!("Position buffer is broken");
            return;
        };
        let Some(state) = position_buffer.newest_mut_or_default() else {
            return;
        };

        let Some(command) = command_access.get_newest(&keys.command) else {
            return;
        };

        match command {
            DoorCommand::Close => {
                if state.position < 1.0 {
                    // The door is not closed, so let's drive it toward 1.0
                    let delta = state.nominal_speed * time.delta_seconds();
                    let new_position = f32::min(state.position + delta, 1.0);
                    state.position = new_position;
                    if new_position == 1.0 {
                        order.streams().status.send(protos::door_state::Status::Closed);
                    }
                }
            }
            DoorCommand::Open => {
                if state.position > 0.0 {
                    // The door is not open, so let's drive it toward 0.0
                    let delta = state.nominal_speed * time.delta_seconds();
                    let new_position = f32::max(state.position - delta, 0.0);
                    state.position = new_position;
                    if new_position == 0.0 {
                        order.streams().status.send(protos::door_state::Status::Open);
                    }
                }
            }
        }
    });
}

#[derive(Clone, Accessor)]
struct DoorStateBuffers {
    sessions: BufferKey<DoorSessions>,
    status: BufferKey<protos::door_state::Status>,
}

fn door_state_notifier(
    In(input): In<DoorStateBuffers>,
    sessions_access: BufferAccess<DoorSessions>,
    status_access: BufferAccess<protos::door_state::Status>,
) -> Option<protos::DoorState> {
    let Some(sessions) = sessions_access.get_newest(&input.sessions) else {
        return None;
    };
    let Some(status) = status_access.get_newest(&input.status) else {
        return None;
    };

    Some(protos::DoorState {
        status: (*status).into(),
        sessions: sessions.names.iter().cloned().collect(),
    })
}
