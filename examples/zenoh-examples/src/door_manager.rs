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
use bevy_time::Time;
use clap::Parser;
use zenoh_examples::protos;
use std::collections::HashSet;
use tracing::error;

use zenoh_examples::{
    zenoh_publisher_node, zenoh_subscription_node,
    ZenohSubscriptionConfig,
};

fn main() {
    let args = Args::parse();

    let mut app = App::new();
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


}

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long, help = "The names of all doors that are being managed by this door manager")]
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
    commands: BufferKey<DoorCommand>,
}

fn process_request(
    In(service): BlockingServiceInput<(protos::DoorRequest, ProcessRequestBuffers)>,
    mut session_access: BufferAccessMut<DoorSessions>,
    mut command_access: BufferAccessMut<DoorCommand>,
) {
    let (request, keys) = service.request;
    let Ok(mut sessions) = session_access.get_mut(&keys.sessions) else {
        error!("Session buffer is broken");
        return;
    };

    let sessions = sessions.newest_mut_or_default().unwrap();

    let Ok(mut commands) = command_access.get_mut(&keys.commands) else {
        error!("Command buffer is broken");
        return;
    };

    match request.mode() {
        protos::door_request::Mode::Open => {
            if sessions.names.insert(request.session) {
                commands.push(DoorCommand::Open);
            }
        }
        protos::door_request::Mode::Release => {
            if sessions.names.remove(&request.session) {
                commands.push(DoorCommand::Close);
            }
        }
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
        let Some(state) = position_buffer.newest_mut() else {
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
