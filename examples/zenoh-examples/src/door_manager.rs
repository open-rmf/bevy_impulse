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
use std::collections::HashSet;
use tracing::error;
use zenoh_examples::protos;

use zenoh_examples::{zenoh_publisher_node, zenoh_subscription_node, ArcError, ZenohImpulsePlugin};

fn main() {
    let args = Args::parse();

    let mut app = App::new();
    app.add_plugins((
        ImpulseAppPlugin::default(),
        ZenohImpulsePlugin::default(),
        TimePlugin::default(),
    ));

    let process_door_request = app.world_mut().spawn_service(process_request);
    let door_controller = app.spawn_continuous_service(Update, door_controller);
    let door_state_notifier = app
        .world_mut()
        .spawn_service(door_state_notifier.into_blocking_service());

    for door_name in args.names {
        let request_topic_name = format!("door_request/{door_name}");
        let state_topic_name = format!("door_state/{door_name}");

        let workflow = app
            .world_mut()
            .spawn_io_workflow::<(), Result<(), ArcError>, _>(|scope, builder| {
                let command_buffer = builder.create_buffer(BufferSettings::default());
                let position_buffer = builder.create_buffer(BufferSettings::default());
                let session_buffer = builder.create_buffer(BufferSettings::default());
                let status_buffer = builder.create_buffer(BufferSettings::default());

                let publisher = zenoh_publisher_node::<protos::DoorState>(
                    state_topic_name.as_str().into(),
                    builder,
                );
                let subscriber = zenoh_subscription_node::<protos::DoorRequest>(
                    request_topic_name.as_str().into(),
                    builder,
                );
                builder.connect(subscriber.output, scope.terminate);

                let door_control_buffers = DoorControlBuffers::select_buffers(
                    position_buffer,
                    command_buffer,
                    status_buffer,
                );

                let controller_node = builder.create_node(door_controller);

                builder.chain(scope.input).fork_clone((
                    |setup: Chain<_>| {
                        setup
                            .with_access(door_control_buffers)
                            .connect(controller_node.input)
                    },
                    |setup: Chain<_>| setup.connect(subscriber.input),
                ));

                let process_request_buffers = ProcessRequestBuffers::select_buffers(session_buffer);
                builder
                    .chain(subscriber.streams.sample)
                    .with_access(process_request_buffers)
                    .then(process_door_request)
                    .connect(command_buffer.input_slot());

                let door_state_buffers =
                    DoorStateBuffers::select_buffers(session_buffer, status_buffer);
                builder
                    .listen(door_state_buffers)
                    .then(door_state_notifier)
                    .dispose_on_none()
                    .connect(publisher.input);
            });

        app.world_mut().command(|commands| {
            let _ = commands.request((), workflow).detach();
        });
    }

    app.run();
}

#[derive(Parser, Debug)]
struct Args {
    /// The names of all doors that are being managed by this door manager
    #[arg(short, long, num_args = 1.., required = true)]
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
            nominal_speed: 0.2,
        }
    }
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
    status: BufferKey<protos::door_state::Status>,
}

fn door_controller(
    In(input): ContinuousServiceInput<((), DoorControlBuffers), ()>,
    mut requests: ContinuousQuery<((), DoorControlBuffers), ()>,
    mut position_access: BufferAccessMut<DoorPosition>,
    command_access: BufferAccess<DoorCommand>,
    mut status_access: BufferAccessMut<protos::door_state::Status>,
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

        let Ok(mut status) = status_access.get_mut(&keys.status) else {
            return;
        };

        match command {
            DoorCommand::Close => {
                if state.position < 1.0 {
                    // The door is not closed, so let's drive it toward 1.0
                    let delta = state.nominal_speed * time.delta_secs();
                    let new_position = f32::min(state.position + delta, 1.0);
                    state.position = new_position;
                    if new_position == 1.0 {
                        status.push(protos::door_state::Status::Closed);
                    } else if status
                        .newest()
                        .is_none_or(|status| *status != protos::door_state::Status::Moving)
                    {
                        status.push(protos::door_state::Status::Moving);
                    }
                }
            }
            DoorCommand::Open => {
                if state.position > 0.0 {
                    // The door is not open, so let's drive it toward 0.0
                    let delta = state.nominal_speed * time.delta_secs();
                    let new_position = f32::max(state.position - delta, 0.0);
                    state.position = new_position;
                    if new_position == 0.0 {
                        status.push(protos::door_state::Status::Open);
                    } else if status
                        .newest()
                        .is_none_or(|status| *status != protos::door_state::Status::Moving)
                    {
                        status.push(protos::door_state::Status::Moving);
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
