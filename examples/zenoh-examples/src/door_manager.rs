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

use bevy_impulse::prelude::*;
use bevy_time::Time;
use clap::Parser;
use zenoh_examples::protos;
use std::collections::HashSet;
use tracing::error;

fn main() {
    let args = Args::parse();


}

#[derive(Parser, Debug)]
struct Args {
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

#[derive(Clone, Copy, Accessor)]
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
    };

    let sessions = sessions.newest_mut_or_default().unwrap();

    let Ok(mut commands) = command_access.get_mut(&keys.commands) else {
        error!("Command buffer is broken");
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

#[derive(Clone, Copy, Accessor)]
struct DoorControlBuffers {
    position: BufferKey<DoorPosition>,
    command: BufferKey<DoorCommand>,
}

fn door_controller(
    In(input): ContinuousServiceInput<((), DoorControlBuffers), (), DoorControlStream>,
    mut requests: ContinuousQuery<((), DoorControlBuffers), (), DoorControlStream>,
    mut position_access: BufferAccessMut<DoorPosition>,
    mut command_access: BufferAccess<DoorCommand>,
    time: Res<Time>,
) {
    let mut orders = requests.get_mut(&input.key).unwrap();
    orders.for_each(|order| {
        let keys = order.request().1;
        let Ok(mut position_buffer) = position_access.get_mut(&keys.position) else {
            error!("Position buffer is broken");
            return;
        };
        let Some(state) = position_buffer.newest_mut() else {
            return;
        };

        let Some(command_buffer) = command_access.get_newest(&keys.command) else {
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
