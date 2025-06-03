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
use clap::Parser;
use zenoh_examples::protos;

fn main() {
    let args = Args::parse();


}

#[derive(Parser, Debug)]
struct Args {
    names: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DoorCommand {
    Close,
    Open,
}

struct DoorPosition {
    position: f32,
    speed: f32,
}

#[derive(Clone, Accessor)]
struct DoorBuffers {
    position: BufferKey<DoorPosition>,
    command: BufferKey<DoorCommand>,
}

#[derive(StreamPack)]
struct DoorControlStream {
    status: protos::door_state::Status,
}

fn door_controller(
    In(input): ContinuousServiceInput<((), DoorBuffers), (), DoorControlStream>,

) {

}
