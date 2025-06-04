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
use serde::{Serialize, Deserialize};
use schemars::JsonSchema;
use serde_json::json;
use std::collections::HashSet;
use tracing::error;
use uuid::Uuid;

use zenoh_examples::{
    zenoh_publisher_node, zenoh_subscription_node,
    ArcError, ZenohImpulsePlugin, ZenohTopicConfig,
};

fn main() {
    let args = Args::parse();

    let mut registry = DiagramElementRegistry::default();

    let session = format!("{}", Uuid::new_v4());

    registry
        .opt_out()
        .no_serializing()
        .no_deserializing()
        .register_node_builder(
            NodeBuilderOptions::new("door_request_publisher"),
            |builder, config: ZenohTopicConfig| {
                zenoh_publisher_node::<protos::DoorRequest>(config.topic_name, builder)
            }
        );

    registry
        .opt_out()
        .no_serializing()
        .no_deserializing()
        .register_node_builder(
            NodeBuilderOptions::new("door_state_subscription"),
            |builder, config: ZenohTopicConfig| {
                zenoh_subscription_node::<protos::DoorState>(config.topic_name, builder)
            }
        );

    // registry.register_node_builder(
    //     NodeBuilderOptions::new("door_handshake"),
    //     |builder, config: HandshakeConfig| {

    //     }
    // )

    let diagram = Diagram::from_json(json!({
        "version": "0.1.0",
        "start": "request_door",
        "ops": {
            "request_door": {

            }
        }
    }))
    .unwrap();
}


#[derive(Serialize, Deserialize, JsonSchema)]
struct HandshakeConfig {
    session: String,
    door: String,
    // status: protos::door_request::Mode,
}

#[derive(Parser)]
struct Args {
    #[arg(short, long, required = true)]
    door: String,

    #[arg(short, long, default_value_t = 1.0)]
    time: f32,
}
