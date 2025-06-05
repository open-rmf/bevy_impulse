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

use bevy_app::{App, AppExit, Update};
use bevy_ecs::prelude::{EventWriter, Res};
use bevy_impulse::prelude::*;
use bevy_time::{Time, TimePlugin};
use clap::Parser;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::error;
use uuid::Uuid;
use zenoh_examples::protos;

use zenoh_examples::{zenoh_publisher_node, zenoh_subscription_node, ZenohImpulsePlugin};

#[derive(Parser)]
struct Args {
    #[arg(short, long, required = true)]
    door: String,

    #[arg(short, long, default_value_t = 5.0)]
    time: f32,
}

fn main() {
    let args = Args::parse();
    let mut app = App::new();
    app.add_plugins((
        ImpulseAppPlugin::default(),
        ZenohImpulsePlugin::default(),
        TimePlugin::default(),
    ));

    let mut registry = DiagramElementRegistry::default();

    let session = format!("{}", Uuid::new_v4());

    registry.register_node_builder(
        NodeBuilderOptions::new("use_door"),
        |builder, config: UseDoorConfig| {
            let door_name = &config.door;
            let request_topic_name = format!("door_request/{door_name}");
            let state_topic_name = format!("door_state/{door_name}");

            let subscriber = zenoh_subscription_node::<protos::DoorState>(
                state_topic_name.as_str().into(),
                builder,
            );
            let publisher = zenoh_publisher_node::<protos::DoorRequest>(
                request_topic_name.as_str().into(),
                builder,
            );

            let (input, startup) = builder.create_fork_clone::<()>();
            startup.clone_chain(builder).connect(subscriber.input);

            let request_msg = match config.usage {
                DoorUsageMode::Open => protos::DoorRequest {
                    mode: protos::door_request::Mode::Open.into(),
                    session: config.session.clone(),
                },
                DoorUsageMode::Release => protos::DoorRequest {
                    mode: protos::door_request::Mode::Release.into(),
                    session: config.session.clone(),
                },
            };

            startup
                .clone_chain(builder)
                .map_block(move |_| request_msg.clone())
                .connect(publisher.input);

            let output = builder
                .chain(subscriber.streams.sample)
                .map_block(move |state| {
                    match config.usage {
                        DoorUsageMode::Open => {
                            // We want to see that the door is both open and
                            // includes our session ID
                            if state.sessions.contains(&config.session) {
                                if state.status() == protos::door_state::Status::Open {
                                    return Some(());
                                }
                            }

                            None
                        }
                        DoorUsageMode::Release => {
                            // We just need to see that the session is not
                            // present in the door state
                            if !state.sessions.contains(&config.session) {
                                return Some(());
                            }

                            None
                        }
                    }
                })
                .dispose_on_none()
                // Stop the subscriber once we have what we need
                .then_trim([TrimBranch::single_point(&subscriber.input)])
                .output();

            Node::<_, _, ()> {
                input,
                output,
                streams: (),
            }
        },
    );

    let move_robot_service = app.spawn_continuous_service(Update, move_robot);
    registry.register_node_builder(
        NodeBuilderOptions::new("move"),
        move |builder, config: MoveConfig| {
            let time_buffer = builder.create_buffer(BufferSettings::default());
            let access = builder.create_buffer_access(time_buffer);
            let output = builder
                .chain(access.output)
                .then(move_robot_service)
                .output();

            let (input, startup) = builder.create_fork_clone::<()>();
            startup
                .clone_chain(builder)
                .map_block(move |_| {
                    println!("Moving for the next {}s", config.time);
                    config.time
                })
                .connect(time_buffer.input_slot());

            startup.clone_chain(builder).connect(access.input);

            Node::<_, _, ()> {
                input,
                output,
                streams: (),
            }
        },
    );

    let diagram = Diagram::from_json(json!({
        "version": "0.1.0",
        "start": "open_door",
        "ops": {
            "open_door": {
                "type": "node",
                "builder": "use_door",
                "config": {
                    "session": session,
                    "door": args.door,
                    "usage": "open"
                },
                "next": "move_through_door"
            },
            "move_through_door": {
                "type": "node",
                "builder": "move",
                "config": {
                    "time": args.time,
                },
                "next": "close_door"
            },
            "close_door": {
                "type": "node",
                "builder": "use_door",
                "config": {
                    "session": session,
                    "door": args.door,
                    "usage": "release"
                },
                "next": { "builtin": "terminate" }
            }
        }
    }))
    .unwrap();

    app.world.command(|commands| {
        let workflow = diagram
            .spawn_io_workflow::<(), ()>(commands, &mut registry)
            .unwrap();
        let _ = commands
            .request((), workflow)
            .then(exit_app.into_blocking_callback())
            .detach();
    });

    app.run();
}

#[derive(Serialize, Deserialize, JsonSchema)]
struct UseDoorConfig {
    session: String,
    door: String,
    usage: DoorUsageMode,
}

#[derive(Serialize, Deserialize, JsonSchema)]
struct MoveConfig {
    time: f32,
}

#[derive(Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum DoorUsageMode {
    Open,
    Release,
}

fn move_robot(
    In(service): ContinuousServiceInput<((), BufferKey<f32>), ()>,
    mut query: ContinuousQuery<((), BufferKey<f32>), ()>,
    mut remaining_time_access: BufferAccessMut<f32>,
    time: Res<Time>,
) {
    let Some(mut requests) = query.get_mut(&service.key) else {
        return;
    };

    requests.for_each(|order| {
        let time_key = &order.request().1;
        let Ok(mut remaining_time) = remaining_time_access.get_mut(time_key) else {
            error!("Unable to access remaining time buffer");
            return;
        };

        let Some(t) = remaining_time.newest_mut() else {
            return;
        };

        *t -= time.delta_seconds();
        if *t <= 0.0 {
            order.respond(());
        }
    });
}

fn exit_app(In(_): In<()>, mut exit: EventWriter<AppExit>) {
    exit.send(AppExit);
}
