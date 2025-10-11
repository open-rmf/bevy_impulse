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

use rclrs::{*, Promise};
use bevy_impulse::prelude::*;
use serde::{Serialize, Deserialize};
use serde_json::json;
use bevy_app::ScheduleRunnerPlugin;
use bevy_core::{FrameCountPlugin, TaskPoolPlugin, TypeRegistrationPlugin};
use schemars::JsonSchema;

use std::collections::VecDeque;

use nav_msgs::{
    msg::{Goals, Path},
    srv::{GetPlan, GetPlan_Request, GetPlan_Response},
};

use std_msgs::msg::Empty as EmptyMsg;

fn main() {
    let context = Context::default_from_env().unwrap();
    let mut executor = context.create_basic_executor();

    let mut app = bevy_app::App::new();

    let mut registry = DiagramElementRegistry::new();
    let node = executor.create_node("nav_manager").unwrap();

    registry
        .enable_ros2(node.clone())
        .register_ros2_message::<Goals>()
        .register_ros2_message::<EmptyMsg>()
        .register_ros2_message::<Path>()
        .register_ros2_service::<GetPlan>();

    // Create a client that fetches plans from a server. Each pair of
    // consecutive goals is planned for independently, in parallel.
    //
    // After issuing every plan request, we will await them in order of first to
    // last and stream them out from the node in that order, regardless of the
    // order in which the plans arrive from the service.
    //
    // We make a custom node builder for this instead of using the generic service
    // client builder because we want the input to be processed in a specific way:
    // breaking it into multiple parallel requests and reassembling them in order
    // as they complete. In the future we might introduce generalized "enumerate"
    // and "collect" operations which could replace this.
    registry
        .register_node_builder(
            NodeBuilderOptions::new("fetch_plans"),
            {
                let node = node.clone();
                move |builder, config: PlanningConfig| {
                    let tolerance = config.tolerance;
                    let client = node.create_client::<GetPlan>(&config.planner_service).unwrap();

                    let logger = node.logger().clone();
                    builder.create_map(move |input: AsyncMap<Goals, PathStream>| {
                        let client = client.clone();
                        let logger = logger.clone();
                        async move {
                            log!(&logger, "Waiting for planning service...");
                            client.notify_on_service_ready().await.unwrap();

                            let mut from_iter = input.request.goals.iter();
                            let mut to_iter = input.request.goals.iter().skip(1);

                            let mut plan_promises = VecDeque::new();
                            while let (Some(start), Some(goal)) = (from_iter.next().cloned(), to_iter.next().cloned()) {
                                log!(&logger, "Requesting a plan from {start:?} to {goal:?}");
                                let request = GetPlan_Request { start, goal, tolerance };
                                let promise: Promise<GetPlan_Response> = client.call(request).unwrap();
                                plan_promises.push_back(promise);
                            }

                            while let Some(promise) = plan_promises.pop_front() {
                                let response = promise.await.unwrap();
                                log!(
                                    &logger,
                                    "Received a plan from {:?} to {:?}",
                                    response.plan.poses.first().map(|x| x.pose.clone()),
                                    response.plan.poses.last().map(|x| x.pose.clone()),
                                );
                                input.streams.paths.send(response.plan);
                            }
                        }
                    })
                }
            }
        );

    // We can't really execute the paths in this example program, so let's just
    // print whatever accumulates in the buffer.
    registry
        .opt_out()
        .no_serializing()
        .no_deserializing()
        .register_node_builder(
            NodeBuilderOptions::new("print_from_buffer"),
            move |builder, msg: Option<String>| {
                let callback = move |In(key): In<JsonBufferKey>, world: &World| {
                    let buffer = world.json_buffer_view(&key).unwrap();
                    let values: Result<Vec<JsonMessage>, _> = buffer.iter().collect();
                    let values = values.unwrap();
                    println!("{}{values:#?}", msg.as_ref().unwrap_or(&String::new()));
                };

                builder.create_node(callback.into_blocking_callback())
            }
        )
        .with_listen();

    registry.register_node_builder(
        NodeBuilderOptions::new("print"),
        move |builder, PrintConfig { message, include_value }: PrintConfig| {
            builder.create_map_block(move |value: JsonMessage| {
                if include_value {
                    println!("{message}{value:#?}");
                } else {
                    println!("{message}");
                }
                value
            })
        }
    );

    let clear_paths = app.world.spawn_service(clear_paths.into_blocking_service());
    registry
        .opt_out()
        .no_serializing()
        .no_deserializing()
        .register_node_builder(
            NodeBuilderOptions::new("clear_paths"),
            move |builder, _config: ()| {
                builder.create_node(clear_paths)
            }
        )
        .with_buffer_access();

    let request_goal_topic = "request_goal";
    let diagram = Diagram::from_json(json!({
        "version": "0.1.0",
        "start": "setup",
        "ops": {
            "setup": {
                "type": "fork_clone",
                "next": [
                    "print_welcome",
                    "receive_cancel"
                ]
            },
            "print_welcome": {
                "type": "node",
                "builder": "print",
                "config": {
                    "message": format!("Waiting to receive goals from {request_goal_topic}"),
                    "include_value": false
                },
                "next": "receive_goals"
            },
            "receive_goals": {
                "type": "node",
                "builder": "nav_manager__nav_msgs_msg_Goals__subscription",
                "config": {
                    "topic": format!("{request_goal_topic}"),
                },
                "stream_out": {
                    "out": "fetch_plans"
                },
                "next": "quit"
            },
            "quit": {
                "type": "transform",
                "cel": "null",
                "next": { "builtin": "terminate" }
            },
            "fetch_plans": {
                "type": "node",
                "builder": "fetch_plans",
                "config": {
                    "planner_service": "get_plan",
                    "tolerance": 0.1
                },
                "stream_out": {
                    "paths": "path_buffer"
                },
                "next": { "builtin": "dispose" }
            },
            "path_buffer": {
                "type": "buffer",
                "settings": { "retention": "keep_all" }
            },
            "listen_to_path_buffer": {
                "type": "listen",
                "buffers": ["path_buffer"],
                "next": "print_paths",
            },
            "print_paths": {
                "type": "node",
                "builder": "print_from_buffer",
                "config": "Paths currently waiting to run:\n",
                "next": { "builtin": "dispose" }
            },
            "receive_cancel": {
                "type": "node",
                "builder": "nav_manager__std_msgs_msg_Empty__subscription",
                "config": {
                    "topic": "cancel_goals"
                },
                "stream_out": {
                    "out": "trigger_clear_paths"
                },
                "next": { "builtin": "dispose" }
            },
            "trigger_clear_paths": {
                "type": "transform",
                "cel": "null",
                "next": "access_paths"
            },
            "access_paths": {
                "type": "buffer_access",
                "buffers": ["path_buffer"],
                "next": "clear_paths"
            },
            "clear_paths": {
                "type": "node",
                "builder": "clear_paths",
                "next": { "builtin": "dispose" }
            }
        }
    }))
    .unwrap();

    app.world.command(|commands| {
        // Generate the workflow from the diagram.
        let workflow = diagram.spawn_io_workflow::<_, ()>(commands, &registry).unwrap();

        // Get the workflow running.
        let _ = commands.request((), workflow).detach();
    });

    app.add_plugins((
        TaskPoolPlugin::default(),
        TypeRegistrationPlugin,
        FrameCountPlugin,
        ScheduleRunnerPlugin::default(),
        ImpulsePlugin::default(),
    ));

    log!(&*node, "Beginning workflow...");
    std::thread::spawn(move || executor.spin(SpinOptions::default()));
    app.run()
}

#[derive(Serialize, Deserialize, JsonSchema)]
struct PlanningConfig {
    planner_service: String,
    tolerance: f32,
}

#[derive(StreamPack)]
struct PathStream {
    paths: Path,
}

fn clear_paths(
    In((_, key)): In<((), BufferKey<Path>)>,
    mut access: BufferAccessMut<Path>,
) {
    let mut buffer = access.get_mut(&key).unwrap();
    // Remove all goals from the buffer by draining its full range.
    buffer.drain(..);
}

#[derive(Serialize, Deserialize, JsonSchema)]
struct PrintConfig {
    message: String,
    include_value: bool,
}
