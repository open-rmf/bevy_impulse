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
use schemars::JsonSchema;
use tokio::sync::mpsc::unbounded_channel;
use serde_json::json;
use bevy_app::ScheduleRunnerPlugin;
use bevy_core::{FrameCountPlugin, TaskPoolPlugin, TypeRegistrationPlugin};

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

    // Create a subscriber that listens for goal messages to arrive.
    registry.register_node_builder(
        NodeBuilderOptions::new("receive_goals"),
        {
            let node = node.clone();
            move |builder, config: SubscriptionConfig| {
                let node = node.clone();
                builder.create_map(move |input: AsyncMap<(), GoalStream>| {
                    let (sender, mut receiver) = unbounded_channel();

                    let _subscription = node.create_subscription(
                        config.topic.transient_local(),
                        move |msg: Goals| {
                            let _ = sender.send(msg);
                        }
                    ).unwrap();

                    let logger = node.logger().clone();
                    log!(&logger, "Waiting to receive goals from topic {}...", config.topic);
                    async move {
                        // Force the _subscription variable to be captured since
                        // it has side effects.
                        let _subscription = _subscription;
                        while let Some(msg) = receiver.recv().await {
                            log!(&logger, "Received a sequence of {} goals", msg.goals.len());
                            input.streams.goals.send(msg);
                        }
                    }
                })
            }
        }
    );

    // Create a client that fetches plans from a server. Each pair of
    // consecutive goals is planned for independently, in parallel.
    //
    // After issuing every plan request, we will await them in order of first to
    // last and stream them out from the node in that order, regardless of the
    // order in which the plans arrive from the service.
    registry
        .opt_out()
        .no_serializing()
        .no_deserializing()
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
    let print_paths = app.world.spawn_service(print_paths.into_blocking_service());
    registry
        .opt_out()
        .no_serializing()
        .no_deserializing()
        .register_node_builder(
            NodeBuilderOptions::new("print_paths"),
            move |builder, _config: ()| {
                builder.create_node(print_paths)
            }
        )
        .with_listen();

    registry
        .opt_out()
        .no_serializing()
        .no_deserializing()
        .register_message::<Path>();

    // We'll provide a topic for clearing the goals from the buffer
    registry.register_node_builder(
        NodeBuilderOptions::new("receive_cancel"),
        {
            let node = node.clone();
            move |builder, config: SubscriptionConfig| {
                let node = node.clone();
                builder.create_map(move |input: AsyncMap<(), ClearSignalString>| {
                    let (sender, mut receiver) = unbounded_channel();

                    let _subscription = node.create_subscription(
                        config.topic.transient_local(),
                        move |_msg: EmptyMsg| {
                            let _ = sender.send(());
                        }
                    ).unwrap();

                    let logger = node.logger().clone();
                    async move {
                        // Force the _subscription variable to be captured since
                        // it has side effects.
                        let _subscription = _subscription;
                        while let Some(_) = receiver.recv().await {
                            log!(&logger, "Received a request to clear the goals");
                            input.streams.clear.send(());
                        }
                    }
                })
            }
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

    let diagram = Diagram::from_json(json!({
        "version": "0.1.0",
        "start": "setup",
        "ops": {
            "setup": {
                "type": "fork_clone",
                "next": [
                    "receive_goals",
                    "receive_cancel"
                ]
            },
            "receive_goals": {
                "type": "node",
                "builder": "receive_goals",
                "config": {
                    "topic": "request_goal"
                },
                "stream_out": {
                    "goals": "fetch_plans",
                },
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
                "builder": "print_paths",
                "next": { "builtin": "dispose" }
            },
            "receive_cancel": {
                "type": "node",
                "builder": "receive_cancel",
                "config": {
                    "topic": "cancel_goals"
                },
                "stream_out": {
                    "clear": "access_paths"
                },
                "next": { "builtin": "dispose" }
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

    std::thread::spawn(move || executor.spin(SpinOptions::default()));
    app.run()
}

#[derive(Serialize, Deserialize, JsonSchema)]
struct SubscriptionConfig {
    topic: String,
}

#[derive(StreamPack)]
struct GoalStream {
    goals: Goals,
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

#[derive(StreamPack)]
struct ClearSignalString {
    clear: (),
}

fn print_paths(
    In(key): In<BufferKey<Path>>,
    access: BufferAccess<Path>,
) {
    let buffer = access.get(&key).unwrap();
    let paths: Vec<&Path> = buffer.iter().collect();
    println!("Paths currently waiting to run:\n{paths:#?}");
}

fn clear_paths(
    In((_, key)): In<((), BufferKey<Path>)>,
    mut access: BufferAccessMut<Path>,
) {
    let mut buffer = access.get_mut(&key).unwrap();
    // Remove all goals from the buffer by draining its full range.
    buffer.drain(..);
}
