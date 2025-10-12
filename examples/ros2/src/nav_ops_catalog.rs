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
use rclrs::{Node as Ros2Node, Promise, *};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use std::collections::VecDeque;

use nav_msgs::{
    msg::{Goals, Path},
    srv::{GetPlan, GetPlan_Request, GetPlan_Response},
};

use example_interfaces::action::Fibonacci;

use std_msgs::msg::Empty as EmptyMsg;

pub fn register_nav_catalog(registry: &mut DiagramElementRegistry, node: &Ros2Node) {
    registry
        .enable_ros2(node.clone())
        .register_ros2_message::<Goals>()
        .register_ros2_message::<EmptyMsg>()
        .register_ros2_message::<Path>()
        .register_ros2_service::<GetPlan>()
        .register_ros2_action::<Fibonacci>();

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
    registry.register_node_builder(NodeBuilderOptions::new("fetch_plans"), {
        let node = node.clone();
        move |builder, config: PlanningConfig| {
            let tolerance = config.tolerance;
            let client = node
                .create_client::<GetPlan>(&config.planner_service)
                .unwrap();

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
                    while let (Some(start), Some(goal)) =
                        (from_iter.next().cloned(), to_iter.next().cloned())
                    {
                        log!(&logger, "Requesting a plan from {start:?} to {goal:?}");
                        let request = GetPlan_Request {
                            start,
                            goal,
                            tolerance,
                        };
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
    });

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
            },
        )
        .with_listen();

    registry.register_node_builder(
        NodeBuilderOptions::new("print"),
        move |builder,
              PrintConfig {
                  message,
                  include_value,
              }: PrintConfig| {
            builder.create_map_block(move |value: JsonMessage| {
                if include_value {
                    println!("{message}{value:#?}");
                } else {
                    println!("{message}");
                }
                value
            })
        },
    );

    registry
        .opt_out()
        .no_serializing()
        .no_deserializing()
        .register_node_builder(
            NodeBuilderOptions::new("clear_paths"),
            move |builder, _config: ()| {
                let clear_paths = clear_paths.into_blocking_callback();
                builder.create_node(clear_paths)
            },
        )
        .with_buffer_access();
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

fn clear_paths(In((_, key)): In<((), BufferKey<Path>)>, mut access: BufferAccessMut<Path>) {
    let mut buffer = access.get_mut(&key).unwrap();
    // Remove all goals from the buffer by draining its full range.
    buffer.drain(..);
}

#[derive(Serialize, Deserialize, JsonSchema)]
struct PrintConfig {
    message: String,
    include_value: bool,
}
