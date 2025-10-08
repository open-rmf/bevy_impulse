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

use rclrs::*;
use clap::Parser;

use geometry_msgs::msg::{Pose, PoseStamped, Point, Quaternion};
use nav_msgs::msg::Goals;
use std_msgs::msg::{Empty as EmptyMsg, Header};
use builtin_interfaces::msg::Time as TimeMsg;

use rand::prelude::*;

fn main() {
    let context = Context::default_from_env().unwrap();
    let args = Args::parse();
    let mut executor = context.create_basic_executor();
    let node = executor.create_node("goal_requester").unwrap();

    let task = if args.cancel {
        let topic = "cancel_goals";
        let publisher = node.create_publisher::<EmptyMsg>(topic.transient_local()).unwrap();

        executor.commands().run(
            async move {
                log!(node.logger(), "Waiting for a subscriber on topic {topic}...");
                let subscriber_ready = node.notify_on_graph_change({
                    let publisher = publisher.clone();
                    move || {
                        publisher.get_subscription_count().unwrap() > 0
                    }
                });

                subscriber_ready.await.unwrap();

                log!(node.logger(), "Requesting cancel");
                publisher.publish(EmptyMsg::default()).unwrap();
            }
        )
    } else {
        let topic = "request_goal";
        let publisher = node.create_publisher::<Goals>(topic.transient_local()).unwrap();

        let mut goals = Vec::new();
        for i in 0..args.count {
            let time = i as i32;
            let mut rng = rand::rng();
            let x = rng.random();
            let y = rng.random();
            let z = 0.0;

            let orientation = Quaternion { x: 0.0, y: 0.0, z: 0.0, w: 1.0 };

            goals.push(PoseStamped {
                header: Header {
                    stamp: TimeMsg {
                        sec: time,
                        nanosec: 0,
                    },
                    frame_id: "map".to_string(),
                },
                pose: Pose {
                    position: Point { x, y, z },
                    orientation,
                },
            });
        }

        let request = Goals {
            header: Header {
                stamp: TimeMsg::default(),
                frame_id: "map".to_string(),
            },
            goals,
        };

        executor.commands().run(
            async move {
                log!(node.logger(), "Waiting for subscriber on topic {topic}...");
                let subscriber_ready = node.notify_on_graph_change({
                    let publisher = publisher.clone();
                    move || {
                        publisher.get_subscription_count().unwrap() > 0
                    }
                });

                subscriber_ready.await.unwrap();

                log!(node.logger(), "Publishing goals: {:#?}", request);
                publisher.publish(request).unwrap();
            }
        )
    };


    executor.spin(
        SpinOptions::default()
        .until_promise_resolved(task)
    );
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// How many random goals to generate
    #[arg(short, long, default_value_t = 3)]
    count: u8,
    #[arg(long)]
    cancel: bool,
}
