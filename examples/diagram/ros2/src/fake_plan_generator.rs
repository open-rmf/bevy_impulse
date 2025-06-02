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
use nav_msgs::srv::{GetPlan, GetPlan_Request, GetPlan_Response};
use geometry_msgs::msg::{Pose, PoseStamped, Point, Quaternion};
use std_msgs::msg::Header;
use builtin_interfaces::msg::Time as TimeMsg;

fn main() {
    let context = Context::default_from_env().unwrap();
    let mut executor = context.create_basic_executor();

    let node = executor.create_node("fake_plan_generator").unwrap();
    let logger = node.logger().clone();
    let _service = node.create_service::<GetPlan, _>(
        "get_plan",
        move |request: GetPlan_Request| {
            log!(
                &logger,
                "Serving a fake plan from:\n - {:?}\nto\n - {:?}",
                request.start,
                request.goal,
            );
            // Just linearly interpolate 5 points between the start and the goal
            let num_points = 5;
            let mut response = GetPlan_Response::default();
            for i in 1..=num_points {
                let s = i as f64 / num_points as f64;
                response.plan.poses.push(
                    interpolate_pose(s, &request.start, &request.goal)
                );
            }

            response
        });

    log!(node.logger(), "Ready to serve fake plans...");
    executor.spin(SpinOptions::default());
}

fn interpolate_pose(
    s: f64,
    start: &PoseStamped,
    goal: &PoseStamped,
) -> PoseStamped {
    PoseStamped {
        header: interpolate_header(s, &start.header, &goal.header),
        pose: Pose {
            position: interpolate_point(s, &start.pose.position, &goal.pose.position),
            orientation: interpolate_orientation(s, &start.pose.orientation, &goal.pose.orientation),
        }
    }
}

fn interpolate_header(
    s: f64,
    start: &Header,
    goal: &Header,
) -> Header {
    let t_start = secs_from_msg(&start.stamp);
    let t_goal = secs_from_msg(&goal.stamp);
    let t = s * (t_goal as f64 - t_start as f64) + t_start as f64;
    let t = t as i64;

    Header {
        stamp: msg_from_secs(t),
        frame_id: start.frame_id.clone(),
    }
}

fn secs_from_msg(msg: &TimeMsg) -> i64 {
    (msg.sec as i64 * 1_000_000_000) + msg.nanosec as i64
}

fn msg_from_secs(t: i64) -> TimeMsg {
    let nanosec = (t % 1_000_000_000) as u32;
    let sec = (t / 1_000_000_000) as i32;
    TimeMsg { sec, nanosec }
}

fn interpolate_point(
    s: f64,
    start: &Point,
    goal: &Point,
) -> Point {
    Point {
        x: s * (goal.x - start.x) + start.x,
        y: s * (goal.y - start.y) + start.y,
        z: s * (goal.z - start.z) + start.z,
    }
}

fn interpolate_orientation(
    s: f64,
    start: &Quaternion,
    goal: &Quaternion,
) -> Quaternion {
    // Just do a rough slerp approximation. This is a fake planner so
    // we don't need to do anything serious here.
    Quaternion {
        x: (1.0-s)*start.x + s * goal.x,
        y: (1.0-s)*start.y + s * goal.y,
        z: (1.0-s)*start.z + s * goal.z,
        w: (1.0-s)*start.w + s * goal.w,
    }
}
