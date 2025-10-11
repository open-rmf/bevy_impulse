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

use ros2_workflow_examples::register_nav_catalog;

use rclrs::*;
use bevy_impulse::prelude::*;
use serde_json::json;
use bevy_app::ScheduleRunnerPlugin;
use bevy_core::{FrameCountPlugin, TaskPoolPlugin, TypeRegistrationPlugin};

fn main() {
    let context = Context::default_from_env().unwrap();
    let mut executor = context.create_basic_executor();

    let mut app = bevy_app::App::new();

    let mut registry = DiagramElementRegistry::new();
    let node = executor.create_node("nav_manager").unwrap();

    register_nav_catalog(&mut registry, &node);

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
