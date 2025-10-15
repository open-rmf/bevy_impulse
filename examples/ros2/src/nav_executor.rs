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
use crossflow_diagram_editor::basic_executor::{self, DiagramElementRegistry, Error};

fn main() -> Result<(), Box<dyn Error>> {
    // Set up the critical rclrs components
    let context = Context::default_from_env().unwrap();
    let mut executor = context.create_basic_executor();
    let node = executor.create_node("nav_manager").unwrap();

    // Set up the custom ROS registry and get rclrs executor running
    let mut registry = DiagramElementRegistry::new();
    register_nav_catalog(&mut registry, &node);

    // Get the rclrs executor running its own thread
    let executor_commands = executor.commands().clone();
    let executor_thread = std::thread::spawn(move || executor.spin(SpinOptions::default()));

    // Run the workflow execution program with our custom registry
    let r = basic_executor::run(registry);

    // Close down the executor and join its thread gracefully to avoid noisy thread-crashing errors
    executor_commands.halt_spinning();
    let _ = executor_thread.join();

    r
}
