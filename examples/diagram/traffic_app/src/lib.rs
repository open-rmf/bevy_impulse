/*
 * Copyright (C) 2026 Open Source Robotics Foundation
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

use bevy::prelude::*;
use crossflow_diagram_editor::basic_executor::{
    self, BasicExecutorSetup, DiagramElementRegistry, Error, PluginSelection,
};
use traffic_ops_catalog::{
    MovementPlugin, PedestrianPlugin, SpawnWorldPlugin, SpeedLimitPlugin, TrafficSignalPlugin,
    UserInputPlugin,
};

pub fn run() -> Result<(), Box<dyn Error>> {
    basic_executor::run_custom_setup(PluginSelection::Minimal, || {
        let mut app = App::new();
        app.add_plugins((
            SpawnWorldPlugin::default(),
            UserInputPlugin::default(),
            MovementPlugin::default(),
            PedestrianPlugin::default(),
            TrafficSignalPlugin::default(),
            SpeedLimitPlugin::default(),
        ));

        let registry = DiagramElementRegistry::new();
        let mut setup = BasicExecutorSetup { app, registry };

        let py_event_loop = setup.registry.enable_python().unwrap();
        py_event_loop.spawn_thread_and_run();

        // Register traffic node builders from the traffic_ops_catalog library.
        traffic_ops_catalog::register(&mut setup);
        setup
    })
}
