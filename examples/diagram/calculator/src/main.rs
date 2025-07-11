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

use std::{error::Error, fs::File, str::FromStr};

use bevy_impulse::{
    Diagram, DiagramElementRegistry, DiagramError, ImpulsePlugin, NodeBuilderOptions, Promise,
    RequestExt, RunCommandsOnWorldExt,
};
use clap::Parser;

#[derive(Parser, Debug)]
/// Example calculator app using diagrams.
struct Args {
    #[arg(help = "path to the diagram to run")]
    diagram: String,

    #[arg(help = "json containing the request to the diagram")]
    request: String,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    tracing_subscriber::fmt::init();

    let mut registry = DiagramElementRegistry::new();
    registry.register_node_builder(
        NodeBuilderOptions::new("add").with_default_display_text("Add"),
        |builder, config: f64| builder.create_map_block(move |req: f64| req + config),
    );
    registry.register_node_builder(
        NodeBuilderOptions::new("sub").with_default_display_text("Subtract"),
        |builder, config: f64| builder.create_map_block(move |req: f64| req - config),
    );
    registry.register_node_builder(
        NodeBuilderOptions::new("mul").with_default_display_text("Multiply"),
        |builder, config: f64| builder.create_map_block(move |req: f64| req * config),
    );
    registry.register_node_builder(
        NodeBuilderOptions::new("div").with_default_display_text("Divide"),
        |builder, config: f64| builder.create_map_block(move |req: f64| req / config),
    );

    let mut app = bevy_app::App::new();
    app.add_plugins(ImpulsePlugin::default());
    let file = File::open(args.diagram).unwrap();
    let diagram = Diagram::from_reader(file)?;

    let request = serde_json::Value::from_str(&args.request)?;
    let mut promise =
        app.world
            .command(|cmds| -> Result<Promise<serde_json::Value>, DiagramError> {
                let workflow = diagram.spawn_io_workflow(cmds, &registry)?;
                Ok(cmds.request(request, workflow).take_response())
            })?;

    while promise.peek().is_pending() {
        app.update();
    }

    println!("{}", promise.take().available().unwrap());
    Ok(())
}
