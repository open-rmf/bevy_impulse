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

use bevy_app;
use bevy_impulse::{
    Diagram, DiagramElementRegistry, DiagramError, ImpulseAppPlugin, NodeBuilderOptions, Promise,
    RequestExt, RunCommandsOnWorldExt,
};
use bevy_impulse_diagram_editor::{new_router, ServerOptions};
use clap::Parser;
use std::{error::Error, fs::File, str::FromStr};

#[derive(Parser, Debug)]
#[clap(
    name = "calculator",
    version = "0.1.0",
    about = "Example calculator app using diagrams."
)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Parser, Debug)]
enum Commands {
    /// Runs a diagram with the given request.
    Run(RunArgs),

    /// Starts a server to edit and run diagrams.
    Serve(ServeArgs),
}

#[derive(Parser, Debug)]
struct RunArgs {
    #[arg(help = "path to the diagram to run")]
    diagram: String,

    #[arg(help = "json containing the request to the diagram")]
    request: String,
}

#[derive(Parser, Debug)]
struct ServeArgs {
    #[arg(short, long, default_value_t = 3000)]
    port: u16,
}

fn run(args: RunArgs, registry: DiagramElementRegistry) -> Result<(), Box<dyn Error>> {
    let mut app = bevy_app::App::new();
    app.add_plugins(ImpulseAppPlugin::default());
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

async fn serve(args: ServeArgs, registry: DiagramElementRegistry) -> Result<(), Box<dyn Error>> {
    println!("Serving diagram editor at http://localhost:{}", args.port);

    let router = new_router(registry, ServerOptions::default());
    let listener = tokio::net::TcpListener::bind(("localhost", args.port))
        .await
        .unwrap();
    axum::serve(listener, router).await?;
    Ok(())
}

fn create_registry() -> DiagramElementRegistry {
    let mut registry = DiagramElementRegistry::new();
    registry.register_node_builder(
        NodeBuilderOptions::new("add").with_name("Add"),
        |builder, config: f64| builder.create_map_block(move |req: f64| req + config),
    );
    registry.register_node_builder(
        NodeBuilderOptions::new("sub").with_name("Subtract"),
        |builder, config: f64| builder.create_map_block(move |req: f64| req - config),
    );
    registry.register_node_builder(
        NodeBuilderOptions::new("mul").with_name("Multiply"),
        |builder, config: f64| builder.create_map_block(move |req: f64| req * config),
    );
    registry.register_node_builder(
        NodeBuilderOptions::new("div").with_name("Divide"),
        |builder, config: f64| builder.create_map_block(move |req: f64| req / config),
    );
    registry
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

    tracing_subscriber::fmt::init();

    let registry = create_registry();

    match cli.command {
        Commands::Run(args) => run(args, registry),
        Commands::Serve(args) => serve(args, registry).await,
    }
}
