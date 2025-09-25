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
    Diagram, DiagramElementRegistry, DiagramError, ImpulseAppPlugin, Promise, RequestExt,
    RunCommandsOnWorldExt,
};
use bevy_impulse_diagram_editor::{new_router, ServerOptions};
use clap::Parser;
use std::thread;
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

    let mut app = bevy_app::App::new();
    app.add_plugins(ImpulseAppPlugin::default());
    let router = new_router(&mut app, registry, ServerOptions::default());
    thread::spawn(move || app.run());
    let listener = tokio::net::TcpListener::bind(("localhost", args.port))
        .await
        .unwrap();
    axum::serve(listener, router).await?;
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

    tracing_subscriber::fmt::init();

    let mut registry = DiagramElementRegistry::new();
    calculator_ops_catalog::register(&mut registry);

    match cli.command {
        Commands::Run(args) => run(args, registry),
        Commands::Serve(args) => serve(args, registry).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bevy_impulse::JsonMessage;
    use serde_json::json;

    #[test]
    fn test_split() {
        let diagram = Diagram::from_json(json!(
            {
                "$schema": "https://raw.githubusercontent.com/open-rmf/bevy_impulse/refs/heads/main/diagram.schema.json",
                "version": "0.1.0",
                "templates": {},
                "start": "62746cc5-19e4-456f-a94f-d49619ccd2c0",
                "ops": {
                    "1283dab4-aec4-41d9-8dd3-ee04b2dc4c2b": {
                        "type": "node",
                        "builder": "mul",
                        "next": "49939e88-eefb-4dcc-8185-c880abe43cc6",
                        "config": 10
                    },
                    "2115dca9-fa22-406a-b4d1-6755732f9e4d": {
                        "type": "node",
                        "builder": "mul",
                        "next": "aa4fdf7b-a554-4683-82de-0bfbf3752d1d",
                        "config": 100
                    },
                    "62746cc5-19e4-456f-a94f-d49619ccd2c0": {
                        "type": "split",
                        "sequential": [
                            "1283dab4-aec4-41d9-8dd3-ee04b2dc4c2b",
                            "2115dca9-fa22-406a-b4d1-6755732f9e4d"
                        ]
                    },
                    "49939e88-eefb-4dcc-8185-c880abe43cc6": {
                        "type": "buffer"
                    },
                    "aa4fdf7b-a554-4683-82de-0bfbf3752d1d": {
                        "type": "buffer"
                    },
                    "f26502a3-84ec-4c4d-9367-15c099248640": {
                        "type": "node",
                        "builder": "add",
                        "next": {
                            "builtin": "terminate"
                        }
                    },
                    "c3f84b5f-5f09-4dc7-b937-bd1cdf504bf9": {
                        "type": "join",
                        "buffers": [
                            "49939e88-eefb-4dcc-8185-c880abe43cc6",
                            "aa4fdf7b-a554-4683-82de-0bfbf3752d1d"
                        ],
                        "next": "f26502a3-84ec-4c4d-9367-15c099248640"
                    }
                }
            }
        ))
        .unwrap();

        let request = json!([1, 2]);

        let mut app = bevy_app::App::new();
        app.add_plugins(ImpulseAppPlugin::default());
        let registry = create_registry();

        let mut promise = app
            .world
            .command(|cmds| -> Result<Promise<JsonMessage>, DiagramError> {
                let workflow = diagram.spawn_io_workflow(cmds, &registry)?;
                Ok(cmds.request(request, workflow).take_response())
            })
            .unwrap();

        while promise.peek().is_pending() {
            app.update();
        }

        let result = promise.take().available().unwrap().as_f64().unwrap();
        assert_eq!(result, 210.0);
    }
}
