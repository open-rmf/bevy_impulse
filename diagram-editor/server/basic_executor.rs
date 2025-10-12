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

use crate::{new_router, ServerOptions};
use bevy_app;
use bevy_impulse::{
    Diagram, DiagramError, ImpulseAppPlugin, Promise, RequestExt, RunCommandsOnWorldExt,
};
use clap::Parser;
use std::thread;
use std::{fs::File, str::FromStr};

pub use bevy_impulse::DiagramElementRegistry;
pub use std::error::Error;

pub mod prelude {
    pub use bevy_impulse::prelude::*;
}

#[derive(Parser, Debug)]
#[clap(
    name = "Basic Diagram Editor / Workflow Executor",
    version = "0.1.0",
    about = "Basic program for running workflow diagrams headlessly (run) or serving a web-based diagram editor (serve)."
)]
pub struct Args {
    #[clap(subcommand)]
    pub command: Commands,
}

#[derive(Parser, Debug)]
pub enum Commands {
    /// Runs a diagram with the given request.
    Run(RunArgs),

    /// Starts a server to edit and run diagrams.
    Serve(ServeArgs),
}

#[derive(Parser, Debug)]
pub struct RunArgs {
    #[arg(help = "path to the diagram to run")]
    diagram: String,

    #[arg(help = "json containing the request to the diagram. Defaults to null if not set.")]
    request: Option<String>,
}

#[derive(Parser, Debug)]
pub struct ServeArgs {
    #[arg(short, long, default_value_t = 3000)]
    port: u16,
}

pub fn headless(args: RunArgs, registry: DiagramElementRegistry) -> Result<(), Box<dyn Error>> {
    let mut app = bevy_app::App::new();
    app.add_plugins(ImpulseAppPlugin::default());
    let file = File::open(args.diagram).unwrap();
    let diagram = Diagram::from_reader(file)?;

    let request = serde_json::Value::from_str(args.request.as_ref().map(|s| s.as_str()).unwrap_or("null"))?;
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

pub async fn serve(
    args: ServeArgs,
    registry: DiagramElementRegistry,
) -> Result<(), Box<dyn Error>> {
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

pub fn run(registry: DiagramElementRegistry) -> Result<(), Box<dyn Error>> {
    run_with_args(Args::parse(), registry)
}

pub fn run_with_args(args: Args, registry: DiagramElementRegistry) -> Result<(), Box<dyn Error>> {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(run_async_with_args(args, registry))
}

pub async fn run_async(registry: DiagramElementRegistry) -> Result<(), Box<dyn Error>> {
    run_async_with_args(Args::parse(), registry).await
}

pub async fn run_async_with_args(
    args: Args,
    registry: DiagramElementRegistry,
) -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();
    match args.command {
        Commands::Run(args) => headless(args, registry),
        Commands::Serve(args) => serve(args, registry).await,
    }
}
