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
use bevy_impulse_diagram_editor::{new_router, ServerOptions};
use bevy_app::ScheduleRunnerPlugin;
use bevy_core::{FrameCountPlugin, TaskPoolPlugin, TypeRegistrationPlugin};
use clap::Parser;
use std::{error::Error, fs::File};

#[derive(Parser, Debug)]
#[clap(
    name = "nav-executor",
    version = "0.1.0",
    about = "Example navigation execution system."
)]
struct Args {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Parser, Debug)]
enum Commands {
    /// Immediately run a nav exeuction diagram
    Run(RunArgs),

    /// Starts a server to edit and run diagrams
    Serve(ServeArgs),
}

#[derive(Parser, Debug)]
struct RunArgs {
    /// Path to the diagram to run.
    ///
    /// If not provided, we will run the default example.
    diagram: Option<String>,
}

#[derive(Parser, Debug)]
struct ServeArgs {
    /// What port to serve the diagram editor on
    #[arg(short, long, default_value_t = 3000)]
    port: u16,
}

fn run(args: RunArgs, registry: DiagramElementRegistry) -> Result<(), Box<dyn Error>> {
    let mut app = bevy_app::App::new();

    let diagram = if let Some(file) = args.diagram {
        Diagram::from_reader(File::open(file).unwrap()).unwrap()
    } else {
        Diagram::from_json_str(include_str!("../diagrams/default-nav-system.json")).unwrap()
    };

    let mut promise = app.world.command(|commands| {
        // Generate the workflow from the diagram.
        let workflow = diagram.spawn_io_workflow::<_, String>(commands, &registry).unwrap();

        // Get the workflow running.
        commands.request((), workflow).take_response()
    });

    app.add_plugins((
        TaskPoolPlugin::default(),
        TypeRegistrationPlugin,
        FrameCountPlugin,
        ScheduleRunnerPlugin::default(),
        ImpulsePlugin::default(),
    ));

    log!("nav-executor", "Beginning workflow...");

    while promise.peek().is_pending() {
        app.update();
    }

    if let Some(response) = promise.take().available() {
        println!("{response}");
    }

    Ok(())
}

async fn serve(args: ServeArgs, registry: DiagramElementRegistry) -> Result<(), Box<dyn Error>> {
    println!("Serving diagram editor at http://localhost:{}", args.port);

    let mut app = bevy_app::App::new();
    app.add_plugins(ImpulseAppPlugin::default());
    let router = new_router(&mut app, registry, ServerOptions::default());
    std::thread::spawn(move || app.run());
    let listener = tokio::net::TcpListener::bind(("localhost", args.port))
        .await
        .unwrap();
    axum::serve(listener, router).await?;
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    let context = Context::default_from_env().unwrap();
    let mut executor = context.create_basic_executor();

    // Set up the registry and get rclrs executor running
    let mut registry = DiagramElementRegistry::new();
    let node = executor.create_node("nav_manager").unwrap();
    let executor_commands = executor.commands().clone();
    register_nav_catalog(&mut registry, &node);
    let executor_thread = std::thread::spawn(move || executor.spin(SpinOptions::default()));

    tracing_subscriber::fmt::init();

    let r = match args.command {
        Commands::Run(args) => run(args, registry),
        Commands::Serve(args) => serve(args, registry).await,
    };

    executor_commands.halt_spinning();
    let _ = executor_thread.join();

    r
}
