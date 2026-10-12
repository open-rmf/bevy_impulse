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

use crate::{ServerOptions, new_router};
use bevy_app::App;
use clap::Parser;
use crossflow::{
    CrossflowExecutorApp, CrossflowPlugin, Diagram, DiagramError, Outcome, RequestExt,
    RunCommandsOnWorldExt,
};
use std::thread;
use std::{fs::File, str::FromStr};

pub use crossflow::DiagramElementRegistry;
pub use std::error::Error;

pub mod prelude {
    pub use crossflow::prelude::*;
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

    #[arg(help = "json containing the request to the diagram")]
    request: String,
}

#[derive(Parser, Debug, Clone, Copy)]
pub struct ServeArgs {
    #[arg(short, long, default_value_t = 3000)]
    port: u16,
}

impl Default for ServeArgs {
    fn default() -> Self {
        Self { port: 3000 }
    }
}

#[derive(Default, Clone, Copy)]
pub enum PluginSelection {
    /// Include the [`CrossflowExecutorApp`] plugins. Use this if you will not
    /// be adding any Bevy plugins yourself.
    #[default]
    App,
    /// Only include the basic [`CrossflowPlugin`]. Use this if you will be
    /// adding the standard Bevy plugins yourself.
    Minimal,
}

#[derive(Default)]
pub struct CustomRun {
    pub plugins: PluginSelection,
    pub args: Option<Args>,
}

impl From<()> for CustomRun {
    fn from(_: ()) -> Self {
        Default::default()
    }
}

impl From<Args> for CustomRun {
    fn from(args: Args) -> Self {
        CustomRun {
            plugins: Default::default(),
            args: Some(args),
        }
    }
}

impl From<PluginSelection> for CustomRun {
    fn from(plugins: PluginSelection) -> Self {
        CustomRun {
            plugins,
            args: Default::default(),
        }
    }
}

pub fn headless(
    args: RunArgs,
    setup: impl FnOnce() -> BasicExecutorSetup + 'static,
) -> Result<(), Box<dyn Error>> {
    let BasicExecutorSetup { mut app, registry } = setup();
    app.add_plugins(CrossflowExecutorApp::default());
    let file = File::open(args.diagram).unwrap();
    let diagram = Diagram::from_reader(file)?;

    let request = serde_json::Value::from_str(&args.request)?;
    let mut outcome =
        app.world_mut()
            .command(|cmds| -> Result<Outcome<serde_json::Value>, DiagramError> {
                let workflow = diagram.spawn_io_workflow(cmds, &registry)?;
                Ok(cmds.request(request, workflow).outcome())
            })?;

    while outcome.is_pending() {
        app.update();
    }

    match outcome.try_recv().unwrap() {
        Ok(response) => println!("response: {response}"),
        Err(err) => println!("error: {err}"),
    }
    Ok(())
}

pub async fn serve(
    args: ServeArgs,
    setup: impl FnOnce() -> BasicExecutorSetup + Send + 'static,
) -> Result<(), Box<dyn Error>> {
    println!("Serving diagram editor at http://localhost:{}", args.port);

    let (router_sender, router_receiver) = tokio::sync::oneshot::channel();
    thread::spawn(move || {
        // The App needs to be created in the same thread that it gets run in,
        // because App does not implement Send.
        let BasicExecutorSetup { mut app, registry } = setup();
        app.add_plugins(CrossflowExecutorApp::default());
        let router = new_router(&mut app, registry, ServerOptions::default());
        let _ = router_sender.send(router);
        app.run()
    });

    axum_serve(router_receiver, args).await
}

pub fn custom_serve(
    plugins: PluginSelection,
    args: ServeArgs,
    setup: impl FnOnce() -> BasicExecutorSetup + 'static,
) -> Result<(), Box<dyn Error>> {
    println!("Serving diagram editor at http://localhost:{}", args.port);

    let BasicExecutorSetup { mut app, registry } = setup();
    // If WinitPlugin is added, add CrossflowPlugin instead of
    // CrossflowExecutorApp to prevent overlapping plugins
    match plugins {
        PluginSelection::App => {
            app.add_plugins(CrossflowExecutorApp::default());
        }
        PluginSelection::Minimal => {
            app.add_plugins(CrossflowPlugin::default());
        }
    }

    let (router_sender, router_receiver) = tokio::sync::oneshot::channel();
    thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let _ = rt.block_on(axum_serve(router_receiver, args));
    });
    let router = new_router(&mut app, registry, ServerOptions::default());
    let _ = router_sender.send(router);
    app.run();
    Ok(())
}

pub async fn axum_serve(
    router_receiver: tokio::sync::oneshot::Receiver<axum::routing::Router>,
    args: ServeArgs,
) -> Result<(), Box<dyn Error>> {
    let router = router_receiver.await?;

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
        Commands::Run(args) => headless(args, move || BasicExecutorSetup::minimal(registry)),
        Commands::Serve(args) => serve(args, move || BasicExecutorSetup::minimal(registry)).await,
    }
}

/// This struct describes how the basic executor app should be set up. You can
/// use this to add arbitrary systems and plugins to your executor app.
///
/// Inside the `setup` closure of `run_custom_setup`, create your app using
///
/// ```
/// # use bevy_app::App;
/// let mut app = App::new();
/// ```
///
/// Then use `App::add_plugins` or `App::add_systems` or any other Bevy App setup
/// methods you need before creating this data structure. Return this data
/// structure from your `setup` closure after you have finished setting it up.
pub struct BasicExecutorSetup {
    /// The app, initialized with everything the executor needs.
    pub app: App,
    /// The registry with all necessary node builders, section builders, and
    /// messages registered.
    pub registry: DiagramElementRegistry,
}

impl BasicExecutorSetup {
    /// Use a minimal setup.
    pub fn minimal(registry: DiagramElementRegistry) -> Self {
        Self {
            app: App::new(),
            registry,
        }
    }
}

/// Run a custom setup of a basic executor. Unlike the simpler run functions,
/// this gives you the opportunity to create a custom [`App`], adding whatever
/// systems and plugins to it that you would like.
///
/// # Arguments
/// * `args` - Custom arguments for running the basic executor.
///   If `args` is set to [`None`], we will use the environment variable arguments.
/// * `setup` - A closure used to set up the app in-place because the [`App`] data
///   structure cannot be moved between threads. This closure will be moved between
///   threads so it must have the Send trait.
pub fn run_custom_setup(
    settings: impl Into<CustomRun>,
    setup: impl FnOnce() -> BasicExecutorSetup + Send + 'static,
) -> Result<(), Box<dyn Error>> {
    let CustomRun { args, plugins } = settings.into();
    let args = args.unwrap_or_else(|| Args::parse());
    match args.command {
        Commands::Run(args) => {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async_headless(args, setup))
        }
        Commands::Serve(args) => custom_serve(plugins, args, setup),
    }
}

pub async fn async_headless(
    args: RunArgs,
    setup: impl FnOnce() -> BasicExecutorSetup + Send + 'static,
) -> Result<(), Box<dyn Error>> {
    headless(args, setup)
}
