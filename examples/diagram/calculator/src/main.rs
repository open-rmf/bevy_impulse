use bevy_impulse::{
    Diagram, DiagramElementRegistry, DiagramError, ImpulsePlugin, NodeBuilderOptions, Promise,
    RequestExt, RunCommandsOnWorldExt,
};
use bevy_impulse_diagram_editor::new_router;
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
struct ServeArgs {}

fn run(args: RunArgs) -> Result<(), Box<dyn Error>> {
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

async fn serve(_args: ServeArgs) -> Result<(), Box<dyn Error>> {
    println!("Serving diagram editor at http://localhost:3000");

    let router = new_router();
    let listener = tokio::net::TcpListener::bind("localhost:3000")
        .await
        .unwrap();
    axum::serve(listener, router).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

    tracing_subscriber::fmt::init();

    match cli.command {
        Commands::Run(args) => run(args),
        Commands::Serve(args) => serve(args).await,
    }
}
