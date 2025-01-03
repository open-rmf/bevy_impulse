use std::{error::Error, fs::File, str::FromStr};

use bevy_impulse::{
    Diagram, DiagramError, ImpulsePlugin, NodeRegistry, Promise, RequestExt, RunCommandsOnWorldExt,
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

    let mut registry = NodeRegistry::default();
    registry.register_node_builder("add", "Add", |builder, config: f64| {
        builder.create_map_block(move |req: f64| req + config)
    });
    registry.register_node_builder("sub", "Subtract", |builder, config: f64| {
        builder.create_map_block(move |req: f64| req - config)
    });
    registry.register_node_builder("mul", "Multiply", |builder, config: f64| {
        builder.create_map_block(move |req: f64| req * config)
    });
    registry.register_node_builder("div", "Divide", |builder, config: f64| {
        builder.create_map_block(move |req: f64| req / config)
    });

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
