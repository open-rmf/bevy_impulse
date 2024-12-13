use std::{error::Error, fs::File, str::FromStr};

use bevy_app::Update;
use bevy_impulse::{flush_impulses, Diagram, NodeRegistry, RequestExt, RunCommandsOnWorldExt};

fn main() -> Result<(), Box<dyn Error>> {
    let mut registry = NodeRegistry::default();
    registry.register_node("add", "Add", |builder, config: f64| {
        builder.create_map_block(move |req: f64| req + config)
    });
    registry.register_node("sub", "Subtract", |builder, config: f64| {
        builder.create_map_block(move |req: f64| req - config)
    });
    registry.register_node("mul", "Multiply", |builder, config: f64| {
        builder.create_map_block(move |req: f64| req * config)
    });
    registry.register_node("div", "Divide", |builder, config: f64| {
        builder.create_map_block(move |req: f64| req / config)
    });

    let mut args = std::env::args();
    args.next();
    let path = args.next().unwrap();
    let request_str = args.next().unwrap();

    let mut app = bevy_app::App::new();
    app.add_systems(Update, flush_impulses());
    let file = File::open(path).unwrap();
    let diagram = Diagram::from_reader(file)?;

    let request = serde_json::Value::from_str(&request_str)?;
    let workflow = diagram.spawn_io_workflow(&mut app, &registry)?;
    let mut promise = app
        .world
        .command(|cmds| cmds.request(request, workflow).take_response());

    while promise.peek().is_pending() {
        app.update();
    }

    println!("{}", promise.take().available().unwrap());
    Ok(())
}
