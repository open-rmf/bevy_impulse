use bevy_impulse::{DiagramElementRegistry, ImpulseAppPlugin, NodeBuilderOptions};
use bevy_impulse_diagram_editor::api::executor::ExecutorOptions;

use crate::{init_wasm, setup_wasm};

init_wasm! {
    let mut app = bevy_app::App::new();
    app.add_plugins(ImpulseAppPlugin::default());

    let mut registry = DiagramElementRegistry::new();
    registry.register_node_builder(NodeBuilderOptions::new("add3"), |builder, _config: ()| {
        builder.create_map_block(|req: i32| req + 3)
    });

    let executor_options = ExecutorOptions::default();

    setup_wasm(app, registry, &executor_options);
}

pub(crate) fn setup_test() {
    init_wasm();
}
