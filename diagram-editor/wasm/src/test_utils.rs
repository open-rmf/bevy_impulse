use bevy_impulse::{DiagramElementRegistry, ImpulseAppPlugin, NodeBuilderOptions};
use bevy_impulse_diagram_editor::api::executor::ExecutorOptions;

use super::{init_wasm, setup_wasm, InitOptions};

init_wasm! {
    let mut app = bevy_app::SubApp::new();
    app.add_plugins(ImpulseAppPlugin::default());

    let mut registry = DiagramElementRegistry::new();
    registry.register_node_builder(NodeBuilderOptions::new("add3"), |builder, _config: ()| {
        builder.create_map_block(|req: i32| req + 3)
    });

    let executor_options = ExecutorOptions::default();

    InitOptions{app, registry, executor_options}
}

#[cfg(target_arch = "wasm32")]
pub(crate) fn setup_test() {
    init_wasm();
}
