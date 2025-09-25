use bevy_impulse::{DiagramElementRegistry, ImpulseAppPlugin};
use bevy_impulse_diagram_editor_wasm::{init_wasm, setup_wasm, ExecutorOptions, InitOptions};

init_wasm! {
    let mut app = bevy_app::App::new();
    app.add_plugins(ImpulseAppPlugin::default());

    let mut registry = DiagramElementRegistry::new();
    calculator_ops_catalog::register(&mut registry);

    let executor_options = ExecutorOptions::default();

    InitOptions{app, registry, executor_options}
}
