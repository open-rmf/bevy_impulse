use bevy_impulse::ImpulseAppPlugin;
use bevy_impulse_diagram_editor_wasm::{init_wasm, setup_wasm, ExecutorOptions};
use calculator_lib::create_registry;

init_wasm! {
    let mut app = bevy_app::App::new();
    app.add_plugins(ImpulseAppPlugin::default());

    let registry = create_registry();

    let executor_options = ExecutorOptions::default();

    setup_wasm(app, registry, &executor_options);
}
