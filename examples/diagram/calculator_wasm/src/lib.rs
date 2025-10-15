use crossflow::{CrossflowExecutorApp, DiagramElementRegistry};
use crossflow_diagram_editor_wasm::{init_wasm, setup_wasm, ExecutorOptions, InitOptions};

init_wasm! {
    wasm_logger::init(wasm_logger::Config::default());
    let mut app = bevy_app::SubApp::new();
    app.add_plugins(CrossflowExecutorApp::default());

    let mut registry = DiagramElementRegistry::new();
    calculator_ops_catalog::register(&mut registry);

    let executor_options = ExecutorOptions::default();

    InitOptions{app, registry, executor_options}
}
