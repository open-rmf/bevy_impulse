## bevy_impulse_diagram_editor_wasm

Provides helper functions and macros to create a wasm payload for diagram-editor.

### Usage

Add required crates:

```bash
cargo install wasm-pack
cargo add wasm_bindgen bevy_impulse_diagram_editor_wasm
```

TODO: `bevy_impulse_diagram_editor_wasm` is not released yet, for now, use `cargo add --git https://github.com/open-rmf/bevy_impulse_diagram_editor bevy_impulse_diagram_editor_wasm`.

Export wasm bindings:

```rust
use bevy_impulse::{DiagramElementRegistry, ImpulseAppPlugin};
use bevy_impulse_diagram_editor_wasm::{init_wasm, setup_wasm, ExecutorOptions};

init_wasm! {
    let mut app = bevy_app::App::new();
    app.add_plugins(ImpulseAppPlugin::default());

    let mut registry = DiagramElementRegistry::new();
    // register node builders, sections etc.

    let executor_options = ExecutorOptions::default();

    setup_wasm(app, registry, &executor_options);
}
```

Build with `wasm-pack`:

```bash
wasm-pack build
```
