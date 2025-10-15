## crossflow_diagram_editor_wasm

Provides helper functions and macros to create a wasm payload for diagram-editor.

### Usage

Add required crates:

```bash
cargo install wasm-pack
cargo add wasm_bindgen crossflow_diagram_editor_wasm
```

> [!IMPORTANT]
> `crossflow_diagram_editor_wasm` is not released yet, for now, use `cargo add --git https://github.com/open-rmf/crossflow crossflow_diagram_editor_wasm`.

`crossflow_diagram_editor_wasm` contains `init_wasm!`, a helper macro that set up everything needed to export a WebAssembly payload for the diagram editor frontend.

```rust
use crossflow::{DiagramElementRegistry, CrossflowExecutorApp};
use crossflow_diagram_editor_wasm::{init_wasm, setup_wasm, ExecutorOptions, InitOptions};

init_wasm! {
    let mut app = bevy_app::App::new();
    app.add_plugins(CrossflowExecutorApp::default());

    let mut registry = DiagramElementRegistry::new();
    // register node builders, sections etc.

    let executor_options = ExecutorOptions::default();

    // the function should return `InitOptions`.
    InitOptions{app, registry, &executor_options}
}
```

Build with `wasm-pack`:

```bash
wasm-pack build
```

This will output a js package in `pkg`, keep the path of the package in mind, it will be needed later when building the frontend.

Build the frontend:

```bash
WASM_PKG_PATH=<path-to-js-package> WASM_PKG_NAME=<name-of-package> pnpm build:wasm
```

This will build the frontend with the wasm bevy app.

Test with a basic server:

```bash
pnpm dlx serve dist
```

Open `http://localhost:3000` and see if it works!
