# bevy_impulse_diagram_editor

![](./docs/assets/diagram-editor-preview.webp)

This contains a SPA React web app to create and edit a `bevy_impulse` diagram and an axum router to serve it.

## Setup

Install the dependencies:

```bash
pnpm install
```

## Embedding the Diagram Editor into a `bevy_impulse` app

The frontend is built using `rsbuild` and embedded inside the crate. The library exposes an axum router that can be used to serve both the frontend and backend:

```rs
use bevy_impulse_diagram_editor::{new_router, ServerOptions};

fn main() {
  let mut registry = DiagramElementRegistry::new();
  // register node builders, section builders etc.

  let mut app = bevy_app::App::new();
  app.add_plugins(ImpulseAppPlugin::default());
  let router = new_router(&mut app, registry, ServerOptions::default());
  let listener = tokio::net::TcpListener::bind(("localhost", 3000))
      .await
      .unwrap();
  axum::serve(listener, router).await?;
}
```

To omit the frontend and serve only the backend API, disable the default features:

```toml
[dependencies]
bevy_impulse_diagram_editor = { version = "0.0.1", default-features = false }
```

See the [calculator demo](../examples/diagram/calculator) for more examples.

## Local development server

Normally the web stack is not required by using this crate as a dependency, but it is required when developing the frontend.

Requirements:

* nodejs
* pnpm

First start the `dev` backend server:

```bash
pnpm dev:backend
```

then in another terminal, start the frontend `dev` server:

```bash
pnpm dev
```

When there are breaking changes in `bevy_impulse`, the typescript definitions need to be regenerated:

```bash
pnpm generate-types
```
