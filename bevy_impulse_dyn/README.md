# bevy_impulse_dyn

`bevy_impulse_dyn` is an extension to [`bevy_impulse`](https://crates.io/crates/bevy_impulse) adding workflow serialization features. Normally, `bevy_impulse` workflows are created from source code and fixed at compile time, `bevy_impulse_dyn` allows you to create and run dynamic workflows that can be serialized and shared.

* Serialize a workflow to json
* Validate and run a serialized workflow
* Service introspection, show a list of services that a `bevy_impulse` app can run
* Type introspection, json schema representation of types used by services

# Examples

```rs
use bevy_app::App;

fn example_service() {}

fn main() {
  let mut app = App::new();
  app.register_service(example_servicee.into_blocking_service());

  // list registered services as json
  println!(serde_json::to_string_pretty(app.service_registry()));

  // TODO(koonpeng): workflow serialization
}
