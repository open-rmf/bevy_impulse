use bevy_impulse_diagram_editor::api::{
    executor::{DebugSessionEnd, PostRunRequest},
    RegistryResponse,
};
use indexmap::IndexMap;
use schemars::SchemaGenerator;

fn main() {
    let mut schema_generator = SchemaGenerator::default();
    schema_generator.subschema_for::<PostRunRequest>();
    schema_generator.subschema_for::<DebugSessionEnd>();
    schema_generator.subschema_for::<RegistryResponse>();

    // using `IndexMap` to preserve ordering
    let schema: IndexMap<&'static str, serde_json::Value> = IndexMap::from_iter([
        (
            "$schema",
            "https://json-schema.org/draft/2020-12/schema".into(),
        ),
        (
            "$defs",
            serde_json::to_value(schema_generator.definitions()).unwrap(),
        ),
    ]);

    println!("{}", serde_json::to_string_pretty(&schema).unwrap(),);
}
