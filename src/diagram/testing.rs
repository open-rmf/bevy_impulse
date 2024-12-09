use crate::{Builder, Promise};

use super::NodeRegistry;

pub(super) fn unwrap_promise<T>(mut p: Promise<T>) -> T {
    let taken = p.take();
    if taken.is_available() {
        taken.available().unwrap()
    } else {
        panic!("{:?}", taken.cancellation().unwrap())
    }
}

fn multiply3(i: i64) -> i64 {
    i * 3
}

fn multiply3_5(x: i64) -> (i64, i64) {
    (x * 3, x * 5)
}

struct Unserializable;

fn opaque(_: Unserializable) -> Unserializable {
    Unserializable {}
}

fn opaque_request(_: Unserializable) {}

fn opaque_response(_: i64) -> Unserializable {
    Unserializable {}
}

/// create a new node registry with some basic nodes registered
pub(super) fn new_registry_with_basic_nodes() -> NodeRegistry {
    let mut registry = NodeRegistry::default();
    registry.register_node(
        "multiply3",
        "multiply3",
        |builder: &mut Builder, _config: ()| builder.create_map_block(multiply3),
    );
    registry
        .registration_builder()
        .with_response_cloneable()
        .register_node(
            "multiply3_cloneable",
            "multiply3_cloneable",
            |builder: &mut Builder, _config: ()| builder.create_map_block(multiply3),
        );
    registry
        .registration_builder()
        .with_unzippable()
        .register_node(
            "multiply3_5",
            "multiply3_5",
            |builder: &mut Builder, _config: ()| builder.create_map_block(multiply3_5),
        );

    registry.register_node(
        "multiplyBy",
        "multiplyBy",
        |builder: &mut Builder, config: i64| builder.create_map_block(move |a: i64| a * config),
    );

    registry
        .registration_builder()
        .with_opaque_request()
        .with_opaque_response()
        .register_node("opaque", "opaque", |builder: &mut Builder, _config: ()| {
            builder.create_map_block(opaque)
        });
    registry
        .registration_builder()
        .with_opaque_request()
        .register_node(
            "opaque_request",
            "opaque_request",
            |builder: &mut Builder, _config: ()| builder.create_map_block(opaque_request),
        );
    registry
        .registration_builder()
        .with_opaque_response()
        .register_node(
            "opaque_response",
            "opaque_response",
            |builder: &mut Builder, _config: ()| builder.create_map_block(opaque_response),
        );
    registry
}
