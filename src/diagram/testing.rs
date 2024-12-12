use std::error::Error;

use crate::{testing::TestingContext, Builder, RequestExt};

use super::{Diagram, NodeRegistry};

pub(super) struct DiagramTestFixture {
    pub(super) context: TestingContext,
    pub(super) registry: NodeRegistry,
}

impl DiagramTestFixture {
    pub(super) fn new() -> Self {
        Self {
            context: TestingContext::minimal_plugins(),
            registry: new_registry_with_basic_nodes(),
        }
    }

    /// Spawns a workflow from a diagram then run the workflow until completion.
    /// Returns the result of the workflow.
    pub(super) fn spawn_and_run(
        &mut self,
        diagram: &Diagram,
        request: serde_json::Value,
    ) -> Result<serde_json::Value, Box<dyn Error>> {
        let workflow = diagram.spawn_io_workflow(&mut self.context.app, &self.registry)?;
        let mut promise = self
            .context
            .command(|cmds| cmds.request(request, workflow).take_response());
        self.context.run_while_pending(&mut promise);
        let taken = promise.take();
        if taken.is_available() {
            Ok(taken.available().unwrap())
        } else if taken.is_cancelled() {
            let cancellation = taken.cancellation().unwrap();
            Err(cancellation.clone().into())
        } else {
            Err(String::from("promise is in invalid state").into())
        }
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
fn new_registry_with_basic_nodes() -> NodeRegistry {
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
