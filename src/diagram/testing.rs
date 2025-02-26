use std::error::Error;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    testing::TestingContext, Builder, RequestExt, RunCommandsOnWorldExt, Service, StreamPack,
};

use super::{
    Diagram, DiagramElementRegistry, DiagramError, DiagramStart, DiagramTerminate,
    NodeBuilderOptions,
};

pub(super) struct DiagramTestFixture {
    pub(super) context: TestingContext,
    pub(super) registry: DiagramElementRegistry,
}

impl DiagramTestFixture {
    pub(super) fn new() -> Self {
        Self {
            context: TestingContext::minimal_plugins(),
            registry: new_registry_with_basic_nodes(),
        }
    }

    pub(super) fn spawn_workflow<Streams: StreamPack>(
        &mut self,
        diagram: &Diagram,
    ) -> Result<Service<DiagramStart, DiagramTerminate, Streams>, DiagramError> {
        self.context
            .app
            .world
            .command(|cmds| diagram.spawn_workflow(cmds, &self.registry))
    }

    /// Equivalent to `self.spawn_workflow::<()>(diagram)`
    pub(super) fn spawn_io_workflow(
        &mut self,
        diagram: &Diagram,
    ) -> Result<Service<DiagramStart, DiagramTerminate, ()>, DiagramError> {
        self.spawn_workflow::<()>(diagram)
    }

    /// Spawns a workflow from a diagram then run the workflow until completion.
    /// Returns the result of the workflow.
    pub(super) fn spawn_and_run(
        &mut self,
        diagram: &Diagram,
        request: serde_json::Value,
    ) -> Result<serde_json::Value, Box<dyn Error>> {
        let workflow = self.spawn_workflow::<()>(diagram)?;
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

#[derive(Serialize, Deserialize, JsonSchema)]
struct Uncloneable<T>(T);

fn multiply3(i: i64) -> i64 {
    i * 3
}

fn multiply3_uncloneable(i: i64) -> Uncloneable<i64> {
    Uncloneable(i * 3)
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
fn new_registry_with_basic_nodes() -> DiagramElementRegistry {
    let mut registry = DiagramElementRegistry::new();
    registry
        .opt_out()
        .no_response_cloning()
        .register_node_builder(
            NodeBuilderOptions::new("multiply3_uncloneable", "i64", "Uncloneable<i64>"),
            |builder: &mut Builder, _config: ()| builder.create_map_block(multiply3_uncloneable),
        )
        .unwrap();
    registry
        .register_node_builder(
            NodeBuilderOptions::new("multiply3", "i64", "i64"),
            |builder: &mut Builder, _config: ()| builder.create_map_block(multiply3),
        )
        .unwrap();
    registry
        .register_node_builder(
            NodeBuilderOptions::new("multiply3_5", "i64", "(i64, i64)"),
            |builder: &mut Builder, _config: ()| builder.create_map_block(multiply3_5),
        )
        .unwrap()
        .with_unzip()
        .unwrap();

    registry
        .register_node_builder(
            NodeBuilderOptions::new("multiply_by", "i64", "i64"),
            |builder: &mut Builder, config: i64| builder.create_map_block(move |a: i64| a * config),
        )
        .unwrap();

    registry
        .opt_out()
        .no_request_deserializing()
        .no_response_serializing()
        .no_response_cloning()
        .register_node_builder(
            NodeBuilderOptions::new("opaque", "Unserializable", "Unserializable"),
            |builder: &mut Builder, _config: ()| builder.create_map_block(opaque),
        )
        .unwrap();
    registry
        .opt_out()
        .no_request_deserializing()
        .register_node_builder(
            NodeBuilderOptions::new("opaque_request", "Unserializable", "()"),
            |builder: &mut Builder, _config: ()| builder.create_map_block(opaque_request),
        )
        .unwrap();
    registry
        .opt_out()
        .no_response_serializing()
        .no_response_cloning()
        .register_node_builder(
            NodeBuilderOptions::new("opaque_response", "i64", "Unserializable"),
            |builder: &mut Builder, _config: ()| builder.create_map_block(opaque_response),
        )
        .unwrap();
    registry
}
