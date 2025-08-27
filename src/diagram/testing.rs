use std::error::Error;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    testing::TestingContext, Builder, JsonMessage, RequestExt, RunCommandsOnWorldExt, Service,
    StreamPack,
};

use super::{Diagram, DiagramElementRegistry, DiagramError, NodeBuilderOptions};

pub(crate) struct DiagramTestFixture {
    pub(crate) context: TestingContext,
    pub(crate) registry: DiagramElementRegistry,
}

impl DiagramTestFixture {
    pub(crate) fn new() -> Self {
        Self {
            context: TestingContext::minimal_plugins(),
            registry: new_registry_with_basic_nodes(),
        }
    }

    pub(crate) fn spawn_json_io_workflow(
        &mut self,
        diagram: &Diagram,
    ) -> Result<Service<JsonMessage, JsonMessage>, DiagramError> {
        self.spawn_io_workflow::<JsonMessage, JsonMessage>(diagram)
    }

    /// Equivalent to `self.spawn_workflow::<JsonMessage, JsonMessage>(diagram)`
    pub(crate) fn spawn_io_workflow<Request, Response>(
        &mut self,
        diagram: &Diagram,
    ) -> Result<Service<Request, Response, ()>, DiagramError>
    where
        Request: 'static + Send + Sync,
        Response: 'static + Send + Sync,
    {
        self.spawn_workflow(diagram)
    }

    pub fn spawn_workflow<Request, Response, Streams>(
        &mut self,
        diagram: &Diagram,
    ) -> Result<Service<Request, Response, Streams>, DiagramError>
    where
        Request: 'static + Send + Sync,
        Response: 'static + Send + Sync,
        Streams: StreamPack,
    {
        self.context
            .app
            .world
            .command(|cmds| diagram.spawn_workflow(cmds, &self.registry))
    }

    /// Spawns a workflow from a diagram then run the workflow until completion.
    /// Returns the result of the workflow.
    pub fn spawn_and_run<Request, Response>(
        &mut self,
        diagram: &Diagram,
        request: Request,
    ) -> Result<Response, Box<dyn Error>>
    where
        Request: 'static + Send + Sync,
        Response: 'static + Send + Sync,
    {
        self.spawn_and_run_with_streams::<_, _, ()>(diagram, request)
            .map(|(response, _)| response)
    }

    pub fn spawn_and_run_with_streams<Request, Response, Streams>(
        &mut self,
        diagram: &Diagram,
        request: Request,
    ) -> Result<(Response, Streams::StreamReceivers), Box<dyn Error>>
    where
        Request: 'static + Send + Sync,
        Response: 'static + Send + Sync,
        Streams: StreamPack,
    {
        let workflow = self.spawn_workflow::<_, _, Streams>(diagram)?;
        let mut recipient = self
            .context
            .command(|cmds| cmds.request(request, workflow).take());
        self.context.run_while_pending(&mut recipient.response);
        assert!(
            self.context.no_unhandled_errors(),
            "{:#?}",
            self.context.get_unhandled_errors()
        );
        let taken = recipient.response.take();
        if taken.is_available() {
            Ok((taken.available().unwrap(), recipient.streams))
        } else if taken.is_cancelled() {
            let cancellation = taken.cancellation().unwrap();
            Err(cancellation.clone().into())
        } else {
            Err(String::from("promise is in invalid state").into())
        }
    }
}

#[derive(Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
struct Uncloneable<T>(T);

fn multiply3(i: i64) -> i64 {
    i * 3
}

fn multiply3_uncloneable(i: Uncloneable<i64>) -> Uncloneable<i64> {
    Uncloneable(i.0 * 3)
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
    registry.opt_out().no_cloning().register_node_builder(
        NodeBuilderOptions::new("multiply3_uncloneable"),
        |builder: &mut Builder, _config: ()| builder.create_map_block(multiply3_uncloneable),
    );
    registry.register_node_builder(
        NodeBuilderOptions::new("multiply3"),
        |builder: &mut Builder, _config: ()| builder.create_map_block(multiply3),
    );
    registry
        .register_node_builder(
            NodeBuilderOptions::new("multiply3_5"),
            |builder: &mut Builder, _config: ()| builder.create_map_block(multiply3_5),
        )
        .with_unzip();

    registry.register_node_builder(
        NodeBuilderOptions::new("multiply_by"),
        |builder: &mut Builder, config: i64| builder.create_map_block(move |a: i64| a * config),
    );

    registry.register_node_builder(NodeBuilderOptions::new("add_to"), |builder, config: i64| {
        builder.create_map_block(move |a: i64| a + config)
    });

    registry.register_node_builder(
        NodeBuilderOptions::new("less_than"),
        |builder, config: u64| {
            builder.create_map_block(move |a: u64| {
                if a < config {
                    Ok(a)
                } else {
                    Err(a)
                }
            })
        })
        .with_fork_result();

    registry
        .opt_out()
        .no_deserializing()
        .no_serializing()
        .no_cloning()
        .register_node_builder(
            NodeBuilderOptions::new("opaque"),
            |builder: &mut Builder, _config: ()| builder.create_map_block(opaque),
        );
    registry
        .opt_out()
        .no_deserializing()
        .no_serializing()
        .no_cloning()
        .register_node_builder(
            NodeBuilderOptions::new("opaque_request"),
            |builder: &mut Builder, _config: ()| builder.create_map_block(opaque_request),
        );
    registry
        .opt_out()
        .no_deserializing()
        .no_serializing()
        .no_cloning()
        .register_node_builder(
            NodeBuilderOptions::new("opaque_response"),
            |builder: &mut Builder, _config: ()| builder.create_map_block(opaque_response),
        );
    registry
}
