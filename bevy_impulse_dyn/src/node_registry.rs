use std::collections::HashMap;

use bevy_ecs::entity::Entity;
use bevy_impulse::{Builder, InputSlot, Node, Output, StreamPack};
use schemars::gen::{SchemaGenerator, SchemaSettings};
use serde::{de::DeserializeOwned, Serialize};

use crate::{DynType, MessageMetadata, OpaqueMessageSerializer, Serializer};

struct DynInputSlot {
    scope: Entity,
    source: Entity,
}

struct DynOutput {
    scope: Entity,
    target: Entity,
}

#[derive(Serialize)]
struct NodeMetadata {
    pub id: &'static str,
    pub name: &'static str,
    pub request: MessageMetadata,
    pub response: MessageMetadata,
}

/// A type erased [`bevy_impulse::Node`]
struct DynNode {
    pub input: DynInputSlot,
    pub output: DynOutput,
}

impl<Request, Response, Streams> From<Node<Request, Response, Streams>> for DynNode
where
    Response: Send + Sync + 'static,
    Streams: StreamPack,
{
    fn from(value: Node<Request, Response, Streams>) -> Self {
        Self {
            input: DynInputSlot {
                scope: value.input.scope(),
                source: value.input.id(),
            },
            output: DynOutput {
                scope: value.output.scope(),
                target: value.output.id(),
            },
        }
    }
}

pub struct NodeRegistration {
    metadata: NodeMetadata,
    create_node: Box<dyn Fn(&mut Builder) -> DynNode>,
    create_receiver: Box<dyn Fn(&mut Builder) -> DynNode>,
    create_sender: Box<dyn Fn(&mut Builder) -> DynNode>,
}

/// Serializes the node registrations as a map of node metadata.
fn serialize_node_registry_nodes<S>(
    nodes: &HashMap<&'static str, NodeRegistration>,
    s: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    s.collect_map(nodes.iter().map(|(k, v)| (*k, &v.metadata)))
}

fn serialize_node_registry_types<S>(gen: &SchemaGenerator, s: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    gen.definitions().serialize(s)
}

#[derive(Serialize)]
pub struct NodeRegistry {
    #[serde(serialize_with = "serialize_node_registry_nodes")]
    nodes: HashMap<&'static str, NodeRegistration>,

    /// List of all request and response types used in all registered nodes, this only
    /// contains serializable types, non serializable types are opaque and is only compatible
    /// with itself.
    #[serde(rename = "types", serialize_with = "serialize_node_registry_types")]
    gen: SchemaGenerator,
}

impl Default for NodeRegistry {
    fn default() -> Self {
        let mut settings = SchemaSettings::default();
        settings.definitions_path = "#/types/".to_string();
        NodeRegistry {
            nodes: HashMap::<&'static str, NodeRegistration>::default(),
            gen: SchemaGenerator::new(settings),
        }
    }
}

impl NodeRegistry {
    pub fn register_node<Request, Response, Streams>(
        &mut self,
        id: &'static str,
        name: &'static str,
        f: impl Fn(&mut Builder) -> Node<Request, Response, Streams> + 'static + Copy,
    ) -> &mut Self
    where
        Request: DynType + Serialize + DeserializeOwned + Send + Sync + 'static,
        Response: DynType + Serialize + DeserializeOwned + Send + Sync + 'static,
        Streams: StreamPack,
    {
        register_node_impl::<Request, Response, Streams, Request, Response>(id, name, self, f);
        self
    }

    pub fn register_opaque_node<Request, Response, Streams>(
        &mut self,
        id: &'static str,
        name: &'static str,
        f: impl Fn(&mut Builder) -> Node<Request, Response, Streams> + 'static + Copy,
    ) -> &mut Self
    where
        Request: Send + Sync + 'static,
        Response: Send + Sync + 'static,
        Streams: StreamPack,
    {
        register_node_impl::<
            Request,
            Response,
            Streams,
            OpaqueMessageSerializer,
            OpaqueMessageSerializer,
        >(id, name, self, f);
        self
    }

    pub fn register_opaque_request_node<Request, Response, Streams>(
        &mut self,
        id: &'static str,
        name: &'static str,
        f: impl Fn(&mut Builder) -> Node<Request, Response, Streams> + 'static + Copy,
    ) -> &mut Self
    where
        Request: Send + Sync + 'static,
        Response: DynType + Serialize + DeserializeOwned + Send + Sync + 'static,
        Streams: StreamPack,
    {
        register_node_impl::<Request, Response, Streams, OpaqueMessageSerializer, Response>(
            id, name, self, f,
        );
        self
    }

    pub fn register_opaque_response_node<Request, Response, Streams>(
        &mut self,
        id: &'static str,
        name: &'static str,
        f: impl Fn(&mut Builder) -> Node<Request, Response, Streams> + 'static + Copy,
    ) -> &mut Self
    where
        Request: DynType + Serialize + DeserializeOwned + Send + Sync + 'static,
        Response: Send + Sync + 'static,
        Streams: StreamPack,
    {
        register_node_impl::<Request, Response, Streams, Request, OpaqueMessageSerializer>(
            id, name, self, f,
        );
        self
    }

    pub fn get_registration(&self, id: &'static str) -> Option<&NodeRegistration> {
        self.nodes.get(id)
    }
}

fn register_node_impl<Request, Response, Streams, RequestSerializer, ResponseSerializer>(
    id: &'static str,
    name: &'static str,
    registry: &mut NodeRegistry,
    f: impl Fn(&mut Builder) -> Node<Request, Response, Streams> + 'static + Copy,
) where
    Request: Send + Sync + 'static,
    Response: Send + Sync + 'static,
    Streams: StreamPack,
    RequestSerializer: Serializer<Request>,
    ResponseSerializer: Serializer<Response>,
{
    let request = RequestSerializer::insert_json_schema(&mut registry.gen);
    let response = ResponseSerializer::insert_json_schema(&mut registry.gen);
    registry.nodes.insert(
        id,
        NodeRegistration {
            metadata: NodeMetadata {
                id,
                name,
                request,
                response,
            },
            create_node: Box::new(move |builder: &mut Builder| f(builder).into()),
            create_receiver: Box::new(move |builder| {
                builder
                    .create_map_block(|json: serde_json::Value| {
                        // FIXME(koonpeng): how to fail the workflow?
                        RequestSerializer::from_json(json).unwrap()
                    })
                    .into()
            }),
            create_sender: Box::new(move |builder| {
                builder
                    .create_map_block(|resp: Response| {
                        // FIXME(koonpeng): how to fail the workflow?
                        ResponseSerializer::to_json(&resp).unwrap()
                    })
                    .into()
            }),
        },
    );
}

struct DynWorkflowBuilder<'b, 'w, 's, 'a> {
    builder: &'b mut Builder<'w, 's, 'a>,
    registry: &'b NodeRegistry,
}

impl<'b, 'w, 's, 'a> DynWorkflowBuilder<'b, 'w, 's, 'a> {
    fn from_serialized_workflow() {
        panic!("TODO");
    }

    fn connect(&mut self, output: DynOutput, input: DynInputSlot) {
        // `InputSlot` and `Output` types are only used for compile time checks, if we could
        // create `InputSlot` and `Output` directly, then we wouldn't need unsafe.
        let o: Output<()> = unsafe { std::mem::transmute(output) };
        let i: InputSlot<()> = unsafe { std::mem::transmute(input) };
        self.builder.connect(o, i);
    }

    fn receive(&mut self, output: Output<serde_json::Value>, input: DynInputSlot) {
        let i: InputSlot<serde_json::Value> = unsafe { std::mem::transmute(input) };
        self.builder.connect(output, i);
    }

    fn send(&mut self, output: DynOutput, input: InputSlot<serde_json::Value>) {
        let o: Output<serde_json::Value> = unsafe { std::mem::transmute(output) };
        self.builder.connect(o, input);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bevy_impulse::{testing::TestingContext, RequestExt, Scope};

    use super::*;

    fn multiply3(i: i64) -> i64 {
        i * 3
    }

    #[test]
    fn test_register_node() {
        let mut registry = NodeRegistry::default();
        registry.register_node("multiply3", "Test Name", move |builder: &mut Builder| {
            builder.create_map_block(multiply3)
        });
        let registration = registry.get_registration("multiply3").unwrap();
        assert!(registration.metadata.request.serializable);
        assert!(registration.metadata.response.serializable);
    }

    struct NonSerializableRequest {}

    #[test]
    fn test_register_opaque_node() {
        let opaque_request_map = |_: NonSerializableRequest| {};

        let mut registry = NodeRegistry::default();
        registry.register_opaque_request_node("opaque_request_map", "Test Name", move |builder| {
            builder.create_map_block(opaque_request_map)
        });
        assert!(registry.get_registration("opaque_request_map").is_some());
        let registration = registry.get_registration("opaque_request_map").unwrap();
        assert!(!registration.metadata.request.serializable);
        assert!(registration.metadata.response.serializable);

        let opaque_response_map = |_: ()| NonSerializableRequest {};
        registry.register_opaque_response_node(
            "opaque_response_map",
            "Test Name",
            move |builder| builder.create_map_block(opaque_response_map),
        );
        assert!(registry.get_registration("opaque_response_map").is_some());
        let registration = registry.get_registration("opaque_response_map").unwrap();
        assert!(registration.metadata.request.serializable);
        assert!(!registration.metadata.response.serializable);

        let opaque_req_resp_map = |_: NonSerializableRequest| NonSerializableRequest {};
        registry.register_opaque_node("opaque_req_resp_map", "Test Name", move |builder| {
            builder.create_map_block(opaque_req_resp_map)
        });
        assert!(registry.get_registration("opaque_req_resp_map").is_some());
        let registration = registry.get_registration("opaque_req_resp_map").unwrap();
        assert!(!registration.metadata.request.serializable);
        assert!(!registration.metadata.response.serializable);
    }

    #[test]
    fn test_run_dyn_workflow() {
        let mut registry = NodeRegistry::default();
        let mut context = TestingContext::minimal_plugins();

        registry.register_node("multiply3", "multiply3", |builder| {
            builder.create_map_block(&multiply3)
        });

        let workflow = context.spawn_io_workflow(
            |scope: Scope<serde_json::Value, serde_json::Value, ()>, builder: &mut Builder| {
                let mut dyn_builder = DynWorkflowBuilder {
                    builder,
                    registry: &registry,
                };
                let builder = &mut dyn_builder.builder;

                let r = registry.get_registration("multiply3").unwrap();
                let receiver = (r.create_receiver)(builder);
                let sender = (r.create_sender)(builder);
                let n = (r.create_node)(builder);
                dyn_builder.receive(scope.input, receiver.input);
                dyn_builder.connect(receiver.output, n.input);
                dyn_builder.connect(n.output, sender.input);
                dyn_builder.send(sender.output, scope.terminate);
            },
        );

        let mut promise = context.command(|cmds| {
            cmds.request(serde_json::Value::from(4), workflow)
                .take_response()
        });
        context.run_with_conditions(&mut promise, Duration::from_secs(1));
        assert_eq!(promise.take().available().unwrap(), 12);
    }
}
