use std::{collections::HashMap, marker::PhantomData};

use bevy_ecs::{entity::Entity, world::World};
use bevy_impulse::{Builder, Node, StreamPack};
use schemars::gen::{SchemaGenerator, SchemaSettings};
use serde::Serialize;

use crate::{DynType, InferDynRequest, MessageMetadata, OpaqueMessage};

pub struct DynInputSlot {
    scope: Entity,
    source: Entity,
}

pub struct DynOutput {
    scope: Entity,
    target: Entity,
}

/// A type erased [`bevy_impulse::Node`]
pub struct DynNode {
    pub input: DynInputSlot,
    pub output: DynOutput,
}

impl<Request, Response, Streams> From<Node<Request, Response, Streams>> for DynNode
where
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    fn from(value: Node<Request, Response, Streams>) -> Self {
        DynNode {
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

#[derive(Serialize)]
pub struct NodeRegistration {
    pub id: &'static str,
    pub name: &'static str,
    pub request: MessageMetadata,
    pub response: MessageMetadata,

    #[serde(skip)]
    create_node: Box<dyn Fn(&mut Builder) -> DynNode>,
}

fn serialize_provider_registry_types<S>(gen: &SchemaGenerator, s: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    gen.definitions().serialize(s)
}

#[derive(Serialize)]
pub struct NodeRegistry {
    nodes: HashMap<&'static str, NodeRegistration>,

    /// List of all request and response types used in all registered nodes, this only
    /// contains serializable types, non serializable types are opaque and is only compatible
    /// with itself.
    #[serde(rename = "types", serialize_with = "serialize_provider_registry_types")]
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

pub struct OpaqueNode<Request, Response, SourceNode>
where
    SourceNode: Into<DynNode>,
{
    source: SourceNode,
    _unused: PhantomData<(Request, Response)>,
}

impl<Request, Response, SourceNode> From<OpaqueNode<Request, Response, SourceNode>> for DynNode
where
    SourceNode: Into<DynNode>,
{
    fn from(value: OpaqueNode<Request, Response, SourceNode>) -> Self {
        value.source.into()
    }
}

pub type FullOpaqueNode<Request, Response, Streams> =
    OpaqueNode<OpaqueMessage<Request>, OpaqueMessage<Response>, Node<Request, Response, Streams>>;

pub type OpaqueRequestNode<Request, Response, Streams> =
    OpaqueNode<OpaqueMessage<Request>, Response, Node<Request, Response, Streams>>;

pub type OpaqueResponseNode<Request, Response, Streams> =
    OpaqueNode<Request, OpaqueMessage<Response>, Node<Request, Response, Streams>>;

pub trait IntoOpaqueNodeExt<Request, Response, Streams> {
    /// Mark this provider as fully opaque, this means that both the request and response cannot
    /// be serialized. Opaque nodes can still be registered into the node registry but
    /// their request and response types are undefined and cannot be transformed.
    fn into_opaque(self) -> FullOpaqueNode<Request, Response, Streams>
    where
        Response: 'static + Send + Sync,
        Streams: StreamPack;

    /// Similar to [`OpaqueRequestExt::into_opaque`] but only mark the request as opaque.
    fn into_opaque_request(self) -> OpaqueRequestNode<Request, Response, Streams>
    where
        Response: DynType + 'static + Send + Sync,
        Streams: StreamPack;

    /// Similar to [`OpaqueRequestExt::into_opaque`] but only mark the response as opaque.
    fn into_opaque_response<InnerReq>(self) -> OpaqueResponseNode<Request, Response, Streams>
    where
        Request: InferDynRequest<InnerReq>,
        InnerReq: DynType,
        Response: 'static + Send + Sync,
        Streams: StreamPack;
}

impl<Request, Response, Streams> IntoOpaqueNodeExt<Request, Response, Streams>
    for Node<Request, Response, Streams>
where
    Streams: StreamPack,
{
    fn into_opaque(self) -> FullOpaqueNode<Request, Response, Streams>
    where
        Response: 'static + Send + Sync,
        Streams: StreamPack,
    {
        FullOpaqueNode {
            source: self,
            _unused: Default::default(),
        }
    }

    fn into_opaque_request(self) -> OpaqueRequestNode<Request, Response, Streams>
    where
        Response: DynType + 'static + Send + Sync,
        Streams: StreamPack,
    {
        OpaqueRequestNode {
            source: self,
            _unused: Default::default(),
        }
    }

    fn into_opaque_response<InnerReq>(self) -> OpaqueResponseNode<Request, Response, Streams>
    where
        Request: InferDynRequest<InnerReq>,
        InnerReq: DynType,
        Response: 'static + Send + Sync,
        Streams: StreamPack,
    {
        OpaqueResponseNode {
            source: self,
            _unused: Default::default(),
        }
    }
}

pub trait SerializableNode<M>: Into<DynNode> {
    type Request: DynType;
    type Response: DynType;
}

impl<Request, InnerReq, Response, Streams> SerializableNode<InnerReq>
    for Node<Request, Response, Streams>
where
    Request: InferDynRequest<InnerReq>,
    InnerReq: DynType,
    Response: DynType + 'static + Send + Sync,
    Streams: StreamPack,
{
    type Request = InnerReq;
    type Response = Response;
}

impl<Request, InnerReq, Response, SourceNode> SerializableNode<InnerReq>
    for OpaqueNode<Request, Response, SourceNode>
where
    Request: InferDynRequest<InnerReq>,
    InnerReq: DynType,
    Response: DynType,
    SourceNode: Into<DynNode>,
{
    type Request = InnerReq;
    type Response = Response;
}

pub trait RegisterNodeExt {
    fn register_node<N, M>(
        &mut self,
        id: &'static str,
        name: &'static str,
        f: impl Fn(&mut Builder) -> N + 'static,
    ) -> &mut Self
    where
        N: SerializableNode<M>;

    fn node_registration(&mut self, id: &'static str) -> Option<&NodeRegistration>;
}

impl RegisterNodeExt for World {
    fn register_node<N, M>(
        &mut self,
        id: &'static str,
        name: &'static str,
        f: impl Fn(&mut Builder) -> N + 'static,
    ) -> &mut Self
    where
        N: SerializableNode<M>,
    {
        self.init_non_send_resource::<NodeRegistry>(); // nothing happens if the resource already exist
        let mut registry = self.non_send_resource_mut::<NodeRegistry>();
        let request = N::Request::insert_json_schema(&mut registry.gen);
        let response = N::Response::insert_json_schema(&mut registry.gen);
        let create_node = Box::new(move |builder: &mut Builder| f(builder).into());
        registry.nodes.insert(
            id,
            NodeRegistration {
                id,
                name,
                request,
                response,
                create_node,
            },
        );
        self
    }

    fn node_registration(&mut self, id: &'static str) -> Option<&NodeRegistration> {
        match self.get_non_send_resource::<NodeRegistry>() {
            Some(registry) => registry.nodes.get(id),
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use bevy_app::App;
    use bevy_impulse::BlockingMap;

    use super::*;

    #[test]
    fn test_register_node() {
        let test_map = |_: BlockingMap<()>| {};

        let mut app = App::new();
        app.world
            .register_node("test_node", "Test Name", move |builder: &mut Builder| {
                builder.create_map_block(test_map)
            });
        let registration = app.world.node_registration("test_node").unwrap();
        assert!(registration.request.serializable);
        assert!(registration.response.serializable);
    }

    struct NonSerializableRequest {}

    #[test]
    fn test_register_opaque_node() {
        let opaque_request_map = |_: BlockingMap<NonSerializableRequest>| {};

        let mut app = App::new();
        app.world
            .register_node("opaque_request_map", "Test Name", move |builder| {
                builder
                    .create_map_block(opaque_request_map)
                    .into_opaque_request()
            });
        assert!(app.world.node_registration("opaque_request_map").is_some());
        let registration = app.world.node_registration("opaque_request_map").unwrap();
        assert!(!registration.request.serializable);
        assert!(registration.response.serializable);

        let opaque_response_map = |_: BlockingMap<()>| NonSerializableRequest {};
        app.world
            .register_node("opaque_response_map", "Test Name", move |builder| {
                builder
                    .create_map_block(opaque_response_map)
                    .into_opaque_response()
            });
        assert!(app.world.node_registration("opaque_response_map").is_some());
        let registration = app.world.node_registration("opaque_response_map").unwrap();
        assert!(registration.request.serializable);
        assert!(!registration.response.serializable);

        let opaque_req_resp_map =
            |_: BlockingMap<NonSerializableRequest>| NonSerializableRequest {};
        app.world
            .register_node("opaque_req_resp_map", "Test Name", move |builder| {
                builder.create_map_block(opaque_req_resp_map).into_opaque()
            });
        assert!(app.world.node_registration("opaque_req_resp_map").is_some());
        let registration = app.world.node_registration("opaque_req_resp_map").unwrap();
        assert!(!registration.request.serializable);
        assert!(!registration.response.serializable);
    }
}
