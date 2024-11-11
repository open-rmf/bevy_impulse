use std::{borrow::Borrow, collections::HashMap};

use crate::{Builder, InputSlot, Node, Output, StreamPack};
use bevy_ecs::entity::Entity;
use schemars::gen::{SchemaGenerator, SchemaSettings};
use serde::{de::DeserializeOwned, Serialize};

use crate::{DynType, MessageMetadata, OpaqueMessageSerializer, Serializer};

/// A type erased [`bevy_impulse::InputSlot`]
#[derive(Copy, Clone)]
pub(crate) struct DynInputSlot {
    #[allow(unused)]
    scope: Entity,

    #[allow(unused)]
    source: Entity,
}

impl DynInputSlot {
    pub(crate) fn into_input<T>(self) -> InputSlot<T> {
        InputSlot::<T>::new(self.scope, self.source)
    }
}

impl<T> From<InputSlot<T>> for DynInputSlot {
    fn from(value: InputSlot<T>) -> Self {
        DynInputSlot {
            scope: value.scope(),
            source: value.id(),
        }
    }
}

/// A type erased [`bevy_impulse::Output`]
pub(crate) struct DynOutput {
    #[allow(unused)]
    scope: Entity,

    #[allow(unused)]
    target: Entity,
}

impl DynOutput {
    pub(crate) fn into_output<T>(self) -> Output<T>
    where
        T: Send + Sync + 'static,
    {
        Output::<T>::new(self.scope, self.target)
    }
}

impl<T> From<Output<T>> for DynOutput
where
    T: Send + Sync + 'static,
{
    fn from(value: Output<T>) -> Self {
        DynOutput {
            scope: value.scope(),
            target: value.id(),
        }
    }
}

#[derive(Serialize)]
pub(crate) struct NodeMetadata {
    pub id: &'static str,
    pub name: &'static str,
    pub request: MessageMetadata,
    pub response: MessageMetadata,
}

/// A type erased [`bevy_impulse::Node`]
pub(crate) struct DynNode {
    pub(crate) input: DynInputSlot,
    pub(crate) output: DynOutput,
}

impl<Request, Response, Streams> From<Node<Request, Response, Streams>> for DynNode
where
    Response: Send + Sync + 'static,
    Streams: StreamPack,
{
    fn from(node: Node<Request, Response, Streams>) -> Self {
        Self {
            input: node.input.into(),
            output: node.output.into(),
        }
    }
}

pub(crate) struct NodeRegistration {
    pub(crate) metadata: NodeMetadata,

    /// Creates an instance of the registered node.
    pub(crate) create_node: Box<dyn FnMut(&mut Builder) -> DynNode>,

    /// Creates a node that deserializes a [`serde_json::Value`] into the registered node input.
    pub(crate) create_receiver: Box<dyn FnMut(&mut Builder) -> DynNode>,

    /// Creates a node that serializes the registered node's output to a [`serde_json::Value`].
    pub(crate) create_sender: Box<dyn FnMut(&mut Builder) -> DynNode>,
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
            nodes: Default::default(),
            gen: SchemaGenerator::new(settings),
        }
    }
}

impl NodeRegistry {
    pub fn register_node<Request, Response, Streams>(
        &mut self,
        id: &'static str,
        name: &'static str,
        f: impl FnMut(&mut Builder) -> Node<Request, Response, Streams> + 'static + Copy,
    ) -> &mut Self
    where
        Request: DynType + Serialize + DeserializeOwned + Send + Sync + 'static,
        Response: DynType + Serialize + DeserializeOwned + Send + Sync + 'static,
        Streams: StreamPack,
    {
        register_node_impl::<Request, Response, Streams, Request, Response>(id, name, self, f);
        self
    }

    /// Registers an opaque node, an opaque node is a node where the request and responses cannot
    /// be serialized/deserialized. Because of that, opaque nodes can only be connected to
    /// another opaque node.
    pub fn register_opaque_node<Request, Response, Streams>(
        &mut self,
        id: &'static str,
        name: &'static str,
        f: impl FnMut(&mut Builder) -> Node<Request, Response, Streams> + 'static + Copy,
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

    /// Similar to [`Self::register_opaque_node`] but only the response is opaque, this allows
    /// non-opaque nodes to connect to this node.
    pub fn register_opaque_request_node<Request, Response, Streams>(
        &mut self,
        id: &'static str,
        name: &'static str,
        f: impl FnMut(&mut Builder) -> Node<Request, Response, Streams> + 'static + Copy,
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

    /// Similar to [`Self::register_opaque_node`] but only the request is opaque, this allows
    /// the node to connect to non-opaque nodes.
    pub fn register_opaque_response_node<Request, Response, Streams>(
        &mut self,
        id: &'static str,
        name: &'static str,
        f: impl FnMut(&mut Builder) -> Node<Request, Response, Streams> + 'static + Copy,
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

    pub(crate) fn get_registration<Q>(&self, id: &Q) -> Option<&NodeRegistration>
    where
        Q: Borrow<str> + ?Sized,
    {
        self.nodes.get(id.borrow())
    }

    pub(crate) fn create_node<Q>(&mut self, id: &Q, builder: &mut Builder) -> Option<DynNode>
    where
        Q: Borrow<str> + ?Sized,
    {
        Some((self.nodes.get_mut(id.borrow())?.create_node)(builder))
    }

    pub(crate) fn create_receiver<Q>(&mut self, id: &Q, builder: &mut Builder) -> Option<DynNode>
    where
        Q: Borrow<str> + ?Sized,
    {
        Some((self.nodes.get_mut(id.borrow())?.create_receiver)(builder))
    }

    pub(crate) fn create_sender<Q>(&mut self, id: &Q, builder: &mut Builder) -> Option<DynNode>
    where
        Q: Borrow<str> + ?Sized,
    {
        Some((self.nodes.get_mut(id.borrow())?.create_sender)(builder))
    }
}

fn register_node_impl<Request, Response, Streams, RequestSerializer, ResponseSerializer>(
    id: &'static str,
    name: &'static str,
    registry: &mut NodeRegistry,
    mut f: impl FnMut(&mut Builder) -> Node<Request, Response, Streams> + 'static + Copy,
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
            create_node: Box::new(move |builder: &mut Builder| {
                let n = f(builder);
                DynNode::from(n)
            }),
            create_receiver: Box::new(move |builder| {
                let n = builder.create_map_block(|json: serde_json::Value| {
                    // FIXME(koonpeng): how to fail the workflow?
                    RequestSerializer::from_json(json).unwrap()
                });
                DynNode::from(n)
            }),
            create_sender: Box::new(move |builder| {
                let n = builder.create_map_block(|resp: Response| {
                    // FIXME(koonpeng): how to fail the workflow?
                    ResponseSerializer::to_json(&resp).unwrap()
                });
                DynNode::from(n)
            }),
        },
    );
}

#[cfg(test)]
mod tests {
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
}
