use std::{borrow::Borrow, collections::HashMap, marker::PhantomData};

use crate::{Builder, InputSlot, Node, Output, StreamPack};
use bevy_ecs::entity::Entity;
use schemars::gen::{SchemaGenerator, SchemaSettings};
use serde::Serialize;

use crate::{RequestMetadata, SerializeMessage};

use super::{
    DefaultDeserializer, DefaultSerializer, DeserializeMessage, OpaqueMessageDeserializer,
    OpaqueMessageSerializer, ResponseMetadata,
};

/// A type erased [`bevy_impulse::InputSlot`]
#[derive(Copy, Clone)]
pub(super) struct DynInputSlot {
    scope: Entity,
    source: Entity,
}

impl DynInputSlot {
    pub(super) fn into_input<T>(self) -> InputSlot<T> {
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
pub(super) struct DynOutput {
    scope: Entity,
    target: Entity,
}

impl DynOutput {
    pub(super) fn into_output<T>(self) -> Output<T>
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
pub(super) struct NodeMetadata {
    pub(super) id: &'static str,
    pub(super) name: &'static str,
    pub(super) request: RequestMetadata,
    pub(super) response: ResponseMetadata,
}

/// A type erased [`bevy_impulse::Node`]
pub(super) struct DynNode {
    pub(super) input: DynInputSlot,
    pub(super) output: DynOutput,
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

pub(super) struct NodeRegistration {
    pub(super) metadata: NodeMetadata,

    /// Creates an instance of the registered node.
    pub(super) create_node: Box<dyn FnMut(&mut Builder) -> DynNode>,

    /// Creates a node that deserializes a [`serde_json::Value`] into the registered node input.
    pub(super) create_receiver: Box<dyn FnMut(&mut Builder) -> DynNode>,

    /// Creates a node that serializes the registered node's output to a [`serde_json::Value`].
    pub(super) create_sender: Box<dyn FnMut(&mut Builder) -> DynNode>,
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

pub struct RegistrationBuilder<NodeT, Deserializer, Serializer, const RESPONSE_CLONEABLE: bool> {
    node: NodeT,
    _unused: PhantomData<(Deserializer, Serializer)>,
}

impl<NodeT, Deserializer, Serializer, const RESPONSE_CLONEABLE: bool>
    RegistrationBuilder<NodeT, Deserializer, Serializer, RESPONSE_CLONEABLE>
{
    pub fn with_opaque_request(
        self,
    ) -> RegistrationBuilder<NodeT, OpaqueMessageDeserializer, Serializer, RESPONSE_CLONEABLE> {
        RegistrationBuilder {
            node: self.node,
            _unused: Default::default(),
        }
    }

    pub fn with_opaque_response(
        self,
    ) -> RegistrationBuilder<NodeT, Deserializer, OpaqueMessageSerializer, RESPONSE_CLONEABLE> {
        RegistrationBuilder {
            node: self.node,
            _unused: Default::default(),
        }
    }

    pub fn with_response_cloneable(
        self,
    ) -> RegistrationBuilder<NodeT, Deserializer, Serializer, true> {
        RegistrationBuilder {
            node: self.node,
            _unused: Default::default(),
        }
    }
}

pub trait IntoRegistrationBuilder<NodeT, Deserializer, Serializer, const RESPONSE_CLONEABLE: bool> {
    fn into_registration_builder(
        self,
    ) -> RegistrationBuilder<NodeT, Deserializer, Serializer, RESPONSE_CLONEABLE>;
}

impl<NodeT, Deserializer, Serializer, const RESPONSE_CLONEABLE: bool>
    IntoRegistrationBuilder<NodeT, Deserializer, Serializer, RESPONSE_CLONEABLE>
    for RegistrationBuilder<NodeT, Deserializer, Serializer, RESPONSE_CLONEABLE>
{
    fn into_registration_builder(
        self,
    ) -> RegistrationBuilder<NodeT, Deserializer, Serializer, RESPONSE_CLONEABLE> {
        self
    }
}

impl<Request, Response, Streams>
    IntoRegistrationBuilder<
        Node<Request, Response, Streams>,
        DefaultDeserializer,
        DefaultSerializer,
        false,
    > for Node<Request, Response, Streams>
where
    Streams: StreamPack,
{
    fn into_registration_builder(
        self,
    ) -> RegistrationBuilder<
        Node<Request, Response, Streams>,
        DefaultDeserializer,
        DefaultSerializer,
        false,
    > {
        RegistrationBuilder {
            node: self,
            _unused: Default::default(),
        }
    }
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
    pub fn register_node<
        IntoRegistrationBuilderT,
        Request,
        Response,
        Streams,
        Deserializer,
        Serializer,
        const RESPONSE_CLONEABLE: bool,
    >(
        &mut self,
        id: &'static str,
        name: &'static str,
        mut f: impl FnMut(&mut Builder) -> IntoRegistrationBuilderT + 'static,
    ) -> &mut Self
    where
        IntoRegistrationBuilderT: IntoRegistrationBuilder<
            Node<Request, Response, Streams>,
            Deserializer,
            Serializer,
            RESPONSE_CLONEABLE,
        >,
        Request: Send + Sync + 'static,
        Response: Send + Sync + 'static,
        Streams: StreamPack,
        Deserializer: DeserializeMessage<Request>,
        Serializer: SerializeMessage<Response>,
    {
        let request = RequestMetadata {
            r#type: Deserializer::type_name(),
            deserializable: Deserializer::deserializable(),
        };
        let response = ResponseMetadata {
            r#type: Serializer::type_name(),
            serializable: Serializer::serializable(),
            cloneable: RESPONSE_CLONEABLE,
        };

        self.nodes.insert(
            id,
            NodeRegistration {
                metadata: NodeMetadata {
                    id,
                    name,
                    request,
                    response,
                },
                create_node: Box::new(move |builder: &mut Builder| {
                    let n = f(builder).into_registration_builder().node;
                    DynNode::from(n)
                }),
                create_receiver: Box::new(move |builder| {
                    let n = builder.create_map_block(|json: serde_json::Value| {
                        // FIXME(koonpeng): how to fail the workflow?
                        Deserializer::from_json(json).unwrap()
                    });
                    DynNode::from(n)
                }),
                create_sender: Box::new(move |builder| {
                    let n = builder.create_map_block(|resp: Response| {
                        // FIXME(koonpeng): how to fail the workflow?
                        Serializer::to_json(&resp).unwrap()
                    });
                    DynNode::from(n)
                }),
            },
        );
        self
    }

    pub(super) fn get_registration<Q>(&self, id: &Q) -> Option<&NodeRegistration>
    where
        Q: Borrow<str> + ?Sized,
    {
        self.nodes.get(id.borrow())
    }

    pub(super) fn create_node<Q>(&mut self, id: &Q, builder: &mut Builder) -> Option<DynNode>
    where
        Q: Borrow<str> + ?Sized,
    {
        Some((self.nodes.get_mut(id.borrow())?.create_node)(builder))
    }

    pub(super) fn create_receiver<Q>(&mut self, id: &Q, builder: &mut Builder) -> Option<DynNode>
    where
        Q: Borrow<str> + ?Sized,
    {
        Some((self.nodes.get_mut(id.borrow())?.create_receiver)(builder))
    }

    pub(super) fn create_sender<Q>(&mut self, id: &Q, builder: &mut Builder) -> Option<DynNode>
    where
        Q: Borrow<str> + ?Sized,
    {
        Some((self.nodes.get_mut(id.borrow())?.create_sender)(builder))
    }
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
        assert!(registration.metadata.request.deserializable);
        assert!(registration.metadata.response.serializable);
        assert!(!registration.metadata.response.cloneable);
    }

    #[test]
    fn test_register_cloneable_node() {
        let mut registry = NodeRegistry::default();
        registry.register_node("multiply3", "Test Name", move |builder: &mut Builder| {
            builder
                .create_map_block(multiply3)
                .into_registration_builder()
                .with_response_cloneable()
        });
        let registration = registry.get_registration("multiply3").unwrap();
        assert!(registration.metadata.request.deserializable);
        assert!(registration.metadata.response.serializable);
        assert!(registration.metadata.response.cloneable);
    }

    struct NonSerializableRequest {}

    #[test]
    fn test_register_opaque_node() {
        let opaque_request_map = |_: NonSerializableRequest| {};

        let mut registry = NodeRegistry::default();
        registry.register_node("opaque_request_map", "Test Name", move |builder| {
            builder
                .create_map_block(opaque_request_map)
                .into_registration_builder()
                .with_opaque_request()
        });
        assert!(registry.get_registration("opaque_request_map").is_some());
        let registration = registry.get_registration("opaque_request_map").unwrap();
        assert!(!registration.metadata.request.deserializable);
        assert!(registration.metadata.response.serializable);
        assert!(!registration.metadata.response.cloneable);

        let opaque_response_map = |_: ()| NonSerializableRequest {};
        registry.register_node("opaque_response_map", "Test Name", move |builder| {
            builder
                .create_map_block(opaque_response_map)
                .into_registration_builder()
                .with_opaque_response()
        });
        assert!(registry.get_registration("opaque_response_map").is_some());
        let registration = registry.get_registration("opaque_response_map").unwrap();
        assert!(registration.metadata.request.deserializable);
        assert!(!registration.metadata.response.serializable);
        assert!(!registration.metadata.response.cloneable);

        let opaque_req_resp_map = |_: NonSerializableRequest| NonSerializableRequest {};
        registry.register_node("opaque_req_resp_map", "Test Name", move |builder| {
            builder
                .create_map_block(opaque_req_resp_map)
                .into_registration_builder()
                .with_opaque_request()
                .with_opaque_response()
        });
        assert!(registry.get_registration("opaque_req_resp_map").is_some());
        let registration = registry.get_registration("opaque_req_resp_map").unwrap();
        assert!(!registration.metadata.request.deserializable);
        assert!(!registration.metadata.response.serializable);
        assert!(!registration.metadata.response.cloneable);
    }
}
