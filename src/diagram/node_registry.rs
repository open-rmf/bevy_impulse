use std::{borrow::Borrow, cell::RefCell, collections::HashMap, marker::PhantomData};

use crate::{Builder, InputSlot, Node, Output, StreamPack};
use bevy_ecs::entity::Entity;
use bevy_utils::all_tuples_with_size;
use log::debug;
use schemars::gen::{SchemaGenerator, SchemaSettings};
use serde::Serialize;

use crate::{RequestMetadata, SerializeMessage};

use super::{
    DefaultDeserializer, DefaultSerializer, DeserializeMessage, DiagramError,
    OpaqueMessageDeserializer, OpaqueMessageSerializer, ResponseMetadata, ScopeTerminate,
};

/// A type erased [`bevy_impulse::InputSlot`]
#[derive(Copy, Clone)]
pub struct DynInputSlot {
    scope: Entity,
    source: Entity,

    /// The rust type name of the input (not the json schema type name).
    pub(super) type_name: &'static str,
}

impl DynInputSlot {
    pub(super) fn into_input<T>(self) -> InputSlot<T> {
        InputSlot::<T>::new(self.scope, self.source)
    }

    pub(super) fn id(&self) -> Entity {
        self.source
    }
}

impl<T> From<InputSlot<T>> for DynInputSlot {
    fn from(input: InputSlot<T>) -> Self {
        Self {
            scope: input.scope(),
            source: input.id(),
            type_name: std::any::type_name::<T>(),
        }
    }
}

/// A type erased [`bevy_impulse::Output`]
pub struct DynOutput {
    scope: Entity,
    target: Entity,

    /// The rust type name of the output (not the json schema type name).
    pub(super) type_name: &'static str,
}

impl DynOutput {
    pub(super) fn into_output<T>(self) -> Output<T>
    where
        T: Send + Sync + 'static,
    {
        Output::<T>::new(self.scope, self.target)
    }

    pub(super) fn id(&self) -> Entity {
        self.target
    }
}

impl<T> From<Output<T>> for DynOutput
where
    T: Send + Sync + 'static,
{
    fn from(output: Output<T>) -> Self {
        Self {
            scope: output.scope(),
            target: output.id(),
            type_name: std::any::type_name::<T>(),
        }
    }
}

#[derive(Clone, Serialize)]
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

pub struct NodeRegistration {
    pub(super) metadata: NodeMetadata,

    /// Creates an instance of the registered node.
    create_node_impl: RefCell<Box<dyn FnMut(&mut Builder) -> DynNode>>,

    /// Creates a node that deserializes a [`serde_json::Value`] into the registered node input.
    create_receiver_impl: Option<RefCell<Box<dyn FnMut(&mut Builder) -> DynNode>>>,

    /// Creates a node that serializes the registered node's output to a [`serde_json::Value`].
    create_sender_impl: Option<RefCell<Box<dyn FnMut(&mut Builder) -> DynNode>>>,

    fork_clone_impl: Option<
        // RefCell to a Box to a Fn that returns Result<(), DiagramError>
        RefCell<
            Box<dyn Fn(&mut Builder, DynOutput, usize) -> Result<Vec<DynOutput>, DiagramError>>,
        >,
    >,

    unzip_impl: Option<
        // RefCell to a Box to a Fn that returns Result<(), DiagramError>
        RefCell<Box<dyn Fn(&mut Builder, DynOutput) -> Result<Vec<DynOutput>, DiagramError>>>,
    >,
}

impl NodeRegistration {
    pub(super) fn create_node(&self, builder: &mut Builder) -> DynNode {
        let n = (self.create_node_impl.borrow_mut())(builder);
        debug!(
            "create node [{}], output: [{:?}], input: [{:?}]",
            self.metadata.id, n.output.target, n.input.source
        );
        n
    }

    pub(super) fn create_receiver(&self, builder: &mut Builder) -> Result<DynNode, DiagramError> {
        let f = self
            .create_receiver_impl
            .as_ref()
            .ok_or(DiagramError::NotSerializable)?;
        let n = (f.borrow_mut())(builder);
        debug!(
            "create receiver [{}], output: [{:?}], input: [{:?}]",
            self.metadata.id, n.output.target, n.input.source
        );
        Ok(n)
    }

    pub(super) fn create_sender(&self, builder: &mut Builder) -> Result<DynNode, DiagramError> {
        let f = self
            .create_sender_impl
            .as_ref()
            .ok_or(DiagramError::NotSerializable)?;
        let n = (f.borrow_mut())(builder);
        debug!(
            "create sender [{}], output: [{:?}], input: [{:?}]",
            self.metadata.id, n.output.target, n.input.source
        );
        Ok(n)
    }

    pub(super) fn fork_clone(
        &self,
        builder: &mut Builder,
        output: DynOutput,
        amount: usize,
    ) -> Result<Vec<DynOutput>, DiagramError> {
        let f = self
            .fork_clone_impl
            .as_ref()
            .ok_or(DiagramError::NotCloneable)?;
        (f.borrow_mut())(builder, output, amount)
    }

    pub(super) fn unzip(
        &self,
        builder: &mut Builder,
        output: DynOutput,
    ) -> Result<Vec<DynOutput>, DiagramError> {
        let f = self
            .unzip_impl
            .as_ref()
            .ok_or(DiagramError::NotUnzippable)?;
        (f.borrow_mut())(builder, output)
    }
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

pub trait DynUnzip {
    const UNZIP_SLOTS: usize;
    type Response;

    fn unzip(self, builder: &mut Builder) -> Result<Vec<DynOutput>, DiagramError>;
}

macro_rules! dyn_unzip_impl {
    ($len:literal, $(($P:ident, $o:ident)),*) => {
        impl<$($P),*> DynUnzip for Output<($($P,)*)> where $($P: Send + Sync + 'static),* {
            const UNZIP_SLOTS: usize = $len;
            type Response = ($($P,)*);

            fn unzip(
                self,
                builder: &mut Builder,
            ) -> Result<Vec<DynOutput>, DiagramError> {
                let mut outputs: Vec<DynOutput> = Vec::with_capacity($len);
                let chain = self.chain(builder);
                let ($($o,)*) = chain.unzip();

                $({
                    outputs.push($o.into());
                })*

                Ok(outputs)
            }
        }
    };
}

all_tuples_with_size!(dyn_unzip_impl, 1, 12, R, o);

pub struct RegistrationBuilder<NodeBuilderT, Deserializer, Serializer> {
    build_node: NodeBuilderT,
    fork_clone: Option<
        RefCell<
            Box<dyn Fn(&mut Builder, DynOutput, usize) -> Result<Vec<DynOutput>, DiagramError>>,
        >,
    >,
    unzip: Option<
        RefCell<Box<dyn Fn(&mut Builder, DynOutput) -> Result<Vec<DynOutput>, DiagramError>>>,
    >,
    /// The number of inputs that the output can be unzipped to
    unzip_slots: usize,

    _unused: PhantomData<(Deserializer, Serializer)>,
}

impl<NodeBuilderT, Request, Response, Streams, Deserializer, Serializer>
    RegistrationBuilder<NodeBuilderT, Deserializer, Serializer>
where
    NodeBuilderT: FnMut(&mut Builder) -> Node<Request, Response, Streams> + 'static,
    Request: Send + Sync + 'static,
    Response: Send + Sync + 'static,
    Streams: StreamPack,
{
    pub fn with_opaque_request(
        self,
    ) -> RegistrationBuilder<NodeBuilderT, OpaqueMessageDeserializer, Serializer> {
        RegistrationBuilder {
            build_node: self.build_node,
            fork_clone: self.fork_clone,
            unzip: self.unzip,
            unzip_slots: self.unzip_slots,
            _unused: Default::default(),
        }
    }

    pub fn with_opaque_response(
        self,
    ) -> RegistrationBuilder<NodeBuilderT, Deserializer, OpaqueMessageSerializer> {
        RegistrationBuilder {
            build_node: self.build_node,
            fork_clone: self.fork_clone,
            unzip: self.unzip,
            unzip_slots: self.unzip_slots,
            _unused: Default::default(),
        }
    }

    pub fn with_response_cloneable(mut self) -> Self
    where
        Response: Clone,
    {
        self.fork_clone = Some(RefCell::new(Box::new(|builder, output, amount| {
            assert_eq!(output.type_name, std::any::type_name::<Response>());

            let fork_clone = output.into_output::<Response>().fork_clone(builder);
            Ok((0..amount)
                .map(|_| fork_clone.clone_output(builder).into())
                .collect())
        })));
        self
    }

    pub fn with_unzippable(mut self) -> Self
    where
        Output<Response>: DynUnzip,
    {
        self.unzip = Some(RefCell::new(Box::new(|builder, output| {
            assert_eq!(std::any::type_name::<Response>(), output.type_name);
            let o = output.into_output::<Response>();
            o.unzip(builder)
        })));
        self.unzip_slots = Output::<Response>::UNZIP_SLOTS;
        self
    }
}

pub trait IntoNodeRegistration {
    fn into_node_registration(self, id: &'static str, name: &'static str) -> NodeRegistration;
}

impl<NodeBuilderT, Deserializer, Serializer, Request, Response, Streams> IntoNodeRegistration
    for RegistrationBuilder<NodeBuilderT, Deserializer, Serializer>
where
    NodeBuilderT: FnMut(&mut Builder) -> Node<Request, Response, Streams> + 'static,
    Deserializer: DeserializeMessage<Request>,
    Serializer: SerializeMessage<Response>,
    Request: Send + Sync + 'static,
    Response: Send + Sync + 'static,
    Streams: StreamPack,
{
    fn into_node_registration(mut self, id: &'static str, name: &'static str) -> NodeRegistration {
        let request = RequestMetadata {
            r#type: Deserializer::type_name(),
            deserializable: Deserializer::deserializable(),
        };
        let response = ResponseMetadata {
            r#type: Serializer::type_name(),
            serializable: Serializer::serializable(),
            cloneable: self.fork_clone.is_some(),
            unzip_slots: self.unzip_slots,
        };
        NodeRegistration {
            metadata: NodeMetadata {
                id,
                name,
                request,
                response,
            },
            create_node_impl: RefCell::new(Box::new(move |builder| {
                let n = (self.build_node)(builder);
                DynNode::from(n)
            })),
            create_receiver_impl: {
                if Deserializer::deserializable() {
                    Some(RefCell::new(Box::new(move |builder| {
                        let n = builder.create_map_block(|json: serde_json::Value| {
                            Deserializer::from_json(json)
                        });
                        let o = n.output.chain(builder).cancel_on_err().output();
                        Node::<serde_json::Value, Request, ()> {
                            input: n.input,
                            output: o,
                            streams: n.streams,
                        }
                        .into()
                    })))
                } else {
                    None
                }
            },
            create_sender_impl: {
                if Serializer::serializable() {
                    Some(RefCell::new(Box::new(move |builder| {
                        let n = builder.create_map_block(|resp: Response| -> ScopeTerminate {
                            Ok(Serializer::to_json(&resp)?)
                        });
                        DynNode::from(n)
                    })))
                } else {
                    None
                }
            },
            fork_clone_impl: self.fork_clone,
            unzip_impl: self.unzip,
        }
    }
}

pub trait IntoRegistrationBuilder<NodeBuilderT, Deserializer, Serializer> {
    fn into_registration_builder(
        self,
    ) -> RegistrationBuilder<NodeBuilderT, Deserializer, Serializer>;
}

impl<F, Request, Response, Streams>
    IntoRegistrationBuilder<F, DefaultDeserializer, DefaultSerializer> for F
where
    F: FnMut(&mut Builder) -> Node<Request, Response, Streams>,
    Streams: StreamPack,
{
    fn into_registration_builder(
        self,
    ) -> RegistrationBuilder<F, DefaultDeserializer, DefaultSerializer> {
        RegistrationBuilder {
            build_node: self,
            fork_clone: None,
            unzip: None,
            unzip_slots: 0,
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
    pub fn register_node<IntoNodeRegistrationT>(
        &mut self,
        id: &'static str,
        name: &'static str,
        registration_builder: IntoNodeRegistrationT,
    ) -> &mut Self
    where
        // we could `impl IntoNodeRegistration for T where T: IntoRegistrationBuilder` so that
        // users can pass a `FnMut(&mut Builder) -> Node` directly without needing to call
        // `into_registration_builder()`. But the need to customize a registration is common
        // enough that we can make it a pattern to always call `into_registration_builder()`.
        IntoNodeRegistrationT: IntoNodeRegistration,
    {
        self.nodes
            .insert(id, registration_builder.into_node_registration(id, name));
        self
    }

    pub(super) fn get_registration<Q>(&self, id: &Q) -> Result<&NodeRegistration, DiagramError>
    where
        Q: Borrow<str> + ?Sized,
    {
        let k = id.borrow();
        self.nodes
            .get(k)
            .ok_or(DiagramError::NodeNotFound(k.to_string()))
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
        registry.register_node(
            "multiply3",
            "Test Name",
            (|builder: &mut Builder| builder.create_map_block(multiply3))
                .into_registration_builder(),
        );
        let registration = registry.get_registration("multiply3").unwrap();
        assert!(registration.metadata.request.deserializable);
        assert!(registration.metadata.response.serializable);
        assert!(!registration.metadata.response.cloneable);
        assert_eq!(registration.metadata.response.unzip_slots, 0);
    }

    #[test]
    fn test_register_cloneable_node() {
        let mut registry = NodeRegistry::default();
        registry.register_node(
            "multiply3",
            "Test Name",
            (|builder: &mut Builder| builder.create_map_block(multiply3))
                .into_registration_builder()
                .with_response_cloneable(),
        );
        let registration = registry.get_registration("multiply3").unwrap();
        assert!(registration.metadata.request.deserializable);
        assert!(registration.metadata.response.serializable);
        assert!(registration.metadata.response.cloneable);
        assert_eq!(registration.metadata.response.unzip_slots, 0);
    }

    #[test]
    fn test_register_unzippable_node() {
        let mut registry = NodeRegistry::default();
        let tuple_resp = |_: ()| -> (i64,) { (1,) };
        registry.register_node(
            "multiply3",
            "Test Name",
            (move |builder: &mut Builder| builder.create_map_block(tuple_resp))
                .into_registration_builder()
                .with_unzippable(),
        );
        let registration = registry.get_registration("multiply3").unwrap();
        assert!(registration.metadata.request.deserializable);
        assert!(registration.metadata.response.serializable);
        assert!(!registration.metadata.response.cloneable);
        assert_eq!(registration.metadata.response.unzip_slots, 1);
    }

    struct NonSerializableRequest {}

    #[test]
    fn test_register_opaque_node() {
        let opaque_request_map = |_: NonSerializableRequest| {};

        let mut registry = NodeRegistry::default();
        registry.register_node(
            "opaque_request_map",
            "Test Name",
            (move |builder: &mut Builder| builder.create_map_block(opaque_request_map))
                .into_registration_builder()
                .with_opaque_request(),
        );
        assert!(registry.get_registration("opaque_request_map").is_ok());
        let registration = registry.get_registration("opaque_request_map").unwrap();
        assert!(!registration.metadata.request.deserializable);
        assert!(registration.metadata.response.serializable);
        assert!(!registration.metadata.response.cloneable);
        assert_eq!(registration.metadata.response.unzip_slots, 0);

        let opaque_response_map = |_: ()| NonSerializableRequest {};
        registry.register_node(
            "opaque_response_map",
            "Test Name",
            (move |builder: &mut Builder| builder.create_map_block(opaque_response_map))
                .into_registration_builder()
                .with_opaque_response(),
        );
        assert!(registry.get_registration("opaque_response_map").is_ok());
        let registration = registry.get_registration("opaque_response_map").unwrap();
        assert!(registration.metadata.request.deserializable);
        assert!(!registration.metadata.response.serializable);
        assert!(!registration.metadata.response.cloneable);
        assert_eq!(registration.metadata.response.unzip_slots, 0);

        let opaque_req_resp_map = |_: NonSerializableRequest| NonSerializableRequest {};
        registry.register_node(
            "opaque_req_resp_map",
            "Test Name",
            (move |builder: &mut Builder| builder.create_map_block(opaque_req_resp_map))
                .into_registration_builder()
                .with_opaque_request()
                .with_opaque_response(),
        );
        assert!(registry.get_registration("opaque_req_resp_map").is_ok());
        let registration = registry.get_registration("opaque_req_resp_map").unwrap();
        assert!(!registration.metadata.request.deserializable);
        assert!(!registration.metadata.response.serializable);
        assert!(!registration.metadata.response.cloneable);
        assert_eq!(registration.metadata.response.unzip_slots, 0);
    }
}
