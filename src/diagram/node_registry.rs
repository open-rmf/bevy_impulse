use std::{
    any::TypeId, borrow::Borrow, cell::RefCell, collections::HashMap, fmt::Debug,
    marker::PhantomData,
};

use crate::{Builder, InputSlot, Node, Output, StreamPack};
use bevy_ecs::entity::Entity;
use schemars::{
    gen::{SchemaGenerator, SchemaSettings},
    schema::Schema,
    JsonSchema,
};
use serde::{de::DeserializeOwned, ser::SerializeStruct, Serialize};
use tracing::debug;

use crate::{RequestMetadata, SerializeMessage};

use super::{
    fork_clone::DynForkClone,
    fork_result::DynForkResult,
    impls::{DefaultImpl, NotSupported},
    register_deserialize, register_serialize,
    unzip::DynUnzip,
    DefaultDeserializer, DefaultSerializer, DeserializeMessage, DiagramError, DynSplit,
    DynSplitOutputs, DynType, OpaqueMessageDeserializer, OpaqueMessageSerializer, ResponseMetadata,
    SplitOp,
};

/// A type erased [`crate::InputSlot`]
#[derive(Copy, Clone, Debug)]
pub struct DynInputSlot {
    scope: Entity,
    source: Entity,
    pub(super) type_id: TypeId,
}

impl DynInputSlot {
    pub(super) fn into_input<T>(self) -> InputSlot<T> {
        InputSlot::<T>::new(self.scope, self.source)
    }
}

impl<T> From<InputSlot<T>> for DynInputSlot
where
    T: 'static,
{
    fn from(input: InputSlot<T>) -> Self {
        Self {
            scope: input.scope(),
            source: input.id(),
            type_id: TypeId::of::<T>(),
        }
    }
}

#[derive(Debug)]
/// A type erased [`crate::Output`]
pub struct DynOutput {
    scope: Entity,
    target: Entity,
    pub(super) type_id: TypeId,
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
    fn from(output: Output<T>) -> Self {
        Self {
            scope: output.scope(),
            target: output.id(),
            type_id: TypeId::of::<T>(),
        }
    }
}

#[derive(Clone, Serialize)]
pub(super) struct NodeMetadata {
    pub(super) id: &'static str,
    pub(super) name: &'static str,
    pub(super) request: RequestMetadata,
    pub(super) response: ResponseMetadata,
    pub(super) config_schema: Schema,
}

/// A type erased [`bevy_impulse::Node`]
pub(super) struct DynNode {
    pub(super) input: DynInputSlot,
    pub(super) output: DynOutput,
}

impl DynNode {
    fn new<Request, Response>(output: Output<Response>, input: InputSlot<Request>) -> Self
    where
        Request: 'static,
        Response: Send + Sync + 'static,
    {
        Self {
            input: input.into(),
            output: output.into(),
        }
    }
}

impl<Request, Response, Streams> From<Node<Request, Response, Streams>> for DynNode
where
    Request: 'static,
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
    create_node_impl:
        RefCell<Box<dyn FnMut(&mut Builder, serde_json::Value) -> Result<DynNode, DiagramError>>>,

    fork_clone_impl:
        Option<Box<dyn Fn(&mut Builder, DynOutput, usize) -> Result<Vec<DynOutput>, DiagramError>>>,

    unzip_impl:
        Option<Box<dyn Fn(&mut Builder, DynOutput) -> Result<Vec<DynOutput>, DiagramError>>>,

    fork_result_impl: Option<
        Box<dyn Fn(&mut Builder, DynOutput) -> Result<(DynOutput, DynOutput), DiagramError>>,
    >,

    split_impl: Option<
        Box<
            dyn for<'a> Fn(
                &mut Builder,
                DynOutput,
                &'a SplitOp,
            ) -> Result<DynSplitOutputs<'a>, DiagramError>,
        >,
    >,
}

impl NodeRegistration {
    pub(super) fn create_node(
        &self,
        builder: &mut Builder,
        config: serde_json::Value,
    ) -> Result<DynNode, DiagramError> {
        let n = (self.create_node_impl.borrow_mut())(builder, config)?;
        debug!(
            "created node of {}, output: {:?}, input: {:?}",
            self.metadata.id, n.output, n.input
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
        f(builder, output, amount)
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
        f(builder, output)
    }

    pub(super) fn fork_result(
        &self,
        builder: &mut Builder,
        output: DynOutput,
    ) -> Result<(DynOutput, DynOutput), DiagramError> {
        let f = self
            .fork_result_impl
            .as_ref()
            .ok_or(DiagramError::CannotForkResult)?;
        f(builder, output)
    }

    pub(super) fn split<'a>(
        &self,
        builder: &mut Builder,
        output: DynOutput,
        split_op: &'a SplitOp,
    ) -> Result<DynSplitOutputs<'a>, DiagramError> {
        let f = self
            .split_impl
            .as_ref()
            .ok_or(DiagramError::NotSplittable)?;
        f(builder, output, split_op)
    }
}

pub struct RegistrationBuilder<
    'a,
    DeserializeImpl,
    SerializeImpl,
    ForkCloneImpl,
    UnzipImpl,
    ForkResultImpl,
    SplitImpl,
> {
    registry: &'a mut NodeRegistry,
    _unused: PhantomData<(
        DeserializeImpl,
        SerializeImpl,
        ForkCloneImpl,
        UnzipImpl,
        ForkResultImpl,
        SplitImpl,
    )>,
}

impl<'a, DeserializeImpl, SerializeImpl, ForkCloneImpl, UnzipImpl, ForkResultImpl, SplitImpl>
    RegistrationBuilder<
        'a,
        DeserializeImpl,
        SerializeImpl,
        ForkCloneImpl,
        UnzipImpl,
        ForkResultImpl,
        SplitImpl,
    >
{
    pub fn new(registry: &'a mut NodeRegistry) -> Self {
        Self {
            registry,
            _unused: Default::default(),
        }
    }

    pub fn register_node_builder<Config, Request, Response, Streams: StreamPack>(
        &mut self,
        id: &'static str,
        name: &'static str,
        mut f: impl FnMut(&mut Builder, Config) -> Node<Request, Response, Streams> + 'static,
    ) where
        Config: JsonSchema + DeserializeOwned,
        Request: Send + Sync + 'static,
        Response: Send + Sync + 'static,
        DeserializeImpl: DeserializeMessage<Request>,
        SerializeImpl: SerializeMessage<Response>,
        ForkCloneImpl: DynForkClone<Response>,
        UnzipImpl: DynUnzip<Response, SerializeImpl>,
        ForkResultImpl: DynForkResult<Response>,
        SplitImpl: DynSplit<Response, SerializeImpl>,
    {
        Config::json_schema(&mut self.registry.gen);

        let request = RequestMetadata {
            schema: DeserializeImpl::json_schema(&mut self.registry.gen)
                .unwrap_or_else(|| self.registry.gen.subschema_for::<()>()),
            deserializable: DeserializeImpl::deserializable(),
        };
        let response = ResponseMetadata {
            schema: SerializeImpl::json_schema(&mut self.registry.gen)
                .unwrap_or_else(|| self.registry.gen.subschema_for::<()>()),
            serializable: SerializeImpl::serializable(),
            cloneable: ForkCloneImpl::CLONEABLE,
            unzip_slots: UnzipImpl::UNZIP_SLOTS,
            fork_result: ForkResultImpl::SUPPORTED,
            splittable: SplitImpl::SUPPORTED,
        };

        let reg = NodeRegistration {
            metadata: NodeMetadata {
                id,
                name,
                request,
                response,
                config_schema: self.registry.gen.subschema_for::<Config>(),
            },
            create_node_impl: RefCell::new(Box::new(move |builder, config| {
                let config = serde_json::from_value(config)?;
                let n = f(builder, config);
                Ok(DynNode::new(n.output, n.input))
            })),
            fork_clone_impl: if ForkCloneImpl::CLONEABLE {
                Some(Box::new(|builder, output, amount| {
                    ForkCloneImpl::dyn_fork_clone(builder, output, amount)
                }))
            } else {
                None
            },
            unzip_impl: if UnzipImpl::UNZIP_SLOTS > 0 {
                Some(Box::new(|builder, output| {
                    UnzipImpl::dyn_unzip(builder, output)
                }))
            } else {
                None
            },
            fork_result_impl: if ForkResultImpl::SUPPORTED {
                Some(Box::new(|builder, output| {
                    ForkResultImpl::dyn_fork_result(builder, output)
                }))
            } else {
                None
            },
            split_impl: if SplitImpl::SUPPORTED {
                Some(Box::new(|builder, output, split_op| {
                    SplitImpl::dyn_split(builder, output, split_op)
                }))
            } else {
                None
            },
        };

        self.registry.nodes.insert(id, reg);

        register_deserialize::<Request, DeserializeImpl>(self.registry);
        register_serialize::<Response, SerializeImpl>(self.registry);

        UnzipImpl::on_register(&mut self.registry);
        SplitImpl::on_register(&mut self.registry);
    }

    /// Mark the node as having a non deserializable request. This allows nodes with
    /// non deserializable requests to be registered but any nodes registered this way will not
    /// be able to be connected to "Start" or any operation that requires deserialization.
    pub fn with_opaque_request(
        self,
    ) -> RegistrationBuilder<
        'a,
        OpaqueMessageDeserializer,
        SerializeImpl,
        ForkCloneImpl,
        UnzipImpl,
        ForkResultImpl,
        SplitImpl,
    > {
        RegistrationBuilder::new(self.registry)
    }

    /// Mark the node as having a non serializable response. This allows nodes with
    /// non serializable responses to be registered but any nodes registered this way will not
    /// be able to be connected to "Terminate" or any operation that requires serialization.
    pub fn with_opaque_response(
        self,
    ) -> RegistrationBuilder<
        'a,
        DeserializeImpl,
        OpaqueMessageSerializer,
        ForkCloneImpl,
        UnzipImpl,
        ForkResultImpl,
        SplitImpl,
    > {
        RegistrationBuilder::new(self.registry)
    }

    /// Mark the node as having a cloneable response. This is required in order for the node
    /// to be able to be connected to a "Fork Clone" operation.
    pub fn with_response_cloneable(
        self,
    ) -> RegistrationBuilder<
        'a,
        DeserializeImpl,
        SerializeImpl,
        DefaultImpl,
        UnzipImpl,
        ForkResultImpl,
        SplitImpl,
    > {
        RegistrationBuilder::new(self.registry)
    }

    /// Mark the node as having a unzippable response. This is required in order for the node
    /// to be able to be connected to a "Unzip" operation.
    pub fn with_unzippable(
        self,
    ) -> RegistrationBuilder<
        'a,
        DeserializeImpl,
        SerializeImpl,
        ForkCloneImpl,
        DefaultImpl,
        ForkResultImpl,
        SplitImpl,
    > {
        RegistrationBuilder::new(self.registry)
    }

    /// Mark the node as having a [`Result<_, _>`] response. This is required in order for the node
    /// to be able to be connected to a "Fork Result" operation.
    pub fn with_fork_result(
        self,
    ) -> RegistrationBuilder<
        'a,
        DeserializeImpl,
        SerializeImpl,
        ForkCloneImpl,
        UnzipImpl,
        DefaultImpl,
        SplitImpl,
    > {
        RegistrationBuilder::new(self.registry)
    }

    /// Mark the node as having a splittable response. This is required in order for the node
    /// to be able to be connected to a "Split" operation.
    pub fn with_splittable(
        self,
    ) -> RegistrationBuilder<
        'a,
        DeserializeImpl,
        SerializeImpl,
        ForkCloneImpl,
        UnzipImpl,
        ForkResultImpl,
        DefaultImpl,
    > {
        RegistrationBuilder::new(self.registry)
    }
}

pub trait IntoNodeRegistration {
    fn into_node_registration(
        self,
        id: &'static str,
        name: &'static str,
        gen: &mut SchemaGenerator,
    ) -> NodeRegistration;
}

pub struct NodeRegistry {
    nodes: HashMap<&'static str, NodeRegistration>,

    /// List of all request and response types used in all registered nodes, this only
    /// contains serializable types, non serializable types are opaque and is only compatible
    /// with itself.
    gen: SchemaGenerator,

    pub(super) deserialize_impls:
        HashMap<TypeId, Box<dyn Fn(&mut Builder, Output<serde_json::Value>) -> DynOutput>>,

    pub(super) serialize_impls:
        HashMap<TypeId, Box<dyn Fn(&mut Builder, DynOutput) -> Output<serde_json::Value>>>,

    pub(super) join_impls: HashMap<
        TypeId,
        Box<dyn Fn(&mut Builder, Vec<DynOutput>) -> Result<DynOutput, DiagramError>>,
    >,
}

impl Default for NodeRegistry {
    fn default() -> Self {
        let mut settings = SchemaSettings::default();
        settings.definitions_path = "#/types/".to_string();
        NodeRegistry {
            nodes: Default::default(),
            gen: SchemaGenerator::new(settings),
            deserialize_impls: HashMap::new(),
            serialize_impls: HashMap::new(),
            join_impls: HashMap::new(),
        }
    }
}

impl NodeRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new [`RegistrationBuilder`]. By default, it is configured for nodes with
    /// deserializable request and serializable responses and without support for any interconnect
    /// operations like "fork_clone" and "unzip". See [`RegistrationBuilder`] for more information
    /// about these operations.
    ///
    /// ```
    /// use bevy_impulse::NodeRegistry;
    ///
    /// let mut registry = NodeRegistry::default();
    /// registry.registration_builder().register_node_builder("echo", "echo",
    ///     |builder, _config: ()| builder.create_map_block(|msg: String| msg));
    /// ```
    ///
    /// In order for the request to be deserializable, it must implement [`schemars::JsonSchema`] and [`serde::de::DeserializeOwned`].
    /// In order for the response to be serializable, it must implement [`schemars::JsonSchema`] and [`serde::Serialize`].
    ///
    /// ```
    /// use schemars::JsonSchema;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(JsonSchema, Deserialize)]
    /// struct DeserializableRequest {}
    ///
    /// #[derive(JsonSchema, Serialize)]
    /// struct SerializableResponse {}
    /// ```
    ///
    /// If your node have a request or response that is not serializable, there is still
    /// a way to register it.
    ///
    /// ```
    /// use bevy_impulse::NodeRegistry;
    ///
    /// struct NonSerializable {
    ///     data: String
    /// }
    ///
    /// let mut registry = NodeRegistry::default();
    /// registry.registration_builder()
    ///     .with_opaque_request()
    ///     .with_opaque_response()
    ///     .register_node_builder("echo", "echo", |builder, _config: ()| {
    ///         builder.create_map_block(|msg: NonSerializable| msg)
    ///     });
    /// ```
    ///
    /// Note that nodes registered this way cannot be connected to "Start" or "Terminate".
    pub fn registration_builder(
        &mut self,
    ) -> RegistrationBuilder<
        DefaultDeserializer,
        DefaultSerializer,
        NotSupported,
        NotSupported,
        NotSupported,
        NotSupported,
    > {
        RegistrationBuilder::new(self)
    }

    /// Register a node builder using the default registration config.
    ///
    /// This is a equivalent to
    ///
    /// ```text
    /// registry.registration_builder().register_node_builder(f)
    /// ```
    pub fn register_node_builder<Config, Request, Response, Streams: StreamPack>(
        &mut self,
        id: &'static str,
        name: &'static str,
        f: impl FnMut(&mut Builder, Config) -> Node<Request, Response, Streams> + 'static,
    ) where
        Config: JsonSchema + DeserializeOwned,
        Request: Send + Sync + 'static + DynType + DeserializeOwned,
        Response: Send + Sync + 'static + DynType + Serialize,
    {
        self.registration_builder()
            .register_node_builder(id, name, f)
    }

    pub(super) fn get_registration<Q>(&self, id: &Q) -> Result<&NodeRegistration, DiagramError>
    where
        Q: Borrow<str> + ?Sized,
    {
        let k = id.borrow();
        self.nodes
            .get(k)
            .ok_or(DiagramError::BuilderNotFound(k.to_string()))
    }
}

impl Serialize for NodeRegistry {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s = serializer.serialize_struct("NodeRegistry", 2)?;
        // serialize only the nodes metadata
        s.serialize_field(
            "nodes",
            // Since the serializer methods are consuming, we can't call `serialize_struct` and `collect_map`.
            // This genius solution of creating an inline struct and impl `Serialize` on it is based on
            // the code that `#[derive(Serialize)]` generates.
            {
                struct SerializeWith<'a> {
                    value: &'a NodeRegistry,
                }
                impl<'a> Serialize for SerializeWith<'a> {
                    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
                    where
                        S: serde::Serializer,
                    {
                        serializer
                            .collect_map(self.value.nodes.iter().map(|(k, v)| (*k, &v.metadata)))
                    }
                }
                &SerializeWith { value: self }
            },
        )?;
        s.serialize_field("types", self.gen.definitions())?;
        s.end()
    }
}

#[cfg(test)]
mod tests {
    use schemars::JsonSchema;
    use serde::Deserialize;

    use super::*;

    fn multiply3(i: i64) -> i64 {
        i * 3
    }

    #[test]
    fn test_register_node_builder() {
        let mut registry = NodeRegistry::default();
        registry.register_node_builder("multiply3", "Test Name", |builder, _config: ()| {
            builder.create_map_block(multiply3)
        });
        let registration = registry.get_registration("multiply3").unwrap();
        assert!(registration.metadata.request.deserializable);
        assert!(registration.metadata.response.serializable);
        assert!(!registration.metadata.response.cloneable);
        assert_eq!(registration.metadata.response.unzip_slots, 0);
    }

    #[test]
    fn test_register_cloneable_node() {
        let mut registry = NodeRegistry::default();
        registry
            .registration_builder()
            .with_response_cloneable()
            .register_node_builder("multiply3", "Test Name", |builder, _config: ()| {
                builder.create_map_block(multiply3)
            });
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
        registry
            .registration_builder()
            .with_unzippable()
            .register_node_builder(
                "multiply3",
                "Test Name",
                move |builder: &mut Builder, _config: ()| builder.create_map_block(tuple_resp),
            );
        let registration = registry.get_registration("multiply3").unwrap();
        assert!(registration.metadata.request.deserializable);
        assert!(registration.metadata.response.serializable);
        assert!(!registration.metadata.response.cloneable);
        assert_eq!(registration.metadata.response.unzip_slots, 1);
    }

    #[test]
    fn test_register_splittable_node() {
        let mut registry = NodeRegistry::default();
        let vec_resp = |_: ()| -> Vec<i64> { vec![1, 2] };
        registry
            .registration_builder()
            .with_splittable()
            .register_node_builder(
                "vec_resp",
                "Test Name",
                move |builder: &mut Builder, _config: ()| builder.create_map_block(vec_resp),
            );
        let registration = registry.get_registration("vec_resp").unwrap();
        assert!(registration.metadata.response.splittable);

        let map_resp = |_: ()| -> HashMap<String, i64> { HashMap::new() };
        registry
            .registration_builder()
            .with_splittable()
            .register_node_builder(
                "map_resp",
                "Test Name",
                move |builder: &mut Builder, _config: ()| builder.create_map_block(map_resp),
            );
        let registration = registry.get_registration("map_resp").unwrap();
        assert!(registration.metadata.response.splittable);

        registry.register_node_builder(
            "not_splittable",
            "Test Name",
            move |builder: &mut Builder, _config: ()| builder.create_map_block(map_resp),
        );
        let registration = registry.get_registration("not_splittable").unwrap();
        assert!(!registration.metadata.response.splittable);
    }

    #[test]
    fn test_register_with_config() {
        let mut registry = NodeRegistry::default();

        #[derive(Deserialize, JsonSchema)]
        struct TestConfig {
            by: i64,
        }

        registry.register_node_builder(
            "multiply",
            "Test Name",
            move |builder: &mut Builder, config: TestConfig| {
                builder.create_map_block(move |operand: i64| operand * config.by)
            },
        );
        assert!(registry.get_registration("multiply").is_ok());
    }

    struct NonSerializableRequest {}

    #[test]
    fn test_register_opaque_node() {
        let opaque_request_map = |_: NonSerializableRequest| {};

        let mut registry = NodeRegistry::default();
        registry
            .registration_builder()
            .with_opaque_request()
            .register_node_builder(
                "opaque_request_map",
                "Test Name",
                move |builder, _config: ()| builder.create_map_block(opaque_request_map),
            );
        assert!(registry.get_registration("opaque_request_map").is_ok());
        let registration = registry.get_registration("opaque_request_map").unwrap();
        assert!(!registration.metadata.request.deserializable);
        assert!(registration.metadata.response.serializable);
        assert!(!registration.metadata.response.cloneable);
        assert_eq!(registration.metadata.response.unzip_slots, 0);

        let opaque_response_map = |_: ()| NonSerializableRequest {};
        registry
            .registration_builder()
            .with_opaque_response()
            .register_node_builder(
                "opaque_response_map",
                "Test Name",
                move |builder: &mut Builder, _config: ()| {
                    builder.create_map_block(opaque_response_map)
                },
            );
        assert!(registry.get_registration("opaque_response_map").is_ok());
        let registration = registry.get_registration("opaque_response_map").unwrap();
        assert!(registration.metadata.request.deserializable);
        assert!(!registration.metadata.response.serializable);
        assert!(!registration.metadata.response.cloneable);
        assert_eq!(registration.metadata.response.unzip_slots, 0);

        let opaque_req_resp_map = |_: NonSerializableRequest| NonSerializableRequest {};
        registry
            .registration_builder()
            .with_opaque_request()
            .with_opaque_response()
            .register_node_builder(
                "opaque_req_resp_map",
                "Test Name",
                move |builder: &mut Builder, _config: ()| {
                    builder.create_map_block(opaque_req_resp_map)
                },
            );
        assert!(registry.get_registration("opaque_req_resp_map").is_ok());
        let registration = registry.get_registration("opaque_req_resp_map").unwrap();
        assert!(!registration.metadata.request.deserializable);
        assert!(!registration.metadata.response.serializable);
        assert!(!registration.metadata.response.cloneable);
        assert_eq!(registration.metadata.response.unzip_slots, 0);
    }
}
