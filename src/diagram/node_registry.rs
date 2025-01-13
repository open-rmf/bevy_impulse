use std::{
    any::{Any, TypeId},
    borrow::Borrow,
    cell::RefCell,
    collections::HashMap,
    fmt::Debug,
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
    BuilderId, DefaultDeserializer, DefaultSerializer, DeserializeMessage, DiagramError,
    DiagramMessage, DynSplit, DynSplitOutputs, DynType, OpaqueMessageDeserializer,
    OpaqueMessageSerializer, ResponseMetadata, SplitOp,
};

/// A type erased [`crate::InputSlot`]
#[derive(Copy, Clone, Debug)]
pub struct DynInputSlot {
    scope: Entity,
    source: Entity,
    pub(super) type_id: TypeId,
}

impl DynInputSlot {
    pub(super) fn scope(&self) -> Entity {
        self.scope
    }

    pub(super) fn id(&self) -> Entity {
        self.source
    }
}

impl<T: Any> From<InputSlot<T>> for DynInputSlot {
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
    pub(super) fn into_output<T>(self) -> Result<Output<T>, DiagramError>
    where
        T: Send + Sync + 'static + Any,
    {
        if self.type_id != TypeId::of::<T>() {
            Err(DiagramError::TypeMismatch)
        } else {
            Ok(Output::<T>::new(self.scope, self.target))
        }
    }

    pub(super) fn scope(&self) -> Entity {
        self.scope
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
            type_id: TypeId::of::<T>(),
        }
    }
}

#[derive(Clone, Serialize)]
pub(super) struct NodeMetadata {
    pub(super) id: BuilderId,
    pub(super) name: String,
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
    create_node_impl: CreateNodeFn,

    fork_clone_impl: Option<ForkCloneFn>,

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
    fn new(
        metadata: NodeMetadata,
        create_node_impl: CreateNodeFn,
        fork_clone_impl: Option<ForkCloneFn>,
    ) -> NodeRegistration {
        NodeRegistration {
            metadata,
            create_node_impl,
            fork_clone_impl,
            unzip_impl: None,
            fork_result_impl: None,
            split_impl: None,
        }
    }

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

type CreateNodeFn =
    RefCell<Box<dyn FnMut(&mut Builder, serde_json::Value) -> Result<DynNode, DiagramError>>>;
type ForkCloneFn =
    Box<dyn Fn(&mut Builder, DynOutput, usize) -> Result<Vec<DynOutput>, DiagramError>>;

pub struct CommonOperations<'a, Deserialize, Serialize, Cloneable> {
    registry: &'a mut NodeRegistry,
    _ignore: PhantomData<(Deserialize, Serialize, Cloneable)>,
}

impl<'a, DeserializeImpl, SerializeImpl, Cloneable>
    CommonOperations<'a, DeserializeImpl, SerializeImpl, Cloneable>
{
    /// Register a node builder with the specified common operations.
    ///
    /// # Arguments
    ///
    /// * `id` - Id of the builder, this must be unique.
    /// * `name` - Friendly name for the builder, this is only used for display purposes.
    /// * `f` - The node builder to register.
    pub fn register_node_builder<Config, Request, Response, Streams>(
        self,
        options: NodeBuilderOptions,
        mut f: impl FnMut(&mut Builder, Config) -> Node<Request, Response, Streams> + 'static,
    ) -> RegistrationBuilder<'a, Request, Response, Streams>
    where
        Config: JsonSchema + DeserializeOwned,
        Request: Send + Sync + 'static,
        Response: Send + Sync + 'static,
        Streams: StreamPack,
        DeserializeImpl: DeserializeMessage<Request>,
        SerializeImpl: SerializeMessage<Response>,
        Cloneable: DynForkClone<Response>,
    {
        register_deserialize::<Request, DeserializeImpl>(&mut self.registry.data);
        register_serialize::<Response, SerializeImpl>(&mut self.registry.data);

        let registration = NodeRegistration::new(
            NodeMetadata {
                id: options.id.clone(),
                name: options.name.unwrap_or(options.id.clone()),
                request: RequestMetadata {
                    schema: DeserializeImpl::json_schema(&mut self.registry.data.schema_generator)
                        .unwrap_or_else(|| {
                            self.registry.data.schema_generator.subschema_for::<()>()
                        }),
                    deserializable: DeserializeImpl::deserializable(),
                },
                response: ResponseMetadata::new(
                    SerializeImpl::json_schema(&mut self.registry.data.schema_generator)
                        .unwrap_or_else(|| {
                            self.registry.data.schema_generator.subschema_for::<()>()
                        }),
                    SerializeImpl::serializable(),
                    Cloneable::CLONEABLE,
                ),
                config_schema: self
                    .registry
                    .data
                    .schema_generator
                    .subschema_for::<Config>(),
            },
            RefCell::new(Box::new(move |builder, config| {
                let config = serde_json::from_value(config)?;
                let n = f(builder, config);
                Ok(DynNode::new(n.output, n.input))
            })),
            if Cloneable::CLONEABLE {
                Some(Box::new(|builder, output, amount| {
                    Cloneable::dyn_fork_clone(builder, output, amount)
                }))
            } else {
                None
            },
        );
        self.registry.nodes.insert(options.id.clone(), registration);

        // SAFETY: We inserted an entry at this ID a moment ago
        let node = self.registry.nodes.get_mut(&options.id).unwrap();

        RegistrationBuilder::<Request, Response, Streams> {
            node,
            data: &mut self.registry.data,
            _ignore: Default::default(),
        }
    }

    /// Register a node builder with the specified common operations.
    ///
    /// # Arguments
    ///
    /// * `id` - Id of the builder, this must be unique.
    /// * `name` - Friendly name for the builder, this is only used for display purposes.
    /// * `f` - The node builder to register.
    pub fn register_with_diagram_message<Config, Request, Response, Streams>(
        self,
        options: NodeBuilderOptions,
        mut f: impl FnMut(&mut Builder, Config) -> Node<Request, Response, Streams> + 'static,
    ) -> RegistrationBuilder<'a, Request, Response, Streams>
    where
        Config: JsonSchema + DeserializeOwned,
        Request: Send + Sync + 'static + DiagramMessage<DeserializeImpl>,
        Response: Send + Sync + 'static + DiagramMessage<SerializeImpl>,
        Streams: StreamPack,
        DeserializeImpl: DeserializeMessage<Request>,
        SerializeImpl: SerializeMessage<Response>,
        Cloneable: DynForkClone<Response>,
    {
        register_deserialize::<Request, DeserializeImpl>(&mut self.registry.data);
        register_serialize::<Response, SerializeImpl>(&mut self.registry.data);

        let mut response = ResponseMetadata::new(
            SerializeImpl::json_schema(&mut self.registry.data.schema_generator)
                .unwrap_or_else(|| self.registry.data.schema_generator.subschema_for::<()>()),
            SerializeImpl::serializable(),
            Cloneable::CLONEABLE,
        );
        response.unzip_slots = <Response::DynUnzipImpl>::UNZIP_SLOTS;

        let registration = NodeRegistration::new(
            NodeMetadata {
                id: options.id.clone(),
                name: options.name.unwrap_or(options.id.clone()),
                request: RequestMetadata {
                    schema: DeserializeImpl::json_schema(&mut self.registry.data.schema_generator)
                        .unwrap_or_else(|| {
                            self.registry.data.schema_generator.subschema_for::<()>()
                        }),
                    deserializable: DeserializeImpl::deserializable(),
                },
                response,
                config_schema: self
                    .registry
                    .data
                    .schema_generator
                    .subschema_for::<Config>(),
            },
            RefCell::new(Box::new(move |builder, config| {
                let config = serde_json::from_value(config)?;
                let n = f(builder, config);
                Ok(DynNode::new(n.output, n.input))
            })),
            if Cloneable::CLONEABLE {
                Some(Box::new(|builder, output, amount| {
                    Cloneable::dyn_fork_clone(builder, output, amount)
                }))
            } else {
                None
            },
        );
        self.registry.nodes.insert(options.id.clone(), registration);

        // SAFETY: We inserted an entry at this ID a moment ago
        let node = self.registry.nodes.get_mut(&options.id).unwrap();

        RegistrationBuilder::<Request, Response, Streams> {
            node,
            data: &mut self.registry.data,
            _ignore: Default::default(),
        }
    }

    /// Opt out of deserializing the request of the node. Use this to build a
    /// node whose request type is not deserializable.
    pub fn no_request_deserializing(
        self,
    ) -> CommonOperations<'a, OpaqueMessageDeserializer, SerializeImpl, Cloneable> {
        CommonOperations {
            registry: self.registry,
            _ignore: Default::default(),
        }
    }

    /// Opt out of serializing the response of the node. Use this to build a
    /// node whose response type is not serializable.
    pub fn no_response_serializing(
        self,
    ) -> CommonOperations<'a, DeserializeImpl, OpaqueMessageSerializer, Cloneable> {
        CommonOperations {
            registry: self.registry,
            _ignore: Default::default(),
        }
    }

    /// Opt out of cloning the response of the node. Use this to build a node
    /// whose response type is not cloneable.
    pub fn no_response_cloning(
        self,
    ) -> CommonOperations<'a, DeserializeImpl, SerializeImpl, NotSupported> {
        CommonOperations {
            registry: self.registry,
            _ignore: Default::default(),
        }
    }
}

pub struct RegistrationBuilder<'a, Request, Response, Streams> {
    node: &'a mut NodeRegistration,
    data: &'a mut MessageRegistry,
    _ignore: PhantomData<(Request, Response, Streams)>,
}

impl<'a, Request, Response, Streams> RegistrationBuilder<'a, Request, Response, Streams> {
    /// Mark the node as having a unzippable response. This is required in order for the node
    /// to be able to be connected to a "Unzip" operation.
    pub fn with_unzip(self) -> Self
    where
        DefaultImpl: DynUnzip<Response, DefaultSerializer>,
    {
        self.with_unzip_impl::<DefaultImpl, DefaultSerializer>()
    }

    /// Mark the node as having an unzippable response whose elements are not serializable.
    pub fn with_unzip_unserializable(self) -> Self
    where
        DefaultImpl: DynUnzip<Response, NotSupported>,
    {
        self.with_unzip_impl::<DefaultImpl, NotSupported>()
    }

    fn with_unzip_impl<UnzipImpl, SerializeImpl>(self) -> Self
    where
        UnzipImpl: DynUnzip<Response, SerializeImpl>,
    {
        self.node.metadata.response.unzip_slots = UnzipImpl::UNZIP_SLOTS;
        self.node.unzip_impl = if UnzipImpl::UNZIP_SLOTS > 0 {
            Some(Box::new(|builder, output| {
                UnzipImpl::dyn_unzip(builder, output)
            }))
        } else {
            None
        };

        UnzipImpl::on_register(self.data);

        self
    }

    /// Mark the node as having a [`Result<_, _>`] response. This is required in order for the node
    /// to be able to be connected to a "Fork Result" operation.
    pub fn with_fork_result(self) -> Self
    where
        DefaultImpl: DynForkResult<Response>,
    {
        self.node.metadata.response.fork_result = true;
        self.node.fork_result_impl = Some(Box::new(|builder, output| {
            <DefaultImpl as DynForkResult<Response>>::dyn_fork_result(builder, output)
        }));

        self
    }

    /// Mark the node as having a splittable response. This is required in order
    /// for the node to be able to be connected to a "Split" operation.
    pub fn with_split(self) -> Self
    where
        DefaultImpl: DynSplit<Response, DefaultSerializer>,
    {
        self.with_split_impl::<DefaultImpl, DefaultSerializer>()
    }

    /// Mark the node as having a splittable response but the items from the split
    /// are unserializable.
    pub fn with_split_unserializable(self) -> Self
    where
        DefaultImpl: DynSplit<Response, NotSupported>,
    {
        self.with_split_impl::<DefaultImpl, NotSupported>()
    }

    pub fn with_split_impl<SplitImpl, SerializeImpl>(self) -> Self
    where
        SplitImpl: DynSplit<Response, SerializeImpl>,
    {
        self.node.metadata.response.splittable = true;
        self.node.split_impl = Some(Box::new(|builder, output, split_op| {
            SplitImpl::dyn_split(builder, output, split_op)
        }));

        SplitImpl::on_register(self.data);

        self
    }
}

pub trait IntoNodeRegistration {
    fn into_node_registration(
        self,
        id: BuilderId,
        name: String,
        schema_generator: &mut SchemaGenerator,
    ) -> NodeRegistration;
}

pub struct NodeRegistry {
    nodes: HashMap<BuilderId, NodeRegistration>,
    pub(super) data: MessageRegistry,
}

pub struct MessageRegistry {
    /// List of all request and response types used in all registered nodes, this only
    /// contains serializable types, non serializable types are opaque and is only compatible
    /// with itself.
    schema_generator: SchemaGenerator,

    pub(super) deserialize_impls: HashMap<
        TypeId,
        Box<dyn Fn(&mut Builder, Output<serde_json::Value>) -> Result<DynOutput, DiagramError>>,
    >,

    pub(super) serialize_impls: HashMap<
        TypeId,
        Box<dyn Fn(&mut Builder, DynOutput) -> Result<Output<serde_json::Value>, DiagramError>>,
    >,

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
            data: MessageRegistry {
                schema_generator: SchemaGenerator::new(settings),
                deserialize_impls: HashMap::new(),
                serialize_impls: HashMap::new(),
                join_impls: HashMap::new(),
            },
        }
    }
}

impl NodeRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a node builder with all the common operations (deserialize the
    /// request, serialize the response, and clone the response) enabled.
    ///
    /// You will receive a [`RegistrationBuilder`] which you can then use to
    /// enable more operations around your node, such as fork result, split,
    /// or unzip. The data types of your node need to be suitable for those
    /// operations or else the compiler will not allow you to enable them.
    ///
    /// ```
    /// use bevy_impulse::{NodeBuilderOptions, NodeRegistry};
    ///
    /// let mut registry = NodeRegistry::default();
    /// registry.register_node_builder(
    ///     NodeBuilderOptions::new("echo".to_string()),
    ///     |builder, _config: ()| builder.create_map_block(|msg: String| msg)
    /// );
    /// ```
    ///
    /// # Arguments
    ///
    /// * `id` - Id of the builder, this must be unique.
    /// * `name` - Friendly name for the builder, this is only used for display purposes.
    /// * `f` - The node builder to register.
    pub fn register_node_builder<Config, Request, Response, Streams: StreamPack>(
        &mut self,
        options: NodeBuilderOptions,
        builder: impl FnMut(&mut Builder, Config) -> Node<Request, Response, Streams> + 'static,
    ) -> RegistrationBuilder<Request, Response, Streams>
    where
        Config: JsonSchema + DeserializeOwned,
        Request: Send + Sync + 'static + DynType + DeserializeOwned,
        Response: Send + Sync + 'static + DynType + Serialize + Clone,
    {
        self.opt_out().register_node_builder(options, builder)
    }

    pub fn register_with_diagram_message<Config, Request, Response, Streams: StreamPack>(
        &mut self,
        options: NodeBuilderOptions,
        builder: impl FnMut(&mut Builder, Config) -> Node<Request, Response, Streams> + 'static,
    ) -> RegistrationBuilder<Request, Response, Streams>
    where
        Config: JsonSchema + DeserializeOwned,
        Request: Send
            + Sync
            + 'static
            + DynType
            + DeserializeOwned
            + DiagramMessage<DefaultDeserializer>,
        Response:
            Send + Sync + 'static + DynType + Serialize + Clone + DiagramMessage<DefaultSerializer>,
    {
        self.opt_out()
            .register_with_diagram_message(options, builder)
    }

    /// In some cases the common operations of deserialization, serialization,
    /// and cloning cannot be performed for the request or response of a node.
    /// When that happens you can still register your node builder by calling
    /// this function and explicitly disabling the common operations that your
    /// node cannot support.
    ///
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
    /// use bevy_impulse::{NodeBuilderOptions, NodeRegistry};
    ///
    /// struct NonSerializable {
    ///     data: String
    /// }
    ///
    /// let mut registry = NodeRegistry::default();
    /// registry
    ///     .opt_out()
    ///     .no_request_deserializing()
    ///     .no_response_serializing()
    ///     .no_response_cloning()
    ///     .register_node_builder(
    ///         NodeBuilderOptions::new("echo"),
    ///         |builder, _config: ()| {
    ///             builder.create_map_block(|msg: NonSerializable| msg)
    ///         }
    ///     );
    /// ```
    ///
    /// Note that nodes registered without deserialization cannot be connected
    /// to the workflow start, and nodes registered without serialization cannot
    /// be connected to the workflow termination.
    pub fn opt_out(
        &mut self,
    ) -> CommonOperations<DefaultDeserializer, DefaultSerializer, DefaultImpl> {
        CommonOperations {
            registry: self,
            _ignore: Default::default(),
        }
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
                        serializer.collect_map(
                            self.value
                                .nodes
                                .iter()
                                .map(|(k, v)| (k.clone(), &v.metadata)),
                        )
                    }
                }
                &SerializeWith { value: self }
            },
        )?;
        s.serialize_field("types", self.data.schema_generator.definitions())?;
        s.end()
    }
}

#[non_exhaustive]
pub struct NodeBuilderOptions {
    pub id: BuilderId,
    pub name: Option<String>,
}

impl NodeBuilderOptions {
    pub fn new(id: impl ToString) -> Self {
        Self {
            id: id.to_string(),
            name: None,
        }
    }

    pub fn with_name(mut self, name: impl ToString) -> Self {
        self.name = Some(name.to_string());
        self
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
        registry
            .opt_out()
            .no_response_cloning()
            .register_node_builder(
                NodeBuilderOptions::new("multiply3_uncloneable").with_name("Test Name"),
                |builder, _config: ()| builder.create_map_block(multiply3),
            );
        let registration = registry.get_registration("multiply3_uncloneable").unwrap();
        assert!(registration.metadata.request.deserializable);
        assert!(registration.metadata.response.serializable);
        assert!(!registration.metadata.response.cloneable);
        assert_eq!(registration.metadata.response.unzip_slots, 0);
    }

    #[test]
    fn test_register_cloneable_node() {
        let mut registry = NodeRegistry::default();
        registry.register_node_builder(
            NodeBuilderOptions::new("multiply3").with_name("Test Name"),
            |builder, _config: ()| builder.create_map_block(multiply3),
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
        registry
            .opt_out()
            .no_response_cloning()
            .register_node_builder(
                NodeBuilderOptions::new("multiply3_uncloneable").with_name("Test Name"),
                move |builder: &mut Builder, _config: ()| builder.create_map_block(tuple_resp),
            )
            .with_unzip();
        let registration = registry.get_registration("multiply3_uncloneable").unwrap();
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
            .register_node_builder(
                NodeBuilderOptions::new("vec_resp").with_name("Test Name"),
                move |builder: &mut Builder, _config: ()| builder.create_map_block(vec_resp),
            )
            .with_split();
        let registration = registry.get_registration("vec_resp").unwrap();
        assert!(registration.metadata.response.splittable);

        let map_resp = |_: ()| -> HashMap<String, i64> { HashMap::new() };
        registry
            .register_node_builder(
                NodeBuilderOptions::new("map_resp").with_name("Test Name"),
                move |builder: &mut Builder, _config: ()| builder.create_map_block(map_resp),
            )
            .with_split();

        let registration = registry.get_registration("map_resp").unwrap();
        assert!(registration.metadata.response.splittable);

        registry.register_node_builder(
            NodeBuilderOptions::new("not_splittable").with_name("Test Name"),
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
            NodeBuilderOptions::new("multiply").with_name("Test Name"),
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
            .opt_out()
            .no_request_deserializing()
            .no_response_cloning()
            .register_node_builder(
                NodeBuilderOptions::new("opaque_request_map").with_name("Test Name"),
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
            .opt_out()
            .no_response_serializing()
            .no_response_cloning()
            .register_node_builder(
                NodeBuilderOptions::new("opaque_response_map").with_name("Test Name"),
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
            .opt_out()
            .no_request_deserializing()
            .no_response_serializing()
            .no_response_cloning()
            .register_node_builder(
                NodeBuilderOptions::new("opaque_req_resp_map").with_name("Test Name"),
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
