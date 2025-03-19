use std::{
    any::{type_name, Any},
    borrow::Borrow,
    cell::RefCell,
    collections::HashMap,
    fmt::Debug,
    marker::PhantomData,
};

use crate::{
    unknown_diagram_error, Accessor, AnyBuffer, AsAnyBuffer, BufferMap, BufferSettings, Builder,
    InputSlot, Joined, JsonBuffer, Node, Output, StreamPack,
};
use bevy_ecs::entity::Entity;
use schemars::{
    gen::{SchemaGenerator, SchemaSettings},
    schema::Schema,
    JsonSchema,
};
use serde::{
    de::DeserializeOwned,
    ser::{SerializeMap, SerializeStruct},
    Serialize,
};
use serde_json::json;
use tracing::debug;

use super::{
    buffer_schema::BufferAccessRequest,
    fork_clone_schema::DynForkClone,
    fork_result_schema::DynForkResult,
    impls::{DefaultImpl, DefaultImplMarker, NotSupported},
    register_json, register_serialize,
    type_info::TypeInfo,
    unzip_schema::DynUnzip,
    BuilderId, DefaultDeserializer, DefaultSerializer, DeserializeMessage, DiagramErrorCode,
    DynSplit, DynSplitOutputs, DynType, JsonRegistration, OpaqueMessageDeserializer,
    OpaqueMessageSerializer, RegisterJson, SerializeMessage, SplitSchema,
};

/// A type erased [`crate::InputSlot`]
#[derive(Copy, Clone, Debug)]
pub struct DynInputSlot {
    scope: Entity,
    source: Entity,
    pub(super) type_info: TypeInfo,
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
            type_info: TypeInfo::of::<T>(),
        }
    }
}

/// A type erased [`crate::Output`]
pub struct DynOutput {
    scope: Entity,
    target: Entity,
    pub(super) type_info: TypeInfo,

    into_any_buffer_impl:
        fn(Self, &mut Builder, BufferSettings) -> Result<AnyBuffer, DiagramErrorCode>,
}

impl DynOutput {
    pub(super) fn into_output<T>(self) -> Result<Output<T>, DiagramErrorCode>
    where
        T: Send + Sync + 'static + Any,
    {
        if self.type_info != TypeInfo::of::<T>() {
            Err(DiagramErrorCode::TypeMismatch {
                source_type: self.type_info,
                target_type: TypeInfo::of::<T>(),
            })
        } else {
            Ok(Output::<T>::new(self.scope, self.target))
        }
    }

    pub(super) fn into_any_buffer(
        self,
        builder: &mut Builder,
        buffer_settings: BufferSettings,
    ) -> Result<AnyBuffer, DiagramErrorCode> {
        (self.into_any_buffer_impl)(self, builder, buffer_settings)
    }

    pub(super) fn scope(&self) -> Entity {
        self.scope
    }

    pub(super) fn id(&self) -> Entity {
        self.target
    }
}

impl Debug for DynOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DynOutput")
            .field("scope", &self.scope)
            .field("target", &self.target)
            .field("type_info", &self.type_info)
            .finish()
    }
}

impl<T> From<Output<T>> for DynOutput
where
    T: Send + Sync + 'static + Any,
{
    fn from(output: Output<T>) -> Self {
        Self {
            scope: output.scope(),
            target: output.id(),
            type_info: TypeInfo::of::<T>(),
            into_any_buffer_impl: |me, builder, buffer_settings| {
                let buffer = builder.create_buffer::<T>(buffer_settings);
                builder.connect(me.into_output()?, buffer.input_slot());
                Ok(buffer.as_any_buffer())
            },
        }
    }
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

#[derive(Serialize)]
pub struct NodeRegistration {
    pub(super) id: BuilderId,
    pub(super) name: String,
    pub(super) request: TypeInfo,
    pub(super) response: TypeInfo,
    pub(super) config_schema: Schema,

    /// Creates an instance of the registered node.
    #[serde(skip)]
    create_node_impl: CreateNodeFn,
}

impl NodeRegistration {
    pub(super) fn create_node(
        &self,
        builder: &mut Builder,
        config: serde_json::Value,
    ) -> Result<DynNode, DiagramErrorCode> {
        let n = (self.create_node_impl.borrow_mut())(builder, config)?;
        debug!(
            "created node of {}, output: {:?}, input: {:?}",
            self.id, n.output, n.input
        );
        Ok(n)
    }
}

type CreateNodeFn =
    RefCell<Box<dyn FnMut(&mut Builder, serde_json::Value) -> Result<DynNode, DiagramErrorCode>>>;
type DeserializeFn =
    fn(&mut Builder, Output<serde_json::Value>) -> Result<DynOutput, DiagramErrorCode>;
type SerializeFn =
    fn(&mut Builder, DynOutput) -> Result<Output<serde_json::Value>, DiagramErrorCode>;
type ForkCloneFn = fn(&mut Builder, DynOutput, usize) -> Result<Vec<DynOutput>, DiagramErrorCode>;
type ForkResultFn = fn(&mut Builder, DynOutput) -> Result<(DynOutput, DynOutput), DiagramErrorCode>;
type SplitFn = Box<
    dyn for<'a> Fn(
        &mut Builder,
        DynOutput,
        &'a SplitSchema,
    ) -> Result<DynSplitOutputs<'a>, DiagramErrorCode>,
>;
type JoinFn = fn(&mut Builder, &BufferMap) -> Result<DynOutput, DiagramErrorCode>;
type BufferAccessFn =
    fn(&mut Builder, DynOutput, &BufferMap) -> Result<DynOutput, DiagramErrorCode>;
type ListenFn = fn(&mut Builder, &BufferMap) -> Result<DynOutput, DiagramErrorCode>;

#[must_use]
pub struct CommonOperations<'a, Deserialize, Serialize, Cloneable> {
    registry: &'a mut DiagramElementRegistry,
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
        mut self,
        options: NodeBuilderOptions,
        mut f: impl FnMut(&mut Builder, Config) -> Node<Request, Response, Streams> + 'static,
    ) -> NodeRegistrationBuilder<'a, Request, Response, Streams>
    where
        Config: JsonSchema + DeserializeOwned,
        Request: Send + Sync + 'static,
        Response: Send + Sync + 'static,
        Streams: StreamPack,
        DeserializeImpl: DeserializeMessage<Request>,
        DeserializeImpl: DeserializeMessage<Response>,
        SerializeImpl: SerializeMessage<Request>,
        SerializeImpl: SerializeMessage<Response>,
        Cloneable: DynForkClone<Request>,
        Cloneable: DynForkClone<Response>,
        JsonRegistration<SerializeImpl, DeserializeImpl>: RegisterJson<Request>,
        JsonRegistration<SerializeImpl, DeserializeImpl>: RegisterJson<Response>,
    {
        self.impl_register_message::<Request>();
        self.impl_register_message::<Response>();

        let registration = NodeRegistration {
            id: options.id.clone(),
            name: options.name.unwrap_or(options.id.clone()),
            request: TypeInfo::of::<Request>(),
            response: TypeInfo::of::<Response>(),
            config_schema: self
                .registry
                .messages
                .schema_generator
                .subschema_for::<Config>(),
            create_node_impl: RefCell::new(Box::new(move |builder, config| {
                let config = serde_json::from_value(config)?;
                let n = f(builder, config);
                Ok(DynNode::new(n.output, n.input))
            })),
        };
        self.registry.nodes.insert(options.id.clone(), registration);

        NodeRegistrationBuilder::<Request, Response, Streams>::new(self.registry)
    }

    /// Register a message with the specified common operations.
    pub fn register_message<Message>(mut self) -> MessageRegistrationBuilder<'a, Message>
    where
        Message: Send + Sync + 'static,
        DeserializeImpl: DeserializeMessage<Message>,
        SerializeImpl: SerializeMessage<Message>,
        Cloneable: DynForkClone<Message>,
        JsonRegistration<SerializeImpl, DeserializeImpl>: RegisterJson<Message>,
    {
        self.impl_register_message();
        MessageRegistrationBuilder::<Message>::new(&mut self.registry.messages)
    }

    fn impl_register_message<Message>(&mut self)
    where
        Message: Send + Sync + 'static,
        DeserializeImpl: DeserializeMessage<Message>,
        SerializeImpl: SerializeMessage<Message>,
        Cloneable: DynForkClone<Message>,
        JsonRegistration<SerializeImpl, DeserializeImpl>: RegisterJson<Message>,
    {
        self.registry
            .messages
            .register_deserialize::<Message, DeserializeImpl>();
        self.registry
            .messages
            .register_serialize::<Message, SerializeImpl>();
        self.registry
            .messages
            .register_fork_clone::<Message, Cloneable>();

        register_json::<Message, SerializeImpl, DeserializeImpl>();
    }

    /// Opt out of deserializing the input and output messages of the node.
    ///
    /// If you want to enable deserializing for only the input or only the output
    /// then use [`DiagramElementRegistry::register_message`] on the message type
    /// directly.
    ///
    /// Note that [`JsonBuffer`] is only enabled for message types that enable
    /// both serializing AND deserializing.
    pub fn no_deserializing(
        self,
    ) -> CommonOperations<'a, OpaqueMessageDeserializer, SerializeImpl, Cloneable> {
        CommonOperations {
            registry: self.registry,
            _ignore: Default::default(),
        }
    }

    /// Opt out of serializing the input and output messages of the node.
    ///
    /// If you want to enable serialization for only the input or only the output
    /// then use [`DiagramElementRegistry::register_message`] on the message type
    /// directly.
    ///
    /// Note that [`JsonBuffer`] is only enabled for message types that enable
    /// both serializing AND deserializing.
    pub fn no_serializing(
        self,
    ) -> CommonOperations<'a, DeserializeImpl, OpaqueMessageSerializer, Cloneable> {
        CommonOperations {
            registry: self.registry,
            _ignore: Default::default(),
        }
    }

    /// Opt out of cloning the input and output messages of the node.
    ///
    /// If you want to enable cloning for only the input or only the output
    /// then use [`DiagramElementRegistry::register_message`] on the message type
    /// directly.
    pub fn no_cloning(self) -> CommonOperations<'a, DeserializeImpl, SerializeImpl, NotSupported> {
        CommonOperations {
            registry: self.registry,
            _ignore: Default::default(),
        }
    }
}

pub struct MessageRegistrationBuilder<'a, Message> {
    data: &'a mut MessageRegistry,
    _ignore: PhantomData<Message>,
}

impl<'a, Message> MessageRegistrationBuilder<'a, Message>
where
    Message: Send + Sync + 'static + Any,
{
    fn new(registry: &'a mut MessageRegistry) -> Self {
        Self {
            data: registry,
            _ignore: Default::default(),
        }
    }

    /// Mark the message as having a unzippable response. This is required in order for the node
    /// to be able to be connected to a "Unzip" operation.
    pub fn with_unzip(&mut self) -> &mut Self
    where
        DefaultImplMarker<(Message, DefaultSerializer)>: DynUnzip,
    {
        self.data.register_unzip::<Message, DefaultSerializer>();
        self
    }

    /// Mark the message as having an unzippable response whose elements are not serializable.
    pub fn with_unzip_minimal(&mut self) -> &mut Self
    where
        DefaultImplMarker<(Message, NotSupported)>: DynUnzip,
    {
        self.data.register_unzip::<Message, NotSupported>();
        self
    }

    /// Mark the message as having a [`Result<_, _>`] response. This is required in order for the node
    /// to be able to be connected to a "Fork Result" operation.
    pub fn with_fork_result(&mut self) -> &mut Self
    where
        DefaultImplMarker<(Message, OpaqueMessageSerializer)>: DynForkResult,
    {
        self.data
            .register_fork_result(DefaultImplMarker::<(Message, OpaqueMessageSerializer)>::new());
        self
    }

    /// Mark the message as having a splittable response. This is required in order
    /// for the node to be able to be connected to a "Split" operation.
    pub fn with_split(&mut self) -> &mut Self
    where
        DefaultImpl: DynSplit<Message, DefaultSerializer>,
    {
        self.data
            .register_split::<Message, DefaultImpl, DefaultSerializer>();
        self
    }

    /// Mark the message as having a splittable response but the items from the split
    /// are unserializable.
    pub fn with_split_minimal(&mut self) -> &mut Self
    where
        DefaultImpl: DynSplit<Message, NotSupported>,
    {
        self.data
            .register_split::<Message, DefaultImpl, NotSupported>();
        self
    }

    /// Mark the message as being joinable.
    pub fn with_join(&mut self) -> &mut Self
    where
        Message: Joined,
    {
        self.data.register_join::<Message>();
        self
    }

    /// Mark the message as being a buffer access.
    pub fn with_buffer_access(&mut self) -> &mut Self
    where
        Message: BufferAccessRequest,
    {
        self.data.register_buffer_access::<Message>();
        self
    }

    /// Mark the message as being listenable.
    pub fn with_listen(&mut self) -> &mut Self
    where
        Message: Accessor,
    {
        self.data.register_listen::<Message>();
        self
    }
}

pub struct NodeRegistrationBuilder<'a, Request, Response, Streams> {
    registry: &'a mut DiagramElementRegistry,
    _ignore: PhantomData<(Request, Response, Streams)>,
}

impl<'a, Request, Response, Streams> NodeRegistrationBuilder<'a, Request, Response, Streams>
where
    Request: Send + Sync + 'static + Any,
    Response: Send + Sync + 'static + Any,
{
    fn new(registry: &'a mut DiagramElementRegistry) -> Self {
        Self {
            registry,
            _ignore: Default::default(),
        }
    }

    /// If you opted out of any common operations in order to accommodate your
    /// response type, you can enable all common operations for your response
    /// type using this.
    pub fn with_common_request(&mut self) -> &mut Self
    where
        Request: DynType + DeserializeOwned + Serialize + Clone,
    {
        self.registry.register_message::<Request>();
        self
    }

    /// If you opted out of cloning, you can enable it specifically for the
    /// input message with this.
    pub fn with_clone_request(&mut self) -> &mut Self
    where
        Request: Clone,
    {
        self.registry
            .messages
            .register_fork_clone::<Request, DefaultImpl>();
        self
    }

    /// If you opted out of deserialization, you can enable it specifically for
    /// the input message with this.
    pub fn with_deserialize_request(&mut self) -> &mut Self
    where
        Request: DeserializeOwned + DynType,
    {
        self.registry
            .messages
            .register_deserialize::<Request, DefaultDeserializer>();
        self
    }

    /// If you opted out of any common operations in order to accommodate your
    /// request type, you can enable all common operations for your response
    /// type using this.
    pub fn with_common_response(&mut self) -> &mut Self
    where
        Response: DynType + DeserializeOwned + Serialize + Clone,
    {
        self.registry.register_message::<Response>();
        self
    }

    /// If you opted out of cloning, you can enable it specifically for the
    /// output message with this.
    pub fn with_clone_response(&mut self) -> &mut Self
    where
        Response: Clone,
    {
        self.registry
            .messages
            .register_fork_clone::<Response, DefaultImpl>();
        self
    }

    /// If you opted out of serialization, you can enable it specifically for
    /// the output message with this.
    pub fn with_serialize_response(&mut self) -> &mut Self
    where
        Response: Serialize + DynType,
    {
        self.registry
            .messages
            .register_serialize::<Response, DefaultSerializer>();
        self
    }

    /// Mark the node as having a unzippable response. This is required in order for the node
    /// to be able to be connected to a "Unzip" operation.
    pub fn with_unzip(&mut self) -> &mut Self
    where
        DefaultImplMarker<(Response, DefaultSerializer)>: DynUnzip,
    {
        MessageRegistrationBuilder::new(&mut self.registry.messages).with_unzip();
        self
    }

    /// Mark the node as having an unzippable response whose elements are not serializable.
    pub fn with_unzip_unserializable(&mut self) -> &mut Self
    where
        DefaultImplMarker<(Response, NotSupported)>: DynUnzip,
    {
        MessageRegistrationBuilder::new(&mut self.registry.messages).with_unzip_minimal();
        self
    }

    /// Mark the node as having a [`Result<_, _>`] response. This is required in order for the node
    /// to be able to be connected to a "Fork Result" operation.
    pub fn with_fork_result(&mut self) -> &mut Self
    where
        DefaultImplMarker<(Response, OpaqueMessageSerializer)>: DynForkResult,
    {
        MessageRegistrationBuilder::new(&mut self.registry.messages).with_fork_result();
        self
    }

    /// Mark the node as having a splittable response. This is required in order
    /// for the node to be able to be connected to a "Split" operation.
    pub fn with_split(&mut self) -> &mut Self
    where
        DefaultImpl: DynSplit<Response, DefaultSerializer>,
    {
        MessageRegistrationBuilder::new(&mut self.registry.messages).with_split();
        self
    }

    /// Mark the node as having a splittable response but the items from the split
    /// are unserializable.
    pub fn with_split_unserializable(&mut self) -> &mut Self
    where
        DefaultImpl: DynSplit<Response, NotSupported>,
    {
        MessageRegistrationBuilder::new(&mut self.registry.messages).with_split_minimal();
        self
    }

    /// Mark the node as having a joinable request.
    pub fn with_join(&mut self) -> &mut Self
    where
        Request: Joined,
    {
        MessageRegistrationBuilder::<Request>::new(&mut self.registry.messages).with_join();
        self
    }

    /// Mark the node as having a buffer access request.
    pub fn with_buffer_access(&mut self) -> &mut Self
    where
        Request: BufferAccessRequest,
    {
        MessageRegistrationBuilder::<Request>::new(&mut self.registry.messages)
            .with_buffer_access();
        self
    }

    /// Mark the node as having a listen request.
    pub fn with_listen(&mut self) -> &mut Self
    where
        Request: Accessor,
    {
        MessageRegistrationBuilder::<Request>::new(&mut self.registry.messages).with_listen();
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

#[derive(Serialize)]
pub struct DiagramElementRegistry {
    pub(super) nodes: HashMap<BuilderId, NodeRegistration>,
    #[serde(flatten)]
    pub(super) messages: MessageRegistry,
}

pub(super) struct MessageOperation {
    pub(super) deserialize_impl: Option<DeserializeFn>,
    pub(super) serialize_impl: Option<SerializeFn>,
    pub(super) fork_clone_impl: Option<ForkCloneFn>,
    pub(super) unzip_impl: Option<Box<dyn DynUnzip>>,
    pub(super) fork_result_impl: Option<ForkResultFn>,
    pub(super) split_impl: Option<SplitFn>,
    pub(super) join_impl: Option<JoinFn>,
    pub(super) buffer_access_impl: Option<BufferAccessFn>,
    pub(super) listen_impl: Option<ListenFn>,
}

impl MessageOperation {
    fn new<T>() -> Self
    where
        T: Send + Sync + 'static + Any,
    {
        Self {
            deserialize_impl: None,
            serialize_impl: None,
            fork_clone_impl: None,
            unzip_impl: None,
            fork_result_impl: None,
            split_impl: None,
            join_impl: None,
            buffer_access_impl: None,
            listen_impl: None,
        }
    }

    /// Try to deserialize `output` into `input_type`. If `output` is not `serde_json::Value`, this does nothing.
    pub(super) fn deserialize(
        &self,
        builder: &mut Builder,
        output: DynOutput,
    ) -> Result<DynOutput, DiagramErrorCode> {
        let f = self
            .deserialize_impl
            .as_ref()
            .ok_or(DiagramErrorCode::NotSerializable)?;
        f(builder, output.into_output()?)
    }

    pub(super) fn serialize(
        &self,
        builder: &mut Builder,
        output: DynOutput,
    ) -> Result<Output<serde_json::Value>, DiagramErrorCode> {
        let f = self
            .serialize_impl
            .as_ref()
            .ok_or(DiagramErrorCode::NotSerializable)?;
        f(builder, output)
    }

    pub(super) fn fork_clone(
        &self,
        builder: &mut Builder,
        output: DynOutput,
        amount: usize,
    ) -> Result<Vec<DynOutput>, DiagramErrorCode> {
        let f = self
            .fork_clone_impl
            .as_ref()
            .ok_or(DiagramErrorCode::NotCloneable)?;
        f(builder, output, amount)
    }

    pub(super) fn unzip(
        &self,
        builder: &mut Builder,
        output: DynOutput,
    ) -> Result<Vec<DynOutput>, DiagramErrorCode> {
        let unzip_impl = &self
            .unzip_impl
            .as_ref()
            .ok_or(DiagramErrorCode::NotUnzippable)?;
        unzip_impl.dyn_unzip(builder, output)
    }

    pub(super) fn fork_result(
        &self,
        builder: &mut Builder,
        output: DynOutput,
    ) -> Result<(DynOutput, DynOutput), DiagramErrorCode> {
        let f = self
            .fork_result_impl
            .as_ref()
            .ok_or(DiagramErrorCode::CannotForkResult)?;
        f(builder, output)
    }

    pub(super) fn split<'a>(
        &self,
        builder: &mut Builder,
        output: DynOutput,
        split_op: &'a SplitSchema,
    ) -> Result<DynSplitOutputs<'a>, DiagramErrorCode> {
        let f = self
            .split_impl
            .as_ref()
            .ok_or(DiagramErrorCode::NotSplittable)?;
        f(builder, output, split_op)
    }

    pub(super) fn join(
        &self,
        builder: &mut Builder,
        buffers: &BufferMap,
    ) -> Result<DynOutput, DiagramErrorCode> {
        let f = self
            .join_impl
            .as_ref()
            .ok_or(DiagramErrorCode::NotJoinable)?;
        f(builder, buffers)
    }

    pub(super) fn with_buffer_access(
        &self,
        builder: &mut Builder,
        output: DynOutput,
        buffers: &BufferMap,
    ) -> Result<DynOutput, DiagramErrorCode> {
        let f = self
            .buffer_access_impl
            .as_ref()
            .ok_or(DiagramErrorCode::CannotBufferAccess)?;
        f(builder, output, buffers)
    }

    pub(super) fn listen(
        &self,
        builder: &mut Builder,
        buffers: &BufferMap,
    ) -> Result<DynOutput, DiagramErrorCode> {
        let f = self
            .listen_impl
            .as_ref()
            .ok_or(DiagramErrorCode::CannotBufferAccess)?;
        f(builder, buffers)
    }
}

impl Serialize for MessageOperation {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s = serializer.serialize_map(None)?;
        if self.deserialize_impl.is_some() {
            s.serialize_entry("deserialize", &serde_json::Value::Null)?;
        }
        if self.serialize_impl.is_some() {
            s.serialize_entry("serialize", &serde_json::Value::Null)?;
        }
        if self.fork_clone_impl.is_some() {
            s.serialize_entry("fork_clone", &serde_json::Value::Null)?;
        }
        if let Some(unzip_impl) = &self.unzip_impl {
            s.serialize_entry("unzip", &json!({"output_types": unzip_impl.output_types()}))?;
        }
        if self.fork_result_impl.is_some() {
            s.serialize_entry("fork_result", &serde_json::Value::Null)?;
        }
        if self.split_impl.is_some() {
            s.serialize_entry("split", &serde_json::Value::Null)?;
        }
        if self.join_impl.is_some() {
            s.serialize_entry("join", &serde_json::Value::Null)?;
        }
        s.end()
    }
}

pub struct MessageRegistration {
    pub(super) type_name: &'static str,
    pub(super) schema: Option<schemars::schema::Schema>,
    pub(super) operations: MessageOperation,
}

impl MessageRegistration {
    pub(super) fn new<T>() -> Self
    where
        T: Send + Sync + 'static + Any,
    {
        Self {
            type_name: type_name::<T>(),
            schema: None,
            operations: MessageOperation::new::<T>(),
        }
    }
}

impl Serialize for MessageRegistration {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s = serializer.serialize_struct("MessageRegistration", 3)?;
        s.serialize_field("schema", &self.schema)?;
        s.serialize_field("operations", &self.operations)?;
        s.end()
    }
}

#[derive(Serialize)]
pub struct MessageRegistry {
    #[serde(serialize_with = "MessageRegistry::serialize_messages")]
    messages: HashMap<TypeInfo, MessageRegistration>,

    #[serde(
        rename = "schemas",
        serialize_with = "MessageRegistry::serialize_schemas"
    )]
    schema_generator: SchemaGenerator,
}

impl MessageRegistry {
    fn new() -> Self {
        let mut settings = SchemaSettings::default();
        settings.definitions_path = "#/schemas/".to_string();

        Self {
            schema_generator: SchemaGenerator::new(settings),
            messages: HashMap::from([(
                TypeInfo::of::<serde_json::Value>(),
                MessageRegistration::new::<serde_json::Value>(),
            )]),
        }
    }

    fn get<T>(&self) -> Option<&MessageRegistration>
    where
        T: Any,
    {
        self.messages.get(&TypeInfo::of::<T>())
    }

    pub(super) fn deserialize(
        &self,
        target_type: &TypeInfo,
        builder: &mut Builder,
        output: DynOutput,
    ) -> Result<DynOutput, DiagramErrorCode> {
        if output.type_info != TypeInfo::of::<serde_json::Value>()
            || &output.type_info == target_type
        {
            Ok(output)
        } else if let Some(reg) = self.messages.get(target_type) {
            reg.operations.deserialize(builder, output)
        } else {
            Err(DiagramErrorCode::NotSerializable)
        }
    }

    /// Register a deserialize function if not already registered, returns true if the new
    /// function is registered.
    pub(super) fn register_deserialize<T, Deserializer>(&mut self) -> bool
    where
        T: Send + Sync + 'static + Any,
        Deserializer: DeserializeMessage<T>,
    {
        let reg = self
            .messages
            .entry(TypeInfo::of::<T>())
            .or_insert(MessageRegistration::new::<T>());
        let ops = &mut reg.operations;
        if !Deserializer::deserializable() || ops.deserialize_impl.is_some() {
            return false;
        }

        debug!(
            "register deserialize for type: {}, with deserializer: {}",
            std::any::type_name::<T>(),
            std::any::type_name::<Deserializer>()
        );
        ops.deserialize_impl = Some(|builder, output| {
            debug!("deserialize output: {:?}", output);
            let receiver =
                builder.create_map_block(|json: serde_json::Value| Deserializer::from_json(json));
            builder.connect(output, receiver.input);
            let deserialized_output = receiver
                .output
                .chain(builder)
                .cancel_on_err()
                .output()
                .into();
            debug!("deserialized output: {:?}", deserialized_output);
            Ok(deserialized_output)
        });

        reg.schema = Deserializer::json_schema(&mut self.schema_generator);

        true
    }

    pub(super) fn serialize(
        &self,
        builder: &mut Builder,
        output: DynOutput,
    ) -> Result<Output<serde_json::Value>, DiagramErrorCode> {
        debug!("serialize {:?}", output);
        if output.type_info == TypeInfo::of::<serde_json::Value>() {
            output.into_output()
        } else if let Some(reg) = self.messages.get(&output.type_info) {
            reg.operations.serialize(builder, output)
        } else {
            Err(DiagramErrorCode::NotSerializable)
        }
    }

    /// Register a serialize function if not already registered, returns true if the new
    /// function is registered.
    pub(super) fn register_serialize<T, Serializer>(&mut self) -> bool
    where
        T: Send + Sync + 'static + Any,
        Serializer: SerializeMessage<T>,
    {
        register_serialize::<T, Serializer>(&mut self.messages, &mut self.schema_generator)
    }

    pub(super) fn fork_clone(
        &self,
        builder: &mut Builder,
        output: DynOutput,
        amount: usize,
    ) -> Result<Vec<DynOutput>, DiagramErrorCode> {
        if let Some(reg) = self.messages.get(&output.type_info) {
            reg.operations.fork_clone(builder, output, amount)
        } else {
            Err(DiagramErrorCode::NotCloneable)
        }
    }

    /// Register a fork_clone function if not already registered, returns true if the new
    /// function is registered.
    pub(super) fn register_fork_clone<T, F>(&mut self) -> bool
    where
        T: Send + Sync + 'static + Any,
        F: DynForkClone<T>,
    {
        let ops = &mut self
            .messages
            .entry(TypeInfo::of::<T>())
            .or_insert(MessageRegistration::new::<T>())
            .operations;
        if !F::CLONEABLE || ops.fork_clone_impl.is_some() {
            return false;
        }

        ops.fork_clone_impl =
            Some(|builder, output, amount| F::dyn_fork_clone(builder, output, amount));

        true
    }

    pub(super) fn unzip(
        &self,
        builder: &mut Builder,
        output: DynOutput,
    ) -> Result<Vec<DynOutput>, DiagramErrorCode> {
        if let Some(reg) = self.messages.get(&output.type_info) {
            reg.operations.unzip(builder, output)
        } else {
            Err(DiagramErrorCode::NotUnzippable)
        }
    }

    /// Register a unzip function if not already registered, returns true if the new
    /// function is registered.
    pub(super) fn register_unzip<T, Serializer>(&mut self) -> bool
    where
        T: Send + Sync + 'static + Any,
        Serializer: 'static,
        DefaultImplMarker<(T, Serializer)>: DynUnzip,
    {
        let unzip_impl = DefaultImplMarker::<(T, Serializer)>::new();
        unzip_impl.on_register(self);

        let ops = &mut self
            .messages
            .entry(TypeInfo::of::<T>())
            .or_insert(MessageRegistration::new::<T>())
            .operations;
        if ops.unzip_impl.is_some() {
            return false;
        }
        ops.unzip_impl = Some(Box::new(unzip_impl));

        true
    }

    pub(super) fn fork_result(
        &self,
        builder: &mut Builder,
        output: DynOutput,
    ) -> Result<(DynOutput, DynOutput), DiagramErrorCode> {
        if let Some(reg) = self.messages.get(&output.type_info) {
            reg.operations.fork_result(builder, output)
        } else {
            Err(DiagramErrorCode::CannotForkResult)
        }
    }

    /// Register a fork_result function if not already registered, returns true if the new
    /// function is registered.
    pub(super) fn register_fork_result<T>(&mut self, implementation: T) -> bool
    where
        T: DynForkResult,
    {
        implementation.on_register(&mut self.messages, &mut self.schema_generator)
    }

    pub(super) fn split<'b>(
        &self,
        builder: &mut Builder,
        output: DynOutput,
        split_op: &'b SplitSchema,
    ) -> Result<DynSplitOutputs<'b>, DiagramErrorCode> {
        if let Some(reg) = self.messages.get(&output.type_info) {
            reg.operations.split(builder, output, split_op)
        } else {
            Err(DiagramErrorCode::NotSplittable)
        }
    }

    /// Register a split function if not already registered, returns true if the new
    /// function is registered.
    pub(super) fn register_split<T, F, S>(&mut self) -> bool
    where
        T: Send + Sync + 'static + Any,
        F: DynSplit<T, S>,
    {
        let ops = &mut self
            .messages
            .entry(TypeInfo::of::<T>())
            .or_insert(MessageRegistration::new::<T>())
            .operations;
        if ops.split_impl.is_some() {
            return false;
        }

        ops.split_impl = Some(Box::new(|builder, output, split_op| {
            F::dyn_split(builder, output, split_op)
        }));
        F::on_register(self);

        true
    }

    pub(super) fn join(
        &self,
        builder: &mut Builder,
        buffers: &BufferMap,
        joinable: TypeInfo,
    ) -> Result<DynOutput, DiagramErrorCode> {
        if let Some(reg) = self.messages.get(&joinable) {
            reg.operations.join(builder, buffers)
        } else {
            Err(DiagramErrorCode::NotJoinable)
        }
    }

    /// Register a join function if not already registered, returns true if the new
    /// function is registered.
    pub(super) fn register_join<T>(&mut self) -> bool
    where
        T: Send + Sync + 'static + Any + Joined,
    {
        let ops = &mut self
            .messages
            .entry(TypeInfo::of::<T>())
            .or_insert(MessageRegistration::new::<T>())
            .operations;
        if ops.join_impl.is_some() {
            return false;
        }

        ops.join_impl =
            Some(|builder, buffers| Ok(builder.try_join::<T>(buffers)?.output().into()));

        true
    }

    pub(super) fn with_buffer_access(
        &self,
        builder: &mut Builder,
        output: DynOutput,
        buffers: &BufferMap,
        target_type: TypeInfo,
    ) -> Result<DynOutput, DiagramErrorCode> {
        if let Some(reg) = self.messages.get(&target_type) {
            reg.operations.with_buffer_access(builder, output, buffers)
        } else {
            Err(unknown_diagram_error!())
        }
    }

    pub(super) fn register_buffer_access<T>(&mut self) -> bool
    where
        T: Send + Sync + 'static + BufferAccessRequest,
    {
        let ops = &mut self
            .messages
            .entry(TypeInfo::of::<T>())
            .or_insert(MessageRegistration::new::<T>())
            .operations;
        if ops.buffer_access_impl.is_some() {
            return false;
        }

        ops.buffer_access_impl = Some(|builder, output, buffers| {
            let buffer_access =
                builder.try_create_buffer_access::<T::Message, T::BufferKeys>(buffers)?;
            builder.connect(output.into_output::<T::Message>()?, buffer_access.input);
            Ok(buffer_access.output.into())
        });

        true
    }

    pub(super) fn listen(
        &self,
        builder: &mut Builder,
        buffers: &BufferMap,
        target_type: TypeInfo,
    ) -> Result<DynOutput, DiagramErrorCode> {
        if let Some(reg) = self.messages.get(&target_type) {
            reg.operations.listen(builder, buffers)
        } else {
            Err(DiagramErrorCode::CannotListen(target_type))
        }
    }

    pub(super) fn register_listen<T>(&mut self) -> bool
    where
        T: Send + Sync + 'static + Any + Accessor,
    {
        let ops = &mut self
            .messages
            .entry(TypeInfo::of::<T>())
            .or_insert(MessageRegistration::new::<T>())
            .operations;
        if ops.listen_impl.is_some() {
            return false;
        }

        ops.listen_impl =
            Some(|builder, buffers| Ok(builder.try_listen::<T>(buffers)?.output().into()));

        true
    }

    fn serialize_messages<S>(
        v: &HashMap<TypeInfo, MessageRegistration>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s = serializer.serialize_map(Some(v.len()))?;
        for (type_id, reg) in v {
            // hide builtin registrations
            if type_id == &TypeInfo::of::<serde_json::Value>() {
                continue;
            }

            // should we use short name? It makes the serialized json more readable at the cost
            // of greatly increased chance of key conflicts.
            // let short_name = {
            //     if let Some(start) = reg.type_name.rfind(":") {
            //         &reg.type_name[start + 1..]
            //     } else {
            //         reg.type_name
            //     }
            // };
            s.serialize_entry(reg.type_name, reg)?;
        }
        s.end()
    }

    fn serialize_schemas<S>(v: &SchemaGenerator, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        v.definitions().serialize(serializer)
    }
}

impl Default for DiagramElementRegistry {
    fn default() -> Self {
        // Ensure buffer downcasting is automatically registered for all basic
        // serializable types.
        JsonBuffer::register_for::<()>();

        DiagramElementRegistry {
            nodes: Default::default(),
            messages: MessageRegistry::new(),
        }
    }
}

impl DiagramElementRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a node builder with all the common operations (deserialize the
    /// request, serialize the response, and clone the response) enabled.
    ///
    /// You will receive a [`NodeRegistrationBuilder`] which you can then use to
    /// enable more operations around your node, such as fork result, split,
    /// or unzip. The data types of your node need to be suitable for those
    /// operations or else the compiler will not allow you to enable them.
    ///
    /// ```
    /// use bevy_impulse::{NodeBuilderOptions, DiagramElementRegistry};
    ///
    /// let mut registry = DiagramElementRegistry::new();
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
    ) -> NodeRegistrationBuilder<Request, Response, Streams>
    where
        Config: JsonSchema + DeserializeOwned,
        Request: Send + Sync + 'static + DynType + Serialize + DeserializeOwned + Clone,
        Response: Send + Sync + 'static + DynType + Serialize + DeserializeOwned + Clone,
    {
        self.opt_out().register_node_builder(options, builder)
    }

    /// Register a single message for general use between nodes. This will
    /// include all common operations for the message (deserialize, serialize,
    /// and clone).
    ///
    /// You will receive a [`MessageRegistrationBuilder`] which you can then use
    /// to enable more operations for the message, such as forking, splitting,
    /// unzipping, and joining. The message type needs to be suitable for each
    /// operation that you register for it or else the compiler will not allow
    /// you to enable them.
    ///
    /// Use [`Self::opt_out`] to opt out of specified common operations before
    /// beginning to register the message. This allows you to register message
    /// types that do not support one or more of the common operations.
    pub fn register_message<Message>(&mut self) -> MessageRegistrationBuilder<Message>
    where
        Message: Send + Sync + 'static + DynType + DeserializeOwned + Serialize + Clone,
    {
        self.opt_out().register_message()
    }

    /// In some cases the common operations of deserialization, serialization,
    /// and cloning cannot be performed for the input or output message of a node.
    /// When that happens you can still register your node builder by calling
    /// this function and explicitly disabling the common operations that your
    /// node cannot support.
    ///
    /// In order for a message type to support all the common operations, it
    /// must implement [`schemars::JsonSchema`], [`serde::de::DeserializeOwned`],
    /// [`serde::Serialize`], and [`Clone`].
    ///
    /// ```
    /// use schemars::JsonSchema;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(JsonSchema, Serialize, Deserialize, Clone)]
    /// struct MyCommonMessage {}
    /// ```
    ///
    /// If your node has an input or output message that is missing one of these
    /// traits, you can still register it by opting out of the relevant common
    /// operation(s):
    ///
    /// ```
    /// use bevy_impulse::{NodeBuilderOptions, DiagramElementRegistry};
    ///
    /// struct NonSerializable {
    ///     data: String
    /// }
    ///
    /// let mut registry = DiagramElementRegistry::new();
    /// registry
    ///     .opt_out()
    ///     .no_deserializing()
    ///     .no_serializing()
    ///     .no_cloning()
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

    pub fn get_node_registration<Q>(&self, id: &Q) -> Result<&NodeRegistration, DiagramErrorCode>
    where
        Q: Borrow<str> + ?Sized,
    {
        let k = id.borrow();
        self.nodes
            .get(k)
            .ok_or(DiagramErrorCode::BuilderNotFound(k.to_string()))
    }

    pub fn get_message_registration<T>(&self) -> Option<&MessageRegistration>
    where
        T: Any,
    {
        self.messages.get::<T>()
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

    /// Some extra impl only used in tests (for now).
    /// If these impls are needed outside tests, then move them to the main impl.
    impl MessageOperation {
        fn deserializable(&self) -> bool {
            self.deserialize_impl.is_some()
        }

        fn serializable(&self) -> bool {
            self.serialize_impl.is_some()
        }

        fn cloneable(&self) -> bool {
            self.fork_clone_impl.is_some()
        }

        fn unzippable(&self) -> bool {
            self.unzip_impl.is_some()
        }

        fn can_fork_result(&self) -> bool {
            self.fork_result_impl.is_some()
        }

        fn splittable(&self) -> bool {
            self.split_impl.is_some()
        }

        fn joinable(&self) -> bool {
            self.join_impl.is_some()
        }
    }

    #[test]
    fn test_register_node_builder() {
        let mut registry = DiagramElementRegistry::new();
        registry.opt_out().register_node_builder(
            NodeBuilderOptions::new("multiply3").with_name("Test Name"),
            |builder, _config: ()| builder.create_map_block(multiply3),
        );
        let req_ops = &registry.messages.get::<i64>().unwrap().operations;
        let resp_ops = &registry.messages.get::<i64>().unwrap().operations;
        assert!(req_ops.deserializable());
        assert!(resp_ops.serializable());
        assert!(resp_ops.cloneable());
        assert!(!resp_ops.unzippable());
        assert!(!resp_ops.can_fork_result());
        assert!(!resp_ops.splittable());
        assert!(!resp_ops.joinable());
    }

    #[test]
    fn test_register_cloneable_node() {
        let mut registry = DiagramElementRegistry::new();
        registry.register_node_builder(
            NodeBuilderOptions::new("multiply3").with_name("Test Name"),
            |builder, _config: ()| builder.create_map_block(multiply3),
        );
        let req_ops = &registry.messages.get::<i64>().unwrap().operations;
        let resp_ops = &registry.messages.get::<i64>().unwrap().operations;
        assert!(req_ops.deserializable());
        assert!(resp_ops.serializable());
        assert!(resp_ops.cloneable());
    }

    #[test]
    fn test_register_unzippable_node() {
        let mut registry = DiagramElementRegistry::new();
        let tuple_resp = |_: ()| -> (i64,) { (1,) };
        registry
            .opt_out()
            .no_cloning()
            .register_node_builder(
                NodeBuilderOptions::new("multiply3_uncloneable").with_name("Test Name"),
                move |builder: &mut Builder, _config: ()| builder.create_map_block(tuple_resp),
            )
            .with_unzip();
        let req_ops = &registry.messages.get::<()>().unwrap().operations;
        let resp_ops = &registry.messages.get::<(i64,)>().unwrap().operations;
        assert!(req_ops.deserializable());
        assert!(resp_ops.serializable());
        assert!(resp_ops.unzippable());
    }

    #[test]
    fn test_register_splittable_node() {
        let mut registry = DiagramElementRegistry::new();
        let vec_resp = |_: ()| -> Vec<i64> { vec![1, 2] };

        registry
            .register_node_builder(
                NodeBuilderOptions::new("vec_resp").with_name("Test Name"),
                move |builder: &mut Builder, _config: ()| builder.create_map_block(vec_resp),
            )
            .with_split();
        assert!(registry
            .messages
            .get::<Vec<i64>>()
            .unwrap()
            .operations
            .splittable());

        let map_resp = |_: ()| -> HashMap<String, i64> { HashMap::new() };
        registry
            .register_node_builder(
                NodeBuilderOptions::new("map_resp").with_name("Test Name"),
                move |builder: &mut Builder, _config: ()| builder.create_map_block(map_resp),
            )
            .with_split();
        assert!(registry
            .messages
            .get::<HashMap<String, i64>>()
            .unwrap()
            .operations
            .splittable());

        registry.register_node_builder(
            NodeBuilderOptions::new("not_splittable").with_name("Test Name"),
            move |builder: &mut Builder, _config: ()| builder.create_map_block(map_resp),
        );
        // even though we didn't register with `with_split`, it is still splittable because we
        // previously registered another splittable node with the same response type.
        assert!(registry
            .messages
            .get::<HashMap<String, i64>>()
            .unwrap()
            .operations
            .splittable());
    }

    #[test]
    fn test_register_with_config() {
        let mut registry = DiagramElementRegistry::new();

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
        assert!(registry.get_node_registration("multiply").is_ok());
    }

    struct NonSerializableRequest {}

    #[test]
    fn test_register_opaque_node() {
        let opaque_request_map = |_: NonSerializableRequest| {};

        let mut registry = DiagramElementRegistry::new();
        registry
            .opt_out()
            .no_serializing()
            .no_deserializing()
            .no_cloning()
            .register_node_builder(
                NodeBuilderOptions::new("opaque_request_map").with_name("Test Name"),
                move |builder, _config: ()| builder.create_map_block(opaque_request_map),
            )
            .with_serialize_response();
        assert!(registry.get_node_registration("opaque_request_map").is_ok());
        let req_ops = &registry
            .messages
            .get::<NonSerializableRequest>()
            .unwrap()
            .operations;
        let resp_ops = &registry.messages.get::<()>().unwrap().operations;
        assert!(!req_ops.deserializable());
        assert!(resp_ops.serializable());

        let opaque_response_map = |_: ()| NonSerializableRequest {};
        registry
            .opt_out()
            .no_serializing()
            .no_deserializing()
            .no_cloning()
            .register_node_builder(
                NodeBuilderOptions::new("opaque_response_map").with_name("Test Name"),
                move |builder: &mut Builder, _config: ()| {
                    builder.create_map_block(opaque_response_map)
                },
            )
            .with_deserialize_request();
        assert!(registry
            .get_node_registration("opaque_response_map")
            .is_ok());
        let req_ops = &registry.messages.get::<()>().unwrap().operations;
        let resp_ops = &registry
            .messages
            .get::<NonSerializableRequest>()
            .unwrap()
            .operations;
        assert!(req_ops.deserializable());
        assert!(!resp_ops.serializable());

        let opaque_req_resp_map = |_: NonSerializableRequest| NonSerializableRequest {};
        registry
            .opt_out()
            .no_deserializing()
            .no_serializing()
            .no_cloning()
            .register_node_builder(
                NodeBuilderOptions::new("opaque_req_resp_map").with_name("Test Name"),
                move |builder: &mut Builder, _config: ()| {
                    builder.create_map_block(opaque_req_resp_map)
                },
            );
        assert!(registry
            .get_node_registration("opaque_req_resp_map")
            .is_ok());
        let req_ops = &registry
            .messages
            .get::<NonSerializableRequest>()
            .unwrap()
            .operations;
        let resp_ops = &registry
            .messages
            .get::<NonSerializableRequest>()
            .unwrap()
            .operations;
        assert!(!req_ops.deserializable());
        assert!(!resp_ops.serializable());
    }

    #[test]
    fn test_register_message() {
        let mut registry = DiagramElementRegistry::new();

        #[derive(Deserialize, Serialize, JsonSchema, Clone)]
        struct TestMessage;

        registry.opt_out().register_message::<TestMessage>();

        let ops = &registry
            .get_message_registration::<TestMessage>()
            .unwrap()
            .operations;
        assert!(ops.deserializable());
        assert!(ops.serializable());
        assert!(ops.cloneable());
        assert!(!ops.unzippable());
        assert!(!ops.can_fork_result());
        assert!(!ops.splittable());
        assert!(!ops.joinable());
    }

    #[test]
    fn test_serialize_registry() {
        let mut reg = DiagramElementRegistry::new();

        #[derive(Deserialize, Serialize, JsonSchema, Clone)]
        struct Foo {
            hello: String,
        }

        #[derive(Deserialize, Serialize, JsonSchema, Clone)]
        struct Bar {
            foo: Foo,
        }

        struct Opaque;

        reg.opt_out()
            .no_serializing()
            .no_deserializing()
            .no_cloning()
            .register_node_builder(NodeBuilderOptions::new("test"), |builder, _config: ()| {
                builder.create_map_block(|_: Opaque| {
                    (
                        Foo {
                            hello: "hello".to_string(),
                        },
                        Bar {
                            foo: Foo {
                                hello: "world".to_string(),
                            },
                        },
                    )
                })
            })
            .with_unzip();

        // print out a pretty json for manual inspection
        println!("{}", serde_json::to_string_pretty(&reg).unwrap());

        // test that schema refs are pointing to the correct path
        let value = serde_json::to_value(&reg).unwrap();
        let messages = &value["messages"];
        let schemas = &value["schemas"];
        let bar_schema = &messages[type_name::<Bar>()]["schema"];
        assert_eq!(bar_schema["$ref"].as_str().unwrap(), "#/schemas/Bar");
        assert!(schemas.get("Bar").is_some());
        assert!(schemas.get("Foo").is_some());
    }
}
