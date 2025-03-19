use std::{
    any::{type_name, Any},
    borrow::{Borrow, Cow},
    cell::RefCell,
    collections::HashMap,
    marker::PhantomData,
};

use crate::{
    unknown_diagram_error, Accessor, BufferMap, Builder, ForRemaining, FromSequential,
    FromSpecific, InputSlot, Joined, MapSplitKey, Node, Output, Splittable, StreamPack,
};
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
    buffer::BufferAccessRequest,
    fork_clone::DynForkClone,
    fork_result::DynForkResult,
    impls::{DefaultImpl, DefaultImplMarker, NotSupported},
    transform::TransformError,
    type_info::TypeInfo,
    unzip::DynUnzip,
    BuilderId, DeserializeFn, DeserializeMessage, DiagramErrorCode, DynInputSlot, DynOutput,
    DynSplit, DynSplitOutputs, FromCelValueFn, JsonDeserializer, JsonSerializer, Section,
    SectionMetadata, SectionMetadataProvider, SerializeCel, SerializeFn, SerializeMessage, SplitOp,
    ToCelValueFn,
};

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
    #[serde(rename = "$key$")]
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
type ForkCloneFn = fn(&mut Builder, DynOutput, usize) -> Result<Vec<DynOutput>, DiagramErrorCode>;
type ForkResultFn = fn(&mut Builder, DynOutput) -> Result<(DynOutput, DynOutput), DiagramErrorCode>;
type SplitFn = Box<
    dyn for<'a> Fn(
        &mut Builder,
        DynOutput,
        &'a SplitOp,
    ) -> Result<DynSplitOutputs, DiagramErrorCode>,
>;
type JoinFn = fn(&mut Builder, &BufferMap) -> Result<DynOutput, DiagramErrorCode>;
type BufferAccessFn =
    fn(&mut Builder, DynOutput, &BufferMap) -> Result<DynOutput, DiagramErrorCode>;
type ListenFn = fn(&mut Builder, &BufferMap) -> Result<DynOutput, DiagramErrorCode>;

#[must_use]
pub struct CommonOperations<'a, SerializationOptionsT, Deserialize, Serialize, Cloneable>
where
    SerializationOptionsT: SerializationOptions,
{
    registry: &'a mut DiagramElementRegistry<SerializationOptionsT>,
    _ignore: PhantomData<(Deserialize, Serialize, Cloneable)>,
}

impl<'a, SerializationOptionsT, DeserializerT, SerializerT, Cloneable>
    CommonOperations<'a, SerializationOptionsT, DeserializerT, SerializerT, Cloneable>
where
    SerializationOptionsT: SerializationOptions,
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
    ) -> NodeRegistrationBuilder<'a, SerializationOptionsT, Request, Response, Streams>
    where
        Config: JsonSchema + DeserializeOwned,
        Request: Send + Sync + 'static,
        Response: Send + Sync + 'static,
        Streams: StreamPack,
        DeserializerT: DeserializeMessage<Request, SerializationOptionsT::Serialized>,
        SerializerT: SerializeMessage<Response, SerializationOptionsT::Serialized>,
        Cloneable: DynForkClone<Response>,
    {
        self.registry
            .messages
            .register_deserialize::<Request, DeserializerT>();
        self.registry
            .messages
            .register_serialize::<Response, SerializerT>();
        self.registry
            .messages
            .register_fork_clone::<Response, Cloneable>();

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

        NodeRegistrationBuilder::<SerializationOptionsT, Request, Response, Streams>::new(
            &mut self.registry.messages,
        )
    }

    /// Register a message with the specified common operations.
    pub fn register_message<Message>(
        self,
    ) -> MessageRegistrationBuilder<'a, Message, SerializationOptionsT>
    where
        Message: Send + Sync + 'static,
        DeserializerT: DeserializeMessage<Message, SerializationOptionsT::Serialized>,
        SerializerT: SerializeMessage<Message, SerializationOptionsT::Serialized>,
        Cloneable: DynForkClone<Message>,
    {
        self.registry
            .messages
            .register_deserialize::<Message, DeserializerT>();
        self.registry
            .messages
            .register_serialize::<Message, SerializerT>();
        self.registry
            .messages
            .register_fork_clone::<Message, Cloneable>();

        MessageRegistrationBuilder::<Message, SerializationOptionsT>::new(
            &mut self.registry.messages,
        )
    }

    /// Opt out of deserializing the request of the node. Use this to build a
    /// node whose request type is not deserializable.
    pub fn no_request_deserializing(
        self,
    ) -> CommonOperations<'a, SerializationOptionsT, NotSupported, SerializerT, Cloneable> {
        CommonOperations {
            registry: self.registry,
            _ignore: Default::default(),
        }
    }

    /// Opt out of serializing the response of the node. Use this to build a
    /// node whose response type is not serializable.
    pub fn no_response_serializing(
        self,
    ) -> CommonOperations<'a, SerializationOptionsT, DeserializerT, NotSupported, Cloneable> {
        CommonOperations {
            registry: self.registry,
            _ignore: Default::default(),
        }
    }

    /// Opt out of cloning the response of the node. Use this to build a node
    /// whose response type is not cloneable.
    pub fn no_response_cloning(
        self,
    ) -> CommonOperations<'a, SerializationOptionsT, DeserializerT, SerializerT, NotSupported> {
        CommonOperations {
            registry: self.registry,
            _ignore: Default::default(),
        }
    }
}

pub struct MessageRegistrationBuilder<'a, Message, SerializationOptionsT>
where
    SerializationOptionsT: SerializationOptions,
{
    data: &'a mut MessageRegistry<SerializationOptionsT>,
    _ignore: PhantomData<Message>,
}

impl<'a, Message, SerializationOptionsT>
    MessageRegistrationBuilder<'a, Message, SerializationOptionsT>
where
    Message: Send + Sync + 'static + Any,
    SerializationOptionsT: SerializationOptions,
{
    pub fn new(registry: &'a mut MessageRegistry<SerializationOptionsT>) -> Self {
        Self {
            data: registry,
            _ignore: Default::default(),
        }
    }

    /// Mark the message as having a unzippable response. This is required in order for the node
    /// to be able to be connected to a "Unzip" operation.
    pub fn with_unzip(&mut self) -> &mut Self
    where
        DefaultImplMarker<(Message, SerializationOptionsT::DefaultSerializer)>:
            DynUnzip<SerializationOptionsT>,
        SerializationOptionsT::DefaultSerializer: 'static,
    {
        self.data
            .register_unzip::<Message, SerializationOptionsT::DefaultSerializer>();
        self
    }

    /// Mark the message as having an unzippable response whose elements are not serializable.
    pub fn with_unzip_minimal(&mut self) -> &mut Self
    where
        DefaultImplMarker<(Message, NotSupported)>: DynUnzip<SerializationOptionsT>,
    {
        self.data.register_unzip::<Message, NotSupported>();
        self
    }

    /// Mark the message as having a [`Result<_, _>`] response. This is required in order for the node
    /// to be able to be connected to a "Fork Result" operation.
    pub fn with_fork_result(&mut self) -> &mut Self
    where
        DefaultImplMarker<(Message, NotSupported)>: DynForkResult,
    {
        self.data
            .register_fork_result(DefaultImplMarker::<(Message, NotSupported)>::new());
        self
    }

    /// Mark the message as having a splittable response. This is required in order
    /// for the node to be able to be connected to a "Split" operation.
    pub fn with_split(&mut self) -> &mut Self
    where
        Message: Splittable,
        DefaultImpl: DynSplit<Message, SerializationOptionsT::DefaultSerializer>,
        SerializationOptionsT::DefaultSerializer:
            SerializeMessage<Message::Item, SerializationOptionsT::Serialized>,
    {
        self.data
            .register_split::<Message, DefaultImpl, SerializationOptionsT::DefaultSerializer>();
        self
    }

    /// Mark the message as having a splittable response but the items from the split
    /// are unserializable.
    pub fn with_split_minimal(&mut self) -> &mut Self
    where
        Message: Splittable,
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

pub struct NodeRegistrationBuilder<'a, SerializationOptionsT, Request, Response, Streams>
where
    SerializationOptionsT: SerializationOptions,
{
    registry: &'a mut MessageRegistry<SerializationOptionsT>,
    _ignore: PhantomData<(Request, Response, Streams)>,
}

impl<'a, SerializationOptionsT, Request, Response, Streams>
    NodeRegistrationBuilder<'a, SerializationOptionsT, Request, Response, Streams>
where
    SerializationOptionsT: SerializationOptions,
    Request: Send + Sync + 'static + Any,
    Response: Send + Sync + 'static + Any,
{
    fn new(registry: &'a mut MessageRegistry<SerializationOptionsT>) -> Self {
        Self {
            registry,
            _ignore: Default::default(),
        }
    }

    /// Mark the node as having a unzippable response. This is required in order for the node
    /// to be able to be connected to a "Unzip" operation.
    pub fn with_unzip(&mut self) -> &mut Self
    where
        DefaultImplMarker<(Response, SerializationOptionsT::DefaultSerializer)>:
            DynUnzip<SerializationOptionsT>,
        SerializationOptionsT::DefaultSerializer: 'static,
    {
        MessageRegistrationBuilder::<Response, SerializationOptionsT>::new(self.registry)
            .with_unzip();
        self
    }

    /// Mark the node as having an unzippable response whose elements are not serializable.
    pub fn with_unzip_unserializable(&mut self) -> &mut Self
    where
        DefaultImplMarker<(Response, NotSupported)>: DynUnzip<SerializationOptionsT>,
    {
        MessageRegistrationBuilder::<Response, SerializationOptionsT>::new(self.registry)
            .with_unzip_minimal();
        self
    }

    /// Mark the node as having a [`Result<_, _>`] response. This is required in order for the node
    /// to be able to be connected to a "Fork Result" operation.
    pub fn with_fork_result(&mut self) -> &mut Self
    where
        DefaultImplMarker<(Response, NotSupported)>: DynForkResult,
    {
        MessageRegistrationBuilder::<Response, SerializationOptionsT>::new(self.registry)
            .with_fork_result();
        self
    }

    /// Mark the node as having a splittable response. This is required in order
    /// for the node to be able to be connected to a "Split" operation.
    pub fn with_split(&mut self) -> &mut Self
    where
        Response: Splittable,
        DefaultImpl: DynSplit<Response, SerializationOptionsT::DefaultSerializer>,
        SerializationOptionsT::DefaultSerializer:
            SerializeMessage<Response::Item, SerializationOptionsT::Serialized>,
    {
        MessageRegistrationBuilder::<Response, SerializationOptionsT>::new(self.registry)
            .with_split();
        self
    }

    /// Mark the node as having a splittable response but the items from the split
    /// are unserializable.
    pub fn with_split_unserializable(&mut self) -> &mut Self
    where
        Response: Splittable,
        DefaultImpl: DynSplit<Response, NotSupported>,
    {
        MessageRegistrationBuilder::<Response, SerializationOptionsT>::new(self.registry)
            .with_split_minimal();
        self
    }

    /// Mark the node as having a joinable request.
    pub fn with_join(&mut self) -> &mut Self
    where
        Request: Joined,
    {
        MessageRegistrationBuilder::<Request, SerializationOptionsT>::new(self.registry)
            .with_join();
        self
    }

    /// Mark the node as having a buffer access request.
    pub fn with_buffer_access(&mut self) -> &mut Self
    where
        Request: BufferAccessRequest,
    {
        MessageRegistrationBuilder::<Request, SerializationOptionsT>::new(self.registry)
            .with_buffer_access();
        self
    }

    /// Mark the node as having a listen request.
    pub fn with_listen(&mut self) -> &mut Self
    where
        Request: Accessor,
    {
        MessageRegistrationBuilder::<Request, SerializationOptionsT>::new(self.registry)
            .with_listen();
        self
    }
}

type CreateSectionFn<SerializationOptionsT> =
    dyn FnMut(&mut Builder, serde_json::Value) -> Box<dyn Section<SerializationOptionsT>>;

#[derive(Serialize)]
pub struct SectionRegistration<SerializationOptionsT>
where
    SerializationOptionsT: SerializationOptions,
{
    pub(super) name: String,
    pub(super) metadata: SectionMetadata,
    pub(super) config_schema: Schema,

    #[serde(skip)]
    create_section_impl: RefCell<Box<CreateSectionFn<SerializationOptionsT>>>,
}

impl<SerializationOptionsT> SectionRegistration<SerializationOptionsT>
where
    SerializationOptionsT: SerializationOptions,
{
    pub(super) fn create_section(
        &self,
        builder: &mut Builder,
        config: serde_json::Value,
    ) -> Result<Box<dyn Section<SerializationOptionsT>>, DiagramErrorCode> {
        let section = (self.create_section_impl.borrow_mut())(builder, config);
        Ok(section)
    }
}

pub trait IntoSectionRegistration<SerializationOptionsT, SectionT, Config>
where
    SerializationOptionsT: SerializationOptions,
{
    fn into_section_registration(
        self,
        name: String,
        schema_generator: &mut SchemaGenerator,
    ) -> SectionRegistration<SerializationOptionsT>;
}

impl<F, SerializationOptionsT, SectionT, Config>
    IntoSectionRegistration<SerializationOptionsT, SectionT, Config> for F
where
    F: 'static + FnMut(&mut Builder, Config) -> SectionT,
    SectionT: 'static + Section<SerializationOptionsT> + SectionMetadataProvider,
    SerializationOptionsT: SerializationOptions,
    Config: DeserializeOwned + JsonSchema,
{
    fn into_section_registration(
        mut self,
        name: String,
        schema_generator: &mut SchemaGenerator,
    ) -> SectionRegistration<SerializationOptionsT> {
        SectionRegistration {
            name,
            metadata: SectionT::metadata().clone(),
            config_schema: schema_generator.subschema_for::<()>(),
            create_section_impl: RefCell::new(Box::new(move |builder, config| {
                let section = self(builder, serde_json::from_value::<Config>(config).unwrap());
                Box::new(section)
            })),
        }
    }
}

pub trait SerializationOptions {
    type SplitKey: FromSequential + FromSpecific<SpecificKey = String> + ForRemaining;
    type Serialized: Send + Sync + 'static + Splittable<Key = Self::SplitKey> + Clone;
    type DefaultDeserializer;
    type DefaultSerializer: SerializeCel<Self::Serialized>
        + SerializeMessage<Vec<Self::Serialized>, Self::Serialized>
        + SerializeMessage<HashMap<Cow<'static, str>, Self::Serialized>, Self::Serialized>;
}

#[derive(Serialize)]
pub struct JsonSerialization;

impl SerializationOptions for JsonSerialization {
    type SplitKey = MapSplitKey<String>;
    type Serialized = serde_json::Value;
    type DefaultDeserializer = JsonDeserializer;
    type DefaultSerializer = JsonSerializer;
}

#[derive(Serialize)]
pub struct DiagramElementRegistry<SerializationOptionsT>
where
    SerializationOptionsT: SerializationOptions,
{
    pub(super) nodes: HashMap<BuilderId, NodeRegistration>,
    pub(super) sections: HashMap<BuilderId, SectionRegistration<SerializationOptionsT>>,

    #[serde(flatten)]
    pub(super) messages: MessageRegistry<SerializationOptionsT>,
}

pub type JsonDiagramRegistry = DiagramElementRegistry<JsonSerialization>;

pub(super) struct MessageOperation<SerializationOptionsT>
where
    SerializationOptionsT: SerializationOptions,
{
    pub(super) from_cel_value_impl: Option<FromCelValueFn>,
    pub(super) deserialize_impl: Option<DeserializeFn<SerializationOptionsT::Serialized>>,
    pub(super) to_cel_value_impl: Option<ToCelValueFn>,
    pub(super) serialize_impl: Option<SerializeFn<SerializationOptionsT::Serialized>>,
    pub(super) fork_clone_impl: Option<ForkCloneFn>,
    pub(super) unzip_impl: Option<Box<dyn DynUnzip<SerializationOptionsT>>>,
    pub(super) fork_result_impl: Option<ForkResultFn>,
    pub(super) split_impl: Option<SplitFn>,
    pub(super) join_impl: Option<JoinFn>,
    pub(super) buffer_access_impl: Option<BufferAccessFn>,
    pub(super) listen_impl: Option<ListenFn>,
}

impl<SerializationOptionsT> MessageOperation<SerializationOptionsT>
where
    SerializationOptionsT: SerializationOptions,
{
    fn new<T>() -> Self
    where
        T: Send + Sync + 'static + Any,
    {
        Self {
            from_cel_value_impl: None,
            deserialize_impl: None,
            to_cel_value_impl: None,
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

    pub(super) fn from_cel_value(
        &self,
        builder: &mut Builder,
        output: Output<cel_interpreter::Value>,
    ) -> Result<DynOutput, DiagramErrorCode> {
        let f = self
            .from_cel_value_impl
            .as_ref()
            .ok_or(TransformError::NotSupported)?;
        f(output, builder)
    }

    /// Try to deserialize `output` into `input_type`. If `output` is not `serde_json::Value`, this does nothing.
    pub(super) fn deserialize(
        &self,
        builder: &mut Builder,
        output: Output<SerializationOptionsT::Serialized>,
    ) -> Result<DynOutput, DiagramErrorCode>
    where
        SerializationOptionsT::Serialized: Send + Sync + 'static,
    {
        let f = self
            .deserialize_impl
            .as_ref()
            .ok_or(DiagramErrorCode::NotSerializable)?;
        f(output, builder)
    }

    pub(super) fn to_cel_value(
        &self,
        builder: &mut Builder,
        output: DynOutput,
    ) -> Result<Output<cel_interpreter::Value>, DiagramErrorCode> {
        let f = self
            .to_cel_value_impl
            .as_ref()
            .ok_or(TransformError::NotSupported)?;
        f(output, builder)
    }

    pub(super) fn serialize(
        &self,
        builder: &mut Builder,
        output: DynOutput,
    ) -> Result<Output<SerializationOptionsT::Serialized>, DiagramErrorCode> {
        let f = self
            .serialize_impl
            .as_ref()
            .ok_or(DiagramErrorCode::NotSerializable)?;
        f(output, builder)
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
        split_op: &'a SplitOp,
    ) -> Result<DynSplitOutputs, DiagramErrorCode> {
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
            .ok_or(DiagramErrorCode::CannotListen)?;
        f(builder, buffers)
    }
}

impl<SerializationOptionsT> Serialize for MessageOperation<SerializationOptionsT>
where
    SerializationOptionsT: SerializationOptions,
{
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

pub struct MessageRegistration<SerializationOptionsT>
where
    SerializationOptionsT: SerializationOptions,
{
    pub(super) type_name: &'static str,
    pub(super) schema: Option<schemars::schema::Schema>,
    pub(super) operations: MessageOperation<SerializationOptionsT>,
}

impl<SerializationOptionsT> MessageRegistration<SerializationOptionsT>
where
    SerializationOptionsT: SerializationOptions,
{
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

impl<SerializationOptionsT> Serialize for MessageRegistration<SerializationOptionsT>
where
    SerializationOptionsT: SerializationOptions,
{
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
pub struct MessageRegistry<SerializationOptionsT>
where
    SerializationOptionsT: SerializationOptions,
    SerializationOptionsT::Serialized: Send + Sync + 'static,
{
    #[serde(serialize_with = "MessageRegistry::<SerializationOptionsT>::serialize_messages")]
    messages: HashMap<TypeInfo, MessageRegistration<SerializationOptionsT>>,

    #[serde(
        rename = "schemas",
        serialize_with = "MessageRegistry::<SerializationOptionsT>::serialize_schemas"
    )]
    schema_generator: SchemaGenerator,
}

impl<SerializationOptionsT> MessageRegistry<SerializationOptionsT>
where
    SerializationOptionsT: SerializationOptions,
    SerializationOptionsT::Serialized: Send + Sync + 'static,
    SerializationOptionsT::DefaultSerializer:
        SerializeMessage<SerializationOptionsT::Serialized, SerializationOptionsT::Serialized>,
    SerializationOptionsT::DefaultDeserializer:
        DeserializeMessage<SerializationOptionsT::Serialized, SerializationOptionsT::Serialized>,
{
    fn new() -> Self {
        let mut settings = SchemaSettings::default();
        settings.definitions_path = "#/schemas/".to_string();

        let mut registry = Self {
            schema_generator: SchemaGenerator::new(settings),
            messages: HashMap::new(),
        };
        registry.register_serialize::<SerializationOptionsT::Serialized, SerializationOptionsT::DefaultSerializer>();
        registry.register_deserialize::<SerializationOptionsT::Serialized, SerializationOptionsT::DefaultDeserializer>();
        registry.register_fork_clone::<SerializationOptionsT::Serialized, DefaultImpl>();
        registry
    }
}
impl<SerializationOptionsT> MessageRegistry<SerializationOptionsT>
where
    SerializationOptionsT: SerializationOptions,
    SerializationOptionsT::Serialized: Send + Sync + 'static,
{
    fn get<T>(&self) -> Option<&MessageRegistration<SerializationOptionsT>>
    where
        T: Any,
    {
        self.messages.get(&TypeInfo::of::<T>())
    }

    pub(super) fn from_cel_value(
        &self,
        target_type: &TypeInfo,
        builder: &mut Builder,
        output: Output<cel_interpreter::Value>,
    ) -> Result<DynOutput, DiagramErrorCode> {
        if let Some(reg) = self.messages.get(target_type) {
            reg.operations.from_cel_value(builder, output)
        } else {
            Err(TransformError::NotSupported.into())
        }
    }

    fn register_from_cel_value<T, DeserializerT>(&mut self) -> bool
    where
        T: Send + Sync + 'static + Any,
        DeserializerT: DeserializeMessage<T, SerializationOptionsT::Serialized>,
    {
        let from_cel_value_impl = if let Some(f) = DeserializerT::from_cel_value_fn() {
            f
        } else {
            return false;
        };

        let reg = self
            .messages
            .entry(TypeInfo::of::<T>())
            .or_insert(MessageRegistration::new::<T>());
        let ops = &mut reg.operations;

        ops.from_cel_value_impl = Some(from_cel_value_impl);

        true
    }

    pub(super) fn deserialize(
        &self,
        target_type: &TypeInfo,
        builder: &mut Builder,
        output: Output<SerializationOptionsT::Serialized>,
    ) -> Result<DynOutput, DiagramErrorCode> {
        if let Some(reg) = self.messages.get(target_type) {
            reg.operations.deserialize(builder, output)
        } else {
            Err(DiagramErrorCode::NotSerializable)
        }
    }

    /// Register a deserialize function if not already registered, returns true if the new
    /// function is registered.
    pub(super) fn register_deserialize<T, DeserializerT>(&mut self) -> bool
    where
        T: Send + Sync + 'static + Any,
        DeserializerT: DeserializeMessage<T, SerializationOptionsT::Serialized>,
    {
        let deserialize_impl = if let Some(f) = DeserializerT::deserialize_fn() {
            f
        } else {
            return false;
        };

        let reg = self
            .messages
            .entry(TypeInfo::of::<T>())
            .or_insert(MessageRegistration::new::<T>());
        let ops = &mut reg.operations;

        debug!(
            "register deserialize for type: {}",
            std::any::type_name::<T>(),
        );
        ops.deserialize_impl = Some(deserialize_impl);
        self.register_from_cel_value::<T, DeserializerT>();

        // reg.schema = Deserializer::json_schema(&mut self.schema_generator);

        true
    }

    pub(super) fn to_cel_value(
        &self,
        builder: &mut Builder,
        output: DynOutput,
    ) -> Result<Output<cel_interpreter::Value>, DiagramErrorCode> {
        if output.type_info == TypeInfo::of::<cel_interpreter::Value>() {
            output.into_output()
        } else if let Some(reg) = self.messages.get(&output.type_info) {
            reg.operations.to_cel_value(builder, output)
        } else {
            Err(TransformError::NotSupported.into())
        }
    }

    fn register_to_cel_value<T, SerializerT>(&mut self) -> bool
    where
        T: Send + Sync + 'static + Any,
        SerializerT: SerializeMessage<T, SerializationOptionsT::Serialized>,
    {
        let to_cel_value_impl = if let Some(f) = SerializerT::to_cel_value_fn() {
            f
        } else {
            return false;
        };

        let reg = self
            .messages
            .entry(TypeInfo::of::<T>())
            .or_insert(MessageRegistration::new::<T>());
        let ops = &mut reg.operations;

        ops.to_cel_value_impl = Some(to_cel_value_impl);

        true
    }

    pub(super) fn serialize(
        &self,
        builder: &mut Builder,
        output: DynOutput,
    ) -> Result<Output<SerializationOptionsT::Serialized>, DiagramErrorCode> {
        debug!("serialize {:?}", output);
        if output.type_info == TypeInfo::of::<SerializationOptionsT::Serialized>() {
            output.into_output()
        } else if output.type_info == TypeInfo::of::<cel_interpreter::Value>() {
            Ok(
                SerializationOptionsT::DefaultSerializer::serialize_cel_output(
                    builder,
                    output.into_output()?,
                ),
            )
        } else if let Some(reg) = self.messages.get(&output.type_info) {
            reg.operations.serialize(builder, output)
        } else {
            Err(DiagramErrorCode::NotSerializable)
        }
    }

    /// Register a serialize function if not already registered, returns true if the new
    /// function is registered.
    pub(super) fn register_serialize<T, SerializerT>(&mut self) -> bool
    where
        T: Send + Sync + 'static + Any,
        SerializerT: SerializeMessage<T, SerializationOptionsT::Serialized>,
    {
        let serialize_impl = if let Some(f) = SerializerT::serialize_fn() {
            f
        } else {
            return false;
        };

        let reg = self
            .messages
            .entry(TypeInfo::of::<T>())
            .or_insert(MessageRegistration::new::<T>());
        let ops = &mut reg.operations;

        debug!(
            "register serialize for type: {}",
            std::any::type_name::<T>(),
        );
        ops.serialize_impl = Some(serialize_impl);
        self.register_to_cel_value::<T, SerializerT>();

        true
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
        DefaultImplMarker<(T, Serializer)>: DynUnzip<SerializationOptionsT>,
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
        split_op: &'b SplitOp,
    ) -> Result<DynSplitOutputs, DiagramErrorCode> {
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
        T: Send + Sync + 'static + Any + Splittable,
        F: DynSplit<T, S>,
        S: SerializeMessage<T::Item, SerializationOptionsT::Serialized>,
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
            Err(DiagramErrorCode::CannotListen)
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
        v: &HashMap<TypeInfo, MessageRegistration<SerializationOptionsT>>,
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

impl<SerializationOptionsT> Default for DiagramElementRegistry<SerializationOptionsT>
where
    SerializationOptionsT: SerializationOptions,
    SerializationOptionsT::DefaultSerializer:
        SerializeMessage<SerializationOptionsT::Serialized, SerializationOptionsT::Serialized>,
    SerializationOptionsT::DefaultDeserializer:
        DeserializeMessage<SerializationOptionsT::Serialized, SerializationOptionsT::Serialized>,
{
    fn default() -> Self {
        DiagramElementRegistry {
            nodes: Default::default(),
            sections: Default::default(),
            messages: MessageRegistry::new(),
        }
    }
}

impl<SerializationOptionsT> DiagramElementRegistry<SerializationOptionsT>
where
    SerializationOptionsT: SerializationOptions,
    SerializationOptionsT::DefaultSerializer:
        SerializeMessage<SerializationOptionsT::Serialized, SerializationOptionsT::Serialized>,
    SerializationOptionsT::DefaultDeserializer:
        DeserializeMessage<SerializationOptionsT::Serialized, SerializationOptionsT::Serialized>,
{
    pub fn new() -> Self {
        Self::default()
    }
}

impl<SerializationOptionsT> DiagramElementRegistry<SerializationOptionsT>
where
    SerializationOptionsT: SerializationOptions,
{
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
    ) -> NodeRegistrationBuilder<SerializationOptionsT, Request, Response, Streams>
    where
        Config: JsonSchema + DeserializeOwned,
        Request: Send + Sync + 'static + DeserializeOwned,
        Response: Send + Sync + 'static + Serialize + Clone,
        SerializationOptionsT::DefaultDeserializer:
            DeserializeMessage<Request, SerializationOptionsT::Serialized>,
        SerializationOptionsT::DefaultSerializer:
            SerializeMessage<Response, SerializationOptionsT::Serialized>,
    {
        self.opt_out().register_node_builder(options, builder)
    }

    /// Register a section builder with the specified common operations.
    ///
    /// # Arguments
    ///
    /// * `id` - Id of the builder, this must be unique.
    /// * `name` - Friendly name for the builder, this is only used for display purposes.
    /// * `f` - The section builder to register.
    pub fn register_section_builder<SectionBuilder, SectionT, Config>(
        &mut self,
        options: SectionBuilderOptions,
        section_builder: SectionBuilder,
    ) where
        SectionBuilder: IntoSectionRegistration<SerializationOptionsT, SectionT, Config>,
        SectionT: Section<SerializationOptionsT>,
    {
        let reg = section_builder.into_section_registration(
            options.name.unwrap_or_else(|| options.id.clone()),
            &mut self.messages.schema_generator,
        );
        self.sections.insert(options.id, reg);
        SectionT::on_register(self);
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
    /// use bevy_impulse::{NodeBuilderOptions, DiagramElementRegistry};
    ///
    /// struct NonSerializable {
    ///     data: String
    /// }
    ///
    /// let mut registry = DiagramElementRegistry::new();
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
    ) -> CommonOperations<
        SerializationOptionsT,
        SerializationOptionsT::DefaultDeserializer,
        SerializationOptionsT::DefaultSerializer,
        DefaultImpl,
    > {
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

    pub fn get_section_registration<Q>(
        &self,
        id: &Q,
    ) -> Result<&SectionRegistration<SerializationOptionsT>, DiagramErrorCode>
    where
        Q: Borrow<str> + ?Sized,
    {
        self.sections
            .get(id.borrow())
            .ok_or_else(|| DiagramErrorCode::BuilderNotFound(id.borrow().to_string()))
    }

    pub fn get_message_registration<T>(&self) -> Option<&MessageRegistration<SerializationOptionsT>>
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

#[non_exhaustive]
pub struct SectionBuilderOptions {
    pub id: BuilderId,
    pub name: Option<String>,
}

impl SectionBuilderOptions {
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
    impl<SerializationOptionsT> MessageOperation<SerializationOptionsT>
    where
        SerializationOptionsT: SerializationOptions,
    {
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
        let mut registry = JsonDiagramRegistry::new();
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
        let mut registry = JsonDiagramRegistry::new();
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
        let mut registry = JsonDiagramRegistry::new();
        let tuple_resp = |_: ()| -> (i64,) { (1,) };
        registry
            .opt_out()
            .no_response_cloning()
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
        let mut registry = JsonDiagramRegistry::new();
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
        let mut registry = JsonDiagramRegistry::new();

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

        let mut registry = JsonDiagramRegistry::new();
        registry
            .opt_out()
            .no_request_deserializing()
            .no_response_cloning()
            .register_node_builder(
                NodeBuilderOptions::new("opaque_request_map").with_name("Test Name"),
                move |builder, _config: ()| builder.create_map_block(opaque_request_map),
            );
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
            .no_response_serializing()
            .no_response_cloning()
            .register_node_builder(
                NodeBuilderOptions::new("opaque_response_map").with_name("Test Name"),
                move |builder: &mut Builder, _config: ()| {
                    builder.create_map_block(opaque_response_map)
                },
            );
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
            .no_request_deserializing()
            .no_response_serializing()
            .no_response_cloning()
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
        let mut registry = JsonDiagramRegistry::new();

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
        let mut reg = JsonDiagramRegistry::new();

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
            .no_request_deserializing()
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
