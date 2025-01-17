use std::{
    any::{type_name, Any},
    borrow::Borrow,
    cell::RefCell,
    collections::HashMap,
    fmt::Debug,
    marker::PhantomData,
};

use crate::{
    unknown_diagram_error, Buffer, Builder, InputSlot, IterBufferable, Node, Output, StreamPack,
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

use crate::SerializeMessage;

use super::{
    fork_clone::DynForkClone,
    fork_result::DynForkResult,
    impls::{DefaultImpl, DefaultImplMarker, NotSupported},
    join::register_join_impl,
    register_serialize,
    type_info::TypeInfo,
    unzip::DynUnzip,
    BuilderId, DefaultDeserializer, DefaultSerializer, DeserializeMessage, DiagramErrorCode,
    DynSplit, DynSplitOutputs, DynType, OpaqueMessageDeserializer, OpaqueMessageSerializer,
    SplitOp,
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

#[derive(Debug)]
/// A type erased [`crate::Output`]
pub struct DynOutput {
    scope: Entity,
    target: Entity,
    pub(super) type_info: TypeInfo,
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

    pub(super) fn scope(&self) -> Entity {
        self.scope
    }

    pub(super) fn id(&self) -> Entity {
        self.target
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
    /// type name of the request
    pub(super) request: &'static str,
    /// type name of the response
    pub(super) response: &'static str,
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

/// A container that can store any message.
///
/// TODO(koonpeng): This is currently only used in buffers, theorically, this can be used
/// to avoid serialization in `join`, but there are some blockers which stops it from being used.
///
/// The problem is that `join` produces a `SmallVec`. "serialize and join" works because the
/// resulting `SmallVec<[serde_json::Value, 4]>` is converted to a `serde_json::Value`. This
/// allows other operations like fork_clone, split, serialize etc to work. With "box and join",
/// we get `SmallVec<[BoxedMessage; 4]>`, unlike `serde_json::Value`, whether a `BoxedMessage`
/// can be cloned, split etc needs to depend on the inner type, which is lost when we box it.
///
/// A way to support "box and join" is to embed the impls into each `DynOutput`,
/// but we can't because `DynOutput` has to be convertible from `Output`. Though it is probably
/// possible with a trait bound.
pub(super) struct BoxedMessage(pub(super) Box<dyn Send + Sync + 'static + Any>);

type CreateNodeFn =
    RefCell<Box<dyn FnMut(&mut Builder, serde_json::Value) -> Result<DynNode, DiagramErrorCode>>>;
type BoxedFn =
    Box<dyn Fn(&mut Builder, DynOutput) -> Result<Output<BoxedMessage>, DiagramErrorCode>>;
type UnboxFn = Box<dyn Fn(&mut Builder, DynOutput) -> Result<DynOutput, DiagramErrorCode>>;
type DeserializeFn =
    Box<dyn Fn(&mut Builder, Output<serde_json::Value>) -> Result<DynOutput, DiagramErrorCode>>;
type SerializeFn =
    Box<dyn Fn(&mut Builder, DynOutput) -> Result<Output<serde_json::Value>, DiagramErrorCode>>;
type ForkCloneFn =
    Box<dyn Fn(&mut Builder, DynOutput, usize) -> Result<Vec<DynOutput>, DiagramErrorCode>>;
type ForkResultFn =
    Box<dyn Fn(&mut Builder, DynOutput) -> Result<(DynOutput, DynOutput), DiagramErrorCode>>;
type SplitFn = Box<
    dyn for<'a> Fn(
        &mut Builder,
        DynOutput,
        &'a SplitOp,
    ) -> Result<DynSplitOutputs<'a>, DiagramErrorCode>,
>;
type JoinFn = Box<dyn Fn(&mut Builder, Vec<DynOutput>) -> Result<DynOutput, DiagramErrorCode>>;
type BufferAccessFn = Box<
    dyn Fn(
        &mut Builder,
        DynOutput,
        Vec<Buffer<BoxedMessage>>,
    ) -> Result<DynOutput, DiagramErrorCode>,
>;

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
        self,
        options: NodeBuilderOptions,
        mut f: impl FnMut(&mut Builder, Config) -> Node<Request, Response, Streams> + 'static,
    ) -> NodeRegistrationBuilder<'a, Request, Response, Streams>
    where
        Config: JsonSchema + DeserializeOwned,
        Request: Send + Sync + 'static,
        Response: Send + Sync + 'static,
        Streams: StreamPack,
        DeserializeImpl: DeserializeMessage<Request>,
        SerializeImpl: SerializeMessage<Response>,
        Cloneable: DynForkClone<Response>,
    {
        self.registry
            .messages
            .register_deserialize::<Request, DeserializeImpl>();
        self.registry
            .messages
            .register_serialize::<Response, SerializeImpl>();
        self.registry
            .messages
            .register_fork_clone::<Response, Cloneable>();

        let registration = NodeRegistration {
            id: options.id.clone(),
            name: options.name.unwrap_or(options.id.clone()),
            request: type_name::<Request>(),
            response: type_name::<Response>(),
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

        NodeRegistrationBuilder::<Request, Response, Streams>::new(&mut self.registry.messages)
    }

    /// Register a message with the specified common operations.
    pub fn register_message<Message>(self) -> MessageRegistrationBuilder<'a, Message>
    where
        Message: Send + Sync + 'static,
        DeserializeImpl: DeserializeMessage<Message>,
        SerializeImpl: SerializeMessage<Message> + SerializeMessage<Vec<Message>>,
        Cloneable: DynForkClone<Message>,
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
        register_join_impl::<Message, SerializeImpl>(&mut self.registry.messages);

        MessageRegistrationBuilder::<Message>::new(&mut self.registry.messages)
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

    /// Mark the node as having a unzippable response. This is required in order for the node
    /// to be able to be connected to a "Unzip" operation.
    pub fn with_unzip(&mut self) -> &mut Self
    where
        DefaultImplMarker<(Message, DefaultSerializer)>: DynUnzip,
    {
        self.data.register_unzip::<Message, DefaultSerializer>();
        self
    }

    /// Mark the node as having an unzippable response whose elements are not serializable.
    pub fn with_unzip_minimal(&mut self) -> &mut Self
    where
        DefaultImplMarker<(Message, NotSupported)>: DynUnzip,
    {
        self.data.register_unzip::<Message, NotSupported>();
        self
    }

    /// Mark the node as having a [`Result<_, _>`] response. This is required in order for the node
    /// to be able to be connected to a "Fork Result" operation.
    pub fn with_fork_result(&mut self) -> &mut Self
    where
        DefaultImplMarker<(Message, OpaqueMessageSerializer)>: DynForkResult,
    {
        self.data
            .register_fork_result(DefaultImplMarker::<(Message, OpaqueMessageSerializer)>::new());
        self
    }

    /// Mark the node as having a splittable response. This is required in order
    /// for the node to be able to be connected to a "Split" operation.
    pub fn with_split(&mut self) -> &mut Self
    where
        DefaultImpl: DynSplit<Message, DefaultSerializer>,
    {
        self.data
            .register_split::<Message, DefaultImpl, DefaultSerializer>();
        self
    }

    /// Mark the node as having a splittable response but the items from the split
    /// are unserializable.
    pub fn with_split_minimal(&mut self) -> &mut Self
    where
        DefaultImpl: DynSplit<Message, NotSupported>,
    {
        self.data
            .register_split::<Message, DefaultImpl, NotSupported>();
        self
    }
}

pub struct NodeRegistrationBuilder<'a, Request, Response, Streams> {
    registry: &'a mut MessageRegistry,
    _ignore: PhantomData<(Request, Response, Streams)>,
}

impl<'a, Request, Response, Streams> NodeRegistrationBuilder<'a, Request, Response, Streams>
where
    Request: Any,
    Response: Send + Sync + 'static + Any,
{
    fn new(registry: &'a mut MessageRegistry) -> Self {
        Self {
            registry,
            _ignore: Default::default(),
        }
    }

    /// Mark the node as having a unzippable response. This is required in order for the node
    /// to be able to be connected to a "Unzip" operation.
    pub fn with_unzip(&mut self) -> &mut Self
    where
        DefaultImplMarker<(Response, DefaultSerializer)>: DynUnzip,
    {
        MessageRegistrationBuilder::new(self.registry).with_unzip();
        self
    }

    /// Mark the node as having an unzippable response whose elements are not serializable.
    pub fn with_unzip_unserializable(&mut self) -> &mut Self
    where
        DefaultImplMarker<(Response, NotSupported)>: DynUnzip,
    {
        MessageRegistrationBuilder::new(self.registry).with_unzip_minimal();
        self
    }

    /// Mark the node as having a [`Result<_, _>`] response. This is required in order for the node
    /// to be able to be connected to a "Fork Result" operation.
    pub fn with_fork_result(&mut self) -> &mut Self
    where
        DefaultImplMarker<(Response, OpaqueMessageSerializer)>: DynForkResult,
    {
        MessageRegistrationBuilder::new(self.registry).with_fork_result();
        self
    }

    /// Mark the node as having a splittable response. This is required in order
    /// for the node to be able to be connected to a "Split" operation.
    pub fn with_split(&mut self) -> &mut Self
    where
        DefaultImpl: DynSplit<Response, DefaultSerializer>,
    {
        MessageRegistrationBuilder::new(self.registry).with_split();
        self
    }

    /// Mark the node as having a splittable response but the items from the split
    /// are unserializable.
    pub fn with_split_unserializable(&mut self) -> &mut Self
    where
        DefaultImpl: DynSplit<Response, NotSupported>,
    {
        MessageRegistrationBuilder::new(self.registry).with_split_minimal();
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
    pub(super) boxed_impl: BoxedFn,
    pub(super) unbox_impl: UnboxFn,
    pub(super) deserialize_impl: Option<DeserializeFn>,
    pub(super) serialize_impl: Option<SerializeFn>,
    pub(super) fork_clone_impl: Option<ForkCloneFn>,
    pub(super) unzip_impl: Option<Box<dyn DynUnzip>>,
    pub(super) fork_result_impl: Option<ForkResultFn>,
    pub(super) split_impl: Option<SplitFn>,
    pub(super) join_impl: Option<JoinFn>,
    pub(super) buffer_access_impl: BufferAccessFn,
}

impl MessageOperation {
    fn new<T>() -> Self
    where
        T: Send + Sync + 'static + Any,
    {
        Self {
            boxed_impl: Box::new(|builder, output| {
                Ok(output
                    .into_output::<T>()?
                    .chain(builder)
                    .map_block(|o| BoxedMessage(Box::new(o)))
                    .output())
            }),
            unbox_impl: Box::new(|builder, output| {
                Ok(output
                    .into_output::<BoxedMessage>()?
                    .chain(builder)
                    .map_block(|boxed| -> Result<T, DiagramErrorCode> {
                        let downcasted = boxed
                            .0
                            .downcast::<T>()
                            .map_err(|_| DiagramErrorCode::CannotBoxOrUnbox)?;
                        Ok(*downcasted)
                    })
                    .cancel_on_err()
                    .output()
                    .into())
            }),
            deserialize_impl: None,
            serialize_impl: None,
            fork_clone_impl: None,
            unzip_impl: None,
            fork_result_impl: None,
            split_impl: None,
            join_impl: None,
            buffer_access_impl: Box::new(|builder, output, buffers| {
                let buffer_output = buffers.join_vec::<4>(builder).output();
                Ok(output
                    .into_output::<T>()?
                    .chain(builder)
                    .with_access(buffer_output)
                    .output()
                    .into())
            }),
        }
    }

    pub(super) fn boxed(
        &self,
        builder: &mut Builder,
        output: DynOutput,
    ) -> Result<Output<BoxedMessage>, DiagramErrorCode> {
        (self.boxed_impl)(builder, output)
    }

    pub(super) fn unbox(
        &self,
        builder: &mut Builder,
        output: DynOutput,
    ) -> Result<DynOutput, DiagramErrorCode> {
        (self.unbox_impl)(builder, output)
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
        split_op: &'a SplitOp,
    ) -> Result<DynSplitOutputs<'a>, DiagramErrorCode> {
        let f = self
            .split_impl
            .as_ref()
            .ok_or(DiagramErrorCode::NotSplittable)?;
        f(builder, output, split_op)
    }

    pub(super) fn join<OutputIter>(
        &self,
        builder: &mut Builder,
        outputs: OutputIter,
    ) -> Result<DynOutput, DiagramErrorCode>
    where
        OutputIter: IntoIterator<Item = DynOutput>,
    {
        let f = self
            .join_impl
            .as_ref()
            .ok_or(DiagramErrorCode::NotJoinable)?;
        f(builder, outputs.into_iter().collect())
    }

    pub(super) fn with_buffer_access<BufferIter>(
        &self,
        builder: &mut Builder,
        output: DynOutput,
        buffers: BufferIter,
    ) -> Result<DynOutput, DiagramErrorCode>
    where
        BufferIter: IntoIterator<Item = Buffer<BoxedMessage>>,
    {
        let f = &self.buffer_access_impl;
        f(builder, output, buffers.into_iter().collect())
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

    /// Box the output if it is not boxed, else do nothing.
    pub(super) fn boxed(
        &self,
        builder: &mut Builder,
        output: DynOutput,
    ) -> Result<Output<BoxedMessage>, DiagramErrorCode> {
        if output.type_info == TypeInfo::of::<BoxedMessage>() {
            // already boxed
            return Ok(output.into_output()?);
        }
        let reg = self
            .messages
            .get(&output.type_info)
            .ok_or(DiagramErrorCode::CannotBoxOrUnbox)?;
        reg.operations.boxed(builder, output)
    }

    /// Unbox the output if it is boxed, else do nothing.
    pub(super) fn unbox(
        &self,
        builder: &mut Builder,
        output: DynOutput,
        target_type_id: &TypeInfo,
    ) -> Result<DynOutput, DiagramErrorCode> {
        if output.type_info != TypeInfo::of::<BoxedMessage>() {
            // already unboxed
            return Ok(output);
        }
        let reg = self
            .messages
            // inner_type_id should always be filled for boxed outputs
            // TODO(koonpeng): we will be able to have a stronger guaranteed when we move to
            // a custom `TypeInfo` container.
            .get(target_type_id)
            .ok_or(DiagramErrorCode::CannotBoxOrUnbox)?;
        reg.operations.unbox(builder, output)
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
        ops.deserialize_impl = Some(Box::new(|builder, output| {
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
        }));

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

        ops.fork_clone_impl = Some(Box::new(|builder, output, amount| {
            F::dyn_fork_clone(builder, output, amount)
        }));

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
        split_op: &'b SplitOp,
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

    pub(super) fn join<OutputIter>(
        &self,
        builder: &mut Builder,
        outputs: OutputIter,
    ) -> Result<DynOutput, DiagramErrorCode>
    where
        OutputIter: IntoIterator<Item = DynOutput>,
    {
        let mut i = outputs.into_iter().peekable();
        let output_type_id = if let Some(o) = i.peek() {
            Some(o.type_info.clone())
        } else {
            None
        };

        if let Some(output_type_id) = output_type_id {
            if let Some(reg) = self.messages.get(&output_type_id) {
                reg.operations.join(builder, i)
            } else {
                Err(DiagramErrorCode::NotJoinable)
            }
        } else {
            Err(DiagramErrorCode::NotJoinable)
        }
    }

    /// Register a join function if not already registered, returns true if the new
    /// function is registered.
    pub(super) fn register_join<T>(&mut self, f: JoinFn) -> bool
    where
        T: Send + Sync + 'static + Any,
    {
        let ops = &mut self
            .messages
            .entry(TypeInfo::of::<T>())
            .or_insert(MessageRegistration::new::<T>())
            .operations;
        if ops.join_impl.is_some() {
            return false;
        }

        ops.join_impl = Some(f);

        true
    }

    pub(super) fn with_buffer_access<BufferIter>(
        &self,
        builder: &mut Builder,
        output: DynOutput,
        buffers: BufferIter,
    ) -> Result<DynOutput, DiagramErrorCode>
    where
        BufferIter: IntoIterator<Item = Buffer<BoxedMessage>>,
    {
        if let Some(reg) = self.messages.get(&output.type_info) {
            reg.operations.with_buffer_access(builder, output, buffers)
        } else {
            // `buffer_access_impl` is auto registered in the constructor.
            Err(unknown_diagram_error!())
        }
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
        Request: Send + Sync + 'static + DynType + DeserializeOwned,
        Response: Send + Sync + 'static + DynType + Serialize + Clone,
    {
        self.opt_out().register_node_builder(options, builder)
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
        assert!(ops.joinable());
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
