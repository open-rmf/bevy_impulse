use std::{
    any::{type_name, Any, TypeId},
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
use serde::{
    de::DeserializeOwned,
    ser::{SerializeMap, SerializeSeq, SerializeStruct},
    Serialize,
};
use tracing::debug;

use crate::SerializeMessage;

use super::{
    fork_clone::DynForkClone,
    fork_result::DynForkResult,
    impls::{DefaultImpl, NotSupported},
    unzip::DynUnzip,
    BuilderId, DefaultDeserializer, DefaultSerializer, DeserializeMessage, DiagramError, DynSplit,
    DynSplitOutputs, DynType, OpaqueMessageDeserializer, OpaqueMessageSerializer, SplitOp,
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
    /// type name of the request
    pub(super) request: &'static str,
    /// type name of the response
    pub(super) response: &'static str,
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

#[derive(Serialize)]
pub struct NodeRegistration {
    pub(super) metadata: NodeMetadata,

    /// Creates an instance of the registered node.
    #[serde(skip)]
    create_node_impl: CreateNodeFn,
}

impl NodeRegistration {
    fn new(metadata: NodeMetadata, create_node_impl: CreateNodeFn) -> NodeRegistration {
        NodeRegistration {
            metadata,
            create_node_impl,
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
}

type CreateNodeFn =
    RefCell<Box<dyn FnMut(&mut Builder, serde_json::Value) -> Result<DynNode, DiagramError>>>;
type DeserializeFn =
    Box<dyn Fn(&mut Builder, Output<serde_json::Value>) -> Result<DynOutput, DiagramError>>;
type SerializeFn =
    Box<dyn Fn(&mut Builder, DynOutput) -> Result<Output<serde_json::Value>, DiagramError>>;
type ForkCloneFn =
    Box<dyn Fn(&mut Builder, DynOutput, usize) -> Result<Vec<DynOutput>, DiagramError>>;
type UnzipFn = Box<dyn Fn(&mut Builder, DynOutput) -> Result<Vec<DynOutput>, DiagramError>>;
type ForkResultFn =
    Box<dyn Fn(&mut Builder, DynOutput) -> Result<(DynOutput, DynOutput), DiagramError>>;
type SplitFn = Box<
    dyn for<'a> Fn(
        &mut Builder,
        DynOutput,
        &'a SplitOp,
    ) -> Result<DynSplitOutputs<'a>, DiagramError>,
>;
type JoinFn = Box<dyn Fn(&mut Builder, Vec<DynOutput>) -> Result<DynOutput, DiagramError>>;

pub struct CommonOperations<'a, Deserialize, Serialize, Cloneable> {
    registry: &'a mut Registry,
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
        self.registry
            .messages
            .register_deserialize::<Request, DeserializeImpl>();
        self.registry
            .messages
            .register_serialize::<Response, SerializeImpl>();
        self.registry
            .messages
            .register_fork_clone::<Response, Cloneable>();

        let registration = NodeRegistration::new(
            NodeMetadata {
                id: options.id.clone(),
                name: options.name.unwrap_or(options.id.clone()),
                request: type_name::<Request>(),
                response: type_name::<Response>(),
                config_schema: self
                    .registry
                    .messages
                    .schema_generator
                    .subschema_for::<Config>(),
            },
            RefCell::new(Box::new(move |builder, config| {
                let config = serde_json::from_value(config)?;
                let n = f(builder, config);
                Ok(DynNode::new(n.output, n.input))
            })),
        );
        self.registry.nodes.insert(options.id.clone(), registration);

        RegistrationBuilder::<Request, Response, Streams> {
            data: &mut self.registry.messages,
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
    data: &'a mut MessageRegistry,
    _ignore: PhantomData<(Request, Response, Streams)>,
}

impl<'a, Request, Response, Streams> RegistrationBuilder<'a, Request, Response, Streams>
where
    Request: Any,
    Response: Any,
{
    /// Mark the node as having a unzippable response. This is required in order for the node
    /// to be able to be connected to a "Unzip" operation.
    pub fn with_unzip(self) -> Self
    where
        DefaultImpl: DynUnzip<Response, DefaultSerializer>,
    {
        self.data
            .register_unzip::<Response, DefaultImpl, DefaultSerializer>();
        self
    }

    /// Mark the node as having an unzippable response whose elements are not serializable.
    pub fn with_unzip_unserializable(self) -> Self
    where
        DefaultImpl: DynUnzip<Response, NotSupported>,
    {
        self.data
            .register_unzip::<Response, DefaultImpl, NotSupported>();
        self
    }

    /// Mark the node as having a [`Result<_, _>`] response. This is required in order for the node
    /// to be able to be connected to a "Fork Result" operation.
    pub fn with_fork_result(self) -> Self
    where
        DefaultImpl: DynForkResult<Response>,
    {
        self.data.register_fork_result::<Response, DefaultImpl>();
        self
    }

    /// Mark the node as having a splittable response. This is required in order
    /// for the node to be able to be connected to a "Split" operation.
    pub fn with_split(self) -> Self
    where
        DefaultImpl: DynSplit<Response, DefaultSerializer>,
    {
        self.data
            .register_split::<Response, DefaultImpl, DefaultSerializer>();
        self
    }

    /// Mark the node as having a splittable response but the items from the split
    /// are unserializable.
    pub fn with_split_unserializable(self) -> Self
    where
        DefaultImpl: DynSplit<Response, NotSupported>,
    {
        self.data
            .register_split::<Response, DefaultImpl, NotSupported>();
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
pub struct Registry {
    pub(super) nodes: HashMap<BuilderId, NodeRegistration>,
    #[serde(flatten)]
    pub(super) messages: MessageRegistry,
}

struct UnzipImpl {
    slots: usize,
    f: UnzipFn,
}

#[derive(Default)]
pub(super) struct MessageOperation {
    deserialize_impl: Option<DeserializeFn>,
    serialize_impl: Option<SerializeFn>,
    fork_clone_impl: Option<ForkCloneFn>,
    unzip_impl: Option<UnzipImpl>,
    fork_result_impl: Option<ForkResultFn>,
    split_impl: Option<SplitFn>,
    join_impl: Option<JoinFn>,
}

impl MessageOperation {
    fn new() -> Self {
        Self::default()
    }

    /// Try to deserialize `output` into `input_type`. If `output` is not `serde_json::Value`, this does nothing.
    pub(super) fn deserialize(
        &self,
        builder: &mut Builder,
        output: DynOutput,
    ) -> Result<DynOutput, DiagramError> {
        let f = self
            .deserialize_impl
            .as_ref()
            .ok_or(DiagramError::NotSerializable)?;
        f(builder, output.into_output()?)
    }

    #[cfg(test)]
    pub(super) fn deserializable(&self) -> bool {
        self.deserialize_impl.is_some()
    }

    #[cfg(test)]
    pub(super) fn serializable(&self) -> bool {
        self.serialize_impl.is_some()
    }

    pub(super) fn serialize(
        &self,
        builder: &mut Builder,
        output: DynOutput,
    ) -> Result<Output<serde_json::Value>, DiagramError> {
        let f = self
            .serialize_impl
            .as_ref()
            .ok_or(DiagramError::NotSerializable)?;
        f(builder, output)
    }

    #[cfg(test)]
    pub(super) fn cloneable(&self) -> bool {
        self.fork_clone_impl.is_some()
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

    #[cfg(test)]
    pub(super) fn unzippable(&self) -> bool {
        self.unzip_impl.is_some()
    }

    pub(super) fn unzip(
        &self,
        builder: &mut Builder,
        output: DynOutput,
    ) -> Result<Vec<DynOutput>, DiagramError> {
        let f = &self
            .unzip_impl
            .as_ref()
            .ok_or(DiagramError::NotUnzippable)?
            .f;
        f(builder, output)
    }

    pub(super) fn unzip_slots(&self) -> usize {
        if let Some(unzip_impl) = &self.unzip_impl {
            unzip_impl.slots
        } else {
            0
        }
    }

    #[cfg(test)]
    pub(super) fn can_fork_result(&self) -> bool {
        self.fork_result_impl.is_some()
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

    #[cfg(test)]
    pub(super) fn splittable(&self) -> bool {
        self.split_impl.is_some()
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

    #[cfg(test)]
    pub(super) fn joinable(&self) -> bool {
        self.join_impl.is_some()
    }

    pub(super) fn join<OutputIter>(
        &self,
        builder: &mut Builder,
        outputs: OutputIter,
    ) -> Result<DynOutput, DiagramError>
    where
        OutputIter: IntoIterator<Item = DynOutput>,
    {
        let f = self.join_impl.as_ref().ok_or(DiagramError::NotJoinable)?;
        f(builder, outputs.into_iter().collect())
    }
}

impl Serialize for MessageOperation {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s = serializer.serialize_seq(None)?;
        if self.deserialize_impl.is_some() {
            s.serialize_element("deserialize")?;
        }
        if self.serialize_impl.is_some() {
            s.serialize_element("serialize")?;
        }
        if self.fork_clone_impl.is_some() {
            s.serialize_element("fork_clone")?;
        }
        if self.unzip_impl.is_some() {
            s.serialize_element("unzip")?;
        }
        if self.fork_result_impl.is_some() {
            s.serialize_element("fork_result")?;
        }
        if self.split_impl.is_some() {
            s.serialize_element("split")?;
        }
        if self.join_impl.is_some() {
            s.serialize_element("join")?;
        }
        s.end()
    }
}

struct MessageRegistration {
    type_name: &'static str,
    schema: Option<schemars::schema::Schema>,
    operations: MessageOperation,
}

impl MessageRegistration {
    fn new<T>() -> Self {
        Self {
            type_name: type_name::<T>(),
            schema: None,
            operations: MessageOperation::new(),
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
        s.serialize_field("unzip_slots", &self.operations.unzip_slots())?;
        s.end()
    }
}

#[derive(Serialize)]
pub struct MessageRegistry {
    #[serde(serialize_with = "MessageRegistry::serialize_messages")]
    messages: HashMap<TypeId, MessageRegistration>,

    #[serde(
        rename = "schemas",
        serialize_with = "MessageRegistry::serialize_schemas"
    )]
    schema_generator: SchemaGenerator,
}

impl MessageRegistry {
    #[cfg(test)]
    fn get<T>(&self) -> Option<&MessageRegistration>
    where
        T: Any,
    {
        self.messages.get(&TypeId::of::<T>())
    }

    pub(super) fn deserialize(
        &self,
        target_type: &TypeId,
        builder: &mut Builder,
        output: DynOutput,
    ) -> Result<DynOutput, DiagramError> {
        if output.type_id != TypeId::of::<serde_json::Value>() || &output.type_id == target_type {
            Ok(output)
        } else if let Some(reg) = self.messages.get(target_type) {
            reg.operations.deserialize(builder, output)
        } else {
            Err(DiagramError::NotSerializable)
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
            .entry(TypeId::of::<T>())
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
    ) -> Result<Output<serde_json::Value>, DiagramError> {
        if output.type_id == TypeId::of::<serde_json::Value>() {
            output.into_output()
        } else if let Some(reg) = self.messages.get(&output.type_id) {
            reg.operations.serialize(builder, output)
        } else {
            Err(DiagramError::NotSerializable)
        }
    }

    /// Register a serialize function if not already registered, returns true if the new
    /// function is registered.
    pub(super) fn register_serialize<T, Serializer>(&mut self) -> bool
    where
        T: Send + Sync + 'static + Any,
        Serializer: SerializeMessage<T>,
    {
        let reg = &mut self
            .messages
            .entry(TypeId::of::<T>())
            .or_insert(MessageRegistration::new::<T>());
        let ops = &mut reg.operations;
        if !Serializer::serializable() || ops.serialize_impl.is_some() {
            return false;
        }

        debug!(
            "register serialize for type: {}, with serializer: {}",
            std::any::type_name::<T>(),
            std::any::type_name::<Serializer>()
        );
        ops.serialize_impl = Some(Box::new(|builder, output| {
            debug!("serialize output: {:?}", output);
            let n = builder.create_map_block(|resp: T| Serializer::to_json(&resp));
            builder.connect(output.into_output()?, n.input);
            let serialized_output = n.output.chain(builder).cancel_on_err().output();
            debug!("serialized output: {:?}", serialized_output);
            Ok(serialized_output)
        }));

        reg.schema = Serializer::json_schema(&mut self.schema_generator);

        true
    }

    pub(super) fn fork_clone(
        &self,
        builder: &mut Builder,
        output: DynOutput,
        amount: usize,
    ) -> Result<Vec<DynOutput>, DiagramError> {
        if let Some(reg) = self.messages.get(&output.type_id) {
            reg.operations.fork_clone(builder, output, amount)
        } else {
            Err(DiagramError::NotCloneable)
        }
    }

    /// Register a fork_clone function if not already registered, returns true if the new
    /// function is registered.
    pub(super) fn register_fork_clone<T, F>(&mut self) -> bool
    where
        T: Any,
        F: DynForkClone<T>,
    {
        let ops = &mut self
            .messages
            .entry(TypeId::of::<T>())
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
    ) -> Result<Vec<DynOutput>, DiagramError> {
        if let Some(reg) = self.messages.get(&output.type_id) {
            reg.operations.unzip(builder, output)
        } else {
            Err(DiagramError::NotUnzippable)
        }
    }

    /// Register a unzip function if not already registered, returns true if the new
    /// function is registered.
    pub(super) fn register_unzip<T, F, S>(&mut self) -> bool
    where
        T: Any,
        F: DynUnzip<T, S>,
    {
        let ops = &mut self
            .messages
            .entry(TypeId::of::<T>())
            .or_insert(MessageRegistration::new::<T>())
            .operations;
        if ops.unzip_impl.is_some() {
            return false;
        }
        ops.unzip_impl = Some(UnzipImpl {
            slots: F::UNZIP_SLOTS,
            f: Box::new(|builder, output| F::dyn_unzip(builder, output)),
        });
        F::on_register(self);

        true
    }

    pub(super) fn fork_result(
        &self,
        builder: &mut Builder,
        output: DynOutput,
    ) -> Result<(DynOutput, DynOutput), DiagramError> {
        if let Some(reg) = self.messages.get(&output.type_id) {
            reg.operations.fork_result(builder, output)
        } else {
            Err(DiagramError::CannotForkResult)
        }
    }

    /// Register a fork_result function if not already registered, returns true if the new
    /// function is registered.
    pub(super) fn register_fork_result<T, F>(&mut self) -> bool
    where
        T: Any,
        F: DynForkResult<T>,
    {
        let ops = &mut self
            .messages
            .entry(TypeId::of::<T>())
            .or_insert(MessageRegistration::new::<T>())
            .operations;
        if ops.fork_result_impl.is_some() {
            return false;
        }

        ops.fork_result_impl = Some(Box::new(|builder, output| {
            F::dyn_fork_result(builder, output)
        }));

        true
    }

    pub(super) fn split<'b>(
        &self,
        builder: &mut Builder,
        output: DynOutput,
        split_op: &'b SplitOp,
    ) -> Result<DynSplitOutputs<'b>, DiagramError> {
        if let Some(reg) = self.messages.get(&output.type_id) {
            reg.operations.split(builder, output, split_op)
        } else {
            Err(DiagramError::NotSplittable)
        }
    }

    /// Register a split function if not already registered, returns true if the new
    /// function is registered.
    pub(super) fn register_split<T, F, S>(&mut self) -> bool
    where
        T: Any,
        F: DynSplit<T, S>,
    {
        let ops = &mut self
            .messages
            .entry(TypeId::of::<T>())
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
    ) -> Result<DynOutput, DiagramError>
    where
        OutputIter: IntoIterator<Item = DynOutput>,
    {
        let mut i = outputs.into_iter().peekable();
        let output_type_id = if let Some(o) = i.peek() {
            Some(o.type_id.clone())
        } else {
            None
        };

        if let Some(output_type_id) = output_type_id {
            if let Some(reg) = self.messages.get(&output_type_id) {
                reg.operations.join(builder, i)
            } else {
                Err(DiagramError::NotJoinable)
            }
        } else {
            Err(DiagramError::NotJoinable)
        }
    }

    /// Register a join function if not already registered, returns true if the new
    /// function is registered.
    pub(super) fn register_join<T>(&mut self, f: JoinFn) -> bool
    where
        T: Any,
    {
        let ops = &mut self
            .messages
            .entry(TypeId::of::<T>())
            .or_insert(MessageRegistration::new::<T>())
            .operations;
        if ops.join_impl.is_some() {
            return false;
        }

        ops.join_impl = Some(f);

        true
    }

    fn serialize_messages<S>(
        v: &HashMap<TypeId, MessageRegistration>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s = serializer.serialize_map(Some(v.len()))?;
        for msg in v.values() {
            // should we use short name? It makes the serialized json more readable at the cost
            // of greatly increased chance of key conflicts.
            // let short_name = {
            //     if let Some(start) = msg.type_name.rfind(":") {
            //         &msg.type_name[start + 1..]
            //     } else {
            //         msg.type_name
            //     }
            // };
            s.serialize_entry(msg.type_name, msg)?;
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

impl Default for Registry {
    fn default() -> Self {
        let mut settings = SchemaSettings::default();
        settings.definitions_path = "#/schemas/".to_string();
        Registry {
            nodes: Default::default(),
            messages: MessageRegistry {
                schema_generator: SchemaGenerator::new(settings),
                messages: HashMap::new(),
            },
        }
    }
}

impl Registry {
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
    /// use bevy_impulse::{NodeBuilderOptions, Registry};
    ///
    /// let mut registry = Registry::new();
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
    /// use bevy_impulse::{NodeBuilderOptions, Registry};
    ///
    /// struct NonSerializable {
    ///     data: String
    /// }
    ///
    /// let mut registry = Registry::new();
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
        let mut registry = Registry::new();
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
        let mut registry = Registry::new();
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
        let mut registry = Registry::new();
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
        let mut registry = Registry::new();
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
        let mut registry = Registry::new();

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

        let mut registry = Registry::new();
        registry
            .opt_out()
            .no_request_deserializing()
            .no_response_cloning()
            .register_node_builder(
                NodeBuilderOptions::new("opaque_request_map").with_name("Test Name"),
                move |builder, _config: ()| builder.create_map_block(opaque_request_map),
            );
        assert!(registry.get_registration("opaque_request_map").is_ok());
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
        assert!(registry.get_registration("opaque_response_map").is_ok());
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
        assert!(registry.get_registration("opaque_req_resp_map").is_ok());
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
    fn test_serialize_registry() {
        let mut reg = Registry::new();

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
                builder.create_map_block(|_: Opaque| Bar {
                    foo: Foo {
                        hello: "hello".to_string(),
                    },
                })
            });

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
