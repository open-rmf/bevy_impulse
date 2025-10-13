/*
 * Copyright (C) 2025 Open Source Robotics Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
*/

mod buffer_schema;
mod fork_clone_schema;
mod fork_result_schema;
mod join_schema;
mod node_schema;
mod operation_ref;
mod registration;
mod scope_schema;
mod section_schema;
mod serialization;
mod split_schema;
mod stream_out_schema;
mod supported;
mod transform_schema;
mod unzip_schema;
mod workflow_builder;

#[cfg(feature = "grpc")]
pub mod grpc;

#[cfg(feature = "zenoh")]
pub mod zenoh;

use bevy_derive::{Deref, DerefMut};
use bevy_ecs::system::Commands;
use buffer_schema::{BufferAccessSchema, BufferSchema, ListenSchema};
use fork_clone_schema::{DynForkClone, ForkCloneSchema, PerformForkClone};
use fork_result_schema::{DynForkResult, ForkResultSchema};
pub use join_schema::JoinOutput;
use join_schema::{JoinSchema, SerializedJoinSchema};
pub use node_schema::NodeSchema;
pub use operation_ref::*;
pub use registration::*;
pub use scope_schema::*;
pub use section_schema::*;
pub use serialization::*;
pub use split_schema::*;
pub use stream_out_schema::*;
use tracing::debug;
use transform_schema::{TransformError, TransformSchema};
use unzip_schema::UnzipSchema;
pub use workflow_builder::*;

use anyhow::Error as Anyhow;

use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    fmt::Display,
    io::Read,
    sync::Arc,
};

pub use crate::type_info::TypeInfo;
use crate::{
    is_default, Builder, IncompatibleLayout, IncrementalScopeError, JsonMessage, Scope, Service,
    SpawnWorkflowExt, SplitConnectionError, StreamPack,
};

use schemars::{json_schema, JsonSchema, Schema, SchemaGenerator};

use serde::{
    de::{Error, Visitor},
    ser::SerializeMap,
    Deserialize, Deserializer, Serialize, Serializer,
};

use thiserror::Error as ThisError;

const CURRENT_DIAGRAM_VERSION: &str = "0.1.0";
const SUPPORTED_DIAGRAM_VERSION: &str = ">=0.1.0, <0.2.0";
const RESERVED_OPERATION_NAMES: [&'static str; 2] = ["", "builtin"];

pub type BuilderId = Arc<str>;
pub type OperationName = Arc<str>;
pub type DisplayText = Arc<str>;

#[derive(
    Debug, Clone, Serialize, Deserialize, JsonSchema, Hash, PartialEq, Eq, PartialOrd, Ord,
)]
#[serde(untagged, rename_all = "snake_case")]
pub enum NextOperation {
    Name(OperationName),
    Builtin {
        builtin: BuiltinTarget,
    },
    /// Refer to an "inner" operation of one of the sibling operations in a
    /// diagram. This can be used to target section inputs.
    Namespace(NamespacedOperation),
}

impl NextOperation {
    pub fn dispose() -> Self {
        NextOperation::Builtin {
            builtin: BuiltinTarget::Dispose,
        }
    }

    pub fn terminate() -> Self {
        NextOperation::Builtin {
            builtin: BuiltinTarget::Terminate,
        }
    }
}

impl Display for NextOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Name(operation_id) => f.write_str(operation_id),
            Self::Namespace(NamespacedOperation {
                namespace,
                operation,
            }) => write!(f, "{namespace}:{operation}"),
            Self::Builtin { builtin } => write!(f, "builtin:{builtin}"),
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
/// This describes an operation that exists inside of some namespace, such as a
/// [`Section`]. This will serialize as a map with a single entry of
/// `{ "<namespace>": "<operation>" }`.
pub struct NamespacedOperation {
    pub namespace: OperationName,
    pub operation: OperationName,
}

impl Serialize for NamespacedOperation {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry(&self.namespace, &self.operation)?;
        map.end()
    }
}

struct NamespacedOperationVisitor;

impl<'de> Visitor<'de> for NamespacedOperationVisitor {
    type Value = NamespacedOperation;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str(
            "a map with exactly one entry of { \"<namespace>\" : \"<operation>\" } \
            whose key is the namespace string and whose value is the operation string",
        )
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let (key, value) = map.next_entry::<String, String>()?.ok_or_else(|| {
            A::Error::custom(
                "namespaced operation must be a map from the namespace to the operation name",
            )
        })?;

        if !map.next_key::<String>()?.is_none() {
            return Err(A::Error::custom(
                "namespaced operation must contain exactly one entry",
            ));
        }

        Ok(NamespacedOperation {
            namespace: key.into(),
            operation: value.into(),
        })
    }
}

impl<'de> Deserialize<'de> for NamespacedOperation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(NamespacedOperationVisitor)
    }
}

impl JsonSchema for NamespacedOperation {
    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        json_schema!({
          "title": "NamespacedOperation",
          "description": "Refer to an operation inside of a namespace, e.g. { \"<namespace>\": \"<operation>\"",
          "type": "object",
          "maxProperties": 1,
          "minProperties": 1,
          "additionalProperties": {
            "type": "string"
          }
        })
    }

    fn schema_name() -> Cow<'static, str> {
        "NamespacedOperation".into()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", untagged)]
pub enum BufferSelection {
    Dict(HashMap<String, NextOperation>),
    Array(Vec<NextOperation>),
}

impl BufferSelection {
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Dict(d) => d.is_empty(),
            Self::Array(a) => a.is_empty(),
        }
    }
}

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    JsonSchema,
    Hash,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    strum::Display,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum BuiltinTarget {
    /// Use the output to terminate the current scope. The value passed into
    /// this operation will be the return value of the scope.
    Terminate,

    /// Dispose of the output.
    Dispose,

    /// When triggered, cancel the current scope. If this is an inner scope of a
    /// workflow then the parent scope will see a disposal happen. If this is
    /// the root scope of a workflow then the whole workflow will cancel.
    Cancel,
}

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    JsonSchema,
    Hash,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    strum::Display,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum BuiltinSource {
    Start,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct TerminateSchema {}

#[derive(Clone, strum::Display, Debug, JsonSchema, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
#[strum(serialize_all = "snake_case")]
pub enum DiagramOperation {
    Node(NodeSchema),
    Section(SectionSchema),
    Scope(ScopeSchema),
    StreamOut(StreamOutSchema),
    ForkClone(ForkCloneSchema),
    Unzip(UnzipSchema),
    ForkResult(ForkResultSchema),
    Split(SplitSchema),
    Join(JoinSchema),
    SerializedJoin(SerializedJoinSchema),
    Transform(TransformSchema),
    Buffer(BufferSchema),
    BufferAccess(BufferAccessSchema),
    Listen(ListenSchema),
}

impl BuildDiagramOperation for DiagramOperation {
    fn build_diagram_operation(
        &self,
        id: &OperationName,
        ctx: &mut DiagramContext,
    ) -> Result<BuildStatus, DiagramErrorCode> {
        match self {
            Self::Buffer(op) => op.build_diagram_operation(id, ctx),
            Self::BufferAccess(op) => op.build_diagram_operation(id, ctx),
            Self::ForkClone(op) => op.build_diagram_operation(id, ctx),
            Self::ForkResult(op) => op.build_diagram_operation(id, ctx),
            Self::Join(op) => op.build_diagram_operation(id, ctx),
            Self::Listen(op) => op.build_diagram_operation(id, ctx),
            Self::Node(op) => op.build_diagram_operation(id, ctx),
            Self::Scope(op) => op.build_diagram_operation(id, ctx),
            Self::Section(op) => op.build_diagram_operation(id, ctx),
            Self::SerializedJoin(op) => op.build_diagram_operation(id, ctx),
            Self::Split(op) => op.build_diagram_operation(id, ctx),
            Self::StreamOut(op) => op.build_diagram_operation(id, ctx),
            Self::Transform(op) => op.build_diagram_operation(id, ctx),
            Self::Unzip(op) => op.build_diagram_operation(id, ctx),
        }
    }
}

/// Returns the schema for [`String`]
fn schema_with_string(generator: &mut SchemaGenerator) -> Schema {
    generator.subschema_for::<String>()
}

/// deserialize semver and validate that it has a supported version
fn deserialize_semver<'de, D>(de: D) -> Result<semver::Version, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(de)?;
    // SAFETY: `SUPPORTED_DIAGRAM_VERSION` is a const, this will never fail.
    let ver_req = semver::VersionReq::parse(SUPPORTED_DIAGRAM_VERSION).unwrap();
    let ver = semver::Version::parse(&s).map_err(|_| {
        serde::de::Error::invalid_value(serde::de::Unexpected::Str(&s), &SUPPORTED_DIAGRAM_VERSION)
    })?;
    if !ver_req.matches(&ver) {
        return Err(serde::de::Error::invalid_value(
            serde::de::Unexpected::Str(&s),
            &SUPPORTED_DIAGRAM_VERSION,
        ));
    }
    Ok(ver)
}

/// serialize semver as a string
fn serialize_semver<S>(o: &semver::Version, ser: S) -> Result<S::Ok, S::Error>
where
    S: serde::ser::Serializer,
{
    o.to_string().serialize(ser)
}

#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Diagram {
    /// Version of the diagram, should always be `0.1.0`.
    #[serde(
        deserialize_with = "deserialize_semver",
        serialize_with = "serialize_semver"
    )]
    #[schemars(schema_with = "schema_with_string")]
    version: semver::Version,

    #[serde(default)]
    pub templates: Templates,

    /// Indicates where the workflow should start running.
    pub start: NextOperation,

    /// To simplify diagram definitions, the diagram workflow builder will
    /// sometimes insert implicit operations into the workflow, such as implicit
    /// serializing and deserializing. These implicit operations may be fallible.
    ///
    /// This field indicates how a failed implicit operation should be handled.
    /// If left unspecified, an implicit error will cause the entire workflow to
    /// be cancelled.
    #[serde(default)]
    pub on_implicit_error: Option<NextOperation>,

    /// Operations that define the workflow
    pub ops: Operations,

    /// Whether the operations in the workflow should be traced by default.
    /// Being traced means each operation will emit an event each time it is
    /// triggered. You can decide whether that event contains the serialized
    /// message data that triggered the operation.
    ///
    /// If bevy_impulse is not compiled with the "trace" feature then this
    /// setting will have no effect.
    #[serde(default, skip_serializing_if = "is_default")]
    pub default_trace: TraceToggle,
}

#[derive(Default, Debug, Clone, Copy, JsonSchema, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TraceToggle {
    /// Do not emit any signal when the operation is activated.
    #[default]
    Off,
    /// Emit a minimal signal with just the operation information when the
    /// operation is activated.
    On,
    /// Emit a signal that includes a serialized copy of the message when the
    /// operation is activated. This may substantially increase the overhead of
    /// triggering operations depending on the size and frequency of the messages,
    /// so it is recommended only for high-level workflows or for debugging.
    ///
    /// If the message is not serializable then it will simply not be included
    /// in the event information.
    Messages,
}

impl TraceToggle {
    pub fn is_on(&self) -> bool {
        !matches!(self, Self::Off)
    }

    pub fn with_messages(&self) -> bool {
        matches!(self, Self::Messages)
    }
}

/// Settings that describe how an operation should be traced. It is recommended
/// to add this to each operation with #[serde(flatten)].
#[derive(Default, Debug, Clone, JsonSchema, Serialize, Deserialize)]
pub struct TraceSettings {
    /// Override for text that should be displayed for an operation within an
    /// editor.
    #[serde(default, skip_serializing_if = "is_default")]
    pub display_text: Option<DisplayText>,
    /// Set what the tracing behavior should be for this operation. If this is
    /// left unspecified then the default trace setting of the diagram will be
    /// used.
    #[serde(default, skip_serializing_if = "is_default")]
    pub trace: Option<TraceToggle>,
}

impl Diagram {
    /// Begin creating a new diagram
    pub fn new(start: NextOperation) -> Self {
        Self {
            version: semver::Version::parse(CURRENT_DIAGRAM_VERSION).unwrap(),
            start,
            templates: Default::default(),
            on_implicit_error: Default::default(),
            ops: Default::default(),
            default_trace: Default::default(),
        }
    }

    /// Spawns a workflow from this diagram.
    ///
    /// # Examples
    ///
    /// ```
    /// use bevy_impulse::*;
    ///
    /// let mut app = bevy_app::App::new();
    /// let mut registry = DiagramElementRegistry::new();
    /// registry.register_node_builder(NodeBuilderOptions::new("echo".to_string()), |builder, _config: ()| {
    ///     builder.create_map_block(|msg: String| msg)
    /// });
    ///
    /// let json_str = r#"
    /// {
    ///     "version": "0.1.0",
    ///     "start": "echo",
    ///     "ops": {
    ///         "echo": {
    ///             "type": "node",
    ///             "builder": "echo",
    ///             "next": { "builtin": "terminate" }
    ///         }
    ///     }
    /// }
    /// "#;
    ///
    /// let diagram = Diagram::from_json_str(json_str)?;
    /// let workflow = app.world_mut().command(|cmds| diagram.spawn_io_workflow::<JsonMessage, JsonMessage>(cmds, &registry))?;
    /// # Ok::<_, Box<dyn std::error::Error>>(())
    /// ```
    // TODO(koonpeng): Support streams other than `()` #43.
    /* pub */
    fn spawn_workflow<Request, Response, Streams>(
        &self,
        cmds: &mut Commands,
        registry: &DiagramElementRegistry,
    ) -> Result<Service<Request, Response, Streams>, DiagramError>
    where
        Request: 'static + Send + Sync,
        Response: 'static + Send + Sync,
        Streams: StreamPack,
    {
        let mut err: Option<DiagramError> = None;

        macro_rules! unwrap_or_return {
            ($v:expr) => {
                match $v {
                    Ok(v) => v,
                    Err(e) => {
                        err = Some(e);
                        return;
                    }
                }
            };
        }

        let w = cmds.spawn_workflow(
            |scope: Scope<Request, Response, Streams>, builder: &mut Builder| {
                debug!(
                    "spawn workflow, scope input: {:?}, terminate: {:?}",
                    scope.input.id(),
                    scope.terminate.id()
                );

                unwrap_or_return!(create_workflow(scope, builder, registry, self));
            },
        );

        if let Some(err) = err {
            return Err(err);
        }

        Ok(w)
    }

    /// Spawns a workflow from this diagram.
    ///
    /// # Examples
    ///
    /// ```
    /// use bevy_impulse::*;
    ///
    /// let mut app = bevy_app::App::new();
    /// let mut registry = DiagramElementRegistry::new();
    /// registry.register_node_builder(NodeBuilderOptions::new("echo".to_string()), |builder, _config: ()| {
    ///     builder.create_map_block(|msg: String| msg)
    /// });
    ///
    /// let json_str = r#"
    /// {
    ///     "version": "0.1.0",
    ///     "start": "echo",
    ///     "ops": {
    ///         "echo": {
    ///             "type": "node",
    ///             "builder": "echo",
    ///             "next": { "builtin": "terminate" }
    ///         }
    ///     }
    /// }
    /// "#;
    ///
    /// let diagram = Diagram::from_json_str(json_str)?;
    /// let workflow = app.world_mut().command(|cmds| diagram.spawn_io_workflow::<JsonMessage, JsonMessage>(cmds, &registry))?;
    /// # Ok::<_, Box<dyn std::error::Error>>(())
    /// ```
    pub fn spawn_io_workflow<Request, Response>(
        &self,
        cmds: &mut Commands,
        registry: &DiagramElementRegistry,
    ) -> Result<Service<Request, Response, ()>, DiagramError>
    where
        Request: 'static + Send + Sync,
        Response: 'static + Send + Sync,
    {
        self.spawn_workflow::<Request, Response, ()>(cmds, registry)
    }

    pub fn from_json(value: serde_json::Value) -> Result<Self, serde_json::Error> {
        serde_json::from_value(value)
    }

    pub fn from_json_str(s: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(s)
    }

    pub fn from_reader<R>(r: R) -> Result<Self, serde_json::Error>
    where
        R: Read,
    {
        serde_json::from_reader(r)
    }

    /// Make sure all operation names are valid, e.g. no reserved words such as
    /// `builtin` are being used.
    pub fn validate_operation_names(&self) -> Result<(), DiagramErrorCode> {
        self.ops.validate_operation_names()?;
        self.templates.validate_operation_names()?;
        Ok(())
    }

    /// Validate the templates that are being used within the `ops` section, or
    /// recursively within any templates used by the `ops` section. Any unused
    /// templates will not be validated.
    pub fn validate_template_usage(&self) -> Result<(), DiagramErrorCode> {
        for op in self.ops.values() {
            match op.as_ref() {
                DiagramOperation::Section(section) => match &section.provider {
                    SectionProvider::Template(template) => {
                        self.templates.validate_template(template)?;
                    }
                    _ => continue,
                },
                _ => continue,
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Default, JsonSchema, Serialize, Deserialize, Deref, DerefMut)]
#[serde(transparent, rename_all = "snake_case")]
pub struct Operations(Arc<HashMap<OperationName, Arc<DiagramOperation>>>);

impl Operations {
    /// Get an operation from this map, or a diagram error code if the operation
    /// is not available.
    pub fn get_op(&self, op_id: &Arc<str>) -> Result<&Arc<DiagramOperation>, DiagramErrorCode> {
        self.get(op_id)
            .ok_or_else(|| DiagramErrorCode::operation_name_not_found(op_id.clone()))
    }

    pub fn validate_operation_names(&self) -> Result<(), DiagramErrorCode> {
        validate_operation_names(&self.0)
    }
}

#[derive(Debug, Clone, Default, JsonSchema, Serialize, Deserialize, Deref, DerefMut)]
#[serde(transparent, rename_all = "snake_case")]
pub struct Templates(HashMap<OperationName, SectionTemplate>);

impl Templates {
    /// Get a template from this map, or a diagram error code if the template is
    /// not available.
    pub fn get_template(
        &self,
        template_id: &OperationName,
    ) -> Result<&SectionTemplate, DiagramErrorCode> {
        self.get(template_id)
            .ok_or_else(|| DiagramErrorCode::TemplateNotFound(template_id.clone()))
    }

    pub fn validate_operation_names(&self) -> Result<(), DiagramErrorCode> {
        for (name, template) in &self.0 {
            validate_operation_name(name)?;
            validate_operation_names(&template.ops)?;
            // TODO(@mxgrey): Validate correctness of input, output, and buffer mapping
        }

        Ok(())
    }

    /// Check for potential issues in one of the templates, e.g. a circular
    /// dependency with other templates.
    pub fn validate_template(&self, template_id: &OperationName) -> Result<(), DiagramErrorCode> {
        check_circular_template_dependency(template_id, &self.0)?;
        Ok(())
    }
}

fn validate_operation_names(
    ops: &HashMap<OperationName, Arc<DiagramOperation>>,
) -> Result<(), DiagramErrorCode> {
    for name in ops.keys() {
        validate_operation_name(name)?;
    }

    Ok(())
}

fn validate_operation_name(name: &str) -> Result<(), DiagramErrorCode> {
    for reserved in &RESERVED_OPERATION_NAMES {
        if name == *reserved {
            return Err(DiagramErrorCode::InvalidUseOfReservedName(*reserved));
        }
    }

    Ok(())
}

fn check_circular_template_dependency(
    start_from: &OperationName,
    templates: &HashMap<OperationName, SectionTemplate>,
) -> Result<(), DiagramErrorCode> {
    let mut queue = Vec::new();
    queue.push(TemplateStack::new(start_from));

    while let Some(top) = queue.pop() {
        let Some(template) = templates.get(&top.next) else {
            return Err(DiagramErrorCode::UnknownTemplate(top.next));
        };

        for op in template.ops.0.values() {
            match op.as_ref() {
                DiagramOperation::Section(section) => match &section.provider {
                    SectionProvider::Template(template) => {
                        queue.push(top.child(template)?);
                    }
                    _ => continue,
                },
                _ => continue,
            }
        }
    }

    Ok(())
}

struct TemplateStack {
    used: HashSet<OperationName>,
    next: OperationName,
}

impl TemplateStack {
    fn new(op: &OperationName) -> Self {
        TemplateStack {
            used: HashSet::from_iter([Arc::clone(op)]),
            next: Arc::clone(op),
        }
    }

    fn child(&self, next: &OperationName) -> Result<Self, DiagramErrorCode> {
        let mut used = self.used.clone();
        if !used.insert(Arc::clone(next)) {
            return Err(DiagramErrorCode::CircularTemplateDependency(
                used.into_iter().collect(),
            ));
        }

        Ok(Self {
            used,
            next: Arc::clone(next),
        })
    }
}

#[derive(ThisError, Debug)]
#[error("{context} {code}")]
pub struct DiagramError {
    pub context: DiagramErrorContext,

    #[source]
    pub code: DiagramErrorCode,
}

impl DiagramError {
    pub fn in_operation(op_id: impl Into<OperationRef>, code: DiagramErrorCode) -> Self {
        Self {
            context: DiagramErrorContext {
                op_id: Some(op_id.into()),
            },
            code,
        }
    }
}

#[derive(Debug)]
pub struct DiagramErrorContext {
    op_id: Option<OperationRef>,
}

impl Display for DiagramErrorContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(op_id) = &self.op_id {
            write!(f, "in operation [{}],", op_id)?;
        }
        Ok(())
    }
}

#[derive(ThisError, Debug)]
pub enum DiagramErrorCode {
    #[error("node builder [{0}] is not registered")]
    BuilderNotFound(BuilderId),

    #[error("node builder [{builder}] encountered an error: {error}")]
    NodeBuildingError { builder: BuilderId, error: Anyhow },

    #[error("section builder [{builder}] encountered an error: {error}")]
    SectionBuildingError { builder: BuilderId, error: Anyhow },

    #[error("operation [{0}] not found")]
    OperationNotFound(NextOperation),

    #[error("section template [{0}] does not exist")]
    TemplateNotFound(OperationName),

    #[error("{0}")]
    TypeMismatch(#[from] TypeMismatch),

    #[error("{0}")]
    MissingStream(#[from] MissingStream),

    #[error("Operation [{0}] attempted to instantiate a duplicate of itself.")]
    DuplicateInputsCreated(OperationRef),

    #[error("Operation [{0}] attempted to instantiate a duplicate buffer.")]
    DuplicateBuffersCreated(OperationRef),

    #[error("Missing a connection to start or terminate. A workflow cannot run with a valid connection to each.")]
    MissingStartOrTerminate,

    #[error("Serialization was disabled for the target message type.")]
    NotSerializable(TypeInfo),

    #[error("Deserialization was disabled for the target message type.")]
    NotDeserializable(TypeInfo),

    #[error("Cloning was disabled for the target message type. Type: {0}")]
    NotCloneable(TypeInfo),

    #[error("The target message type does not support unzipping. Type: {0}")]
    NotUnzippable(TypeInfo),

    #[error("The number of elements in the unzip expected by the diagram [{expected}] is different from the real number [{actual}]")]
    UnzipMismatch {
        expected: usize,
        actual: usize,
        elements: Vec<TypeInfo>,
    },

    #[error("Call .with_fork_result() on your node to be able to fork its Result-type output. Type: {0}")]
    CannotForkResult(TypeInfo),

    #[error("Response cannot be split. Make sure to use .with_split() when building the node. Type: {0}")]
    NotSplittable(TypeInfo),

    #[error(
        "Message cannot be joined. Make sure to use .with_join() when building the target node. Type: {0}"
    )]
    NotJoinable(TypeInfo),

    #[error("Empty join is not allowed.")]
    EmptyJoin,

    #[error("Target type cannot be determined from [next] and [target_node] is not provided or cannot be inferred from.")]
    UnknownTarget,

    #[error("There was an attempt to connect to an unknown operation: [{0}]")]
    UnknownOperation(OperationRef),

    #[error("There was an attempt to use an unknown section template: [{0}]")]
    UnknownTemplate(OperationName),

    #[error("There was an attempt to use an operation in an invalid way: [{0}]")]
    InvalidOperation(OperationRef),

    #[error(transparent)]
    CannotTransform(#[from] TransformError),

    #[error("box/unbox operation for the message is not registered")]
    CannotBoxOrUnbox,

    #[error("Buffer access was not enabled for a node connected to a buffer access operation. Make sure to use .with_buffer_access() when building the node.")]
    CannotBufferAccess,

    #[error("cannot listen on these buffers to produce a request of [{0}]")]
    CannotListen(TypeInfo),

    #[error(transparent)]
    IncompatibleBuffers(#[from] IncompatibleLayout),

    #[error(transparent)]
    SectionError(#[from] SectionError),

    #[error("one or more operation is missing inputs")]
    IncompleteDiagram,

    #[error("the config of the operation has an error: {0}")]
    ConfigError(serde_json::Error),

    #[error("failed to create trace info for the operation: {0}")]
    TraceInfoError(serde_json::Error),

    #[error(transparent)]
    ConnectionError(#[from] SplitConnectionError),

    #[error("a type being used in the diagram was not registered {0}")]
    UnregisteredType(TypeInfo),

    #[error("The build of the workflow came to a halt, reasons:\n{reasons:?}")]
    BuildHalted {
        /// Reasons that operations were unable to make progress building
        reasons: HashMap<OperationRef, Cow<'static, str>>,
    },

    #[error(
        "The workflow building process has had an excessive number of iterations. \
        This may indicate an implementation bug or an extraordinarily complex diagram."
    )]
    ExcessiveIterations,

    #[error("An operation was given a reserved name [{0}]")]
    InvalidUseOfReservedName(&'static str),

    #[error("an error happened while building a nested diagram: {0}")]
    NestedError(Box<DiagramError>),

    #[error("A circular redirection exists between operations: {}", format_list(&.0))]
    CircularRedirect(Vec<OperationRef>),

    #[error("A circular dependency exists between templates: {}", format_list(&.0))]
    CircularTemplateDependency(Vec<OperationName>),

    #[error("An error occurred while finishing the workflow build: {0}")]
    FinishingErrors(FinishingErrors),

    #[error("An error occurred while creating a scope: {0}")]
    IncrementalScopeError(#[from] IncrementalScopeError),
}

fn format_list<T: std::fmt::Display>(list: &[T]) -> String {
    let mut output = String::new();
    for op in list {
        output += &format!("[{op}]");
    }

    output
}

impl From<DiagramErrorCode> for DiagramError {
    fn from(code: DiagramErrorCode) -> Self {
        DiagramError {
            context: DiagramErrorContext { op_id: None },
            code,
        }
    }
}

impl DiagramErrorCode {
    pub fn operation_name_not_found(name: OperationName) -> Self {
        DiagramErrorCode::OperationNotFound(NextOperation::Name(name))
    }

    pub fn in_operation(self, op_id: OperationRef) -> DiagramError {
        DiagramError::in_operation(op_id, self)
    }
}

/// An error that occurs when a diagram description expects a node to provide a
/// named output stream, but the node does not provide any output stream that
/// matches the expected name.
#[derive(ThisError, Debug)]
#[error("An expected stream is not provided by this node: {missing_name}. Available stream names: {}", format_list(&available_names))]
pub struct MissingStream {
    pub missing_name: OperationName,
    pub available_names: Vec<OperationName>,
}

#[derive(ThisError, Debug, Default)]
pub struct FinishingErrors {
    pub errors: HashMap<OperationRef, DiagramErrorCode>,
}

impl FinishingErrors {
    pub fn as_result(self) -> Result<(), Self> {
        if self.errors.is_empty() {
            Ok(())
        } else {
            Err(self)
        }
    }
}

impl std::fmt::Display for FinishingErrors {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (op, code) in &self.errors {
            write!(f, " - [{op}]: {code}")?;
        }

        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod testing;

#[cfg(test)]
mod tests {
    use crate::{Cancellation, CancellationCause};
    use serde_json::json;
    use test_log::test;
    use testing::DiagramTestFixture;

    use super::*;

    #[test]
    fn test_no_terminate() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "op1",
            "ops": {
                "op1": {
                    "type": "node",
                    "builder": "multiply3",
                    "next": { "builtin": "dispose" },
                },
            },
        }))
        .unwrap();

        let err = fixture
            .spawn_and_run::<_, JsonMessage>(&diagram, JsonMessage::from(4))
            .unwrap_err();
        assert!(fixture.context.no_unhandled_errors());
        assert!(matches!(
            *err.downcast_ref::<Cancellation>().unwrap().cause,
            CancellationCause::Unreachable(_)
        ));
    }

    #[test]
    fn test_unserializable_start() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "op1",
            "ops": {
                "op1": {
                    "type": "node",
                    "builder": "opaque_request",
                    "next": { "builtin": "terminate" },
                },
            },
        }))
        .unwrap();

        let err = fixture.spawn_json_io_workflow(&diagram).unwrap_err();
        assert!(
            matches!(err.code, DiagramErrorCode::TypeMismatch { .. }),
            "{:?}",
            err
        );
    }

    #[test]
    fn test_unserializable_terminate() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "op1",
            "ops": {
                "op1": {
                    "type": "node",
                    "builder": "opaque_response",
                    "next": { "builtin": "terminate" },
                },
            },
        }))
        .unwrap();

        let err = fixture.spawn_json_io_workflow(&diagram).unwrap_err();
        assert!(
            matches!(err.code, DiagramErrorCode::NotSerializable(_)),
            "{:?}",
            err
        );
    }

    #[test]
    fn test_mismatch_types() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "op1",
            "ops": {
                "op1": {
                    "type": "node",
                    "builder": "multiply3",
                    "next": "op2",
                },
                "op2": {
                    "type": "node",
                    "builder": "opaque_request",
                    "next": { "builtin": "terminate" },
                },
            },
        }))
        .unwrap();

        let err = fixture.spawn_json_io_workflow(&diagram).unwrap_err();
        assert!(
            matches!(
                err.code,
                DiagramErrorCode::TypeMismatch(TypeMismatch {
                    target_type: _,
                    source_type: _
                })
            ),
            "{:?}",
            err
        );
    }

    #[test]
    fn test_disconnected() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "op1",
            "ops": {
                "op1": {
                    "type": "node",
                    "builder": "multiply3",
                    "next": "op2",
                },
                "op2": {
                    "type": "node",
                    "builder": "multiply3",
                    "next": "op1",
                },
            },
        }))
        .unwrap();

        let err = fixture
            .spawn_and_run::<_, JsonMessage>(&diagram, JsonMessage::from(4))
            .unwrap_err();
        assert!(fixture.context.no_unhandled_errors());
        assert!(matches!(
            *err.downcast_ref::<Cancellation>().unwrap().cause,
            CancellationCause::Unreachable(_),
        ));
    }

    #[test]
    fn test_looping_diagram() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "op1",
            "ops": {
                "op1": {
                    "type": "node",
                    "builder": "multiply3",
                    "next": "fork_clone",
                },
                "fork_clone": {
                    "type": "fork_clone",
                    "next": ["op1", "op2"],
                },
                "op2": {
                    "type": "node",
                    "builder": "multiply3",
                    "next": { "builtin": "terminate" },
                },
            },
        }))
        .unwrap();

        let result: JsonMessage = fixture
            .spawn_and_run(&diagram, JsonMessage::from(4))
            .unwrap();
        assert!(fixture.context.no_unhandled_errors());
        assert_eq!(result, 36);
    }

    #[test]
    fn test_noop_diagram() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": { "builtin": "terminate" },
            "ops": {},
        }))
        .unwrap();

        let result: JsonMessage = fixture
            .spawn_and_run(&diagram, JsonMessage::from(4))
            .unwrap();
        assert!(fixture.context.no_unhandled_errors());
        assert_eq!(result, 4);
    }

    #[test]
    fn test_serialized_diagram() {
        let mut fixture = DiagramTestFixture::new();

        let json_str = r#"
        {
            "version": "0.1.0",
            "start": "multiply3",
            "ops": {
                "multiply3": {
                    "type": "node",
                    "builder": "multiply_by",
                    "config": 7,
                    "next": { "builtin": "terminate" }
                }
            }
        }
        "#;

        let result: JsonMessage = fixture
            .spawn_and_run(
                &Diagram::from_json_str(json_str).unwrap(),
                JsonMessage::from(4),
            )
            .unwrap();
        assert!(fixture.context.no_unhandled_errors());
        assert_eq!(result, 28);
    }

    /// Test that we can transform on a slot of a unzipped response. Operations which changes
    /// the output type has extra serialization logic.
    #[test]
    fn test_transform_unzip() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "op1",
            "ops": {
                "op1": {
                    "type": "node",
                    "builder": "multiply3_5",
                    "next": "unzip",
                },
                "unzip": {
                    "type": "unzip",
                    "next": [
                        "transform",
                        { "builtin": "dispose" },
                    ],
                },
                "transform": {
                    "type": "transform",
                    "cel": "777",
                    "next": { "builtin": "terminate" },
                },
            },
        }))
        .unwrap();

        let result: JsonMessage = fixture
            .spawn_and_run(&diagram, JsonMessage::from(4))
            .unwrap();
        assert!(fixture.context.no_unhandled_errors());
        assert_eq!(result, 777);
    }

    #[test]
    fn test_unknown_operation_detection() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "op1",
            "ops": {
                "op1": {
                    "type": "node",
                    "builder": "multiply3_5",
                    "next": "clone",
                },
                "clone": {
                    "type": "fork_clone",
                    "next": [
                        "unknown",
                        { "builtin": "terminate" },
                    ],
                },
            },
        }))
        .unwrap();

        let result = fixture.spawn_json_io_workflow(&diagram).unwrap_err();

        assert!(matches!(result.code, DiagramErrorCode::UnknownOperation(_),));
    }

    #[test]
    fn test_fork_result_termination() {
        let mut fixture = DiagramTestFixture::new();
        fixture
            .registry
            .register_message::<Result<f32, ()>>()
            .with_fork_result();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "fork",
            "ops": {
                "fork": {
                    "type": "fork_result",
                    "ok": { "builtin": "terminate" },
                    "err": { "builtin": "dispose" }
                }
            }
        }))
        .unwrap();

        let result: f32 = fixture.spawn_and_run(&diagram, Ok::<_, ()>(5_f32)).unwrap();
        assert!(fixture.context.no_unhandled_errors());
        assert_eq!(result, 5.0);
    }
}
